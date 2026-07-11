use crate::fetch::{AimdLimiter, FetchConfig, HedgeBudget};
use crate::{parse_list_v2, ObjectMeta};
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use holys3_core::{DocumentBody, DocumentSpool, StaleSource};
use holys3_sigv4::{encode_path, encode_query_component, sign_request, Credentials, SignedHeaders};
use reqwest::StatusCode;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Notify;

mod download;
mod upload;

#[cfg(test)]
use download::coalesce_ranges;

static FORMAT: LazyLock<Vec<time::format_description::BorrowedFormatItem<'static>>> =
    LazyLock::new(|| {
        time::format_description::parse_borrowed::<2>("[year][month][day]T[hour][minute][second]Z")
            .expect("invalid amz date format")
    });

/// Hedge window for small ranged reads (index blocks): ~2x a slow S3
/// first-byte latency. Reads below `SMALL_READ_MAX` qualify.
const SMALL_READ_HEDGE: Duration = Duration::from_millis(300);
const SMALL_READ_MAX: u64 = 4 * 1024 * 1024;

/// Bodies above one part upload as concurrent multipart parts: single PUTs
/// of GB-scale blobs time out on slow uplinks and cap at 5 GiB anyway.
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 4;
const MULTIPART_BUFFER_BUDGET: usize = 256 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u64 = 10_000;
const MAX_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_OBJECT_SIZE: u64 = MAX_MULTIPART_PARTS * MAX_MULTIPART_PART_SIZE;
/// Request bodies at or above this use the upload client + deadline.
const UPLOAD_BODY_THRESHOLD: usize = 1024 * 1024;
const BYTE_PERMIT_SIZE: u64 = 1024 * 1024;

fn multipart_part_size(len: u64) -> Result<usize> {
    anyhow::ensure!(
        len <= MAX_MULTIPART_OBJECT_SIZE,
        "S3 object is {len} bytes, exceeds {MAX_MULTIPART_OBJECT_SIZE}"
    );
    let mib = 1024 * 1024u64;
    let needed = len
        .div_ceil(MAX_MULTIPART_PARTS)
        .max(MULTIPART_PART_SIZE as u64);
    let rounded = needed.div_ceil(mib) * mib;
    anyhow::ensure!(
        rounded <= MAX_MULTIPART_PART_SIZE,
        "S3 multipart part exceeds {MAX_MULTIPART_PART_SIZE} bytes"
    );
    Ok(usize::try_from(rounded)?)
}

fn multipart_concurrency(part_size: usize) -> usize {
    (MULTIPART_BUFFER_BUDGET / part_size).clamp(1, MULTIPART_CONCURRENCY)
}

/// `SigV4` canonical query string: sorted keys, both halves percent-encoded.
fn canonical_query(params: &mut Vec<(&str, String)>) -> String {
    params.sort_by(|a, b| a.0.cmp(b.0));
    params
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                encode_query_component(k),
                encode_query_component(v)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn read_xml_text(body: &[u8], element: &[u8]) -> Result<Option<String>> {
    let mut reader = quick_xml::Reader::from_reader(body);
    loop {
        match reader.read_event()? {
            quick_xml::events::Event::Start(start) if start.local_name().as_ref() == element => {
                let text = reader.read_text(start.name())?;
                let decoded = text.xml10_content()?;
                return Ok(Some(quick_xml::escape::unescape(&decoded)?.into_owned()));
            }
            quick_xml::events::Event::Eof => return Ok(None),
            _ => {}
        }
    }
}

fn validate_complete_multipart(body: &[u8]) -> Result<()> {
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut success = false;
    let mut depth = 0usize;
    let mut closed = false;
    loop {
        match reader.read_event()? {
            quick_xml::events::Event::Start(start) if !success => {
                let root = start.local_name();
                if root.as_ref() == b"CompleteMultipartUploadResult" {
                    success = true;
                    depth = 1;
                    continue;
                }
                if root.as_ref() == b"Error" {
                    let code = read_xml_text(body, b"Code")?
                        .context("multipart error response missing Code")?;
                    anyhow::bail!("S3 multipart error {code}");
                }
                anyhow::bail!(
                    "unexpected multipart response root {}",
                    String::from_utf8_lossy(root.as_ref())
                );
            }
            quick_xml::events::Event::Start(_) => {
                anyhow::ensure!(!closed, "multipart response has multiple roots");
                depth = depth
                    .checked_add(1)
                    .context("multipart XML depth overflows")?;
            }
            quick_xml::events::Event::End(_) if success => {
                depth = depth.checked_sub(1).context("multipart XML closes early")?;
                closed = depth == 0;
            }
            quick_xml::events::Event::Text(text) if !success || closed => {
                anyhow::ensure!(
                    text.xml10_content()?.trim().is_empty(),
                    "multipart response is not XML"
                );
            }
            quick_xml::events::Event::Eof => {
                anyhow::ensure!(success && closed, "incomplete multipart response");
                return Ok(());
            }
            _ => {}
        }
    }
}

fn build_http(pool_max_idle: usize) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(pool_max_idle)
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(30))
        // S3 never negotiates HTTP/2; forcing h1 also keeps a custom
        // endpoint's proxy from multiplexing 750 streams onto one socket.
        .http1_only()
        .connect_timeout(Duration::from_secs(3))
        .read_timeout(Duration::from_secs(20))
        .build()
}

/// Client for large uploads: the server legitimately sends nothing while a
/// multi-MB body is in flight, so a read timeout would kill every slow-uplink
/// part. Robustness comes from `upload_deadline` per request instead.
fn build_upload_http() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(MULTIPART_CONCURRENCY)
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(30))
        .http1_only()
        .connect_timeout(Duration::from_secs(3))
        .build()
}

/// Total deadline for one upload request: a 256 KiB/s floor plus headroom.
fn upload_deadline(body_len: usize) -> Duration {
    Duration::from_secs(60 + (body_len / (256 * 1024)) as u64)
}

/// Timestamp helper: returns (`amz_date`, `date`).
fn now() -> (String, String) {
    let dt = time::OffsetDateTime::now_utc();
    let amz = dt
        .format(FORMAT.as_slice())
        .expect("invalid amz date format");
    let date = amz[..8].to_string();
    (amz, date)
}

fn apply_signed(
    req: reqwest::RequestBuilder,
    signed: &SignedHeaders,
    host: &str,
    session_token: Option<&str>,
) -> reqwest::RequestBuilder {
    let req = req
        .header("host", host)
        .header("x-amz-date", &signed.x_amz_date)
        .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
        .header("authorization", &signed.authorization);
    match session_token {
        Some(token) => req.header("x-amz-security-token", token),
        None => req,
    }
}

struct S3Request<'a> {
    method: &'static str,
    bucket: &'a str,
    key: Option<&'a str>,
    canonical_query: &'a str,
    range: Option<(u64, u64)>,
    body: Option<Bytes>,
    /// Conditional write: `Some(Some(etag))` = If-Match, `Some(None)` =
    /// If-None-Match: * (must not exist), `None` = unconditional.
    precondition: Option<Option<&'a str>>,
}

/// Typed marker for a failed conditional write (HTTP 412/409).
#[derive(Debug)]
pub(crate) struct PreconditionFailed;

impl std::fmt::Display for PreconditionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("conditional write precondition failed")
    }
}

impl std::error::Error for PreconditionFailed {}

enum Outcome {
    Success(StatusCode, Option<String>, DocumentBody),
    Throttle,
    NotFound,
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

type RefreshFn =
    dyn Fn() -> Result<(Credentials, Option<time::OffsetDateTime>)> + Send + Sync + 'static;

/// Refresh when credentials expire within this margin.
const REFRESH_MARGIN: time::Duration = time::Duration::minutes(5);

struct CredentialCell {
    state: std::sync::Mutex<(Arc<Credentials>, Option<time::OffsetDateTime>)>,
    refresher: std::sync::OnceLock<Arc<RefreshFn>>,
    refresh_gate: tokio::sync::Mutex<()>,
}

impl CredentialCell {
    fn snapshot(&self) -> Result<(Arc<Credentials>, Option<time::OffsetDateTime>)> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("credential refresh panicked"))?;
        Ok(state.clone())
    }

    fn expiring(expires_at: Option<time::OffsetDateTime>) -> bool {
        expires_at.is_some_and(|at| at - time::OffsetDateTime::now_utc() < REFRESH_MARGIN)
    }

    /// The credentials to sign with, refreshed via the registered resolver
    /// when close to expiry. The portal call inside the resolver builds its
    /// own runtime, so it runs on a blocking thread.
    async fn current(&self) -> Result<Arc<Credentials>> {
        let (creds, expires_at) = self.snapshot()?;
        if !Self::expiring(expires_at) {
            return Ok(creds);
        }
        let Some(refresher) = self.refresher.get() else {
            return Ok(creds);
        };
        let _gate = self.refresh_gate.lock().await;
        let (creds, expires_at) = self.snapshot()?;
        if !Self::expiring(expires_at) {
            return Ok(creds);
        }
        let refresher = Arc::clone(refresher);
        let (fresh, fresh_expiry) = tokio::task::spawn_blocking(move || refresher())
            .await
            .map_err(|err| anyhow::anyhow!("credential refresh panicked: {err}"))?
            .context("credential refresh failed")?;
        let fresh = Arc::new(fresh);
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("credential refresh panicked"))?;
        *state = (Arc::clone(&fresh), fresh_expiry);
        Ok(fresh)
    }
}

struct ClientInner {
    region: String,
    creds: CredentialCell,
    endpoint_host: Option<String>,
    endpoint_base: Option<String>,
    cfg: FetchConfig,
    http: reqwest::Client,
    retry_http: reqwest::Client,
    upload_http: reqwest::Client,
    limiter: AimdLimiter,
    hedges: HedgeBudget,
    rt: tokio::runtime::Runtime,
}

/// Signed S3 client with one owned runtime, two shared connection pools, and
/// uniform retry + AIMD + hedging across every operation.
///
/// All methods are synchronous and must not be called from inside an async
/// runtime (they `block_on` internally). Cloning is cheap (shared `Arc`).
#[derive(Clone)]
pub struct S3Client(Arc<ClientInner>);

impl S3Client {
    pub fn new(
        region: String,
        creds: Credentials,
        endpoint: Option<String>,
        cfg: FetchConfig,
    ) -> Result<S3Client> {
        anyhow::ensure!(
            cfg.start > 0,
            "initial S3 concurrency must be greater than 0"
        );
        anyhow::ensure!(cfg.cap > 0, "maximum S3 concurrency must be greater than 0");
        anyhow::ensure!(
            cfg.start <= cfg.cap,
            "initial S3 concurrency exceeds its maximum"
        );
        anyhow::ensure!(
            cfg.cap <= tokio::sync::Semaphore::MAX_PERMITS,
            "S3 concurrency exceeds Tokio's semaphore limit"
        );
        anyhow::ensure!(
            cfg.max_inflight_bytes > 0,
            "in-flight S3 byte cap must be greater than 0"
        );
        anyhow::ensure!(
            cfg.max_inflight_bytes.div_ceil(BYTE_PERMIT_SIZE)
                <= u64::try_from(tokio::sync::Semaphore::MAX_PERMITS)?,
            "in-flight S3 byte cap exceeds Tokio's semaphore limit"
        );
        let (endpoint_host, endpoint_base) = match &endpoint {
            Some(endpoint) => {
                let url = reqwest::Url::parse(endpoint)?;
                anyhow::ensure!(
                    matches!(url.scheme(), "http" | "https")
                        && url.path() == "/"
                        && url.query().is_none()
                        && url.fragment().is_none()
                        && url.username().is_empty()
                        && url.password().is_none(),
                    "S3 endpoint must be an HTTP(S) origin URL without path, query, fragment, or credentials: {endpoint}"
                );
                let host = url
                    .host_str()
                    .ok_or_else(|| anyhow::anyhow!("S3 endpoint missing host: {endpoint}"))?;
                let host = if host.starts_with('[') && host.ends_with(']') {
                    host.to_owned()
                } else if host.contains(':') {
                    format!("[{host}]")
                } else {
                    host.to_owned()
                };
                let host = match url.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host,
                };
                (Some(host), Some(endpoint.trim_end_matches('/').to_owned()))
            }
            None => (None, None),
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        Ok(S3Client(Arc::new(ClientInner {
            http: build_http(cfg.cap)?,
            retry_http: build_http(0)?,
            upload_http: build_upload_http()?,
            limiter: AimdLimiter::new(cfg.start, cfg.cap),
            hedges: HedgeBudget::new(cfg.hedge_tokens),
            region,
            creds: CredentialCell {
                state: std::sync::Mutex::new((Arc::new(creds), None)),
                refresher: std::sync::OnceLock::new(),
                refresh_gate: tokio::sync::Mutex::new(()),
            },
            endpoint_host,
            endpoint_base,
            cfg,
            rt,
        })))
    }

    /// Arm credential refresh: `expires_at` marks the current credentials'
    /// lifetime, and `refresh` re-resolves them (called off-runtime when they
    /// near expiry). Static-credential clients never call this.
    pub fn enable_refresh(
        &self,
        expires_at: time::OffsetDateTime,
        refresh: impl Fn() -> Result<(Credentials, Option<time::OffsetDateTime>)>
            + Send
            + Sync
            + 'static,
    ) {
        if let Ok(mut state) = self.0.creds.state.lock() {
            state.1 = Some(expires_at);
        }
        self.0.creds.refresher.get_or_init(|| Arc::new(refresh));
    }

    fn host(&self, bucket: &str) -> String {
        match &self.0.endpoint_host {
            Some(host) => host.clone(),
            None if bucket.contains('.') => format!("s3.{}.amazonaws.com", self.0.region),
            None => format!("{bucket}.s3.{}.amazonaws.com", self.0.region),
        }
    }

    pub(crate) fn endpoint_identity(&self) -> String {
        self.0
            .endpoint_base
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.0.region))
    }

    pub(crate) fn max_concurrency(&self) -> usize {
        self.0.cfg.cap
    }

    fn request_path(&self, bucket: &str, key: Option<&str>) -> String {
        let path_style = self.0.endpoint_base.is_some() || bucket.contains('.');
        let raw = match (path_style, key) {
            (true, Some(key)) => format!("/{bucket}/{key}"),
            (true, None) => format!("/{bucket}/"),
            (false, Some(key)) => format!("/{key}"),
            (false, None) => "/".to_owned(),
        };
        encode_path(&raw)
    }

    fn request_url(&self, host: &str, encoded_path: &str, canonical_query: &str) -> String {
        let base = match &self.0.endpoint_base {
            Some(base) => base.clone(),
            None => format!("https://{host}"),
        };
        if canonical_query.is_empty() {
            format!("{base}{encoded_path}")
        } else {
            format!("{base}{encoded_path}?{canonical_query}")
        }
    }

    /// One signed attempt: build, sign, send, and read the full body.
    async fn attempt(&self, req: &S3Request<'_>, http: &reqwest::Client) -> Outcome {
        let creds = match self.0.creds.current().await {
            Ok(creds) => creds,
            Err(err) => return Outcome::Fatal(err),
        };
        let host = self.host(req.bucket);
        let path = self.request_path(req.bucket, req.key);
        let (amz, date) = now();
        let range_header = req.range.map(|(a, b)| format!("bytes={a}-{b}"));
        let mut extra = Vec::with_capacity(2);
        if let Some(range) = &range_header {
            extra.push(("range", range.as_str()));
        }
        match req.precondition {
            Some(Some(etag)) => extra.push(("if-match", etag)),
            Some(None) => extra.push(("if-none-match", "*")),
            None => {}
        }
        let signed = sign_request(
            req.method,
            &creds,
            &self.0.region,
            &host,
            &path,
            req.canonical_query,
            &extra,
            &amz,
            &date,
            "UNSIGNED-PAYLOAD",
        );
        let url = self.request_url(&host, &path, req.canonical_query);
        let http = if req
            .body
            .as_ref()
            .is_some_and(|body| body.len() >= UPLOAD_BODY_THRESHOLD)
        {
            &self.0.upload_http
        } else {
            http
        };
        let builder = match req.method {
            "PUT" => http.put(url),
            "POST" => http.post(url),
            "DELETE" => http.delete(url),
            _ => http.get(url),
        };
        let mut builder = apply_signed(builder, &signed, &host, creds.session_token.as_deref());
        if let Some(range) = &range_header {
            builder = builder.header("range", range);
        }
        if let Some(body) = &req.body {
            builder = builder.header("content-length", body.len());
            builder = builder.body(body.clone());
            if body.len() >= UPLOAD_BODY_THRESHOLD {
                builder = builder.timeout(upload_deadline(body.len()));
            }
        }
        match req.precondition {
            Some(Some(etag)) => builder = builder.header("if-match", etag),
            Some(None) => builder = builder.header("if-none-match", "*"),
            None => {}
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(err) if err.is_builder() => return Outcome::Fatal(err.into()),
            Err(err) => return Outcome::Transient(err.into()),
        };
        let status = response.status();
        if status.is_success() {
            if req.range.is_some() && status != StatusCode::PARTIAL_CONTENT {
                return Outcome::Fatal(anyhow::anyhow!(
                    "range GET s3://{}/{} returned HTTP {status} instead of 206 (endpoint ignores Range?)",
                    req.bucket,
                    req.key.unwrap_or_default()
                ));
            }
            let etag = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            let stream_len = req
                .range
                .and_then(|(start, end)| end.checked_sub(start))
                .and_then(|len| len.checked_add(1))
                .filter(|len| *len >= SMALL_READ_MAX);
            let body = match stream_len {
                Some(len) => {
                    let mut spool = match DocumentSpool::new(len) {
                        Ok(spool) => spool,
                        Err(error) => return Outcome::Fatal(error),
                    };
                    let mut at = 0u64;
                    let mut chunks = response.bytes_stream();
                    while let Some(chunk) = chunks.next().await {
                        let chunk = match chunk {
                            Ok(chunk) => chunk,
                            Err(error) => return Outcome::Transient(error.into()),
                        };
                        let end = match at.checked_add(chunk.len() as u64) {
                            Some(end) if end <= len => end,
                            _ => {
                                return Outcome::Fatal(anyhow::anyhow!(
                                    "streamed S3 response exceeds its range"
                                ));
                            }
                        };
                        if let Err(error) = spool.write_at(at, &chunk) {
                            return Outcome::Fatal(error);
                        }
                        at = end;
                    }
                    if at != len {
                        return Outcome::Transient(anyhow::anyhow!(
                            "streamed S3 response is {at} bytes, expected {len}"
                        ));
                    }
                    match spool.finish() {
                        Ok(body) => body,
                        Err(error) => return Outcome::Fatal(error),
                    }
                }
                None => match response.bytes().await {
                    Ok(bytes) => DocumentBody::from_bytes(bytes),
                    Err(error) => return Outcome::Transient(error.into()),
                },
            };
            return Outcome::Success(status, etag, body);
        }
        match status {
            StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS => Outcome::Throttle,
            StatusCode::NOT_FOUND => Outcome::NotFound,
            // 412 = precondition failed; 409 = S3's concurrent-conditional
            // conflict. Both mean: another writer won, do not retry blindly.
            StatusCode::PRECONDITION_FAILED if req.method == "GET" => {
                match (
                    req.key,
                    req.precondition.and_then(|precondition| precondition),
                ) {
                    (Some(key), Some(expected)) => {
                        Outcome::Fatal(anyhow::Error::new(StaleSource {
                            key: key.to_owned(),
                            expected: expected.to_owned(),
                        }))
                    }
                    _ => Outcome::Fatal(anyhow::anyhow!("HTTP {status} for {host}{path}")),
                }
            }
            StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT => {
                Outcome::Fatal(anyhow::Error::new(PreconditionFailed))
            }
            StatusCode::REQUEST_TIMEOUT => {
                Outcome::Transient(anyhow::anyhow!("HTTP {status} for {host}{path}"))
            }
            status if status.is_server_error() => {
                Outcome::Transient(anyhow::anyhow!("HTTP {status} for {host}{path}"))
            }
            status => Outcome::Fatal(anyhow::anyhow!("HTTP {status} for {host}{path}")),
        }
    }

    /// Retry loop shared by every operation: limiter permit per attempt,
    /// re-signed requests, jittered exponential backoff, AIMD shrink on
    /// throttle. Returns `None` on HTTP 404.
    async fn send_resilient(
        &self,
        req: &S3Request<'_>,
        on_permit: Option<&Notify>,
    ) -> Result<Option<(StatusCode, Option<String>, DocumentBody)>> {
        let label = || {
            format!(
                "{} s3://{}/{}",
                req.method,
                req.bucket,
                req.key.unwrap_or_default()
            )
        };
        let mut attempt = 0u32;
        loop {
            let http = if attempt == 0 {
                &self.0.http
            } else {
                &self.0.retry_http
            };
            let permit = self.0.limiter.acquire().await?;
            if let Some(notify) = on_permit {
                notify.notify_one();
            }
            let outcome = self.attempt(req, http).await;
            drop(permit);
            let error = match outcome {
                Outcome::Success(status, etag, bytes) => {
                    self.0.limiter.on_success();
                    return Ok(Some((status, etag, bytes)));
                }
                Outcome::NotFound => return Ok(None),
                Outcome::Fatal(err) => return Err(err.context(label())),
                Outcome::Throttle => {
                    self.0.limiter.on_throttle();
                    anyhow::anyhow!("throttled (HTTP 503/429)")
                }
                Outcome::Transient(err) => err,
            };
            if attempt >= self.0.cfg.max_retries {
                return Err(error.context(format!(
                    "{} failed after {} retries",
                    label(),
                    self.0.cfg.max_retries
                )));
            }
            let exponential = self
                .0
                .cfg
                .backoff_base_ms
                .saturating_mul(2_u64.saturating_pow(attempt));
            let delay = rand::random_range(0..=exponential.min(self.0.cfg.backoff_cap_ms));
            tokio::time::sleep(Duration::from_millis(delay)).await;
            attempt += 1;
        }
    }

    /// Conditional PUT (compare-and-swap): `Some(etag)` = overwrite only if
    /// unchanged, `None` = create only if absent. Returns false when another
    /// writer won the race.
    pub fn put_if(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        expected: Option<&str>,
    ) -> Result<bool> {
        let req = S3Request {
            method: "PUT",
            bucket,
            key: Some(key),
            canonical_query: "",
            range: None,
            body: Some(Bytes::copy_from_slice(body)),
            precondition: Some(expected),
        };
        match self.0.rt.block_on(self.send_resilient(&req, None)) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => anyhow::bail!("conditional PUT s3://{bucket}/{key}: bucket not found"),
            Err(err) if err.root_cause().is::<PreconditionFailed>() => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Delete one object. Deleting an absent object is not an error (S3
    /// DELETE returns 204 either way; a 404 here means the bucket).
    pub fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let req = S3Request {
            method: "DELETE",
            bucket,
            key: Some(key),
            canonical_query: "",
            range: None,
            body: None,
            precondition: None,
        };
        self.0
            .rt
            .block_on(self.send_resilient(&req, None))?
            .with_context(|| format!("DELETE s3://{bucket}/{key}: bucket not found"))?;
        Ok(())
    }

    /// Upload many objects concurrently; first failure aborts the rest.
    pub fn put_many(&self, bucket: &str, objects: Vec<(String, Vec<u8>)>) -> Result<()> {
        self.0.rt.block_on(async {
            let mut puts = stream::iter(objects.iter().map(|(key, body)| {
                let client = self;
                async move { client.put_async(bucket, key, body).await }
            }))
            .buffer_unordered(self.0.cfg.cap);
            while let Some(result) = puts.next().await {
                result?;
            }
            Ok(())
        })
    }

    /// List all objects under `prefix` (paginated, retried).
    pub fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.0.rt.block_on(async {
            let mut all = Vec::new();
            let mut token: Option<String> = None;
            let mut tokens = std::collections::HashSet::new();
            loop {
                let mut params = vec![
                    ("encoding-type", "url".to_owned()),
                    ("list-type", "2".to_owned()),
                    ("prefix", prefix.to_owned()),
                ];
                if let Some(t) = &token {
                    params.push(("continuation-token", t.clone()));
                }
                let canonical_query = canonical_query(&mut params);
                let req = S3Request {
                    method: "GET",
                    bucket,
                    key: None,
                    canonical_query: &canonical_query,
                    range: None,
                    body: None,
                    precondition: None,
                };
                let (_, _, body) = self
                    .send_resilient(&req, None)
                    .await?
                    .with_context(|| format!("list s3://{bucket}: bucket not found"))?;
                let body = String::from_utf8(body.into_bytes()?.to_vec())
                    .context("ListObjectsV2 response not UTF-8")?;
                let (objects, next) = parse_list_v2(&body)?;
                all.extend(objects);
                match next {
                    Some(t) => {
                        anyhow::ensure!(
                            tokens.insert(t.clone()),
                            "ListObjectsV2 repeated continuation token"
                        );
                        token = Some(t);
                    }
                    None => break,
                }
            }
            Ok(all)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        coalesce_ranges, multipart_concurrency, multipart_part_size, read_xml_text,
        validate_complete_multipart, FetchConfig, S3Client, MAX_MULTIPART_OBJECT_SIZE,
        MAX_MULTIPART_PARTS, MAX_MULTIPART_PART_SIZE, MULTIPART_PART_SIZE, SMALL_READ_MAX,
    };
    use holys3_sigv4::Credentials;
    use std::io::{Read, Seek, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn start_status_server(status: &str) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        (format!("http://{address}"), thread)
    }

    fn start_response_server(
        status: &str,
        body: &[u8],
    ) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let body = body.to_vec();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 8192];
            let read = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
            String::from_utf8_lossy(&request[..read]).into_owned()
        });
        (format!("http://{address}"), thread)
    }

    #[test]
    fn coalesce_merges_within_gap_and_places_blocks() {
        let ranges = [(0u64, 100u64), (150, 50), (10_000, 8), (10_008, 4)];
        let coalesced = coalesce_ranges(&ranges, 64).unwrap();
        let merged = coalesced.ranges;
        let placements = coalesced.placements;
        assert_eq!(merged, vec![(0, 200), (10_000, 12)]);
        assert_eq!(placements, vec![(0, 0), (0, 150), (1, 0), (1, 8)]);

        let coalesced = coalesce_ranges(&ranges, 0).unwrap();
        let merged = coalesced.ranges;
        let placements = coalesced.placements;
        assert_eq!(merged.len(), 3);
        assert_eq!(placements[3], (2, 8));

        assert!(coalesce_ranges(&[], 64).unwrap().ranges.is_empty());
        assert!(coalesce_ranges(&[(u64::MAX, 1)], 0).is_err());
    }

    #[test]
    fn multipart_xml_requires_success_result() {
        let success = br#"<?xml version="1.0"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Key>a</Key></CompleteMultipartUploadResult>"#;
        let error = br#"<?xml version="1.0"?><Error><Code>InternalError</Code></Error>"#;
        assert!(validate_complete_multipart(success).is_ok());
        assert!(validate_complete_multipart(error).is_err());
        assert!(validate_complete_multipart(b"<CompleteMultipartUploadResult>").is_err());
        assert!(validate_complete_multipart(b"").is_err());
    }

    #[test]
    fn multipart_shape_covers_current_s3_limits() {
        assert_eq!(
            multipart_part_size(MULTIPART_PART_SIZE as u64 + 1).unwrap(),
            MULTIPART_PART_SIZE
        );
        let large = 200 * 1024 * 1024 * 1024u64;
        let part_size = multipart_part_size(large).unwrap();
        assert!(large.div_ceil(part_size as u64) <= MAX_MULTIPART_PARTS);
        assert_eq!(
            multipart_part_size(MAX_MULTIPART_OBJECT_SIZE).unwrap() as u64,
            MAX_MULTIPART_PART_SIZE
        );
        assert!(multipart_part_size(MAX_MULTIPART_OBJECT_SIZE + 1).is_err());
        assert_eq!(multipart_concurrency(MULTIPART_PART_SIZE), 4);
        assert_eq!(multipart_concurrency(128 * 1024 * 1024), 2);
        assert_eq!(
            multipart_concurrency(usize::try_from(MAX_MULTIPART_PART_SIZE).unwrap()),
            1
        );
    }

    #[test]
    fn reads_xml_text_by_element_name() {
        let xml = br#"<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><UploadId>a&amp;b</UploadId></InitiateMultipartUploadResult>"#;
        assert_eq!(
            read_xml_text(xml, b"UploadId").unwrap().as_deref(),
            Some("a&b")
        );
    }

    #[test]
    fn custom_endpoint_rejects_unsigned_url_components() {
        let credentials = Credentials {
            access_key: "test".into(),
            secret_key: "test".into(),
            session_token: None,
        };
        for endpoint in [
            "http://localhost:9000/base",
            "http://localhost:9000/?x=1",
            "http://user@localhost:9000/",
            "ftp://localhost:9000/",
        ] {
            let err = S3Client::new(
                "us-east-1".into(),
                credentials.clone(),
                Some(endpoint.into()),
                FetchConfig::default(),
            )
            .err()
            .expect("endpoint should fail");
            assert!(err.to_string().contains("origin URL"), "{err:#}");
        }

        let client = S3Client::new(
            "us-east-1".into(),
            credentials,
            Some("http://[::1]:9000".into()),
            FetchConfig::default(),
        )
        .unwrap();
        assert_eq!(client.0.endpoint_host.as_deref(), Some("[::1]:9000"));
    }

    #[test]
    fn aws_dotted_buckets_use_path_style() {
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            None,
            FetchConfig::default(),
        )
        .unwrap();
        assert_eq!(
            client.host("logs.example.com"),
            "s3.us-east-1.amazonaws.com"
        );
        assert_eq!(
            client.request_path("logs.example.com", Some("logs/a b")),
            "/logs.example.com/logs/a%20b"
        );
        assert_eq!(
            client.host("logs-example-com"),
            "logs-example-com.s3.us-east-1.amazonaws.com"
        );
        assert_eq!(
            client.request_path("logs-example-com", Some("logs/a b")),
            "/logs/a%20b"
        );
    }

    #[test]
    fn signed_requests_do_not_follow_redirects() {
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        target.set_nonblocking(true).unwrap();
        let target_address = target.local_addr().unwrap();
        let target_server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_millis(500);
            loop {
                match target.accept() {
                    Ok((mut stream, _)) => {
                        let mut request = [0u8; 4096];
                        let _ = stream.read(&mut request).unwrap();
                        write!(
                            stream,
                            "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody"
                        )
                        .unwrap();
                        return true;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if std::time::Instant::now() >= deadline {
                            return false;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("redirect target failed: {error}"),
                }
            }
        });
        let redirect = TcpListener::bind("127.0.0.1:0").unwrap();
        let redirect_address = redirect.local_addr().unwrap();
        let redirect_server = std::thread::spawn(move || {
            let (mut stream, _) = redirect.accept().unwrap();
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{target_address}/stolen\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(format!("http://{redirect_address}")),
            FetchConfig::default(),
        )
        .unwrap();
        let result = client.get("bucket", "key");
        redirect_server.join().unwrap();
        let target_was_hit = target_server.join().unwrap();
        assert!(result.is_err());
        assert!(!target_was_hit);
    }

    #[test]
    fn rejects_concurrency_above_tokio_semaphore_limit() {
        let mut config = FetchConfig::default();
        config.start = tokio::sync::Semaphore::MAX_PERMITS + 1;
        config.cap = config.start;
        let credentials = Credentials {
            access_key: "access".into(),
            secret_key: "secret".into(),
            session_token: None,
        };
        assert!(S3Client::new("us-east-1".into(), credentials, None, config).is_err());
    }

    #[test]
    fn conditional_get_signs_header_and_types_stale_source() {
        let credentials = Credentials {
            access_key: "test".into(),
            secret_key: "test".into(),
            session_token: None,
        };
        let (endpoint, server) = start_response_server("200 OK", b"body");
        let client = S3Client::new(
            "us-east-1".into(),
            credentials.clone(),
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        assert_eq!(
            client
                .get_if_match("bucket", "key", "\"abc\"")
                .unwrap()
                .unwrap(),
            bytes::Bytes::from_static(b"body")
        );
        let request = server.join().unwrap().to_ascii_lowercase();
        assert!(request.contains("if-match: \"abc\"\r\n"), "{request}");
        assert!(
            request.contains("signedheaders=host;if-match;x-amz-content-sha256;x-amz-date"),
            "{request}"
        );

        let (endpoint, server) = start_response_server("412 Precondition Failed", b"");
        let client = S3Client::new(
            "us-east-1".into(),
            credentials,
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let error = client.get_if_match("bucket", "key", "\"old\"").unwrap_err();
        assert!(error.is::<holys3_core::StaleSource>(), "{error:#}");
        let stale = error.downcast_ref::<holys3_core::StaleSource>().unwrap();
        assert_eq!(stale.key, "key");
        assert_eq!(stale.expected, "\"old\"");
        server.join().unwrap();
    }

    #[test]
    fn list_requests_url_encoding_and_decodes_keys() {
        let body = br#"<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>logs%2Fa%2Bb%25.log</Key><Size>4</Size><ETag>&quot;etag&quot;</ETag></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let (endpoint, server) = start_response_server("200 OK", body);
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        assert_eq!(
            client.list("bucket", "logs/").unwrap(),
            vec![super::ObjectMeta {
                key: "logs/a+b%.log".into(),
                etag: "\"etag\"".into(),
                size: 4,
            }]
        );
        let request = server.join().unwrap();
        assert!(
            request.starts_with(
                "GET /bucket/?encoding-type=url&list-type=2&prefix=logs%2F HTTP/1.1\r\n"
            ),
            "{request}"
        );
    }

    #[test]
    fn oversized_source_holds_byte_budget_through_consumer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(AtomicUsize::new(0));
        let accepted_server = Arc::clone(&accepted);
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                accepted_server.fetch_add(1, Ordering::SeqCst);
                let mut request = [0u8; 8192];
                let read = stream.read(&mut request).unwrap();
                assert!(read > 0);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    2 * 1024 * 1024
                )
                .unwrap();
                stream.write_all(&vec![b'x'; 2 * 1024 * 1024]).unwrap();
            }
        });
        let config = FetchConfig {
            max_inflight_bytes: 1024 * 1024,
            ..FetchConfig::default()
        };
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            config,
        )
        .unwrap();
        let mut consumed = 0;
        client
            .get_each(
                "bucket",
                vec![
                    (0, "a".into(), 2 * 1024 * 1024),
                    (1, "b".into(), 2 * 1024 * 1024),
                ],
                &mut |_, bytes| {
                    consumed += 1;
                    if consumed == 1 {
                        std::thread::sleep(Duration::from_millis(50));
                        assert_eq!(accepted.load(Ordering::SeqCst), 1);
                    }
                    assert_eq!(bytes.unwrap().len(), 2 * 1024 * 1024);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(consumed, 2);
        server.join().unwrap();
    }

    #[test]
    fn fetches_large_sources_as_conditional_ranges() {
        let body = b"abcdefghijklmnopqrstuvwxyz0123456789".to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let expected_requests = body.len().div_ceil(8);
        let server_body = body.clone();
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..expected_requests {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 8192];
                let read = stream.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(&request[..read]).into_owned();
                let range = request
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("range: bytes=")
                            .map(str::to_owned)
                    })
                    .unwrap();
                let (start, end) = range.split_once('-').unwrap();
                let start = start.parse::<usize>().unwrap();
                let end = end.parse::<usize>().unwrap();
                let bytes = &server_body[start..=end];
                write!(
                    stream,
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    bytes.len()
                )
                .unwrap();
                stream.write_all(bytes).unwrap();
                requests.push(request);
            }
            requests
        });
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let fetched = client
            .0
            .rt
            .block_on(client.fetch_source_ranges(
                "bucket",
                "key",
                Some("\"etag\""),
                body.len() as u64,
                8,
            ))
            .unwrap()
            .unwrap();
        assert!(fetched.is_file());
        assert_eq!(fetched.into_bytes().unwrap(), body);
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), expected_requests);
        assert!(requests.iter().all(|request| {
            let request = request.to_ascii_lowercase();
            request.contains("if-match: \"etag\"\r\n")
                && request
                    .contains("signedheaders=host;if-match;range;x-amz-content-sha256;x-amz-date")
        }));
    }

    #[test]
    fn streams_large_range_response_to_file() {
        let body = vec![b'x'; usize::try_from(SMALL_READ_MAX).unwrap() + 1];
        let (endpoint, server) = start_response_server("206 Partial Content", &body);
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let fetched = client
            .0
            .rt
            .block_on(client.fetch_hedged(
                "bucket",
                "key",
                Some((0, u64::try_from(body.len()).unwrap() - 1)),
                None,
            ))
            .unwrap()
            .unwrap();
        assert!(fetched.is_file());
        assert_eq!(fetched.into_bytes().unwrap(), body);
        server.join().unwrap();
    }

    #[test]
    fn writes_large_object_to_file_with_ranges() {
        let body = vec![b'x'; usize::try_from(SMALL_READ_MAX).unwrap() + 1];
        let (endpoint, server) = start_response_server("206 Partial Content", &body);
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let mut output = tempfile::tempfile().unwrap();
        assert!(client
            .get_file("bucket", "key", &mut output, body.len() as u64)
            .unwrap());
        output.rewind().unwrap();
        let mut fetched = Vec::new();
        output.read_to_end(&mut fetched).unwrap();
        assert_eq!(fetched, body);
        let request = server.join().unwrap().to_ascii_lowercase();
        let range = format!("range: bytes=0-{}\r\n", body.len() - 1);
        assert!(request.contains(&range), "{request}");
        assert!(!request.contains("if-match:"), "{request}");
    }

    #[test]
    fn writes_empty_object_to_file() {
        let (endpoint, server) = start_response_server("200 OK", b"");
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let mut output = tempfile::tempfile().unwrap();
        output.write_all(b"stale").unwrap();
        assert!(client.get_file("bucket", "key", &mut output, 0).unwrap());
        assert_eq!(output.metadata().unwrap().len(), 0);
        let request = server.join().unwrap().to_ascii_lowercase();
        assert!(!request.contains("range:"), "{request}");
    }

    #[test]
    fn rejects_range_ignoring_endpoint() {
        let (endpoint, server) = start_response_server("200 OK", b"body");
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let error = client.get_range("bucket", "key", 0, 4).unwrap_err();
        assert!(format!("{error:#}").contains("instead of 206"), "{error:#}");
        server.join().unwrap();
    }

    #[test]
    fn missing_bucket_is_not_a_successful_write_or_delete() {
        let credentials = Credentials {
            access_key: "test".into(),
            secret_key: "test".into(),
            session_token: None,
        };
        let (endpoint, server) = start_status_server("404 Not Found");
        let client = S3Client::new(
            "us-east-1".into(),
            credentials.clone(),
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        assert!(client.put_if("missing", "key", b"body", None).is_err());
        server.join().unwrap();

        let (endpoint, server) = start_status_server("404 Not Found");
        let client = S3Client::new(
            "us-east-1".into(),
            credentials,
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        assert!(client.delete("missing", "key").is_err());
        server.join().unwrap();
    }
}
