use crate::fetch::{AimdLimiter, FetchConfig, HedgeBudget};
use crate::{parse_list_v2, ObjectMeta};
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt};
use holys3_core::DocId;
use holys3_sigv4::{encode_path, encode_query_component, sign_request, Credentials, SignedHeaders};
use reqwest::StatusCode;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Notify;

static FORMAT: LazyLock<Vec<time::format_description::BorrowedFormatItem<'static>>> =
    LazyLock::new(|| {
        time::format_description::parse("[year][month][day]T[hour][minute][second]Z")
            .expect("invalid amz date format")
    });

/// Hedge window for small ranged reads (index blocks): ~2x a slow S3
/// first-byte latency. Reads below `SMALL_READ_MAX` qualify.
const SMALL_READ_HEDGE: Duration = Duration::from_millis(300);
const SMALL_READ_MAX: u64 = 4 * 1024 * 1024;

/// Ranges within this gap of each other merge into one ranged GET: a request
/// round trip costs more than transferring the gap bytes.
const RANGE_COALESCE_GAP: u64 = 512 * 1024;

/// Bodies above one part upload as concurrent multipart parts: single PUTs
/// of GB-scale blobs time out on slow uplinks and cap at 5 GiB anyway.
const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
/// Parts in flight at once. Uploads are uplink-bound, not request-bound:
/// more concurrency just slows every part toward its deadline.
const MULTIPART_CONCURRENCY: usize = 4;
/// Request bodies at or above this use the upload client + deadline.
const UPLOAD_BODY_THRESHOLD: usize = 1024 * 1024;

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

/// First text content of `<element>` in an S3 XML response body.
fn xml_text(body: &[u8], element: &str) -> Option<String> {
    let text = std::str::from_utf8(body).ok()?;
    let start = text.find(&format!("<{element}>"))? + element.len() + 2;
    let end = start + text[start..].find(&format!("</{element}>"))?;
    Some(text[start..end].to_owned())
}

/// Where an input range landed after coalescing: (merged index, byte start).
type Placement = (usize, usize);

/// Merge sorted (offset, len) ranges whose gap is at most `max_gap` and
/// return, per input range, which merged range holds it and at what offset.
fn coalesce_ranges(ranges: &[(u64, u64)], max_gap: u64) -> (Vec<(u64, u64)>, Vec<Placement>) {
    let mut merged: Vec<(u64, u64)> = Vec::new();
    let mut placements = Vec::with_capacity(ranges.len());
    for &(offset, len) in ranges {
        let next = merged.len();
        match merged.last_mut() {
            Some((m_off, m_len)) if offset <= *m_off + *m_len + max_gap => {
                placements.push((next - 1, (offset - *m_off) as usize));
                *m_len = (offset + len).max(*m_off + *m_len) - *m_off;
            }
            _ => {
                placements.push((next, 0));
                merged.push((offset, len));
            }
        }
    }
    (merged, placements)
}

fn build_http(pool_max_idle: usize) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
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
    body: Option<&'a [u8]>,
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
    Success(StatusCode, Option<String>, Vec<u8>),
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
        let (endpoint_host, endpoint_base) = match &endpoint {
            Some(endpoint) => {
                let url = reqwest::Url::parse(endpoint)?;
                let host = url
                    .host_str()
                    .ok_or_else(|| anyhow::anyhow!("S3 endpoint missing host: {endpoint}"))?;
                let host = match url.port() {
                    Some(port) => format!("{host}:{port}"),
                    None => host.to_owned(),
                };
                (Some(host), Some(endpoint.trim_end_matches('/').to_owned()))
            }
            None => (None, None),
        };
        let rt = tokio::runtime::Builder::new_multi_thread()
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

    pub fn region(&self) -> &str {
        &self.0.region
    }

    fn host(&self, bucket: &str) -> String {
        match &self.0.endpoint_host {
            Some(host) => host.clone(),
            None => format!("{bucket}.s3.{}.amazonaws.com", self.0.region),
        }
    }

    /// Path-style addressing is used exactly when a custom endpoint is set.
    fn request_path(&self, bucket: &str, key: Option<&str>) -> String {
        let raw = match (self.0.endpoint_base.is_some(), key) {
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
        let extra: Vec<(&str, &str)> = match &range_header {
            Some(r) => vec![("range", r.as_str())],
            None => vec![],
        };
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
        let http = if req.body.is_some_and(|b| b.len() >= UPLOAD_BODY_THRESHOLD) {
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
        if let Some(body) = req.body {
            builder = builder.body(body.to_vec());
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
            let etag = response
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            return match response.bytes().await {
                Ok(bytes) => Outcome::Success(status, etag, bytes.to_vec()),
                Err(err) => Outcome::Transient(err.into()),
            };
        }
        match status {
            StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS => Outcome::Throttle,
            StatusCode::NOT_FOUND => Outcome::NotFound,
            // 412 = precondition failed; 409 = S3's concurrent-conditional
            // conflict. Both mean: another writer won, do not retry blindly.
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
    ) -> Result<Option<(StatusCode, Option<String>, Vec<u8>)>> {
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

    /// Object GET with hedging: when the first attempt has held a permit for
    /// the hedge window without completing, race a budgeted duplicate
    /// request. Small index reads (posting blocks, metadata) hedge much
    /// earlier than whole-object fetches: their expected latency is one
    /// round trip, so 2x-median is a few hundred ms, not seconds.
    async fn fetch_hedged(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<Vec<u8>>> {
        let hedge_after = match range {
            Some((start, end)) if end.saturating_sub(start) < SMALL_READ_MAX => {
                SMALL_READ_HEDGE.min(self.0.cfg.hedge_after)
            }
            _ => self.0.cfg.hedge_after,
        };
        let req = S3Request {
            method: "GET",
            bucket,
            key: Some(key),
            canonical_query: "",
            range,
            body: None,
            precondition: None,
        };
        let started = Notify::new();
        let primary = self.send_resilient(&req, Some(&started));
        tokio::pin!(primary);
        let result = tokio::select! {
            biased;
            result = &mut primary => result,
            () = async { started.notified().await; tokio::time::sleep(hedge_after).await } => {
                match self.0.hedges.try_take() {
                    None => primary.await,
                    Some(token) => {
                        let hedge = async {
                            let _token = token;
                            self.send_resilient(&req, None).await
                        };
                        tokio::pin!(hedge);
                        tokio::select! {
                            biased;
                            result = &mut primary => result,
                            result = &mut hedge => result,
                        }
                    }
                }
            }
        };
        let Some((status, _, bytes)) = result? else {
            return Ok(None);
        };
        if let Some((start, end)) = range {
            anyhow::ensure!(
                status == StatusCode::PARTIAL_CONTENT,
                "range GET s3://{bucket}/{key} returned HTTP {status} instead of 206 (endpoint ignores Range?)"
            );
            let expected = end - start + 1;
            anyhow::ensure!(
                bytes.len() as u64 == expected,
                "range GET s3://{bucket}/{key} returned {} bytes, expected {expected}",
                bytes.len()
            );
        }
        Ok(Some(bytes))
    }

    /// Fetch one object in full. `None` = object does not exist.
    pub fn get(&self, bucket: &str, key: &str) -> Result<Option<Vec<u8>>> {
        self.0.rt.block_on(self.fetch_hedged(bucket, key, None))
    }

    /// Fetch `len` bytes at `start`. `None` = object does not exist.
    pub fn get_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        len: u64,
    ) -> Result<Option<Vec<u8>>> {
        let range = byte_range(start, len)?;
        self.0
            .rt
            .block_on(self.fetch_hedged(bucket, key, Some(range)))
    }

    /// Fetch many byte ranges of one object concurrently, preserving order.
    /// `None` = object does not exist. Nearby ranges merge into one GET
    /// (`RANGE_COALESCE_GAP`) before fetching; each requested range is then
    /// sliced back out of its merged blob.
    pub fn get_ranges(
        &self,
        bucket: &str,
        key: &str,
        ranges: &[(u64, u64)],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        let mut order: Vec<usize> = (0..ranges.len()).collect();
        order.sort_by_key(|&i| ranges[i]);
        let sorted: Vec<(u64, u64)> = order.iter().map(|&i| ranges[i]).collect();
        let (merged, placements) = coalesce_ranges(&sorted, RANGE_COALESCE_GAP);
        let merged = &merged;
        let blobs = self.0.rt.block_on(async {
            let mut blobs: Vec<Option<Vec<u8>>> = vec![None; merged.len()];
            let mut fetches = stream::iter(merged.iter().enumerate().map(
                |(i, &(start, len))| async move {
                    let range = byte_range(start, len)?;
                    let bytes = self.fetch_hedged(bucket, key, Some(range)).await?;
                    Ok::<_, anyhow::Error>((i, bytes))
                },
            ))
            .buffer_unordered(self.0.cfg.cap);
            while let Some(result) = fetches.next().await {
                let (i, bytes) = result?;
                match bytes {
                    Some(bytes) => blobs[i] = Some(bytes),
                    None => return Ok(None),
                }
            }
            drop(fetches);
            Ok::<_, anyhow::Error>(Some(blobs))
        })?;
        let Some(blobs) = blobs else {
            return Ok(None);
        };
        let mut out: Vec<Vec<u8>> = vec![Vec::new(); ranges.len()];
        for (k, &original) in order.iter().enumerate() {
            let (blob_idx, start) = placements[k];
            let len = usize::try_from(sorted[k].1)?;
            let blob = blobs[blob_idx].as_ref().expect("all ranges fetched");
            let slice = blob.get(start..start + len).with_context(|| {
                format!(
                    "coalesced read of {key} is {} bytes, range needs {}",
                    blob.len(),
                    start + len
                )
            })?;
            out[original] = slice.to_vec();
        }
        Ok(Some(out))
    }

    /// Stream objects to `consume` as fetches complete (unordered). `None`
    /// body = object does not exist. The first fetch or `consume` error
    /// aborts the remaining fetches.
    ///
    /// Fetches are driven by a spawned runtime task while `consume` runs on
    /// the calling thread, so a `consume` that blocks (e.g. on a full
    /// downstream channel) applies backpressure without stalling in-flight
    /// requests.
    pub fn get_each(
        &self,
        bucket: &str,
        keys: Vec<(DocId, String)>,
        consume: &mut dyn FnMut(DocId, Option<Vec<u8>>) -> Result<()>,
    ) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(DocId, Option<Vec<u8>>)>(64);
        let cap = self.0.cfg.cap;
        let bucket_shared: Arc<str> = Arc::from(bucket);
        let client = self.clone();
        let driver = self.0.rt.spawn(async move {
            let mut fetches = stream::iter(keys.into_iter().map(|(id, key)| {
                let client = client.clone();
                let bucket = Arc::clone(&bucket_shared);
                async move { (id, client.fetch_hedged(&bucket, &key, None).await) }
            }))
            .buffer_unordered(cap);
            loop {
                // Notice a dropped receiver immediately (e.g. `| head`
                // closed the pipe), not at the next fetch completion —
                // in-flight requests are cancelled right away.
                tokio::select! {
                    biased;
                    () = tx.closed() => break,
                    next = fetches.next() => match next {
                        Some((id, result)) => {
                            if tx.send((id, result?)).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    },
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        let mut consume_result = Ok(());
        while let Some((id, bytes)) = rx.blocking_recv() {
            if let Err(err) = consume(id, bytes) {
                consume_result = Err(err);
                break;
            }
        }
        drop(rx);
        let driver_result = self
            .0
            .rt
            .block_on(driver)
            .map_err(|err| anyhow::anyhow!("fetch driver panicked: {err}"))?;
        consume_result?;
        driver_result
    }

    pub fn put(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        self.0.rt.block_on(self.put_async(bucket, key, body))
    }

    async fn put_async(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        if body.len() > MULTIPART_PART_SIZE {
            return self.put_multipart(bucket, key, body).await;
        }
        let req = S3Request {
            method: "PUT",
            bucket,
            key: Some(key),
            canonical_query: "",
            range: None,
            body: Some(body),
            precondition: None,
        };
        self.send_resilient(&req, None)
            .await?
            .with_context(|| format!("PUT s3://{bucket}/{key} returned HTTP 404"))?;
        Ok(())
    }

    /// Multipart upload for bodies over one part size: parts upload
    /// concurrently through the same retry engine, and any failure aborts
    /// the upload server-side before returning the error.
    async fn put_multipart(&self, bucket: &str, key: &str, body: &[u8]) -> Result<()> {
        let initiate = canonical_query(&mut vec![("uploads", String::new())]);
        let (_, _, init_body) = self
            .send_resilient(
                &S3Request {
                    method: "POST",
                    bucket,
                    key: Some(key),
                    canonical_query: &initiate,
                    range: None,
                    body: None,
                    precondition: None,
                },
                None,
            )
            .await?
            .with_context(|| format!("initiate multipart s3://{bucket}/{key}: HTTP 404"))?;
        let upload_id = xml_text(&init_body, "UploadId")
            .with_context(|| format!("initiate multipart s3://{bucket}/{key}: no UploadId"))?;

        let parts = self.upload_parts(bucket, key, &upload_id, body).await;
        let parts = match parts {
            Ok(parts) => parts,
            Err(err) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                return Err(err);
            }
        };

        let mut complete_body = String::from(
            r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
        );
        for (number, etag) in &parts {
            complete_body.push_str(&format!(
                "<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
        }
        complete_body.push_str("</CompleteMultipartUpload>");
        let query = canonical_query(&mut vec![("uploadId", upload_id.clone())]);
        let completed = self
            .send_resilient(
                &S3Request {
                    method: "POST",
                    bucket,
                    key: Some(key),
                    canonical_query: &query,
                    range: None,
                    body: Some(complete_body.as_bytes()),
                    precondition: None,
                },
                None,
            )
            .await;
        match completed {
            // S3 can return 200 OK with an error document inside; only a
            // body naming our key (CompleteMultipartUploadResult) is success.
            Ok(Some((_, _, response))) if !response.windows(7).any(|w| w == b"<Error>") => Ok(()),
            Ok(Some((_, _, response))) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                anyhow::bail!(
                    "complete multipart s3://{bucket}/{key} returned an error document: {}",
                    String::from_utf8_lossy(&response[..response.len().min(300)])
                )
            }
            Ok(None) => {
                anyhow::bail!("complete multipart s3://{bucket}/{key}: HTTP 404")
            }
            Err(err) => {
                self.abort_multipart(bucket, key, &upload_id).await;
                Err(err)
            }
        }
    }

    async fn upload_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        body: &[u8],
    ) -> Result<Vec<(usize, String)>> {
        let mut uploads = stream::iter(body.chunks(MULTIPART_PART_SIZE).enumerate().map(
            |(i, chunk)| async move {
                let number = i + 1;
                let query = canonical_query(&mut vec![
                    ("partNumber", number.to_string()),
                    ("uploadId", upload_id.to_owned()),
                ]);
                let (_, etag, _) = self
                    .send_resilient(
                        &S3Request {
                            method: "PUT",
                            bucket,
                            key: Some(key),
                            canonical_query: &query,
                            range: None,
                            body: Some(chunk),
                            precondition: None,
                        },
                        None,
                    )
                    .await?
                    .with_context(|| format!("upload part {number} of s3://{bucket}/{key}"))?;
                let etag =
                    etag.with_context(|| format!("part {number} of s3://{bucket}/{key}: no ETag"))?;
                Ok::<_, anyhow::Error>((number, etag))
            },
        ))
        .buffer_unordered(MULTIPART_CONCURRENCY);
        let mut parts = Vec::new();
        while let Some(result) = uploads.next().await {
            parts.push(result?);
        }
        parts.sort_unstable_by_key(|(number, _)| *number);
        Ok(parts)
    }

    /// Best-effort: a leaked multipart upload only costs storage until the
    /// bucket lifecycle cleans it, never correctness.
    async fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) {
        let query = canonical_query(&mut vec![("uploadId", upload_id.to_owned())]);
        let req = S3Request {
            method: "DELETE",
            bucket,
            key: Some(key),
            canonical_query: &query,
            range: None,
            body: None,
            precondition: None,
        };
        if self.send_resilient(&req, None).await.is_err() {
            eprintln!("warning: failed to abort multipart upload of s3://{bucket}/{key}");
        }
    }

    /// Fetch one object plus its `ETag` (the version token for `put_if`).
    pub fn get_with_version(&self, bucket: &str, key: &str) -> Result<Option<(Vec<u8>, String)>> {
        let req = S3Request {
            method: "GET",
            bucket,
            key: Some(key),
            canonical_query: "",
            range: None,
            body: None,
            precondition: None,
        };
        match self.0.rt.block_on(self.send_resilient(&req, None))? {
            None => Ok(None),
            Some((_, etag, bytes)) => {
                let etag = etag.with_context(|| format!("GET s3://{bucket}/{key}: no ETag"))?;
                Ok(Some((bytes, etag)))
            }
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
            body: Some(body),
            precondition: Some(expected),
        };
        match self.0.rt.block_on(self.send_resilient(&req, None)) {
            Ok(_) => Ok(true),
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
        self.0.rt.block_on(self.send_resilient(&req, None))?;
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
            loop {
                let mut params = vec![("list-type", "2".to_owned()), ("prefix", prefix.to_owned())];
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
                let (_, _, bytes) = self
                    .send_resilient(&req, None)
                    .await?
                    .with_context(|| format!("list s3://{bucket}: bucket not found"))?;
                let body = String::from_utf8(bytes).context("ListObjectsV2 response not UTF-8")?;
                let (objects, next) = parse_list_v2(&body)?;
                all.extend(objects);
                match next {
                    Some(t) => token = Some(t),
                    None => break,
                }
            }
            Ok(all)
        })
    }
}

fn byte_range(start: u64, len: u64) -> Result<(u64, u64)> {
    let end = start
        .checked_add(len)
        .and_then(|v| v.checked_sub(1))
        .ok_or_else(|| anyhow::anyhow!("invalid empty S3 range"))?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::coalesce_ranges;

    #[test]
    fn coalesce_merges_within_gap_and_places_blocks() {
        let ranges = [(0u64, 100u64), (150, 50), (10_000, 8), (10_008, 4)];
        let (merged, placements) = coalesce_ranges(&ranges, 64);
        assert_eq!(merged, vec![(0, 200), (10_000, 12)]);
        assert_eq!(placements, vec![(0, 0), (0, 150), (1, 0), (1, 8)]);

        let (merged, placements) = coalesce_ranges(&ranges, 0);
        assert_eq!(merged.len(), 3);
        assert_eq!(placements[3], (2, 8));

        let (merged, _) = coalesce_ranges(&[], 64);
        assert!(merged.is_empty());
    }
}
