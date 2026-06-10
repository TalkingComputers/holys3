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

fn build_http(pool_max_idle: usize) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .pool_max_idle_per_host(pool_max_idle)
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(30))
        .http2_adaptive_window(true)
        .connect_timeout(Duration::from_secs(3))
        .read_timeout(Duration::from_secs(20))
        .build()
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
}

enum Outcome {
    Success(StatusCode, Vec<u8>),
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
        let builder = match req.method {
            "PUT" => http.put(url),
            _ => http.get(url),
        };
        let mut builder = apply_signed(builder, &signed, &host, creds.session_token.as_deref());
        if let Some(range) = &range_header {
            builder = builder.header("range", range);
        }
        if let Some(body) = req.body {
            builder = builder.body(body.to_vec());
        }
        let response = match builder.send().await {
            Ok(response) => response,
            Err(err) if err.is_builder() => return Outcome::Fatal(err.into()),
            Err(err) => return Outcome::Transient(err.into()),
        };
        let status = response.status();
        if status.is_success() {
            return match response.bytes().await {
                Ok(bytes) => Outcome::Success(status, bytes.to_vec()),
                Err(err) => Outcome::Transient(err.into()),
            };
        }
        match status {
            StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS => Outcome::Throttle,
            StatusCode::NOT_FOUND => Outcome::NotFound,
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
    ) -> Result<Option<(StatusCode, Vec<u8>)>> {
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
                Outcome::Success(status, bytes) => {
                    self.0.limiter.on_success();
                    return Ok(Some((status, bytes)));
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
    /// `hedge_after` without completing, race a budgeted duplicate request.
    async fn fetch_hedged(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> Result<Option<Vec<u8>>> {
        let req = S3Request {
            method: "GET",
            bucket,
            key: Some(key),
            canonical_query: "",
            range,
            body: None,
        };
        let started = Notify::new();
        let primary = self.send_resilient(&req, Some(&started));
        tokio::pin!(primary);
        let result = tokio::select! {
            biased;
            result = &mut primary => result,
            () = async { started.notified().await; tokio::time::sleep(self.0.cfg.hedge_after).await } => {
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
        let Some((status, bytes)) = result? else {
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
    /// `None` = object does not exist.
    pub fn get_ranges(
        &self,
        bucket: &str,
        key: &str,
        ranges: &[(u64, u64)],
    ) -> Result<Option<Vec<Vec<u8>>>> {
        self.0.rt.block_on(async {
            let mut out: Vec<Option<Vec<u8>>> = vec![None; ranges.len()];
            let mut fetches = stream::iter(ranges.iter().enumerate().map(
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
                    Some(bytes) => out[i] = Some(bytes),
                    None => return Ok(None),
                }
            }
            drop(fetches);
            Ok(Some(
                out.into_iter()
                    .map(|bytes| bytes.expect("all ranges fetched"))
                    .collect(),
            ))
        })
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
        let req = S3Request {
            method: "PUT",
            bucket,
            key: Some(key),
            canonical_query: "",
            range: None,
            body: Some(body),
        };
        self.send_resilient(&req, None)
            .await?
            .with_context(|| format!("PUT s3://{bucket}/{key} returned HTTP 404"))?;
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
                params.sort_by(|a, b| a.0.cmp(b.0));
                let canonical_query = params
                    .iter()
                    .map(|(k, v)| {
                        format!(
                            "{}={}",
                            encode_query_component(k),
                            encode_query_component(v)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("&");
                let req = S3Request {
                    method: "GET",
                    bucket,
                    key: None,
                    canonical_query: &canonical_query,
                    range: None,
                    body: None,
                };
                let (_, bytes) = self
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
