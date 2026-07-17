use crate::fetch::{AimdLimiter, FetchConfig, HedgeBudget};
use crate::ObjectMeta;
use anyhow::{Context, Result};
use aws_sdk_s3::config::{
    Credentials, ProvideCredentials, Region, RequestChecksumCalculation, ResponseChecksumValidation,
};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::types::EncodingType;
use aws_smithy_runtime_api::client::result::SdkError as SmithyError;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use futures::stream::{self, StreamExt};
use seagrep_core::{DocumentBody, DocumentSpool};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use url::Url;

mod download;
mod upload;
pub(crate) use upload::StreamingUpload;

#[cfg(test)]
use download::coalesce_ranges;

const SMALL_READ_HEDGE: Duration = Duration::from_millis(300);
const SMALL_READ_MAX: u64 = 4 * 1024 * 1024;

const MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024;
const MULTIPART_CONCURRENCY: usize = 4;
const MULTIPART_BUFFER_BUDGET: usize = 256 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u64 = 10_000;
const MAX_MULTIPART_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_OBJECT_SIZE: u64 = MAX_MULTIPART_PARTS * MAX_MULTIPART_PART_SIZE;
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

fn upload_deadline(body_len: usize) -> Duration {
    Duration::from_secs(60 + (body_len / (256 * 1024)) as u64)
}

#[derive(Debug)]
pub(crate) struct PreconditionFailed;

impl std::fmt::Display for PreconditionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("conditional write precondition failed")
    }
}

impl std::error::Error for PreconditionFailed {}

pub(super) enum Outcome<T> {
    Success(T),
    Throttle,
    NotFound,
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

fn parse_endpoint(endpoint: Option<&str>) -> Result<Option<String>> {
    endpoint
        .map(|endpoint| {
            let url = Url::parse(endpoint)?;
            anyhow::ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.username().is_empty()
                    && url.password().is_none(),
                "S3 endpoint must be an HTTP(S) origin URL without path, query, fragment, or credentials: {endpoint}"
            );
            Ok(endpoint.trim_end_matches('/').to_owned())
        })
        .transpose()
}

fn decode_list_key(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            anyhow::ensure!(
                bytes
                    .get(index + 1..index + 3)
                    .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit)),
                "ListObjectsV2 returned malformed URL-encoded Key"
            );
            index += 3;
        } else {
            index += 1;
        }
    }
    let value = if value.as_bytes().contains(&b'+') {
        std::borrow::Cow::Owned(value.replace('+', " "))
    } else {
        std::borrow::Cow::Borrowed(value)
    };
    percent_encoding::percent_decode_str(&value)
        .decode_utf8()
        .context("ListObjectsV2 Key is not valid UTF-8")
        .map(|key| key.into_owned())
}

pub(super) fn classify_sdk_error<E, T>(error: SdkError<E>) -> Outcome<T>
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    let code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code);
    if matches!(status, Some(429 | 503))
        || matches!(
            code,
            Some("SlowDown" | "Throttling" | "ThrottlingException")
        )
    {
        return Outcome::Throttle;
    }
    if status == Some(404) || matches!(code, Some("NoSuchKey" | "NoSuchBucket")) {
        return Outcome::NotFound;
    }
    if matches!(status, Some(409 | 412))
        || matches!(
            code,
            Some("ConditionalRequestConflict" | "PreconditionFailed")
        )
    {
        return Outcome::Fatal(anyhow::Error::new(PreconditionFailed));
    }
    let is_transport = matches!(
        &error,
        SmithyError::TimeoutError(_)
            | SmithyError::DispatchFailure(_)
            | SmithyError::ResponseError(_)
    );
    let is_server = status.is_some_and(|status| status == 408 || status >= 500);
    let is_transient = matches!(
        code,
        Some("InternalError" | "RequestTimeout" | "RequestTimeoutException")
    );
    let error = anyhow::Error::new(error);
    if is_transport || is_server || is_transient {
        Outcome::Transient(error)
    } else {
        Outcome::Fatal(error)
    }
}

pub(super) async fn read_body(mut stream: ByteStream, hint: u64) -> Result<DocumentBody> {
    if hint < SMALL_READ_MAX {
        return Ok(DocumentBody::from_bytes(
            stream.collect().await?.into_bytes(),
        ));
    }
    let mut body = DocumentSpool::new(hint)?;
    let mut at = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let end = at
            .checked_add(u64::try_from(chunk.len())?)
            .context("streamed S3 response length overflows")?;
        anyhow::ensure!(
            end <= hint,
            "streamed S3 response exceeds its expected length"
        );
        body.write_at(at, &chunk)?;
        at = end;
    }
    anyhow::ensure!(
        at == hint,
        "streamed S3 response is {at} bytes, expected {hint}"
    );
    body.finish()
}

struct ClientInner {
    region: String,
    sdk: aws_sdk_s3::Client,
    upload_sdk: aws_sdk_s3::Client,
    endpoint_base: Option<String>,
    cfg: FetchConfig,
    limiter: AimdLimiter,
    hedges: HedgeBudget,
    rt: tokio::runtime::Runtime,
}

#[derive(Clone)]
pub struct S3Client(Arc<ClientInner>);

#[cfg(test)]
#[derive(Clone)]
struct TestCredentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl S3Client {
    #[cfg(test)]
    fn new(
        region: String,
        credentials: TestCredentials,
        endpoint: Option<String>,
        cfg: FetchConfig,
    ) -> Result<S3Client> {
        Self::connect_static(
            region,
            credentials.access_key,
            credentials.secret_key,
            credentials.session_token,
            endpoint,
            cfg,
        )
    }

    pub fn connect(
        region: Option<String>,
        endpoint: Option<String>,
        cfg: FetchConfig,
    ) -> Result<S3Client> {
        Self::connect_with_credentials(region, endpoint, cfg, None)
    }

    pub fn connect_static(
        region: String,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        endpoint: Option<String>,
        cfg: FetchConfig,
    ) -> Result<S3Client> {
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            None,
            "seagrep-static",
        );
        Self::connect_with_credentials(Some(region), endpoint, cfg, Some(credentials))
    }

    fn connect_with_credentials(
        region: Option<String>,
        endpoint: Option<String>,
        cfg: FetchConfig,
        credentials: Option<Credentials>,
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
        let endpoint_base = parse_endpoint(endpoint.as_deref())?;
        let worker_threads = cfg
            .cap
            .min(std::thread::available_parallelism()?.get())
            .min(4);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()?;
        let endpoint_for_sdk = endpoint_base.clone();
        // Snapshot the credential-cache key before the SDK reads any of its
        // inputs; the provider re-verifies it before writing, bracketing the
        // SDK's reads so an edit in between voids the cache write.
        let cache_base = if credentials.is_none() {
            crate::creds::key_base()
        } else {
            None
        };
        let (sdk, upload_sdk, region) = rt.block_on(async move {
            let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .retry_config(aws_config::retry::RetryConfig::disabled())
                .timeout_config(
                    aws_config::timeout::TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(3))
                        .read_timeout(Duration::from_secs(20))
                        .build(),
                );
            if let Some(region) = region {
                loader = loader.region(Region::new(region));
            }
            let has_static_credentials = credentials.is_some();
            if let Some(credentials) = credentials {
                loader = loader.credentials_provider(credentials);
            }
            if endpoint_for_sdk.is_some() {
                loader = loader
                    .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
                    .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
            }
            let shared = loader.load().await;
            let region = shared
                .region()
                .context(
                    "provide --region, set AWS_REGION/AWS_DEFAULT_REGION, or configure region in the active AWS profile",
                )?
                .as_ref()
                .to_owned();
            let cached_provider = if has_static_credentials {
                None
            } else if let Some(base) = cache_base {
                // Cache-enabled mode resolves through the profile provider
                // alone: the default chain would silently fall through to
                // instance metadata when SSO fails, and those credentials
                // must never be persisted under the SSO key. For a pure-SSO
                // profile a loud "log in again" beats a silent identity
                // switch.
                let profile_only =
                    aws_config::profile::ProfileFileCredentialsProvider::builder().build();
                let provider = crate::creds::DiskCachedProvider::new(
                    aws_credential_types::provider::SharedCredentialsProvider::new(profile_only),
                    base,
                );
                provider.provide_credentials().await.context(
                    "no AWS credentials: run `aws sso login` for the active AWS profile",
                )?;
                Some(provider)
            } else {
                let chain = shared.credentials_provider().context(
                    "no AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or configure the active AWS profile",
                )?;
                chain.provide_credentials().await.context(
                    "no AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or configure the active AWS profile",
                )?;
                None
            };
            let mut service = aws_sdk_s3::config::Builder::from(&shared)
                .retry_config(aws_config::retry::RetryConfig::disabled())
                .timeout_config(
                    aws_config::timeout::TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(3))
                        .read_timeout(Duration::from_secs(20))
                        .build(),
                );
            if let Some(endpoint) = endpoint_for_sdk {
                service = service
                    .endpoint_url(endpoint)
                    .force_path_style(true)
                    .disable_multi_region_access_points(true)
                    .disable_s3_express_session_auth(true)
                    .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
                    .response_checksum_validation(ResponseChecksumValidation::WhenRequired);
            }
            if let Some(provider) = cached_provider {
                service = service.credentials_provider(provider);
            }
            let service = service.build();
            let upload_service = service
                .to_builder()
                .timeout_config(
                    aws_config::timeout::TimeoutConfig::disabled()
                        .to_builder()
                        .connect_timeout(Duration::from_secs(3))
                        .build(),
                )
                .build();
            Ok::<_, anyhow::Error>(
                (
                    aws_sdk_s3::Client::from_conf(service),
                    aws_sdk_s3::Client::from_conf(upload_service),
                    region,
                ),
            )
        })?;
        Ok(S3Client(Arc::new(ClientInner {
            sdk,
            upload_sdk,
            limiter: AimdLimiter::new(cfg.start, cfg.cap),
            hedges: HedgeBudget::new(cfg.hedge_tokens),
            region,
            endpoint_base,
            cfg,
            rt,
        })))
    }

    pub fn endpoint_identity(&self) -> String {
        self.0
            .endpoint_base
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.0.region))
    }

    pub(crate) fn max_concurrency(&self) -> usize {
        self.0.cfg.cap
    }

    pub(super) async fn run_resilient<T, F, Fut>(
        &self,
        label: &str,
        on_permit: Option<&Notify>,
        mut send: F,
    ) -> Result<Option<T>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Outcome<T>>,
    {
        let mut attempt = 0u32;
        loop {
            let permit = self.0.limiter.acquire().await?;
            if let Some(notify) = on_permit {
                notify.notify_one();
            }
            let outcome = send().await;
            drop(permit);
            let error = match outcome {
                Outcome::Success(output) => {
                    self.0.limiter.on_success();
                    return Ok(Some(output));
                }
                Outcome::NotFound => return Ok(None),
                Outcome::Fatal(error) => return Err(error.context(label.to_owned())),
                Outcome::Throttle => {
                    self.0.limiter.on_throttle();
                    anyhow::anyhow!("throttled (HTTP 503/429)")
                }
                Outcome::Transient(error) => error,
            };
            if attempt >= self.0.cfg.max_retries {
                return Err(error.context(format!(
                    "{label} failed after {} retries",
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

    pub fn put_if(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        expected: Option<&str>,
    ) -> Result<bool> {
        let body = Bytes::copy_from_slice(body);
        let body_len = body.len();
        let expected = expected.map(str::to_owned);
        let label = format!("conditional PUT s3://{bucket}/{key}");
        let result = self.0.rt.block_on(self.run_resilient(&label, None, || {
            let mut request = self
                .0
                .upload_sdk
                .put_object()
                .bucket(bucket)
                .key(key)
                .body(ByteStream::from(body.clone()));
            request = match &expected {
                Some(etag) => request.if_match(etag),
                None => request.if_none_match("*"),
            };
            async move {
                match tokio::time::timeout(upload_deadline(body_len), request.send()).await {
                    Ok(Ok(output)) => Outcome::Success(output),
                    Ok(Err(error)) => classify_sdk_error(error),
                    Err(error) => Outcome::Transient(error.into()),
                }
            }
        }));
        match result {
            Ok(Some(_)) => Ok(true),
            Ok(None) => anyhow::bail!("{label}: bucket not found"),
            Err(error) if error.root_cause().is::<PreconditionFailed>() => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let label = format!("DELETE s3://{bucket}/{key}");
        self.0
            .rt
            .block_on(self.run_resilient(&label, None, || {
                let request = self.0.sdk.delete_object().bucket(bucket).key(key);
                async move {
                    match request.send().await {
                        Ok(output) => Outcome::Success(output),
                        Err(error) => classify_sdk_error(error),
                    }
                }
            }))?
            .with_context(|| format!("{label}: bucket not found"))?;
        Ok(())
    }

    pub fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<ObjectMeta>> {
        self.list_with_progress(bucket, prefix, None)
    }

    pub fn list_with_progress(
        &self,
        bucket: &str,
        prefix: &str,
        progress: Option<&seagrep_core::ProgressSender>,
    ) -> Result<Vec<ObjectMeta>> {
        self.0.rt.block_on(async {
            let mut objects = Vec::new();
            let mut token: Option<String> = None;
            let mut tokens = std::collections::HashSet::new();
            loop {
                let label = format!("LIST s3://{bucket}/{prefix}");
                let output = self
                    .run_resilient(&label, None, || {
                        let request = self
                            .0
                            .sdk
                            .list_objects_v2()
                            .bucket(bucket)
                            .prefix(prefix)
                            .encoding_type(EncodingType::Url)
                            .set_continuation_token(token.clone());
                        async move {
                            match request.send().await {
                                Ok(output) => Outcome::Success(output),
                                Err(error) => classify_sdk_error(error),
                            }
                        }
                    })
                    .await?
                    .with_context(|| format!("list s3://{bucket}: bucket not found"))?;
                let is_url_encoded = match output.encoding_type() {
                    Some(EncodingType::Url) => true,
                    Some(encoding) => anyhow::bail!(
                        "unsupported ListObjectsV2 EncodingType {}",
                        encoding.as_str()
                    ),
                    None => false,
                };
                for object in output.contents() {
                    let mut key = object
                        .key()
                        .context("ListObjectsV2 object missing Key")?
                        .to_owned();
                    if is_url_encoded {
                        key = decode_list_key(&key)?;
                    }
                    let etag = object
                        .e_tag()
                        .context("ListObjectsV2 object missing ETag")?
                        .to_owned();
                    let size =
                        u64::try_from(object.size().context("ListObjectsV2 object missing Size")?)
                            .context("ListObjectsV2 object has negative Size")?;
                    objects.push(ObjectMeta { key, etag, size });
                }
                if let Some(progress) = progress {
                    progress.emit(seagrep_core::ProgressEvent::Listed {
                        objects: objects.len() as u64,
                    });
                }
                let truncated = output
                    .is_truncated()
                    .context("ListObjectsV2 response missing IsTruncated")?;
                let next = output.next_continuation_token();
                anyhow::ensure!(
                    !truncated || next.is_some_and(|token| !token.is_empty()),
                    "truncated ListObjectsV2 response missing NextContinuationToken"
                );
                anyhow::ensure!(
                    truncated || next.is_none(),
                    "untruncated ListObjectsV2 response included NextContinuationToken"
                );
                match next {
                    Some(next) => {
                        anyhow::ensure!(
                            tokens.insert(next.to_owned()),
                            "ListObjectsV2 repeated continuation token"
                        );
                        token = Some(next.to_owned());
                    }
                    None => break,
                }
            }
            Ok(objects)
        })
    }

    pub fn put_many(&self, bucket: &str, objects: Vec<(String, Vec<u8>)>) -> Result<()> {
        self.0.rt.block_on(async {
            let mut puts = stream::iter(objects.iter().map(|(key, body)| {
                let client = self;
                async move { client.put_async(bucket, key, body, None).await }
            }))
            .buffer_unordered(self.0.cfg.cap);
            while let Some(result) = puts.next().await {
                result?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        coalesce_ranges, multipart_concurrency, multipart_part_size, FetchConfig, S3Client,
        TestCredentials as Credentials, MAX_MULTIPART_OBJECT_SIZE, MAX_MULTIPART_PARTS,
        MAX_MULTIPART_PART_SIZE, MULTIPART_PART_SIZE, SMALL_READ_MAX,
    };
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
        content_range: Option<&str>,
    ) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let status = status.to_owned();
        let body = body.to_vec();
        let content_range = content_range.map(str::to_owned);
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 8192];
            let read = stream.read(&mut request).unwrap();
            let content_range = content_range
                .map(|value| format!("Content-Range: {value}\r\n"))
                .unwrap_or_default();
            write!(stream, "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{content_range}Connection: close\r\n\r\n", body.len()).unwrap();
            stream.write_all(&body).unwrap();
            String::from_utf8_lossy(&request[..read]).into_owned()
        });
        (format!("http://{address}"), thread)
    }

    fn start_range_version_server(
        part_len: usize,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for (etag, byte) in [("\"v1\"", b'a'), ("\"v2\"", b'b')] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 8192];
                let read = stream.read(&mut request).unwrap();
                let request = String::from_utf8_lossy(&request[..read]).into_owned();
                let lower = request.to_ascii_lowercase();
                let range = lower
                    .lines()
                    .find_map(|line| line.strip_prefix("range: bytes="))
                    .unwrap();
                write!(
                    stream,
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {part_len}\r\nContent-Range: bytes {range}/*\r\nETag: {etag}\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                stream.write_all(&vec![byte; part_len]).unwrap();
                requests.push(request);
            }
            requests
        });
        (format!("http://{address}"), thread)
    }

    fn start_error_then_success_server(code: &str) -> (String, std::thread::JoinHandle<usize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let code = code.to_owned();
        let thread = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            let mut requests = 0;
            while requests < 2 && std::time::Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .unwrap();
                        let mut request = [0u8; 8192];
                        let _ = stream.read(&mut request).unwrap();
                        requests += 1;
                        if requests == 1 {
                            let body = format!("<Error><Code>{code}</Code></Error>");
                            write!(
                                stream,
                                "HTTP/1.1 400 Bad Request\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                body.len()
                            )
                            .unwrap();
                            stream.write_all(body.as_bytes()).unwrap();
                        } else {
                            write!(
                                stream,
                                "HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbody"
                            )
                            .unwrap();
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("request timeout test server failed: {error}"),
                }
            }
            requests
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
        assert_eq!(client.endpoint_identity(), "http://[::1]:9000");
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
        let (endpoint, server) = start_response_server("200 OK", b"body", None);
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

        let (endpoint, server) = start_response_server("412 Precondition Failed", b"", None);
        let client = S3Client::new(
            "us-east-1".into(),
            credentials,
            Some(endpoint),
            FetchConfig::default(),
        )
        .unwrap();
        let error = client.get_if_match("bucket", "key", "\"old\"").unwrap_err();
        assert!(error.is::<seagrep_core::StaleSource>(), "{error:#}");
        let stale = error.downcast_ref::<seagrep_core::StaleSource>().unwrap();
        assert_eq!(stale.key, "key");
        assert_eq!(stale.expected, "\"old\"");
        server.join().unwrap();
    }

    #[test]
    fn list_requests_url_encoding_and_decodes_keys() {
        let body = br#"<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>logs%2Fspace+and%2Bplus%25.log</Key><Size>4</Size><ETag>&quot;etag&quot;</ETag></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let (endpoint, server) = start_response_server("200 OK", body, None);
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
                key: "logs/space and+plus%.log".into(),
                etag: "\"etag\"".into(),
                size: 4,
            }]
        );
        let request = server.join().unwrap();
        let request_line = request.lines().next().unwrap();
        assert!(request_line.starts_with("GET /bucket/?"), "{request}");
        assert!(request_line.contains("encoding-type=url"), "{request}");
        assert!(request_line.contains("list-type=2"), "{request}");
        assert!(request_line.contains("prefix=logs%2F"), "{request}");
    }

    #[test]
    fn rejects_truncated_listing_without_token() {
        let body = br#"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"#;
        let (endpoint, server) = start_response_server("200 OK", body, None);
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
        let error = client.list("bucket", "").unwrap_err();
        assert!(
            format!("{error:#}").contains("NextContinuationToken"),
            "{error:#}"
        );
        server.join().unwrap();
    }

    #[test]
    fn rejects_unknown_listing_encoding() {
        let body = br#"<ListBucketResult><EncodingType>other</EncodingType><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let (endpoint, server) = start_response_server("200 OK", body, None);
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
        let error = client.list("bucket", "").unwrap_err();
        assert!(
            format!("{error:#}").contains("unsupported ListObjectsV2 EncodingType"),
            "{error:#}"
        );
        server.join().unwrap();
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
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
                    bytes.len(),
                    server_body.len()
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
        let content_range = format!("bytes 0-{}/{}", body.len() - 1, body.len());
        let (endpoint, server) =
            start_response_server("206 Partial Content", &body, Some(&content_range));
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
        let content_range = format!("bytes 0-{}/{}", body.len() - 1, body.len());
        let (endpoint, server) =
            start_response_server("206 Partial Content", &body, Some(&content_range));
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
    fn rejects_object_overwrite_between_file_ranges() {
        let part_len = 8 * 1024 * 1024usize;
        let size = 2 * part_len;
        let (endpoint, server) = start_range_version_server(part_len);
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
        let result = client.get_file("bucket", "key", &mut output, u64::try_from(size).unwrap());
        let requests = server.join().unwrap();
        let error = result.unwrap_err();
        assert!(error.is::<seagrep_core::StaleSource>(), "{error:#}");
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| {
            let request = request.to_ascii_lowercase();
            request.starts_with("get ")
                && request.contains("range: bytes=")
                && !request.contains("if-match:")
        }));
    }

    #[test]
    fn rejects_object_overwrite_between_range_reads() {
        let (endpoint, server) = start_range_version_server(4);
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
        let result = client.get_ranges("bucket", "key", &[(0, 4), (1024 * 1024, 4)]);
        let requests = server.join().unwrap();
        let error = result.unwrap_err();
        assert!(error.is::<seagrep_core::StaleSource>(), "{error:#}");
        assert_eq!(requests.len(), 2);
    }

    #[test]
    fn writes_empty_object_to_file() {
        let (endpoint, server) = start_response_server("200 OK", b"", None);
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
        let (endpoint, server) = start_response_server("200 OK", b"body", None);
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
        assert!(
            format!("{error:#}").contains("did not return Content-Range"),
            "{error:#}"
        );
        server.join().unwrap();
    }

    #[test]
    fn rejects_wrong_content_range() {
        let (endpoint, server) =
            start_response_server("206 Partial Content", b"body", Some("bytes 0-3/8"));
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
        let error = client.get_range("bucket", "key", 4, 4).unwrap_err();
        assert!(
            format!("{error:#}").contains("wrong Content-Range"),
            "{error:#}"
        );
        server.join().unwrap();
    }

    #[test]
    fn retries_request_timeout_error_code() {
        let (endpoint, server) = start_error_then_success_server("RequestTimeout");
        let config = FetchConfig {
            max_retries: 1,
            backoff_base_ms: 0,
            backoff_cap_ms: 0,
            hedge_tokens: 0,
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
        let result = client.get("bucket", "key");
        assert_eq!(server.join().unwrap(), 2);
        assert_eq!(result.unwrap(), Some(b"body".to_vec()));
    }

    #[test]
    fn retries_internal_error_code() {
        let (endpoint, server) = start_error_then_success_server("InternalError");
        let config = FetchConfig {
            max_retries: 1,
            backoff_base_ms: 0,
            backoff_cap_ms: 0,
            hedge_tokens: 0,
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
        let result = client.get("bucket", "key");
        assert_eq!(server.join().unwrap(), 2);
        assert_eq!(result.unwrap(), Some(b"body".to_vec()));
    }

    #[test]
    fn upload_client_disables_time_to_first_byte_timeout() {
        let client = S3Client::new(
            "us-east-1".into(),
            Credentials {
                access_key: "test".into(),
                secret_key: "test".into(),
                session_token: None,
            },
            Some("http://127.0.0.1:9000".into()),
            FetchConfig::default(),
        )
        .unwrap();
        let upload = client.0.upload_sdk.config().timeout_config().unwrap();
        assert_eq!(upload.connect_timeout(), Some(Duration::from_secs(3)));
        assert_eq!(upload.read_timeout(), None);
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
