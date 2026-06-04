#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! S3 client, blob store, and corpus implementations.

pub mod fetch;

use anyhow::Context;
use fetch::{fetch_one_hedged, AimdLimiter, RetryBudget};
use futures::stream::{self, StreamExt};
use holys3_core::{BlobStore, Corpus, DocId};
use holys3_sigv4::{encode_query_component, sign_get, sign_request, Credentials};
use std::sync::Arc;
use std::time::Duration;

pub use fetch::FetchConfig;

pub fn build_fetch_config(concurrency: usize) -> FetchConfig {
    let default = FetchConfig::default();
    FetchConfig {
        start: default.start.min(concurrency),
        cap: concurrency,
        ..default
    }
}

pub fn region_from_env() -> anyhow::Result<String> {
    std::env::var("AWS_REGION").context("provide --region or set AWS_REGION")
}

pub fn s3_client_from_env(region: &str, endpoint: Option<String>) -> anyhow::Result<S3Client> {
    let creds = holys3_sigv4::resolve("default")?;
    let path_style = endpoint.is_some();
    Ok(S3Client::new(
        region.to_owned(),
        creds,
        endpoint,
        path_style,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub etag: String,
    pub size: u64,
}

/// Parse one `ListObjectsV2` XML page: returns (`objects`, `next_continuation_token`).
pub fn parse_list_v2(xml: &str) -> anyhow::Result<(Vec<ObjectMeta>, Option<String>)> {
    use quick_xml::events::Event;
    use quick_xml::Reader;
    let mut reader = Reader::from_str(xml);
    let mut objs = Vec::new();
    let mut next = None;
    let (mut key, mut etag, mut size) = (String::new(), String::new(), 0u64);
    let mut cur = String::new();
    let mut in_contents = false;
    loop {
        match reader.read_event()? {
            Event::Start(e) => {
                cur = String::from_utf8_lossy(e.name().as_ref()).into_owned();
                if cur == "Contents" {
                    in_contents = true;
                    key.clear();
                    etag.clear();
                    size = 0;
                }
            }
            Event::Text(t) => {
                let txt = t.unescape()?.into_owned();
                match cur.as_str() {
                    "Key" if in_contents => key = txt,
                    "ETag" if in_contents => etag = txt.trim_matches('"').to_owned(),
                    "Size" if in_contents => {
                        size = txt.parse().context("invalid Size in ListObjectsV2")?;
                    }
                    "NextContinuationToken" => next = Some(txt),
                    _ => {}
                }
            }
            Event::End(e) => {
                if String::from_utf8_lossy(e.name().as_ref()) == "Contents" {
                    in_contents = false;
                    objs.push(ObjectMeta {
                        key: key.clone(),
                        etag: etag.clone(),
                        size,
                    });
                }
                cur.clear();
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok((objs, next))
}

#[derive(Clone)]
pub struct S3Client {
    pub region: String,
    pub creds: Credentials,
    pub endpoint: Option<String>,
    pub path_style: bool,
    http: reqwest::Client,
    retry_http: reqwest::Client,
}

impl S3Client {
    pub fn new(
        region: String,
        creds: Credentials,
        endpoint: Option<String>,
        path_style: bool,
    ) -> S3Client {
        S3Client {
            region,
            creds,
            endpoint,
            path_style,
            http: reqwest::Client::new(),
            retry_http: reqwest::Client::new(),
        }
    }

    pub fn with_fetch_config(&self, cfg: &FetchConfig) -> anyhow::Result<S3Client> {
        Ok(S3Client {
            region: self.region.clone(),
            creds: self.creds.clone(),
            endpoint: self.endpoint.clone(),
            path_style: self.path_style,
            http: reqwest::Client::builder()
                .pool_max_idle_per_host(cfg.cap)
                .pool_idle_timeout(Duration::from_secs(60))
                .tcp_keepalive(Duration::from_secs(30))
                .http2_adaptive_window(true)
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(20))
                .build()?,
            retry_http: reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .pool_idle_timeout(Duration::from_secs(60))
                .tcp_keepalive(Duration::from_secs(30))
                .http2_adaptive_window(true)
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(20))
                .build()?,
        })
    }

    fn endpoint_host(endpoint: &str) -> anyhow::Result<String> {
        let url = reqwest::Url::parse(endpoint)?;
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("S3 endpoint missing host: {endpoint}"))?;
        match url.port() {
            Some(port) => Ok(format!("{host}:{port}")),
            None => Ok(host.to_owned()),
        }
    }

    fn host(&self, bucket: &str) -> anyhow::Result<String> {
        match &self.endpoint {
            Some(endpoint) => Self::endpoint_host(endpoint),
            None => Ok(format!("{bucket}.s3.{}.amazonaws.com", self.region)),
        }
    }

    fn object_path(&self, bucket: &str, key: &str) -> anyhow::Result<String> {
        match &self.endpoint {
            Some(_) => {
                anyhow::ensure!(
                    self.path_style,
                    "custom S3 endpoint requires path-style addressing"
                );
                Ok(format!("/{bucket}/{key}"))
            }
            None => Ok(format!("/{key}")),
        }
    }

    fn list_path(&self, bucket: &str) -> anyhow::Result<String> {
        match &self.endpoint {
            Some(_) => {
                anyhow::ensure!(
                    self.path_style,
                    "custom S3 endpoint requires path-style addressing"
                );
                Ok(format!("/{bucket}/"))
            }
            None => Ok("/".to_owned()),
        }
    }

    fn request_url(&self, host: &str, path: &str, query: &str) -> String {
        let base = match &self.endpoint {
            Some(endpoint) => endpoint.trim_end_matches('/').to_owned(),
            None => format!("https://{host}"),
        };
        if query.is_empty() {
            format!("{base}{path}")
        } else {
            format!("{base}{path}?{query}")
        }
    }

    /// Timestamp helper: returns (`amz_date`, `date`).
    fn now() -> (String, String) {
        let dt = time::OffsetDateTime::now_utc();
        let amz = dt
            .format(
                &time::format_description::parse("[year][month][day]T[hour][minute][second]Z")
                    .unwrap(),
            )
            .unwrap();
        let date = amz[..8].to_string();
        (amz, date)
    }

    pub(crate) async fn send_get(
        &self,
        http: &reqwest::Client,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> anyhow::Result<reqwest::Response> {
        let host = self.host(bucket)?;
        let path = self.object_path(bucket, key)?;
        let (amz, date) = Self::now();
        let range_hdr = range.map(|(a, b)| format!("bytes={a}-{b}"));
        let extra: Vec<(&str, &str)> = match &range_hdr {
            Some(r) => vec![("range", r.as_str())],
            None => vec![],
        };
        let signed = sign_get(
            &self.creds,
            &self.region,
            &host,
            &path,
            "",
            &extra,
            &amz,
            &date,
        );
        let mut req = http
            .get(self.request_url(&host, &path, ""))
            .header("host", &host)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
            .header("authorization", &signed.authorization);
        if let Some(r) = &range_hdr {
            req = req.header("range", r);
        }
        if let Some(tok) = &self.creds.session_token {
            req = req.header("x-amz-security-token", tok);
        }
        Ok(req.send().await?)
    }

    pub async fn get(
        &self,
        bucket: &str,
        key: &str,
        range: Option<(u64, u64)>,
    ) -> anyhow::Result<Vec<u8>> {
        let resp = self
            .send_get(&self.http, bucket, key, range)
            .await?
            .error_for_status()?;
        Ok(resp.bytes().await?.to_vec())
    }

    pub async fn get_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        len: u64,
    ) -> anyhow::Result<Vec<u8>> {
        let end = start
            .checked_add(len)
            .and_then(|v| v.checked_sub(1))
            .ok_or_else(|| anyhow::anyhow!("invalid empty S3 range"))?;
        self.get(bucket, key, Some((start, end))).await
    }

    pub async fn put(&self, bucket: &str, key: &str, body: &[u8]) -> anyhow::Result<()> {
        let host = self.host(bucket)?;
        let path = self.object_path(bucket, key)?;
        let (amz, date) = Self::now();
        let signed = sign_request(
            "PUT",
            &self.creds,
            &self.region,
            &host,
            &path,
            "",
            &[],
            &amz,
            &date,
            "UNSIGNED-PAYLOAD",
        );
        let mut req = self
            .http
            .put(self.request_url(&host, &path, ""))
            .header("host", &host)
            .header("x-amz-date", &signed.x_amz_date)
            .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
            .header("authorization", &signed.authorization)
            .body(body.to_vec());
        if let Some(tok) = &self.creds.session_token {
            req = req.header("x-amz-security-token", tok);
        }
        req.send().await?.error_for_status()?;
        Ok(())
    }

    pub async fn list(&self, bucket: &str, prefix: &str) -> anyhow::Result<Vec<ObjectMeta>> {
        let host = self.host(bucket)?;
        let path = self.list_path(bucket)?;
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
            let (amz, date) = Self::now();
            let signed = sign_get(
                &self.creds,
                &self.region,
                &host,
                &path,
                &canonical_query,
                &[],
                &amz,
                &date,
            );
            let mut req = self
                .http
                .get(self.request_url(&host, &path, &canonical_query))
                .header("host", &host)
                .header("x-amz-date", &signed.x_amz_date)
                .header("x-amz-content-sha256", &signed.x_amz_content_sha256)
                .header("authorization", &signed.authorization);
            if let Some(tok) = &self.creds.session_token {
                req = req.header("x-amz-security-token", tok);
            }
            let body = req.send().await?.error_for_status()?.text().await?;
            let (objs, next) = parse_list_v2(&body)?;
            all.extend(objs);
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        Ok(all)
    }
}

pub struct S3BlobStore {
    client: S3Client,
    bucket: String,
    prefix: String,
    rt: tokio::runtime::Handle,
}

impl S3BlobStore {
    pub fn new(
        client: S3Client,
        bucket: String,
        prefix: String,
        rt: tokio::runtime::Handle,
        cfg: FetchConfig,
    ) -> anyhow::Result<S3BlobStore> {
        Ok(S3BlobStore {
            client: client.with_fetch_config(&cfg)?,
            bucket,
            prefix,
            rt,
        })
    }

    fn build_key(&self, name: &str) -> String {
        build_index_key(&self.prefix, name)
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(self.client.put(&self.bucket, &self.build_key(name), bytes))
        })
    }

    fn get(&self, name: &str) -> anyhow::Result<Vec<u8>> {
        tokio::task::block_in_place(|| {
            self.rt
                .block_on(self.client.get(&self.bucket, &self.build_key(name), None))
        })
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        tokio::task::block_in_place(|| {
            self.rt.block_on(
                self.client
                    .get_range(&self.bucket, &self.build_key(name), start, len),
            )
        })
    }
}

pub fn build_index_key(prefix: &str, name: &str) -> String {
    format!(
        "{}/{}",
        build_index_namespace(prefix),
        name.trim_start_matches('/')
    )
}

pub fn build_index_namespace(prefix: &str) -> String {
    let prefix = normalize_prefix(prefix);
    if prefix.is_empty() {
        ".holys3".into()
    } else {
        format!("{prefix}/.holys3")
    }
}

pub fn normalize_prefix(prefix: &str) -> String {
    prefix
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn is_index_key(prefix: &str, key: &str) -> bool {
    key.starts_with(&format!("{}/", build_index_namespace(prefix)))
}

/// Corpus over an S3 prefix. Loads object list eagerly; fetches bytes on demand.
pub struct S3Corpus {
    client: S3Client,
    bucket: String,
    docs: Vec<(DocId, String)>,
    rt: tokio::runtime::Handle,
    cfg: FetchConfig,
}

impl S3Corpus {
    pub fn new(
        client: S3Client,
        bucket: String,
        objects: Vec<ObjectMeta>,
        rt: tokio::runtime::Handle,
        cfg: FetchConfig,
    ) -> anyhow::Result<S3Corpus> {
        let docs = objects
            .iter()
            .enumerate()
            .map(|(i, o)| (i as DocId, o.key.clone()))
            .collect();
        Ok(S3Corpus {
            client: client.with_fetch_config(&cfg)?,
            bucket,
            docs,
            rt,
            cfg,
        })
    }

    pub fn from_docs(
        client: S3Client,
        bucket: String,
        docs: Vec<(DocId, String)>,
        rt: tokio::runtime::Handle,
        cfg: FetchConfig,
    ) -> anyhow::Result<S3Corpus> {
        Ok(S3Corpus {
            client: client.with_fetch_config(&cfg)?,
            bucket,
            docs,
            rt,
            cfg,
        })
    }
}

impl Corpus for S3Corpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn fetch(&self, id: DocId) -> anyhow::Result<Vec<u8>> {
        let key = self.docs[id as usize].1.clone();
        tokio::task::block_in_place(|| self.rt.block_on(self.client.get(&self.bucket, &key, None)))
    }

    fn fetch_many(&self, ids: &[DocId]) -> anyhow::Result<Vec<(DocId, anyhow::Result<Vec<u8>>)>> {
        let keys = ids
            .iter()
            .map(|&id| (id, self.docs[id as usize].1.clone()))
            .collect::<Vec<_>>();
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let cfg = self.cfg.clone();
        tokio::task::block_in_place(|| {
            self.rt.block_on(async move {
                let limiter = Arc::new(AimdLimiter::new(cfg.start, cfg.cap));
                let budget = Arc::new(RetryBudget::new(cfg.retry_tokens));
                let results = stream::iter(keys.into_iter().map(|(id, key)| {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let limiter = Arc::clone(&limiter);
                    let budget = Arc::clone(&budget);
                    let cfg = cfg.clone();
                    async move {
                        (
                            id,
                            fetch_one_hedged(&client, &bucket, &key, &limiter, &budget, &cfg).await,
                        )
                    }
                }))
                .buffer_unordered(cfg.buffer)
                .collect::<Vec<_>>()
                .await;
                Ok(results)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_two_objects_with_token() {
        let xml = r#"<?xml version="1.0"?>
        <ListBucketResult>
          <Contents><Key>a.txt</Key><Size>10</Size><ETag>"abc"</ETag></Contents>
          <Contents><Key>b/c.log</Key><Size>20</Size><ETag>"def"</ETag></Contents>
          <NextContinuationToken>TOK</NextContinuationToken>
        </ListBucketResult>"#;
        let (objs, next) = parse_list_v2(xml).unwrap();
        assert_eq!(
            objs,
            vec![
                ObjectMeta {
                    key: "a.txt".into(),
                    etag: "abc".into(),
                    size: 10
                },
                ObjectMeta {
                    key: "b/c.log".into(),
                    etag: "def".into(),
                    size: 20
                },
            ]
        );
        assert_eq!(next.as_deref(), Some("TOK"));
    }

    #[test]
    fn parse_list_v2_rejects_invalid_size() {
        let xml = r#"<ListBucketResult><Contents><Key>a.txt</Key><Size>nope</Size><ETag>"abc"</ETag></Contents></ListBucketResult>"#;
        let err = parse_list_v2(xml).unwrap_err();
        assert!(err.to_string().contains("invalid Size in ListObjectsV2"));
    }

    #[test]
    fn build_fetch_config_caps_initial_concurrency() {
        let cfg = build_fetch_config(16);
        assert_eq!(cfg.start, 16);
        assert_eq!(cfg.cap, 16);
        assert_eq!(cfg.buffer, FetchConfig::default().buffer);
    }

    #[test]
    fn index_keys_are_normalized() {
        assert_eq!(build_index_key("", "CURRENT"), ".holys3/CURRENT");
        assert_eq!(
            build_index_key("/root//path/", "/builds/1/footer.bin"),
            "root/path/.holys3/builds/1/footer.bin"
        );
        assert!(is_index_key("root/path", "root/path/.holys3/CURRENT"));
        assert!(!is_index_key("root/path", "root/path/file.txt"));
    }
}
