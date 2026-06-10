#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! S3 client, blob store, and corpus implementations.

mod client;
pub mod fetch;
mod sso;

use anyhow::Context;
use holys3_core::{BlobStore, Corpus, DocFetcher, DocId};
use holys3_sigv4::Credentials;

pub use client::S3Client;
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

/// Credentials plus the instant they stop working (None = static, never).
pub struct ResolvedCredentials {
    pub credentials: Credentials,
    pub expires_at: Option<time::OffsetDateTime>,
}

/// Credential chain in botocore precedence order: env vars, then the active
/// profile's IAM Identity Center (SSO) config, then static keys in
/// ~/.aws/credentials.
pub fn resolve_credentials() -> anyhow::Result<ResolvedCredentials> {
    if let Some(credentials) = holys3_sigv4::from_env() {
        return Ok(ResolvedCredentials {
            credentials,
            expires_at: None,
        });
    }
    if let Some(profile) = holys3_sigv4::sso_profile()? {
        let (credentials, expires_at) = sso::role_credentials(&profile)?;
        return Ok(ResolvedCredentials {
            credentials,
            expires_at: Some(expires_at),
        });
    }
    if let Some(credentials) = holys3_sigv4::resolve_static()? {
        return Ok(ResolvedCredentials {
            credentials,
            expires_at: None,
        });
    }
    let profile = holys3_sigv4::profile_name()?;
    anyhow::bail!(
        "no AWS credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, add profile `{profile}` \
         to ~/.aws/credentials, or configure SSO for it in ~/.aws/config"
    )
}

pub fn s3_client_from_env(
    region: &str,
    endpoint: Option<String>,
    cfg: FetchConfig,
) -> anyhow::Result<S3Client> {
    let resolved = resolve_credentials()?;
    let client = S3Client::new(region.to_owned(), resolved.credentials, endpoint, cfg)?;
    if let Some(expires_at) = resolved.expires_at {
        client.enable_refresh(expires_at, || {
            let resolved = resolve_credentials()?;
            Ok((resolved.credentials, resolved.expires_at))
        });
    }
    Ok(client)
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

/// `ListObjectsV2` prefix with directory semantics: "foo" must not match
/// sibling keys like "foobar/x".
pub fn list_prefix(prefix: &str) -> String {
    let normalized = normalize_prefix(prefix);
    if normalized.is_empty() {
        normalized
    } else {
        format!("{normalized}/")
    }
}

pub fn is_index_key(prefix: &str, key: &str) -> bool {
    key.starts_with(&format!("{}/", build_index_namespace(prefix)))
}

/// Index blob storage under `<prefix>/.holys3/` in the bucket.
pub struct S3BlobStore {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl S3BlobStore {
    pub fn new(client: S3Client, bucket: String, prefix: String) -> S3BlobStore {
        S3BlobStore {
            client,
            bucket,
            prefix,
        }
    }

    fn build_key(&self, name: &str) -> String {
        build_index_key(&self.prefix, name)
    }

    fn blob_context(&self, name: &str) -> String {
        format!(
            "index blob s3://{}/{} not found — run `holys3 index` first",
            self.bucket,
            self.build_key(name)
        )
    }
}

impl BlobStore for S3BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.client.put(&self.bucket, &self.build_key(name), bytes)
    }

    fn get(&self, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.client.get(&self.bucket, &self.build_key(name))
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        self.client
            .get_range(&self.bucket, &self.build_key(name), start, len)?
            .with_context(|| self.blob_context(name))
    }

    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> anyhow::Result<Vec<Vec<u8>>> {
        self.client
            .get_ranges(&self.bucket, &self.build_key(name), ranges)?
            .with_context(|| self.blob_context(name))
    }
}

/// Corpus over a fixed S3 object list — the index BUILD side.
pub struct S3Corpus {
    client: S3Client,
    bucket: String,
    docs: Vec<(DocId, String)>,
}

impl S3Corpus {
    pub fn new(client: S3Client, bucket: String, objects: &[ObjectMeta]) -> S3Corpus {
        let docs = objects
            .iter()
            .enumerate()
            .map(|(i, o)| (i as DocId, o.key.clone()))
            .collect();
        S3Corpus {
            client,
            bucket,
            docs,
        }
    }
}

impl Corpus for S3Corpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn fetch(&self, id: DocId) -> anyhow::Result<Vec<u8>> {
        let key = &self.docs[id as usize].1;
        self.client
            .get(&self.bucket, key)?
            .with_context(|| format!("s3://{}/{key} not found", self.bucket))
    }

    /// Concurrent batch fetch. Objects deleted since listing (404) are
    /// skipped with a warning.
    fn fetch_many(&self, ids: &[DocId]) -> anyhow::Result<Vec<(DocId, Vec<u8>)>> {
        let keys = ids
            .iter()
            .map(|&id| (id, self.docs[id as usize].1.clone()))
            .collect::<Vec<_>>();
        let mut docs = Vec::with_capacity(keys.len());
        self.client
            .get_each(&self.bucket, keys, &mut |id, bytes| match bytes {
                Some(bytes) => {
                    docs.push((id, bytes));
                    Ok(())
                }
                None => {
                    eprintln!(
                        "warning: s3://{}/{} vanished since listing; skipping",
                        self.bucket, self.docs[id as usize].1
                    );
                    Ok(())
                }
            })?;
        Ok(docs)
    }
}

/// Fetches objects by key for search verification — no doc table at all.
pub struct S3Fetcher {
    client: S3Client,
    bucket: String,
}

impl S3Fetcher {
    pub fn new(client: S3Client, bucket: String) -> S3Fetcher {
        S3Fetcher { client, bucket }
    }
}

impl DocFetcher for S3Fetcher {
    /// Concurrent streaming fetch. Objects deleted since indexing (404) are
    /// skipped with a warning — the index entry is stale, not the search.
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let indexed_keys = keys
            .iter()
            .enumerate()
            .map(|(idx, key)| (idx as DocId, key.clone()))
            .collect::<Vec<_>>();
        self.client
            .get_each(&self.bucket, indexed_keys, &mut |idx, bytes| match bytes {
                Some(bytes) => consume(idx as usize, bytes),
                None => {
                    eprintln!(
                        "warning: s3://{}/{} vanished since indexing; skipping",
                        self.bucket, keys[idx as usize]
                    );
                    Ok(())
                }
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

    #[test]
    fn list_prefix_uses_directory_semantics() {
        assert_eq!(list_prefix(""), "");
        assert_eq!(list_prefix("foo"), "foo/");
        assert_eq!(list_prefix("foo/"), "foo/");
        assert_eq!(list_prefix("/a//b/"), "a/b/");
    }
}
