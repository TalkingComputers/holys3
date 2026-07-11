#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! S3 client, blob store, and corpus implementations.

mod cache;
mod client;
pub mod fetch;
mod sso;

use anyhow::Context;
use holys3_core::{
    decode_requested_body, BlobStore, Corpus, DocAddress, DocFetcher, DocId, DocumentBody,
    SourceObject,
};
use holys3_sigv4::Credentials;
use std::ops::Range;

pub use cache::{CacheKey, ObjectCache, ObjectCacheConfig};
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
    let region = std::env::var("AWS_REGION");
    let default_region = std::env::var("AWS_DEFAULT_REGION");
    let has_environment = !matches!(&region, Err(std::env::VarError::NotPresent))
        || !matches!(&default_region, Err(std::env::VarError::NotPresent));
    let profile_region = if has_environment {
        None
    } else {
        holys3_sigv4::region_from_config()?
    };
    read_region(region, default_region, profile_region)
}

fn read_region(
    region: Result<String, std::env::VarError>,
    default_region: Result<String, std::env::VarError>,
    profile_region: Option<String>,
) -> anyhow::Result<String> {
    let region = match region {
        Ok(region) => region,
        Err(std::env::VarError::NotPresent) => match default_region {
            Ok(region) => region,
            Err(std::env::VarError::NotPresent) => profile_region.context(
                "provide --region, set AWS_REGION/AWS_DEFAULT_REGION, or configure region in the active AWS profile",
            )?,
            Err(error) => return Err(error.into()),
        },
        Err(err) => return Err(err.into()),
    };
    anyhow::ensure!(!region.is_empty(), "AWS region is empty");
    Ok(region)
}

/// Credentials plus the instant they stop working (None = static, never).
pub struct ResolvedCredentials {
    pub credentials: Credentials,
    pub expires_at: Option<time::OffsetDateTime>,
}

/// Credential chain in standardized precedence order: env vars, static keys
/// in ~/.aws/credentials, then the active profile's IAM Identity Center config.
pub fn resolve_credentials() -> anyhow::Result<ResolvedCredentials> {
    if let Some(credentials) = holys3_sigv4::from_env()? {
        return Ok(ResolvedCredentials {
            credentials,
            expires_at: None,
        });
    }
    if let Some(credentials) = holys3_sigv4::resolve_static()? {
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

fn read_xml_element(
    reader: &mut quick_xml::Reader<&[u8]>,
    name: quick_xml::name::QName<'_>,
) -> anyhow::Result<String> {
    let text = reader.read_text(name)?;
    let decoded = text.xml10_content()?;
    Ok(quick_xml::escape::unescape(&decoded)?.into_owned())
}

/// Parse one `ListObjectsV2` XML page: returns (`objects`, `next_continuation_token`).
pub fn parse_list_v2(xml: &str) -> anyhow::Result<(Vec<ObjectMeta>, Option<String>)> {
    use quick_xml::events::Event;
    use quick_xml::Reader;
    let mut reader = Reader::from_str(xml);
    let mut objs = Vec::new();
    let mut next = None;
    let mut encoding = None;
    let (mut key, mut etag, mut size) = (None, None, None);
    let mut truncated: Option<bool> = None;
    let mut in_contents = false;
    let mut root_open = false;
    let mut root_closed = false;
    loop {
        match reader.read_event()? {
            Event::Start(e) => {
                let name = e.local_name();
                if name.as_ref() != b"ListBucketResult" {
                    anyhow::ensure!(root_open && !root_closed, "invalid ListObjectsV2 document");
                }
                match name.as_ref() {
                    b"ListBucketResult" => {
                        anyhow::ensure!(!root_open && !root_closed, "invalid ListObjectsV2 root");
                        root_open = true;
                    }
                    b"Contents" => {
                        anyhow::ensure!(
                            root_open && !in_contents,
                            "invalid ListObjectsV2 Contents"
                        );
                        in_contents = true;
                        key = None;
                        etag = None;
                        size = None;
                    }
                    b"Key" if in_contents => {
                        key = Some(read_xml_element(&mut reader, e.name())?);
                    }
                    b"ETag" if in_contents => {
                        etag = Some(read_xml_element(&mut reader, e.name())?);
                    }
                    b"Size" if in_contents => {
                        size = Some(
                            read_xml_element(&mut reader, e.name())?
                                .parse()
                                .context("invalid Size in ListObjectsV2")?,
                        );
                    }
                    b"NextContinuationToken" => {
                        next = Some(read_xml_element(&mut reader, e.name())?);
                    }
                    b"EncodingType" => {
                        encoding = Some(read_xml_element(&mut reader, e.name())?);
                    }
                    b"IsTruncated" => {
                        truncated = Some(
                            read_xml_element(&mut reader, e.name())?
                                .parse()
                                .context("invalid IsTruncated in ListObjectsV2")?,
                        );
                    }
                    _ => {}
                }
            }
            Event::End(e) => match e.local_name().as_ref() {
                b"Contents" => {
                    anyhow::ensure!(in_contents, "invalid ListObjectsV2 Contents");
                    in_contents = false;
                    objs.push(ObjectMeta {
                        key: key
                            .take()
                            .context("Contents missing Key in ListObjectsV2")?,
                        etag: etag
                            .take()
                            .context("Contents missing ETag in ListObjectsV2")?,
                        size: size
                            .take()
                            .context("Contents missing Size in ListObjectsV2")?,
                    });
                }
                b"ListBucketResult" => {
                    anyhow::ensure!(root_open && !in_contents, "invalid ListObjectsV2 root");
                    root_open = false;
                    root_closed = true;
                }
                _ => {}
            },
            Event::Eof => {
                anyhow::ensure!(
                    root_closed && !root_open,
                    "incomplete ListObjectsV2 response"
                );
                break;
            }
            Event::Empty(_) if !root_open || root_closed => {
                anyhow::bail!("invalid ListObjectsV2 document")
            }
            Event::Text(text) if !root_open || root_closed => {
                let bytes: &[u8] = text.as_ref();
                anyhow::ensure!(
                    bytes.iter().all(u8::is_ascii_whitespace),
                    "invalid ListObjectsV2 document"
                );
            }
            Event::CData(_) if !root_open || root_closed => {
                anyhow::bail!("invalid ListObjectsV2 document")
            }
            _ => {}
        }
    }
    match encoding.as_deref() {
        Some("url") => {
            for object in &mut objs {
                object.key = decode_list_key(&object.key)?;
            }
        }
        Some(other) => anyhow::bail!("unsupported ListObjectsV2 EncodingType {other}"),
        None => {}
    }
    let truncated = truncated.context("ListObjectsV2 response missing IsTruncated")?;
    anyhow::ensure!(
        !truncated || next.as_ref().is_some_and(|token| !token.is_empty()),
        "truncated ListObjectsV2 response missing NextContinuationToken"
    );
    anyhow::ensure!(
        truncated || next.is_none(),
        "untruncated ListObjectsV2 response included NextContinuationToken"
    );
    Ok((objs, next))
}

fn decode_list_key(value: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        valid_percent_encoding(value),
        "ListObjectsV2 returned malformed URL-encoded Key"
    );
    let value = if value.contains('+') {
        std::borrow::Cow::Owned(value.replace('+', " "))
    } else {
        std::borrow::Cow::Borrowed(value)
    };
    Ok(percent_encoding::percent_decode_str(&value)
        .decode_utf8()
        .context("ListObjectsV2 Key is not valid UTF-8")?
        .into_owned())
}

fn valid_percent_encoding(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if bytes
                .get(index + 1..index + 3)
                .is_none_or(|hex| !hex.iter().all(u8::is_ascii_hexdigit))
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

pub fn build_index_key(prefix: &str, name: &str) -> String {
    format!(
        "{}/{}",
        build_index_namespace(prefix),
        name.trim_start_matches('/')
    )
}

pub fn build_index_namespace(prefix: &str) -> String {
    if prefix.is_empty() {
        ".holys3".into()
    } else {
        format!("{}.holys3", list_prefix(prefix))
    }
}

/// `ListObjectsV2` prefix with directory semantics: "foo" must not match
/// sibling keys like "foobar/x".
pub fn list_prefix(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_owned()
    } else {
        format!("{prefix}/")
    }
}

pub fn is_index_key(prefix: &str, key: &str) -> bool {
    let namespace = build_index_namespace(prefix);
    key == namespace
        || key
            .strip_prefix(&namespace)
            .is_some_and(|relative| relative.starts_with('/'))
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

    fn put_file(&self, name: &str, path: &std::path::Path) -> anyhow::Result<()> {
        self.client
            .put_file(&self.bucket, &self.build_key(name), path)
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

    fn delete(&self, name: &str) -> anyhow::Result<()> {
        self.client.delete(&self.bucket, &self.build_key(name))
    }

    fn get_versioned(&self, name: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
        self.client
            .get_with_version(&self.bucket, &self.build_key(name))
    }

    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> anyhow::Result<bool> {
        self.client
            .put_if(&self.bucket, &self.build_key(name), bytes, expected)
    }
}

/// Corpus over a fixed S3 object list — the index BUILD side.
pub struct S3Corpus {
    client: S3Client,
    bucket: String,
    sources: Vec<SourceObject>,
}

impl S3Corpus {
    pub fn new(client: S3Client, bucket: String, listing: &[(String, String, u64)]) -> S3Corpus {
        let sources = listing
            .iter()
            .map(|(key, version, size)| SourceObject {
                key: key.clone(),
                version: version.clone(),
                encoded_size: *size,
            })
            .collect();
        S3Corpus {
            client,
            bucket,
            sources,
        }
    }

    fn fetch_body_batch(
        &self,
        sources: Range<usize>,
    ) -> anyhow::Result<Vec<(usize, DocumentBody)>> {
        let keys = sources
            .map(|idx| {
                Ok((
                    DocId::try_from(idx)?,
                    self.sources[idx].key.clone(),
                    self.sources[idx].version.clone(),
                    self.sources[idx].encoded_size,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut fetched = Vec::with_capacity(keys.len());
        self.client
            .get_each_bodies_if_match(&self.bucket, keys, &mut |idx, body| match body {
                Some(body) => {
                    fetched.push((idx as usize, body));
                    Ok(())
                }
                None => {
                    eprintln!(
                        "warning: s3://{}/{} vanished since listing; skipping",
                        self.bucket, self.sources[idx as usize].key
                    );
                    Ok(())
                }
            })?;
        Ok(fetched)
    }
}

impl Corpus for S3Corpus {
    fn sources(&self) -> &[SourceObject] {
        &self.sources
    }

    fn fetch(&self, idx: usize) -> anyhow::Result<bytes::Bytes> {
        let source = &self.sources[idx];
        self.client
            .get_if_match(&self.bucket, &source.key, &source.version)?
            .with_context(|| format!("s3://{}/{} not found", self.bucket, source.key))
    }

    /// Concurrent batch fetch. Objects deleted since listing (404) are
    /// skipped with a warning.
    fn fetch_many(&self, sources: Range<usize>) -> anyhow::Result<Vec<(usize, bytes::Bytes)>> {
        self.fetch_body_batch(sources)?
            .into_iter()
            .map(|(idx, body)| Ok((idx, body.into_bytes()?)))
            .collect()
    }

    fn fetch_bodies(&self, sources: Range<usize>) -> anyhow::Result<Vec<(usize, DocumentBody)>> {
        self.fetch_body_batch(sources)
    }
}

/// Fetches objects by key for search verification — no doc table at all.
pub struct S3Fetcher {
    client: S3Client,
    bucket: String,
    endpoint: String,
    cache: Option<ObjectCache>,
}

impl S3Fetcher {
    pub fn new(client: S3Client, bucket: String) -> S3Fetcher {
        let endpoint = client.endpoint_identity();
        S3Fetcher {
            client,
            bucket,
            endpoint,
            cache: None,
        }
    }

    pub fn with_cache(
        client: S3Client,
        bucket: String,
        config: ObjectCacheConfig,
    ) -> anyhow::Result<S3Fetcher> {
        let endpoint = client.endpoint_identity();
        Ok(S3Fetcher {
            client,
            bucket,
            endpoint,
            cache: Some(ObjectCache::open(&config.root, config.cap_bytes)?),
        })
    }
}

impl DocFetcher for S3Fetcher {
    /// Concurrent streaming fetch. Objects deleted since indexing (404) are
    /// skipped with a warning — the index entry is stale, not the search.
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, DocumentBody) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut grouped = std::collections::BTreeMap::new();
        for (idx, document) in documents.iter().enumerate() {
            grouped
                .entry((
                    document.source_key.clone(),
                    document.source_version.clone(),
                    document.encoded_size,
                ))
                .or_insert_with(Vec::new)
                .push((idx, document.member_path.clone()));
        }
        let groups = grouped.into_iter().collect::<Vec<_>>();
        let mut indexed_keys = Vec::new();
        if let Some(cache) = &self.cache {
            let cache_keys = groups
                .iter()
                .map(|((key, version, _), _)| CacheKey {
                    endpoint: &self.endpoint,
                    bucket: &self.bucket,
                    key,
                    version,
                })
                .collect::<Vec<_>>();
            cache.get_each(
                &cache_keys,
                self.client.max_concurrency(),
                &mut |idx, body| {
                    let ((key, version, encoded_size), requests) = &groups[idx];
                    match body {
                        Some(body) => decode_requested_body(key, requests, body, consume),
                        None => {
                            indexed_keys.push((
                                DocId::try_from(idx)?,
                                key.clone(),
                                version.clone(),
                                *encoded_size,
                            ));
                            Ok(())
                        }
                    }
                },
            )?;
        } else {
            indexed_keys = groups
                .iter()
                .enumerate()
                .map(|(idx, ((key, version, encoded_size), _))| {
                    Ok((
                        DocId::try_from(idx)?,
                        key.clone(),
                        version.clone(),
                        *encoded_size,
                    ))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
        }
        self.client.get_each_bodies_if_match(
            &self.bucket,
            indexed_keys,
            &mut |idx, body| match body {
                Some(body) => {
                    let ((key, version, _), requests) = &groups[idx as usize];
                    let cached = self.cache.as_ref().map(|_| body.try_clone()).transpose()?;
                    decode_requested_body(key, requests, body, consume)?;
                    if let (Some(cache), Some(cached)) = (&self.cache, cached) {
                        cache.put_body(
                            &CacheKey {
                                endpoint: &self.endpoint,
                                bucket: &self.bucket,
                                key,
                                version,
                            },
                            cached,
                        )?;
                    }
                    Ok(())
                }
                None => {
                    let ((key, _, _), _) = &groups[idx as usize];
                    eprintln!(
                        "warning: s3://{}/{} vanished since indexing; skipping",
                        self.bucket, key
                    );
                    Ok(())
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holys3_core::{DocAddress, DocFetcher, SourceEncoding};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn start_body_server(body: Vec<u8>) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 8192];
            let read = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
            String::from_utf8_lossy(&request[..read]).into_owned()
        });
        (format!("http://{address}"), thread)
    }

    #[test]
    fn parse_two_objects_with_token() {
        let xml = r#"<?xml version="1.0"?>
        <ListBucketResult>
          <Contents><Key>a.txt</Key><Size>10</Size><ETag>"abc"</ETag></Contents>
          <Contents><Key>b/c.log</Key><Size>20</Size><ETag>"def"</ETag></Contents>
          <IsTruncated>true</IsTruncated>
          <NextContinuationToken>TOK</NextContinuationToken>
        </ListBucketResult>"#;
        let (objs, next) = parse_list_v2(xml).unwrap();
        assert_eq!(
            objs,
            vec![
                ObjectMeta {
                    key: "a.txt".into(),
                    etag: "\"abc\"".into(),
                    size: 10
                },
                ObjectMeta {
                    key: "b/c.log".into(),
                    etag: "\"def\"".into(),
                    size: 20
                },
            ]
        );
        assert_eq!(next.as_deref(), Some("TOK"));
    }

    #[test]
    fn grouped_archive_fetches_once_and_warm_cache_avoids_origin() {
        let body = holys3_core::testutil::encode::zip(&[("a.log", b"alpha"), ("b.log", b"beta")]);
        let (endpoint, server) = start_body_server(body.clone());
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
        let documents = [
            DocAddress {
                display_key: "bundle.zip!/a.log".into(),
                source_key: "bundle.zip".into(),
                source_version: "\"etag\"".into(),
                encoded_size: body.len() as u64,
                encoding: SourceEncoding::Zip,
                member_path: Some("a.log".into()),
            },
            DocAddress {
                display_key: "bundle.zip!/b.log".into(),
                source_key: "bundle.zip".into(),
                source_version: "\"etag\"".into(),
                encoded_size: body.len() as u64,
                encoding: SourceEncoding::Zip,
                member_path: Some("b.log".into()),
            },
        ];
        let cache = tempfile::tempdir().unwrap();
        let config = ObjectCacheConfig {
            root: cache.path().to_path_buf(),
            cap_bytes: 1024 * 1024,
        };
        let fetcher =
            S3Fetcher::with_cache(client.clone(), "bucket".into(), config.clone()).unwrap();
        let mut first = Vec::new();
        fetcher
            .fetch_each(&documents, &mut |idx, body| {
                first.push((idx, body.into_bytes()?));
                Ok(())
            })
            .unwrap();
        first.sort_unstable_by_key(|(idx, _)| *idx);
        assert_eq!(
            first,
            [
                (0, bytes::Bytes::from_static(b"alpha")),
                (1, bytes::Bytes::from_static(b"beta"))
            ]
        );
        let request = server.join().unwrap().to_ascii_lowercase();
        assert!(request.contains("if-match: \"etag\"\r\n"), "{request}");

        let fetcher = S3Fetcher::with_cache(client, "bucket".into(), config).unwrap();
        let mut warm = Vec::new();
        fetcher
            .fetch_each(&documents, &mut |idx, body| {
                warm.push((idx, body.into_bytes()?));
                Ok(())
            })
            .unwrap();
        warm.sort_unstable_by_key(|(idx, _)| *idx);
        assert_eq!(warm, first);
    }

    #[test]
    fn parse_list_v2_unescapes_keys_and_tokens() {
        let xml = r#"<ListBucketResult><Contents><Key>a&amp;b</Key><Size>1</Size><ETag>&quot;abc&quot;</ETag></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>x&amp;y</NextContinuationToken></ListBucketResult>"#;
        let (objects, next) = parse_list_v2(xml).unwrap();
        assert_eq!(objects[0].key, "a&b");
        assert_eq!(objects[0].etag, "\"abc\"");
        assert_eq!(next.as_deref(), Some("x&y"));
    }

    #[test]
    fn parse_list_v2_decodes_url_encoded_keys_only() {
        let xml = r#"<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>logs%2Fspace+and%2Bplus%2F%F0%9F%92%BE%25100.log</Key><Size>1</Size><ETag>&quot;abc&quot;</ETag></Contents><IsTruncated>true</IsTruncated><NextContinuationToken>x%2Fy+z</NextContinuationToken></ListBucketResult>"#;
        let (objects, next) = parse_list_v2(xml).unwrap();
        assert_eq!(objects[0].key, "logs/space and+plus/💾%100.log");
        assert_eq!(next.as_deref(), Some("x%2Fy+z"));

        let malformed = r#"<ListBucketResult><EncodingType>url</EncodingType><Contents><Key>bad%2</Key><Size>1</Size><ETag>&quot;abc&quot;</ETag></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        assert!(parse_list_v2(malformed).is_err());
    }

    #[test]
    fn parse_list_v2_rejects_invalid_size() {
        let xml = r#"<ListBucketResult><Contents><Key>a.txt</Key><Size>nope</Size><ETag>"abc"</ETag></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        let err = parse_list_v2(xml).unwrap_err();
        assert!(err.to_string().contains("invalid Size in ListObjectsV2"));
    }

    #[test]
    fn parse_list_v2_rejects_missing_object_fields() {
        for (xml, field) in [
            (
                r#"<ListBucketResult><Contents><Size>1</Size><ETag>"abc"</ETag></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#,
                "Key",
            ),
            (
                r#"<ListBucketResult><Contents><Key>a</Key><ETag>"abc"</ETag></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#,
                "Size",
            ),
            (
                r#"<ListBucketResult><Contents><Key>a</Key><Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>"#,
                "ETag",
            ),
        ] {
            let err = parse_list_v2(xml).unwrap_err();
            assert!(err.to_string().contains(field), "{err:#}");
        }
    }

    #[test]
    fn parse_list_v2_requires_token_for_truncated_page() {
        let xml = r#"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"#;
        let err = parse_list_v2(xml).unwrap_err();
        assert!(err.to_string().contains("NextContinuationToken"));
    }

    #[test]
    fn parse_list_v2_rejects_incomplete_documents() {
        for xml in [
            r#"<ListBucketResult><IsTruncated>false</IsTruncated>"#,
            r#"<ListBucketResult><Contents><Key>a</Key><Size>1</Size><ETag>e</ETag>"#,
            r#"<IsTruncated>false</IsTruncated>"#,
            r#"<ListBucketResult></ListBucketResult><IsTruncated>false</IsTruncated>"#,
            r#"<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult><Other/>"#,
        ] {
            assert!(parse_list_v2(xml).is_err(), "{xml}");
        }
    }

    #[test]
    fn build_fetch_config_caps_initial_concurrency() {
        let cfg = build_fetch_config(16);
        assert_eq!(cfg.start, 16);
        assert_eq!(cfg.cap, 16);
    }

    #[test]
    fn region_uses_sdk_then_cli_environment_order() {
        let missing = || Err(std::env::VarError::NotPresent);
        assert_eq!(
            read_region(
                Ok("us-east-2".into()),
                Ok("us-west-1".into()),
                Some("ca-central-1".into())
            )
            .unwrap(),
            "us-east-2"
        );
        assert_eq!(
            read_region(
                missing(),
                Ok("us-west-1".into()),
                Some("ca-central-1".into())
            )
            .unwrap(),
            "us-west-1"
        );
        assert!(read_region(missing(), missing(), None).is_err());
        assert!(read_region(Ok(String::new()), missing(), None).is_err());
    }

    #[test]
    fn region_uses_profile_after_environment() {
        let missing = || Err(std::env::VarError::NotPresent);
        assert_eq!(
            read_region(missing(), missing(), Some("ca-central-1".into())).unwrap(),
            "ca-central-1"
        );
    }

    #[test]
    fn index_keys_preserve_prefix() {
        assert_eq!(build_index_key("", "CURRENT"), ".holys3/CURRENT");
        assert_eq!(
            build_index_key("root//path/", "/builds/1/footer.bin"),
            "root//path/.holys3/builds/1/footer.bin"
        );
        assert!(is_index_key("root/path", "root/path/.holys3/CURRENT"));
        assert!(!is_index_key(
            "root/path",
            "root/path/child/.holys3/segments.bin"
        ));
        assert!(!is_index_key("root/path", "root/path/.holys3-data/log"));
        assert!(!is_index_key("root/path", "root/path/file.txt"));
    }

    #[test]
    fn list_prefix_uses_directory_semantics() {
        assert_eq!(list_prefix(""), "");
        assert_eq!(list_prefix("foo"), "foo/");
        assert_eq!(list_prefix("foo/"), "foo/");
        assert_eq!(list_prefix("/a//b/"), "/a//b/");
    }
}
