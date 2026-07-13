#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! S3 client, blob store, and corpus implementations.

mod cache;
mod client;
pub mod fetch;

use anyhow::Context;
use bytes::Bytes;
use holys3_core::{
    decode_requested_body, BlobStore, Corpus, DocAddress, DocFetcher, DocId, DocumentBody,
    SourceObject,
};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub etag: String,
    pub size: u64,
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

/// Index blob storage under an S3 key prefix.
pub struct S3BlobStore {
    client: S3Client,
    bucket: String,
    root: String,
}

impl S3BlobStore {
    pub fn new(client: S3Client, bucket: String, prefix: String) -> S3BlobStore {
        Self::at(client, bucket, build_index_namespace(&prefix))
    }

    pub fn at(client: S3Client, bucket: String, root: String) -> S3BlobStore {
        S3BlobStore {
            client,
            bucket,
            root: root.trim_matches('/').to_owned(),
        }
    }

    fn build_key(&self, name: &str) -> String {
        let name = name.trim_start_matches('/');
        if self.root.is_empty() {
            name.to_owned()
        } else {
            format!("{}/{name}", self.root)
        }
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

    fn get_file(&self, name: &str, output: &mut std::fs::File, len: u64) -> anyhow::Result<()> {
        self.client
            .get_file(&self.bucket, &self.build_key(name), output, len)?
            .then_some(())
            .with_context(|| self.blob_context(name))
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        self.client
            .get_range(&self.bucket, &self.build_key(name), start, len)?
            .with_context(|| self.blob_context(name))
    }

    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> anyhow::Result<Vec<Bytes>> {
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

    fn fetch(&self, idx: usize) -> anyhow::Result<Bytes> {
        let source = &self.sources[idx];
        self.client
            .get_if_match(&self.bucket, &source.key, &source.version)?
            .with_context(|| format!("s3://{}/{} not found", self.bucket, source.key))
    }

    /// Concurrent batch fetch. Objects deleted since listing (404) are
    /// skipped with a warning.
    fn fetch_many(&self, sources: Range<usize>) -> anyhow::Result<Vec<(usize, Bytes)>> {
        self.fetch_body_batch(sources)?
            .into_iter()
            .map(|(idx, body)| Ok((idx, body.into_bytes()?)))
            .collect()
    }

    fn fetch_bodies(&self, sources: Range<usize>) -> anyhow::Result<Vec<(usize, DocumentBody)>> {
        self.fetch_body_batch(sources)
    }
}

/// Direct S3-source candidate fetcher for library callers and tests.
/// The product CLI reads canonical bodies from index snapshot packs instead.
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
    /// Concurrent streaming fetch. Source objects deleted since indexing
    /// are skipped with a warning.
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
    fn grouped_archive_fetches_once_and_warm_cache_avoids_origin() {
        let body = holys3_core::testutil::encode::zip(&[("a.log", b"alpha"), ("b.log", b"beta")]);
        let (endpoint, server) = start_body_server(body.clone());
        let client = S3Client::connect_static(
            "us-east-1".into(),
            "test".into(),
            "test".into(),
            None,
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
                index: None,
            },
            DocAddress {
                display_key: "bundle.zip!/b.log".into(),
                source_key: "bundle.zip".into(),
                source_version: "\"etag\"".into(),
                encoded_size: body.len() as u64,
                encoding: SourceEncoding::Zip,
                member_path: Some("b.log".into()),
                index: None,
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
                (0, Bytes::from_static(b"alpha")),
                (1, Bytes::from_static(b"beta"))
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
    fn build_fetch_config_caps_initial_concurrency() {
        let cfg = build_fetch_config(16);
        assert_eq!(cfg.start, 16);
        assert_eq!(cfg.cap, 16);
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

    #[test]
    fn explicit_index_root_preserves_blob_name_semantics() {
        let client = S3Client::connect_static(
            "us-east-1".into(),
            "test".into(),
            "test".into(),
            None,
            Some("http://127.0.0.1:9000".into()),
            FetchConfig::default(),
        )
        .unwrap();
        let store = S3BlobStore::at(client, "bucket".into(), "/index/".into());
        assert_eq!(store.build_key("/segments.bin"), "index/segments.bin");
    }
}
