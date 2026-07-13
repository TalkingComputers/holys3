use crate::codec::{decode_source, DecodeSink, DocumentBody, LogicalDocumentMeta, DECODE_LIMITS};
use crate::grep::has_line_match;
use anyhow::Result as AnyhowResult;
use bytes::Bytes;
use fs4::FileExt;
use std::io::{Seek, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceObject {
    pub key: String,
    pub version: String,
    pub encoded_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAddress {
    pub segment: u32,
    pub document: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocAddress {
    pub display_key: String,
    pub source_key: String,
    pub source_version: String,
    pub encoded_size: u64,
    pub encoding: crate::SourceEncoding,
    pub member_path: Option<String>,
    pub index: Option<IndexAddress>,
}

#[derive(Debug)]
pub struct StaleSource {
    pub key: String,
    pub expected: String,
}

impl std::fmt::Display for StaleSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "indexed source changed: {} expected version {}",
            self.key, self.expected
        )
    }
}

impl std::error::Error for StaleSource {}

/// A fully enumerable source used to build an index.
/// Implemented by the local benchmark/test adapter and the S3 product adapter.
pub trait Corpus {
    /// All sources; a source's id is its position in this slice.
    fn sources(&self) -> &[SourceObject];
    /// Fetch the full bytes of one source by position.
    fn fetch(&self, idx: usize) -> AnyhowResult<Bytes>;
    /// Fetch a contiguous run of sources concurrently. Result order is NOT
    /// guaranteed; each item carries its position. Implementations may
    /// return fewer sources than requested when an object vanished between
    /// indexing and fetching. Default = sequential, fail-fast.
    fn fetch_many(&self, docs: Range<usize>) -> AnyhowResult<Vec<(usize, Bytes)>> {
        docs.map(|idx| Ok((idx, self.fetch(idx)?))).collect()
    }
    fn fetch_bodies(&self, docs: Range<usize>) -> AnyhowResult<Vec<(usize, DocumentBody)>> {
        self.fetch_many(docs)?
            .into_iter()
            .map(|(idx, bytes)| Ok((idx, DocumentBody::from_bytes(bytes))))
            .collect()
    }
}

/// Fetches canonical bodies for candidate-document verification.
/// `consume` receives the index into `documents` plus the body, as
/// fetches complete (order NOT guaranteed). Implementations may fetch
/// concurrently; the first `consume` error aborts the remaining fetches.
pub trait DocFetcher {
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, DocumentBody) -> AnyhowResult<()>,
    ) -> AnyhowResult<()>;
}

pub trait BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()>;
    fn put_file(&self, name: &str, path: &Path) -> AnyhowResult<()>;
    fn get_file(&self, name: &str, file: &mut std::fs::File, len: u64) -> AnyhowResult<()> {
        const PART_SIZE: u64 = 8 * 1024 * 1024;
        const PARTS_PER_BATCH: usize = 2;

        file.set_len(0)?;
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut start = 0u64;
        while start < len {
            let mut ranges = Vec::with_capacity(PARTS_PER_BATCH);
            while ranges.len() < PARTS_PER_BATCH && start < len {
                let part_len = PART_SIZE.min(len - start);
                ranges.push((start, part_len));
                start += part_len;
            }
            let parts = self.get_ranges(name, &ranges)?;
            anyhow::ensure!(
                parts.len() == ranges.len(),
                "get_ranges returned {} blocks for {} ranges",
                parts.len(),
                ranges.len()
            );
            for ((_, expected), part) in ranges.into_iter().zip(parts) {
                anyhow::ensure!(
                    part.len() as u64 == expected,
                    "blob range is {} bytes, expected {expected}",
                    part.len()
                );
                file.write_all(&part)?;
            }
        }
        file.flush()?;
        Ok(())
    }
    /// `Ok(None)` = blob does not exist. Transient store failures are `Err`
    /// so callers never mistake an outage for an empty store.
    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>>;
    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>>;
    /// Fetch many byte ranges of one blob, preserving order. Implementations
    /// may fetch concurrently. Default = sequential.
    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> AnyhowResult<Vec<Bytes>> {
        ranges
            .iter()
            .map(|&(start, len)| self.get_range(name, start, len).map(Bytes::from))
            .collect()
    }
    /// Remove a blob; deleting an absent blob is not an error.
    fn delete(&self, name: &str) -> AnyhowResult<()>;
    /// Fetch a blob plus an opaque version token for `put_if`.
    fn get_versioned(&self, name: &str) -> AnyhowResult<Option<(Vec<u8>, String)>>;
    /// Compare-and-swap write: succeeds only if the blob's current version
    /// matches `expected` (`None` = the blob must not exist). Returns false
    /// when another writer won the race — never silently overwrites.
    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> AnyhowResult<bool>;
}

/// Version token for stores without native versions: content-derived.
pub fn content_version(bytes: &[u8]) -> String {
    format!("{}-{}", blake3::hash(bytes).to_hex(), bytes.len())
}

pub struct LocalBlobStore {
    root: PathBuf,
}

impl LocalBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> LocalBlobStore {
        LocalBlobStore { root: root.into() }
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> AnyhowResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("blob path has no parent: {}", path.display()))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(bytes)?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|err| err.error)?;
    Ok(())
}

impl BlobStore for LocalBlobStore {
    fn delete(&self, name: &str) -> AnyhowResult<()> {
        match std::fs::remove_file(self.root.join(name)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn get_versioned(&self, name: &str) -> AnyhowResult<Option<(Vec<u8>, String)>> {
        Ok(self.get(name)?.map(|bytes| {
            let version = content_version(&bytes);
            (bytes, version)
        }))
    }

    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> AnyhowResult<bool> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let lock_path = path.with_extension("lock");
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        FileExt::lock(&lock)?;
        let current = match std::fs::read(&path) {
            Ok(bytes) => Some(content_version(&bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(err.into()),
        };
        if current.as_deref() != expected {
            return Ok(false);
        }
        write_atomic(&path, bytes)?;
        Ok(true)
    }

    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        write_atomic(&path, bytes)?;
        Ok(())
    }

    fn put_file(&self, name: &str, source: &Path) -> AnyhowResult<()> {
        let path = self.root.join(name);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("blob path has no parent: {}", path.display()))?;
        std::fs::create_dir_all(parent)?;
        let mut input = std::fs::File::open(source)?;
        let mut temp = tempfile::NamedTempFile::new_in(parent)?;
        std::io::copy(&mut input, &mut temp)?;
        temp.as_file().sync_all()?;
        temp.persist(path).map_err(|err| err.error)?;
        Ok(())
    }

    fn get_file(&self, name: &str, output: &mut std::fs::File, len: u64) -> AnyhowResult<()> {
        let mut input = std::fs::File::open(self.root.join(name))?;
        output.set_len(0)?;
        output.seek(std::io::SeekFrom::Start(0))?;
        let copied = std::io::copy(&mut input, output)?;
        anyhow::ensure!(copied == len, "blob is {copied} bytes, expected {len}");
        output.flush()?;
        Ok(())
    }

    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>> {
        match std::fs::read(self.root.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = std::fs::File::open(self.root.join(name))?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; usize::try_from(len)?];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

/// Oracle: keys of docs containing at least one matching line, sorted. The
/// differential ground truth.
pub fn scan_matching_docs(
    corpus: &dyn Corpus,
    re: &regex::bytes::Regex,
) -> AnyhowResult<Vec<String>> {
    struct ScanSink<'a> {
        re: &'a regex::bytes::Regex,
        key: String,
        bytes: Vec<u8>,
        hits: Vec<String>,
    }

    impl DecodeSink for ScanSink<'_> {
        fn begin(&mut self, document: &LogicalDocumentMeta) -> AnyhowResult<()> {
            self.key.clone_from(&document.display_key);
            self.bytes.clear();
            Ok(())
        }

        fn write(&mut self, bytes: &[u8]) -> AnyhowResult<()> {
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }

        fn finish(&mut self) -> AnyhowResult<()> {
            if has_line_match(&self.bytes, self.re) {
                self.hits.push(self.key.clone());
            }
            Ok(())
        }
    }

    let mut sink = ScanSink {
        re,
        key: String::new(),
        bytes: Vec::new(),
        hits: Vec::new(),
    };
    for (idx, source) in corpus.sources().iter().enumerate() {
        let bytes = corpus.fetch(idx)?;
        decode_source(&source.key, bytes, DECODE_LIMITS, &mut sink)?;
    }
    sink.hits.sort_unstable();
    Ok(sink.hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemCorpus;
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn source_and_document_contracts_are_complete() {
        let source = SourceObject {
            key: "logs/bundle.zip".to_owned(),
            version: "etag-1".to_owned(),
            encoded_size: 1024,
        };
        let document = DocAddress {
            display_key: "logs/bundle.zip!/app.log".to_owned(),
            source_key: source.key.clone(),
            source_version: source.version.clone(),
            encoded_size: source.encoded_size,
            encoding: crate::SourceEncoding::Zip,
            member_path: Some("app.log".to_owned()),
            index: None,
        };
        assert_eq!(document.source_key, source.key);
        assert_eq!(document.source_version, source.version);
        assert_eq!(document.encoded_size, source.encoded_size);
    }

    #[test]
    fn scan_finds_matching_docs() {
        let c = MemCorpus::new(
            vec!["a".into(), "b".into()],
            vec![b"hello world".to_vec(), b"nothing here".to_vec()],
        );
        let re = regex::bytes::Regex::new("world").unwrap();
        assert_eq!(scan_matching_docs(&c, &re).unwrap(), vec!["a".to_owned()]);
    }

    #[test]
    fn scan_finds_archive_members() {
        let c = MemCorpus::new(
            vec!["bundle.zip".into()],
            vec![crate::testutil::encode::zip(&[
                ("a.log", b"hello world"),
                ("b.log", b"nothing"),
            ])],
        );
        let re = regex::bytes::Regex::new("world").unwrap();
        assert_eq!(
            scan_matching_docs(&c, &re).unwrap(),
            vec!["bundle.zip!/a.log".to_owned()]
        );
    }

    #[test]
    fn local_blob_store_round_trips_ranges() -> AnyhowResult<()> {
        let root = std::env::temp_dir().join(format!(
            "holys3-core-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let store = LocalBlobStore::new(&root);
        store.put("builds/a/postings.bin", b"abcdef")?;
        assert_eq!(
            store.get("builds/a/postings.bin")?.as_deref(),
            Some(b"abcdef".as_slice())
        );
        assert_eq!(store.get("missing")?, None);
        assert_eq!(store.get_range("builds/a/postings.bin", 2, 3)?, b"cde");
        let ranges: Vec<Bytes> = store.get_ranges("builds/a/postings.bin", &[(0, 2), (4, 2)])?;
        assert_eq!(
            ranges,
            [Bytes::from_static(b"ab"), Bytes::from_static(b"ef")]
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn local_blob_store_puts_files_atomically() -> AnyhowResult<()> {
        use std::io::Read;

        let root = tempfile::tempdir()?;
        let mut source = tempfile::NamedTempFile::new()?;
        source.write_all(b"file-backed index blob")?;
        source.as_file().sync_all()?;
        let store = LocalBlobStore::new(root.path());
        store.put_file("segments/a/postings.bin", source.path())?;
        assert_eq!(
            store.get("segments/a/postings.bin")?.as_deref(),
            Some(b"file-backed index blob".as_slice())
        );
        let mut output = tempfile::tempfile()?;
        store.get_file("segments/a/postings.bin", &mut output, 22)?;
        output.seek(std::io::SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        output.read_to_end(&mut bytes)?;
        assert_eq!(bytes, b"file-backed index blob");
        Ok(())
    }

    #[test]
    fn doc_fetcher_resolves_keys() {
        use crate::testutil::MemCorpus;
        use crate::DocFetcher;
        let c = MemCorpus::new(
            vec!["a".into(), "b".into()],
            vec![b"one".to_vec(), b"two".to_vec()],
        );
        let documents = vec![
            DocAddress {
                display_key: "b".into(),
                source_key: "b".into(),
                source_version: content_version(b"two"),
                encoded_size: 3,
                encoding: crate::SourceEncoding::Raw,
                member_path: None,
                index: None,
            },
            DocAddress {
                display_key: "a".into(),
                source_key: "a".into(),
                source_version: content_version(b"one"),
                encoded_size: 3,
                encoding: crate::SourceEncoding::Raw,
                member_path: None,
                index: None,
            },
        ];
        let mut seen = Vec::new();
        c.fetch_each(&documents, &mut |idx, body| {
            seen.push((idx, body.into_bytes()?));
            Ok(())
        })
        .unwrap();
        seen.sort_unstable_by_key(|(idx, _)| *idx);
        assert_eq!(
            seen,
            vec![
                (0, Bytes::from_static(b"two")),
                (1, Bytes::from_static(b"one"))
            ]
        );
    }

    #[test]
    fn doc_fetcher_rejects_stale_versions() {
        let corpus = MemCorpus::new(vec!["a".into()], vec![b"current".to_vec()]);
        let documents = vec![DocAddress {
            display_key: "a".into(),
            source_key: "a".into(),
            source_version: "stale".into(),
            encoded_size: 7,
            encoding: crate::SourceEncoding::Raw,
            member_path: None,
            index: None,
        }];
        let error = corpus
            .fetch_each(&documents, &mut |_, _| Ok(()))
            .unwrap_err();
        assert!(error.is::<StaleSource>(), "{error:#}");
    }

    #[test]
    fn fetch_many_aborts_on_first_error() {
        struct BrokenCorpus {
            sources: Vec<SourceObject>,
        }

        impl Corpus for BrokenCorpus {
            fn sources(&self) -> &[SourceObject] {
                &self.sources
            }

            fn fetch(&self, idx: usize) -> AnyhowResult<Bytes> {
                if idx == 1 {
                    anyhow::bail!("broken");
                }
                Ok(Bytes::from_static(b"ok"))
            }
        }

        let corpus = BrokenCorpus {
            sources: vec![
                SourceObject {
                    key: "a".into(),
                    version: "a-1".into(),
                    encoded_size: 2,
                },
                SourceObject {
                    key: "b".into(),
                    version: "b-1".into(),
                    encoded_size: 2,
                },
            ],
        };
        assert!(corpus.fetch_many(0..2).is_err());
    }

    #[test]
    fn preexisting_lock_file_does_not_block_put_if() -> AnyhowResult<()> {
        let root = std::env::temp_dir().join(format!(
            "holys3-lock-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir_all(&root)?;
        let lock_path = root.join("segments.lock");
        std::fs::write(&lock_path, [])?;
        let thread_root = root.clone();
        let (tx, rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = LocalBlobStore::new(thread_root).put_if("segments.bin", b"root", None);
            let _ = tx.send(result);
        });
        let result = match rx.recv_timeout(std::time::Duration::from_millis(500)) {
            Ok(result) => result,
            Err(_) => {
                std::fs::remove_file(&lock_path)?;
                let _ = rx.recv_timeout(std::time::Duration::from_secs(1));
                worker
                    .join()
                    .map_err(|_| anyhow::anyhow!("put_if worker panicked"))?;
                std::fs::remove_dir_all(&root)?;
                anyhow::bail!("put_if blocked on a stale lock file");
            }
        };
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("put_if worker panicked"))?;
        assert!(result?);
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
