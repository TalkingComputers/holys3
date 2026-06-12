use crate::grams::hash_ngram;
use crate::grep::has_line_match;
use anyhow::Result as AnyhowResult;
use std::ops::Range;
use std::path::PathBuf;

/// One document in a corpus; its id is its position in `Corpus::docs()`.
pub struct Doc {
    /// Object key / file path.
    pub key: String,
    /// Compressed/raw byte size, bounding the build's per-chunk fetch
    /// memory; 0 = unknown.
    pub size: u64,
}

/// A source of documents for INDEX BUILDS, which need full enumeration.
/// Implemented by a local dir (tests) and S3 (prod).
pub trait Corpus {
    /// All documents; a doc's id is its position in this slice.
    fn docs(&self) -> &[Doc];
    /// Fetch the full bytes of one document by position.
    fn fetch(&self, idx: usize) -> AnyhowResult<Vec<u8>>;
    /// Fetch a contiguous run of docs concurrently. Result order is NOT
    /// guaranteed; each item carries its position. Implementations may
    /// return fewer docs than requested when a doc vanished between
    /// indexing and fetching. Default = sequential, fail-fast.
    fn fetch_many(&self, docs: Range<usize>) -> AnyhowResult<Vec<(usize, Vec<u8>)>> {
        docs.map(|idx| Ok((idx, self.fetch(idx)?))).collect()
    }
}

/// Fetches documents by key for SEARCH verification — no enumeration, no
/// doc table. `consume` receives the index into `keys` plus the body, as
/// fetches complete (order NOT guaranteed). Implementations may fetch
/// concurrently and may skip vanished docs; the first `consume` error
/// aborts the remaining fetches.
pub trait DocFetcher {
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> AnyhowResult<()>,
    ) -> AnyhowResult<()>;
}

pub trait BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()>;
    /// `Ok(None)` = blob does not exist. Transient store failures are `Err`
    /// so callers never mistake an outage for an empty store.
    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>>;
    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>>;
    /// Fetch many byte ranges of one blob, preserving order. Implementations
    /// may fetch concurrently. Default = sequential.
    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> AnyhowResult<Vec<Vec<u8>>> {
        ranges
            .iter()
            .map(|&(start, len)| self.get_range(name, start, len))
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
    format!("{:016x}-{}", hash_ngram(bytes), bytes.len())
}

pub struct LocalBlobStore {
    root: PathBuf,
}

impl LocalBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> LocalBlobStore {
        LocalBlobStore { root: root.into() }
    }
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

    /// CAS via an exclusive lock file beside the blob: check-then-write runs
    /// under the lock, so two local writers serialize.
    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> AnyhowResult<bool> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let lock_path = path.with_extension("lock");
        let lock = loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(file) => break file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(err) => return Err(err.into()),
            }
        };
        drop(lock);
        let result = (|| {
            let current = match std::fs::read(&path) {
                Ok(bytes) => Some(content_version(&bytes)),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
                Err(err) => return Err(err.into()),
            };
            if current.as_deref() != expected {
                return Ok(false);
            }
            std::fs::write(&path, bytes)?;
            Ok(true)
        })();
        std::fs::remove_file(&lock_path).ok();
        result
    }

    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, bytes)?;
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
    let mut hits = Vec::new();
    for (idx, doc) in corpus.docs().iter().enumerate() {
        let bytes = corpus.fetch(idx)?;
        if has_line_match(&bytes, re) {
            hits.push(doc.key.clone());
        }
    }
    hits.sort_unstable();
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::MemCorpus;
    use std::time::{SystemTime, UNIX_EPOCH};

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
        assert_eq!(
            store.get_ranges("builds/a/postings.bin", &[(0, 2), (4, 2)])?,
            vec![b"ab".to_vec(), b"ef".to_vec()]
        );
        std::fs::remove_dir_all(root)?;
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
        let keys = vec!["b".to_owned(), "a".to_owned()];
        let mut seen = Vec::new();
        c.fetch_each(&keys, &mut |idx, bytes| {
            seen.push((idx, bytes));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![(0, b"two".to_vec()), (1, b"one".to_vec())]);
    }

    #[test]
    fn fetch_many_aborts_on_first_error() {
        struct BrokenCorpus {
            docs: Vec<Doc>,
        }

        impl Corpus for BrokenCorpus {
            fn docs(&self) -> &[Doc] {
                &self.docs
            }

            fn fetch(&self, idx: usize) -> AnyhowResult<Vec<u8>> {
                if idx == 1 {
                    anyhow::bail!("broken");
                }
                Ok(b"ok".to_vec())
            }
        }

        let corpus = BrokenCorpus {
            docs: vec![
                Doc {
                    key: "a".into(),
                    size: 2,
                },
                Doc {
                    key: "b".into(),
                    size: 2,
                },
            ],
        };
        assert!(corpus.fetch_many(0..2).is_err());
    }
}
