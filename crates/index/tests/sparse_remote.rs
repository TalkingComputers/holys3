//! Remote sparse dictionary tests. These force remote mode through
//! `HOLYS3_SPARSE_REMOTE_MIN`, which is process-global state, so they live in
//! their own integration binary and run as one sequential test.

use anyhow::Result;
use holys3_core::{testutil::MemCorpus, BlobStore, LocalBlobStore, Strategy};
use holys3_index::{search_collect, update_index, SegmentedReader, SourceIdentity, UpdateOptions};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct CountingStore {
    inner: LocalBlobStore,
    terms_get_ranges: Arc<AtomicUsize>,
}

impl CountingStore {
    fn open(dir: &Path, terms_get_ranges: Arc<AtomicUsize>) -> CountingStore {
        CountingStore {
            inner: LocalBlobStore::new(dir),
            terms_get_ranges,
        }
    }
}

impl BlobStore for CountingStore {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put(name, bytes)
    }

    fn put_file(&self, name: &str, path: &Path) -> Result<()> {
        self.inner.put_file(name, path)
    }

    fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(name)
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        if name.ends_with("terms.fst") {
            self.terms_get_ranges.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_range(name, start, len)
    }

    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> Result<Vec<bytes::Bytes>> {
        if name.ends_with("terms.fst") {
            self.terms_get_ranges.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_ranges(name, ranges)
    }

    fn delete(&self, name: &str) -> Result<()> {
        self.inner.delete(name)
    }

    fn get_versioned(&self, name: &str) -> Result<Option<(Vec<u8>, String)>> {
        self.inner.get_versioned(name)
    }

    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> Result<bool> {
        self.inner.put_if(name, bytes, expected)
    }
}

fn test_source() -> SourceIdentity {
    SourceIdentity::Local {
        prefix: "/remote-test/".into(),
    }
}

fn search(reader: &SegmentedReader, pattern: &str) -> Result<Vec<(String, u64)>> {
    let (lines, _) = search_collect(reader, pattern)?;
    Ok(lines
        .into_iter()
        .map(|(key, event)| (key, event.line))
        .collect())
}

#[test]
fn remote_readers_cache_the_index_tail_and_match_cached_mode() -> Result<()> {
    let objects: BTreeMap<String, Vec<u8>> = (0..50)
        .map(|index| {
            let body = format!(
                "line one of document {index}\nthe quick brown fox {index}\nneedle-{index} appears here\n"
            );
            (format!("doc-{index:03}.txt"), body.into_bytes())
        })
        .collect();
    let listing: Vec<(String, String, u64)> = objects
        .iter()
        .map(|(key, body)| {
            (
                key.clone(),
                format!("{:016x}", holys3_core::hash_ngram(body)),
                body.len() as u64,
            )
        })
        .collect();
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let counter = Arc::new(AtomicUsize::new(0));
    let corpus_for = |shard: &[(String, String, u64)]| {
        let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
        let bodies = keys.iter().map(|key| objects[key].clone()).collect();
        MemCorpus::new(keys, bodies)
    };

    std::env::set_var("HOLYS3_SPARSE_REMOTE_MIN", "1");
    let store = CountingStore::open(store_dir.path(), counter.clone());
    update_index(
        &store,
        cache_dir.path(),
        &test_source(),
        Some(Strategy::Sparse),
        &listing,
        UpdateOptions::default(),
        &|shard| Ok(Box::new(corpus_for(shard))),
    )?;

    let open = || {
        SegmentedReader::open(
            Box::new(CountingStore::open(store_dir.path(), counter.clone())),
            cache_dir.path(),
            &test_source(),
        )
    };

    counter.store(0, Ordering::Relaxed);
    let cold = open()?;
    let cold_fetches = counter.load(Ordering::Relaxed);
    assert!(cold_fetches > 0, "first remote open must fetch the tail");
    let remote_hits = search(&cold, "quick brown fox")?;
    assert_eq!(remote_hits.len(), 50);

    counter.store(0, Ordering::Relaxed);
    let warm = open()?;
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "second remote open must serve the index tail from the local cache"
    );
    assert_eq!(search(&warm, "quick brown fox")?, remote_hits);
    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "a repeated query must serve its gram blocks from the local cache"
    );
    assert_eq!(search(&warm, "needle-7 appears")?.len(), 1);

    let seg_dir = std::fs::read_dir(cache_dir.path())?
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.path().is_dir())
        .expect("segment cache directory exists");
    let tail_path = seg_dir.path().join("terms.tail");
    assert!(tail_path.exists(), "index tail is cached on disk");
    let block_paths: Vec<std::path::PathBuf> = std::fs::read_dir(seg_dir.path())?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("terms-block-"))
        })
        .collect();
    assert!(!block_paths.is_empty(), "gram blocks are cached on disk");
    for path in [&tail_path, &block_paths[0]] {
        let mut corrupted = std::fs::read(path)?;
        corrupted[0] ^= 0xff;
        std::fs::write(path, &corrupted)?;
    }

    counter.store(0, Ordering::Relaxed);
    let healed = open()?;
    assert!(
        counter.load(Ordering::Relaxed) > 0,
        "corrupted cached tail must be refetched, not trusted"
    );
    counter.store(0, Ordering::Relaxed);
    assert_eq!(search(&healed, "quick brown fox")?, remote_hits);
    assert!(
        counter.load(Ordering::Relaxed) > 0,
        "corrupted cached block must be refetched, not trusted"
    );

    std::env::set_var("HOLYS3_SPARSE_REMOTE_MIN", u64::MAX.to_string());
    let cached_mode = open()?;
    assert_eq!(search(&cached_mode, "quick brown fox")?, remote_hits);
    std::env::remove_var("HOLYS3_SPARSE_REMOTE_MIN");
    Ok(())
}
