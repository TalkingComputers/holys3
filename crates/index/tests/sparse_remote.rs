//! Remote sparse dictionary tests. These force remote mode through
//! `SEAGREP_SPARSE_REMOTE_MIN`, which is process-global state, so they live in
//! their own integration binary and run as one sequential test.

use anyhow::Result;
use seagrep_core::{testutil::MemCorpus, BlobStore, LocalBlobStore, Strategy};
use seagrep_index::{search_collect, update_index, SegmentedReader, SourceIdentity, UpdateOptions};
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

/// Both tests mutate process-global env vars; they share one lock so a
/// parallel test runner cannot interleave them. A panicking test poisons
/// the mutex, but the env guard below restores state on unwind, so the
/// poison carries no meaning and later tests just take the lock.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn env_lock() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Restores an env var to its pre-test state on drop, panics included.
struct EnvGuard {
    name: &'static str,
    prior: Option<String>,
}

impl EnvGuard {
    fn new(name: &'static str) -> EnvGuard {
        EnvGuard {
            name,
            prior: std::env::var(name).ok(),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prior {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
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
    let _lock = env_lock();
    let _remote_min = EnvGuard::new("SEAGREP_SPARSE_REMOTE_MIN");
    let _cache_max = EnvGuard::new("SEAGREP_CACHE_MAX");
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
                format!("{:016x}", seagrep_core::hash_ngram(body)),
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

    std::env::set_var("SEAGREP_SPARSE_REMOTE_MIN", "1");
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

    std::env::set_var("SEAGREP_SPARSE_REMOTE_MIN", u64::MAX.to_string());
    let cached_mode = open()?;
    assert_eq!(search(&cached_mode, "quick brown fox")?, remote_hits);
    std::env::remove_var("SEAGREP_SPARSE_REMOTE_MIN");
    Ok(())
}

#[test]
fn range_cache_evicts_to_its_cap_and_stays_correct() -> Result<()> {
    let _lock = env_lock();
    let _remote_min = EnvGuard::new("SEAGREP_SPARSE_REMOTE_MIN");
    let _cache_max = EnvGuard::new("SEAGREP_CACHE_MAX");
    let objects: BTreeMap<String, Vec<u8>> = (0..30)
        .map(|index| {
            let body = if index % 3 == 0 {
                format!("padding line for document {index}\nthe rare needle lives here {index}\n")
            } else {
                format!("padding line for document {index}\nplain body {index}\n")
            };
            (format!("doc-{index:03}.txt"), body.into_bytes())
        })
        .collect();
    let listing: Vec<(String, String, u64)> = objects
        .iter()
        .map(|(key, body)| {
            (
                key.clone(),
                format!("{:016x}", seagrep_core::hash_ngram(body)),
                body.len() as u64,
            )
        })
        .collect();
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let corpus_for = |shard: &[(String, String, u64)]| {
        let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
        let bodies = keys.iter().map(|key| objects[key].clone()).collect();
        MemCorpus::new(keys, bodies)
    };
    std::env::remove_var("SEAGREP_SPARSE_REMOTE_MIN");
    std::env::remove_var("SEAGREP_CACHE_MAX");
    update_index(
        &LocalBlobStore::new(store_dir.path()),
        cache_dir.path(),
        &test_source(),
        Some(Strategy::Sparse),
        &listing,
        UpdateOptions::default(),
        &|shard| Ok(Box::new(corpus_for(shard))),
    )?;

    let hits = |label: &str| -> Result<usize> {
        let reader = SegmentedReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
            &test_source(),
        )?;
        let count = search(&reader, "rare needle lives")?.len();
        let _ = label;
        Ok(count)
    };

    assert_eq!(hits("cold")?, 10);
    let range_files = |dir: &Path| -> usize {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|entry| entry.path().is_dir())
            .flat_map(|seg| {
                std::fs::read_dir(seg.path())
                    .into_iter()
                    .flatten()
                    .flatten()
            })
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with("pack-") || name.starts_with("postings-")
            })
            .count()
    };
    assert!(range_files(cache_dir.path()) > 0, "cache populated");

    // A one-byte cap forces the open-time sweep to evict everything; the
    // sweep runs at open, before any search can repopulate.
    std::env::set_var("SEAGREP_CACHE_MAX", "1");
    let swept = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
        &test_source(),
    )?;
    assert_eq!(
        range_files(cache_dir.path()),
        0,
        "cap of 1 byte evicts all range files"
    );
    assert_eq!(search(&swept, "rare needle lives")?.len(), 10);
    drop(swept);

    // Zero disables range caching: correct results, no new cache writes.
    std::env::set_var("SEAGREP_CACHE_MAX", "0");
    let before = range_files(cache_dir.path());
    assert_eq!(hits("disabled")?, 10);
    assert_eq!(
        range_files(cache_dir.path()),
        before,
        "disabled cache must not write range files"
    );
    Ok(())
}

struct RangeTallyStore {
    inner: LocalBlobStore,
    pack_ranges: Arc<AtomicUsize>,
    posting_ranges: Arc<AtomicUsize>,
}

impl RangeTallyStore {
    fn tally(&self, name: &str, ranges: usize) {
        if name.starts_with("packs/") {
            self.pack_ranges.fetch_add(ranges, Ordering::Relaxed);
        } else if name.ends_with("postings.bin") {
            self.posting_ranges.fetch_add(ranges, Ordering::Relaxed);
        }
    }
}

impl BlobStore for RangeTallyStore {
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
        self.tally(name, 1);
        self.inner.get_range(name, start, len)
    }

    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> Result<Vec<bytes::Bytes>> {
        self.tally(name, ranges.len());
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

#[test]
fn repeat_queries_serve_ranges_from_the_local_cache() -> Result<()> {
    let _lock = env_lock();
    let _remote_min = EnvGuard::new("SEAGREP_SPARSE_REMOTE_MIN");
    let _cache_max = EnvGuard::new("SEAGREP_CACHE_MAX");
    std::env::remove_var("SEAGREP_SPARSE_REMOTE_MIN");
    std::env::set_var("SEAGREP_CACHE_MAX", (4u64 << 30).to_string());

    let objects: BTreeMap<String, Vec<u8>> = (0..40)
        .map(|index| {
            let body = if index % 3 == 0 {
                format!(
                    "filler text about document {index}\nthe needle sits right here in {index}\n"
                )
            } else {
                format!("filler text about document {index}\nnothing interesting in {index}\n")
            };
            (format!("doc-{index:02}.log"), body.into_bytes())
        })
        .collect();
    let listing: Vec<(String, String, u64)> = objects
        .iter()
        .map(|(key, body)| {
            (
                key.clone(),
                format!("{:016x}", seagrep_core::hash_ngram(body)),
                body.len() as u64,
            )
        })
        .collect();
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let corpus_for = |shard: &[(String, String, u64)]| {
        let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
        let bodies = keys.iter().map(|key| objects[key].clone()).collect();
        MemCorpus::new(keys, bodies)
    };
    update_index(
        &LocalBlobStore::new(store_dir.path()),
        cache_dir.path(),
        &test_source(),
        Some(Strategy::Sparse),
        &listing,
        UpdateOptions::default(),
        &|shard| Ok(Box::new(corpus_for(shard))),
    )?;

    let run = |expect_fetch: bool| -> Result<Vec<(String, u64)>> {
        let pack_ranges = Arc::new(AtomicUsize::new(0));
        let posting_ranges = Arc::new(AtomicUsize::new(0));
        let reader = SegmentedReader::open(
            Box::new(RangeTallyStore {
                inner: LocalBlobStore::new(store_dir.path()),
                pack_ranges: pack_ranges.clone(),
                posting_ranges: posting_ranges.clone(),
            }),
            cache_dir.path(),
            &test_source(),
        )?;
        let hits = search(&reader, "needle sits right")?;
        let packs = pack_ranges.load(Ordering::Relaxed);
        let postings = posting_ranges.load(Ordering::Relaxed);
        if expect_fetch {
            assert!(packs > 0, "cold run must fetch pack ranges");
            assert!(postings > 0, "cold run must fetch posting ranges");
        } else {
            assert_eq!(packs, 0, "repeat run must not fetch pack ranges");
            assert_eq!(postings, 0, "repeat run must not fetch posting ranges");
        }
        Ok(hits)
    };

    let cold = run(true)?;
    assert_eq!(cold.len(), 14);
    let warm = run(false)?;
    assert_eq!(cold, warm);

    let seg_dir = std::fs::read_dir(cache_dir.path())?
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.path().is_dir())
        .expect("segment cache dir exists");
    let mut corrupted = 0;
    for entry in std::fs::read_dir(seg_dir.path())?.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy().to_string();
        if name.starts_with("pack-") || name.starts_with("postings-") {
            let mut bytes = std::fs::read(entry.path())?;
            if bytes.is_empty() {
                continue;
            }
            bytes[0] ^= 0xff;
            std::fs::write(entry.path(), &bytes)?;
            corrupted += 1;
        }
    }
    assert!(corrupted >= 2, "expected cached pack and posting files");
    let healed = run(true)?;
    assert_eq!(cold, healed);
    let warm_again = run(false)?;
    assert_eq!(cold, warm_again);
    Ok(())
}

#[test]
fn partially_cached_large_documents_reassemble_in_order() -> Result<()> {
    let _lock = env_lock();
    let _remote_min = EnvGuard::new("SEAGREP_SPARSE_REMOTE_MIN");
    let _cache_max = EnvGuard::new("SEAGREP_CACHE_MAX");
    std::env::remove_var("SEAGREP_SPARSE_REMOTE_MIN");
    std::env::set_var("SEAGREP_CACHE_MAX", (4u64 << 30).to_string());

    // A small doc first in pack order shares its pack block with the head of
    // a large multi-block doc; querying the small doc caches that block, so
    // the later large-doc fetch mixes a cache hit (first blocks) with misses.
    let mut objects: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    objects.insert(
        "aaa-small.log".into(),
        b"tiny document with the shared-needle inside\n".to_vec(),
    );
    let mut large = Vec::new();
    // Past the 32 MiB decoded threshold so the fetch takes the
    // order-sensitive large-document path.
    for line in 0..500_000u32 {
        large.extend_from_slice(
            format!("large doc line {line:06} with plenty of padding text to fill blocks\n")
                .as_bytes(),
        );
    }
    large.extend_from_slice(b"the shared-needle also lives at the very end\n");
    objects.insert("bbb-large.log".into(), large);

    let listing: Vec<(String, String, u64)> = objects
        .iter()
        .map(|(key, body)| {
            (
                key.clone(),
                format!("{:016x}", seagrep_core::hash_ngram(body)),
                body.len() as u64,
            )
        })
        .collect();
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let corpus_for = |shard: &[(String, String, u64)]| {
        let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
        let bodies = keys.iter().map(|key| objects[key].clone()).collect();
        MemCorpus::new(keys, bodies)
    };
    update_index(
        &LocalBlobStore::new(store_dir.path()),
        cache_dir.path(),
        &test_source(),
        Some(Strategy::Sparse),
        &listing,
        UpdateOptions::default(),
        &|shard| Ok(Box::new(corpus_for(shard))),
    )?;

    let open = || {
        SegmentedReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
            &test_source(),
        )
    };
    // Warm the cache with only the small doc's block(s).
    let warm = open()?;
    assert_eq!(search(&warm, "tiny document with")?.len(), 1);
    drop(warm);

    // The large doc now fetches with mixed hits and misses; both needles and
    // an interior line must come back intact and in order.
    let reader = open()?;
    let hits = search(&reader, "shared-needle")?;
    assert_eq!(hits.len(), 2, "{hits:?}");
    let interior = search(&reader, "large doc line 250000 with plenty")?;
    assert_eq!(interior.len(), 1);
    let repeat = search(&reader, "large doc line 499999 with plenty")?;
    assert_eq!(repeat.len(), 1);
    Ok(())
}

#[test]
fn corrupt_postings_blocks_abort_the_query_loudly() -> Result<()> {
    // Same-length corruption of a postings block is the one shape that
    // could silently drop documents; the per-block table must turn it into
    // a loud error, never an empty result (#45).
    let _lock = env_lock();
    let _cache_max = EnvGuard::new("SEAGREP_CACHE_MAX");
    // Disable the local range cache so the query reads the corrupted blob.
    std::env::set_var("SEAGREP_CACHE_MAX", "0");
    // The needle lives in a strict subset: a gram in every document
    // resolves to ALL and never touches postings at all.
    let objects: BTreeMap<String, Vec<u8>> = (0..40)
        .map(|index| {
            let body = if index % 3 == 0 {
                format!("filler text for document {index}\nthe shared needle body\n")
            } else {
                format!("filler text for document {index}\nplain body only\n")
            };
            (format!("doc-{index:03}.txt"), body.into_bytes())
        })
        .collect();
    let listing: Vec<(String, String, u64)> = objects
        .iter()
        .map(|(key, body)| {
            (
                key.clone(),
                format!("{:016x}", seagrep_core::hash_ngram(body)),
                body.len() as u64,
            )
        })
        .collect();
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let corpus_for = |shard: &[(String, String, u64)]| {
        let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
        let bodies = keys.iter().map(|key| objects[key].clone()).collect();
        MemCorpus::new(keys, bodies)
    };
    update_index(
        &LocalBlobStore::new(store_dir.path()),
        cache_dir.path(),
        &test_source(),
        Some(Strategy::Sparse),
        &listing,
        UpdateOptions::default(),
        &|shard| Ok(Box::new(corpus_for(shard))),
    )?;

    let reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
        &test_source(),
    )?;
    let clean = search(&reader, "shared needle body")?;
    assert_eq!(
        clean.len(),
        14,
        "the needle subset matches before corruption"
    );
    drop(reader);

    // Flip one byte in every postings.bin data region (first byte: inside
    // the first verification block whenever any posting list exists).
    let mut corrupted = 0;
    for entry in walkdir(store_dir.path())? {
        if entry.file_name() == Some(std::ffi::OsStr::new("postings.bin")) {
            let mut bytes = std::fs::read(&entry)?;
            if bytes.len() > 64 {
                bytes[0] ^= 0x01;
                std::fs::write(&entry, bytes)?;
                corrupted += 1;
            }
        }
    }
    assert!(
        corrupted > 0,
        "test must corrupt at least one postings blob"
    );

    let fresh_cache = tempfile::tempdir()?;
    let reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        fresh_cache.path(),
        &test_source(),
    )?;
    let error = match search(&reader, "shared needle body") {
        Err(error) => error,
        Ok(results) => panic!(
            "corrupt postings must error, not return {} results",
            results.len()
        ),
    };
    assert!(
        format!("{error:#}").contains("failed verification"),
        "{error:#}"
    );
    Ok(())
}

fn walkdir(root: &Path) -> Result<Vec<std::path::PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    Ok(out)
}
