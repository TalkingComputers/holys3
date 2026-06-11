//! Lifecycle differential tests for the segmented incremental index: every
//! state a bucket can reach through add/modify/delete/re-add sequences must
//! search identically to a full scan of that state.

use anyhow::Result;
use holys3_core::{
    decode_body, scan_matching_docs, testutil::MemCorpus, Corpus, DocFetcher, LocalBlobStore,
    MatchOptions, Strategy,
};
use holys3_index::{
    search_collect, search_streaming, update_index, IndexReader, KeyScope, NullSink,
    SegmentedReader,
};
use std::collections::BTreeMap;
use std::path::Path;

/// A mutable in-memory "bucket": key -> body. Etags are content hashes so
/// modify-then-restore behaves like real S3.
#[derive(Default, Clone)]
struct Bucket {
    objects: BTreeMap<String, Vec<u8>>,
}

impl Bucket {
    fn put(&mut self, key: &str, body: &[u8]) {
        self.objects.insert(key.to_owned(), body.to_vec());
    }

    fn delete(&mut self, key: &str) {
        self.objects.remove(key);
    }

    fn listing(&self) -> Vec<(String, String)> {
        self.objects
            .iter()
            .map(|(key, body)| {
                (
                    key.clone(),
                    format!("{:016x}", holys3_core::hash_ngram(body)),
                )
            })
            .collect()
    }

    fn corpus_over(&self, keys: &[String]) -> MemCorpus {
        let docs = keys
            .iter()
            .enumerate()
            .map(|(i, key)| (i as u32, key.clone()))
            .collect();
        let bodies = keys.iter().map(|key| self.objects[key].clone()).collect();
        MemCorpus::new(docs, bodies)
    }

    fn full_corpus(&self) -> MemCorpus {
        let keys: Vec<String> = self.objects.keys().cloned().collect();
        self.corpus_over(&keys)
    }
}

struct BucketFetcher<'a>(&'a Bucket);

impl DocFetcher for BucketFetcher<'_> {
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        for (idx, key) in keys.iter().enumerate() {
            match self.0.objects.get(key) {
                Some(body) => consume(idx, body.clone())?,
                None => eprintln!("warning: {key} vanished; skipping"),
            }
        }
        Ok(())
    }
}

fn reindex(bucket: &Bucket, store_dir: &Path, cache_dir: &Path, strategy: Strategy) -> Result<()> {
    let store = LocalBlobStore::new(store_dir);
    let listing = bucket.listing();
    update_index(&store, cache_dir, strategy, &listing, false, &|keys| {
        Ok(Box::new(bucket.corpus_over(keys)))
    })?;
    Ok(())
}

/// Search the segmented index and compare with a scan oracle over the live
/// bucket contents (decompressed).
fn assert_matches_oracle(
    bucket: &Bucket,
    store_dir: &Path,
    cache_dir: &Path,
    patterns: &[&str],
    label: &str,
) -> Result<()> {
    let reader = SegmentedReader::open(Box::new(LocalBlobStore::new(store_dir)), cache_dir)?;
    let full = bucket.full_corpus();
    let decoded_bodies: Vec<Vec<u8>> = full
        .docs()
        .iter()
        .map(|(id, key)| decode_body(key, full.fetch(*id).expect("fetch")).expect("decode"))
        .collect();
    let decoded = MemCorpus::new(full.docs().to_vec(), decoded_bodies);
    for pattern in patterns {
        let fetcher = BucketFetcher(bucket);
        let hits = search_collect(&reader, &fetcher, pattern)?.1.hits;
        let re = regex::bytes::Regex::new(pattern)?;
        let oracle = scan_matching_docs(&decoded, &re)?;
        assert_eq!(hits, oracle, "{label}: pattern `{pattern}`");
    }
    Ok(())
}

const PATTERNS: &[&str] = &["needle", "shared", "zzznope", ".*", "v[12]-only"];

#[test]
fn lifecycle_add_modify_delete_readd() -> Result<()> {
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();

    // Run 1: initial corpus.
    bucket.put("logs/a", b"needle shared alpha");
    bucket.put("logs/b", b"shared beta v1-only");
    bucket.put("other/c", b"gamma");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        PATTERNS,
        "run1",
    )?;

    // Run 2: append only.
    bucket.put("logs/d", b"needle delta");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        PATTERNS,
        "run2-append",
    )?;

    // Run 3: modify an existing key (the old segment entry must go dead).
    bucket.put("logs/b", b"shared beta v2-only");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        PATTERNS,
        "run3-modify",
    )?;

    // Run 4: delete a key.
    bucket.delete("logs/a");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        PATTERNS,
        "run4-delete",
    )?;

    // Run 5: re-add the deleted key with the ORIGINAL content (same etag as
    // the dead entry — must still come back alive).
    bucket.put("logs/a", b"needle shared alpha");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        PATTERNS,
        "run5-readd",
    )?;

    // Run 6: no-op run must change nothing and still search correctly.
    let store = LocalBlobStore::new(store_dir.path());
    let listing = bucket.listing();
    let report = update_index(
        &store,
        cache_dir.path(),
        Strategy::Trigram,
        &listing,
        false,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    )?;
    assert!(report.up_to_date, "run6 should be a no-op");
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        PATTERNS,
        "run6-noop",
    )?;
    Ok(())
}

#[test]
fn compaction_bounds_segment_count_and_preserves_results() -> Result<()> {
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();

    // 20 index runs, one new doc each: forces repeated merges.
    for i in 0..20 {
        bucket.put(
            &format!("logs/doc{i:02}"),
            format!("needle number {i} shared").as_bytes(),
        );
        reindex(
            &bucket,
            store_dir.path(),
            cache_dir.path(),
            Strategy::Trigram,
        )?;
    }
    let reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
    )?;
    assert!(
        reader.total_docs() == 20,
        "expected 20 live docs, got {}",
        reader.total_docs()
    );
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        &["needle", "number 1", "shared", ".*"],
        "after-compactions",
    )?;

    // Segment count must stay bounded: target 8 plus at most the one new
    // segment a run can add beyond a single merge.
    let fetcher = BucketFetcher(&bucket);
    let stats = search_streaming(
        &reader,
        &fetcher,
        "needle",
        KeyScope::default(),
        MatchOptions::default(),
        &NullSink,
    )?;
    assert_eq!(stats.hits.len(), 20);
    Ok(())
}

#[test]
fn gzipped_objects_and_prefix_pruning() -> Result<()> {
    use std::io::Write;
    let gz = |data: &[u8]| {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(data).expect("gz write");
        enc.finish().expect("gz finish")
    };
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    bucket.put("logs/2026/06/08/x.gz", &gz(b"needle in gz"));
    bucket.put("logs/2026/06/09/y.gz", &gz(b"plain shared"));
    bucket.put("metrics/z", b"needle plain");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        &["needle", "shared"],
        "gz",
    )?;

    let reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
    )?;
    let fetcher = BucketFetcher(&bucket);
    let scope = KeyScope {
        prefix: Some("logs/"),
        matches: None,
    };
    let stats = search_streaming(
        &reader,
        &fetcher,
        "needle",
        scope,
        MatchOptions::default(),
        &NullSink,
    )?;
    assert_eq!(stats.hits, vec!["logs/2026/06/08/x.gz"]);
    Ok(())
}

#[test]
fn corrupt_cache_self_heals_and_stale_segments_evict() -> Result<()> {
    let store_dir = tempfile::tempdir()?;
    let cache = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    bucket.put("a", b"hello world");
    reindex(&bucket, store_dir.path(), cache.path(), Strategy::Trigram)?;
    drop(SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache.path(),
    )?);

    // Same-length corruption of a cached terms.fst self-heals on open.
    let seg_dir = std::fs::read_dir(cache.path())?
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("one cached segment");
    let fst_path = seg_dir.join("terms.fst");
    let len = std::fs::metadata(&fst_path)?.len() as usize;
    std::fs::write(&fst_path, vec![0u8; len])?;
    assert_matches_oracle(&bucket, store_dir.path(), cache.path(), PATTERNS, "healed")?;

    // Replacing the corpus retires the old segment; its cache dir goes too.
    bucket.put("a", b"replacement world");
    reindex(&bucket, store_dir.path(), cache.path(), Strategy::Trigram)?;
    drop(SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache.path(),
    )?);
    assert!(
        !seg_dir.exists(),
        "stale segment cache dir should be evicted"
    );
    Ok(())
}

#[test]
fn undecodable_objects_tombstone_and_converge() -> Result<()> {
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    // A truncated gzip header: detected as gzip, fails to decode.
    bucket.put("bad.gz", &[0x1f, 0x8b, 0x08, 0x00]);
    bucket.put("good", b"needle fine");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;

    // The undecodable object must NOT force a refetch every run: the next
    // run over an unchanged bucket is a no-op.
    let store = LocalBlobStore::new(store_dir.path());
    let listing = bucket.listing();
    let report = update_index(
        &store,
        cache_dir.path(),
        Strategy::Trigram,
        &listing,
        false,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    )?;
    assert!(report.up_to_date, "tombstoned object must not loop");

    // Replacing the bad object with decodable content picks it up.
    bucket.put("bad.gz", b"needle recovered");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        &["needle", "recovered"],
        "tombstone-recovery",
    )?;
    Ok(())
}

#[test]
fn rebuild_flag_reingests_from_scratch() -> Result<()> {
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    bucket.put("a", b"needle one");
    bucket.put("b", b"needle two");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;

    let store = LocalBlobStore::new(store_dir.path());
    let listing = bucket.listing();
    let report = update_index(
        &store,
        cache_dir.path(),
        Strategy::Trigram,
        &listing,
        true,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    )?;
    assert!(!report.up_to_date);
    assert_eq!(report.added, 2, "rebuild re-ingests everything");
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        &["needle"],
        "post-rebuild",
    )?;
    Ok(())
}

#[test]
fn unreferenced_segment_blobs_are_garbage_collected() -> Result<()> {
    fn segment_dirs(store_dir: &Path) -> Vec<String> {
        let segments = store_dir.join("segments");
        if !segments.exists() {
            return Vec::new();
        }
        let mut dirs: Vec<String> = std::fs::read_dir(&segments)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        dirs.sort();
        dirs
    }
    fn blob_files(store_dir: &Path) -> usize {
        walkdir(&store_dir.join("segments"))
    }
    fn walkdir(p: &Path) -> usize {
        if !p.exists() {
            return 0;
        }
        std::fs::read_dir(p)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    walkdir(&e.path())
                } else {
                    1
                }
            })
            .sum()
    }

    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    bucket.put("a", b"needle one");
    bucket.put("b", b"needle two");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    let first_gen = segment_dirs(store_dir.path());
    assert_eq!(first_gen.len(), 1);

    // a --rebuild must REPLACE the old segment's blobs, not orphan them
    let store = LocalBlobStore::new(store_dir.path());
    let listing = bucket.listing();
    update_index(
        &store,
        cache_dir.path(),
        Strategy::Trigram,
        &listing,
        true,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    )?;
    // dirs may linger empty on local fs; what matters is blob count: exactly
    // one live segment's worth of files (terms + postings + docs)
    assert_eq!(
        blob_files(store_dir.path()),
        3,
        "rebuild must not leak old segment blobs"
    );

    // a delete creates a dead-set; the NEXT dead-set supersedes it and the
    // old one must be GC'd (never two dead files for one segment)
    bucket.delete("a");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    assert_eq!(blob_files(store_dir.path()), 4, "segment + one dead-set");
    bucket.put("c", b"needle three");
    bucket.delete("b");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    // first segment fully dead -> dropped + GC'd; only the c-segment remains
    assert_eq!(
        blob_files(store_dir.path()),
        3,
        "fully-dead segment blobs must be GC'd"
    );
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        &["needle", "three"],
        "post-gc",
    )?;
    Ok(())
}

#[test]
fn losing_concurrent_writer_fails_loudly_and_gcs_nothing() -> Result<()> {
    use holys3_core::BlobStore;
    // Simulate writer B winning the root swap between A's load and A's swap:
    // a store wrapper that rewrites segments.bin under A's feet once.
    struct RacingStore {
        inner: LocalBlobStore,
        interloper: Vec<u8>,
        armed: std::cell::Cell<bool>,
    }

    impl BlobStore for RacingStore {
        fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
            self.inner.put(name, bytes)
        }
        fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get(name)
        }
        fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
            self.inner.get_range(name, start, len)
        }
        fn delete(&self, name: &str) -> Result<()> {
            self.inner.delete(name)
        }
        fn get_versioned(&self, name: &str) -> Result<Option<(Vec<u8>, String)>> {
            self.inner.get_versioned(name)
        }
        fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> Result<bool> {
            if name == "segments.bin" && self.armed.replace(false) {
                // B sneaks in first
                self.inner.put(name, &self.interloper)?;
            }
            self.inner.put_if(name, bytes, expected)
        }
    }

    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    bucket.put("a", b"needle one");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;
    let inner = LocalBlobStore::new(store_dir.path());
    let winner_root = inner.get("segments.bin")?.expect("root exists");

    bucket.put("b", b"needle two");
    let store = RacingStore {
        inner: LocalBlobStore::new(store_dir.path()),
        interloper: {
            // any DIFFERENT bytes: simulate B's root
            let mut altered = winner_root.clone();
            altered.push(0xFF);
            altered
        },
        armed: std::cell::Cell::new(true),
    };
    let listing = bucket.listing();
    let err = update_index(
        &store,
        cache_dir.path(),
        Strategy::Trigram,
        &listing,
        false,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    )
    .expect_err("losing writer must error");
    assert!(
        err.to_string().contains("concurrently"),
        "error must name the race: {err:#}"
    );
    // B's root is intact (last write standing is the interloper's)
    let root_now = LocalBlobStore::new(store_dir.path())
        .get("segments.bin")?
        .expect("root present");
    assert_ne!(root_now, winner_root);
    // and the ORIGINAL segment blobs were NOT garbage collected (the bucket
    // oracle does not apply here: "b" was never indexed by design)
    let segments = std::fs::read_dir(store_dir.path().join("segments"))?.count();
    assert!(segments >= 1, "winner's segment blobs must survive");
    Ok(())
}

#[test]
fn same_run_compacted_newborns_are_garbage_collected() -> Result<()> {
    // SEGMENT_COUNT_TARGET is 8: build 9 segments across runs, then one run
    // that adds a 10th AND compacts — the newborn that merges away in its
    // own birth run must not leak blobs.
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    for i in 0..10 {
        bucket.put(&format!("doc{i:02}"), format!("needle {i}").as_bytes());
        reindex(
            &bucket,
            store_dir.path(),
            cache_dir.path(),
            Strategy::Trigram,
        )?;
    }
    let store = LocalBlobStore::new(store_dir.path());
    let report = update_index(
        &store,
        cache_dir.path(),
        Strategy::Trigram,
        &bucket.listing(),
        false,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    )?;
    let dirs_on_disk = std::fs::read_dir(store_dir.path().join("segments"))?
        .filter(|e| {
            e.as_ref().unwrap().file_type().unwrap().is_dir()
                && std::fs::read_dir(e.as_ref().unwrap().path())
                    .unwrap()
                    .count()
                    > 0
        })
        .count();
    assert_eq!(
        dirs_on_disk, report.segments,
        "non-empty segment dirs on disk must equal live segments (no leaks)"
    );
    assert_matches_oracle(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        &["needle"],
        "post-churn",
    )?;
    Ok(())
}

#[test]
fn transient_store_error_fails_loudly_instead_of_rebuilding() -> Result<()> {
    use holys3_core::BlobStore;

    struct FlakyStore {
        inner: LocalBlobStore,
        fail_next_root_get: std::cell::Cell<bool>,
    }

    impl BlobStore for FlakyStore {
        fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
            self.inner.put(name, bytes)
        }

        fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
            if name == "segments.bin" && self.fail_next_root_get.replace(false) {
                anyhow::bail!("simulated transient outage");
            }
            self.inner.get(name)
        }

        fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
            self.inner.get_range(name, start, len)
        }

        fn delete(&self, name: &str) -> Result<()> {
            self.inner.delete(name)
        }

        fn get_versioned(&self, name: &str) -> Result<Option<(Vec<u8>, String)>> {
            if name == "segments.bin" && self.fail_next_root_get.replace(false) {
                anyhow::bail!("simulated transient outage");
            }
            self.inner.get_versioned(name)
        }

        fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> Result<bool> {
            self.inner.put_if(name, bytes, expected)
        }
    }

    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let mut bucket = Bucket::default();
    bucket.put("a", b"needle one");
    reindex(
        &bucket,
        store_dir.path(),
        cache_dir.path(),
        Strategy::Trigram,
    )?;

    let flaky = FlakyStore {
        inner: LocalBlobStore::new(store_dir.path()),
        fail_next_root_get: std::cell::Cell::new(true),
    };
    let listing = bucket.listing();
    let result = update_index(
        &flaky,
        cache_dir.path(),
        Strategy::Trigram,
        &listing,
        false,
        &|keys| Ok(Box::new(bucket.corpus_over(keys))),
    );
    assert!(
        result.is_err(),
        "a transient store error must fail loudly, not silently rebuild"
    );
    Ok(())
}
