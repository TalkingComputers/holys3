#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Index construction and local or store-backed index readers.

mod eval;
mod search;

pub use search::{search_collect, search_streaming, KeyScope, MatchSink, NullSink, SinkFlow};

use anyhow::{Context, Result};
use eval::Selection;
use holys3_core::{
    decode_body, grams_index, hash_ngram, BlobStore, Corpus, DocFetcher, DocId, Strategy,
};
use holys3_query::Query;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Docs are fetched and gram-extracted in chunks of this many, bounding build
/// memory to one chunk of bodies instead of the whole corpus.
const BUILD_FETCH_CHUNK: usize = 1024;

/// Bumped whenever index semantics change (e.g. grams now cover decompressed
/// bodies); an index built by an older holys3 must error, not silently
/// return wrong results.
const INDEX_FORMAT: u32 = 2;

#[derive(Serialize, Deserialize)]
struct Manifest {
    format: u32,
    build_id: String,
    strategy: Strategy,
    terms_fst_len: u64,
    postings_len: u64,
    docs: Vec<(DocId, String)>,
}

fn parse_manifest(bytes: &[u8]) -> Result<Manifest> {
    let manifest: Manifest = postcard::from_bytes(bytes)
        .context("index manifest unreadable; run `holys3 index` to rebuild")?;
    anyhow::ensure!(
        manifest.format == INDEX_FORMAT,
        "index format {} is not the current {INDEX_FORMAT}; run `holys3 index` to rebuild",
        manifest.format
    );
    Ok(manifest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStats {
    pub distinct_grams: u64,
    pub terms_fst_bytes: u64,
    pub postings_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchStats {
    /// Sorted keys of docs with at least one verified match.
    pub hits: Vec<String>,
    pub candidates: usize,
    pub total_docs: usize,
    pub bytes_fetched: usize,
}

pub trait IndexReader {
    fn strategy(&self) -> Strategy;
    fn total_docs(&self) -> usize;
    /// Candidate object keys: a superset of the docs that can match.
    /// `key_prefix` lets implementations prune whole index regions before
    /// any fetch; the engine re-applies it per key regardless.
    fn candidate_keys(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<String>>;
    fn stats(&self) -> IndexStats;
}

/// Map candidate local ids to keys, applying the prefix filter.
fn ids_to_keys(docs: &[(DocId, String)], ids: Vec<DocId>, key_prefix: Option<&str>) -> Vec<String> {
    ids.into_iter()
        .map(|id| docs[id as usize].1.clone())
        .filter(|key| key_prefix.is_none_or(|prefix| key.starts_with(prefix)))
        .collect()
}

fn decode_ids(bytes: &[u8], count: u32) -> Result<Vec<DocId>> {
    let expected = usize::try_from(count)? * 4;
    anyhow::ensure!(
        bytes.len() == expected,
        "posting block is {} bytes, expected {expected}",
        bytes.len()
    );
    bytes
        .chunks_exact(4)
        .map(|chunk| Ok(u32::from_le_bytes(chunk.try_into()?)))
        .collect()
}

/// Shared candidates pipeline: resolve grams against the term dict (no IO),
/// fetch every needed posting block via `fetch_blocks`, evaluate purely.
fn candidates_with(
    map: &fst::Map<memmap2::Mmap>,
    docs: &[(DocId, String)],
    q: &Query,
    fetch_blocks: impl FnOnce(&BTreeMap<u64, u32>) -> Result<BTreeMap<u64, Vec<DocId>>>,
) -> Result<Vec<DocId>> {
    let resolved = eval::resolve(q, &|gram| map.get(gram));
    let mut needed = BTreeMap::new();
    eval::blocks_needed(&resolved, &mut needed);
    let blocks = fetch_blocks(&needed)?;
    match eval::eval(&resolved, &blocks)? {
        Selection::All => Ok(docs.iter().map(|&(id, _)| id).collect()),
        Selection::Ids(ids) => Ok(ids),
    }
}

fn build_index_bytes(corpus: &dyn Corpus, strategy: Strategy) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut postings: BTreeMap<Vec<u8>, Vec<DocId>> = BTreeMap::new();
    let doc_keys = corpus.docs();
    let ids = doc_keys.iter().map(|&(id, _)| id).collect::<Vec<_>>();
    let undecodable = AtomicUsize::new(0);
    for chunk in ids.chunks(BUILD_FETCH_CHUNK) {
        let docs = corpus
            .fetch_many(chunk)?
            .into_par_iter()
            .filter_map(
                |(id, bytes)| match decode_body(&doc_keys[id as usize].1, bytes) {
                    Ok(text) => Some((id, grams_index(&text, strategy))),
                    Err(err) => {
                        eprintln!("warning: {err:#}; object excluded from index");
                        undecodable.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                },
            )
            .collect::<Vec<_>>();
        for (id, grams) in docs {
            for gram in grams {
                postings.entry(gram).or_default().push(id);
            }
        }
    }
    let undecodable = undecodable.into_inner();
    if undecodable > 0 {
        eprintln!("warning: {undecodable} objects could not be decompressed and were excluded");
    }
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut builder = fst::MapBuilder::new(Vec::new())?;
    for (gram, mut ids) in postings {
        ids.sort_unstable();
        ids.dedup();
        let offset = postings_buf.len() as u64;
        for id in &ids {
            postings_buf.extend_from_slice(&id.to_le_bytes());
        }
        builder.insert(gram, eval::pack_posting(offset, ids.len())?)?;
    }
    Ok((builder.into_inner()?, postings_buf))
}

fn make_manifest(
    corpus: &dyn Corpus,
    strategy: Strategy,
    build_id: &str,
    fst_bytes: &[u8],
    postings_buf: &[u8],
) -> Result<Manifest> {
    Ok(Manifest {
        format: INDEX_FORMAT,
        build_id: build_id.to_owned(),
        strategy,
        terms_fst_len: u64::try_from(fst_bytes.len())?,
        postings_len: u64::try_from(postings_buf.len())?,
        docs: corpus.docs().to_vec(),
    })
}

/// Write terms.fst + postings.bin + manifest.bin into `dir`.
pub fn build_to_dir(corpus: &dyn Corpus, dir: &Path, strategy: Strategy) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let (fst_bytes, postings_buf) = build_index_bytes(corpus, strategy)?;
    let ids = corpus
        .docs()
        .iter()
        .map(|(_, key)| (key.clone(), String::new()))
        .collect::<Vec<_>>();
    let manifest = make_manifest(
        corpus,
        strategy,
        &compute_build_id(&ids, strategy),
        &fst_bytes,
        &postings_buf,
    )?;
    std::fs::write(dir.join("terms.fst"), &fst_bytes)?;
    std::fs::write(dir.join("postings.bin"), &postings_buf)?;
    std::fs::write(dir.join("manifest.bin"), postcard::to_allocvec(&manifest)?)?;
    Ok(())
}

/// Content-addressed build id. Includes the strategy and index format so a
/// rebuild of the same corpus with different settings can never collide with
/// (and silently mix into) a cached build under `builds/<id>`.
pub fn compute_build_id(objects: &[(String, String)], strategy: Strategy) -> String {
    let mut objects = objects.iter().collect::<Vec<_>>();
    objects.sort_unstable();
    let mut bytes = format!("format={INDEX_FORMAT};strategy={strategy:?}\n").into_bytes();
    for (key, etag) in objects {
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(etag.as_bytes());
        bytes.push(b'\n');
    }
    format!("{:016x}", hash_ngram(&bytes))
}

pub fn build_to_store(
    corpus: &dyn Corpus,
    store: &dyn BlobStore,
    strategy: Strategy,
    build_id: &str,
) -> Result<()> {
    let (fst_bytes, postings_buf) = build_index_bytes(corpus, strategy)?;
    let manifest = make_manifest(corpus, strategy, build_id, &fst_bytes, &postings_buf)?;
    let base = format!("builds/{build_id}");
    store.put(&format!("{base}/terms.fst"), &fst_bytes)?;
    store.put(&format!("{base}/postings.bin"), &postings_buf)?;
    store.put(
        &format!("{base}/manifest.bin"),
        &postcard::to_allocvec(&manifest)?,
    )?;
    store.put("CURRENT", build_id.as_bytes())?;
    Ok(())
}

pub struct MmapIndexReader {
    map: fst::Map<memmap2::Mmap>,
    postings: memmap2::Mmap,
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
}

impl MmapIndexReader {
    pub fn open(dir: &Path) -> Result<MmapIndexReader> {
        let fst_file = std::fs::File::open(dir.join("terms.fst"))?;
        let map = fst::Map::new(unsafe {
            // Build dirs are immutable while readers are open.
            memmap2::Mmap::map(&fst_file)?
        })?;
        let post_file = std::fs::File::open(dir.join("postings.bin"))?;
        let postings = unsafe {
            // Build dirs are immutable while readers are open.
            memmap2::Mmap::map(&post_file)?
        };
        let manifest = parse_manifest(&std::fs::read(dir.join("manifest.bin"))?)?;
        Ok(MmapIndexReader {
            map,
            postings,
            docs: manifest.docs,
            strategy: manifest.strategy,
        })
    }
}

impl MmapIndexReader {
    /// All doc keys in this index (id order). Local-mode helper.
    pub fn doc_keys(&self) -> impl Iterator<Item = &str> {
        self.docs.iter().map(|(_, key)| key.as_str())
    }
}

impl IndexReader for MmapIndexReader {
    fn strategy(&self) -> Strategy {
        self.strategy
    }

    fn total_docs(&self) -> usize {
        self.docs.len()
    }

    fn candidate_keys(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<String>> {
        let ids = candidates_with(&self.map, &self.docs, q, |needed| {
            needed
                .iter()
                .map(|(&offset, &count)| {
                    let start = usize::try_from(offset)?;
                    let end = start
                        .checked_add(usize::try_from(count)? * 4)
                        .context("posting block end overflow")?;
                    let bytes = self
                        .postings
                        .get(start..end)
                        .context("truncated postings.bin")?;
                    Ok((offset, decode_ids(bytes, count)?))
                })
                .collect()
        })?;
        Ok(ids_to_keys(&self.docs, ids, key_prefix))
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            distinct_grams: self.map.len() as u64,
            terms_fst_bytes: self.map.as_fst().as_bytes().len() as u64,
            postings_bytes: self.postings.len() as u64,
        }
    }
}

pub struct StoreIndexReader {
    map: fst::Map<memmap2::Mmap>,
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
    store_postings_name: String,
    store: Box<dyn BlobStore>,
    terms_fst_len: u64,
    postings_len: u64,
}

impl StoreIndexReader {
    pub fn open(store: Box<dyn BlobStore>, cache_dir: &Path) -> Result<StoreIndexReader> {
        let build_id = String::from_utf8(store.get("CURRENT")?)?;
        let build_cache_dir = cache_dir.join(&build_id);
        let (map, manifest) = match open_cached_build(store.as_ref(), &build_cache_dir, &build_id) {
            Ok(parts) => parts,
            Err(_) => {
                // Cached blobs can be truncated or corrupt; refetch once.
                std::fs::remove_dir_all(&build_cache_dir).ok();
                open_cached_build(store.as_ref(), &build_cache_dir, &build_id)?
            }
        };
        evict_stale_builds(cache_dir, &build_id);
        Ok(StoreIndexReader {
            map,
            docs: manifest.docs,
            strategy: manifest.strategy,
            store_postings_name: format!("builds/{build_id}/postings.bin"),
            store,
            terms_fst_len: manifest.terms_fst_len,
            postings_len: manifest.postings_len,
        })
    }
}

fn open_cached_build(
    store: &dyn BlobStore,
    build_cache_dir: &Path,
    build_id: &str,
) -> Result<(fst::Map<memmap2::Mmap>, Manifest)> {
    let manifest_bytes = read_cached_blob(
        store,
        &build_cache_dir.join("manifest.bin"),
        &format!("builds/{build_id}/manifest.bin"),
    )?;
    let manifest = parse_manifest(&manifest_bytes)?;
    anyhow::ensure!(
        manifest.build_id == build_id,
        "manifest build_id {} does not match CURRENT {build_id}",
        manifest.build_id
    );
    let terms_path = build_cache_dir.join("terms.fst");
    let terms_bytes =
        read_cached_blob(store, &terms_path, &format!("builds/{build_id}/terms.fst"))?;
    anyhow::ensure!(
        u64::try_from(terms_bytes.len())? == manifest.terms_fst_len,
        "terms.fst length mismatch"
    );
    let fst_file = std::fs::File::open(&terms_path)?;
    let map = fst::Map::new(unsafe {
        // The cache file was just written atomically and is never edited in place.
        memmap2::Mmap::map(&fst_file)?
    })?;
    Ok((map, manifest))
}

fn evict_stale_builds(cache_dir: &Path, current: &str) {
    let Ok(entries) = std::fs::read_dir(cache_dir) else {
        return;
    };
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy() != current {
            std::fs::remove_dir_all(entry.path()).ok();
        }
    }
}

/// Read a blob through the local cache; cache writes are atomic (temp +
/// rename) so an interrupted write can never produce a half-written blob.
fn read_cached_blob(store: &dyn BlobStore, cache_path: &Path, store_name: &str) -> Result<Vec<u8>> {
    if let Ok(bytes) = std::fs::read(cache_path) {
        return Ok(bytes);
    }
    let bytes = store.get(store_name)?;
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file_name = cache_path
        .file_name()
        .context("cache path has no file name")?
        .to_string_lossy()
        .into_owned();
    let tmp = cache_path.with_file_name(format!("{file_name}.tmp.{}", std::process::id()));
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, cache_path)?;
    Ok(bytes)
}

impl IndexReader for StoreIndexReader {
    fn strategy(&self) -> Strategy {
        self.strategy
    }

    fn total_docs(&self) -> usize {
        self.docs.len()
    }

    fn candidate_keys(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<String>> {
        let ids = candidates_with(&self.map, &self.docs, q, |needed| {
            let ranges = needed
                .iter()
                .map(|(&offset, &count)| (offset, u64::from(count) * 4))
                .collect::<Vec<_>>();
            let blocks = self.store.get_ranges(&self.store_postings_name, &ranges)?;
            anyhow::ensure!(
                blocks.len() == ranges.len(),
                "get_ranges returned {} blocks for {} ranges",
                blocks.len(),
                ranges.len()
            );
            needed
                .iter()
                .zip(blocks)
                .map(|((&offset, &count), bytes)| Ok((offset, decode_ids(&bytes, count)?)))
                .collect()
        })?;
        Ok(ids_to_keys(&self.docs, ids, key_prefix))
    }

    fn stats(&self) -> IndexStats {
        IndexStats {
            distinct_grams: self.map.len() as u64,
            terms_fst_bytes: self.terms_fst_len,
            postings_bytes: self.postings_len,
        }
    }
}

pub struct LocalCorpus {
    docs: Vec<(DocId, String)>,
    paths: Vec<PathBuf>,
}

impl LocalCorpus {
    /// Walk `root` recursively. Symlinks are skipped, so cycles cannot hang
    /// the walk.
    pub fn new(root: &Path) -> Result<LocalCorpus> {
        let mut paths = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(p) = stack.pop() {
            for entry in std::fs::read_dir(&p)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    stack.push(entry.path());
                } else if file_type.is_file() {
                    paths.push(entry.path());
                }
            }
        }
        paths.sort();
        let docs = paths
            .iter()
            .enumerate()
            .map(|(i, p)| (i as DocId, p.to_string_lossy().into_owned()))
            .collect();
        Ok(LocalCorpus { docs, paths })
    }
}

impl Corpus for LocalCorpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.paths[id as usize])?)
    }
}

/// Search-side fetch for local files: the key IS the path.
pub struct LocalFetcher;

impl DocFetcher for LocalFetcher {
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        for (idx, key) in keys.iter().enumerate() {
            consume(idx, std::fs::read(key)?)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use holys3_core::testutil::MemCorpus;
    use holys3_core::{LocalBlobStore, Match};

    fn build_tmp(c: &MemCorpus, strategy: Strategy) -> (tempfile::TempDir, MmapIndexReader) {
        let dir = tempfile::tempdir().unwrap();
        build_to_dir(c, dir.path(), strategy).unwrap();
        let r = MmapIndexReader::open(dir.path()).unwrap();
        (dir, r)
    }

    #[test]
    fn candidate_superset_then_verify() {
        let c = MemCorpus::new(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"world".to_vec(), b"word".to_vec()],
        );
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_d, r) = build_tmp(&c, strategy);
            let cands = r
                .candidate_keys(&holys3_query::plan("world", r.strategy()).unwrap(), None)
                .unwrap();
            assert!(cands.iter().any(|key| key == "x"));
            assert!(cands.iter().all(|key| key == "x" || key == "y"));
        }
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus::new(vec![(0, "x".into())], vec![b"abcdef".to_vec()]);
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_d, r) = build_tmp(&c, strategy);
            assert_eq!(r.candidate_keys(&Query::All, None).unwrap(), vec!["x"]);
        }
    }

    #[test]
    fn search_collect_returns_verified_matches_and_stats() {
        let c = MemCorpus::new(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"abc world".to_vec(), b"nomatch".to_vec()],
        );
        let (_d, r) = build_tmp(&c, Strategy::Trigram);
        let (matches, stats) = search_collect(&r, &c, "world").unwrap();
        assert_eq!(
            matches,
            vec![(
                "x".to_owned(),
                Match {
                    line: 1,
                    col: 5,
                    text: "abc world".into(),
                }
            )]
        );
        assert_eq!(stats.hits, vec!["x"]);
        assert_eq!(stats.candidates, 1);
        assert_eq!(stats.total_docs, 2);
        assert_eq!(stats.bytes_fetched, b"abc world".len());
    }

    #[test]
    fn files_only_streaming_matches_full_search() {
        let c = MemCorpus::new(
            vec![(0, "x".into()), (1, "y".into()), (2, "z".into())],
            vec![
                b"abc world".to_vec(),
                b"nomatch".to_vec(),
                b"world world".to_vec(),
            ],
        );
        let (_d, r) = build_tmp(&c, Strategy::Trigram);
        let stats = search_streaming(&r, &c, "world", KeyScope::default(), &NullSink).unwrap();
        let (_, full_stats) = search_collect(&r, &c, "world").unwrap();
        assert_eq!(stats.hits, full_stats.hits);
        assert_eq!(stats.hits, vec!["x", "z"]);
    }

    #[test]
    fn key_filter_prunes_before_fetch() {
        let c = MemCorpus::new(
            vec![(0, "logs/a".into()), (1, "other/b".into())],
            vec![b"abc world".to_vec(), b"abc world".to_vec()],
        );
        let (_d, r) = build_tmp(&c, Strategy::Trigram);
        let scope = KeyScope {
            prefix: Some("logs/"),
            matches: None,
        };
        let stats = search_streaming(&r, &c, "world", scope, &NullSink).unwrap();
        assert_eq!(stats.hits, vec!["logs/a"]);
        assert_eq!(stats.candidates, 1);
        assert_eq!(stats.bytes_fetched, b"abc world".len());
    }

    #[test]
    fn gzipped_docs_are_indexed_and_searched_as_text() {
        use std::io::Write;
        let gz = |data: &[u8]| {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        let mut multi = gz(b"first line\n");
        multi.extend(gz(b"needle in second member\n"));
        let c = MemCorpus::new(
            vec![(0, "a.log.gz".into()), (1, "b.log".into())],
            vec![multi, b"plain needle\n".to_vec()],
        );
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_d, r) = build_tmp(&c, strategy);
            let (matches, stats) = search_collect(&r, &c, "needle").unwrap();
            assert_eq!(
                stats.hits,
                vec!["a.log.gz", "b.log"],
                "strategy {strategy:?}"
            );
            assert_eq!(matches[0].1.line, 2);
            assert_eq!(matches[0].1.text, "needle in second member");
        }
    }

    #[test]
    fn sink_stop_ends_search_early_without_error() {
        struct StopAfterFirst;

        impl MatchSink for StopAfterFirst {
            fn on_doc(&self, _key: &str, _matches: &[Match]) -> Result<SinkFlow> {
                Ok(SinkFlow::Stop)
            }
        }

        let docs = (0..100u32).map(|i| (i, format!("doc{i}"))).collect();
        let bodies = (0..100u32)
            .map(|i| format!("needle {i}").into_bytes())
            .collect();
        let c = MemCorpus::new(docs, bodies);
        let (_d, r) = build_tmp(&c, Strategy::Trigram);
        let stats =
            search_streaming(&r, &c, "needle", KeyScope::default(), &StopAfterFirst).unwrap();
        // Stop is cooperative: at least one hit was reported, the search
        // ended Ok, and whatever was skipped is simply absent from hits.
        assert!(!stats.hits.is_empty());
        assert_eq!(stats.candidates, 100);
    }

    #[test]
    fn store_reader_round_trips_and_skips_absent_grams() -> Result<()> {
        use std::cell::Cell;
        use std::rc::Rc;

        struct CountingStore {
            inner: LocalBlobStore,
            range_calls: Rc<Cell<usize>>,
        }

        impl BlobStore for CountingStore {
            fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
                self.inner.put(name, bytes)
            }

            fn get(&self, name: &str) -> Result<Vec<u8>> {
                self.inner.get(name)
            }

            fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
                self.range_calls.set(self.range_calls.get() + 1);
                self.inner.get_range(name, start, len)
            }
        }

        let corpus = MemCorpus::new(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"hello world".to_vec(), b"goodbye moon".to_vec()],
        );
        let store_dir = tempfile::tempdir()?;
        let range_calls = Rc::new(Cell::new(0));
        let store = CountingStore {
            inner: LocalBlobStore::new(store_dir.path()),
            range_calls: Rc::clone(&range_calls),
        };
        build_to_store(&corpus, &store, Strategy::Trigram, "test-build")?;
        let cache_dir = tempfile::tempdir()?;
        let reader = StoreIndexReader::open(
            Box::new(CountingStore {
                inner: LocalBlobStore::new(store_dir.path()),
                range_calls: Rc::clone(&range_calls),
            }),
            cache_dir.path(),
        )?;

        let cands = reader.candidate_keys(&Query::Gram(b"wor".to_vec()), None)?;
        assert_eq!(cands, vec!["x"]);
        let calls_after_hit = range_calls.get();
        assert!(calls_after_hit >= 1);

        let cands = reader.candidate_keys(&Query::Gram(b"zzz".to_vec()), None)?;
        assert!(cands.is_empty());
        assert_eq!(range_calls.get(), calls_after_hit);
        Ok(())
    }

    #[test]
    fn store_reader_heals_corrupt_cache() -> Result<()> {
        let corpus = MemCorpus::new(vec![(0, "x".into())], vec![b"hello world".to_vec()]);
        let store_dir = tempfile::tempdir()?;
        let store = LocalBlobStore::new(store_dir.path());
        build_to_store(&corpus, &store, Strategy::Trigram, "build-a")?;

        let cache_dir = tempfile::tempdir()?;
        drop(StoreIndexReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
        )?);
        let cached_manifest = cache_dir.path().join("build-a/manifest.bin");
        std::fs::write(&cached_manifest, b"garbage")?;

        let reader = StoreIndexReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
        )?;
        assert_eq!(reader.total_docs(), 1);
        Ok(())
    }

    #[test]
    fn store_reader_evicts_stale_builds() -> Result<()> {
        let corpus = MemCorpus::new(vec![(0, "x".into())], vec![b"hello world".to_vec()]);
        let store_dir = tempfile::tempdir()?;
        let store = LocalBlobStore::new(store_dir.path());
        build_to_store(&corpus, &store, Strategy::Trigram, "build-b")?;

        let cache_dir = tempfile::tempdir()?;
        std::fs::create_dir_all(cache_dir.path().join("stale-build"))?;
        drop(StoreIndexReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
        )?);
        assert!(!cache_dir.path().join("stale-build").exists());
        assert!(cache_dir.path().join("build-b").exists());
        Ok(())
    }
}
