#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Index construction and local or store-backed index readers.

mod eval;
mod search;
mod segment;

pub use search::{
    search_collect, search_streaming, DocResult, KeyScope, MatchSink, NullSink, SinkFlow,
};
pub use segment::{update_index, CorpusFactory, SegmentedReader, UpdateReport};

use anyhow::{Context, Result};
use eval::Selection;
use holys3_core::{decode_body, grams_index, Corpus, Doc, DocFetcher, DocId, Strategy};
use holys3_query::Query;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};

/// Docs are fetched and gram-extracted in chunks bounded BOTH by doc count
/// and by total (compressed) bytes, so neither many-small nor few-huge
/// objects blow build memory.
const BUILD_FETCH_CHUNK: usize = 1024;
const BUILD_FETCH_BYTES: u64 = 256 * 1024 * 1024;

/// Greedy chunk boundaries over `docs()` positions respecting both caps; a
/// single over-budget doc still forms its own chunk.
fn build_chunks(docs: &[Doc]) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut start = 0usize;
    std::iter::from_fn(move || {
        if start >= docs.len() {
            return None;
        }
        let mut end = start;
        let mut bytes = 0u64;
        while end < docs.len() && end - start < BUILD_FETCH_CHUNK {
            let size = docs[end].size;
            if end > start && bytes + size > BUILD_FETCH_BYTES {
                break;
            }
            bytes += size;
            end += 1;
        }
        let chunk = start..end;
        start = end;
        Some(chunk)
    })
}

/// Bumped whenever index semantics change (e.g. grams now cover decompressed
/// bodies); an index built by an older holys3 must error, not silently
/// return wrong results.
const INDEX_FORMAT: u32 = 5;

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
    /// any fetch; candidates MAY include keys outside the prefix — the
    /// engine filters.
    fn candidate_keys(&self, q: &Query, key_prefix: Option<&str>) -> Result<Vec<String>>;
    fn stats(&self) -> IndexStats;
}

/// Bit width of one stored doc id: just wide enough for the largest id in
/// `0..doc_count`. A pure function of `doc_count`, so block byte lengths
/// stay derivable BEFORE any fetch.
fn posting_id_bits(doc_count: u32) -> u32 {
    (32 - doc_count.saturating_sub(1).leading_zeros()).max(1)
}

/// How many ids a block physically stores: the COMPLEMENT (absent ids) when
/// the gram is in more than half the docs, the present ids otherwise, and
/// nothing at all when the gram is in every doc. The representation class is
/// a pure function of `(count, doc_count)` — no flags, no sniffing.
fn stored_id_count(count: u32, doc_count: u32) -> u64 {
    if count == doc_count {
        0
    } else if u64::from(count) * 2 > u64::from(doc_count) {
        // saturating: a corrupt count > doc_count yields 0 here and is then
        // rejected loudly by decode_posting_block's count <= doc_count check
        u64::from(doc_count.saturating_sub(count))
    } else {
        u64::from(count)
    }
}

/// On-disk byte length of a posting block: `stored_id_count` ids bit-packed
/// at `posting_id_bits` each, rounded up to whole bytes.
pub(crate) fn posting_block_len(count: u32, doc_count: u32) -> u64 {
    let bits = stored_id_count(count, doc_count) * u64::from(posting_id_bits(doc_count));
    bits.div_ceil(8)
}

fn pack_ids(buf: &mut Vec<u8>, ids: impl Iterator<Item = DocId>, width: u32) {
    let mut acc: u64 = 0;
    let mut filled: u32 = 0;
    for id in ids {
        acc |= u64::from(id) << filled;
        filled += width;
        while filled >= 8 {
            buf.push(acc as u8);
            acc >>= 8;
            filled -= 8;
        }
    }
    if filled > 0 {
        buf.push(acc as u8);
    }
}

fn unpack_ids(bytes: &[u8], n: u64, width: u32) -> Vec<DocId> {
    let mut out = Vec::with_capacity(usize::try_from(n).unwrap_or(0));
    let mut acc: u64 = 0;
    let mut filled: u32 = 0;
    let mut input = bytes.iter();
    let mask: u64 = (1u64 << width) - 1;
    for _ in 0..n {
        while filled < width {
            acc |= u64::from(*input.next().expect("length validated")) << filled;
            filled += 8;
        }
        out.push((acc & mask) as u32);
        acc >>= width;
        filled -= width;
    }
    out
}

fn encode_posting_block(buf: &mut Vec<u8>, ids: &[DocId], doc_count: u32) {
    let count = ids.len() as u32;
    if count == doc_count {
        return;
    }
    let width = posting_id_bits(doc_count);
    if u64::from(count) * 2 > u64::from(doc_count) {
        let mut present = ids.iter().copied().peekable();
        let absent = (0..doc_count).filter(|id| {
            if present.peek() == Some(id) {
                present.next();
                false
            } else {
                true
            }
        });
        pack_ids(buf, absent, width);
    } else {
        pack_ids(buf, ids.iter().copied(), width);
    }
}

/// Inverse of `encode_posting_block`. Validates exact length, strict
/// ascending order, and id bounds — a block that fails any of these is a
/// corrupt index, reported loudly.
pub(crate) fn decode_posting_block(bytes: &[u8], count: u32, doc_count: u32) -> Result<Vec<DocId>> {
    anyhow::ensure!(
        count <= doc_count,
        "posting count {count} exceeds doc count {doc_count}"
    );
    let expected = posting_block_len(count, doc_count);
    anyhow::ensure!(
        bytes.len() as u64 == expected,
        "posting block is {} bytes, expected {expected}",
        bytes.len()
    );
    if count == doc_count {
        return Ok((0..doc_count).collect());
    }
    let stored = unpack_ids(
        bytes,
        stored_id_count(count, doc_count),
        posting_id_bits(doc_count),
    );
    for pair in stored.windows(2) {
        anyhow::ensure!(
            pair[0] < pair[1],
            "posting block ids are not strictly ascending"
        );
    }
    if let Some(&last) = stored.last() {
        anyhow::ensure!(
            last < doc_count,
            "posting block references doc {last} >= doc_count {doc_count}"
        );
    }
    if u64::from(count) * 2 > u64::from(doc_count) {
        let mut absent = stored.into_iter().peekable();
        let mut present = Vec::with_capacity(count as usize);
        for id in 0..doc_count {
            if absent.peek() == Some(&id) {
                absent.next();
            } else {
                present.push(id);
            }
        }
        Ok(present)
    } else {
        Ok(stored)
    }
}

/// Shared candidates pipeline: resolve grams against the term dict (no IO),
/// fetch every needed posting block via `fetch_blocks`, evaluate purely.
/// Returns local ids in `0..doc_count`.
pub(crate) fn candidates_with<D: AsRef<[u8]>>(
    map: &fst::Map<D>,
    doc_count: u32,
    q: &Query,
    fetch_blocks: impl FnOnce(&BTreeMap<u64, u32>) -> Result<BTreeMap<u64, Vec<DocId>>>,
) -> Result<Vec<DocId>> {
    let resolved = eval::resolve(q, doc_count, &|gram| map.get(gram));
    let mut needed = BTreeMap::new();
    eval::blocks_needed(&resolved, &mut needed);
    let blocks = fetch_blocks(&needed)?;
    match eval::eval(&resolved, &blocks)? {
        Selection::All => Ok((0..doc_count).collect()),
        Selection::Ids(ids) => Ok(ids),
    }
}

/// Build terms.fst + postings.bin over the corpus. Also returns the ids of
/// docs that contributed NO grams because they vanished mid-build (404) or
/// failed to decompress — segment writers mark them failed so the next
/// incremental run retries them only when the object changes.
fn build_index_bytes(
    corpus: &dyn Corpus,
    strategy: Strategy,
) -> Result<(Vec<u8>, Vec<u8>, Vec<DocId>)> {
    let mut postings: BTreeMap<Vec<u8>, Vec<DocId>> = BTreeMap::new();
    let docs = corpus.docs();
    let mut ungrammed: Vec<DocId> = Vec::new();
    for chunk in build_chunks(docs) {
        let fetched = corpus.fetch_many(chunk.clone())?;
        let mut seen = vec![false; chunk.len()];
        let grammed = fetched
            .into_par_iter()
            .filter_map(|(idx, bytes)| match decode_body(&docs[idx].key, bytes) {
                Ok(text) => Some((idx, grams_index(&text, strategy))),
                Err(err) => {
                    eprintln!("warning: {err:#}; object excluded from index");
                    None
                }
            })
            .collect::<Vec<_>>();
        for (idx, _) in &grammed {
            seen[idx - chunk.start] = true;
        }
        ungrammed.extend(
            chunk
                .zip(&seen)
                .filter(|(_, seen)| !**seen)
                .map(|(idx, _)| idx as DocId),
        );
        for (idx, grams) in grammed {
            for gram in grams {
                postings.entry(gram).or_default().push(idx as DocId);
            }
        }
    }
    if !ungrammed.is_empty() {
        eprintln!(
            "warning: {} objects vanished or could not be decompressed and were excluded",
            ungrammed.len()
        );
    }
    let (fst_bytes, postings_buf) = serialize_postings(postings, docs.len() as u32)?;
    ungrammed.sort_unstable();
    Ok((fst_bytes, postings_buf, ungrammed))
}

/// THE postings format: per gram, a density-classed block in postings.bin
/// (see `posting_block_len`); the fst maps gram -> packed (offset, count).
/// Shared by fresh builds and compaction merges so the format is defined
/// once. Dense grams cost zero bytes — the query path never fetches them
/// (`resolve` short-circuits them to ALL) and decode reconstructs them.
pub(crate) fn serialize_postings(
    postings: BTreeMap<Vec<u8>, Vec<DocId>>,
    doc_count: u32,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut builder = fst::MapBuilder::new(Vec::new())?;
    for (gram, mut ids) in postings {
        ids.sort_unstable();
        ids.dedup();
        // A gram whose docs all died (compaction) must be ABSENT, not empty:
        // a zero-length block shares its offset with the next block, and the
        // offset-keyed fetch map would clobber the neighbor's count.
        if ids.is_empty() {
            continue;
        }
        let offset = postings_buf.len() as u64;
        encode_posting_block(&mut postings_buf, &ids, doc_count);
        builder.insert(gram, eval::pack_posting(offset, ids.len())?)?;
    }
    Ok((builder.into_inner()?, postings_buf))
}

pub struct LocalCorpus {
    docs: Vec<Doc>,
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
        // Keys are S3-style `/`-separated on every platform so globs,
        // prefixes, and key-time scoping behave identically; Windows file
        // APIs accept forward slashes, so fetching by key still works.
        #[cfg(windows)]
        let key_of = |p: &PathBuf| p.to_string_lossy().replace('\\', "/");
        #[cfg(not(windows))]
        let key_of = |p: &PathBuf| p.to_string_lossy().into_owned();
        let docs = paths
            .iter()
            .map(|p| {
                Ok(Doc {
                    key: key_of(p),
                    size: std::fs::metadata(p)?.len(),
                })
            })
            .collect::<Result<Vec<Doc>>>()?;
        Ok(LocalCorpus { docs, paths })
    }

    /// Corpus over exactly the listed files ((key, etag, size) triples; keys
    /// are full paths; ids = positions): the changed subset an incremental
    /// index run fetches.
    pub fn from_listing(listing: &[(String, String, u64)]) -> LocalCorpus {
        let docs = listing
            .iter()
            .map(|(key, _, size)| Doc {
                key: key.clone(),
                size: *size,
            })
            .collect();
        let paths = listing
            .iter()
            .map(|(key, _, _)| PathBuf::from(key))
            .collect();
        LocalCorpus { docs, paths }
    }

    /// (key, etag, size) listing of the walked files. Etags are synthesized
    /// as `{size}-{mtime_ns}` — the standard freshness heuristic for local
    /// files.
    pub fn listing(&self) -> Result<Vec<(String, String, u64)>> {
        self.docs
            .iter()
            .zip(&self.paths)
            .map(|(doc, path)| {
                let meta = std::fs::metadata(path)?;
                let mtime = meta
                    .modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .context("file mtime before the unix epoch")?;
                Ok((
                    doc.key.clone(),
                    format!("{}-{}", meta.len(), mtime.as_nanos()),
                    meta.len(),
                ))
            })
            .collect()
    }
}

impl Corpus for LocalCorpus {
    fn docs(&self) -> &[Doc] {
        &self.docs
    }

    fn fetch(&self, idx: usize) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.paths[idx])?)
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
    use holys3_core::{LineEvent, LineKind, LocalBlobStore, MatchOptions, SubMatch};

    fn build_tmp(
        c: &MemCorpus,
        strategy: Strategy,
    ) -> (tempfile::TempDir, tempfile::TempDir, SegmentedReader) {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let listing: Vec<(String, String, u64)> = c
            .docs()
            .iter()
            .map(|doc| (doc.key.clone(), format!("etag-{}", doc.key), doc.size))
            .collect();
        update_index(
            &LocalBlobStore::new(store_dir.path()),
            cache_dir.path(),
            strategy,
            &listing,
            false,
            &|shard| {
                let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
                let bodies = keys
                    .iter()
                    .map(|key| {
                        let idx = c
                            .docs()
                            .iter()
                            .position(|doc| doc.key == *key)
                            .expect("listed key exists");
                        c.fetch(idx)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(MemCorpus::new(keys, bodies)))
            },
        )
        .unwrap();
        let r = SegmentedReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
        )
        .unwrap();
        (store_dir, cache_dir, r)
    }

    #[test]
    fn posting_blocks_round_trip_every_density_class() {
        let cases: Vec<(Vec<u32>, u32)> = vec![
            (vec![], 10),             // empty list
            (vec![3], 10),            // single
            (vec![0, 1, 2], 7),       // sparse (below half)
            ((0..5).collect(), 10),   // exactly half: stored as ids
            ((0..6).collect(), 10),   // just over half: complement
            ((0..9).collect(), 10),   // doc_count - 1: complement of one
            ((0..10).collect(), 10),  // fully dense: zero bytes
            (vec![0], 1),             // doc_count = 1, dense
            (vec![1, 3, 5, 7], 8),    // exactly half at even doc_count
            (vec![0, 2, 4, 6, 7], 8), // over half
        ];
        for (ids, doc_count) in cases {
            let mut buf = Vec::new();
            encode_posting_block(&mut buf, &ids, doc_count);
            assert_eq!(
                buf.len() as u64,
                posting_block_len(ids.len() as u32, doc_count),
                "len mismatch for {ids:?}/{doc_count}"
            );
            let decoded = decode_posting_block(&buf, ids.len() as u32, doc_count).unwrap();
            assert_eq!(decoded, ids, "round trip failed for doc_count {doc_count}");
        }
        // dense stores nothing
        let mut buf = Vec::new();
        encode_posting_block(&mut buf, &(0..10).collect::<Vec<_>>(), 10);
        assert!(buf.is_empty());
    }

    #[test]
    fn posting_block_decode_rejects_corruption() {
        // wrong length
        assert!(decode_posting_block(&[0, 0, 0, 0], 2, 10).is_err());
        // count above doc_count
        assert!(decode_posting_block(&[], 11, 10).is_err());
        // out-of-bounds id (sparse class: 1 of 10 -> 4 bytes)
        assert!(decode_posting_block(&10u32.to_le_bytes(), 1, 10).is_err());
        // unsorted ids (2 of 10 -> stored as ids)
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(&3u32.to_le_bytes());
        assert!(decode_posting_block(&buf, 2, 10).is_err());
        // duplicate ids are not strictly ascending
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(&3u32.to_le_bytes());
        assert!(decode_posting_block(&buf, 2, 10).is_err());
    }

    #[test]
    fn candidate_superset_then_verify() {
        let c = MemCorpus::new(
            vec!["x".into(), "y".into()],
            vec![b"world".to_vec(), b"word".to_vec()],
        );
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_s, _c, r) = build_tmp(&c, strategy);
            let cands = r
                .candidate_keys(&holys3_query::plan("world", r.strategy()).unwrap(), None)
                .unwrap();
            assert!(cands.iter().any(|key| key == "x"));
            assert!(cands.iter().all(|key| key == "x" || key == "y"));
        }
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus::new(vec!["x".into()], vec![b"abcdef".to_vec()]);
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_s, _c, r) = build_tmp(&c, strategy);
            assert_eq!(r.candidate_keys(&Query::All, None).unwrap(), vec!["x"]);
        }
    }

    #[test]
    fn search_collect_returns_verified_matches_and_stats() {
        let c = MemCorpus::new(
            vec!["x".into(), "y".into()],
            vec![b"abc world".to_vec(), b"nomatch".to_vec()],
        );
        let (_s, _c, r) = build_tmp(&c, Strategy::Trigram);
        let (matches, stats) = search_collect(&r, &c, "world").unwrap();
        assert_eq!(
            matches,
            vec![(
                "x".to_owned(),
                LineEvent {
                    line: 1,
                    kind: LineKind::Match,
                    offset: 0,
                    text: b"abc world".to_vec(),
                    submatches: vec![SubMatch { start: 4, end: 9 }],
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
            vec!["x".into(), "y".into(), "z".into()],
            vec![
                b"abc world".to_vec(),
                b"nomatch".to_vec(),
                b"world world".to_vec(),
            ],
        );
        let (_s, _c, r) = build_tmp(&c, Strategy::Trigram);
        let stats = search_streaming(
            &r,
            &c,
            "world",
            KeyScope::default(),
            MatchOptions::default(),
            &NullSink,
        )
        .unwrap();
        let (_, full_stats) = search_collect(&r, &c, "world").unwrap();
        assert_eq!(stats.hits, full_stats.hits);
        assert_eq!(stats.hits, vec!["x", "z"]);
    }

    #[test]
    fn key_filter_prunes_before_fetch() {
        let c = MemCorpus::new(
            vec!["logs/a".into(), "other/b".into()],
            vec![b"abc world".to_vec(), b"abc world".to_vec()],
        );
        let (_s, _c, r) = build_tmp(&c, Strategy::Trigram);
        let scope = KeyScope {
            prefix: Some("logs/"),
            matches: None,
        };
        let stats =
            search_streaming(&r, &c, "world", scope, MatchOptions::default(), &NullSink).unwrap();
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
            vec!["a.log.gz".into(), "b.log".into()],
            vec![multi, b"plain needle\n".to_vec()],
        );
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_s, _c, r) = build_tmp(&c, strategy);
            let (matches, stats) = search_collect(&r, &c, "needle").unwrap();
            assert_eq!(
                stats.hits,
                vec!["a.log.gz", "b.log"],
                "strategy {strategy:?}"
            );
            assert_eq!(matches[0].1.line, 2);
            assert_eq!(matches[0].1.text, b"needle in second member\n".to_vec());
        }
    }

    #[test]
    fn sink_stop_ends_search_early_without_error() {
        struct StopAfterFirst;

        impl MatchSink for StopAfterFirst {
            fn on_doc(&self, _key: &str, _doc: &DocResult<'_>) -> Result<SinkFlow> {
                Ok(SinkFlow::Stop)
            }
        }

        let keys = (0..100u32).map(|i| format!("doc{i}")).collect();
        let bodies = (0..100u32)
            .map(|i| format!("needle {i}").into_bytes())
            .collect();
        let c = MemCorpus::new(keys, bodies);
        let (_s, _c, r) = build_tmp(&c, Strategy::Trigram);
        let stats = search_streaming(
            &r,
            &c,
            "needle",
            KeyScope::default(),
            MatchOptions::default(),
            &StopAfterFirst,
        )
        .unwrap();
        // Stop is cooperative: at least one hit was reported, the search
        // ended Ok, and whatever was skipped is simply absent from hits.
        assert!(!stats.hits.is_empty());
        assert_eq!(stats.candidates, 100);
    }
}
