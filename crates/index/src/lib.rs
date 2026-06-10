#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Index construction and local or store-backed index readers.

mod eval;
mod search;
mod segment;

pub use search::{search_collect, search_streaming, KeyScope, MatchSink, NullSink, SinkFlow};
pub use segment::{update_index, CorpusFactory, SegmentedReader, UpdateReport};

use anyhow::{Context, Result};
use eval::Selection;
use holys3_core::{decode_body, grams_index, hash_ngram, Corpus, DocFetcher, DocId, Strategy};
use holys3_query::Query;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Docs are fetched and gram-extracted in chunks of this many, bounding build
/// memory to one chunk of bodies instead of the whole corpus.
const BUILD_FETCH_CHUNK: usize = 1024;

/// Bumped whenever index semantics change (e.g. grams now cover decompressed
/// bodies); an index built by an older holys3 must error, not silently
/// return wrong results.
const INDEX_FORMAT: u32 = 3;

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

pub(crate) fn decode_ids(bytes: &[u8], count: u32) -> Result<Vec<DocId>> {
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
/// Returns local ids in `0..doc_count`.
pub(crate) fn candidates_with<D: AsRef<[u8]>>(
    map: &fst::Map<D>,
    doc_count: u32,
    q: &Query,
    fetch_blocks: impl FnOnce(&BTreeMap<u64, u32>) -> Result<BTreeMap<u64, Vec<DocId>>>,
) -> Result<Vec<DocId>> {
    let resolved = eval::resolve(q, &|gram| map.get(gram));
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
/// failed to decompress — segment writers tombstone their etags so the next
/// incremental run retries them.
fn build_index_bytes(
    corpus: &dyn Corpus,
    strategy: Strategy,
) -> Result<(Vec<u8>, Vec<u8>, Vec<DocId>)> {
    let mut postings: BTreeMap<Vec<u8>, Vec<DocId>> = BTreeMap::new();
    let doc_keys = corpus.docs();
    let ids = doc_keys.iter().map(|&(id, _)| id).collect::<Vec<_>>();
    let mut ungrammed: Vec<DocId> = Vec::new();
    for chunk in ids.chunks(BUILD_FETCH_CHUNK) {
        let fetched = corpus.fetch_many(chunk)?;
        let mut seen = vec![false; chunk.len()];
        let base = chunk[0];
        let docs = fetched
            .into_par_iter()
            .filter_map(
                |(id, bytes)| match decode_body(&doc_keys[id as usize].1, bytes) {
                    Ok(text) => Some((id, grams_index(&text, strategy))),
                    Err(err) => {
                        eprintln!("warning: {err:#}; object excluded from index");
                        None
                    }
                },
            )
            .collect::<Vec<_>>();
        for (id, _) in &docs {
            seen[(id - base) as usize] = true;
        }
        ungrammed.extend(
            chunk
                .iter()
                .zip(&seen)
                .filter(|(_, seen)| !**seen)
                .map(|(&id, _)| id),
        );
        for (id, grams) in docs {
            for gram in grams {
                postings.entry(gram).or_default().push(id);
            }
        }
    }
    if !ungrammed.is_empty() {
        eprintln!(
            "warning: {} objects vanished or could not be decompressed and were excluded",
            ungrammed.len()
        );
    }
    let (fst_bytes, postings_buf) = serialize_postings(postings)?;
    ungrammed.sort_unstable();
    Ok((fst_bytes, postings_buf, ungrammed))
}

/// THE postings format: per gram, sorted deduped doc ids as u32 LE runs in
/// postings.bin; the fst maps gram -> packed (offset, count). Shared by
/// fresh builds and compaction merges so the format is defined once.
pub(crate) fn serialize_postings(
    postings: BTreeMap<Vec<u8>, Vec<DocId>>,
) -> Result<(Vec<u8>, Vec<u8>)> {
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
    let (fst_bytes, postings_buf, _) = build_index_bytes(corpus, strategy)?;
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
fn compute_build_id(objects: &[(String, String)], strategy: Strategy) -> String {
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
        let ids = candidates_with(&self.map, self.docs.len() as u32, q, |needed| {
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
    use holys3_core::Match;

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
}
