#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Index construction and snapshot-backed search readers.

mod build;
mod delta_blocks;
mod eval;
mod format;
mod pack;
mod postings_table;
mod remote_terms;
mod search;
mod segment;
mod sparse_table;
mod terms;

pub use search::{
    search_collect, search_streaming, DocResult, KeyScope, MatchSink, NullSink, SinkFlow,
};
pub use segment::{
    update_index, CorpusFactory, IndexChanged, SegmentedReader, SourceIdentity, UpdateOptions,
    UpdateReport,
};

#[cfg(test)]
use build::{
    build_chunks, collapse_posting_runs, collect_file_trigrams, merge_posting_runs,
    pack_file_trigrams, write_posting_record, write_posting_runs, write_trigram_run_merge,
    write_trigram_run_radix, IndexedGrams, PostingRun, MAX_OPEN_POSTING_RUNS, SPARSE_FILE_CHUNK,
};
pub(crate) use build::{build_index_files, BuiltIndexFiles, DocumentCapExceeded};

use anyhow::{Context, Result};
use eval::Selection;
use rayon::prelude::*;
#[cfg(test)]
use seagrep_core::pack_trigram_grams;
use seagrep_core::{
    Corpus, DocFetcher, DocId, DocumentBody, DocumentSpool, SourceEncoding, SourceObject, Strategy,
};
use seagrep_query::Query;
#[cfg(test)]
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
#[cfg(test)]
use std::io::{BufReader, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[cfg(not(test))]
const LOCAL_BODY_MEMORY_LIMIT: u64 = 8 * 1024 * 1024;
#[cfg(test)]
const LOCAL_BODY_MEMORY_LIMIT: u64 = 1024;

/// Bumped whenever index semantics change (e.g. grams now cover decompressed
/// bodies); an index built by an older seagrep must error, not silently
/// return wrong results.
const INDEX_FORMAT: u32 = 19;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStats {
    pub distinct_grams: u64,
    pub terms_fst_bytes: u64,
    pub postings_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchStats {
    /// Sorted keys of docs with at least one verified match. Empty when the
    /// sink's `wants_hit_keys` is false; `hit_count` is authoritative.
    pub hits: Vec<String>,
    /// Number of docs with at least one verified match, collected or not.
    pub hit_count: usize,
    pub candidates: usize,
    pub total_docs: usize,
    pub bytes_fetched: usize,
    /// Source objects the index could not decode at build time: their
    /// contents are not searchable, and results cannot include them.
    pub excluded_objects: usize,
}

pub trait IndexReader: DocFetcher {
    fn strategy(&self) -> Strategy;
    fn total_docs(&self) -> usize;
    /// Objects excluded at build time (undecodable); zero when unknown.
    fn excluded_objects(&self) -> usize {
        0
    }
    fn candidate_docs(
        &self,
        q: &Query,
        key_prefix: Option<&str>,
    ) -> Result<Vec<seagrep_core::DocAddress>>;
    fn visit_candidates(
        &self,
        q: &Query,
        key_prefix: Option<&str>,
        batch_size: usize,
        visit: &mut dyn FnMut(Vec<seagrep_core::DocAddress>) -> Result<bool>,
    ) -> Result<()> {
        anyhow::ensure!(batch_size > 0, "candidate batch size must be positive");
        let documents = self.candidate_docs(q, key_prefix)?;
        for chunk in documents.chunks(batch_size) {
            if !visit(chunk.to_vec())? {
                break;
            }
        }
        Ok(())
    }
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
pub(crate) fn candidates_with(
    get: impl Fn(&[u8]) -> Result<Option<eval::TermValue>>,
    doc_count: u32,
    q: &Query,
    fetch_blocks: impl FnOnce(&BTreeMap<u64, (u32, u64)>) -> Result<BTreeMap<u64, Vec<DocId>>>,
) -> Result<Vec<DocId>> {
    let resolved = eval::resolve(q, doc_count, &get)?;
    let mut needed = BTreeMap::new();
    eval::blocks_needed(&resolved, &mut needed);
    let blocks = fetch_blocks(&needed)?;
    match eval::eval(&resolved, &blocks)? {
        Selection::All => Ok((0..doc_count).collect()),
        Selection::Ids(ids) => Ok(ids),
    }
}

pub struct LocalCorpus {
    sources: Vec<SourceObject>,
    paths: Vec<PathBuf>,
}

fn build_local_key(path: &Path) -> Result<String> {
    let key = path
        .to_str()
        .with_context(|| format!("local path is not valid UTF-8: {}", path.display()))?;
    #[cfg(windows)]
    {
        return Ok(key.replace('\\', "/"));
    }
    #[cfg(not(windows))]
    Ok(key.to_owned())
}

impl LocalCorpus {
    /// Walk `root` recursively. Symlinks are skipped, so cycles cannot hang
    /// the walk.
    pub fn new(root: &Path) -> Result<LocalCorpus> {
        Self::new_excluding(root, None)
    }

    pub fn new_excluding(root: &Path, excluded: Option<&Path>) -> Result<LocalCorpus> {
        let mut paths = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(p) = stack.pop() {
            for entry in std::fs::read_dir(&p)? {
                let entry = entry?;
                let path = entry.path();
                if excluded.is_some_and(|excluded| path.starts_with(excluded)) {
                    continue;
                }
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    stack.push(path);
                } else if file_type.is_file() {
                    paths.push(path);
                }
            }
        }
        paths.sort();
        let sources = paths
            .par_iter()
            .map(|p| {
                Ok(SourceObject {
                    key: build_local_key(p)?,
                    version: hash_file(p)?,
                    encoded_size: std::fs::metadata(p)?.len(),
                })
            })
            .collect::<Result<Vec<SourceObject>>>()?;
        Ok(LocalCorpus { sources, paths })
    }

    /// Corpus over exactly the listed files ((key, etag, size) triples; keys
    /// are full paths; ids = positions): the changed subset an incremental
    /// index run fetches.
    pub fn from_listing(listing: &[(String, String, u64)]) -> LocalCorpus {
        let sources = listing
            .iter()
            .map(|(key, version, size)| SourceObject {
                key: key.clone(),
                version: version.clone(),
                encoded_size: *size,
            })
            .collect();
        let paths = listing
            .iter()
            .map(|(key, _, _)| PathBuf::from(key))
            .collect();
        LocalCorpus { sources, paths }
    }

    pub fn listing(&self) -> Result<Vec<(String, String, u64)>> {
        self.sources
            .iter()
            .map(|source| {
                Ok((
                    source.key.clone(),
                    source.version.clone(),
                    source.encoded_size,
                ))
            })
            .collect()
    }
}

fn hash_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = vec![0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut bytes)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&bytes[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn read_local_body(
    path: &Path,
    key: &str,
    expected_version: &str,
    expected_size: u64,
) -> Result<DocumentBody> {
    let stale = || {
        anyhow::Error::new(seagrep_core::StaleSource {
            key: key.to_owned(),
            expected: expected_version.to_owned(),
        })
    };
    let size = std::fs::metadata(path)?.len();
    if size != expected_size {
        return Err(stale());
    }
    if size <= LOCAL_BODY_MEMORY_LIMIT {
        let bytes = bytes::Bytes::from(std::fs::read(path)?);
        if blake3::hash(&bytes).to_hex().as_str() != expected_version {
            return Err(stale());
        }
        return Ok(DocumentBody::from_bytes(bytes));
    }
    let mut input = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut body = DocumentSpool::new(size)?;
    let mut hasher = blake3::Hasher::new();
    let mut chunk = [0u8; 64 * 1024];
    let mut at = 0u64;
    loop {
        let read = input
            .read(&mut chunk)
            .with_context(|| format!("read local document {}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
        body.write_at(at, &chunk[..read])?;
        at = at
            .checked_add(u64::try_from(read)?)
            .context("local document length overflows")?;
    }
    if at != size || hasher.finalize().to_hex().as_str() != expected_version {
        return Err(stale());
    }
    body.finish()
}

impl Corpus for LocalCorpus {
    fn sources(&self) -> &[SourceObject] {
        &self.sources
    }

    fn fetch(&self, idx: usize) -> Result<bytes::Bytes> {
        let path = self
            .paths
            .get(idx)
            .with_context(|| format!("document index {idx} is out of range"))?;
        let source = &self.sources[idx];
        read_local_body(path, &source.key, &source.version, source.encoded_size)?.into_bytes()
    }

    fn fetch_many(&self, sources: Range<usize>) -> Result<Vec<(usize, bytes::Bytes)>> {
        sources
            .into_par_iter()
            .map(|idx| {
                let path = self
                    .paths
                    .get(idx)
                    .with_context(|| format!("document index {idx} is out of range"))?;
                let source = &self.sources[idx];
                Ok((
                    idx,
                    read_local_body(path, &source.key, &source.version, source.encoded_size)?
                        .into_bytes()?,
                ))
            })
            .collect()
    }

    fn fetch_bodies(&self, sources: Range<usize>) -> Result<Vec<(usize, DocumentBody)>> {
        sources
            .into_par_iter()
            .map(|idx| {
                let path = self
                    .paths
                    .get(idx)
                    .with_context(|| format!("document index {idx} is out of range"))?;
                let source = &self.sources[idx];
                Ok((
                    idx,
                    read_local_body(path, &source.key, &source.version, source.encoded_size)?,
                ))
            })
            .collect()
    }
}

/// Direct local-source candidate fetcher retained for library tests.
/// The product CLI reads canonical bodies from index snapshot packs instead.
pub struct LocalFetcher {
    concurrency: usize,
}

impl LocalFetcher {
    pub fn new(concurrency: usize) -> Result<LocalFetcher> {
        anyhow::ensure!(
            concurrency > 0,
            "local fetch concurrency must be greater than 0"
        );
        Ok(LocalFetcher { concurrency })
    }
}

const LOCAL_FETCH_BYTES: u64 = 512 * 1024 * 1024;

struct LocalFetchGroup {
    key: String,
    version: String,
    encoded_size: u64,
    encoding: SourceEncoding,
    requests: Vec<(usize, Option<String>)>,
}

fn read_local_group(
    group: &LocalFetchGroup,
    consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
) -> Result<()> {
    let body = read_local_body(
        Path::new(&group.key),
        &group.key,
        &group.version,
        group.encoded_size,
    )?;
    seagrep_core::decode_requested_body(&group.key, &group.requests, body, consume)
}

fn fetch_local_parallel(
    groups: &[&LocalFetchGroup],
    workers: usize,
    consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
) -> Result<()> {
    let next = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    let (sender, receiver) =
        std::sync::mpsc::sync_channel::<Result<(usize, DocumentBody)>>(workers * 2);
    let failure = std::thread::scope(|scope| {
        let next = &next;
        let cancelled = &cancelled;
        for _ in 0..workers {
            let sender = sender.clone();
            scope.spawn(move || {
                while !cancelled.load(Ordering::Relaxed) {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(group) = groups.get(index) else {
                        break;
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        read_local_group(group, &mut |index, bytes| {
                            sender
                                .send(Ok((index, bytes)))
                                .map_err(|_| anyhow::anyhow!("local fetch consumer exited early"))
                        })
                    }))
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("a local fetch worker panicked")));
                    if let Err(error) = result {
                        cancelled.store(true, Ordering::Relaxed);
                        let _ = sender.send(Err(error));
                        break;
                    }
                }
            });
        }
        drop(sender);
        let mut failure = None;
        while let Ok(delivery) = receiver.recv() {
            let result = delivery.and_then(|(index, bytes)| consume(index, bytes));
            if let Err(error) = result {
                cancelled.store(true, Ordering::Relaxed);
                failure = Some(error);
                break;
            }
        }
        drop(receiver);
        failure
    });
    match failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

impl DocFetcher for LocalFetcher {
    fn fetch_each(
        &self,
        documents: &[seagrep_core::DocAddress],
        consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
    ) -> Result<()> {
        let mut groups = BTreeMap::new();
        for (idx, document) in documents.iter().enumerate() {
            let group = groups
                .entry((document.source_key.clone(), document.source_version.clone()))
                .or_insert_with(|| {
                    (
                        document.encoded_size,
                        document.encoding,
                        Vec::<(usize, Option<String>)>::new(),
                    )
                });
            anyhow::ensure!(
                group.0 == document.encoded_size && group.1 == document.encoding,
                "index has inconsistent metadata for {}",
                document.source_key
            );
            group.2.push((idx, document.member_path.clone()));
        }
        let groups = groups
            .into_iter()
            .map(
                |((key, version), (encoded_size, encoding, requests))| LocalFetchGroup {
                    key,
                    version,
                    encoded_size,
                    encoding,
                    requests,
                },
            )
            .collect::<Vec<_>>();
        let available = self
            .concurrency
            .min(std::thread::available_parallelism()?.get());
        let raw_count = groups
            .iter()
            .filter(|group| group.encoding == SourceEncoding::Raw)
            .count();
        let workers = available.min(raw_count);
        let per_source = LOCAL_FETCH_BYTES / u64::try_from(workers.max(1))?;
        let (parallel, serial): (Vec<_>, Vec<_>) = groups.iter().partition(|group| {
            workers > 1 && group.encoding == SourceEncoding::Raw && group.encoded_size <= per_source
        });
        if !parallel.is_empty() {
            fetch_local_parallel(&parallel, workers.min(parallel.len()), consume)?;
        }
        for group in serial {
            read_local_group(group, consume)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seagrep_core::testutil::MemCorpus;
    use seagrep_core::BlobStore as _;
    use seagrep_core::{LineEvent, LineKind, LocalBlobStore, MatchOptions, SubMatch};

    fn test_source() -> SourceIdentity {
        SourceIdentity::Local {
            prefix: "/test/".into(),
        }
    }

    #[test]
    fn index_build_writes_decoded_documents_to_content_packs() {
        let corpus = MemCorpus::new(
            vec!["a.txt".into(), "b.txt".into()],
            vec![b"alpha".to_vec(), b"beta".to_vec()],
        );
        let built = build_index_files(&corpus, Strategy::Trigram, None, None).unwrap();

        assert_eq!(built.packs.len(), 1);
        assert_eq!(built.tables.blocks.len(), 1);
        assert_eq!(built.tables.documents[0].first_block, 0);
        assert_eq!(built.tables.documents[0].block_offset, 0);
        assert_eq!(built.tables.documents[1].first_block, 0);
        assert_eq!(built.tables.documents[1].block_offset, 5);
    }

    struct OutOfRangeCorpus {
        sources: Vec<SourceObject>,
    }

    impl Corpus for OutOfRangeCorpus {
        fn sources(&self) -> &[SourceObject] {
            &self.sources
        }

        fn fetch(&self, index: usize) -> Result<bytes::Bytes> {
            Ok(bytes::Bytes::from(format!("document {index}")))
        }

        fn fetch_many(&self, sources: Range<usize>) -> Result<Vec<(usize, bytes::Bytes)>> {
            Ok(vec![(
                sources.end,
                bytes::Bytes::from_static(b"outside requested range"),
            )])
        }
    }

    fn build_tmp(
        c: &MemCorpus,
        strategy: Strategy,
    ) -> (tempfile::TempDir, tempfile::TempDir, SegmentedReader) {
        let store_dir = tempfile::tempdir().unwrap();
        let cache_dir = tempfile::tempdir().unwrap();
        let listing: Vec<(String, String, u64)> = c
            .sources()
            .iter()
            .map(|source| {
                (
                    source.key.clone(),
                    source.version.clone(),
                    source.encoded_size,
                )
            })
            .collect();
        update_index(
            &LocalBlobStore::new(store_dir.path()),
            cache_dir.path(),
            &test_source(),
            Some(strategy),
            &listing,
            UpdateOptions::default(),
            &|shard| {
                let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
                let bodies = keys
                    .iter()
                    .map(|key| {
                        let idx = c
                            .sources()
                            .iter()
                            .position(|source| source.key == *key)
                            .expect("listed key exists");
                        Ok(c.fetch(idx)?.to_vec())
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Box::new(MemCorpus::new(keys, bodies)))
            },
        )
        .unwrap();
        let r = SegmentedReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
            &test_source(),
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
    fn rejects_out_of_range_fetch_results() {
        let corpus = OutOfRangeCorpus {
            sources: vec![SourceObject {
                key: "document".to_owned(),
                version: "version".to_owned(),
                encoded_size: 8,
            }],
        };
        let error = build_index_files(&corpus, Strategy::Trigram, None, None)
            .err()
            .expect("out-of-range fetch result should fail");
        assert!(
            error
                .to_string()
                .contains("fetch_many returned out-of-range document 1"),
            "{error:#}"
        );
    }

    #[test]
    fn merged_blobs_stream_with_truthful_hashes() {
        let corpus = MemCorpus::new(
            vec!["a.log".to_owned(), "b.log".to_owned(), "c.log".to_owned()],
            vec![
                b"alpha needle".to_vec(),
                b"beta needle".to_vec(),
                b"gamma unrelated".to_vec(),
            ],
        );
        let built = build_index_files(&corpus, Strategy::Trigram, None, None).unwrap();
        let doc_count = u32::try_from(built.tables.documents.len()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::new(dir.path());
        let (fst, postings, tail, _postings_tail) = merge_posting_runs(
            built.runs,
            Strategy::Trigram,
            doc_count,
            store.put_streaming("terms.fst").unwrap(),
            store.put_streaming("postings.bin").unwrap(),
        )
        .unwrap();
        assert!(tail.is_empty(), "trigram dictionaries have no sparse tail");
        for (name, len, hash) in [
            ("terms.fst", fst.len, &fst.hash),
            ("postings.bin", postings.len, &postings.hash),
        ] {
            let bytes = store.get(name).unwrap().unwrap();
            assert!(!bytes.is_empty());
            assert_eq!(bytes.len() as u64, len);
            let expected = Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            assert_eq!(hash, &expected);
        }
    }

    #[test]
    fn file_trigrams_match_memory_trigrams() {
        for len in [0usize, 1, 2, 3, 65_535, 65_536, 65_537, 1_100_003] {
            let bytes = (0..len)
                .map(|index| u8::try_from((index * 31 + index / 7) % 251).unwrap())
                .collect::<Vec<_>>();
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(b"prefix").unwrap();
            file.write_all(&bytes).unwrap();
            assert_eq!(
                pack_file_trigrams(&mut file, 6, u64::try_from(len).unwrap()).unwrap(),
                pack_trigram_grams(&bytes),
                "length {len}"
            );
        }
    }

    #[test]
    fn file_trigram_bitmap_run_matches_packed_run() {
        let bytes = (0..1_100_003usize)
            .map(|index| u8::try_from((index * 31 + index / 7) % 251).unwrap())
            .collect::<Vec<_>>();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        let indexed =
            collect_file_trigrams(&mut file, 0, u64::try_from(bytes.len()).unwrap()).unwrap();
        assert!(matches!(indexed, IndexedGrams::TrigramBitmap(_)));
        let actual = write_posting_runs(
            vec![(
                0,
                IndexedGrams::TrigramSpool {
                    offset: 0,
                    len: u64::try_from(bytes.len()).unwrap(),
                },
            )],
            Strategy::Trigram,
            1024,
            Some(&file),
        )
        .unwrap();
        let expected = write_trigram_run_radix(vec![(0, pack_trigram_grams(&bytes))]).unwrap();
        assert_eq!(
            std::fs::read(&actual[0]).unwrap(),
            std::fs::read(expected).unwrap()
        );
    }

    #[test]
    fn trigram_term_map_shards_prefixes() {
        let mut builder = terms::TermBuilder::new(Strategy::Trigram, true, Vec::new()).unwrap();
        builder.insert(&[0x00, 0x01, 0x02], 1, None).unwrap();
        builder.insert(&[0x7f, 0x03, 0x04], 2, None).unwrap();
        builder.insert(&[0xff, 0x05, 0x06], 3, None).unwrap();
        let (bytes, _) = builder.finish().unwrap();
        assert_eq!(&bytes[..8], b"SGTERM01");
    }

    /// Merge runs into a scratch store and return (terms bytes, postings
    /// bytes, sparse tail hash) — the streamed replacement for reading the
    /// old temp-file blobs.
    fn merge_to_store(
        runs: Vec<tempfile::TempPath>,
        strategy: Strategy,
        doc_count: u32,
    ) -> (Vec<u8>, Vec<u8>, String) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::new(dir.path());
        let (fst, postings, tail, _postings_tail) = merge_posting_runs(
            runs,
            strategy,
            doc_count,
            store.put_streaming("terms.fst").unwrap(),
            store.put_streaming("postings.bin").unwrap(),
        )
        .unwrap();
        let terms_bytes = store.get("terms.fst").unwrap().unwrap();
        let postings_bytes = store.get("postings.bin").unwrap().unwrap();
        assert_eq!(terms_bytes.len() as u64, fst.len);
        assert_eq!(postings_bytes.len() as u64, postings.len);
        (terms_bytes, postings_bytes, tail)
    }

    #[test]
    fn small_trigram_term_map_stays_single() {
        let runs = write_posting_runs(
            vec![(0, IndexedGrams::Trigram(vec![0x000102, 0x7f0304, 0xff0506]))],
            Strategy::Trigram,
            1024,
            None,
        )
        .unwrap();
        let (bytes, _, _) = merge_to_store(runs, Strategy::Trigram, 1);
        assert_ne!(&bytes[..8], b"SGTERM01");
    }

    #[test]
    fn splits_sparse_postings_into_bounded_runs() {
        let text = (0..16_384usize)
            .map(|index| ((index * 31 + index / 7) % 251) as u8)
            .collect::<Vec<_>>();
        let expected = seagrep_core::sparse_grams_all_bytes(&text);
        let runs = write_posting_runs(
            vec![(0, IndexedGrams::Sparse(text.into()))],
            Strategy::Sparse,
            1024,
            None,
        )
        .unwrap();
        assert!(runs.len() > 1);
        let (table, postings, tail_hash) = merge_to_store(runs, Strategy::Sparse, 1);
        assert_eq!(tail_hash.len(), 64);
        let index = sparse_table::SparseTableIndex::parse(table.len() as u64, &table).unwrap();
        let lookup = |hash: u64| -> Option<u64> {
            let block = &index.blocks[index.block_for(hash)?];
            let start = usize::try_from(block.offset).unwrap();
            let end = start + usize::try_from(block.len).unwrap();
            sparse_table::lookup_in_block(&table[start..end], hash).unwrap()
        };
        let mut expected_hashes: Vec<u64> = expected
            .iter()
            .map(|gram| seagrep_core::hash_ngram(gram))
            .collect();
        expected_hashes.sort_unstable();
        expected_hashes.dedup();
        assert_eq!(index.entry_count, expected_hashes.len() as u64);
        for gram in expected {
            let packed = lookup(seagrep_core::hash_ngram(&gram)).expect("indexed sparse gram");
            let (offset, count) = eval::unpack_posting(packed);
            let start = usize::try_from(offset).unwrap();
            let end = start + usize::try_from(posting_block_len(count, 1)).unwrap();
            assert_eq!(
                decode_posting_block(&postings[start..end], count, 1).unwrap(),
                [0]
            );
        }
    }

    #[test]
    fn parallel_spooled_runs_match_memory() {
        let mut spool = tempfile::tempfile().unwrap();
        let mut memory_docs = Vec::new();
        let mut spooled_docs = Vec::new();
        let mut offset = 0u64;
        for idx in 0..64 {
            let text = (0..SPARSE_FILE_CHUNK / 3 + idx * 31)
                .map(|byte| b'a' + u8::try_from((byte * 7 + idx) % 23).unwrap())
                .collect::<Vec<_>>();
            spool.write_all(&text).unwrap();
            let len = u64::try_from(text.len()).unwrap();
            memory_docs.push((idx, IndexedGrams::Sparse(text.into())));
            spooled_docs.push((idx, IndexedGrams::SparseSpool { offset, len }));
            offset += len;
        }
        let memory = write_posting_runs(memory_docs, Strategy::Sparse, 1 << 20, None).unwrap();
        let spooled =
            write_posting_runs(spooled_docs, Strategy::Sparse, 1 << 20, Some(&spool)).unwrap();
        let (memory_fst, memory_postings, _) = merge_to_store(memory, Strategy::Sparse, 64);
        let (spooled_fst, spooled_postings, _) = merge_to_store(spooled, Strategy::Sparse, 64);
        assert_eq!(memory_fst, spooled_fst);
        assert_eq!(memory_postings, spooled_postings);
    }

    #[test]
    fn sparse_file_runs_match_memory_across_chunks() {
        let text = (0..SPARSE_FILE_CHUNK + 17)
            .map(|index| if index % 2 == 0 { b'a' } else { b'b' })
            .collect::<Vec<_>>();
        let memory = write_posting_runs(
            vec![(0, IndexedGrams::Sparse(text.clone().into()))],
            Strategy::Sparse,
            1024,
            None,
        )
        .unwrap();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&text).unwrap();
        let streamed = write_posting_runs(
            vec![(0, IndexedGrams::SparseFile(file))],
            Strategy::Sparse,
            1024,
            None,
        )
        .unwrap();
        let mut spool = tempfile::tempfile().unwrap();
        spool.write_all(b"prefix").unwrap();
        spool.write_all(&text).unwrap();
        spool.write_all(b"suffix").unwrap();
        let spooled = write_posting_runs(
            vec![(
                0,
                IndexedGrams::SparseSpool {
                    offset: 6,
                    len: u64::try_from(text.len()).unwrap(),
                },
            )],
            Strategy::Sparse,
            1024,
            Some(&spool),
        )
        .unwrap();
        let (memory_fst, memory_postings, _) = merge_to_store(memory, Strategy::Sparse, 1);
        let (streamed_fst, streamed_postings, _) = merge_to_store(streamed, Strategy::Sparse, 1);
        let (spooled_fst, spooled_postings, _) = merge_to_store(spooled, Strategy::Sparse, 1);
        assert_eq!(memory_fst, streamed_fst);
        assert_eq!(memory_postings, streamed_postings);
        assert_eq!(memory_fst, spooled_fst);
        assert_eq!(memory_postings, spooled_postings);
    }

    #[test]
    fn posting_run_merge_bounds_open_files() {
        let runs = (0..MAX_OPEN_POSTING_RUNS * 4)
            .map(|id| {
                let mut file = tempfile::NamedTempFile::new().unwrap();
                write_posting_record(
                    &mut file,
                    Strategy::Trigram,
                    u64::from(u32::from_be_bytes(*b"\0abc")),
                    DocId::try_from(id).unwrap(),
                )
                .unwrap();
                file.flush().unwrap();
                file.into_temp_path()
            })
            .collect();
        let collapsed = collapse_posting_runs(runs, Strategy::Trigram).unwrap();
        assert!(collapsed.len() <= MAX_OPEN_POSTING_RUNS);
        let mut ids = collapsed
            .into_iter()
            .flat_map(|path| {
                let mut run = PostingRun {
                    reader: BufReader::new(File::open(&path).unwrap()),
                    strategy: Strategy::Trigram,
                };
                let mut ids = Vec::new();
                while let Some((_, id)) = run.read_record().unwrap() {
                    ids.push(id);
                }
                ids
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(
            ids,
            (0..DocId::try_from(MAX_OPEN_POSTING_RUNS * 4).unwrap()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn trigram_run_algorithms_are_byte_identical() {
        let documents = (0..257usize)
            .rev()
            .map(|id| {
                let mut grams = vec![(id % 31) as u32, (id % 7) as u32, 0x61_62_63, 0x61_62_63];
                grams.sort_unstable();
                grams.dedup();
                (id, grams)
            })
            .collect::<Vec<_>>();
        let radix = write_trigram_run_radix(documents.clone()).unwrap();
        let merged = write_trigram_run_merge(documents).unwrap();
        let radix_bytes = std::fs::read(radix).unwrap();
        let merged_bytes = std::fs::read(merged).unwrap();
        assert_eq!(radix_bytes, merged_bytes);
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
    fn build_chunks_bounds_encoded_bytes() {
        let mib = 1024 * 1024;
        let sources = [40, 30, 70, 1]
            .into_iter()
            .enumerate()
            .map(|(index, size)| SourceObject {
                key: index.to_string(),
                version: index.to_string(),
                encoded_size: size * mib,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            build_chunks(&sources).collect::<Vec<_>>(),
            [0..1, 1..2, 2..3, 3..4]
        );
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
                .candidate_docs(&seagrep_query::plan("world", r.strategy()).unwrap(), None)
                .unwrap();
            assert!(cands.iter().any(|document| document.display_key == "x"));
            assert!(cands
                .iter()
                .all(|document| document.display_key == "x" || document.display_key == "y"));
        }
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus::new(vec!["x".into()], vec![b"abcdef".to_vec()]);
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_s, _c, r) = build_tmp(&c, strategy);
            assert_eq!(
                r.candidate_docs(&Query::All, None)
                    .unwrap()
                    .into_iter()
                    .map(|document| document.display_key)
                    .collect::<Vec<_>>(),
                vec!["x"]
            );
        }
    }

    #[test]
    fn search_collect_returns_verified_matches_and_stats() {
        let c = MemCorpus::new(
            vec!["x".into(), "y".into()],
            vec![b"abc world".to_vec(), b"nomatch".to_vec()],
        );
        let (_s, _c, r) = build_tmp(&c, Strategy::Trigram);
        let (matches, stats) = search_collect(&r, "world").unwrap();
        assert_eq!(
            matches,
            vec![(
                "x".to_owned(),
                LineEvent {
                    line: 1,
                    kind: LineKind::Match,
                    offset: 0,
                    text: bytes::Bytes::from_static(b"abc world"),
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
            "world",
            KeyScope::default(),
            MatchOptions::default(),
            &NullSink,
        )
        .unwrap();
        let (_, full_stats) = search_collect(&r, "world").unwrap();
        assert_eq!(stats.hits, full_stats.hits);
        assert_eq!(stats.hits, vec!["x", "z"]);
    }

    #[test]
    fn count_only_sink_agrees_with_collected_hits() {
        struct CountOnlySink;
        impl MatchSink for CountOnlySink {
            fn wants_matches(&self) -> bool {
                false
            }
            fn wants_hit_keys(&self) -> bool {
                false
            }
            fn on_doc(&self, _key: &str, _doc: &DocResult<'_>) -> Result<SinkFlow> {
                Ok(SinkFlow::Continue)
            }
        }
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
            "world",
            KeyScope::default(),
            MatchOptions::default(),
            &CountOnlySink,
        )
        .unwrap();
        let (_, full_stats) = search_collect(&r, "world").unwrap();
        assert!(stats.hits.is_empty(), "count-only mode must not collect");
        assert_eq!(stats.hit_count, full_stats.hits.len());
        assert_eq!(full_stats.hit_count, full_stats.hits.len());
        assert_eq!(stats.hit_count, 2);
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
            search_streaming(&r, "world", scope, MatchOptions::default(), &NullSink).unwrap();
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
            let (matches, stats) = search_collect(&r, "needle").unwrap();
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

    #[test]
    fn local_listing_is_ordered_blake3() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("b.log"), b"beta").unwrap();
        std::fs::write(root.path().join("a.log"), b"alpha").unwrap();
        let corpus = LocalCorpus::new(root.path()).unwrap();
        let listing = corpus.listing().unwrap();
        assert!(listing[0].0.ends_with("a.log"));
        assert!(listing[1].0.ends_with("b.log"));
        assert_eq!(listing[0].1, blake3::hash(b"alpha").to_hex().as_str());
        assert_eq!(listing[1].1, blake3::hash(b"beta").to_hex().as_str());
        assert_eq!(
            corpus.fetch_many(0..2).unwrap(),
            vec![
                (0, bytes::Bytes::from_static(b"alpha")),
                (1, bytes::Bytes::from_static(b"beta"))
            ]
        );
    }

    #[test]
    fn local_large_sources_stay_file_backed() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("large.log");
        let expected = vec![b'x'; usize::try_from(LOCAL_BODY_MEMORY_LIMIT).unwrap() + 1];
        std::fs::write(&path, &expected).unwrap();
        let corpus = LocalCorpus::new(root.path()).unwrap();
        let mut bodies = corpus.fetch_bodies(0..1).unwrap();
        let (_, body) = bodies.pop().unwrap();
        assert!(body.is_file());
        assert_eq!(body.into_bytes().unwrap(), expected);

        let source = &corpus.sources()[0];
        let document = seagrep_core::DocAddress {
            display_key: source.key.clone(),
            source_key: source.key.clone(),
            source_version: source.version.clone(),
            encoded_size: source.encoded_size,
            encoding: SourceEncoding::Raw,
            member_path: None,
            index: None,
        };
        LocalFetcher::new(1)
            .unwrap()
            .fetch_each(&[document], &mut |_, body| {
                assert!(body.is_file());
                assert_eq!(body.into_bytes()?, expected);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn local_corpus_skips_excluded_subtrees() {
        let root = tempfile::tempdir().unwrap();
        let excluded = root.path().join("index");
        std::fs::create_dir(&excluded).unwrap();
        std::fs::write(root.path().join("source.log"), b"source").unwrap();
        std::fs::write(excluded.join("postings.bin"), b"index").unwrap();
        let corpus = LocalCorpus::new_excluding(root.path(), Some(&excluded)).unwrap();
        assert_eq!(corpus.sources.len(), 1);
        assert!(corpus.sources[0].key.ends_with("source.log"));
    }

    #[test]
    fn local_fetch_rejects_stale_source_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event.log");
        std::fs::write(&path, b"alpha").unwrap();
        let key = path.to_string_lossy().into_owned();
        let document = seagrep_core::DocAddress {
            display_key: key.clone(),
            source_key: key.clone(),
            source_version: blake3::hash(b"alpha").to_hex().to_string(),
            encoded_size: 5,
            encoding: SourceEncoding::Raw,
            member_path: None,
            index: None,
        };
        std::fs::write(&path, b"bravo").unwrap();
        let error = LocalFetcher::new(1)
            .unwrap()
            .fetch_each(&[document], &mut |_, _| Ok(()))
            .unwrap_err();
        assert!(error.is::<seagrep_core::StaleSource>(), "{error:#}");
    }

    #[test]
    fn local_fetch_parallel_delivers_all_and_stops_on_consumer_error() {
        let dir = tempfile::tempdir().unwrap();
        let documents = (0..64)
            .map(|index| {
                let path = dir.path().join(format!("event-{index}.log"));
                let body = format!("event {index}");
                std::fs::write(&path, &body).unwrap();
                let key = path.to_string_lossy().into_owned();
                seagrep_core::DocAddress {
                    display_key: key.clone(),
                    source_key: key,
                    source_version: blake3::hash(body.as_bytes()).to_hex().to_string(),
                    encoded_size: u64::try_from(body.len()).unwrap(),
                    encoding: SourceEncoding::Raw,
                    member_path: None,
                    index: None,
                }
            })
            .collect::<Vec<_>>();
        let fetcher = LocalFetcher::new(8).unwrap();
        let mut delivered = Vec::new();
        fetcher
            .fetch_each(&documents, &mut |index, _| {
                delivered.push(index);
                Ok(())
            })
            .unwrap();
        delivered.sort_unstable();
        assert_eq!(delivered, (0..64).collect::<Vec<_>>());
        let error = fetcher
            .fetch_each(&documents, &mut |_, _| anyhow::bail!("stop local fetch"))
            .unwrap_err();
        assert!(error.to_string().contains("stop local fetch"), "{error:#}");
    }

    #[test]
    fn local_build_rejects_stale_source_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("event.log");
        std::fs::write(&path, b"alpha").unwrap();
        let corpus = LocalCorpus::new(dir.path()).unwrap();
        std::fs::write(&path, b"bravo").unwrap();
        let error = corpus.fetch(0).unwrap_err();
        assert!(error.is::<seagrep_core::StaleSource>(), "{error:#}");
        let error = corpus.fetch_many(0..1).unwrap_err();
        assert!(error.is::<seagrep_core::StaleSource>(), "{error:#}");
    }

    #[test]
    fn parallel_hashing_fits_worker_stacks() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("object.log");
        std::fs::write(&path, b"production-shaped log body").unwrap();
        let sources = (0..25_000usize)
            .map(|id| SourceObject {
                key: format!("object-{id}.log"),
                version: format!("version-{id}"),
                encoded_size: 26,
            })
            .collect();
        let corpus = LocalCorpus {
            sources,
            paths: vec![path; 25_000],
        };
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(18)
            .build()
            .unwrap();
        let listing = pool.install(|| corpus.listing());
        assert_eq!(listing.unwrap().len(), 25_000);
    }

    #[cfg(unix)]
    #[test]
    fn local_corpus_rejects_non_utf8_paths() {
        use std::os::unix::ffi::OsStringExt;

        let path = PathBuf::from(std::ffi::OsString::from_vec(b"invalid-\xff".to_vec()));
        let err = build_local_key(&path).expect_err("non-UTF-8 path should fail");
        assert!(err.to_string().contains("valid UTF-8"), "{err:#}");
    }
}
