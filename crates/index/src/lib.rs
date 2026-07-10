#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Index construction and local or store-backed index readers.

mod eval;
mod format;
mod search;
mod segment;

pub use search::{
    search_collect, search_streaming, DocResult, KeyScope, MatchSink, NullSink, SinkFlow,
};
pub use segment::{update_index, CorpusFactory, IndexChanged, SegmentedReader, UpdateReport};

use anyhow::{Context, Result};
use eval::Selection;
use format::{DocEntry, SegmentTables, SourceEntry};
use holys3_core::{
    decode_source, is_raw_source, iterate_sparse_grams, pack_trigram_grams, Corpus, DecodeSink,
    DocFetcher, DocId, LogicalDocumentMeta, SourceEncoding, SourceObject, Strategy, DECODE_LIMITS,
};
use holys3_query::Query;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Docs are fetched and gram-extracted in chunks bounded BOTH by doc count
/// and by total (compressed) bytes, so neither many-small nor few-huge
/// objects blow build memory.
const BUILD_FETCH_CHUNK: usize = 1280;
const BUILD_FETCH_BYTES: u64 = 64 * 1024 * 1024;
const SPARSE_RUN_BYTES: usize = 16 * 1024 * 1024;

/// Greedy chunk boundaries over `docs()` positions respecting both caps; a
/// single over-budget doc still forms its own chunk.
fn build_chunks(sources: &[SourceObject]) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut start = 0usize;
    std::iter::from_fn(move || {
        if start >= sources.len() {
            return None;
        }
        let mut end = start;
        let mut bytes = 0u64;
        while end < sources.len() && end - start < BUILD_FETCH_CHUNK {
            let size = sources[end].encoded_size;
            if end > start && size > BUILD_FETCH_BYTES.saturating_sub(bytes) {
                break;
            }
            bytes = bytes.saturating_add(size);
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
const INDEX_FORMAT: u32 = 8;

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
    fn candidate_docs(
        &self,
        q: &Query,
        key_prefix: Option<&str>,
    ) -> Result<Vec<holys3_core::DocAddress>>;
    fn visit_candidates(
        &self,
        q: &Query,
        key_prefix: Option<&str>,
        batch_size: usize,
        visit: &mut dyn FnMut(Vec<holys3_core::DocAddress>) -> Result<bool>,
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
/// failed to decompress. Transient fetch misses retry on the next run;
/// unchanged decode failures wait for the object to change.
pub(crate) struct TempBlob {
    file: tempfile::NamedTempFile,
    len: u64,
    hash: String,
}

impl TempBlob {
    pub(crate) fn path(&self) -> &Path {
        self.file.path()
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }
}

struct HashWriter<W> {
    inner: W,
    hasher: Sha256,
}

impl<W> HashWriter<W> {
    fn new(inner: W) -> HashWriter<W> {
        HashWriter {
            inner,
            hasher: Sha256::new(),
        }
    }

    fn finish(self) -> (W, String) {
        let hash = self
            .hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        (self.inner, hash)
    }
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) struct BuiltIndexFiles {
    pub fst: TempBlob,
    pub postings: TempBlob,
    pub tables: SegmentTables,
}

struct BuiltDocument {
    meta: LogicalDocumentMeta,
    grams: IndexedGrams,
    decoded_size: u64,
}

struct IndexDecodeSink {
    strategy: Strategy,
    current_meta: Option<LogicalDocumentMeta>,
    current_bytes: Vec<bytes::Bytes>,
    documents: Vec<BuiltDocument>,
}

impl IndexDecodeSink {
    fn new(strategy: Strategy) -> Self {
        Self {
            strategy,
            current_meta: None,
            current_bytes: Vec::new(),
            documents: Vec::new(),
        }
    }
}

impl DecodeSink for IndexDecodeSink {
    fn begin(&mut self, document: &LogicalDocumentMeta) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_none(),
            "decoder began a document before finishing the previous document"
        );
        self.current_meta = Some(document.clone());
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_some(),
            "decoder wrote bytes before beginning a document"
        );
        self.current_bytes
            .push(bytes::Bytes::copy_from_slice(bytes));
        Ok(())
    }

    fn write_bytes(&mut self, bytes: bytes::Bytes) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_some(),
            "decoder wrote bytes before beginning a document"
        );
        self.current_bytes.push(bytes);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let meta = self
            .current_meta
            .take()
            .context("decoder finished without beginning a document")?;
        let mut chunks = std::mem::take(&mut self.current_bytes);
        let bytes = match chunks.len() {
            0 => bytes::Bytes::new(),
            1 => chunks.pop().expect("one chunk"),
            _ => {
                let len = chunks.iter().try_fold(0usize, |len, chunk| {
                    len.checked_add(chunk.len())
                        .context("decoded document length overflows")
                })?;
                let mut joined = bytes::BytesMut::with_capacity(len);
                for chunk in chunks {
                    joined.extend_from_slice(&chunk);
                }
                joined.freeze()
            }
        };
        let decoded_size = u64::try_from(bytes.len())?;
        let grams = match self.strategy {
            Strategy::Trigram => IndexedGrams::Trigram(pack_trigram_grams(&bytes)),
            Strategy::Sparse => IndexedGrams::Sparse(bytes),
        };
        self.documents.push(BuiltDocument {
            meta,
            grams,
            decoded_size,
        });
        Ok(())
    }
}

enum SourceBuild {
    Decoded {
        encoding: SourceEncoding,
        documents: Vec<BuiltDocument>,
    },
    Failed,
}

fn build_source(source: &SourceObject, bytes: bytes::Bytes, strategy: Strategy) -> SourceBuild {
    let mut sink = IndexDecodeSink::new(strategy);
    match decode_source(&source.key, bytes, DECODE_LIMITS, &mut sink) {
        Ok(summary) => SourceBuild::Decoded {
            encoding: summary.encoding,
            documents: sink.documents,
        },
        Err(err) => {
            eprintln!("warning: {err:#}; object excluded from index");
            SourceBuild::Failed
        }
    }
}

fn build_index_files(corpus: &dyn Corpus, strategy: Strategy) -> Result<BuiltIndexFiles> {
    let sources = corpus.sources();
    let mut tables = SegmentTables {
        sources: Vec::with_capacity(sources.len()),
        documents: Vec::new(),
    };
    let mut failed = 0usize;
    let mut runs = Vec::new();
    for chunk in build_chunks(sources) {
        let chunk_start = chunk.start;
        let fetched = corpus.fetch_many(chunk.clone())?;
        let mut bodies = (0..chunk.len()).map(|_| None).collect::<Vec<_>>();
        for (idx, bytes) in fetched {
            let position = idx
                .checked_sub(chunk_start)
                .filter(|position| *position < bodies.len())
                .with_context(|| format!("fetch_many returned out-of-range document {idx}"))?;
            anyhow::ensure!(
                bodies[position].is_none(),
                "fetch_many returned document {idx} twice"
            );
            bodies[position] = Some(bytes);
        }
        let mut raw = bodies
            .par_iter()
            .enumerate()
            .map(|(offset, bytes)| {
                let source = &sources[chunk_start + offset];
                bytes
                    .as_ref()
                    .filter(|bytes| is_raw_source(&source.key, bytes))
                    .map(|bytes| build_source(source, bytes.clone(), strategy))
            })
            .collect::<Vec<_>>();
        let mut grammed = Vec::new();
        for offset in 0..bodies.len() {
            let source = &sources[chunk_start + offset];
            let expanding = bodies[offset]
                .as_ref()
                .is_some_and(|bytes| !is_raw_source(&source.key, bytes));
            if expanding && !grammed.is_empty() {
                runs.extend(write_posting_runs(
                    std::mem::take(&mut grammed),
                    strategy,
                    SPARSE_RUN_BYTES,
                )?);
            }
            let outcome = match (raw[offset].take(), bodies[offset].take()) {
                (Some(outcome), _) => Some(outcome),
                (None, Some(bytes)) => Some(build_source(source, bytes, strategy)),
                (None, None) => None,
            };
            let source_id = u32::try_from(tables.sources.len())?;
            let first_doc = u32::try_from(tables.documents.len())?;
            let (encoding, retry, source_failed, mut documents) = match outcome {
                Some(SourceBuild::Decoded {
                    encoding,
                    documents,
                }) => (encoding, false, false, documents),
                Some(SourceBuild::Failed) => {
                    failed += 1;
                    (SourceEncoding::Raw, false, true, Vec::new())
                }
                None => {
                    failed += 1;
                    (SourceEncoding::Raw, true, true, Vec::new())
                }
            };
            documents
                .sort_unstable_by(|left, right| left.meta.display_key.cmp(&right.meta.display_key));
            for document in documents {
                let doc_id = tables.documents.len();
                grammed.push((doc_id, document.grams));
                tables.documents.push(DocEntry {
                    display_key: document.meta.display_key,
                    source_id,
                    member_path: document.meta.member_path,
                    decoded_size: document.decoded_size,
                });
            }
            tables.sources.push(SourceEntry {
                key: source.key.clone(),
                version: source.version.clone(),
                encoded_size: source.encoded_size,
                encoding,
                first_doc,
                doc_count: u32::try_from(tables.documents.len())? - first_doc,
                failed: source_failed,
                retry,
            });
            if expanding && !grammed.is_empty() {
                runs.extend(write_posting_runs(
                    std::mem::take(&mut grammed),
                    strategy,
                    SPARSE_RUN_BYTES,
                )?);
            }
        }
        if !grammed.is_empty() {
            runs.extend(write_posting_runs(grammed, strategy, SPARSE_RUN_BYTES)?);
        }
    }
    if failed > 0 {
        eprintln!(
            "warning: {} objects vanished or could not be decompressed and were excluded",
            failed
        );
    }
    tables.validate()?;
    let (fst, postings) =
        merge_posting_runs(runs, strategy, u32::try_from(tables.documents.len())?)?;
    Ok(BuiltIndexFiles {
        fst,
        postings,
        tables,
    })
}

enum IndexedGrams {
    Trigram(Vec<u32>),
    Sparse(bytes::Bytes),
}

fn write_posting_runs(
    grammed: Vec<(usize, IndexedGrams)>,
    strategy: Strategy,
    sparse_run_bytes: usize,
) -> Result<Vec<File>> {
    match strategy {
        Strategy::Trigram => Ok(vec![write_trigram_run(grammed)?]),
        Strategy::Sparse => {
            let mut runs = Vec::new();
            for (idx, grams) in grammed {
                let IndexedGrams::Sparse(text) = grams else {
                    anyhow::bail!("mixed gram strategies in build chunk");
                };
                runs.extend(write_sparse_runs(idx, &text, sparse_run_bytes)?);
            }
            Ok(runs)
        }
    }
}

fn write_trigram_run(grammed: Vec<(usize, IndexedGrams)>) -> Result<File> {
    let documents = grammed
        .into_iter()
        .map(|(idx, grams)| {
            let IndexedGrams::Trigram(grams) = grams else {
                anyhow::bail!("mixed gram strategies in build chunk");
            };
            Ok((idx, grams))
        })
        .collect::<Result<Vec<_>>>()?;
    write_trigram_run_radix(documents)
}

fn write_trigram_run_radix(grammed: Vec<(usize, Vec<u32>)>) -> Result<File> {
    let mut file = tempfile::tempfile()?;
    let mut writer = BufWriter::new(&mut file);
    let mut entries = Vec::new();
    for (idx, grams) in grammed {
        let id = DocId::try_from(idx)?;
        entries.extend(
            grams
                .into_iter()
                .map(|gram| u64::from(gram) << 32 | u64::from(id)),
        );
    }
    radsort::sort(&mut entries);
    entries.dedup();
    for entry in entries {
        let gram = (entry >> 32) as u32;
        let id = entry as DocId;
        writer.write_all(&gram.to_be_bytes()[1..])?;
        writer.write_all(&id.to_be_bytes())?;
    }
    writer.flush()?;
    drop(writer);
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

#[cfg(test)]
fn write_trigram_run_merge(grammed: Vec<(usize, Vec<u32>)>) -> Result<File> {
    let mut documents = grammed
        .into_iter()
        .map(|(idx, grams)| Ok((DocId::try_from(idx)?, grams)))
        .collect::<Result<Vec<_>>>()?;
    documents.sort_unstable_by_key(|(id, _)| *id);
    let mut pending = BinaryHeap::new();
    for (document_index, (id, grams)) in documents.iter().enumerate() {
        if let Some(&gram) = grams.first() {
            pending.push(Reverse((gram, *id, document_index, 0usize)));
        }
    }
    let mut file = tempfile::tempfile()?;
    let mut writer = BufWriter::new(&mut file);
    let mut previous = None;
    while let Some(Reverse((gram, id, document_index, gram_index))) = pending.pop() {
        let record = (gram, id);
        if previous != Some(record) {
            writer.write_all(&gram.to_be_bytes()[1..])?;
            writer.write_all(&id.to_be_bytes())?;
            previous = Some(record);
        }
        let next_index = gram_index + 1;
        if let Some(&next_gram) = documents[document_index].1.get(next_index) {
            pending.push(Reverse((next_gram, id, document_index, next_index)));
        }
    }
    writer.flush()?;
    drop(writer);
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn write_sparse_runs(idx: usize, text: &[u8], run_bytes: usize) -> Result<Vec<File>> {
    anyhow::ensure!(run_bytes > 0, "sparse posting run size must be positive");
    let id = DocId::try_from(idx)?;
    let mut runs = Vec::new();
    let mut entries = rapidhash::RapidHashSet::default();
    let mut bytes = 0usize;
    let mut recent: [Option<&[u8]>; 2] = [None, None];
    let mut recent_index = 0usize;
    for gram in iterate_sparse_grams(text) {
        if recent.iter().flatten().any(|previous| *previous == gram) {
            continue;
        }
        recent[recent_index] = Some(gram);
        recent_index = (recent_index + 1) % recent.len();
        if entries.contains(gram) {
            continue;
        }
        bytes = bytes
            .saturating_add(size_of::<Vec<u8>>() + size_of::<usize>())
            .saturating_add(gram.len());
        entries.insert(gram.to_vec());
        if bytes >= run_bytes {
            runs.push(write_sparse_run(&entries, id)?);
            entries.clear();
            bytes = 0;
        }
    }
    if !entries.is_empty() {
        runs.push(write_sparse_run(&entries, id)?);
    }
    Ok(runs)
}

fn write_sparse_run(entries: &rapidhash::RapidHashSet<Vec<u8>>, id: DocId) -> Result<File> {
    let mut ordered = entries.iter().collect::<Vec<_>>();
    ordered.sort_unstable();
    let mut file = tempfile::tempfile()?;
    let mut writer = BufWriter::new(&mut file);
    for gram in ordered {
        writer.write_all(&u32::try_from(gram.len())?.to_be_bytes())?;
        writer.write_all(gram)?;
        writer.write_all(&id.to_be_bytes())?;
    }
    writer.flush()?;
    drop(writer);
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn insert_posting(
    builder: &mut fst::MapBuilder<Vec<u8>>,
    postings_buf: &mut Vec<u8>,
    gram: &[u8],
    mut ids: Vec<DocId>,
    doc_count: u32,
) -> Result<()> {
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(());
    }
    let offset = postings_buf.len() as u64;
    encode_posting_block(postings_buf, &ids, doc_count);
    builder.insert(gram, eval::pack_posting(offset, ids.len())?)?;
    Ok(())
}

fn insert_posting_file<W: Write>(
    builder: &mut fst::MapBuilder<W>,
    postings: &mut impl Write,
    offset: &mut u64,
    gram: &[u8],
    mut ids: Vec<DocId>,
    doc_count: u32,
) -> Result<()> {
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() {
        return Ok(());
    }
    let mut block = Vec::new();
    encode_posting_block(&mut block, &ids, doc_count);
    builder.insert(gram, eval::pack_posting(*offset, ids.len())?)?;
    postings.write_all(&block)?;
    *offset += u64::try_from(block.len())?;
    Ok(())
}

struct PostingRun {
    reader: BufReader<File>,
    strategy: Strategy,
}

impl PostingRun {
    fn read_record(&mut self) -> Result<Option<(Vec<u8>, DocId)>> {
        match self.strategy {
            Strategy::Trigram => {
                let mut record = [0u8; 7];
                if !read_exact_or_eof(&mut self.reader, &mut record)? {
                    return Ok(None);
                }
                Ok(Some((
                    record[..3].to_vec(),
                    DocId::from_be_bytes(record[3..].try_into()?),
                )))
            }
            Strategy::Sparse => {
                let mut length = [0u8; 4];
                if !read_exact_or_eof(&mut self.reader, &mut length)? {
                    return Ok(None);
                }
                let mut gram = vec![0; usize::try_from(u32::from_be_bytes(length))?];
                self.reader
                    .read_exact(&mut gram)
                    .context("truncated temporary posting run")?;
                let mut id = [0u8; 4];
                self.reader
                    .read_exact(&mut id)
                    .context("truncated temporary posting run")?;
                Ok(Some((gram, DocId::from_be_bytes(id))))
            }
        }
    }
}

fn read_exact_or_eof(reader: &mut impl Read, bytes: &mut [u8]) -> Result<bool> {
    match reader.read(&mut bytes[..1])? {
        0 => Ok(false),
        1 => {
            reader
                .read_exact(&mut bytes[1..])
                .context("truncated temporary posting run")?;
            Ok(true)
        }
        _ => unreachable!(),
    }
}

fn merge_posting_runs(
    runs: Vec<File>,
    strategy: Strategy,
    doc_count: u32,
) -> Result<(TempBlob, TempBlob)> {
    let mut runs = runs
        .into_iter()
        .map(|file| PostingRun {
            reader: BufReader::new(file),
            strategy,
        })
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::new();
    for (run_idx, run) in runs.iter_mut().enumerate() {
        if let Some((gram, id)) = run.read_record()? {
            heap.push(Reverse((gram, id, run_idx)));
        }
    }
    let fst = tempfile::NamedTempFile::new()?;
    let postings = tempfile::NamedTempFile::new()?;
    let mut postings_writer = BufWriter::new(HashWriter::new(postings.reopen()?));
    let mut builder = fst::MapBuilder::new(BufWriter::new(HashWriter::new(fst.reopen()?)))?;
    let mut postings_len = 0u64;
    let mut current_gram: Option<Vec<u8>> = None;
    let mut ids = Vec::new();
    while let Some(Reverse((gram, id, run_idx))) = heap.pop() {
        if current_gram.as_deref() != Some(gram.as_slice()) {
            if let Some(current) = current_gram.replace(gram) {
                insert_posting_file(
                    &mut builder,
                    &mut postings_writer,
                    &mut postings_len,
                    &current,
                    std::mem::take(&mut ids),
                    doc_count,
                )?;
            }
        }
        if ids.last() != Some(&id) {
            ids.push(id);
        }
        if let Some((next_gram, next_id)) = runs[run_idx].read_record()? {
            heap.push(Reverse((next_gram, next_id, run_idx)));
        }
    }
    if let Some(current) = current_gram {
        insert_posting_file(
            &mut builder,
            &mut postings_writer,
            &mut postings_len,
            &current,
            ids,
            doc_count,
        )?;
    }
    let mut fst_writer = builder.into_inner()?;
    fst_writer.flush()?;
    postings_writer.flush()?;
    let (_, fst_hash) = fst_writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?
        .finish();
    let (_, postings_hash) = postings_writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?
        .finish();
    let fst_len = fst.as_file().metadata()?.len();
    let written_postings_len = postings.as_file().metadata()?.len();
    anyhow::ensure!(
        postings_len == written_postings_len,
        "postings writer tracked {postings_len} bytes but wrote {written_postings_len}"
    );
    Ok((
        TempBlob {
            file: fst,
            len: fst_len,
            hash: fst_hash,
        },
        TempBlob {
            file: postings,
            len: written_postings_len,
            hash: postings_hash,
        },
    ))
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
    for (gram, ids) in postings {
        insert_posting(&mut builder, &mut postings_buf, &gram, ids, doc_count)?;
    }
    Ok((builder.into_inner()?, postings_buf))
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

impl Corpus for LocalCorpus {
    fn sources(&self) -> &[SourceObject] {
        &self.sources
    }

    fn fetch(&self, idx: usize) -> Result<bytes::Bytes> {
        let path = self
            .paths
            .get(idx)
            .with_context(|| format!("document index {idx} is out of range"))?;
        let bytes = bytes::Bytes::from(std::fs::read(path)?);
        let current = blake3::hash(&bytes).to_hex().to_string();
        if current != self.sources[idx].version {
            return Err(anyhow::Error::new(holys3_core::StaleSource {
                key: self.sources[idx].key.clone(),
                expected: self.sources[idx].version.clone(),
            }));
        }
        Ok(bytes)
    }

    fn fetch_many(&self, sources: Range<usize>) -> Result<Vec<(usize, bytes::Bytes)>> {
        sources
            .into_par_iter()
            .map(|idx| {
                let path = self
                    .paths
                    .get(idx)
                    .with_context(|| format!("document index {idx} is out of range"))?;
                let bytes = std::fs::read(path)
                    .with_context(|| format!("read local document {}", path.display()))?;
                let bytes = bytes::Bytes::from(bytes);
                let source = &self.sources[idx];
                let current = blake3::hash(&bytes).to_hex().to_string();
                if current != source.version {
                    return Err(anyhow::Error::new(holys3_core::StaleSource {
                        key: source.key.clone(),
                        expected: source.version.clone(),
                    }));
                }
                Ok((idx, bytes))
            })
            .collect()
    }
}

/// Search-side fetch for local files: the key IS the path.
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
    consume: &mut dyn FnMut(usize, bytes::Bytes) -> Result<()>,
) -> Result<()> {
    let bytes =
        std::fs::read(&group.key).with_context(|| format!("read local document {}", group.key))?;
    let bytes = bytes::Bytes::from(bytes);
    let current = blake3::hash(&bytes).to_hex().to_string();
    if current != group.version {
        return Err(anyhow::Error::new(holys3_core::StaleSource {
            key: group.key.clone(),
            expected: group.version.clone(),
        }));
    }
    holys3_core::decode_requested(&group.key, &group.requests, bytes, consume)
}

fn fetch_local_parallel(
    groups: &[&LocalFetchGroup],
    workers: usize,
    consume: &mut dyn FnMut(usize, bytes::Bytes) -> Result<()>,
) -> Result<()> {
    let next = AtomicUsize::new(0);
    let cancelled = AtomicBool::new(false);
    let (sender, receiver) =
        std::sync::mpsc::sync_channel::<Result<(usize, bytes::Bytes)>>(workers * 2);
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
        documents: &[holys3_core::DocAddress],
        consume: &mut dyn FnMut(usize, bytes::Bytes) -> Result<()>,
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
    use holys3_core::testutil::MemCorpus;
    use holys3_core::{LineEvent, LineKind, LocalBlobStore, MatchOptions, SubMatch};

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
            strategy,
            &listing,
            false,
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
        let error = build_index_files(&corpus, Strategy::Trigram)
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
    fn index_build_returns_file_backed_blobs() {
        let corpus = MemCorpus::new(
            vec!["a.log".to_owned(), "b.log".to_owned()],
            vec![b"alpha needle".to_vec(), b"beta needle".to_vec()],
        );
        let built = build_index_files(&corpus, Strategy::Trigram).unwrap();
        assert_eq!(
            built.fst.len(),
            std::fs::metadata(built.fst.path()).unwrap().len()
        );
        assert_eq!(
            built.postings.len(),
            std::fs::metadata(built.postings.path()).unwrap().len()
        );
        assert!(built.fst.len() > 0);
        assert!(built.postings.len() > 0);
        for blob in [&built.fst, &built.postings] {
            let bytes = std::fs::read(blob.path()).unwrap();
            let expected = Sha256::digest(&bytes)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            assert_eq!(blob.hash(), expected);
        }
    }

    #[test]
    fn splits_sparse_postings_into_bounded_runs() {
        let text = (0..16_384usize)
            .map(|index| ((index * 31 + index / 7) % 251) as u8)
            .collect::<Vec<_>>();
        let expected = holys3_core::sparse_grams_all_bytes(&text);
        let runs = write_posting_runs(
            vec![(0, IndexedGrams::Sparse(text.into()))],
            Strategy::Sparse,
            1024,
        )
        .unwrap();
        assert!(runs.len() > 1);
        let (fst, postings) = merge_posting_runs(runs, Strategy::Sparse, 1).unwrap();
        let map = fst::Map::new(std::fs::read(fst.path()).unwrap()).unwrap();
        let postings = std::fs::read(postings.path()).unwrap();
        assert_eq!(map.len(), expected.len());
        for gram in expected {
            let packed = map.get(&gram).expect("indexed sparse gram");
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
        let mut radix = write_trigram_run_radix(documents.clone()).unwrap();
        let mut merged = write_trigram_run_merge(documents).unwrap();
        let mut radix_bytes = Vec::new();
        let mut merged_bytes = Vec::new();
        radix.read_to_end(&mut radix_bytes).unwrap();
        merged.read_to_end(&mut merged_bytes).unwrap();
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
                .candidate_docs(&holys3_query::plan("world", r.strategy()).unwrap(), None)
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
        let (matches, stats) = search_collect(&r, &c, "world").unwrap();
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
        let document = holys3_core::DocAddress {
            display_key: key.clone(),
            source_key: key.clone(),
            source_version: blake3::hash(b"alpha").to_hex().to_string(),
            encoded_size: 5,
            encoding: SourceEncoding::Raw,
            member_path: None,
        };
        std::fs::write(&path, b"bravo").unwrap();
        let error = LocalFetcher::new(1)
            .unwrap()
            .fetch_each(&[document], &mut |_, _| Ok(()))
            .unwrap_err();
        assert!(error.is::<holys3_core::StaleSource>(), "{error:#}");
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
                holys3_core::DocAddress {
                    display_key: key.clone(),
                    source_key: key,
                    source_version: blake3::hash(body.as_bytes()).to_hex().to_string(),
                    encoded_size: u64::try_from(body.len()).unwrap(),
                    encoding: SourceEncoding::Raw,
                    member_path: None,
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
        assert!(error.is::<holys3_core::StaleSource>(), "{error:#}");
        let error = corpus.fetch_many(0..1).unwrap_err();
        assert!(error.is::<holys3_core::StaleSource>(), "{error:#}");
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
