use crate::format::{DocEntry, SegmentTables, SourceEntry};
use crate::pack::{PackBuilder, PackFile};
use anyhow::{Context, Result};
use holys3_core::{
    decode_source_body, is_raw_body, pack_trigram_grams, Corpus, DecodeSink, DocumentBody,
    LogicalDocumentMeta, SourceEncoding, SourceObject, Strategy, DECODE_LIMITS,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;

mod runs;

#[cfg(test)]
pub(super) use runs::{
    collapse_posting_runs, pack_file_trigrams, write_trigram_run_merge, write_trigram_run_radix,
    PostingRun, MAX_OPEN_POSTING_RUNS, SPARSE_FILE_CHUNK,
};
pub(super) use runs::{
    collect_file_trigrams, key_bytes, merge_posting_runs, write_posting_record, write_posting_runs,
    IndexedGrams, MergedBlob,
};

/// Docs are fetched and gram-extracted in chunks bounded BOTH by doc count
/// and by total (compressed) bytes, so neither many-small nor few-huge
/// objects blow build memory.
const BUILD_FETCH_CHUNK: usize = 1280;
const BUILD_FETCH_BYTES: u64 = 64 * 1024 * 1024;
const SPARSE_RUN_BYTES: usize = 16 * 1024 * 1024;

/// Drives chunk processing with one chunk of prefetch: while `process` works
/// on chunk N, chunk N+1's `fetch` runs on a scoped thread — but only when
/// `prefetchable` allows both chunks, so an over-budget chunk is never in
/// flight alongside another and in-flight bytes stay bounded. Errors from a
/// prefetched fetch surface when its chunk is reached.
pub(super) fn drive_prefetched<T: Send>(
    chunks: &[Range<usize>],
    prefetchable: &(dyn Fn(&Range<usize>) -> bool + Sync),
    fetch: &(dyn Fn(Range<usize>) -> Result<T> + Sync),
    process: &mut dyn FnMut(Range<usize>, T) -> Result<()>,
) -> Result<()> {
    std::thread::scope(|scope| {
        let mut pending: Option<std::thread::ScopedJoinHandle<'_, Result<T>>> = None;
        for (index, chunk) in chunks.iter().enumerate() {
            let fetched = match pending.take() {
                Some(handle) => handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("prefetch thread panicked"))??,
                None => fetch(chunk.clone())?,
            };
            if let Some(next) = chunks.get(index + 1) {
                // An over-budget current chunk is fully buffered here (it was
                // fetched synchronously), so starting the next fetch would
                // double-buffer it with the next chunk.
                if prefetchable(chunk) && prefetchable(next) {
                    let next = next.clone();
                    // catch_unwind keeps a panicking fetch from reaching the
                    // scope's automatic join, which would clobber an error
                    // returned by `process` with a scope-level re-panic.
                    pending = Some(scope.spawn(move || {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fetch(next)))
                            .unwrap_or_else(|_| Err(anyhow::anyhow!("prefetch thread panicked")))
                    }));
                }
            }
            process(chunk.clone(), fetched)?;
        }
        Ok(())
    })
}

fn chunk_encoded_bytes(sources: &[SourceObject], chunk: &Range<usize>) -> u64 {
    sources[chunk.clone()].iter().fold(0u64, |total, source| {
        total.saturating_add(source.encoded_size)
    })
}

/// Greedy chunk boundaries over `docs()` positions respecting both caps; a
/// single over-budget doc still forms its own chunk.
pub(super) fn build_chunks(sources: &[SourceObject]) -> impl Iterator<Item = Range<usize>> + '_ {
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
    pub runs: Vec<tempfile::TempPath>,
    pub tables: SegmentTables,
    pub packs: Vec<PackFile>,
}

struct BuiltDocument {
    meta: LogicalDocumentMeta,
    grams: IndexedGrams,
    decoded_size: u64,
    content: BuiltContent,
}

enum IndexOutput {
    Bytes(Vec<bytes::Bytes>),
    File {
        grams: Option<IndexedGrams>,
        len: u64,
        content: File,
    },
}

enum BuiltContent {
    Bytes(bytes::Bytes),
    File(File),
    Spool { offset: u64 },
}

impl BuiltContent {
    fn append(
        self,
        builder: &mut PackBuilder,
        len: u64,
        spool: Option<&mut tempfile::NamedTempFile>,
    ) -> Result<crate::pack::PackSlice> {
        match self {
            Self::Bytes(bytes) => builder.append(std::io::Cursor::new(bytes), len),
            Self::File(mut file) => {
                file.seek(SeekFrom::Start(0))?;
                builder.append(file, len)
            }
            Self::Spool { offset } => {
                let spool = spool.context("spooled document has no content file")?;
                spool.as_file_mut().seek(SeekFrom::Start(offset))?;
                builder.append(spool.as_file_mut().take(len), len)
            }
        }
    }
}

struct IndexDecodeSink {
    strategy: Strategy,
    document_limit: Option<usize>,
    current_meta: Option<LogicalDocumentMeta>,
    current_output: IndexOutput,
    documents: Vec<BuiltDocument>,
    spool: Option<tempfile::NamedTempFile>,
    spool_len: u64,
}

impl IndexDecodeSink {
    fn new(strategy: Strategy, document_limit: Option<usize>, spool_content: bool) -> Result<Self> {
        Ok(Self {
            strategy,
            document_limit,
            current_meta: None,
            current_output: IndexOutput::Bytes(Vec::new()),
            documents: Vec::new(),
            spool: spool_content
                .then(tempfile::NamedTempFile::new)
                .transpose()?,
            spool_len: 0,
        })
    }

    fn store_content(&mut self, mut content: BuiltContent, len: u64) -> Result<BuiltContent> {
        let Some(spool) = &mut self.spool else {
            return Ok(content);
        };
        let offset = self.spool_len;
        let written = match &mut content {
            BuiltContent::Bytes(bytes) => {
                spool.write_all(bytes)?;
                u64::try_from(bytes.len())?
            }
            BuiltContent::File(file) => {
                file.seek(SeekFrom::Start(0))?;
                std::io::copy(&mut file.take(len), spool)?
            }
            BuiltContent::Spool { .. } => anyhow::bail!("document content was spooled twice"),
        };
        anyhow::ensure!(written == len, "decoded document content length changed");
        self.spool_len = self
            .spool_len
            .checked_add(len)
            .context("decoded content spool length overflows")?;
        Ok(BuiltContent::Spool { offset })
    }
}

impl DecodeSink for IndexDecodeSink {
    fn begin(&mut self, document: &LogicalDocumentMeta) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_none(),
            "decoder began a document before finishing the previous document"
        );
        if self
            .document_limit
            .is_some_and(|limit| self.documents.len() >= limit)
        {
            return Err(anyhow::Error::new(DocumentCapExceeded));
        }
        self.current_meta = Some(document.clone());
        Ok(())
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_some(),
            "decoder wrote bytes before beginning a document"
        );
        let IndexOutput::Bytes(chunks) = &mut self.current_output else {
            anyhow::bail!("decoder mixed file and byte output for one document");
        };
        chunks.push(bytes::Bytes::copy_from_slice(bytes));
        Ok(())
    }

    fn write_bytes(&mut self, bytes: bytes::Bytes) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_some(),
            "decoder wrote bytes before beginning a document"
        );
        let IndexOutput::Bytes(chunks) = &mut self.current_output else {
            anyhow::bail!("decoder mixed file and byte output for one document");
        };
        chunks.push(bytes);
        Ok(())
    }

    fn write_file(&mut self, mut file: File, len: u64) -> Result<()> {
        anyhow::ensure!(
            self.current_meta.is_some(),
            "decoder wrote a file before beginning a document"
        );
        let IndexOutput::Bytes(chunks) = &self.current_output else {
            anyhow::bail!("decoder wrote more than one file for one document");
        };
        anyhow::ensure!(chunks.is_empty(), "decoder mixed file and byte output");
        let content = file.try_clone()?;
        let grams = if self.spool.is_some() {
            None
        } else {
            Some(match self.strategy {
                Strategy::Trigram => collect_file_trigrams(&mut file, 0, len)?,
                Strategy::Sparse
                    if self
                        .current_meta
                        .as_ref()
                        .is_some_and(|meta| meta.member_path.is_some()) =>
                {
                    file.seek(SeekFrom::Start(0))?;
                    let mut temp = tempfile::NamedTempFile::new()?;
                    std::io::copy(&mut file, &mut temp)?;
                    IndexedGrams::SparsePath(temp.into_temp_path())
                }
                Strategy::Sparse => IndexedGrams::SparseFile(file),
            })
        };
        self.current_output = IndexOutput::File {
            grams,
            len,
            content,
        };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let meta = self
            .current_meta
            .take()
            .context("decoder finished without beginning a document")?;
        let output = std::mem::replace(&mut self.current_output, IndexOutput::Bytes(Vec::new()));
        let (grams, decoded_size, content) = match output {
            IndexOutput::File {
                grams,
                len,
                content,
            } => (grams, len, BuiltContent::File(content)),
            IndexOutput::Bytes(mut chunks) => {
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
                let grams = match (self.spool.is_some(), self.strategy) {
                    (true, _) => None,
                    (false, Strategy::Trigram) => {
                        Some(IndexedGrams::Trigram(pack_trigram_grams(&bytes)))
                    }
                    (false, Strategy::Sparse) => Some(IndexedGrams::Sparse(bytes.clone())),
                };
                (grams, decoded_size, BuiltContent::Bytes(bytes))
            }
        };
        let content = self.store_content(content, decoded_size)?;
        let grams = match (grams, &content, self.strategy) {
            (Some(grams), _, _) => grams,
            (None, BuiltContent::Spool { offset }, Strategy::Trigram) => {
                IndexedGrams::TrigramSpool {
                    offset: *offset,
                    len: decoded_size,
                }
            }
            (None, BuiltContent::Spool { offset }, Strategy::Sparse) => IndexedGrams::SparseSpool {
                offset: *offset,
                len: decoded_size,
            },
            (None, _, _) => anyhow::bail!("deferred grams have no content spool"),
        };
        self.documents.push(BuiltDocument {
            meta,
            grams,
            decoded_size,
            content,
        });
        Ok(())
    }
}

enum SourceBuild {
    Decoded {
        encoding: SourceEncoding,
        documents: Vec<BuiltDocument>,
        spool: Option<tempfile::NamedTempFile>,
        expanded_bytes: u64,
    },
    Failed,
}

#[derive(Debug)]
pub(crate) struct DocumentCapExceeded;

impl std::fmt::Display for DocumentCapExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("segment document cap exceeded")
    }
}

impl std::error::Error for DocumentCapExceeded {}

fn build_source(
    source: &SourceObject,
    body: DocumentBody,
    strategy: Strategy,
    document_limit: Option<usize>,
    spool_content: bool,
) -> Result<SourceBuild> {
    let mut sink = IndexDecodeSink::new(strategy, document_limit, spool_content)?;
    match decode_source_body(&source.key, body, DECODE_LIMITS, &mut sink) {
        Ok(summary) => Ok(SourceBuild::Decoded {
            encoding: summary.encoding,
            documents: sink.documents,
            spool: sink.spool,
            expanded_bytes: summary.expanded_bytes,
        }),
        Err(err) if err.is::<DocumentCapExceeded>() => Err(err),
        Err(err) => {
            eprintln!("warning: {err:#}; object excluded from index");
            Ok(SourceBuild::Failed)
        }
    }
}

fn build_raw_source(
    source: &SourceObject,
    body: Option<&DocumentBody>,
    strategy: Strategy,
    document_limit: Option<usize>,
) -> Result<Option<SourceBuild>> {
    let Some(body) = body else {
        return Ok(None);
    };
    if !is_raw_body(&source.key, body)? {
        return Ok(None);
    }
    Ok(Some(build_source(
        source,
        body.try_clone()?,
        strategy,
        document_limit,
        false,
    )?))
}

/// Everything one fetched chunk mutates while it is ingested: decoded
/// documents append to the pack builder and tables, grams flow into posting
/// runs, and vanished/undecodable sources count as failed.
struct ChunkIngest<'a> {
    sources: &'a [SourceObject],
    strategy: Strategy,
    document_cap: Option<usize>,
    progress: Option<&'a holys3_core::ProgressSender>,
    tables: &'a mut SegmentTables,
    pack_builder: &'a mut PackBuilder,
    failed: &'a mut usize,
    runs: &'a mut Vec<tempfile::TempPath>,
}

impl ChunkIngest<'_> {
    /// Decode, gram, and pack one fetched chunk of sources, in listing order.
    fn ingest(&mut self, chunk: Range<usize>, fetched: Vec<(usize, DocumentBody)>) -> Result<()> {
        let (sources, strategy, document_cap, progress) = (
            self.sources,
            self.strategy,
            self.document_cap,
            self.progress,
        );
        let chunk_start = chunk.start;
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
        let build_raw = |(offset, body): (usize, &Option<DocumentBody>)| {
            build_raw_source(
                &sources[chunk_start + offset],
                body.as_ref(),
                strategy,
                document_cap,
            )
        };
        let mut raw = if bodies.len() == 1 {
            bodies
                .iter()
                .enumerate()
                .map(build_raw)
                .collect::<Result<Vec<_>>>()?
        } else {
            bodies
                .par_iter()
                .enumerate()
                .map(build_raw)
                .collect::<Result<Vec<_>>>()?
        };
        let mut grammed = Vec::new();
        for offset in 0..bodies.len() {
            let source = &sources[chunk_start + offset];
            let expanding = bodies[offset].is_some() && raw[offset].is_none();
            if expanding && !grammed.is_empty() {
                self.runs.extend(write_posting_runs(
                    std::mem::take(&mut grammed),
                    strategy,
                    SPARSE_RUN_BYTES,
                    None,
                )?);
            }
            let outcome = match (raw[offset].take(), bodies[offset].take()) {
                (Some(outcome), _) => Some(outcome),
                (None, Some(body)) => {
                    let document_limit =
                        document_cap.map(|cap| cap.saturating_sub(self.tables.documents.len()));
                    if document_limit == Some(0) {
                        return Err(anyhow::Error::new(DocumentCapExceeded));
                    }
                    Some(build_source(source, body, strategy, document_limit, true)?)
                }
                (None, None) => None,
            };
            let source_id = u32::try_from(self.tables.sources.len())?;
            let first_doc = u32::try_from(self.tables.documents.len())?;
            let (encoding, retry, source_failed, mut documents, mut spool, expanded_bytes) =
                match outcome {
                    Some(SourceBuild::Decoded {
                        encoding,
                        documents,
                        spool,
                        expanded_bytes,
                    }) => (encoding, false, false, documents, spool, expanded_bytes),
                    Some(SourceBuild::Failed) => {
                        *self.failed += 1;
                        (SourceEncoding::Raw, false, true, Vec::new(), None, 0)
                    }
                    None => {
                        *self.failed += 1;
                        (SourceEncoding::Raw, true, true, Vec::new(), None, 0)
                    }
                };
            if let Some(progress) = progress {
                progress.emit(holys3_core::ProgressEvent::SourceIngested {
                    decoded_bytes: expanded_bytes,
                });
            }
            documents
                .sort_unstable_by(|left, right| left.meta.display_key.cmp(&right.meta.display_key));
            let next_document_count = self
                .tables
                .documents
                .len()
                .checked_add(documents.len())
                .context("segment document count overflows")?;
            if document_cap.is_some_and(|cap| next_document_count > cap) {
                return Err(anyhow::Error::new(DocumentCapExceeded));
            }
            for document in documents {
                let doc_id = self.tables.documents.len();
                let slice = document.content.append(
                    self.pack_builder,
                    document.decoded_size,
                    spool.as_mut(),
                )?;
                grammed.push((doc_id, document.grams));
                self.tables.documents.push(DocEntry {
                    display_key: document.meta.display_key,
                    source_id,
                    member_path: document.meta.member_path,
                    decoded_size: document.decoded_size,
                    first_block: slice.first_block,
                    block_offset: slice.block_offset,
                });
            }
            self.tables.sources.push(SourceEntry {
                key: source.key.clone(),
                version: source.version.clone(),
                encoded_size: source.encoded_size,
                encoding,
                first_doc,
                doc_count: u32::try_from(self.tables.documents.len())? - first_doc,
                failed: source_failed,
                retry,
            });
            if expanding && !grammed.is_empty() {
                self.runs.extend(write_posting_runs(
                    std::mem::take(&mut grammed),
                    strategy,
                    SPARSE_RUN_BYTES,
                    spool.as_ref().map(tempfile::NamedTempFile::as_file),
                )?);
            }
        }
        if !grammed.is_empty() {
            self.runs.extend(write_posting_runs(
                grammed,
                strategy,
                SPARSE_RUN_BYTES,
                None,
            )?);
        }
        Ok(())
    }
}

pub(crate) fn build_index_files(
    corpus: &dyn Corpus,
    strategy: Strategy,
    document_cap: Option<usize>,
    progress: Option<&holys3_core::ProgressSender>,
) -> Result<BuiltIndexFiles> {
    if let Some(document_cap) = document_cap {
        anyhow::ensure!(document_cap > 0, "segment document cap must be positive");
    }
    let sources = corpus.sources();
    let mut tables = SegmentTables {
        sources: Vec::with_capacity(sources.len()),
        documents: Vec::new(),
        blocks: Vec::new(),
    };
    let mut pack_builder = PackBuilder::production()?;
    let mut failed = 0usize;
    let mut runs = Vec::new();
    let chunks: Vec<Range<usize>> = build_chunks(sources).collect();
    let fetch = |chunk: Range<usize>| corpus.fetch_bodies(chunk);
    let prefetchable =
        |chunk: &Range<usize>| chunk_encoded_bytes(sources, chunk) <= BUILD_FETCH_BYTES;
    let mut ingest = ChunkIngest {
        sources,
        strategy,
        document_cap,
        progress,
        tables: &mut tables,
        pack_builder: &mut pack_builder,
        failed: &mut failed,
        runs: &mut runs,
    };
    drive_prefetched(&chunks, &prefetchable, &fetch, &mut |chunk, fetched| {
        ingest.ingest(chunk, fetched)
    })?;
    if failed > 0 {
        eprintln!(
            "warning: {} objects vanished or could not be decompressed and were excluded",
            failed
        );
    }
    let packed = pack_builder.finish()?;
    tables.blocks = packed.blocks;
    tables.validate()?;
    Ok(BuiltIndexFiles {
        runs,
        tables,
        packs: packed.packs,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn drive_prefetched_returns_process_error_even_when_prefetch_panics() {
        let chunks = vec![0..1usize, 1..2];
        let (process_failed, prefetch_gate) = std::sync::mpsc::channel();
        let prefetch_gate = std::sync::Mutex::new(prefetch_gate);
        let fetch = move |chunk: Range<usize>| {
            if chunk.start == 1 {
                let () = prefetch_gate
                    .lock()
                    .unwrap()
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("process error signal");
                panic!("prefetch blew up");
            }
            Ok(chunk.start)
        };
        let error = drive_prefetched(&chunks, &|_| true, &fetch, &mut |_, _| {
            process_failed.send(()).unwrap();
            anyhow::bail!("process failed first")
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("process failed first"),
            "{error:#}"
        );
    }

    #[test]
    fn drive_prefetched_starts_next_fetch_during_processing() {
        let chunks = vec![0..2usize, 2..4, 4..6];
        let (fetch_started, started) = std::sync::mpsc::channel();
        let fetch = move |chunk: Range<usize>| {
            fetch_started.send(chunk.start).unwrap();
            Ok(chunk.start)
        };
        let mut seen = Vec::new();
        drive_prefetched(&chunks, &|_| true, &fetch, &mut |chunk, fetched: usize| {
            assert_eq!(chunk.start, fetched);
            if chunk.start == 0 {
                let own = started
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("own fetch signal");
                assert_eq!(own, 0);
            }
            if chunk.start < 4 {
                let next = started
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("next chunk's fetch must start while this chunk processes");
                assert_eq!(next, chunk.start + 2);
            }
            seen.push(chunk.start);
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [0, 2, 4]);
    }

    #[test]
    fn drive_prefetched_surfaces_prefetch_errors_in_chunk_order() {
        let chunks = vec![0..1usize, 1..2, 2..3];
        let fetch = |chunk: Range<usize>| {
            if chunk.start == 1 {
                anyhow::bail!("fetch of chunk 1 failed");
            }
            Ok(chunk.start)
        };
        let mut processed = Vec::new();
        let error = drive_prefetched(&chunks, &|_| true, &fetch, &mut |chunk, _| {
            processed.push(chunk.start);
            Ok(())
        })
        .unwrap_err();
        assert!(error.to_string().contains("chunk 1 failed"), "{error:#}");
        assert_eq!(processed, [0]);
    }

    #[test]
    fn drive_prefetched_respects_the_prefetch_guard() {
        let chunks = vec![0..1usize, 1..2];
        let in_process = std::sync::atomic::AtomicBool::new(false);
        let fetch = |chunk: Range<usize>| {
            if chunk.start == 1 {
                assert!(
                    !in_process.load(std::sync::atomic::Ordering::SeqCst),
                    "guarded chunk must not be prefetched during processing"
                );
            }
            Ok(chunk.start)
        };
        drive_prefetched(&chunks, &|chunk| chunk.start == 0, &fetch, &mut |_, _| {
            in_process.store(true, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(50));
            in_process.store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn drive_prefetched_holds_the_next_fetch_while_a_guarded_chunk_processes() {
        let chunks = vec![0..1usize, 1..2, 2..3];
        let in_guarded_process = std::sync::atomic::AtomicBool::new(false);
        let fetch = |chunk: Range<usize>| {
            if chunk.start == 2 {
                assert!(
                    !in_guarded_process.load(std::sync::atomic::Ordering::SeqCst),
                    "nothing may be prefetched while a guarded chunk processes"
                );
            }
            Ok(chunk.start)
        };
        drive_prefetched(
            &chunks,
            &|chunk| chunk.start != 1,
            &fetch,
            &mut |chunk, _| {
                if chunk.start == 1 {
                    in_guarded_process.store(true, std::sync::atomic::Ordering::SeqCst);
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    in_guarded_process.store(false, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(())
            },
        )
        .unwrap();
    }

    use super::*;

    #[test]
    fn expanding_sink_spools_document_content() {
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let mut sink = IndexDecodeSink::new(strategy, None, true).unwrap();
            for (key, body) in [("a", b"alpha".as_slice()), ("b", b"beta".as_slice())] {
                sink.begin(&LogicalDocumentMeta {
                    display_key: key.to_owned(),
                    member_path: Some(key.to_owned()),
                })
                .unwrap();
                sink.write(body).unwrap();
                sink.finish().unwrap();
            }

            assert!(sink
                .documents
                .iter()
                .all(|document| matches!(document.content, BuiltContent::Spool { .. })));
            assert!(sink.documents.iter().all(|document| match document.grams {
                IndexedGrams::TrigramSpool { .. } => strategy == Strategy::Trigram,
                IndexedGrams::SparseSpool { .. } => strategy == Strategy::Sparse,
                _ => false,
            }));
            assert_eq!(sink.spool_len, 9);
        }
    }
}
