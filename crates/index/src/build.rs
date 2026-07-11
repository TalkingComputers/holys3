use crate::format::{DocEntry, SegmentTables, SourceEntry};
use anyhow::{Context, Result};
use holys3_core::{
    decode_source_body, is_raw_body, pack_trigram_grams, Corpus, DecodeSink, DocumentBody,
    LogicalDocumentMeta, SourceEncoding, SourceObject, Strategy, DECODE_LIMITS,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;

mod runs;

#[cfg(test)]
pub(super) use runs::{
    collapse_posting_runs, pack_file_trigrams, write_trigram_run_merge, write_trigram_run_radix,
    PostingRun, MAX_OPEN_POSTING_RUNS, SPARSE_FILE_CHUNK,
};
pub(super) use runs::{
    collect_file_trigrams, merge_posting_runs, write_posting_record, write_posting_runs,
    IndexedGrams,
};

/// Docs are fetched and gram-extracted in chunks bounded BOTH by doc count
/// and by total (compressed) bytes, so neither many-small nor few-huge
/// objects blow build memory.
const BUILD_FETCH_CHUNK: usize = 1280;
const BUILD_FETCH_BYTES: u64 = 64 * 1024 * 1024;
const SPARSE_RUN_BYTES: usize = 16 * 1024 * 1024;

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

enum IndexOutput {
    Bytes(Vec<bytes::Bytes>),
    File { grams: IndexedGrams, len: u64 },
}

struct IndexDecodeSink {
    strategy: Strategy,
    document_limit: Option<usize>,
    current_meta: Option<LogicalDocumentMeta>,
    current_output: IndexOutput,
    documents: Vec<BuiltDocument>,
}

impl IndexDecodeSink {
    fn new(strategy: Strategy, document_limit: Option<usize>) -> Self {
        Self {
            strategy,
            document_limit,
            current_meta: None,
            current_output: IndexOutput::Bytes(Vec::new()),
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
        let grams = match self.strategy {
            Strategy::Trigram => collect_file_trigrams(&mut file, len)?,
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
        };
        self.current_output = IndexOutput::File { grams, len };
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let meta = self
            .current_meta
            .take()
            .context("decoder finished without beginning a document")?;
        let output = std::mem::replace(&mut self.current_output, IndexOutput::Bytes(Vec::new()));
        let (grams, decoded_size) = match output {
            IndexOutput::File { grams, len } => (grams, len),
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
                let grams = match self.strategy {
                    Strategy::Trigram => IndexedGrams::Trigram(pack_trigram_grams(&bytes)),
                    Strategy::Sparse => IndexedGrams::Sparse(bytes),
                };
                (grams, decoded_size)
            }
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
) -> Result<SourceBuild> {
    let mut sink = IndexDecodeSink::new(strategy, document_limit);
    match decode_source_body(&source.key, body, DECODE_LIMITS, &mut sink) {
        Ok(summary) => Ok(SourceBuild::Decoded {
            encoding: summary.encoding,
            documents: sink.documents,
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
    )?))
}

pub(crate) fn build_index_files(
    corpus: &dyn Corpus,
    strategy: Strategy,
    document_cap: Option<usize>,
) -> Result<BuiltIndexFiles> {
    if let Some(document_cap) = document_cap {
        anyhow::ensure!(document_cap > 0, "segment document cap must be positive");
    }
    let sources = corpus.sources();
    let mut tables = SegmentTables {
        sources: Vec::with_capacity(sources.len()),
        documents: Vec::new(),
    };
    let mut failed = 0usize;
    let mut runs = Vec::new();
    for chunk in build_chunks(sources) {
        let chunk_start = chunk.start;
        let fetched = corpus.fetch_bodies(chunk.clone())?;
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
                runs.extend(write_posting_runs(
                    std::mem::take(&mut grammed),
                    strategy,
                    SPARSE_RUN_BYTES,
                )?);
            }
            let outcome = match (raw[offset].take(), bodies[offset].take()) {
                (Some(outcome), _) => Some(outcome),
                (None, Some(body)) => {
                    let document_limit =
                        document_cap.map(|cap| cap.saturating_sub(tables.documents.len()));
                    if document_limit == Some(0) {
                        return Err(anyhow::Error::new(DocumentCapExceeded));
                    }
                    Some(build_source(source, body, strategy, document_limit)?)
                }
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
            let next_document_count = tables
                .documents
                .len()
                .checked_add(documents.len())
                .context("segment document count overflows")?;
            if document_cap.is_some_and(|cap| next_document_count > cap) {
                return Err(anyhow::Error::new(DocumentCapExceeded));
            }
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
