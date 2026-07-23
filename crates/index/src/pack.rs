use crate::segment::cache::{read_verified, write_back};
use anyhow::{Context, Result};
use seagrep_core::{BlobStore, DocumentBody, DocumentRegion, DocumentSpool, FetchedDocument};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::sync::{Condvar, Mutex};

pub(crate) const PACK_BLOCK_BYTES: usize = 128 * 1024;
pub(crate) const PACK_TARGET_BYTES: u64 = 256 * 1024 * 1024;
const RANGE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const LARGE_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;
const WINDOW_BYTES: u64 = 32 * 1024 * 1024;
const WINDOW_DOCUMENTS: usize = 1024;
const REGION_GAP_BYTES: u64 = 1024 * 1024;
const REGION_MAX_RANGES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PackSlice {
    pub first_block: u32,
    pub block_offset: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PackBlock {
    pub pack: u32,
    pub offset: u64,
    pub compressed_len: u32,
    pub decoded_len: u32,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PackMeta {
    pub hash: String,
    pub len: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct PackRequest {
    pub index: usize,
    pub slice: PackSlice,
    pub decoded_size: u64,
}

#[derive(Debug, Clone)]
pub(crate) enum PackRange {
    Regional {
        bytes: Range<u64>,
        span: usize,
    },
    FullLines {
        bytes: Range<u64>,
        before_context: usize,
        after_context: usize,
    },
}

pub(crate) struct PackRegionRequest<'a> {
    pub index: usize,
    pub slice: PackSlice,
    pub decoded_size: u64,
    pub ranges: &'a [PackRange],
    pub block_newlines: &'a [u32],
}

struct DocumentBlock {
    block_id: usize,
    start: u64,
    offset: usize,
    len: usize,
}

struct PackRun {
    pack: u32,
    offset: u64,
    len: u64,
    blocks: Vec<usize>,
}

#[derive(Clone)]
enum DecodedBlock {
    Memory(bytes::Bytes),
    File {
        file: std::sync::Arc<Mutex<std::fs::File>>,
        offset: u64,
        len: usize,
    },
}

impl DecodedBlock {
    fn read(&self, range: Range<usize>) -> Result<bytes::Bytes> {
        match self {
            Self::Memory(bytes) => {
                let slice = bytes
                    .get(range)
                    .context("document block range is out of bounds")?;
                Ok(bytes.slice_ref(slice))
            }
            Self::File { file, offset, len } => {
                anyhow::ensure!(range.end <= *len, "document block range is out of bounds");
                let mut bytes = vec![0; range.len()];
                let start = offset
                    .checked_add(u64::try_from(range.start)?)
                    .context("document block range overflows")?;
                let mut file = file.lock().unwrap();
                file.seek(SeekFrom::Start(start))?;
                file.read_exact(&mut bytes)?;
                Ok(bytes::Bytes::from(bytes))
            }
        }
    }
}

enum BlockEntry {
    Loading,
    Ready(DecodedBlock),
}

struct BlockState {
    entries: BTreeMap<usize, BlockEntry>,
}

struct LoadingClaims<'a> {
    state: &'a Mutex<BlockState>,
    ready: &'a Condvar,
    block_ids: BTreeSet<usize>,
}

impl Drop for LoadingClaims<'_> {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for block_id in &self.block_ids {
            if matches!(state.entries.get(block_id), Some(BlockEntry::Loading)) {
                state.entries.remove(block_id);
            }
        }
        self.ready.notify_all();
    }
}

pub(crate) struct PackBatch<'a> {
    store: &'a dyn BlobStore,
    packs: &'a [PackMeta],
    blocks: &'a [PackBlock],
    state: Mutex<BlockState>,
    ready: Condvar,
}

enum PlannedRead {
    Regional,
    FullLines {
        before_context: usize,
        after_context: usize,
    },
}

struct PlannedRange {
    bytes: Range<u64>,
    read: PlannedRead,
}

enum PlannedRequest {
    Whole(Vec<DocumentBlock>),
    Regions {
        parts: Vec<DocumentBlock>,
        ranges: Vec<PlannedRange>,
    },
}

pub(crate) struct PackFile {
    path: tempfile::TempPath,
    len: u64,
    hash: String,
}

impl PackFile {
    pub(crate) fn path(&self) -> &std::path::Path {
        self.path.as_ref()
    }

    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }

    pub(crate) fn meta(&self) -> PackMeta {
        PackMeta {
            hash: self.hash.clone(),
            len: self.len,
        }
    }
}

pub(crate) struct BuiltPacks {
    pub packs: Vec<PackFile>,
    pub blocks: Vec<PackBlock>,
}

struct ActivePack {
    file: tempfile::NamedTempFile,
    len: u64,
    hasher: Sha256,
}

pub(crate) struct PackBuilder {
    block_bytes: usize,
    pack_bytes: u64,
    block: Vec<u8>,
    blocks: Vec<PackBlock>,
    packs: Vec<PackFile>,
    active: Option<ActivePack>,
    compressor: zstd::bulk::Compressor<'static>,
}

impl PackBuilder {
    pub(crate) fn production() -> Result<Self> {
        Self::new(PACK_BLOCK_BYTES, PACK_TARGET_BYTES)
    }

    fn new(block_bytes: usize, pack_bytes: u64) -> Result<Self> {
        anyhow::ensure!(block_bytes > 0, "pack block size must be positive");
        anyhow::ensure!(pack_bytes > 0, "pack target size must be positive");
        let mut compressor = zstd::bulk::Compressor::new(1)?;
        compressor.include_checksum(true)?;
        Ok(Self {
            block_bytes,
            pack_bytes,
            block: Vec::with_capacity(block_bytes),
            blocks: Vec::new(),
            packs: Vec::new(),
            active: None,
            compressor,
        })
    }

    pub(crate) fn append(&mut self, mut reader: impl Read, len: u64) -> Result<PackSlice> {
        if len == 0 {
            return Ok(PackSlice {
                first_block: 0,
                block_offset: 0,
            });
        }
        let slice = PackSlice {
            first_block: u32::try_from(self.blocks.len())?,
            block_offset: u32::try_from(self.block.len())?,
        };
        let mut remaining = len;
        while remaining > 0 {
            let available = self.block_bytes - self.block.len();
            let read = usize::try_from(remaining.min(u64::try_from(available)?))?;
            let start = self.block.len();
            self.block.resize(start + read, 0);
            reader
                .read_exact(&mut self.block[start..])
                .context("decoded document ended before its declared length")?;
            remaining -= u64::try_from(read)?;
            if self.block.len() == self.block_bytes {
                self.flush_block()?;
            }
        }
        Ok(slice)
    }

    pub(crate) fn finish(mut self) -> Result<BuiltPacks> {
        self.flush_block()?;
        self.finish_pack()?;
        Ok(BuiltPacks {
            packs: self.packs,
            blocks: self.blocks,
        })
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let compressed = self.compressor.compress(&self.block)?;
        let compressed_len = u32::try_from(compressed.len())?;
        let rotate = self.active.as_ref().is_some_and(|pack| {
            pack.len > 0 && pack.len.saturating_add(u64::from(compressed_len)) > self.pack_bytes
        });
        if rotate {
            self.finish_pack()?;
        }
        let pack = self.active.get_or_insert(ActivePack {
            file: tempfile::NamedTempFile::new()?,
            len: 0,
            hasher: Sha256::new(),
        });
        let offset = pack.len;
        pack.file.write_all(&compressed)?;
        pack.hasher.update(&compressed);
        pack.len = pack
            .len
            .checked_add(u64::from(compressed_len))
            .context("pack length overflows")?;
        self.blocks.push(PackBlock {
            pack: u32::try_from(self.packs.len())?,
            offset,
            compressed_len,
            decoded_len: u32::try_from(self.block.len())?,
            hash: Sha256::digest(&compressed).into(),
        });
        self.block.clear();
        Ok(())
    }

    fn finish_pack(&mut self) -> Result<()> {
        let Some(mut active) = self.active.take() else {
            return Ok(());
        };
        active.file.flush()?;
        let hash = active
            .hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        self.packs.push(PackFile {
            path: active.file.into_temp_path(),
            len: active.len,
            hash,
        });
        Ok(())
    }
}

pub(crate) fn pack_blob(hash: &str) -> String {
    format!("packs/{hash}.pack")
}

pub(crate) fn block_span(slice: PackSlice, len: u64, blocks: &[PackBlock]) -> Result<Range<usize>> {
    if len == 0 {
        anyhow::ensure!(
            slice.first_block == 0 && slice.block_offset == 0,
            "empty document has pack coordinates"
        );
        return Ok(0..0);
    }
    let start = usize::try_from(slice.first_block)?;
    let mut block_id = start;
    let mut offset = usize::try_from(slice.block_offset)?;
    let mut remaining = len;
    while remaining > 0 {
        let block = blocks
            .get(block_id)
            .context("document points outside pack blocks")?;
        let available = usize::try_from(block.decoded_len)?
            .checked_sub(offset)
            .context("document block offset is out of bounds")?;
        anyhow::ensure!(available > 0, "document block has no available bytes");
        remaining -= remaining.min(u64::try_from(available)?);
        block_id += 1;
        offset = 0;
    }
    Ok(start..block_id)
}

fn block_runs(
    block_ids: impl IntoIterator<Item = usize>,
    blocks: &[PackBlock],
) -> Result<Vec<PackRun>> {
    let mut runs: Vec<PackRun> = Vec::new();
    for block_id in block_ids {
        let block = blocks.get(block_id).context("missing pack block")?;
        let contiguous = runs.last().is_some_and(|run| {
            run.pack == block.pack
                && run.offset.saturating_add(run.len) == block.offset
                && run.len.saturating_add(u64::from(block.compressed_len)) <= RANGE_BYTES
        });
        if contiguous {
            let run = runs.last_mut().expect("run exists");
            run.len = run
                .len
                .checked_add(u64::from(block.compressed_len))
                .context("pack range length overflows")?;
            run.blocks.push(block_id);
        } else {
            runs.push(PackRun {
                pack: block.pack,
                offset: block.offset,
                len: u64::from(block.compressed_len),
                blocks: vec![block_id],
            });
        }
    }
    Ok(runs)
}

fn visit_blocks(
    store: &dyn BlobStore,
    cache: Option<&PackBlockCache<'_>>,
    packs: &[PackMeta],
    blocks: &[PackBlock],
    runs: &[PackRun],
    batch_bytes: u64,
    visit: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
) -> Result<()> {
    for pack_runs in runs.chunk_by(|left, right| left.pack == right.pack) {
        let pack_id = usize::try_from(pack_runs[0].pack)?;
        let pack = packs.get(pack_id).context("pack ID is out of bounds")?;
        for batch in run_batches(pack_runs, batch_bytes) {
            let batch = &pack_runs[batch];
            let mut resolved: Vec<Option<Vec<Vec<u8>>>> = Vec::with_capacity(batch.len());
            for run in batch {
                // A run is served from disk only when every block hits, so a
                // fetched run stays one contiguous range; each hit re-verifies
                // against the docs.bin block hash before use.
                let cached: Option<Vec<Vec<u8>>> = cache.and_then(|cache| {
                    run.blocks
                        .iter()
                        .map(|block_id| cache.read(pack, &blocks[*block_id]))
                        .collect()
                });
                resolved.push(cached);
            }
            let misses: Vec<usize> = resolved
                .iter()
                .enumerate()
                .filter(|(_, hit)| hit.is_none())
                .map(|(at, _)| at)
                .collect();
            if !misses.is_empty() {
                let ranges = misses
                    .iter()
                    .map(|&at| (batch[at].offset, batch[at].len))
                    .collect::<Vec<_>>();
                anyhow::ensure!(
                    ranges
                        .iter()
                        .all(|(offset, len)| offset.saturating_add(*len) <= pack.len),
                    "pack range is out of bounds"
                );
                let fetched = store.get_ranges(&pack_blob(&pack.hash), &ranges)?;
                anyhow::ensure!(
                    fetched.len() == misses.len(),
                    "get_ranges returned {} ranges for {} requests",
                    fetched.len(),
                    misses.len()
                );
                for (&at, bytes) in misses.iter().zip(fetched) {
                    let run = &batch[at];
                    anyhow::ensure!(bytes.len() as u64 == run.len, "pack range length mismatch");
                    let mut cursor = 0usize;
                    let mut compressed_blocks = Vec::with_capacity(run.blocks.len());
                    for block_id in &run.blocks {
                        let block = &blocks[*block_id];
                        let end = cursor
                            .checked_add(usize::try_from(block.compressed_len)?)
                            .context("compressed block range overflows")?;
                        let compressed = bytes
                            .get(cursor..end)
                            .context("pack range ended inside a block")?;
                        anyhow::ensure!(
                            <[u8; 32]>::from(Sha256::digest(compressed)) == block.hash,
                            "pack block hash mismatch"
                        );
                        // Cache only after the block verifies: a corrupt
                        // fetch must stay a transient failure, never a
                        // written artifact.
                        if let Some(cache) = cache {
                            cache.store(pack, block, compressed);
                        }
                        compressed_blocks.push(compressed.to_vec());
                        cursor = end;
                    }
                    anyhow::ensure!(cursor == bytes.len(), "unparsed bytes remain in pack range");
                    resolved[at] = Some(compressed_blocks);
                }
            }
            // Visit every run in its original order: fetch_large consumes
            // blocks with sequential offset bookkeeping, so a hits-first
            // traversal would reassemble a partially-cached large document
            // out of order.
            for (run, compressed_blocks) in batch.iter().zip(resolved) {
                let compressed_blocks =
                    compressed_blocks.context("pack run was neither cached nor fetched")?;
                for (block_id, compressed) in run.blocks.iter().zip(compressed_blocks) {
                    visit_compressed_block(blocks, *block_id, &compressed, visit)?;
                }
            }
        }
    }
    Ok(())
}

fn visit_compressed_block(
    blocks: &[PackBlock],
    block_id: usize,
    compressed: &[u8],
    visit: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
) -> Result<()> {
    let block = &blocks[block_id];
    anyhow::ensure!(
        <[u8; 32]>::from(Sha256::digest(compressed)) == block.hash,
        "pack block hash mismatch"
    );
    let decoded = zstd::bulk::decompress(compressed, usize::try_from(block.decoded_len)?)?;
    anyhow::ensure!(
        decoded.len() == usize::try_from(block.decoded_len)?,
        "pack block decoded length mismatch"
    );
    visit(block_id, decoded)
}

fn run_batches(runs: &[PackRun], max_bytes: u64) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut start = 0usize;
    std::iter::from_fn(move || {
        if start >= runs.len() {
            return None;
        }
        let mut end = start;
        let mut bytes = 0u64;
        while end < runs.len() {
            let next = runs[end].len;
            if end > start && next > max_bytes.saturating_sub(bytes) {
                break;
            }
            bytes = bytes.saturating_add(next);
            end += 1;
        }
        let batch = start..end;
        start = end;
        Some(batch)
    })
}

fn is_large_request(request: &PackRequest) -> bool {
    request.decoded_size >= LARGE_DOCUMENT_BYTES
}

fn fetch_large(
    store: &dyn BlobStore,
    cache: Option<&PackBlockCache<'_>>,
    packs: &[PackMeta],
    blocks: &[PackBlock],
    request: &PackRequest,
    consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
) -> Result<()> {
    let span = block_span(request.slice, request.decoded_size, blocks)?;
    let runs = block_runs(span, blocks)?;
    let mut spool = DocumentSpool::new(request.decoded_size)?;
    let mut remaining = request.decoded_size;
    let mut block_offset = usize::try_from(request.slice.block_offset)?;
    let mut output_offset = 0u64;
    visit_blocks(
        store,
        cache,
        packs,
        blocks,
        &runs,
        RANGE_BYTES,
        &mut |_block_id, decoded| {
            let available = decoded
                .len()
                .checked_sub(block_offset)
                .context("document block offset is out of bounds")?;
            let take = usize::try_from(remaining.min(u64::try_from(available)?))?;
            spool.write_at(output_offset, &decoded[block_offset..block_offset + take])?;
            remaining -= u64::try_from(take)?;
            output_offset += u64::try_from(take)?;
            block_offset = 0;
            Ok(())
        },
    )?;
    anyhow::ensure!(remaining == 0, "pack blocks ended before the document");
    consume(request.index, spool.finish()?)
}

/// Where verified compressed pack blocks persist between queries. Keys live
/// under the segment cache dir so stale-segment eviction cleans them; every
/// hit re-verifies against the block hash recorded in the (whole-file
/// verified) docs.bin block table, so the cache adds no trust surface.
pub(crate) struct PackBlockCache<'a> {
    pub(crate) cache_dir: &'a std::path::Path,
    pub(crate) seg_id: &'a str,
    pub(crate) note_written: &'a dyn Fn(u64),
}

impl PackBlockCache<'_> {
    fn block_path(&self, pack: &PackMeta, block: &PackBlock) -> std::path::PathBuf {
        self.cache_dir
            .join(self.seg_id)
            .join(format!("pack-{}-{:016x}", pack.hash, block.offset))
    }

    fn read(&self, pack: &PackMeta, block: &PackBlock) -> Option<Vec<u8>> {
        read_verified(
            &self.block_path(pack, block),
            &crate::sparse_table::hex(&block.hash),
        )
    }

    fn store(&self, pack: &PackMeta, block: &PackBlock, compressed: &[u8]) {
        (self.note_written)(compressed.len() as u64);
        write_back(self.cache_dir, &self.block_path(pack, block), compressed).ok();
    }
}

fn document_blocks(
    slice: PackSlice,
    decoded_size: u64,
    blocks: &[PackBlock],
) -> Result<Vec<DocumentBlock>> {
    let span = block_span(slice, decoded_size, blocks)?;
    let mut parts = Vec::with_capacity(span.len());
    let mut remaining = decoded_size;
    let mut start = 0u64;
    let mut offset = usize::try_from(slice.block_offset)?;
    for block_id in span {
        let block = &blocks[block_id];
        let len = usize::try_from(remaining.min(u64::from(block.decoded_len) - offset as u64))?;
        parts.push(DocumentBlock {
            block_id,
            start,
            offset,
            len,
        });
        remaining -= u64::try_from(len)?;
        start += u64::try_from(len)?;
        offset = 0;
    }
    anyhow::ensure!(remaining == 0, "pack blocks ended before the document");
    Ok(parts)
}

fn collect_request_blocks(
    slice: PackSlice,
    decoded_size: u64,
    ranges: &[Range<u64>],
    blocks: &[PackBlock],
    output: &mut BTreeSet<usize>,
) -> Result<()> {
    let parts = document_blocks(slice, decoded_size, blocks)?;
    for range in ranges {
        for part in &parts {
            let part_end = part.start + u64::try_from(part.len)?;
            if part_end > range.start && part.start < range.end {
                output.insert(part.block_id);
            }
        }
    }
    Ok(())
}

fn merge_planned_ranges(mut ranges: Vec<PlannedRange>, gap_bytes: u64) -> Vec<PlannedRange> {
    ranges.sort_unstable_by_key(|range| range.bytes.start);
    let mut merged: Vec<PlannedRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        let PlannedRange { bytes, read } = range;
        if let Some(previous) = merged.last_mut() {
            if bytes.start.saturating_sub(previous.bytes.end) <= gap_bytes {
                previous.bytes.end = previous.bytes.end.max(bytes.end);
                match (&mut previous.read, read) {
                    (
                        PlannedRead::FullLines {
                            before_context,
                            after_context,
                        },
                        PlannedRead::FullLines {
                            before_context: next_before,
                            after_context: next_after,
                        },
                    ) => {
                        *before_context = (*before_context).max(next_before);
                        *after_context = (*after_context).max(next_after);
                    }
                    (
                        current @ PlannedRead::Regional,
                        PlannedRead::FullLines {
                            before_context,
                            after_context,
                        },
                    ) => {
                        *current = PlannedRead::FullLines {
                            before_context,
                            after_context,
                        };
                    }
                    _ => {}
                }
                continue;
            }
        }
        merged.push(PlannedRange { bytes, read });
    }
    merged
}

fn plan_pack_ranges(request: &PackRegionRequest<'_>) -> Result<Option<Vec<PlannedRange>>> {
    let mut ranges = Vec::with_capacity(request.ranges.len());
    for range in request.ranges {
        let bytes = match range {
            PackRange::Regional { bytes, .. } | PackRange::FullLines { bytes, .. } => bytes,
        };
        anyhow::ensure!(
            bytes.start < bytes.end && bytes.end <= request.decoded_size,
            "candidate byte range {}..{} is outside a {}-byte document",
            bytes.start,
            bytes.end,
            request.decoded_size
        );
        let planned = match range {
            PackRange::Regional { bytes, span } => {
                let overlap = u64::try_from(
                    span.checked_sub(1)
                        .context("regional span must be positive")?,
                )?;
                PlannedRange {
                    bytes: bytes.start.saturating_sub(overlap)
                        ..bytes.end.saturating_add(overlap).min(request.decoded_size),
                    read: PlannedRead::Regional,
                }
            }
            PackRange::FullLines {
                bytes,
                before_context,
                after_context,
            } => PlannedRange {
                bytes: bytes.clone(),
                read: PlannedRead::FullLines {
                    before_context: *before_context,
                    after_context: *after_context,
                },
            },
        };
        ranges.push(planned);
    }
    let ranges = merge_planned_ranges(ranges, REGION_GAP_BYTES);
    let covered = ranges.iter().try_fold(0u64, |covered, range| {
        covered
            .checked_add(range.bytes.end - range.bytes.start)
            .context("candidate range coverage overflows")
    })?;
    if ranges.len() > REGION_MAX_RANGES || covered.saturating_mul(2) > request.decoded_size {
        Ok(None)
    } else {
        Ok(Some(ranges))
    }
}

impl<'a> PackBatch<'a> {
    pub(crate) fn create(
        store: &'a dyn BlobStore,
        packs: &'a [PackMeta],
        blocks: &'a [PackBlock],
    ) -> PackBatch<'a> {
        PackBatch {
            store,
            packs,
            blocks,
            state: Mutex::new(BlockState {
                entries: BTreeMap::new(),
            }),
            ready: Condvar::new(),
        }
    }

    fn load_blocks(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        block_ids: &BTreeSet<usize>,
    ) -> Result<()> {
        self.load_blocks_with(cache, block_ids, &mut |decoded| {
            Ok(DecodedBlock::Memory(bytes::Bytes::from(decoded)))
        })
    }

    fn load_blocks_to_file(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        block_ids: &BTreeSet<usize>,
    ) -> Result<()> {
        let file = std::sync::Arc::new(Mutex::new(tempfile::tempfile()?));
        let mut offset = 0u64;
        self.load_blocks_with(cache, block_ids, &mut |decoded| {
            let len = decoded.len();
            {
                let mut file = file.lock().unwrap();
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&decoded)?;
            }
            let stored = DecodedBlock::File {
                file: std::sync::Arc::clone(&file),
                offset,
                len,
            };
            offset = offset
                .checked_add(u64::try_from(len)?)
                .context("decoded block file offset overflows")?;
            Ok(stored)
        })
    }

    fn load_blocks_with(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        block_ids: &BTreeSet<usize>,
        store_block: &mut dyn FnMut(Vec<u8>) -> Result<DecodedBlock>,
    ) -> Result<()> {
        if block_ids.is_empty() {
            return Ok(());
        }
        let claimed =
            loop {
                let mut state = self.state.lock().unwrap();
                if block_ids.iter().all(|block_id| {
                    matches!(state.entries.get(block_id), Some(BlockEntry::Ready(_)))
                }) {
                    return Ok(());
                }
                if block_ids.iter().any(|block_id| {
                    matches!(state.entries.get(block_id), Some(BlockEntry::Loading))
                }) {
                    let state = self.ready.wait(state).unwrap();
                    drop(state);
                    continue;
                }
                let claimed = block_ids
                    .iter()
                    .filter(|block_id| !state.entries.contains_key(block_id))
                    .copied()
                    .collect::<BTreeSet<_>>();
                for block_id in &claimed {
                    state.entries.insert(*block_id, BlockEntry::Loading);
                }
                break claimed;
            };
        let claims = LoadingClaims {
            state: &self.state,
            ready: &self.ready,
            block_ids: claimed,
        };
        let runs = block_runs(claims.block_ids.iter().copied(), self.blocks)?;
        visit_blocks(
            self.store,
            cache,
            self.packs,
            self.blocks,
            &runs,
            RANGE_BYTES,
            &mut |block_id, decoded| {
                let decoded = store_block(decoded)?;
                let mut state = self.state.lock().unwrap();
                if matches!(state.entries.get(&block_id), Some(BlockEntry::Loading)) {
                    state.entries.insert(block_id, BlockEntry::Ready(decoded));
                }
                Ok(())
            },
        )?;
        let complete = {
            let state = self.state.lock().unwrap();
            block_ids
                .iter()
                .all(|block_id| matches!(state.entries.get(block_id), Some(BlockEntry::Ready(_))))
        };
        anyhow::ensure!(complete, "decoded pack block is missing");
        drop(claims);
        Ok(())
    }

    fn read_range(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        parts: &[DocumentBlock],
        range: Range<u64>,
    ) -> Result<bytes::Bytes> {
        let len = range
            .end
            .checked_sub(range.start)
            .context("document range is invalid")?;
        if len == 0 {
            return Ok(bytes::Bytes::new());
        }
        let mut block_ids = BTreeSet::new();
        for part in parts {
            let part_end = part.start + u64::try_from(part.len)?;
            if part_end > range.start && part.start < range.end {
                block_ids.insert(part.block_id);
            }
        }
        self.load_blocks(cache, &block_ids)?;
        let matching = parts
            .iter()
            .filter(|part| {
                let part_end = part.start.saturating_add(part.len as u64);
                part_end > range.start && part.start < range.end
            })
            .collect::<Vec<_>>();
        if matching.len() == 1 {
            let part = matching[0];
            let part_end = part.start + u64::try_from(part.len)?;
            let from = usize::try_from(range.start.max(part.start) - part.start)?;
            let to = usize::try_from(range.end.min(part_end) - part.start)?;
            let decoded = {
                let state = self.state.lock().unwrap();
                match state.entries.get(&part.block_id) {
                    Some(BlockEntry::Ready(decoded)) => decoded.clone(),
                    _ => anyhow::bail!("decoded pack block is missing"),
                }
            };
            let start = part
                .offset
                .checked_add(from)
                .context("document block range overflows")?;
            let end = part
                .offset
                .checked_add(to)
                .context("document block range overflows")?;
            let bytes = decoded.read(start..end)?;
            anyhow::ensure!(
                bytes.len() == usize::try_from(len)?,
                "document range is incomplete"
            );
            return Ok(bytes);
        }
        let mut output = Vec::with_capacity(usize::try_from(len)?);
        for part in matching {
            let part_end = part.start + u64::try_from(part.len)?;
            let from = usize::try_from(range.start.max(part.start) - part.start)?;
            let to = usize::try_from(range.end.min(part_end) - part.start)?;
            let decoded = {
                let state = self.state.lock().unwrap();
                match state.entries.get(&part.block_id) {
                    Some(BlockEntry::Ready(decoded)) => decoded.clone(),
                    _ => anyhow::bail!("decoded pack block is missing"),
                }
            };
            let start = part
                .offset
                .checked_add(from)
                .context("document block range overflows")?;
            let end = part
                .offset
                .checked_add(to)
                .context("document block range overflows")?;
            output.extend_from_slice(&decoded.read(start..end)?);
        }
        anyhow::ensure!(
            output.len() == usize::try_from(len)?,
            "document range is incomplete"
        );
        Ok(bytes::Bytes::from(output))
    }

    fn extend_line_range(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        parts: &[DocumentBlock],
        block_newlines: &[u32],
        decoded_size: u64,
        range: Range<u64>,
        context: (usize, usize),
    ) -> Result<Range<u64>> {
        let (before_context, after_context) = context;
        let block_bytes = seagrep_core::CANDIDATE_BLOCK_BYTES as u64;
        let mut start = range.start;
        let mut needed = before_context
            .checked_add(1)
            .context("line context overflows")?;
        while start > 0 && needed > 0 {
            let block = usize::try_from((start - 1) / block_bytes)?;
            let block_start = u64::try_from(block)? * block_bytes;
            let newlines = *block_newlines
                .get(block)
                .context("document newline block is missing")?;
            if newlines == 0 {
                start = block_start;
                continue;
            }
            let bytes = self.read_range(cache, parts, block_start..start)?;
            for (at, byte) in bytes.iter().enumerate().rev() {
                if *byte == b'\n' {
                    needed -= 1;
                    if needed == 0 {
                        start = block_start + u64::try_from(at + 1)?;
                        break;
                    }
                }
            }
            if needed > 0 {
                start = block_start;
            }
        }
        if needed > 0 {
            start = 0;
        }

        let mut end = range.end;
        let ends_line = end > 0 && self.read_range(cache, parts, end - 1..end)?.as_ref() == b"\n";
        let mut needed = after_context
            .checked_add(usize::from(!ends_line))
            .context("line context overflows")?;
        while end < decoded_size && needed > 0 {
            let block = usize::try_from(end / block_bytes)?;
            let block_end = (u64::try_from(block)? + 1)
                .saturating_mul(block_bytes)
                .min(decoded_size);
            let newlines = *block_newlines
                .get(block)
                .context("document newline block is missing")?;
            if newlines == 0 {
                end = block_end;
                continue;
            }
            let bytes = self.read_range(cache, parts, end..block_end)?;
            for (at, byte) in bytes.iter().enumerate() {
                if *byte == b'\n' {
                    needed -= 1;
                    if needed == 0 {
                        end = end
                            .checked_add(u64::try_from(at + 1)?)
                            .context("document line range overflows")?;
                        break;
                    }
                }
            }
            if needed > 0 {
                end = block_end;
            }
        }
        if needed > 0 {
            end = decoded_size;
        }
        Ok(start..end)
    }

    fn locate_line(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        parts: &[DocumentBlock],
        block_newlines: &[u32],
        offset: u64,
    ) -> Result<(u64, u64)> {
        if offset == 0 {
            return Ok((1, 0));
        }
        let decoded_size = parts
            .last()
            .map_or(0u64, |part| part.start.saturating_add(part.len as u64));
        anyhow::ensure!(offset <= decoded_size, "document range is out of bounds");
        let block_bytes = seagrep_core::CANDIDATE_BLOCK_BYTES as u64;
        let block = usize::try_from(offset / block_bytes)?;
        anyhow::ensure!(
            block <= block_newlines.len(),
            "document newline block is missing"
        );
        let before = block_newlines[..block]
            .iter()
            .try_fold(0u64, |total, count| {
                total
                    .checked_add(u64::from(*count))
                    .context("document line number overflows")
            })?;
        let block_start = u64::try_from(block)? * block_bytes;
        let within = if block < block_newlines.len() && block_newlines[block] > 0 {
            self.read_range(cache, parts, block_start..offset)?
        } else {
            bytes::Bytes::new()
        };
        let line = before
            .checked_add(within.iter().filter(|byte| **byte == b'\n').count() as u64)
            .and_then(|count| count.checked_add(1))
            .context("document line number overflows")?;
        if let Some(at) = within.iter().rposition(|byte| *byte == b'\n') {
            return Ok((line, block_start + u64::try_from(at + 1)?));
        }
        for previous in (0..block).rev() {
            if block_newlines[previous] == 0 {
                continue;
            }
            let previous_start = u64::try_from(previous)? * block_bytes;
            let previous_end = previous_start.saturating_add(block_bytes).min(decoded_size);
            let bytes = self.read_range(cache, parts, previous_start..previous_end)?;
            let at = bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .context("document newline count is inconsistent")?;
            return Ok((line, previous_start + u64::try_from(at + 1)?));
        }
        Ok((line, 0))
    }

    fn fetch_large_reusing_blocks(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        request: &PackRequest,
        consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
    ) -> Result<()> {
        let parts = document_blocks(request.slice, request.decoded_size, self.blocks)?;
        let block_ids = parts
            .iter()
            .map(|part| part.block_id)
            .collect::<BTreeSet<_>>();
        self.load_blocks_to_file(cache, &block_ids)?;
        let mut spool = DocumentSpool::new(request.decoded_size)?;
        for part in &parts {
            let end = part
                .start
                .checked_add(u64::try_from(part.len)?)
                .context("document block range overflows")?;
            let bytes = self.read_range(cache, &parts, part.start..end)?;
            spool.write_at(part.start, &bytes)?;
        }
        consume(request.index, spool.finish()?)
    }

    pub(crate) fn fetch_documents(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        requests: &[PackRequest],
        consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
    ) -> Result<()> {
        for window in request_windows(requests) {
            let requests = &requests[window];
            if requests.len() == 1 && is_large_request(&requests[0]) {
                fetch_large(
                    self.store,
                    cache,
                    self.packs,
                    self.blocks,
                    &requests[0],
                    consume,
                )?;
                continue;
            }
            let mut block_ids = BTreeSet::new();
            let mut parts = Vec::with_capacity(requests.len());
            for request in requests {
                let document_parts =
                    document_blocks(request.slice, request.decoded_size, self.blocks)?;
                if request.decoded_size > 0 {
                    let whole = 0..request.decoded_size;
                    collect_request_blocks(
                        request.slice,
                        request.decoded_size,
                        std::slice::from_ref(&whole),
                        self.blocks,
                        &mut block_ids,
                    )?;
                }
                parts.push(document_parts);
            }
            self.load_blocks(cache, &block_ids)?;
            for (request, parts) in requests.iter().zip(parts) {
                let bytes = self.read_range(cache, &parts, 0..request.decoded_size)?;
                consume(request.index, DocumentBody::from_bytes(bytes))?;
            }
        }
        Ok(())
    }

    pub(crate) fn fetch_regions(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        requests: &[PackRegionRequest<'_>],
        consume: &mut dyn FnMut(usize, FetchedDocument) -> Result<()>,
    ) -> Result<()> {
        let mut block_ids = BTreeSet::new();
        let mut plans = Vec::with_capacity(requests.len());
        for request in requests {
            let parts = document_blocks(request.slice, request.decoded_size, self.blocks)?;
            match plan_pack_ranges(request)? {
                None => {
                    if request.decoded_size < LARGE_DOCUMENT_BYTES && request.decoded_size > 0 {
                        let whole = 0..request.decoded_size;
                        collect_request_blocks(
                            request.slice,
                            request.decoded_size,
                            std::slice::from_ref(&whole),
                            self.blocks,
                            &mut block_ids,
                        )?;
                    }
                    plans.push(PlannedRequest::Whole(parts));
                }
                Some(ranges) => {
                    let bytes = ranges
                        .iter()
                        .map(|range| range.bytes.clone())
                        .collect::<Vec<_>>();
                    collect_request_blocks(
                        request.slice,
                        request.decoded_size,
                        &bytes,
                        self.blocks,
                        &mut block_ids,
                    )?;
                    plans.push(PlannedRequest::Regions { parts, ranges });
                }
            }
        }
        self.load_blocks(cache, &block_ids)?;
        for (request, plan) in requests.iter().zip(plans) {
            match plan {
                PlannedRequest::Whole(parts) => {
                    if request.decoded_size >= LARGE_DOCUMENT_BYTES {
                        let whole = PackRequest {
                            index: request.index,
                            slice: request.slice,
                            decoded_size: request.decoded_size,
                        };
                        self.fetch_large_reusing_blocks(cache, &whole, &mut |index, body| {
                            consume(index, FetchedDocument::Whole(body))
                        })?;
                    } else {
                        let bytes = self.read_range(cache, &parts, 0..request.decoded_size)?;
                        consume(
                            request.index,
                            FetchedDocument::Whole(DocumentBody::from_bytes(bytes)),
                        )?;
                    }
                }
                PlannedRequest::Regions { parts, ranges } => {
                    let mut exact = Vec::with_capacity(ranges.len());
                    for range in ranges {
                        match range.read {
                            PlannedRead::Regional => exact.push(range),
                            PlannedRead::FullLines {
                                before_context,
                                after_context,
                            } => exact.push(PlannedRange {
                                bytes: self.extend_line_range(
                                    cache,
                                    &parts,
                                    request.block_newlines,
                                    request.decoded_size,
                                    range.bytes,
                                    (before_context, after_context),
                                )?,
                                read: PlannedRead::FullLines {
                                    before_context,
                                    after_context,
                                },
                            }),
                        }
                    }
                    let exact = merge_planned_ranges(exact, 0);
                    let mut regions = Vec::with_capacity(exact.len());
                    for range in exact {
                        let (line, line_offset) = self.locate_line(
                            cache,
                            &parts,
                            request.block_newlines,
                            range.bytes.start,
                        )?;
                        let program = match range.read {
                            PlannedRead::Regional => seagrep_core::RegionProgram::Regional,
                            PlannedRead::FullLines { .. } => seagrep_core::RegionProgram::Full,
                        };
                        regions.push(DocumentRegion {
                            start: range.bytes.start,
                            line,
                            line_offset,
                            bytes: self.read_range(cache, &parts, range.bytes)?,
                            program,
                        });
                    }
                    consume(
                        request.index,
                        FetchedDocument::Regions {
                            decoded_size: request.decoded_size,
                            regions,
                        },
                    )?;
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn fetch_documents(
    store: &dyn BlobStore,
    cache: Option<&PackBlockCache<'_>>,
    packs: &[PackMeta],
    blocks: &[PackBlock],
    requests: &[PackRequest],
    consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
) -> Result<()> {
    PackBatch::create(store, packs, blocks).fetch_documents(cache, requests, consume)
}

pub(crate) fn request_windows(requests: &[PackRequest]) -> impl Iterator<Item = Range<usize>> + '_ {
    let mut start = 0usize;
    std::iter::from_fn(move || {
        if start >= requests.len() {
            return None;
        }
        let mut end = start;
        let mut bytes = 0u64;
        while end < requests.len() && end - start < WINDOW_DOCUMENTS {
            let request = &requests[end];
            let next = request.decoded_size;
            if end > start
                && (is_large_request(request) || next > WINDOW_BYTES.saturating_sub(bytes))
            {
                break;
            }
            bytes = bytes.saturating_add(next);
            end += 1;
            if is_large_request(request) {
                break;
            }
        }
        let window = start..end;
        start = end;
        Some(window)
    })
}

#[cfg(test)]
impl BuiltPacks {
    fn read(&self, slice: &PackSlice, len: u64) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut remaining = usize::try_from(len)?;
        let mut offset = usize::try_from(slice.block_offset)?;
        let mut output = Vec::with_capacity(remaining);
        for block in self.blocks.iter().skip(usize::try_from(slice.first_block)?) {
            let pack = self
                .packs
                .get(usize::try_from(block.pack)?)
                .context("pack block points outside pack table")?;
            let mut file = std::fs::File::open(pack.path())?;
            file.seek(SeekFrom::Start(block.offset))?;
            let mut compressed = vec![0; usize::try_from(block.compressed_len)?];
            file.read_exact(&mut compressed)?;
            anyhow::ensure!(
                <[u8; 32]>::from(Sha256::digest(&compressed)) == block.hash,
                "pack block hash mismatch"
            );
            let decoded = zstd::bulk::decompress(&compressed, usize::try_from(block.decoded_len)?)?;
            anyhow::ensure!(
                decoded.len() == usize::try_from(block.decoded_len)?,
                "pack block decoded length mismatch"
            );
            let take = remaining.min(decoded.len().saturating_sub(offset));
            output.extend_from_slice(&decoded[offset..offset + take]);
            remaining -= take;
            offset = 0;
            if remaining == 0 {
                return Ok(output);
            }
        }
        anyhow::bail!("pack blocks ended before the document")
    }

    fn corrupt(&mut self, block: usize) -> Result<()> {
        let block = self.blocks.get(block).context("missing pack block")?;
        let pack = self
            .packs
            .get(usize::try_from(block.pack)?)
            .context("missing pack")?;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(pack.path())?;
        file.seek(SeekFrom::Start(block.offset))?;
        let mut byte = [0];
        file.read_exact(&mut byte)?;
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Start(block.offset))?;
        file.write_all(&byte)?;
        file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seagrep_core::LocalBlobStore;
    use std::io::Cursor;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    struct ReadFault {
        call: usize,
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
        kind: ReadFaultKind,
    }

    enum ReadFaultKind {
        Error,
        Panic,
    }

    #[derive(Debug)]
    struct InjectedPackRead;

    impl std::fmt::Display for InjectedPackRead {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("injected pack read failure")
        }
    }

    impl std::error::Error for InjectedPackRead {}

    struct CountingStore {
        inner: LocalBlobStore,
        reads: Mutex<Vec<(String, u64, u64)>>,
        calls: AtomicUsize,
        fault: Mutex<Option<ReadFault>>,
    }

    impl CountingStore {
        fn fail_read(&self, call: usize) -> (Arc<Barrier>, Arc<Barrier>) {
            let entered = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            *self.fault.lock().unwrap() = Some(ReadFault {
                call,
                entered: entered.clone(),
                release: release.clone(),
                kind: ReadFaultKind::Error,
            });
            (entered, release)
        }

        fn panic_read(&self, call: usize) -> (Arc<Barrier>, Arc<Barrier>) {
            let entered = Arc::new(Barrier::new(2));
            let release = Arc::new(Barrier::new(2));
            *self.fault.lock().unwrap() = Some(ReadFault {
                call,
                entered: entered.clone(),
                release: release.clone(),
                kind: ReadFaultKind::Panic,
            });
            (entered, release)
        }
    }

    impl BlobStore for CountingStore {
        fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
            self.inner.put(name, bytes)
        }

        fn put_file(&self, name: &str, path: &std::path::Path) -> Result<()> {
            self.inner.put_file(name, path)
        }

        fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
            self.inner.get(name)
        }

        fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
            self.reads
                .lock()
                .unwrap()
                .push((name.to_owned(), start, len));
            self.inner.get_range(name, start, len)
        }

        fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> Result<Vec<bytes::Bytes>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.reads.lock().unwrap().extend(
                ranges
                    .iter()
                    .map(|&(start, len)| (name.to_owned(), start, len)),
            );
            let fault = {
                let mut fault = self.fault.lock().unwrap();
                fault
                    .as_ref()
                    .is_some_and(|fault| fault.call == call)
                    .then(|| fault.take().expect("read fault exists"))
            };
            if let Some(fault) = fault {
                fault.entered.wait();
                fault.release.wait();
                match fault.kind {
                    ReadFaultKind::Error => return Err(anyhow::Error::new(InjectedPackRead)),
                    ReadFaultKind::Panic => panic!("injected pack read panic"),
                }
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

    fn fetch_batch_bytes(batch: &PackBatch<'_>, request: PackRequest) -> Result<bytes::Bytes> {
        let mut fetched = None;
        batch.fetch_documents(None, std::slice::from_ref(&request), &mut |_, body| {
            fetched = Some(body.into_bytes()?);
            Ok(())
        })?;
        fetched.context("candidate batch returned no document")
    }

    fn expand_read_blocks(
        reads: &[(String, u64, u64)],
        packs: &[PackMeta],
        blocks: &[PackBlock],
    ) -> Vec<usize> {
        reads
            .iter()
            .flat_map(|(name, start, len)| {
                let pack = packs
                    .iter()
                    .position(|pack| pack_blob(&pack.hash) == *name)
                    .unwrap();
                let end = start + len;
                blocks
                    .iter()
                    .enumerate()
                    .filter(move |(_, block)| {
                        usize::try_from(block.pack).unwrap() == pack
                            && block.offset >= *start
                            && block.offset + u64::from(block.compressed_len) <= end
                    })
                    .map(|(block_id, _)| block_id)
            })
            .collect()
    }

    fn fetch_test_ranges(decoded_size: u64, ranges: &[PackRange]) -> FetchedDocument {
        let mut builder = PackBuilder::new(PACK_BLOCK_BYTES, PACK_TARGET_BYTES).unwrap();
        let slice = builder
            .append(std::io::repeat(b'x').take(decoded_size), decoded_size)
            .unwrap();
        let built = builder.finish().unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::new(store_dir.path());
        for pack in &built.packs {
            store
                .put_file(&pack_blob(pack.hash()), pack.path())
                .unwrap();
        }
        let packs = built.packs.iter().map(PackFile::meta).collect::<Vec<_>>();
        let block_newlines =
            vec![
                0;
                usize::try_from(decoded_size.div_ceil(seagrep_core::CANDIDATE_BLOCK_BYTES as u64))
                    .unwrap()
            ];
        let batch = PackBatch::create(&store, &packs, &built.blocks);
        let mut fetched = None;
        batch
            .fetch_regions(
                None,
                &[PackRegionRequest {
                    index: 0,
                    slice,
                    decoded_size,
                    ranges,
                    block_newlines: &block_newlines,
                }],
                &mut |_, document| {
                    fetched = Some(document);
                    Ok(())
                },
            )
            .unwrap();
        fetched.unwrap()
    }

    #[test]
    fn packs_documents_across_shared_blocks_and_files() {
        let mut builder = PackBuilder::new(8, 12).unwrap();
        let first = builder.append(Cursor::new(b"abc"), 3).unwrap();
        let second = builder.append(Cursor::new(b"defghijkl"), 9).unwrap();
        let empty = builder.append(Cursor::new([]), 0).unwrap();
        let built = builder.finish().unwrap();

        assert_eq!(
            first,
            PackSlice {
                first_block: 0,
                block_offset: 0
            }
        );
        assert_eq!(
            second,
            PackSlice {
                first_block: 0,
                block_offset: 3
            }
        );
        assert_eq!(
            empty,
            PackSlice {
                first_block: 0,
                block_offset: 0
            }
        );
        assert_eq!(built.blocks.len(), 2);
        assert_eq!(built.packs.len(), 2);
        assert_eq!(built.read(&first, 3).unwrap(), b"abc");
        assert_eq!(built.read(&second, 9).unwrap(), b"defghijkl");
        assert!(built.read(&empty, 0).unwrap().is_empty());
    }

    #[test]
    fn rejects_corrupt_compressed_blocks() {
        let mut builder = PackBuilder::new(8, 1024).unwrap();
        let document = builder.append(Cursor::new(b"abcdefgh"), 8).unwrap();
        let mut built = builder.finish().unwrap();
        built.corrupt(0).unwrap();

        let error = built.read(&document, 8).unwrap_err();
        assert!(error.to_string().contains("hash"), "{error:#}");
    }

    #[test]
    fn coalesces_only_adjacent_blocks_within_the_range_cap() {
        let mut builder = PackBuilder::new(8, 1024).unwrap();
        builder
            .append(Cursor::new(b"abcdefghijklmnopqrstuvwx"), 24)
            .unwrap();
        let built = builder.finish().unwrap();

        assert_eq!(block_runs([0, 1, 2], &built.blocks).unwrap().len(), 1);
        assert_eq!(block_runs([0, 2], &built.blocks).unwrap().len(), 2);
    }

    #[test]
    fn lazy_line_fetch_assembles_a_line_spanning_pack_blocks() {
        let block = PACK_BLOCK_BYTES;
        let mut body = vec![b'x'; 3 * block + 6];
        body[2 * block + 17..2 * block + 23].copy_from_slice(b"needle");
        body[3 * block] = b'\n';
        body[3 * block + 1..].copy_from_slice(b"tail\n");
        let mut builder = PackBuilder::new(block, 8 * block as u64).unwrap();
        let slice = builder
            .append(Cursor::new(&body), body.len() as u64)
            .unwrap();
        let built = builder.finish().unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store = LocalBlobStore::new(store_dir.path());
        for pack in &built.packs {
            store
                .put_file(&pack_blob(pack.hash()), pack.path())
                .unwrap();
        }
        let packs = built.packs.iter().map(PackFile::meta).collect::<Vec<_>>();
        let block_newlines = body
            .chunks(seagrep_core::CANDIDATE_BLOCK_BYTES)
            .map(|chunk| chunk.iter().filter(|byte| **byte == b'\n').count() as u32)
            .collect::<Vec<_>>();
        let range = (2 * block + 17) as u64..(2 * block + 23) as u64;

        let batch = PackBatch::create(&store, &packs, &built.blocks);
        let ranges = [PackRange::FullLines {
            bytes: range,
            before_context: 0,
            after_context: 0,
        }];
        let mut fetched = None;
        batch
            .fetch_regions(
                None,
                &[PackRegionRequest {
                    index: 0,
                    slice,
                    decoded_size: body.len() as u64,
                    ranges: &ranges,
                    block_newlines: &block_newlines,
                }],
                &mut |_, document| {
                    fetched = Some(document);
                    Ok(())
                },
            )
            .unwrap();
        let fetched = fetched.unwrap();

        let FetchedDocument::Regions { regions, .. } = fetched else {
            panic!("expected regional line fetch")
        };
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].start, 0);
        assert_eq!(regions[0].line, 1);
        assert_eq!(regions[0].line_offset, 0);
        assert_eq!(regions[0].program, seagrep_core::RegionProgram::Full);
        assert_eq!(
            regions[0].bytes,
            bytes::Bytes::copy_from_slice(&body[..=3 * block])
        );
    }

    #[test]
    fn batch_ranges_coalesce_and_fall_back_after_expansion() {
        let block = seagrep_core::CANDIDATE_BLOCK_BYTES as u64;
        let disjoint = [
            PackRange::Regional {
                bytes: 0..block,
                span: 1,
            },
            PackRange::Regional {
                bytes: 8 * block..9 * block,
                span: 1,
            },
        ];
        let FetchedDocument::Regions { regions, .. } = fetch_test_ranges(20 * block, &disjoint)
        else {
            panic!("disjoint ranges should stay regional")
        };
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].start, 0);
        assert_eq!(regions[0].bytes.len() as u64, 9 * block);

        let majority = [PackRange::Regional {
            bytes: 0..3 * block,
            span: 1,
        }];
        assert!(matches!(
            fetch_test_ranges(4 * block, &majority),
            FetchedDocument::Whole(_)
        ));

        let sparse = (0..64u64)
            .map(|index| PackRange::Regional {
                bytes: index * 10 * block..index * 10 * block + block,
                span: 1,
            })
            .collect::<Vec<_>>();
        let FetchedDocument::Regions { regions, .. } = fetch_test_ranges(640 * block, &sparse)
        else {
            panic!("64 sparse ranges should stay regional")
        };
        assert_eq!(regions.len(), 64);
        assert!(regions
            .iter()
            .all(|region| region.bytes.len() as u64 == block));

        let saturated = [PackRange::FullLines {
            bytes: 0..4 * block,
            before_context: 0,
            after_context: 0,
        }];
        assert!(matches!(
            fetch_test_ranges(4 * block, &saturated),
            FetchedDocument::Whole(_)
        ));
    }

    #[test]
    fn batch_and_lazy_reads_load_each_pack_block_once() {
        let first_body = b"abcdef";
        let second_body = b"ghijklmnopqrstuv";
        let mut builder = PackBuilder::new(8, 1024).unwrap();
        let first = builder
            .append(Cursor::new(first_body), first_body.len() as u64)
            .unwrap();
        let second = builder
            .append(Cursor::new(second_body), second_body.len() as u64)
            .unwrap();
        let built = builder.finish().unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store = CountingStore {
            inner: LocalBlobStore::new(store_dir.path()),
            reads: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            fault: Mutex::new(None),
        };
        for pack in &built.packs {
            store
                .put_file(&pack_blob(pack.hash()), pack.path())
                .unwrap();
        }
        let packs = built.packs.iter().map(PackFile::meta).collect::<Vec<_>>();
        let batch = PackBatch::create(&store, &packs, &built.blocks);
        let first_ranges = [PackRange::Regional {
            bytes: 0..3,
            span: 1,
        }];
        let second_ranges = [PackRange::Regional {
            bytes: 0..2,
            span: 1,
        }];
        let newlines = [0];
        let requests = [
            PackRegionRequest {
                index: 0,
                slice: first,
                decoded_size: first_body.len() as u64,
                ranges: &first_ranges,
                block_newlines: &newlines,
            },
            PackRegionRequest {
                index: 1,
                slice: second,
                decoded_size: second_body.len() as u64,
                ranges: &second_ranges,
                block_newlines: &newlines,
            },
        ];
        let mut initial = Vec::new();
        batch
            .fetch_regions(None, &requests, &mut |index, document| {
                let FetchedDocument::Regions { regions, .. } = document else {
                    panic!("initial range should stay regional")
                };
                initial.push((index, regions[0].bytes.clone()));
                Ok(())
            })
            .unwrap();
        initial.sort_unstable_by_key(|(index, _)| *index);
        assert_eq!(initial[0].1, bytes::Bytes::from_static(b"abc"));
        assert_eq!(initial[1].1, bytes::Bytes::from_static(b"gh"));

        let lazy_ranges = [PackRange::Regional {
            bytes: 0..2,
            span: 1,
        }];
        let lazy_request = PackRegionRequest {
            index: 1,
            slice: second,
            decoded_size: second_body.len() as u64,
            ranges: &lazy_ranges,
            block_newlines: &newlines,
        };
        std::thread::scope(|scope| {
            let start = Arc::new(Barrier::new(9));
            let batch = &batch;
            let lazy_request = &lazy_request;
            let workers = (0..8)
                .map(|_| {
                    let start = start.clone();
                    scope.spawn(move || {
                        start.wait();
                        let mut fetched = None;
                        batch
                            .fetch_regions(
                                None,
                                std::slice::from_ref(lazy_request),
                                &mut |_, document| {
                                    fetched = Some(document);
                                    Ok(())
                                },
                            )
                            .unwrap();
                        let FetchedDocument::Regions { regions, .. } = fetched.unwrap() else {
                            panic!("lazy range should stay regional")
                        };
                        assert_eq!(regions[0].bytes, bytes::Bytes::from_static(b"gh"));
                    })
                })
                .collect::<Vec<_>>();
            start.wait();
            for worker in workers {
                worker.join().unwrap();
            }
        });

        let reads = store.reads.lock().unwrap();
        let unique = reads.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(reads.len(), unique.len(), "{reads:?}");
        assert_eq!(reads.len(), 1, "{reads:?}");
    }

    #[test]
    fn retries_failed_single_flight_without_reloading_ready_blocks() {
        let body = bytes::Bytes::from_static(b"abcdefghijklmnop");
        let mut builder = PackBuilder::new(8, 1).unwrap();
        let slice = builder
            .append(Cursor::new(&body), body.len() as u64)
            .unwrap();
        let built = builder.finish().unwrap();
        assert_eq!(built.blocks.len(), 2);
        assert_eq!(built.packs.len(), 2);
        let store_dir = tempfile::tempdir().unwrap();
        let store = CountingStore {
            inner: LocalBlobStore::new(store_dir.path()),
            reads: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            fault: Mutex::new(None),
        };
        for pack in &built.packs {
            store
                .put_file(&pack_blob(pack.hash()), pack.path())
                .unwrap();
        }
        let packs = built.packs.iter().map(PackFile::meta).collect::<Vec<_>>();
        let batch = PackBatch::create(&store, &packs, &built.blocks);
        let request = PackRequest {
            index: 0,
            slice,
            decoded_size: body.len() as u64,
        };
        let (entered, release) = store.fail_read(2);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| fetch_batch_bytes(&batch, request));
            entered.wait();
            let start = Arc::new(Barrier::new(9));
            let workers = (0..8)
                .map(|_| {
                    let batch = &batch;
                    let start = start.clone();
                    scope.spawn(move || {
                        start.wait();
                        fetch_batch_bytes(batch, request)
                    })
                })
                .collect::<Vec<_>>();
            start.wait();
            release.wait();
            let error = first.join().unwrap().unwrap_err();
            assert_eq!(error.to_string(), "injected pack read failure");
            assert!(error.downcast_ref::<InjectedPackRead>().is_some());
            for worker in workers {
                assert_eq!(worker.join().unwrap().unwrap(), body);
            }
        });
        assert_eq!(fetch_batch_bytes(&batch, request).unwrap(), body);
        let reads = store.reads.lock().unwrap();
        assert_eq!(reads.len(), 3, "{reads:?}");
        assert_ne!(reads[0], reads[1], "{reads:?}");
        assert_eq!(reads[1], reads[2], "{reads:?}");
    }

    #[test]
    fn panicking_loader_releases_waiting_block_claim() {
        let body = bytes::Bytes::from_static(b"abcdefgh");
        let mut builder = PackBuilder::new(8, 1024).unwrap();
        let slice = builder
            .append(Cursor::new(&body), body.len() as u64)
            .unwrap();
        let built = builder.finish().unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store = CountingStore {
            inner: LocalBlobStore::new(store_dir.path()),
            reads: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            fault: Mutex::new(None),
        };
        for pack in &built.packs {
            store
                .put_file(&pack_blob(pack.hash()), pack.path())
                .unwrap();
        }
        let packs = built.packs.iter().map(PackFile::meta).collect::<Vec<_>>();
        let batch = PackBatch::create(&store, &packs, &built.blocks);
        let request = PackRequest {
            index: 0,
            slice,
            decoded_size: body.len() as u64,
        };
        let (entered, release) = store.panic_read(1);

        std::thread::scope(|scope| {
            let owner = scope.spawn(|| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    fetch_batch_bytes(&batch, request)
                }))
            });
            entered.wait();

            let (waiting_sender, waiting_receiver) = std::sync::mpsc::channel();
            let (finished_sender, finished_receiver) = std::sync::mpsc::channel();
            let waiter_batch = &batch;
            let waiter = scope.spawn(move || {
                let mut state = waiter_batch.state.lock().unwrap();
                assert!(matches!(state.entries.get(&0), Some(BlockEntry::Loading)));
                waiting_sender.send(()).unwrap();
                while matches!(state.entries.get(&0), Some(BlockEntry::Loading)) {
                    state = waiter_batch.ready.wait(state).unwrap();
                }
                drop(state);
                finished_sender
                    .send(fetch_batch_bytes(waiter_batch, request))
                    .unwrap();
            });
            waiting_receiver.recv().unwrap();
            drop(batch.state.lock().unwrap());
            release.wait();

            assert!(owner.join().unwrap().is_err());
            let finished = finished_receiver.recv_timeout(std::time::Duration::from_secs(1));
            let finished_without_rescue = finished.is_ok();
            let fetched = match finished {
                Ok(fetched) => fetched,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let mut state = batch.state.lock().unwrap();
                    state.entries.remove(&0);
                    drop(state);
                    batch.ready.notify_all();
                    finished_receiver.recv().unwrap()
                }
                Err(error) => panic!("waiting caller disconnected: {error}"),
            };
            assert!(finished_without_rescue, "waiting caller remained blocked");
            assert_eq!(fetched.unwrap(), body);
            waiter.join().unwrap();
        });

        assert_eq!(fetch_batch_bytes(&batch, request).unwrap(), body);
        assert_eq!(store.calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn large_regional_fallback_reads_each_pack_block_once() {
        let mut body = vec![b'x'; usize::try_from(LARGE_DOCUMENT_BYTES).unwrap()];
        body[0] = b'a';
        let last = body.len() - 1;
        body[last] = b'z';
        let mut builder = PackBuilder::new(PACK_BLOCK_BYTES, PACK_TARGET_BYTES).unwrap();
        let slice = builder
            .append(Cursor::new(&body), body.len() as u64)
            .unwrap();
        let built = builder.finish().unwrap();
        let store_dir = tempfile::tempdir().unwrap();
        let store = CountingStore {
            inner: LocalBlobStore::new(store_dir.path()),
            reads: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            fault: Mutex::new(None),
        };
        for pack in &built.packs {
            store
                .put_file(&pack_blob(pack.hash()), pack.path())
                .unwrap();
        }
        let packs = built.packs.iter().map(PackFile::meta).collect::<Vec<_>>();
        let batch = PackBatch::create(&store, &packs, &built.blocks);
        let newlines = vec![0; body.len().div_ceil(seagrep_core::CANDIDATE_BLOCK_BYTES)];
        let tiny = [PackRange::Regional {
            bytes: 0..1,
            span: 1,
        }];
        let tiny_request = PackRegionRequest {
            index: 0,
            slice,
            decoded_size: body.len() as u64,
            ranges: &tiny,
            block_newlines: &newlines,
        };
        batch
            .fetch_regions(None, &[tiny_request], &mut |_, document| {
                let FetchedDocument::Regions { regions, .. } = document else {
                    panic!("tiny range should stay regional")
                };
                assert_eq!(regions[0].bytes, bytes::Bytes::from_static(b"a"));
                Ok(())
            })
            .unwrap();

        let majority = [PackRange::Regional {
            bytes: 0..u64::try_from(body.len() / 2 + 1).unwrap(),
            span: 1,
        }];
        let majority_request = PackRegionRequest {
            index: 0,
            slice,
            decoded_size: body.len() as u64,
            ranges: &majority,
            block_newlines: &newlines,
        };
        batch
            .fetch_regions(None, &[majority_request], &mut |_, document| {
                let FetchedDocument::Whole(whole) = document else {
                    panic!("majority range should fetch the whole document")
                };
                assert!(whole.is_file());
                assert_eq!(whole.into_bytes().unwrap().as_ref(), body.as_slice());
                Ok(())
            })
            .unwrap();

        let fetched = expand_read_blocks(&store.reads.lock().unwrap(), &packs, &built.blocks);
        let counts =
            fetched
                .into_iter()
                .fold(vec![0usize; built.blocks.len()], |mut counts, block_id| {
                    counts[block_id] += 1;
                    counts
                });
        assert_eq!(counts, vec![1; built.blocks.len()]);
    }

    #[test]
    fn batches_large_document_runs_by_compressed_bytes() {
        let runs = [4u64, 4, 5]
            .into_iter()
            .enumerate()
            .map(|(index, len)| PackRun {
                pack: 0,
                offset: u64::try_from(index).unwrap() * 8,
                len,
                blocks: vec![index],
            })
            .collect::<Vec<_>>();

        assert_eq!(run_batches(&runs, 8).collect::<Vec<_>>(), [0..2, 2..3]);
        assert_eq!(
            run_batches(&runs, 3).collect::<Vec<_>>(),
            [0..1, 1..2, 2..3]
        );
    }

    #[test]
    fn treats_sixteen_mib_as_a_large_document() {
        let request = PackRequest {
            index: 0,
            slice: PackSlice {
                first_block: 0,
                block_offset: 0,
            },
            decoded_size: 16 * 1024 * 1024,
        };

        assert!(is_large_request(&request));
    }

    #[test]
    fn isolates_large_documents_from_request_windows() {
        let sizes = [1, 16 * 1024 * 1024, 1];
        let requests = sizes.map(|decoded_size| PackRequest {
            index: 0,
            slice: PackSlice {
                first_block: 0,
                block_offset: 0,
            },
            decoded_size,
        });

        assert_eq!(
            request_windows(&requests).collect::<Vec<_>>(),
            [0..1, 1..2, 2..3]
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn finished_packs_do_not_hold_open_files() {
        use std::os::unix::fs::MetadataExt;

        let mut builder = PackBuilder::new(1, 1).unwrap();
        builder.append(Cursor::new(vec![0; 512]), 512).unwrap();
        let built = builder.finish().unwrap();
        let open_files = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::metadata(entry.path()).ok())
            .map(|metadata| (metadata.dev(), metadata.ino()))
            .collect::<std::collections::HashSet<_>>();

        assert_eq!(built.packs.len(), 512);
        assert!(built.packs.iter().all(|pack| {
            let metadata = std::fs::metadata(pack.path()).unwrap();
            !open_files.contains(&(metadata.dev(), metadata.ino()))
        }));
    }
}
