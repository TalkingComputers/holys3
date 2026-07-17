use crate::segment::cache::{read_verified, write_back};
use anyhow::{Context, Result};
use seagrep_core::{BlobStore, DocumentBody, DocumentSpool};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read, Write};
#[cfg(test)]
use std::io::{Seek, SeekFrom};
use std::ops::Range;

pub(crate) const PACK_BLOCK_BYTES: usize = 128 * 1024;
pub(crate) const PACK_TARGET_BYTES: u64 = 256 * 1024 * 1024;
const RANGE_BYTES: u64 = 8 * 1024 * 1024;
const LARGE_DOCUMENT_BYTES: u64 = 16 * 1024 * 1024;
const WINDOW_BYTES: u64 = 32 * 1024 * 1024;
const WINDOW_DOCUMENTS: usize = 1024;

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

struct PackRun {
    pack: u32,
    offset: u64,
    len: u64,
    blocks: Vec<usize>,
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

fn block_span(slice: PackSlice, len: u64, blocks: &[PackBlock]) -> Result<Range<usize>> {
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

fn fetch_window(
    store: &dyn BlobStore,
    cache: Option<&PackBlockCache<'_>>,
    packs: &[PackMeta],
    blocks: &[PackBlock],
    requests: &[PackRequest],
    consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
) -> Result<()> {
    let spans = requests
        .iter()
        .map(|request| block_span(request.slice, request.decoded_size, blocks))
        .collect::<Result<Vec<_>>>()?;
    let block_ids = spans
        .iter()
        .flat_map(|span| span.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let runs = block_runs(block_ids, blocks)?;
    let mut decoded = BTreeMap::new();
    visit_blocks(
        store,
        cache,
        packs,
        blocks,
        &runs,
        RANGE_BYTES,
        &mut |block_id, bytes| {
            decoded.insert(block_id, bytes::Bytes::from(bytes));
            Ok(())
        },
    )?;
    for (request, span) in requests.iter().zip(spans) {
        if request.decoded_size == 0 {
            consume(request.index, DocumentBody::from_bytes(bytes::Bytes::new()))?;
            continue;
        }
        let mut remaining = usize::try_from(request.decoded_size)?;
        let mut offset = usize::try_from(request.slice.block_offset)?;
        if span.len() == 1 {
            let block = decoded
                .get(&span.start)
                .context("decoded pack block is missing")?;
            let end = offset
                .checked_add(remaining)
                .context("document block range overflows")?;
            let body = block
                .get(offset..end)
                .context("document block range is out of bounds")?;
            consume(
                request.index,
                DocumentBody::from_bytes(block.slice_ref(body)),
            )?;
            continue;
        }
        let mut body = Vec::with_capacity(remaining);
        for block_id in span {
            let block = decoded
                .get(&block_id)
                .context("decoded pack block is missing")?;
            let take = remaining.min(
                block
                    .len()
                    .checked_sub(offset)
                    .context("document block offset is out of bounds")?,
            );
            body.extend_from_slice(&block[offset..offset + take]);
            remaining -= take;
            offset = 0;
        }
        anyhow::ensure!(remaining == 0, "pack blocks ended before the document");
        consume(
            request.index,
            DocumentBody::from_bytes(bytes::Bytes::from(body)),
        )?;
    }
    Ok(())
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

pub(crate) fn fetch_documents(
    store: &dyn BlobStore,
    cache: Option<&PackBlockCache<'_>>,
    packs: &[PackMeta],
    blocks: &[PackBlock],
    requests: &[PackRequest],
    consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
) -> Result<()> {
    for window in request_windows(requests) {
        if window.len() == 1 && is_large_request(&requests[window.start]) {
            fetch_large(
                store,
                cache,
                packs,
                blocks,
                &requests[window.start],
                consume,
            )?;
        } else {
            fetch_window(store, cache, packs, blocks, &requests[window], consume)?;
        }
    }
    Ok(())
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
    use std::io::Cursor;

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
