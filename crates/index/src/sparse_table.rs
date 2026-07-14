//! Sorted hash table replacing the FST for sparse term dictionaries: fixed
//! 16-byte (hash, value) entries in 128 KiB blocks, a per-block first-hash
//! index, and per-block SHA-256 so blocks can be fetched and verified by
//! ranged reads without downloading the whole dictionary.

use anyhow::{Context, Result};
use sha2::Digest;
use std::io::Write;

pub(crate) const SPARSE_TABLE_MAGIC: &[u8; 8] = b"H3SPARSE";

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
pub(crate) const ENTRY_BYTES: usize = 16;
pub(crate) const BLOCK_ENTRIES: usize = 8192;
pub(crate) const BLOCK_BYTES: usize = ENTRY_BYTES * BLOCK_ENTRIES;
const INDEX_ENTRY_BYTES: usize = 8 + 8 + 32;
pub(crate) const FOOTER_BYTES: usize = 8 + 8 + 8 + 8;

/// Writes a sparse term table from ascending unique (hash, value) inserts.
pub(crate) struct SparseTableWriter<W: Write> {
    writer: W,
    block: Vec<u8>,
    block_first_hash: u64,
    last_hash: Option<u64>,
    entry_count: u64,
    written: u64,
    index: Vec<SparseBlockRef>,
}

impl<W: Write> SparseTableWriter<W> {
    pub(crate) fn new(mut writer: W) -> Result<Self> {
        writer.write_all(SPARSE_TABLE_MAGIC)?;
        Ok(Self {
            writer,
            block: Vec::with_capacity(BLOCK_BYTES),
            block_first_hash: 0,
            last_hash: None,
            entry_count: 0,
            written: SPARSE_TABLE_MAGIC.len() as u64,
            index: Vec::new(),
        })
    }

    pub(crate) fn insert(&mut self, hash: u64, value: u64) -> Result<()> {
        anyhow::ensure!(
            self.last_hash.is_none_or(|last| hash > last),
            "sparse table inserts must be ascending and unique"
        );
        if self.block.is_empty() {
            self.block_first_hash = hash;
        }
        self.block.extend_from_slice(&hash.to_be_bytes());
        self.block.extend_from_slice(&value.to_be_bytes());
        self.last_hash = Some(hash);
        self.entry_count += 1;
        if self.block.len() >= BLOCK_BYTES {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let hash = <[u8; 32]>::from(sha2::Sha256::digest(&self.block));
        self.index.push(SparseBlockRef {
            first_hash: self.block_first_hash,
            offset: self.written,
            len: self.block.len() as u64,
            hash,
        });
        self.writer.write_all(&self.block)?;
        self.written += self.block.len() as u64;
        self.block.clear();
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<W> {
        self.flush_block()?;
        let index_offset = self.written;
        for block in &self.index {
            self.writer.write_all(&block.first_hash.to_le_bytes())?;
            self.writer.write_all(&block.offset.to_le_bytes())?;
            self.writer.write_all(&block.hash)?;
        }
        self.writer.write_all(&self.entry_count.to_le_bytes())?;
        self.writer
            .write_all(&(self.index.len() as u64).to_le_bytes())?;
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(SPARSE_TABLE_MAGIC)?;
        Ok(self.writer)
    }
}

/// Parsed index + footer of a sparse term table; entry blocks are accessed
/// separately (mmap slice or verified ranged read).
#[derive(Debug)]
pub(crate) struct SparseTableIndex {
    pub(crate) entry_count: u64,
    pub(crate) blocks: Vec<SparseBlockRef>,
}

#[derive(Debug)]
pub(crate) struct SparseBlockRef {
    pub(crate) first_hash: u64,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) hash: [u8; 32],
}

impl SparseTableIndex {
    /// Parse and validate the index + footer from the table's trailing bytes.
    /// `table_len` is the full blob length; `tail` must be its final bytes
    /// (at least the footer; typically footer + index).
    pub(crate) fn parse(table_len: u64, tail: &[u8]) -> Result<Self> {
        anyhow::ensure!(
            table_len >= (SPARSE_TABLE_MAGIC.len() + FOOTER_BYTES) as u64
                && tail.len() >= FOOTER_BYTES
                && tail.len() as u64 <= table_len,
            "sparse table is truncated"
        );
        let footer = &tail[tail.len() - FOOTER_BYTES..];
        anyhow::ensure!(
            &footer[24..32] == SPARSE_TABLE_MAGIC,
            "sparse table footer magic mismatch"
        );
        let read_u64 = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
        let entry_count = read_u64(&footer[0..8]);
        let block_count = read_u64(&footer[8..16]);
        let index_offset = read_u64(&footer[16..24]);
        let index_len = block_count
            .checked_mul(INDEX_ENTRY_BYTES as u64)
            .context("sparse table index length overflows")?;
        anyhow::ensure!(
            index_offset
                .checked_add(index_len)
                .and_then(|end| end.checked_add(FOOTER_BYTES as u64))
                == Some(table_len),
            "sparse table index does not abut the footer"
        );
        let tail_start = table_len - tail.len() as u64;
        anyhow::ensure!(
            tail_start <= index_offset,
            "sparse table tail does not include the index"
        );
        let index_in_tail = usize::try_from(index_offset - tail_start)?;
        let index_bytes = tail
            .get(index_in_tail..tail.len() - FOOTER_BYTES)
            .context("sparse table index is out of tail bounds")?;
        anyhow::ensure!(
            index_bytes.len() as u64 == index_len,
            "sparse table index length mismatch"
        );
        let mut blocks = Vec::with_capacity(usize::try_from(block_count)?);
        let mut expected_offset = SPARSE_TABLE_MAGIC.len() as u64;
        for entry in index_bytes.chunks_exact(INDEX_ENTRY_BYTES) {
            let first_hash = read_u64(&entry[0..8]);
            let offset = read_u64(&entry[8..16]);
            anyhow::ensure!(
                offset == expected_offset,
                "sparse table blocks are not contiguous"
            );
            let next = blocks.len() + 1;
            let next_offset = if next < usize::try_from(block_count)? {
                read_u64(&index_bytes[next * INDEX_ENTRY_BYTES + 8..next * INDEX_ENTRY_BYTES + 16])
            } else {
                index_offset
            };
            let len = next_offset
                .checked_sub(offset)
                .context("sparse table block length underflows")?;
            anyhow::ensure!(
                len > 0 && len.is_multiple_of(ENTRY_BYTES as u64) && len <= BLOCK_BYTES as u64,
                "sparse table block length is invalid"
            );
            expected_offset = next_offset;
            blocks.push(SparseBlockRef {
                first_hash,
                offset,
                len,
                hash: entry[16..48].try_into().expect("thirty-two bytes"),
            });
        }
        anyhow::ensure!(
            blocks
                .windows(2)
                .all(|pair| pair[0].first_hash < pair[1].first_hash),
            "sparse table block index is not sorted"
        );
        let capacity: u64 = blocks
            .iter()
            .map(|block| block.len / ENTRY_BYTES as u64)
            .sum();
        anyhow::ensure!(
            capacity == entry_count,
            "sparse table entry count does not match its blocks"
        );
        Ok(Self {
            entry_count,
            blocks,
        })
    }

    /// Which block may contain `hash`, if any.
    pub(crate) fn block_for(&self, hash: u64) -> Option<usize> {
        let position = self
            .blocks
            .partition_point(|block| block.first_hash <= hash);
        position.checked_sub(1)
    }
}

/// Bisect one verified block's raw bytes for `hash`.
pub(crate) fn lookup_in_block(block: &[u8], hash: u64) -> Result<Option<u64>> {
    anyhow::ensure!(
        !block.is_empty() && block.len().is_multiple_of(ENTRY_BYTES),
        "sparse table block length is invalid"
    );
    let entries = block.len() / ENTRY_BYTES;
    let entry_hash = |at: usize| {
        u64::from_be_bytes(
            block[at * ENTRY_BYTES..at * ENTRY_BYTES + 8]
                .try_into()
                .expect("eight bytes"),
        )
    };
    let mut low = 0usize;
    let mut high = entries;
    while low < high {
        let mid = low + (high - low) / 2;
        match entry_hash(mid).cmp(&hash) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => {
                return Ok(Some(u64::from_be_bytes(
                    block[mid * ENTRY_BYTES + 8..mid * ENTRY_BYTES + 16]
                        .try_into()
                        .expect("eight bytes"),
                )));
            }
        }
    }
    Ok(None)
}

/// SHA-256 of a finished table file's index+footer tail, or None when the
/// file is not a sparse table (trigram dictionaries keep an empty hash).
pub(crate) fn tail_hash_of(path: &std::path::Path) -> Result<Option<String>> {
    use sha2::Sha256;
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let mut magic = [0u8; SPARSE_TABLE_MAGIC.len()];
    if len < (SPARSE_TABLE_MAGIC.len() + FOOTER_BYTES) as u64 {
        return Ok(None);
    }
    file.read_exact(&mut magic)?;
    if &magic != SPARSE_TABLE_MAGIC {
        return Ok(None);
    }
    file.seek(SeekFrom::End(-(FOOTER_BYTES as i64)))?;
    let mut footer = [0u8; FOOTER_BYTES];
    file.read_exact(&mut footer)?;
    anyhow::ensure!(
        &footer[24..32] == SPARSE_TABLE_MAGIC,
        "sparse table footer magic mismatch"
    );
    let index_offset = u64::from_le_bytes(
        footer[16..24]
            .try_into()
            .context("sparse table footer is malformed")?,
    );
    anyhow::ensure!(
        index_offset < len,
        "sparse table index offset is out of bounds"
    );
    file.seek(SeekFrom::Start(index_offset))?;
    let mut hasher = Sha256::new();
    let mut chunk = vec![0u8; 1 << 20];
    loop {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
    }
    Ok(Some(hex(&<[u8; 32]>::from(hasher.finalize()))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn build(entries: &[(u64, u64)]) -> Vec<u8> {
        let mut writer = SparseTableWriter::new(Vec::new()).unwrap();
        for (hash, value) in entries {
            writer.insert(*hash, *value).unwrap();
        }
        writer.finish().unwrap()
    }

    fn open(bytes: &[u8]) -> SparseTableIndex {
        SparseTableIndex::parse(bytes.len() as u64, bytes).unwrap()
    }

    fn get(bytes: &[u8], index: &SparseTableIndex, hash: u64) -> Option<u64> {
        let block = index.block_for(hash)?;
        let block_ref = &index.blocks[block];
        let start = usize::try_from(block_ref.offset).unwrap();
        let end = start + usize::try_from(block_ref.len).unwrap();
        lookup_in_block(&bytes[start..end], hash).unwrap()
    }

    #[test]
    fn round_trips_entries_across_block_boundaries() {
        let entries: Vec<(u64, u64)> = (0..(BLOCK_ENTRIES as u64 * 2 + 7))
            .map(|i| (i * 3 + 1, i * 7))
            .collect();
        let bytes = build(&entries);
        let index = open(&bytes);
        assert_eq!(index.entry_count, entries.len() as u64);
        assert_eq!(index.blocks.len(), 3);
        for (hash, value) in [
            entries[0],
            entries[BLOCK_ENTRIES - 1],
            entries[BLOCK_ENTRIES],
            *entries.last().unwrap(),
        ] {
            assert_eq!(get(&bytes, &index, hash), Some(value), "hash {hash}");
        }
        assert_eq!(get(&bytes, &index, 0), None);
        assert_eq!(get(&bytes, &index, 2), None);
        assert_eq!(get(&bytes, &index, u64::MAX), None);
    }

    #[test]
    fn rejects_unsorted_and_duplicate_inserts() {
        let mut writer = SparseTableWriter::new(Vec::new()).unwrap();
        writer.insert(10, 0).unwrap();
        assert!(writer.insert(10, 1).is_err(), "duplicate hash");
        let mut writer = SparseTableWriter::new(Vec::new()).unwrap();
        writer.insert(10, 0).unwrap();
        assert!(writer.insert(9, 1).is_err(), "descending hash");
    }

    #[test]
    fn per_block_hashes_detect_corruption() {
        let entries: Vec<(u64, u64)> = (0..100u64).map(|i| (i + 1, i)).collect();
        let mut bytes = build(&entries);
        let index = open(&bytes);
        let block = &index.blocks[0];
        let start = usize::try_from(block.offset).unwrap();
        let end = start + usize::try_from(block.len).unwrap();
        assert_eq!(
            <[u8; 32]>::from(Sha256::digest(&bytes[start..end])),
            block.hash,
            "clean block matches its recorded hash"
        );
        bytes[start] ^= 0xff;
        assert_ne!(
            <[u8; 32]>::from(Sha256::digest(&bytes[start..end])),
            block.hash,
            "corrupted block no longer matches"
        );
    }

    #[test]
    fn parse_rejects_truncated_and_foreign_bytes() {
        assert!(SparseTableIndex::parse(4, b"abcd").is_err());
        let bytes = build(&[(1, 2), (3, 4)]);
        assert!(
            SparseTableIndex::parse(bytes.len() as u64 - 1, &bytes[..bytes.len() - 1]).is_err()
        );
        let mut foreign = bytes.clone();
        let len = foreign.len();
        foreign[len - 8..].copy_from_slice(b"NOTMAGIC");
        assert!(SparseTableIndex::parse(len as u64, &foreign).is_err());
    }

    #[test]
    fn empty_table_round_trips() {
        let bytes = build(&[]);
        let index = open(&bytes);
        assert_eq!(index.entry_count, 0);
        assert!(index.blocks.is_empty());
        assert_eq!(index.block_for(42), None);
    }
}
