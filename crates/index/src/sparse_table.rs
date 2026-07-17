//! Sorted hash table replacing the FST for sparse term dictionaries:
//! delta-varint (hash, value) entries in 128 KiB blocks, a per-block
//! first-hash index, and per-block SHA-256 so blocks can be fetched and
//! verified by ranged reads without downloading the whole dictionary.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::Write;

pub(crate) const SPARSE_TABLE_MAGIC: &[u8; 8] = b"SGSPARSE";

pub(crate) fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
pub(crate) const BLOCK_BYTES: usize = 128 * 1024;

/// LEB128. Sorted hash deltas average well under five bytes on real corpora
/// (vs eight raw), and singleton values fold their count tag into one small
/// doc-id varint; together they roughly halve the dictionary.
fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(block: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *block
            .get(*cursor)
            .context("sparse table block ended inside a varint")?;
        *cursor += 1;
        anyhow::ensure!(shift < 64, "sparse table varint overflows u64");
        anyhow::ensure!(
            shift < 63 || byte & 0x7f <= 1,
            "sparse table varint overflows u64"
        );
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

/// Decode every (hash, value) entry of a block in order. The first entry
/// stores its hash raw (big-endian); the rest store deltas from the previous
/// hash. A value is one tag varint whose low bit marks a singleton — the
/// tag's high bits are then the doc id — else the high bits are the posting
/// count and a second varint carries the offset.
pub(crate) fn for_each_entry(
    block: &[u8],
    mut visit: impl FnMut(u64, u64) -> Result<()>,
) -> Result<()> {
    anyhow::ensure!(block.len() > 8, "sparse table block is too short");
    let mut cursor = 8usize;
    let mut hash = u64::from_be_bytes(block[..8].try_into().expect("eight bytes"));
    let read_value = |cursor: &mut usize| -> Result<u64> {
        let tag = read_varint(block, cursor)?;
        if tag & 1 == 1 {
            return crate::eval::pack_posting(tag >> 1, 1);
        }
        // The writer always folds count == 1 into an odd tag; accepting the
        // two-varint spelling too would give singletons a second encoding.
        anyhow::ensure!(tag >> 1 != 1, "sparse table singleton is not canonical");
        let offset = read_varint(block, cursor)?;
        crate::eval::pack_posting(offset, usize::try_from(tag >> 1)?)
    };
    let value = read_value(&mut cursor)?;
    visit(hash, value)?;
    while cursor < block.len() {
        let delta = read_varint(block, &mut cursor)?;
        anyhow::ensure!(delta > 0, "sparse table hashes must be strictly ascending");
        hash = hash
            .checked_add(delta)
            .context("sparse table hash delta overflows")?;
        let value = read_value(&mut cursor)?;
        visit(hash, value)?;
    }
    Ok(())
}
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
            self.block.extend_from_slice(&hash.to_be_bytes());
        } else {
            let last = self.last_hash.context("delta after the first entry")?;
            write_varint(&mut self.block, hash - last);
        }
        // The packed u64 keeps the count in its high bits, which would force
        // every value to six-plus varint bytes, so values split into varints.
        // Singletons dominate real dictionaries (87% of grams on books), so
        // count == 1 folds into the doc-id varint's low bit and costs one
        // varint; other counts shift left and a second varint carries the
        // offset.
        let (offset, count) = crate::eval::unpack_posting(value);
        if count == 1 {
            write_varint(&mut self.block, (offset << 1) | 1);
        } else {
            write_varint(&mut self.block, u64::from(count) << 1);
            write_varint(&mut self.block, offset);
        }
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
        let hash = <[u8; 32]>::from(Sha256::digest(&self.block));
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

    /// Returns the writer and the SHA-256 of the index+footer tail — the
    /// hash segment metadata records so remote readers can trust a ranged
    /// fetch of just the block index.
    pub(crate) fn finish(mut self) -> Result<(W, String)> {
        self.flush_block()?;
        let index_offset = self.written;
        let mut tail = Sha256::new();
        let emit = |writer: &mut W, tail: &mut Sha256, bytes: &[u8]| -> Result<()> {
            tail.update(bytes);
            writer.write_all(bytes)?;
            Ok(())
        };
        for block in &self.index {
            emit(&mut self.writer, &mut tail, &block.first_hash.to_le_bytes())?;
            emit(&mut self.writer, &mut tail, &block.offset.to_le_bytes())?;
            emit(&mut self.writer, &mut tail, &block.hash)?;
        }
        emit(&mut self.writer, &mut tail, &self.entry_count.to_le_bytes())?;
        emit(
            &mut self.writer,
            &mut tail,
            &(self.index.len() as u64).to_le_bytes(),
        )?;
        emit(&mut self.writer, &mut tail, &index_offset.to_le_bytes())?;
        emit(&mut self.writer, &mut tail, SPARSE_TABLE_MAGIC)?;
        Ok((self.writer, hex(&<[u8; 32]>::from(tail.finalize()))))
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
            // Varint entries have no fixed width; blocks may exceed the
            // flush target by one entry's worth of bytes.
            anyhow::ensure!(
                len > 8 && len <= (BLOCK_BYTES + 32) as u64,
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
        anyhow::ensure!(
            (entry_count == 0) == blocks.is_empty(),
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
#[cfg(test)]
pub(crate) fn lookup_in_block(block: &[u8], hash: u64) -> Result<Option<u64>> {
    let mut found = None;
    let mut done = false;
    for_each_entry(block, |entry_hash, value| {
        if done {
            return Ok(());
        }
        if entry_hash >= hash {
            if entry_hash == hash {
                found = Some(value);
            }
            done = true;
        }
        Ok(())
    })?;
    Ok(found)
}

/// SHA-256 of a finished table file's index+footer tail, or None when the
/// file is not a sparse table. Production records the hash as the writer
/// streams the tail; this file-derived variant cross-checks that in tests.
#[cfg(test)]
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
    #[test]
    fn streamed_tail_hash_matches_file_derived_tail_hash() {
        let mut writer = SparseTableWriter::new(Vec::new()).unwrap();
        for entry in 0..20_000u64 {
            writer.insert(entry * 3 + 1, entry).unwrap();
        }
        let (bytes, streamed) = writer.finish().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("terms.fst");
        std::fs::write(&path, &bytes).unwrap();
        let derived = tail_hash_of(&path).unwrap().unwrap();
        assert_eq!(streamed, derived);
    }

    use super::*;
    use sha2::{Digest, Sha256};

    fn build(entries: &[(u64, u64)]) -> Vec<u8> {
        let mut writer = SparseTableWriter::new(Vec::new()).unwrap();
        for (hash, value) in entries {
            writer.insert(*hash, *value).unwrap();
        }
        writer.finish().unwrap().0
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
        // Enough small-delta entries to fill several varint blocks, plus
        // wide deltas and large values to exercise multi-byte varints.
        let mut entries: Vec<(u64, u64)> = (0..80_000u64).map(|i| (i * 3 + 1, i * 7)).collect();
        entries.push((1 << 60, u64::MAX));
        entries.push((u64::MAX - 5, 1));
        let bytes = build(&entries);
        let index = open(&bytes);
        assert_eq!(index.entry_count, entries.len() as u64);
        assert!(index.blocks.len() > 2, "spans multiple blocks");
        let boundary = usize::try_from(
            index.blocks[0].len * u64::try_from(entries.len()).unwrap()
                / u64::try_from(bytes.len()).unwrap(),
        )
        .unwrap();
        for (hash, value) in [
            entries[0],
            entries[boundary.min(entries.len() - 1)],
            entries[entries.len() - 2],
            *entries.last().unwrap(),
        ] {
            assert_eq!(get(&bytes, &index, hash), Some(value), "hash {hash}");
        }
        // every entry survives the round trip
        let mut walked = Vec::new();
        for block in &index.blocks {
            let start = usize::try_from(block.offset).unwrap();
            let end = start + usize::try_from(block.len).unwrap();
            for_each_entry(&bytes[start..end], |hash, value| {
                walked.push((hash, value));
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(walked, entries);
        assert_eq!(get(&bytes, &index, 0), None);
        assert_eq!(get(&bytes, &index, 2), None);
        assert_eq!(get(&bytes, &index, u64::MAX), None);
    }

    #[test]
    fn singletons_cost_one_varint() {
        // A singleton (count == 1) folds its tag into the doc-id varint; a
        // two-doc list at the same postings offset costs a second varint.
        let single = crate::eval::pack_posting(300, 1).unwrap();
        let pair = crate::eval::pack_posting(300, 2).unwrap();
        let singleton_table = build(&[(10, single)]);
        let pair_table = build(&[(10, pair)]);
        assert!(
            singleton_table.len() < pair_table.len(),
            "singleton {} must be smaller than pair {}",
            singleton_table.len(),
            pair_table.len()
        );
        let index = open(&singleton_table);
        assert_eq!(get(&singleton_table, &index, 10), Some(single));
        let index = open(&pair_table);
        assert_eq!(get(&pair_table, &index, 10), Some(pair));
    }

    #[test]
    fn value_tags_reject_hostile_and_noncanonical_spellings() {
        // Maximum representable singleton: the full 40-bit offset space.
        let max_single = crate::eval::pack_posting((1 << 40) - 1, 1).unwrap();
        let table = build(&[(10, max_single)]);
        let index = open(&table);
        assert_eq!(get(&table, &index, 10), Some(max_single));

        // Raw block: an odd tag whose doc id exceeds the 40-bit offset
        // space must error through pack_posting, not wrap.
        let mut block = 10u64.to_be_bytes().to_vec();
        write_varint(&mut block, (1u64 << 41) | 1);
        assert!(for_each_entry(&block, |_, _| Ok(())).is_err());

        // Raw block: the two-varint spelling of count == 1 is not canonical
        // and must be rejected, not decoded as a singleton.
        let mut block = 10u64.to_be_bytes().to_vec();
        write_varint(&mut block, 1 << 1);
        write_varint(&mut block, 300);
        let error = for_each_entry(&block, |_, _| Ok(())).expect_err("non-canonical singleton");
        assert!(error.to_string().contains("canonical"), "{error:#}");

        // Raw block: a varint running past ten bytes must error, not spin.
        let mut block = 10u64.to_be_bytes().to_vec();
        block.extend_from_slice(&[0xff; 10]);
        assert!(for_each_entry(&block, |_, _| Ok(())).is_err());
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
