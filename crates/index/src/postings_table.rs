//! Per-block verification table appended to postings.bin: SHA-256 of every
//! 64 KiB block of the data region, then a footer. Ranged posting reads
//! round to block boundaries and verify against the table — a whole-file
//! hash cannot verify a range, and unverified same-length corruption that
//! still parses is the one path to a silent false negative (#45). The
//! table+footer tail is itself trusted via `postings_tail_hash` in segment
//! metadata, exactly like the sparse dictionary's tail.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::Write;

pub(crate) const POSTINGS_MAGIC: &[u8; 8] = b"SGPOST01";
pub(crate) const VERIFY_BLOCK_BYTES: usize = 64 * 1024;
pub(crate) const FOOTER_BYTES: usize = 8 + 8 + 8;

/// Streams postings data through to `inner`, hashing each 64 KiB block;
/// `finish` appends the table and footer through the same writer so the
/// whole-blob hash covers them too.
pub(crate) struct PostingsTableWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    block_fill: usize,
    hashes: Vec<[u8; 32]>,
    data_len: u64,
}

impl<W: Write> PostingsTableWriter<W> {
    pub(crate) fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha256::new(),
            block_fill: 0,
            hashes: Vec::new(),
            data_len: 0,
        }
    }

    fn close_block(&mut self) {
        let hasher = std::mem::take(&mut self.hasher);
        self.hashes.push(hasher.finalize().into());
        self.block_fill = 0;
    }

    /// Writes the table and footer, returning the inner writer, the data
    /// region length, and the SHA-256 of the appended tail.
    pub(crate) fn finish(mut self) -> Result<(W, u64, String)> {
        if self.block_fill > 0 {
            self.close_block();
        }
        let mut tail = Sha256::new();
        let mut emit = |inner: &mut W, bytes: &[u8]| -> Result<()> {
            tail.update(bytes);
            inner.write_all(bytes)?;
            Ok(())
        };
        for hash in &self.hashes {
            emit(&mut self.inner, hash)?;
        }
        emit(&mut self.inner, &self.data_len.to_le_bytes())?;
        emit(&mut self.inner, &(self.hashes.len() as u64).to_le_bytes())?;
        emit(&mut self.inner, POSTINGS_MAGIC)?;
        Ok((
            self.inner,
            self.data_len,
            crate::sparse_table::hex(&<[u8; 32]>::from(tail.finalize())),
        ))
    }
}

impl<W: Write> Write for PostingsTableWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        let mut rest = &bytes[..written];
        while !rest.is_empty() {
            let take = rest.len().min(VERIFY_BLOCK_BYTES - self.block_fill);
            self.hasher.update(&rest[..take]);
            self.block_fill += take;
            if self.block_fill == VERIFY_BLOCK_BYTES {
                self.close_block();
            }
            rest = &rest[take..];
        }
        self.data_len += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Parsed verification table; block payloads are checked lazily per fetch.
#[derive(Debug)]
pub(crate) struct PostingsTableIndex {
    pub(crate) data_len: u64,
    hashes: Vec<[u8; 32]>,
}

impl PostingsTableIndex {
    /// Parse and validate from the blob's trailing bytes. `blob_len` is the
    /// full postings.bin length; `tail` must be its final bytes (at least
    /// footer; typically table+footer).
    pub(crate) fn parse(blob_len: u64, tail: &[u8]) -> Result<Self> {
        anyhow::ensure!(
            tail.len() >= FOOTER_BYTES && tail.len() as u64 <= blob_len,
            "postings table tail is truncated"
        );
        let footer = &tail[tail.len() - FOOTER_BYTES..];
        anyhow::ensure!(
            &footer[16..24] == POSTINGS_MAGIC,
            "postings table footer magic mismatch"
        );
        let read_u64 = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().expect("eight bytes"));
        let data_len = read_u64(&footer[0..8]);
        let block_count = read_u64(&footer[8..16]);
        anyhow::ensure!(
            block_count == data_len.div_ceil(VERIFY_BLOCK_BYTES as u64),
            "postings table block count does not match its data length"
        );
        let table_len = block_count
            .checked_mul(32)
            .context("postings table length overflows")?;
        anyhow::ensure!(
            data_len
                .checked_add(table_len)
                .and_then(|end| end.checked_add(FOOTER_BYTES as u64))
                == Some(blob_len),
            "postings table does not abut its data region"
        );
        let table_in_tail = tail
            .len()
            .checked_sub(FOOTER_BYTES + usize::try_from(table_len)?)
            .context("postings table tail does not include the full table")?;
        let hashes = tail[table_in_tail..tail.len() - FOOTER_BYTES]
            .chunks_exact(32)
            .map(|chunk| chunk.try_into().expect("thirty-two bytes"))
            .collect();
        Ok(Self { data_len, hashes })
    }

    /// The data-region byte range of verification block `index`.
    pub(crate) fn block_range(&self, index: usize) -> (u64, u64) {
        let offset = index as u64 * VERIFY_BLOCK_BYTES as u64;
        (
            offset,
            (VERIFY_BLOCK_BYTES as u64).min(self.data_len - offset),
        )
    }

    /// Which verification blocks cover the data-region range.
    pub(crate) fn blocks_covering(&self, offset: u64, len: u64) -> Result<std::ops::Range<usize>> {
        let end = offset
            .checked_add(len)
            .context("postings range overflows")?;
        anyhow::ensure!(
            end <= self.data_len && len > 0,
            "postings range is outside the data region"
        );
        Ok(usize::try_from(offset / VERIFY_BLOCK_BYTES as u64)?
            ..usize::try_from(end.div_ceil(VERIFY_BLOCK_BYTES as u64))?)
    }

    /// Verify one fetched block's payload. Failure is index corruption and
    /// must abort the query: unverified bytes could hide documents.
    pub(crate) fn verify(&self, index: usize, payload: &[u8]) -> Result<()> {
        let (_, len) = self.block_range(index);
        anyhow::ensure!(
            payload.len() as u64 == len
                && <[u8; 32]>::from(Sha256::digest(payload)) == self.hashes[index],
            "postings block {index} failed verification"
        );
        Ok(())
    }

    pub(crate) fn block_hash(&self, index: usize) -> &[u8; 32] {
        &self.hashes[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(data: &[u8]) -> (Vec<u8>, u64, String) {
        let mut writer = PostingsTableWriter::new(Vec::new());
        writer.write_all(data).unwrap();
        let (blob, data_len, tail_hash) = writer.finish().unwrap();
        (blob, data_len, tail_hash)
    }

    #[test]
    fn table_round_trips_and_verifies_blocks() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let (blob, data_len, _) = build(&data);
        assert_eq!(data_len, data.len() as u64);
        assert_eq!(&blob[..data.len()], &data[..]);

        let index = PostingsTableIndex::parse(blob.len() as u64, &blob).unwrap();
        assert_eq!(index.data_len, data.len() as u64);
        let blocks = index.blocks_covering(70_000, 1_000).unwrap();
        assert_eq!(blocks, 1..2);
        for block in index.blocks_covering(0, data.len() as u64).unwrap() {
            let (offset, len) = index.block_range(block);
            let payload =
                &data[usize::try_from(offset).unwrap()..][..usize::try_from(len).unwrap()];
            index.verify(block, payload).unwrap();
        }
    }

    #[test]
    fn corrupt_blocks_fail_verification_loudly() {
        let data = vec![7u8; VERIFY_BLOCK_BYTES + 10];
        let (blob, _, _) = build(&data);
        let index = PostingsTableIndex::parse(blob.len() as u64, &blob).unwrap();
        let mut payload = data[..VERIFY_BLOCK_BYTES].to_vec();
        payload[100] ^= 0x01;
        assert!(index.verify(0, &payload).is_err(), "bit flip must fail");
        assert!(
            index.verify(1, &data[..9]).is_err(),
            "wrong length must fail"
        );
    }

    #[test]
    fn parse_rejects_inconsistent_tails() {
        let (blob, _, _) = build(b"hello postings");
        assert!(PostingsTableIndex::parse(blob.len() as u64 + 1, &blob).is_err());
        let mut wrong_magic = blob.clone();
        let len = wrong_magic.len();
        wrong_magic[len - 8..].copy_from_slice(b"NOTMAGIC");
        assert!(PostingsTableIndex::parse(len as u64, &wrong_magic).is_err());
        assert!(PostingsTableIndex::parse(4, b"abcd").is_err());

        // empty data region round-trips (single-doc segments can have no
        // multi-doc posting lists at all)
        let (blob, data_len, _) = build(b"");
        assert_eq!(data_len, 0);
        let index = PostingsTableIndex::parse(blob.len() as u64, &blob).unwrap();
        assert_eq!(index.data_len, 0);
        assert!(index.blocks_covering(0, 1).is_err());
    }
}
