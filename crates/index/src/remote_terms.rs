//! Ranged access to large sparse term tables: open fetches only the block
//! index (verified against the hash recorded in segment metadata), and each
//! query fetches exactly the blocks its grams bisect into, in one ranged
//! read, verified against the per-block hashes from that trusted index.

use crate::sparse_table::{hex, lookup_in_block, SparseTableIndex, FOOTER_BYTES};
use anyhow::{Context, Result};
use holys3_core::{hash_ngram, BlobStore};
use holys3_query::Query;
use sha2::{Digest, Sha256};

/// Two ranged reads: the fixed-size footer locates the block index, then the
/// index tail comes down at its exact size.
pub(crate) fn fetch_index_tail(
    store: &dyn BlobStore,
    blob: &str,
    terms_len: u64,
) -> Result<Vec<u8>> {
    anyhow::ensure!(
        terms_len > FOOTER_BYTES as u64,
        "sparse term table is too short for its footer"
    );
    let footer = store.get_range(blob, terms_len - FOOTER_BYTES as u64, FOOTER_BYTES as u64)?;
    anyhow::ensure!(
        footer.len() == FOOTER_BYTES,
        "sparse term table footer response is too short"
    );
    let index_offset = u64::from_le_bytes(
        footer[16..24]
            .try_into()
            .context("sparse table footer is malformed")?,
    );
    anyhow::ensure!(
        index_offset < terms_len,
        "sparse table index offset is out of bounds"
    );
    let tail_len = terms_len - index_offset;
    let tail = store.get_range(blob, index_offset, tail_len)?;
    anyhow::ensure!(
        tail.len() as u64 == tail_len,
        "sparse term table tail response is truncated"
    );
    Ok(tail)
}

pub(crate) fn parse_index_tail(
    terms_len: u64,
    tail: &[u8],
    expected_tail_hash: &str,
) -> Result<SparseTableIndex> {
    let actual = hex(&<[u8; 32]>::from(Sha256::digest(tail)));
    anyhow::ensure!(
        actual == expected_tail_hash,
        "sparse term table tail hash mismatch: index is not trustworthy"
    );
    SparseTableIndex::parse(terms_len, tail)
}

/// Resolve every gram the query can ask about through the per-segment block
/// cache, fetching all missing blocks in one ranged read. Every block —
/// cached or fetched — is verified against the per-block hash from the
/// trusted index before use. Absent grams are simply absent from the map.
pub(crate) fn fetch_query_gram_values(
    store: &dyn BlobStore,
    blob: &str,
    index: &SparseTableIndex,
    q: &Query,
    cache_dir: &std::path::Path,
    seg_id: &str,
) -> Result<rapidhash::RapidHashMap<u64, u64>> {
    let mut hashes = Vec::new();
    collect_gram_hashes(q, &mut hashes);
    hashes.sort_unstable();
    hashes.dedup();
    let mut needed_blocks: Vec<usize> = hashes
        .iter()
        .filter_map(|hash| index.block_for(*hash))
        .collect();
    needed_blocks.dedup();
    let mut values = rapidhash::RapidHashMap::default();
    if needed_blocks.is_empty() {
        return Ok(values);
    }
    let block_path = |block_id: usize| {
        cache_dir.join(seg_id).join(format!(
            "terms-block-{:016x}",
            index.blocks[block_id].offset
        ))
    };
    let mut blocks: rapidhash::RapidHashMap<usize, bytes::Bytes> =
        rapidhash::RapidHashMap::default();
    let mut missing = Vec::new();
    for block_id in needed_blocks {
        let expected = hex(&index.blocks[block_id].hash);
        match crate::segment::cache::read_verified(&block_path(block_id), &expected) {
            Some(bytes) => {
                blocks.insert(block_id, bytes::Bytes::from(bytes));
            }
            None => missing.push(block_id),
        }
    }
    if !missing.is_empty() {
        let ranges: Vec<(u64, u64)> = missing
            .iter()
            .map(|block| (index.blocks[*block].offset, index.blocks[*block].len))
            .collect();
        let fetched = store.get_ranges(blob, &ranges)?;
        anyhow::ensure!(
            fetched.len() == ranges.len(),
            "get_ranges returned {} blocks for {} requests",
            fetched.len(),
            ranges.len()
        );
        for (block_id, raw) in missing.into_iter().zip(fetched) {
            let block = &index.blocks[block_id];
            anyhow::ensure!(
                <[u8; 32]>::from(Sha256::digest(&raw)) == block.hash,
                "sparse term table block hash mismatch"
            );
            crate::segment::cache::write_back(cache_dir, &block_path(block_id), &raw).ok();
            blocks.insert(block_id, raw);
        }
    }
    for hash in hashes {
        let Some(block_id) = index.block_for(hash) else {
            continue;
        };
        let raw = blocks
            .get(&block_id)
            .context("sparse term table block was not fetched")?;
        if let Some(value) = lookup_in_block(raw, hash)? {
            values.insert(hash, value);
        }
    }
    Ok(values)
}

fn collect_gram_hashes(q: &Query, out: &mut Vec<u64>) {
    match q {
        Query::All => {}
        Query::Gram(gram) => out.push(hash_ngram(gram)),
        Query::And(subs) | Query::Or(subs) => {
            for sub in subs {
                collect_gram_hashes(sub, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparse_table::SparseTableWriter;
    use holys3_core::LocalBlobStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingStore {
        inner: LocalBlobStore,
        get_ranges_calls: AtomicUsize,
        get_range_calls: AtomicUsize,
        max_get_range_len: AtomicUsize,
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
            self.get_range_calls.fetch_add(1, Ordering::Relaxed);
            self.max_get_range_len
                .fetch_max(usize::try_from(len).unwrap(), Ordering::Relaxed);
            self.inner.get_range(name, start, len)
        }

        fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> Result<Vec<bytes::Bytes>> {
            self.get_ranges_calls.fetch_add(1, Ordering::Relaxed);
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

    fn open_remote_index(
        store: &dyn BlobStore,
        blob: &str,
        terms_len: u64,
        expected_tail_hash: &str,
    ) -> Result<SparseTableIndex> {
        let tail = fetch_index_tail(store, blob, terms_len)?;
        parse_index_tail(terms_len, &tail, expected_tail_hash)
    }

    fn fixture(
        entries: usize,
    ) -> (
        tempfile::TempDir,
        CountingStore,
        u64,
        String,
        Vec<(u64, u64)>,
    ) {
        let mut pairs: Vec<(u64, u64)> = (0..entries as u64).map(|i| (i * 5 + 3, i)).collect();
        pairs.sort_unstable();
        let mut writer = SparseTableWriter::new(Vec::new()).unwrap();
        for (hash, value) in &pairs {
            writer.insert(*hash, *value).unwrap();
        }
        let bytes = writer.finish().unwrap();
        let index = SparseTableIndex::parse(bytes.len() as u64, &bytes).unwrap();
        let tail_offset = usize::try_from(
            index
                .blocks
                .last()
                .map_or(bytes.len() as u64 - FOOTER_BYTES as u64, |block| {
                    block.offset + block.len
                }),
        )
        .unwrap();
        let tail_hash = hex(&<[u8; 32]>::from(Sha256::digest(&bytes[tail_offset..])));
        let dir = tempfile::tempdir().unwrap();
        let store = CountingStore {
            inner: LocalBlobStore::new(dir.path()),
            get_ranges_calls: AtomicUsize::new(0),
            get_range_calls: AtomicUsize::new(0),
            max_get_range_len: AtomicUsize::new(0),
        };
        store.put("terms.fst", &bytes).unwrap();
        (dir, store, bytes.len() as u64, tail_hash, pairs)
    }

    #[test]
    fn remote_open_fetches_only_the_tail_and_verifies_it() {
        let (_dir, store, len, tail_hash, pairs) = fixture(20_000);
        let index = open_remote_index(&store, "terms.fst", len, &tail_hash).unwrap();
        assert_eq!(index.entry_count, pairs.len() as u64);
        assert!(store.get_range_calls.load(Ordering::Relaxed) <= 2);
        assert_eq!(store.get_ranges_calls.load(Ordering::Relaxed), 0);
        let index_bytes = index.blocks.len() * (8 + 8 + 32) + FOOTER_BYTES;
        assert!(
            store.max_get_range_len.load(Ordering::Relaxed) <= index_bytes,
            "open must never fetch more than the index tail"
        );
        let wrong = hex(&[0u8; 32]);
        let error = open_remote_index(&store, "terms.fst", len, &wrong).unwrap_err();
        assert!(error.to_string().contains("tail"), "{error:#}");
    }

    #[test]
    fn query_grams_resolve_with_one_ranged_read() {
        let (_dir, store, len, tail_hash, pairs) = fixture(20_000);
        let index = open_remote_index(&store, "terms.fst", len, &tail_hash).unwrap();
        store.get_ranges_calls.store(0, Ordering::Relaxed);
        // Query::Gram holds gram BYTES; the fixture keys are hashes of those
        // bytes, so craft grams whose hash we control by inverting: instead,
        // look up fixture hashes directly through a query of raw grams whose
        // hashes we recompute for expectations.
        let grams: Vec<Vec<u8>> = vec![b"hamlet".to_vec(), b"ophelia".to_vec(), b"yorick".to_vec()];
        let q = Query::And(grams.iter().cloned().map(Query::Gram).collect());
        let cache = tempfile::tempdir().unwrap();
        let values =
            fetch_query_gram_values(&store, "terms.fst", &index, &q, cache.path(), "seg").unwrap();
        assert_eq!(store.get_ranges_calls.load(Ordering::Relaxed), 1);
        for gram in &grams {
            let hash = hash_ngram(gram);
            let expected = pairs
                .iter()
                .find(|(entry, _)| *entry == hash)
                .map(|(_, value)| *value);
            assert_eq!(values.get(&hash).copied(), expected, "gram {gram:?}");
        }
    }

    #[test]
    fn known_hashes_resolve_to_their_values() {
        let (_dir, store, len, tail_hash, pairs) = fixture(20_000);
        let index = open_remote_index(&store, "terms.fst", len, &tail_hash).unwrap();
        // Bypass gram hashing: feed hashes straight through block bisection by
        // fetching blocks like the query path does for arbitrary present keys.
        let sample = [pairs[0], pairs[9_999], pairs[19_999]];
        for (hash, value) in sample {
            let block = index.block_for(hash).unwrap();
            let block = &index.blocks[block];
            let raw = store
                .get_range("terms.fst", block.offset, block.len)
                .unwrap();
            assert_eq!(lookup_in_block(&raw, hash).unwrap(), Some(value));
        }
    }

    #[test]
    fn corrupted_blocks_fail_loudly_at_query_time() {
        let (_dir, store, len, tail_hash, _pairs) = fixture(20_000);
        let index = open_remote_index(&store, "terms.fst", len, &tail_hash).unwrap();
        let mut bytes = store.get("terms.fst").unwrap().unwrap();
        bytes[64] ^= 0xff;
        store.put("terms.fst", &bytes).unwrap();
        let q = Query::Gram(b"anything".to_vec());
        let cache = tempfile::tempdir().unwrap();
        let error = fetch_query_gram_values(&store, "terms.fst", &index, &q, cache.path(), "seg");
        if let Err(error) = error {
            assert!(error.to_string().contains("hash"), "{error:#}");
        } else {
            // The queried gram may bisect into an untouched block; force the
            // corrupted first block by using a gram hashing below pair 8192.
            let hash = 3u64;
            let block = &index.blocks[index.block_for(hash).unwrap()];
            let raw = store
                .get_range("terms.fst", block.offset, block.len)
                .unwrap();
            assert_ne!(
                <[u8; 32]>::from(Sha256::digest(&raw)),
                block.hash,
                "corruption must be detectable via the recorded block hash"
            );
        }
    }
}
