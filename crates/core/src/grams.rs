use serde::{Deserialize, Serialize};
use std::mem::size_of;
use std::ops::Range;

const RADIX_SORT_MIN: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Trigram,
    Sparse,
}

pub fn pack_trigram_grams(data: &[u8]) -> Vec<u32> {
    const BITMAP_WORDS: usize = (1 << 24) / 64;
    const BITMAP_THRESHOLD: usize = BITMAP_WORDS * size_of::<u64>() / size_of::<u32>();
    if data.len().saturating_sub(2) > BITMAP_THRESHOLD {
        let mut bitmap = vec![0u64; BITMAP_WORDS];
        for window in data.windows(3) {
            let gram =
                usize::from(window[0]) << 16 | usize::from(window[1]) << 8 | usize::from(window[2]);
            bitmap[gram / 64] |= 1u64 << (gram % 64);
        }
        let count = bitmap.iter().map(|word| word.count_ones() as usize).sum();
        let mut packed = Vec::with_capacity(count);
        for (word_index, mut word) in bitmap.into_iter().enumerate() {
            while word != 0 {
                let bit = word.trailing_zeros() as usize;
                packed.push((word_index * 64 + bit) as u32);
                word &= word - 1;
            }
        }
        return packed;
    }
    let mut packed: Vec<u32> = data
        .windows(3)
        .map(|w| u32::from(w[0]) << 16 | u32::from(w[1]) << 8 | u32::from(w[2]))
        .collect();
    sort_packed_grams(&mut packed);
    packed
}

/// Logical candidate-block size: postings can attribute grams to fixed
/// 128 KiB windows of a document's decoded content, so verification fetches
/// blocks instead of whole documents (#85). A trigram straddling a window
/// boundary is attributed to the window of its FIRST byte; the reader
/// compensates with per-document line-length slack when intersecting.
pub const CANDIDATE_BLOCK_BYTES: usize = 128 * 1024;

/// Distinct (block, packed-trigram) pairs of `data`, sorted by block then
/// gram. Each 128 KiB window is deduplicated independently, so the output
/// is the block-granular form of `pack_trigram_grams` (whose output equals
/// the gram set of this function with blocks erased — pinned by test).
pub fn pack_trigram_block_grams(data: &[u8]) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    if data.len() < 3 {
        return out;
    }
    let mut block = 0u32;
    let mut start = 0usize;
    while start + 2 < data.len() {
        // Windows overlap by 2 bytes so straddling trigrams exist exactly
        // once, attributed to the window containing their first byte.
        let end = (start + CANDIDATE_BLOCK_BYTES + 2).min(data.len());
        let grams = pack_trigram_grams(&data[start..end]);
        out.extend(grams.into_iter().map(|gram| (block, gram)));
        start += CANDIDATE_BLOCK_BYTES;
        block += 1;
    }
    out
}

fn sort_packed_grams(grams: &mut Vec<u32>) {
    if grams.len() < RADIX_SORT_MIN {
        grams.sort_unstable();
    } else {
        radsort::sort(grams);
    }
    grams.dedup();
}

/// Every overlapping 3-byte window as raw bytes (sorted, deduped). <3 bytes => empty.
/// Windows pack big-endian into u32 so sort+dedup run over integers (u32
/// order == lexicographic byte order) and only distinct grams allocate.
pub fn trigram_grams_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    pack_trigram_grams(data)
        .into_iter()
        .map(|g| vec![(g >> 16) as u8, (g >> 8) as u8, g as u8])
        .collect()
}

/// Stable u64 hash of an n-gram's bytes. Deterministic across runs/platforms
/// (used as the on-disk + in-memory gram key).
/// Sparse gram keys are 40-bit hashes. Truncation shrinks the dictionary's
/// sorted hash deltas (measured on real prose: −22% of the whole dictionary
/// vs 48 bits), and a collision only merges the colliding grams' postings —
/// a slightly larger candidate superset that verification filters exactly,
/// and that AND queries deprioritize the way they do any common gram.
/// Measured on a 126M-gram corpus: 0.01% of grams collide, adding 0.0003
/// candidate docs per query gram on average. Changing this width changes
/// the index format.
pub fn hash_ngram(gram: &[u8]) -> u64 {
    rapidhash::v3::rapidhash_v3(gram) & ((1 << 40) - 1)
}

/// Deterministic weight of an adjacent byte pair. Drives sparse-ngram
/// boundary selection. Only affects selectivity, never correctness.
fn pair_weight(a: u8, b: u8) -> u32 {
    rapidhash::v3::rapidhash_v3(&[a, b]) as u32
}

pub fn iterate_sparse_gram_ranges(
    len: usize,
    mut byte: impl FnMut(usize) -> u8,
) -> impl Iterator<Item = Range<usize>> {
    let mut ranges = start_sparse_gram_ranges(len);
    std::iter::from_fn(move || {
        match ranges.next_with(|index| Ok::<u8, std::convert::Infallible>(byte(index))) {
            Ok(range) => range,
            Err(error) => match error {},
        }
    })
}

pub struct SparseGramRanges {
    pair_count: usize,
    start: usize,
    end: usize,
    interior_max: u32,
    start_weight: u32,
    emit_pair: bool,
}

pub fn start_sparse_gram_ranges(len: usize) -> SparseGramRanges {
    SparseGramRanges {
        pair_count: len.saturating_sub(1),
        start: 0,
        end: 0,
        interior_max: 0,
        start_weight: 0,
        emit_pair: true,
    }
}

impl SparseGramRanges {
    pub fn next_with<E>(
        &mut self,
        mut byte: impl FnMut(usize) -> Result<u8, E>,
    ) -> Result<Option<Range<usize>>, E> {
        loop {
            if self.start >= self.pair_count {
                return Ok(None);
            }
            if self.emit_pair {
                self.emit_pair = false;
                self.end = self.start + 1;
                self.interior_max = 0;
                self.start_weight = pair_weight(byte(self.start)?, byte(self.start + 1)?);
                return Ok(Some(self.start..self.start + 2));
            }
            while self.end < self.pair_count {
                if self.end > self.start + 1 {
                    self.interior_max = self
                        .interior_max
                        .max(pair_weight(byte(self.end - 1)?, byte(self.end)?));
                }
                if self.interior_max >= self.start_weight {
                    break;
                }
                let current = self.end;
                self.end += 1;
                if pair_weight(byte(current)?, byte(current + 1)?) > self.interior_max {
                    return Ok(Some(self.start..current + 2));
                }
            }
            self.start += 1;
            self.emit_pair = true;
        }
    }
}

pub fn iterate_sparse_grams(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    iterate_sparse_gram_ranges(data.len(), |index| data[index]).map(move |range| &data[range])
}

/// `build_all` as raw gram byte strings (sorted, deduped). Index-time.
pub fn sparse_grams_all_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = iterate_sparse_grams(data)
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    out.sort_unstable();
    out.dedup();
    out
}

/// `build_covering` as raw gram byte strings (sorted, deduped). Query-time.
pub fn sparse_grams_covering_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() < 2 {
        return out;
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    // Every emission goes through `is_indexed_gram`: a query-side gram that
    // the index-side builder would never emit (weight TIES inside
    // repeated-byte runs create exactly that) would silently return zero
    // candidates for true matches. covering ⊆ all holds by construction.
    let push = |out: &mut Vec<Vec<u8>>, a: usize, end: usize| {
        if is_indexed_gram(&weights, a, end) {
            out.push(data[a..end].to_vec());
        }
    };
    let mut stack: Vec<usize> = Vec::new();
    for i in 0..weights.len() {
        while let Some(&top) = stack.last() {
            if weights[top] <= weights[i] {
                push(&mut out, top, i + 2);
                if weights[top] == weights[i] {
                    stack.pop();
                    break;
                }
                stack.pop();
            } else {
                break;
            }
        }
        stack.push(i);
    }
    while stack.len() > 1 {
        let top = stack.pop().unwrap();
        if let Some(&prev) = stack.last() {
            push(&mut out, prev, top + 2);
        }
    }
    if let Some(&pos) = stack.last() {
        push(&mut out, pos, pos + 2);
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// Would `sparse_grams_all_bytes` emit the gram `data[a..end]`? Mirrors its
/// loop exactly: length-2 grams always; longer grams need every interior
/// weight below the start weight AND the final pair weight above all
/// interior weights.
fn is_indexed_gram(weights: &[u32], a: usize, end: usize) -> bool {
    let last = end - 2; // index of the gram's final pair
    if last == a {
        return true;
    }
    let interior_max = weights[a + 1..last].iter().copied().max().unwrap_or(0);
    interior_max < weights[a] && weights[last] > interior_max
}

/// Index-time grams for a strategy.
pub fn grams_index(data: &[u8], s: Strategy) -> Vec<Vec<u8>> {
    match s {
        Strategy::Trigram => trigram_grams_bytes(data),
        Strategy::Sparse => sparse_grams_all_bytes(data),
    }
}

/// Query-time grams for a strategy (trigram has no separate covering form).
pub fn grams_query(data: &[u8], s: Strategy) -> Vec<Vec<u8>> {
    match s {
        Strategy::Trigram => trigram_grams_bytes(data),
        Strategy::Sparse => sparse_grams_covering_bytes(data),
    }
}

#[cfg(test)]
mod invariant_grams {
    use super::{hash_ngram, sparse_grams_all_bytes, sparse_grams_covering_bytes};

    pub(super) fn extract_sparse_ngrams_all(data: &[u8]) -> Vec<(u64, usize)> {
        sparse_grams_all_bytes(data)
            .iter()
            .map(|g| (hash_ngram(g), g.len()))
            .collect()
    }

    pub(super) fn extract_sparse_ngrams_covering(data: &[u8]) -> Vec<(u64, usize)> {
        sparse_grams_covering_bytes(data)
            .iter()
            .map(|g| (hash_ngram(g), g.len()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    /// A real 40-bit collision pair (brute-forced against rapidhash v3).
    /// Pins both the hash function's stability and a fixture other crates'
    /// collision tests rely on: colliding grams must merge into a candidate
    /// superset, never lose documents.
    #[test]
    fn collision_fixture_still_collides() {
        assert_eq!(
            hash_ngram(b"czopbaaa"),
            hash_ngram(b"plo ba"),
            "fixture pair must collide at 40 bits"
        );
        assert_eq!(hash_ngram(b"czopbaaa"), 0x00bc_761f_b68b);
    }

    use super::*;

    #[test]
    fn trigrams_basic() {
        // "abcab" -> abc, bca, cab sorted; "abc" appears once after dedup
        assert_eq!(
            trigram_grams_bytes(b"abcab"),
            vec![b"abc".to_vec(), b"bca".to_vec(), b"cab".to_vec()]
        );
        assert_eq!(
            pack_trigram_grams(b"abcab"),
            vec![0x616263, 0x626361, 0x636162]
        );
    }

    #[test]
    fn block_grams_erase_to_doc_grams() {
        // Blocks erased and deduped must equal the doc-level gram set, for
        // sizes hitting the empty, single-window, exact-boundary,
        // boundary+straddler, and multi-window cases.
        for len in [
            0,
            2,
            3,
            4096,
            CANDIDATE_BLOCK_BYTES - 1,
            CANDIDATE_BLOCK_BYTES,
            CANDIDATE_BLOCK_BYTES + 1,
            CANDIDATE_BLOCK_BYTES + 2,
            CANDIDATE_BLOCK_BYTES + 3,
            3 * CANDIDATE_BLOCK_BYTES + 17,
        ] {
            let mut state = 0x9e37_79b9_u32;
            let data: Vec<u8> = (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 17;
                    state ^= state << 5;
                    (state % 7) as u8 + b'a'
                })
                .collect();
            let mut erased: Vec<u32> = pack_trigram_block_grams(&data)
                .into_iter()
                .map(|(_, gram)| gram)
                .collect();
            erased.sort_unstable();
            erased.dedup();
            assert_eq!(erased, pack_trigram_grams(&data), "length {len}");
        }
    }

    #[test]
    fn straddling_trigram_attributed_to_first_byte_block() {
        // A trigram whose bytes cross the window boundary belongs to the
        // window of its first byte, and only that window.
        let mut data = vec![b'a'; CANDIDATE_BLOCK_BYTES + 4];
        let boundary = CANDIDATE_BLOCK_BYTES;
        data[boundary - 1] = b'x';
        data[boundary] = b'y';
        data[boundary + 1] = b'z';
        let gram = u32::from(b'x') << 16 | u32::from(b'y') << 8 | u32::from(b'z');
        let pairs = pack_trigram_block_grams(&data);
        let hits: Vec<u32> = pairs
            .iter()
            .filter(|(_, g)| *g == gram)
            .map(|(b, _)| *b)
            .collect();
        assert_eq!(hits, vec![0], "xyz starts at the last byte of block 0");
    }

    #[test]
    fn trigrams_short_is_empty() {
        assert!(trigram_grams_bytes(b"ab").is_empty());
        assert!(trigram_grams_bytes(b"").is_empty());
    }

    #[test]
    fn trigrams_large_repeated_input_is_deduplicated() {
        assert_eq!(pack_trigram_grams(&vec![b'a'; 600_000]), vec![0x616161]);
    }

    #[test]
    fn packed_sort_matches_control() {
        for len in [0, 1, 2, 3, 31, 255, 256, 4096, 600_000] {
            let mut state = 0x9e37_79b9_u32;
            let grams = (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 17;
                    state ^= state << 5;
                    state & 0x00ff_ffff
                })
                .collect::<Vec<_>>();
            let mut expected = grams.clone();
            expected.sort_unstable();
            expected.dedup();
            let mut actual = grams;
            sort_packed_grams(&mut actual);
            assert_eq!(actual, expected, "length {len}");
        }
    }

    #[test]
    fn trigram_query_subset_of_index() {
        use std::collections::HashSet;
        let pattern = b"CONSTANT";
        let content = b"let CONSTANT = 1;";
        let all: HashSet<Vec<u8>> = grams_index(content, Strategy::Trigram)
            .into_iter()
            .collect();
        let q: HashSet<Vec<u8>> = grams_query(pattern, Strategy::Trigram)
            .into_iter()
            .collect();
        assert!(q.is_subset(&all));
    }
}

#[cfg(test)]
mod sparse_tests {
    use super::invariant_grams::{extract_sparse_ngrams_all, extract_sparse_ngrams_covering};
    use super::*;
    use std::collections::HashSet;

    fn collect_sparse_reference(data: &[u8]) -> Vec<Vec<u8>> {
        let mut grams = Vec::new();
        if data.len() < 2 {
            return grams;
        }
        let weights = data
            .windows(2)
            .map(|window| pair_weight(window[0], window[1]))
            .collect::<Vec<_>>();
        for start in 0..weights.len() {
            grams.push(data[start..start + 2].to_vec());
            let mut interior_max = 0;
            for end in start + 1..weights.len() {
                if end > start + 1 {
                    interior_max = interior_max.max(weights[end - 1]);
                }
                if interior_max >= weights[start] {
                    break;
                }
                if weights[end] > interior_max {
                    grams.push(data[start..end + 2].to_vec());
                }
            }
        }
        grams
    }

    #[test]
    fn sparse_short_input() {
        assert!(extract_sparse_ngrams_all(b"a").is_empty());
        assert!(!extract_sparse_ngrams_all(b"ab").is_empty());
        assert!(extract_sparse_ngrams_covering(b"a").is_empty());
        assert!(!extract_sparse_ngrams_covering(b"ab").is_empty());
    }

    #[test]
    fn sparse_iterator_matches_reference_emissions() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for len in 0..256 {
            let input = (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state.to_le_bytes()[0]
                })
                .collect::<Vec<_>>();
            let actual = iterate_sparse_grams(&input)
                .map(<[u8]>::to_vec)
                .collect::<Vec<_>>();
            assert_eq!(actual, collect_sparse_reference(&input), "length {len}");
        }
    }

    #[test]
    fn covering_subset_of_all_same_input() {
        let input = b"MAX_FILE_SIZE";
        let all: HashSet<u64> = extract_sparse_ngrams_all(input)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        let cov: HashSet<u64> = extract_sparse_ngrams_covering(input)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        assert!(cov.is_subset(&all));
        assert!(all.len() >= cov.len());
    }

    #[test]
    fn subset_invariant_modified_constant() {
        // covering(pattern) must be a subset of all(content) when pattern occurs in content.
        let pattern = b"MODIFIED_CONSTANT";
        let content = b"fn main() {\n let x = MODIFIED_CONSTANT;\n}\n";
        let all: HashSet<u64> = extract_sparse_ngrams_all(content)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        let cov: HashSet<u64> = extract_sparse_ngrams_covering(pattern)
            .iter()
            .map(|(h, _)| *h)
            .collect();
        let missing: Vec<u64> = cov.difference(&all).copied().collect();
        assert!(
            missing.is_empty(),
            "covering(pattern) must be subset of all(content); missing: {missing:?}"
        );
    }

    #[test]
    fn covering_bytes_subset_of_all_bytes() {
        let pattern = b"MODIFIED_CONSTANT";
        let content = b"fn main() {\n let x = MODIFIED_CONSTANT;\n}\n";
        let all: HashSet<Vec<u8>> = sparse_grams_all_bytes(content).into_iter().collect();
        let cov: HashSet<Vec<u8>> = sparse_grams_covering_bytes(pattern).into_iter().collect();
        assert!(
            cov.is_subset(&all),
            "covering bytes must be subset of all bytes"
        );
    }

    #[test]
    fn subset_invariant_randomized() {
        // Deterministic pseudo-random fuzz of the invariant across many embeddings.
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..200 {
            let plen = 2 + (next() % 12) as usize;
            let pattern: Vec<u8> = (0..plen).map(|_| (next() % 96 + 32) as u8).collect();
            let pre: Vec<u8> = (0..(next() % 8) as usize)
                .map(|_| (next() % 96 + 32) as u8)
                .collect();
            let post: Vec<u8> = (0..(next() % 8) as usize)
                .map(|_| (next() % 96 + 32) as u8)
                .collect();
            let mut content = pre.clone();
            content.extend_from_slice(&pattern);
            content.extend_from_slice(&post);
            let all: HashSet<u64> = extract_sparse_ngrams_all(&content)
                .iter()
                .map(|(h, _)| *h)
                .collect();
            let cov: HashSet<u64> = extract_sparse_ngrams_covering(&pattern)
                .iter()
                .map(|(h, _)| *h)
                .collect();
            assert!(
                cov.is_subset(&all),
                "invariant broke for pattern {pattern:?} in content {content:?}"
            );
        }
    }

    #[test]
    fn sparse_covering_grams_subset_on_repeated_byte_runs() {
        for input in [
            b"uniq000".to_vec(),
            b"aaa".to_vec(),
            b"xaaay".to_vec(),
            b"err000timeout".to_vec(),
            b"aaaaaaaaaa".to_vec(),
            b"ab".repeat(8),
        ] {
            let all: HashSet<Vec<u8>> = sparse_grams_all_bytes(&input).into_iter().collect();
            for gram in sparse_grams_covering_bytes(&input) {
                assert!(
                    all.contains(&gram),
                    "covering gram {:?} of {:?} never indexed",
                    String::from_utf8_lossy(&gram),
                    String::from_utf8_lossy(&input)
                );
            }
        }
    }
}
