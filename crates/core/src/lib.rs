//! Shared types for holys3.

use anyhow::Result;
use std::collections::BTreeSet;

pub type DocId = u32;

/// Pack a 3-byte window into a u32 trigram key: b0<<16 | b1<<8 | b2.
/// Returns sorted, deduplicated trigrams. Fewer than 3 bytes => empty.
pub fn trigrams(bytes: &[u8]) -> Vec<u32> {
    let mut v: Vec<u32> = bytes
        .windows(3)
        .map(|w| (w[0] as u32) << 16 | (w[1] as u32) << 8 | w[2] as u32)
        .collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// Stable u64 hash of an n-gram's bytes. Deterministic across runs/platforms
/// (used as the on-disk + in-memory gram key).
pub fn hash_ngram(gram: &[u8]) -> u64 {
    rapidhash::v3::rapidhash_v3(gram)
}

/// Deterministic weight of an adjacent byte pair. Drives sparse-ngram
/// boundary selection. Only affects selectivity, never correctness.
pub fn pair_weight(a: u8, b: u8) -> u32 {
    rapidhash::v3::rapidhash_v3(&[a, b]) as u32
}

/// build_all — every sparse n-gram: substring data[i..=j+1] whose boundary
/// pair-weights at positions i and j both strictly exceed every interior
/// pair-weight. Index-time. Returns sorted, deduped (hash, gram_len).
pub fn extract_sparse_ngrams_all(data: &[u8]) -> Vec<(u64, usize)> {
    if data.len() < 2 {
        return Vec::new();
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let n = weights.len();
    let mut ngrams = Vec::new();
    for i in 0..n {
        let gram = &data[i..i + 2];
        ngrams.push((hash_ngram(gram), gram.len()));
        let mut interior_max: u32 = 0;
        for j in (i + 1)..n {
            if j > i + 1 {
                interior_max = interior_max.max(weights[j - 1]);
            }
            if interior_max >= weights[i] {
                break;
            }
            if weights[j] > interior_max {
                let end = j + 2;
                if end <= data.len() {
                    let gram = &data[i..end];
                    ngrams.push((hash_ngram(gram), gram.len()));
                }
            }
        }
    }
    ngrams.sort_unstable();
    ngrams.dedup();
    ngrams
}

/// build_covering — minimal covering set via monotone-stack partitioning.
/// Query-time. covering(L) ⊆ all(F) whenever L is a substring of F.
pub fn extract_sparse_ngrams_covering(data: &[u8]) -> Vec<(u64, usize)> {
    if data.len() < 2 {
        return Vec::new();
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let mut ngrams = Vec::new();
    let mut stack: Vec<usize> = Vec::new();
    for i in 0..weights.len() {
        while let Some(&top) = stack.last() {
            if weights[top] <= weights[i] {
                let start = top;
                let end = i + 2;
                if end <= data.len() {
                    let gram = &data[start..end];
                    ngrams.push((hash_ngram(gram), gram.len()));
                }
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
            let end = top + 2;
            if end <= data.len() {
                let gram = &data[prev..end];
                ngrams.push((hash_ngram(gram), gram.len()));
            }
        }
    }
    if let Some(&pos) = stack.last() {
        let end = pos + 2;
        if end <= data.len() {
            let gram = &data[pos..end];
            ngrams.push((hash_ngram(gram), gram.len()));
        }
    }
    ngrams.sort_unstable();
    ngrams.dedup();
    ngrams
}

/// A source of documents. Implemented by a local dir (tests) and S3 (prod).
pub trait Corpus {
    /// All document ids with their keys (object key / file path).
    fn docs(&self) -> &[(DocId, String)];
    /// Fetch the full bytes of one document.
    fn fetch(&self, id: DocId) -> Result<Vec<u8>>;
}

/// One verified match: which doc, 1-based line, 1-based column (byte), the line text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub doc: DocId,
    pub line: usize,
    pub col: usize,
    pub text: String,
}

/// Run `re` over `bytes`, returning one Match per matching line occurrence.
pub fn matches_in(doc: DocId, bytes: &[u8], re: &regex::bytes::Regex) -> Vec<Match> {
    let mut out = Vec::new();
    for m in re.find_iter(bytes) {
        let start = m.start();
        let line_start = bytes[..start]
            .iter()
            .rposition(|&b| b == b'\n')
            .map_or(0, |p| p + 1);
        let line_end = bytes[start..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(bytes.len(), |p| start + p);
        out.push(Match {
            doc,
            line: bytes[..start].iter().filter(|&&b| b == b'\n').count() + 1,
            col: start - line_start + 1,
            text: String::from_utf8_lossy(&bytes[line_start..line_end]).into_owned(),
        });
    }
    out
}

/// Oracle: docs that contain at least one match. The differential ground truth.
pub fn scan_matching_docs(
    corpus: &dyn Corpus,
    re: &regex::bytes::Regex,
) -> Result<BTreeSet<DocId>> {
    let mut hits = BTreeSet::new();
    for &(id, _) in corpus.docs() {
        let bytes = corpus.fetch(id)?;
        if re.is_match(&bytes) {
            hits.insert(id);
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigrams_basic() {
        // "abcab" -> abc, bca, cab ; "abc" appears once after dedup
        let t = trigrams(b"abcab");
        let abc = (b'a' as u32) << 16 | (b'b' as u32) << 8 | b'c' as u32;
        let bca = (b'b' as u32) << 16 | (b'c' as u32) << 8 | b'a' as u32;
        let cab = (b'c' as u32) << 16 | (b'a' as u32) << 8 | b'b' as u32;
        assert_eq!(t, {
            let mut e = vec![abc, bca, cab];
            e.sort_unstable();
            e
        });
    }

    #[test]
    fn trigrams_short_is_empty() {
        assert!(trigrams(b"ab").is_empty());
        assert!(trigrams(b"").is_empty());
    }
}

#[cfg(test)]
mod sparse_tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn sparse_short_input() {
        assert!(extract_sparse_ngrams_all(b"a").is_empty());
        assert!(!extract_sparse_ngrams_all(b"ab").is_empty());
        assert!(extract_sparse_ngrams_covering(b"a").is_empty());
        assert!(!extract_sparse_ngrams_covering(b"ab").is_empty());
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
}

#[cfg(test)]
mod corpus_tests {
    use super::*;

    struct MemCorpus(Vec<(DocId, String)>, Vec<Vec<u8>>);
    impl Corpus for MemCorpus {
        fn docs(&self) -> &[(DocId, String)] {
            &self.0
        }
        fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
            Ok(self.1[id as usize].clone())
        }
    }

    #[test]
    fn scan_finds_matching_docs() {
        let c = MemCorpus(
            vec![(0, "a".into()), (1, "b".into())],
            vec![b"hello world".to_vec(), b"nothing here".to_vec()],
        );
        let re = regex::bytes::Regex::new("world").unwrap();
        assert_eq!(scan_matching_docs(&c, &re).unwrap(), BTreeSet::from([0]));
    }

    #[test]
    fn match_line_col() {
        let m = matches_in(
            7,
            b"foo\nbar baz",
            &regex::bytes::Regex::new("baz").unwrap(),
        );
        assert_eq!(
            m,
            vec![Match {
                doc: 7,
                line: 2,
                col: 5,
                text: "bar baz".into()
            }]
        );
    }
}
