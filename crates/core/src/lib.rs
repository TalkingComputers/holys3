//! Shared types for holys3.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::PathBuf;

pub type DocId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Trigram,
    Sparse,
}

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

/// Every overlapping 3-byte window as raw bytes (sorted, deduped). <3 bytes => empty.
pub fn trigram_grams_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = data.windows(3).map(|w| w.to_vec()).collect();
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

/// `build_all` as raw gram byte strings (sorted, deduped). Index-time.
pub fn sparse_grams_all_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() < 2 {
        return out;
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let n = weights.len();
    for i in 0..n {
        out.push(data[i..i + 2].to_vec());
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
                    out.push(data[i..end].to_vec());
                }
            }
        }
    }
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
    let mut stack: Vec<usize> = Vec::new();
    for i in 0..weights.len() {
        while let Some(&top) = stack.last() {
            if weights[top] <= weights[i] {
                let end = i + 2;
                if end <= data.len() {
                    out.push(data[top..end].to_vec());
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
                out.push(data[prev..end].to_vec());
            }
        }
    }
    if let Some(&pos) = stack.last() {
        let end = pos + 2;
        if end <= data.len() {
            out.push(data[pos..end].to_vec());
        }
    }
    out.sort_unstable();
    out.dedup();
    out
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

/// `build_all` - every sparse n-gram: substring `data[i..=j+1]` whose boundary
/// pair-weights at positions i and j both strictly exceed every interior
/// pair-weight. Index-time. Returns sorted, deduped (`hash`, `gram_len`).
pub fn extract_sparse_ngrams_all(data: &[u8]) -> Vec<(u64, usize)> {
    sparse_grams_all_bytes(data)
        .iter()
        .map(|g| (hash_ngram(g), g.len()))
        .collect()
}

/// `build_covering` - minimal covering set via monotone-stack partitioning.
/// Query-time. `covering(L)` is a subset of `all(F)` whenever `L` is a substring of `F`.
pub fn extract_sparse_ngrams_covering(data: &[u8]) -> Vec<(u64, usize)> {
    sparse_grams_covering_bytes(data)
        .iter()
        .map(|g| (hash_ngram(g), g.len()))
        .collect()
}

/// A source of documents. Implemented by a local dir (tests) and S3 (prod).
pub trait Corpus {
    /// All document ids with their keys (object key / file path).
    fn docs(&self) -> &[(DocId, String)];
    /// Fetch the full bytes of one document.
    fn fetch(&self, id: DocId) -> Result<Vec<u8>>;
}

pub trait BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()>;
    fn get(&self, name: &str) -> Result<Vec<u8>>;
    fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>>;
}

pub struct LocalBlobStore {
    root: PathBuf,
}

impl LocalBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> LocalBlobStore {
        LocalBlobStore { root: root.into() }
    }
}

impl BlobStore for LocalBlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>> {
        Ok(std::fs::read(self.root.join(name))?)
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = std::fs::File::open(self.root.join(name))?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; usize::try_from(len)?];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
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
    use std::time::{SystemTime, UNIX_EPOCH};

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

    #[test]
    fn local_blob_store_round_trips_ranges() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "holys3-core-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let store = LocalBlobStore::new(&root);
        store.put("builds/a/postings.bin", b"abcdef")?;
        assert_eq!(store.get("builds/a/postings.bin")?, b"abcdef");
        assert_eq!(store.get_range("builds/a/postings.bin", 2, 3)?, b"cde");
        std::fs::remove_dir_all(root)?;
        Ok(())
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
