#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Shared types, gram extraction, storage traits, and scan verification.

use anyhow::{Context, Result as AnyhowResult};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub type DocId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    Raw,
    Gzip,
    Zstd,
}

/// Detect compression by magic bytes; key extensions are not trusted.
/// Gzip requires the deflate method byte (1f 8b 08) — the only method the
/// format defines. Zstd covers both regular frames (28 b5 2f fd) and
/// skippable frames (5? 2a 4d 18), which may legally come first.
pub fn detect_codec(bytes: &[u8]) -> Codec {
    if bytes.starts_with(&[0x1f, 0x8b, 0x08]) {
        Codec::Gzip
    } else if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd])
        || (bytes.len() >= 4
            && bytes[0] & 0xf0 == 0x50
            && bytes[1] == 0x2a
            && bytes[2] == 0x4d
            && bytes[3] == 0x18)
    {
        Codec::Zstd
    } else {
        Codec::Raw
    }
}

/// Transparently decompress an object body. Gzip uses `MultiGzDecoder`
/// because AWS log deliveries (ALB, `CloudTrail`, `CloudFront`) concatenate
/// gzip members; a single-member decoder silently truncates them. A decode
/// error after some members already decoded (trailing padding or a truncated
/// tail) salvages the decoded text with a warning — for grep, partial
/// coverage beats dropping the object.
pub fn decode_body(key: &str, bytes: Vec<u8>) -> AnyhowResult<Vec<u8>> {
    match detect_codec(&bytes) {
        Codec::Raw => Ok(bytes),
        Codec::Gzip => {
            let mut out = Vec::new();
            match std::io::Read::read_to_end(
                &mut flate2::read::MultiGzDecoder::new(bytes.as_slice()),
                &mut out,
            ) {
                Ok(_) => Ok(out),
                Err(err) if !out.is_empty() => {
                    eprintln!(
                        "warning: {key}: gzip stream ends in garbage ({err}); \
                         searching the {} bytes that decoded",
                        out.len()
                    );
                    Ok(out)
                }
                Err(err) => {
                    Err(anyhow::Error::new(err).context(format!("gzip decode failed for {key}")))
                }
            }
        }
        Codec::Zstd => zstd::stream::decode_all(bytes.as_slice())
            .with_context(|| format!("zstd decode failed for {key}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    Trigram,
    Sparse,
}

/// Every overlapping 3-byte window as raw bytes (sorted, deduped). <3 bytes => empty.
/// Windows pack big-endian into u32 so sort+dedup run over integers (u32
/// order == lexicographic byte order) and only distinct grams allocate.
pub fn trigram_grams_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut packed: Vec<u32> = data
        .windows(3)
        .map(|w| u32::from(w[0]) << 16 | u32::from(w[1]) << 8 | u32::from(w[2]))
        .collect();
    packed.sort_unstable();
    packed.dedup();
    packed
        .into_iter()
        .map(|g| vec![(g >> 16) as u8, (g >> 8) as u8, g as u8])
        .collect()
}

/// Stable u64 hash of an n-gram's bytes. Deterministic across runs/platforms
/// (used as the on-disk + in-memory gram key).
pub fn hash_ngram(gram: &[u8]) -> u64 {
    rapidhash::v3::rapidhash_v3(gram)
}

/// Deterministic weight of an adjacent byte pair. Drives sparse-ngram
/// boundary selection. Only affects selectivity, never correctness.
pub(crate) fn pair_weight(a: u8, b: u8) -> u32 {
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

#[cfg(test)]
mod invariant_grams {
    use super::{hash_ngram, sparse_grams_all_bytes, sparse_grams_covering_bytes};

    pub(super) fn trigrams(bytes: &[u8]) -> Vec<u32> {
        let mut v: Vec<u32> = bytes
            .windows(3)
            .map(|w| (w[0] as u32) << 16 | (w[1] as u32) << 8 | w[2] as u32)
            .collect();
        v.sort_unstable();
        v.dedup();
        v
    }

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

/// A source of documents for INDEX BUILDS, which need full enumeration.
/// Implemented by a local dir (tests) and S3 (prod).
pub trait Corpus {
    /// All document ids with their keys (object key / file path).
    fn docs(&self) -> &[(DocId, String)];
    /// Fetch the full bytes of one document.
    fn fetch(&self, id: DocId) -> AnyhowResult<Vec<u8>>;
    /// Fetch many docs concurrently. Result order is NOT guaranteed; each item
    /// carries its `DocId`. Implementations may return fewer docs than
    /// requested when a doc vanished between indexing and fetching.
    /// Default = sequential, fail-fast.
    fn fetch_many(&self, ids: &[DocId]) -> anyhow::Result<Vec<(DocId, Vec<u8>)>> {
        ids.iter().map(|&id| Ok((id, self.fetch(id)?))).collect()
    }
}

/// Fetches documents by key for SEARCH verification — no enumeration, no
/// doc table. `consume` receives the index into `keys` plus the body, as
/// fetches complete (order NOT guaranteed). Implementations may fetch
/// concurrently and may skip vanished docs; the first `consume` error
/// aborts the remaining fetches.
pub trait DocFetcher {
    fn fetch_each(
        &self,
        keys: &[String],
        consume: &mut dyn FnMut(usize, Vec<u8>) -> AnyhowResult<()>,
    ) -> AnyhowResult<()>;
}

pub trait BlobStore {
    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()>;
    /// `Ok(None)` = blob does not exist. Transient store failures are `Err`
    /// so callers never mistake an outage for an empty store.
    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>>;
    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>>;
    /// Fetch many byte ranges of one blob, preserving order. Implementations
    /// may fetch concurrently. Default = sequential.
    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> AnyhowResult<Vec<Vec<u8>>> {
        ranges
            .iter()
            .map(|&(start, len)| self.get_range(name, start, len))
            .collect()
    }
}

#[cfg(any(test, feature = "testutil"))]
pub mod testutil {
    use super::{Corpus, DocId};
    use anyhow::Result;

    pub struct MemCorpus {
        docs: Vec<(DocId, String)>,
        bodies: Vec<Vec<u8>>,
    }

    impl MemCorpus {
        pub fn new(docs: Vec<(DocId, String)>, bodies: Vec<Vec<u8>>) -> MemCorpus {
            assert_eq!(docs.len(), bodies.len());
            MemCorpus { docs, bodies }
        }
    }

    impl Corpus for MemCorpus {
        fn docs(&self) -> &[(DocId, String)] {
            &self.docs
        }

        fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
            Ok(self.bodies[id as usize].clone())
        }
    }

    impl crate::DocFetcher for MemCorpus {
        fn fetch_each(
            &self,
            keys: &[String],
            consume: &mut dyn FnMut(usize, Vec<u8>) -> Result<()>,
        ) -> Result<()> {
            for (idx, key) in keys.iter().enumerate() {
                let (id, _) = self
                    .docs
                    .iter()
                    .find(|(_, k)| k == key)
                    .ok_or_else(|| anyhow::anyhow!("unknown key {key}"))?;
                consume(idx, self.bodies[*id as usize].clone())?;
            }
            Ok(())
        }
    }
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
    fn put(&self, name: &str, bytes: &[u8]) -> AnyhowResult<()> {
        let path = self.root.join(name);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, bytes)?;
        Ok(())
    }

    fn get(&self, name: &str) -> AnyhowResult<Option<Vec<u8>>> {
        match std::fs::read(self.root.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> AnyhowResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};

        let mut file = std::fs::File::open(self.root.join(name))?;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; usize::try_from(len)?];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

/// One verified match: 1-based line, 1-based column (byte), the line text.
/// The owning object's key travels alongside, not inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub line: usize,
    pub col: usize,
    pub text: String,
}

/// Run `re` over `bytes`, returning one Match per matching occurrence.
/// Single pass: line numbers are tracked incrementally across matches.
pub fn matches_in(bytes: &[u8], re: &regex::bytes::Regex) -> Vec<Match> {
    let mut out = Vec::new();
    let mut line = 1usize;
    let mut line_start = 0usize;
    let mut counted_to = 0usize;
    for m in re.find_iter(bytes) {
        let start = m.start();
        let gap = &bytes[counted_to..start];
        line += bytecount::count(gap, b'\n');
        if let Some(p) = memchr::memrchr(b'\n', gap) {
            line_start = counted_to + p + 1;
        }
        counted_to = start;
        let line_end = memchr::memchr(b'\n', &bytes[start..]).map_or(bytes.len(), |p| start + p);
        out.push(Match {
            line,
            col: start - line_start + 1,
            text: String::from_utf8_lossy(&bytes[line_start..line_end]).into_owned(),
        });
    }
    out
}

/// Oracle: keys of docs containing at least one match, sorted. The
/// differential ground truth.
pub fn scan_matching_docs(
    corpus: &dyn Corpus,
    re: &regex::bytes::Regex,
) -> AnyhowResult<Vec<String>> {
    let mut hits = Vec::new();
    for (id, key) in corpus.docs() {
        let bytes = corpus.fetch(*id)?;
        if re.is_match(&bytes) {
            hits.push(key.clone());
        }
    }
    hits.sort_unstable();
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::invariant_grams::trigrams;
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
    fn local_blob_store_round_trips_ranges() -> AnyhowResult<()> {
        let root = std::env::temp_dir().join(format!(
            "holys3-core-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        let store = LocalBlobStore::new(&root);
        store.put("builds/a/postings.bin", b"abcdef")?;
        assert_eq!(
            store.get("builds/a/postings.bin")?.as_deref(),
            Some(b"abcdef".as_slice())
        );
        assert_eq!(store.get("missing")?, None);
        assert_eq!(store.get_range("builds/a/postings.bin", 2, 3)?, b"cde");
        assert_eq!(
            store.get_ranges("builds/a/postings.bin", &[(0, 2), (4, 2)])?,
            vec![b"ab".to_vec(), b"ef".to_vec()]
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}

#[cfg(test)]
mod sparse_tests {
    use super::invariant_grams::{extract_sparse_ngrams_all, extract_sparse_ngrams_covering};
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
    use crate::testutil::MemCorpus;

    #[test]
    fn scan_finds_matching_docs() {
        let c = MemCorpus::new(
            vec![(0, "a".into()), (1, "b".into())],
            vec![b"hello world".to_vec(), b"nothing here".to_vec()],
        );
        let re = regex::bytes::Regex::new("world").unwrap();
        assert_eq!(scan_matching_docs(&c, &re).unwrap(), vec!["a".to_owned()]);
    }

    #[test]
    fn match_line_col() {
        let m = matches_in(b"foo\nbar baz", &regex::bytes::Regex::new("baz").unwrap());
        assert_eq!(
            m,
            vec![Match {
                line: 2,
                col: 5,
                text: "bar baz".into()
            }]
        );
    }

    #[test]
    fn matches_in_tracks_lines_across_matches() {
        let bytes = b"alpha x\nbeta\nx gamma x\nx";
        let re = regex::bytes::Regex::new("x").unwrap();
        let m = matches_in(bytes, &re);
        let positions: Vec<(usize, usize, &str)> =
            m.iter().map(|m| (m.line, m.col, m.text.as_str())).collect();
        assert_eq!(
            positions,
            vec![
                (1, 7, "alpha x"),
                (3, 1, "x gamma x"),
                (3, 9, "x gamma x"),
                (4, 1, "x"),
            ]
        );
    }

    #[test]
    fn decode_body_handles_raw_gzip_multimember_and_zstd() {
        use std::io::Write;

        assert_eq!(
            decode_body("k", b"plain text".to_vec()).unwrap(),
            b"plain text"
        );

        let gz = |data: &[u8]| {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(data).unwrap();
            enc.finish().unwrap()
        };
        let mut multi = gz(b"first member\n");
        multi.extend(gz(b"second member\n"));
        assert_eq!(
            decode_body("k.gz", multi).unwrap(),
            b"first member\nsecond member\n"
        );

        let zst = zstd::stream::encode_all(&b"zstd body"[..], 0).unwrap();
        assert_eq!(decode_body("k.zst", zst).unwrap(), b"zstd body");

        let truncated = gz(b"data")[..6].to_vec();
        let err = decode_body("bad.gz", truncated).unwrap_err();
        assert!(err.to_string().contains("bad.gz"));
    }

    #[test]
    fn doc_fetcher_resolves_keys() {
        use crate::testutil::MemCorpus;
        use crate::DocFetcher;
        let c = MemCorpus::new(
            vec![(0, "a".into()), (1, "b".into())],
            vec![b"one".to_vec(), b"two".to_vec()],
        );
        let keys = vec!["b".to_owned(), "a".to_owned()];
        let mut seen = Vec::new();
        c.fetch_each(&keys, &mut |idx, bytes| {
            seen.push((idx, bytes));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, vec![(0, b"two".to_vec()), (1, b"one".to_vec())]);
    }

    #[test]
    fn fetch_many_aborts_on_first_error() {
        struct BrokenCorpus {
            docs: Vec<(DocId, String)>,
        }

        impl Corpus for BrokenCorpus {
            fn docs(&self) -> &[(DocId, String)] {
                &self.docs
            }

            fn fetch(&self, id: DocId) -> AnyhowResult<Vec<u8>> {
                if id == 1 {
                    anyhow::bail!("broken");
                }
                Ok(b"ok".to_vec())
            }
        }

        let corpus = BrokenCorpus {
            docs: vec![(0, "a".into()), (1, "b".into())],
        };
        assert!(corpus.fetch_many(&[0, 1]).is_err());
    }
}
