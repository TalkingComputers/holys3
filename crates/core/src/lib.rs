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
