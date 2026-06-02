use holys3_core::{trigrams, Corpus, DocId};
use holys3_query::Query;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Serialize, Deserialize)]
pub struct Index {
    /// doc id -> key
    pub docs: Vec<(DocId, String)>,
    /// trigram -> sorted doc ids
    postings: BTreeMap<u32, Vec<DocId>>,
}

impl Index {
    /// Build by fetching and trigram-izing every doc in the corpus.
    pub fn build(corpus: &dyn Corpus) -> anyhow::Result<Index> {
        let mut idx = Index {
            docs: corpus.docs().to_vec(),
            postings: BTreeMap::new(),
        };
        for &(id, _) in corpus.docs() {
            let bytes = corpus.fetch(id)?;
            for t in trigrams(&bytes) {
                idx.postings.entry(t).or_default().push(id);
            }
        }
        for v in idx.postings.values_mut() {
            v.sort_unstable();
            v.dedup();
        }
        Ok(idx)
    }

    fn all_docs(&self) -> BTreeSet<DocId> {
        self.docs.iter().map(|&(id, _)| id).collect()
    }

    /// Candidate doc ids that satisfy the trigram query (superset of true matches).
    pub fn candidates(&self, q: &Query) -> BTreeSet<DocId> {
        match q {
            Query::All => self.all_docs(),
            Query::None => BTreeSet::new(),
            Query::Trigram(t) => self
                .postings
                .get(t)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
            Query::And(subs) => {
                let mut iter = subs.iter().map(|s| self.candidates(s));
                match iter.next() {
                    None => self.all_docs(),
                    Some(first) => {
                        iter.fold(first, |acc, s| acc.intersection(&s).copied().collect())
                    }
                }
            }
            Query::Or(subs) => subs.iter().flat_map(|s| self.candidates(s)).collect(),
        }
    }

    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        std::fs::write(path, postcard::to_allocvec(self)?)?;
        Ok(())
    }

    pub fn load(path: &std::path::Path) -> anyhow::Result<Index> {
        Ok(postcard::from_bytes(&std::fs::read(path)?)?)
    }

    /// Measurement for the §5 A/B decision.
    pub fn stats(&self) -> Stats {
        let entry_bytes = 4 + 8 + 4; // hash u32 + offset u64 + len u32 (sorted-table model)
        Stats {
            distinct_trigrams: self.postings.len(),
            termdict_bytes_estimate: self.postings.len() * entry_bytes,
            total_postings: self.postings.values().map(|v| v.len()).sum(),
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub distinct_trigrams: usize,
    pub termdict_bytes_estimate: usize,
    pub total_postings: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    struct MemCorpus(Vec<(DocId, String)>, Vec<Vec<u8>>);
    impl Corpus for MemCorpus {
        fn docs(&self) -> &[(DocId, String)] {
            &self.0
        }
        fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
            Ok(self.1[id as usize].clone())
        }
    }

    fn tg(s: &str) -> u32 {
        let b = s.as_bytes();
        (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32
    }

    #[test]
    fn candidates_intersect() {
        let c = MemCorpus(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"world".to_vec(), b"word".to_vec()],
        );
        let idx = Index::build(&c).unwrap();
        // "rld" only in doc 0
        let q = Query::And(vec![Query::Trigram(tg("rld"))]);
        assert_eq!(idx.candidates(&q), BTreeSet::from([0]));
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus(vec![(0, "x".into())], vec![b"abc".to_vec()]);
        let idx = Index::build(&c).unwrap();
        assert_eq!(idx.candidates(&Query::All), BTreeSet::from([0]));
    }

    #[test]
    fn save_load_roundtrip() {
        let c = MemCorpus(vec![(0, "x".into())], vec![b"abcdef".to_vec()]);
        let idx = Index::build(&c).unwrap();
        let tmp = std::env::temp_dir().join("holys3_idx_test.bin");
        idx.save(&tmp).unwrap();
        let loaded = Index::load(&tmp).unwrap();
        assert_eq!(
            loaded.candidates(&Query::Trigram(tg("abc"))),
            BTreeSet::from([0])
        );
    }
}
