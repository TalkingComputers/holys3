use anyhow::Result;
use holys3_core::{grams_index, Corpus, DocId, Strategy};
use holys3_query::Query;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Manifest {
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
}

/// Write terms.fst + postings.bin + manifest.bin into `dir`.
pub fn build_to_dir(corpus: &dyn Corpus, dir: &Path, strategy: Strategy) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut postings: BTreeMap<Vec<u8>, Vec<DocId>> = BTreeMap::new();
    for &(id, _) in corpus.docs() {
        let bytes = corpus.fetch(id)?;
        for gram in grams_index(&bytes, strategy) {
            postings.entry(gram).or_default().push(id);
        }
    }
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut builder = fst::MapBuilder::new(Vec::new())?;
    for (gram, ids) in &postings {
        let mut ids = ids.clone();
        ids.sort_unstable();
        ids.dedup();
        let offset = postings_buf.len() as u64;
        postings_buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for id in &ids {
            postings_buf.extend_from_slice(&id.to_le_bytes());
        }
        builder.insert(gram, offset)?;
    }
    let fst_bytes = builder.into_inner()?;
    std::fs::write(dir.join("terms.fst"), &fst_bytes)?;
    std::fs::write(dir.join("postings.bin"), &postings_buf)?;
    let manifest = Manifest {
        docs: corpus.docs().to_vec(),
        strategy,
    };
    std::fs::write(dir.join("manifest.bin"), postcard::to_allocvec(&manifest)?)?;
    Ok(())
}

pub struct IndexReader {
    map: fst::Map<memmap2::Mmap>,
    postings: memmap2::Mmap,
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
}

impl IndexReader {
    pub fn open(dir: &Path) -> Result<IndexReader> {
        let fst_file = std::fs::File::open(dir.join("terms.fst"))?;
        let map = fst::Map::new(unsafe {
            // Build dirs are immutable while readers are open.
            memmap2::Mmap::map(&fst_file)?
        })?;
        let post_file = std::fs::File::open(dir.join("postings.bin"))?;
        let postings = unsafe {
            // Build dirs are immutable while readers are open.
            memmap2::Mmap::map(&post_file)?
        };
        let manifest: Manifest = postcard::from_bytes(&std::fs::read(dir.join("manifest.bin"))?)?;
        Ok(IndexReader {
            map,
            postings,
            docs: manifest.docs,
            strategy: manifest.strategy,
        })
    }

    pub fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    pub fn strategy(&self) -> Strategy {
        self.strategy
    }

    fn all_docs(&self) -> BTreeSet<DocId> {
        self.docs.iter().map(|&(id, _)| id).collect()
    }

    fn read_block(&self, offset: u64) -> BTreeSet<DocId> {
        let o = offset as usize;
        let count = u32::from_le_bytes(self.postings[o..o + 4].try_into().unwrap()) as usize;
        let mut set = BTreeSet::new();
        let base = o + 4;
        for k in 0..count {
            let p = base + k * 4;
            set.insert(u32::from_le_bytes(
                self.postings[p..p + 4].try_into().unwrap(),
            ));
        }
        set
    }

    pub fn candidates(&self, q: &Query) -> BTreeSet<DocId> {
        match q {
            Query::All => self.all_docs(),
            Query::None => BTreeSet::new(),
            Query::Gram(g) => match self.map.get(g) {
                Some(off) => self.read_block(off),
                None => BTreeSet::new(),
            },
            Query::And(subs) => {
                let mut it = subs.iter().map(|s| self.candidates(s));
                match it.next() {
                    None => self.all_docs(),
                    Some(first) => it.fold(first, |a, s| a.intersection(&s).copied().collect()),
                }
            }
            Query::Or(subs) => subs.iter().flat_map(|s| self.candidates(s)).collect(),
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            distinct_grams: self.map.len(),
            terms_fst_bytes: self.map.as_fst().as_bytes().len(),
            postings_bytes: self.postings.len(),
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub distinct_grams: usize,
    pub terms_fst_bytes: usize,
    pub postings_bytes: usize,
}

pub struct LocalCorpus {
    docs: Vec<(DocId, String)>,
    paths: Vec<PathBuf>,
}

impl LocalCorpus {
    pub fn new(root: &Path) -> Result<LocalCorpus> {
        let mut paths = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(p) = stack.pop() {
            for entry in std::fs::read_dir(&p)? {
                let path = entry?.path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    paths.push(path);
                }
            }
        }
        paths.sort();
        let docs = paths
            .iter()
            .enumerate()
            .map(|(i, p)| (i as DocId, p.to_string_lossy().into_owned()))
            .collect();
        Ok(LocalCorpus { docs, paths })
    }
}

impl Corpus for LocalCorpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn fetch(&self, id: DocId) -> Result<Vec<u8>> {
        Ok(std::fs::read(&self.paths[id as usize])?)
    }
}

/// Full indexed search via the on-disk reader: plan -> candidates -> verify.
pub fn search_matching_docs(
    reader: &IndexReader,
    corpus: &dyn Corpus,
    pattern: &str,
) -> Result<BTreeSet<DocId>> {
    let q = holys3_query::plan(pattern, reader.strategy())?;
    let re = regex::bytes::Regex::new(pattern)?;
    let mut hits = BTreeSet::new();
    for id in reader.candidates(&q) {
        if re.is_match(&corpus.fetch(id)?) {
            hits.insert(id);
        }
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
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

    fn build_tmp(c: &MemCorpus, strategy: Strategy) -> (tempfile::TempDir, IndexReader) {
        let dir = tempfile::tempdir().unwrap();
        build_to_dir(c, dir.path(), strategy).unwrap();
        let r = IndexReader::open(dir.path()).unwrap();
        (dir, r)
    }

    #[test]
    fn candidate_superset_then_verify() {
        let c = MemCorpus(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"world".to_vec(), b"word".to_vec()],
        );
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_d, r) = build_tmp(&c, strategy);
            let cands = r.candidates(&holys3_query::plan("world", r.strategy()).unwrap());
            assert!(cands.contains(&0));
            assert!(cands.is_subset(&BTreeSet::from([0, 1])));
        }
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus(vec![(0, "x".into())], vec![b"abcdef".to_vec()]);
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_d, r) = build_tmp(&c, strategy);
            assert_eq!(r.candidates(&Query::All), BTreeSet::from([0]));
        }
    }
}
