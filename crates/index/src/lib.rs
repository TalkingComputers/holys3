use anyhow::{Context, Result};
use holys3_core::{grams_index, hash_ngram, BlobStore, Corpus, DocId, Strategy};
use holys3_query::Query;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
struct Manifest {
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
}

#[derive(Serialize, Deserialize)]
struct Footer {
    strategy: Strategy,
    doc_count: u32,
    terms_fst_len: u64,
    postings_len: u64,
    build_id: String,
}

fn build_index_bytes(
    corpus: &dyn Corpus,
    strategy: Strategy,
) -> Result<(Vec<u8>, Vec<u8>, Manifest)> {
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
    let manifest = Manifest {
        docs: corpus.docs().to_vec(),
        strategy,
    };
    Ok((fst_bytes, postings_buf, manifest))
}

/// Write terms.fst + postings.bin + manifest.bin into `dir`.
pub fn build_to_dir(corpus: &dyn Corpus, dir: &Path, strategy: Strategy) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let (fst_bytes, postings_buf, manifest) = build_index_bytes(corpus, strategy)?;
    std::fs::write(dir.join("terms.fst"), &fst_bytes)?;
    std::fs::write(dir.join("postings.bin"), &postings_buf)?;
    std::fs::write(dir.join("manifest.bin"), postcard::to_allocvec(&manifest)?)?;
    Ok(())
}

pub fn compute_build_id(objects: &[(String, String)]) -> String {
    let mut objects = objects.to_vec();
    objects.sort();
    let mut bytes = Vec::new();
    for (key, etag) in objects {
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(etag.as_bytes());
        bytes.push(b'\n');
    }
    format!("{:016x}", hash_ngram(&bytes))
}

pub fn build_to_store(
    corpus: &dyn Corpus,
    store: &dyn BlobStore,
    strategy: Strategy,
    build_id: &str,
) -> Result<()> {
    let (fst_bytes, postings_buf, manifest) = build_index_bytes(corpus, strategy)?;
    let base = format!("builds/{build_id}");
    let footer = Footer {
        strategy,
        doc_count: u32::try_from(manifest.docs.len())?,
        terms_fst_len: u64::try_from(fst_bytes.len())?,
        postings_len: u64::try_from(postings_buf.len())?,
        build_id: build_id.to_string(),
    };
    store.put(&format!("{base}/terms.fst"), &fst_bytes)?;
    store.put(&format!("{base}/postings.bin"), &postings_buf)?;
    store.put(
        &format!("{base}/manifest.bin"),
        &postcard::to_allocvec(&manifest)?,
    )?;
    store.put(
        &format!("{base}/footer.bin"),
        &postcard::to_allocvec(&footer)?,
    )?;
    store.put("CURRENT", build_id.as_bytes())?;
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

pub struct StoreIndexReader {
    map: fst::Map<memmap2::Mmap>,
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
    store_postings_name: String,
    store: Box<dyn BlobStore>,
    terms_fst_len: u64,
    postings_len: u64,
}

impl StoreIndexReader {
    pub fn open(store: Box<dyn BlobStore>, cache_dir: &Path) -> Result<StoreIndexReader> {
        let build_id = String::from_utf8(store.get("CURRENT")?)?;
        let build_cache_dir = cache_dir.join(&build_id);
        std::fs::create_dir_all(&build_cache_dir)?;
        let footer_name = format!("builds/{build_id}/footer.bin");
        let footer_bytes = read_cached_blob(
            store.as_ref(),
            &build_cache_dir.join("footer.bin"),
            &footer_name,
        )?;
        let footer: Footer = postcard::from_bytes(&footer_bytes)?;
        anyhow::ensure!(
            footer.build_id == build_id,
            "footer build_id {} does not match CURRENT {}",
            footer.build_id,
            build_id
        );
        let terms_name = format!("builds/{build_id}/terms.fst");
        let terms_path = build_cache_dir.join("terms.fst");
        let terms_bytes = read_cached_blob(store.as_ref(), &terms_path, &terms_name)?;
        anyhow::ensure!(
            footer.terms_fst_len == u64::try_from(terms_bytes.len())?,
            "terms.fst length mismatch"
        );
        let fst_file = std::fs::File::open(&terms_path)?;
        let map = fst::Map::new(unsafe { memmap2::Mmap::map(&fst_file)? })?;
        let manifest_name = format!("builds/{build_id}/manifest.bin");
        let manifest_bytes = read_cached_blob(
            store.as_ref(),
            &build_cache_dir.join("manifest.bin"),
            &manifest_name,
        )?;
        let manifest: Manifest = postcard::from_bytes(&manifest_bytes)?;
        anyhow::ensure!(
            footer.strategy == manifest.strategy,
            "footer strategy does not match manifest strategy"
        );
        anyhow::ensure!(
            footer.doc_count == u32::try_from(manifest.docs.len())?,
            "footer doc_count does not match manifest docs"
        );
        Ok(StoreIndexReader {
            map,
            docs: manifest.docs,
            strategy: footer.strategy,
            store_postings_name: format!("builds/{build_id}/postings.bin"),
            store,
            terms_fst_len: footer.terms_fst_len,
            postings_len: footer.postings_len,
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

    fn read_block(&self, offset: u64) -> Result<BTreeSet<DocId>> {
        let count_bytes = self.store.get_range(&self.store_postings_name, offset, 4)?;
        let count = u32::from_le_bytes(count_bytes.as_slice().try_into()?);
        if count == 0 {
            return Ok(BTreeSet::new());
        }
        let bytes_len = u64::from(count)
            .checked_mul(4)
            .context("postings block byte length overflow")?;
        let ids_bytes = self
            .store
            .get_range(&self.store_postings_name, offset + 4, bytes_len)?;
        let mut set = BTreeSet::new();
        for chunk in ids_bytes.chunks_exact(4) {
            set.insert(u32::from_le_bytes(chunk.try_into()?));
        }
        Ok(set)
    }

    pub fn candidates(&self, q: &Query) -> Result<BTreeSet<DocId>> {
        match q {
            Query::All => Ok(self.all_docs()),
            Query::None => Ok(BTreeSet::new()),
            Query::Gram(g) => match self.map.get(g) {
                Some(off) => self.read_block(off),
                None => Ok(BTreeSet::new()),
            },
            Query::And(subs) => {
                let mut it = subs.iter();
                let Some(first) = it.next() else {
                    return Ok(self.all_docs());
                };
                let mut out = self.candidates(first)?;
                for sub in it {
                    let set = self.candidates(sub)?;
                    out = out.intersection(&set).copied().collect();
                }
                Ok(out)
            }
            Query::Or(subs) => {
                let mut out = BTreeSet::new();
                for sub in subs {
                    out.extend(self.candidates(sub)?);
                }
                Ok(out)
            }
        }
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            distinct_grams: self.map.len(),
            terms_fst_bytes: self.terms_fst_len,
            postings_bytes: self.postings_len,
        }
    }
}

fn read_cached_blob(store: &dyn BlobStore, cache_path: &Path, store_name: &str) -> Result<Vec<u8>> {
    if cache_path.exists() {
        return Ok(std::fs::read(cache_path)?);
    }
    let bytes = store.get(store_name)?;
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(cache_path, &bytes)?;
    Ok(bytes)
}

#[derive(Debug)]
pub struct Stats {
    pub distinct_grams: usize,
    pub terms_fst_bytes: usize,
    pub postings_bytes: usize,
}

#[derive(Debug)]
pub struct StoreStats {
    pub distinct_grams: usize,
    pub terms_fst_bytes: u64,
    pub postings_bytes: u64,
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

pub fn search_via_store(
    reader: &StoreIndexReader,
    corpus: &dyn Corpus,
    pattern: &str,
) -> Result<BTreeSet<DocId>> {
    let q = holys3_query::plan(pattern, reader.strategy())?;
    let re = regex::bytes::Regex::new(pattern)?;
    let mut hits = BTreeSet::new();
    for id in reader.candidates(&q)? {
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
