#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Index construction and local or store-backed index readers.

use anyhow::{Context, Result};
use holys3_core::{grams_index, hash_ngram, BlobStore, Corpus, DocId, Strategy};
use holys3_query::Query;
use rayon::prelude::*;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStats {
    pub distinct_grams: u64,
    pub terms_fst_bytes: u64,
    pub postings_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchStats {
    pub hits: BTreeSet<DocId>,
    pub candidates: usize,
    pub total_docs: usize,
    pub bytes_fetched: usize,
}

pub trait IndexReader {
    fn docs(&self) -> &[(DocId, String)];
    fn strategy(&self) -> Strategy;
    fn candidates(&self, q: &Query) -> Result<BTreeSet<DocId>>;
    fn stats(&self) -> IndexStats;
}

#[doc(hidden)]
pub fn eval_query(
    q: &Query,
    all_docs: &BTreeSet<DocId>,
    offset: &dyn Fn(&[u8]) -> Option<u64>,
    read_block: &dyn Fn(u64) -> Result<BTreeSet<DocId>>,
) -> Result<BTreeSet<DocId>> {
    match q {
        Query::All => Ok(all_docs.clone()),
        Query::None => Ok(BTreeSet::new()),
        Query::Gram(g) => match offset(g) {
            Some(off) => read_block(off),
            None => Ok(BTreeSet::new()),
        },
        Query::And(subs) => {
            let mut it = subs.iter();
            let Some(first) = it.next() else {
                return Ok(all_docs.clone());
            };
            let mut out = eval_query(first, all_docs, offset, read_block)?;
            for sub in it {
                let set = eval_query(sub, all_docs, offset, read_block)?;
                out = out.intersection(&set).copied().collect();
            }
            Ok(out)
        }
        Query::Or(subs) => {
            let mut out = BTreeSet::new();
            for sub in subs {
                out.extend(eval_query(sub, all_docs, offset, read_block)?);
            }
            Ok(out)
        }
    }
}

#[doc(hidden)]
pub fn decode_postings_block(postings: &[u8], offset: u64) -> Result<BTreeSet<DocId>> {
    let o = usize::try_from(offset).context("postings offset does not fit usize")?;
    let count_end = o.checked_add(4).context("postings count offset overflow")?;
    let count_bytes: [u8; 4] = postings
        .get(o..count_end)
        .context("truncated postings.bin count")?
        .try_into()?;
    let count = u32::from_le_bytes(count_bytes) as usize;
    let ids_len = count
        .checked_mul(4)
        .context("postings block byte length overflow")?;
    let ids_end = count_end
        .checked_add(ids_len)
        .context("postings block end overflow")?;
    let ids_bytes = postings
        .get(count_end..ids_end)
        .context("truncated postings.bin ids")?;
    let mut set = BTreeSet::new();
    for chunk in ids_bytes.chunks_exact(4) {
        set.insert(u32::from_le_bytes(chunk.try_into()?));
    }
    Ok(set)
}

fn build_index_bytes(
    corpus: &dyn Corpus,
    strategy: Strategy,
) -> Result<(Vec<u8>, Vec<u8>, Manifest)> {
    let mut postings: BTreeMap<Vec<u8>, Vec<DocId>> = BTreeMap::new();
    let ids = corpus.docs().iter().map(|&(id, _)| id).collect::<Vec<_>>();
    let docs = corpus
        .fetch_many(&ids)?
        .into_par_iter()
        .map(|(id, bytes)| Ok((id, grams_index(&bytes?, strategy))))
        .collect::<Result<Vec<_>>>()?;
    for (id, grams) in docs {
        for gram in grams {
            postings.entry(gram).or_default().push(id);
        }
    }
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut builder = fst::MapBuilder::new(Vec::new())?;
    for (gram, mut ids) in postings {
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
        build_id: build_id.to_owned(),
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

pub struct MmapIndexReader {
    map: fst::Map<memmap2::Mmap>,
    postings: memmap2::Mmap,
    docs: Vec<(DocId, String)>,
    strategy: Strategy,
}

impl MmapIndexReader {
    pub fn open(dir: &Path) -> Result<MmapIndexReader> {
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
        Ok(MmapIndexReader {
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

    fn read_block(&self, offset: u64) -> Result<BTreeSet<DocId>> {
        decode_postings_block(&self.postings, offset)
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            distinct_grams: self.map.len() as u64,
            terms_fst_bytes: self.map.as_fst().as_bytes().len() as u64,
            postings_bytes: self.postings.len() as u64,
        }
    }
}

impl IndexReader for MmapIndexReader {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn strategy(&self) -> Strategy {
        self.strategy
    }

    fn candidates(&self, q: &Query) -> Result<BTreeSet<DocId>> {
        let all_docs = self.all_docs();
        eval_query(q, &all_docs, &|gram| self.map.get(gram), &|offset| {
            self.read_block(offset)
        })
    }

    fn stats(&self) -> IndexStats {
        MmapIndexReader::stats(self)
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
        let ids_len = u64::from(count)
            .checked_mul(4)
            .context("postings block byte length overflow")?;
        let block_len = 4u64
            .checked_add(ids_len)
            .context("postings block byte length overflow")?;
        let block = self
            .store
            .get_range(&self.store_postings_name, offset, block_len)?;
        decode_postings_block(&block, 0)
    }
    pub fn stats(&self) -> IndexStats {
        IndexStats {
            distinct_grams: self.map.len() as u64,
            terms_fst_bytes: self.terms_fst_len,
            postings_bytes: self.postings_len,
        }
    }
}

impl IndexReader for StoreIndexReader {
    fn docs(&self) -> &[(DocId, String)] {
        &self.docs
    }

    fn strategy(&self) -> Strategy {
        self.strategy
    }

    fn candidates(&self, q: &Query) -> Result<BTreeSet<DocId>> {
        let all_docs = self.all_docs();
        eval_query(q, &all_docs, &|gram| self.map.get(gram), &|offset| {
            self.read_block(offset)
        })
    }

    fn stats(&self) -> IndexStats {
        StoreIndexReader::stats(self)
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

pub fn search(
    reader: &dyn IndexReader,
    corpus: &dyn Corpus,
    pattern: &str,
) -> Result<BTreeSet<DocId>> {
    Ok(search_with_stats(reader, corpus, pattern)?.hits)
}

pub fn search_with_stats(
    reader: &dyn IndexReader,
    corpus: &dyn Corpus,
    pattern: &str,
) -> Result<SearchStats> {
    let q = holys3_query::plan(pattern, reader.strategy())?;
    let re = regex::bytes::Regex::new(pattern)?;
    let ids = reader.candidates(&q)?.into_iter().collect::<Vec<_>>();
    let mut hits = BTreeSet::new();
    let mut bytes_fetched = 0usize;
    for (id, bytes) in corpus.fetch_many(&ids)? {
        let bytes = bytes?;
        bytes_fetched = bytes_fetched
            .checked_add(bytes.len())
            .context("bytes fetched overflow")?;
        if re.is_match(&bytes) {
            hits.insert(id);
        }
    }
    Ok(SearchStats {
        hits,
        candidates: ids.len(),
        total_docs: reader.docs().len(),
        bytes_fetched,
    })
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

    fn build_tmp(c: &MemCorpus, strategy: Strategy) -> (tempfile::TempDir, MmapIndexReader) {
        let dir = tempfile::tempdir().unwrap();
        build_to_dir(c, dir.path(), strategy).unwrap();
        let r = MmapIndexReader::open(dir.path()).unwrap();
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
            let cands = r
                .candidates(&holys3_query::plan("world", r.strategy()).unwrap())
                .unwrap();
            assert!(cands.contains(&0));
            assert!(cands.is_subset(&BTreeSet::from([0, 1])));
        }
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus(vec![(0, "x".into())], vec![b"abcdef".to_vec()]);
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            let (_d, r) = build_tmp(&c, strategy);
            assert_eq!(r.candidates(&Query::All).unwrap(), BTreeSet::from([0]));
        }
    }

    #[test]
    fn search_stats_counts_candidates_and_bytes() {
        let c = MemCorpus(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"abc world".to_vec(), b"nomatch".to_vec()],
        );
        let (_d, r) = build_tmp(&c, Strategy::Trigram);
        let stats = search_with_stats(&r, &c, "world").unwrap();
        assert_eq!(stats.hits, BTreeSet::from([0]));
        assert_eq!(stats.candidates, 1);
        assert_eq!(stats.total_docs, 2);
        assert_eq!(stats.bytes_fetched, b"abc world".len());
    }
}
