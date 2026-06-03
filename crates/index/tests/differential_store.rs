use holys3_core::{scan_matching_docs, Corpus, DocId, LocalBlobStore, Strategy};
use holys3_index::{build_to_store, compute_build_id, search, StoreIndexReader};
use std::collections::BTreeSet;

struct MemCorpus(Vec<(DocId, String)>, Vec<Vec<u8>>);

impl Corpus for MemCorpus {
    fn docs(&self) -> &[(DocId, String)] {
        &self.0
    }

    fn fetch(&self, id: DocId) -> anyhow::Result<Vec<u8>> {
        Ok(self.1[id as usize].clone())
    }
}

fn corpus() -> MemCorpus {
    let bodies: Vec<&[u8]> = vec![
        b"fn handleClick() { return 42; }",
        b"the quick brown fox",
        b"hello world\nsecond line with world",
        b"nothing interesting",
        b"EMAIL: a@b.com and c@d.org",
        b"",
        b"\xff\xfe binary-ish \x00 bytes world",
    ];
    let docs = (0..bodies.len())
        .map(|i| (i as DocId, format!("doc{i}")))
        .collect();
    MemCorpus(docs, bodies.into_iter().map(|b| b.to_vec()).collect())
}

#[test]
fn store_index_equals_scan_for_many_patterns() -> anyhow::Result<()> {
    let c = corpus();
    let patterns = [
        "world",
        "handleClick",
        "quick.*fox",
        "EMAIL",
        r"\w+@\w+",
        ".*",
        "zzzznotpresent",
        "ab",
        "second line",
    ];
    for strategy in [Strategy::Trigram, Strategy::Sparse] {
        eprintln!("differential_store strategy={strategy:?}");
        let store_dir = tempfile::tempdir()?;
        let cache_dir = tempfile::tempdir()?;
        let store = LocalBlobStore::new(store_dir.path());
        let objects = c
            .docs()
            .iter()
            .map(|(_, key)| (key.clone(), format!("etag-{key}")))
            .collect::<Vec<_>>();
        let build_id = compute_build_id(&objects);
        build_to_store(&c, &store, strategy, &build_id)?;
        let reader = StoreIndexReader::open(
            Box::new(LocalBlobStore::new(store_dir.path())),
            cache_dir.path(),
        )?;
        for p in patterns {
            let indexed: BTreeSet<DocId> = search(&reader, &c, p)?;
            let re = regex::bytes::Regex::new(p)?;
            let oracle = scan_matching_docs(&c, &re)?;
            assert_eq!(
                indexed, oracle,
                "strategy {strategy:?} pattern `{p}`: store index != scan"
            );
        }
    }
    Ok(())
}
