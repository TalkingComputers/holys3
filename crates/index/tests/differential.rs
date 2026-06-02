use holys3_core::{scan_matching_docs, Corpus, DocId};
use holys3_index::{build_to_dir, search_matching_docs, IndexReader};
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
fn index_equals_scan_for_many_patterns() {
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
    for strategy in [
        holys3_core::Strategy::Trigram,
        holys3_core::Strategy::Sparse,
    ] {
        let dir = tempfile::tempdir().unwrap();
        build_to_dir(&c, dir.path(), strategy).unwrap();
        let reader = IndexReader::open(dir.path()).unwrap();
        for p in patterns {
            let indexed: BTreeSet<DocId> = search_matching_docs(&reader, &c, p).unwrap();
            let re = regex::bytes::Regex::new(p).unwrap();
            let oracle = scan_matching_docs(&c, &re).unwrap();
            assert_eq!(
                indexed, oracle,
                "strategy {strategy:?} pattern `{p}`: index != scan"
            );
        }
    }
}
