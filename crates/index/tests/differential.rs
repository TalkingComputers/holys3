mod common;

use common::{corpus, PATTERNS};
use holys3_core::{scan_matching_docs, DocId};
use holys3_index::{build_to_dir, search, MmapIndexReader};
use std::collections::BTreeSet;

#[test]
fn index_equals_scan_for_many_patterns() {
    let c = corpus();
    for strategy in [
        holys3_core::Strategy::Trigram,
        holys3_core::Strategy::Sparse,
    ] {
        eprintln!("differential strategy={strategy:?}");
        let dir = tempfile::tempdir().unwrap();
        build_to_dir(&c, dir.path(), strategy).unwrap();
        let reader = MmapIndexReader::open(dir.path()).unwrap();
        for p in PATTERNS {
            let indexed: BTreeSet<DocId> = search(&reader, &c, p).unwrap();
            let re = regex::bytes::Regex::new(p).unwrap();
            let oracle = scan_matching_docs(&c, &re).unwrap();
            assert_eq!(
                indexed, oracle,
                "strategy {strategy:?} pattern `{p}`: index != scan"
            );
        }
    }
}
