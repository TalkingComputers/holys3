mod common;

use common::{corpus, encoded_corpus, gzipped_corpus, PATTERNS};
use holys3_core::{scan_matching_docs, Corpus};
use holys3_index::{build_to_dir, search_collect, MmapIndexReader};

#[test]
fn index_equals_scan_for_many_patterns() {
    for (label, c) in [
        ("plain", corpus()),
        ("gzipped", gzipped_corpus()),
        ("encoded", encoded_corpus()),
    ] {
        for strategy in [
            holys3_core::Strategy::Trigram,
            holys3_core::Strategy::Sparse,
        ] {
            eprintln!("differential corpus={label} strategy={strategy:?}");
            let dir = tempfile::tempdir().unwrap();
            build_to_dir(&c, dir.path(), strategy).unwrap();
            let reader = MmapIndexReader::open(dir.path()).unwrap();
            for p in PATTERNS {
                let indexed: Vec<String> = search_collect(&reader, &c, p).unwrap().1.hits;
                let re = regex::bytes::Regex::new(p).unwrap();
                let oracle = scan_decoded(&c, &re);
                assert_eq!(
                    indexed, oracle,
                    "corpus {label} strategy {strategy:?} pattern `{p}`: index != scan"
                );
            }
        }
    }
}

/// Oracle over decompressed bodies — searches must behave as if every object
/// were plain text.
fn scan_decoded(c: &dyn Corpus, re: &regex::bytes::Regex) -> Vec<String> {
    let decoded = common::decoded_corpus(c);
    scan_matching_docs(&decoded, re).unwrap()
}
