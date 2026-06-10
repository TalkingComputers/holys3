mod common;

use common::{corpus, decoded_corpus, gzipped_corpus, PATTERNS};
use holys3_core::{scan_matching_docs, Corpus, LocalBlobStore, Strategy};
use holys3_index::{
    build_to_store, compute_build_id, search_collect, search_streaming, KeyScope, NullSink,
    StoreIndexReader,
};

#[test]
fn store_index_equals_scan_for_many_patterns() -> anyhow::Result<()> {
    for (label, c) in [("plain", corpus()), ("gzipped", gzipped_corpus())] {
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            eprintln!("differential_store corpus={label} strategy={strategy:?}");
            let store_dir = tempfile::tempdir()?;
            let cache_dir = tempfile::tempdir()?;
            let store = LocalBlobStore::new(store_dir.path());
            let objects = c
                .docs()
                .iter()
                .map(|(_, key)| (key.clone(), format!("etag-{key}")))
                .collect::<Vec<_>>();
            let build_id = compute_build_id(&objects, strategy);
            build_to_store(&c, &store, strategy, &build_id)?;
            let reader = StoreIndexReader::open(
                Box::new(LocalBlobStore::new(store_dir.path())),
                cache_dir.path(),
            )?;
            let decoded = decoded_corpus(&c);
            for p in PATTERNS {
                let indexed: Vec<String> = search_collect(&reader, &c, p)?.1.hits;
                let re = regex::bytes::Regex::new(p)?;
                let oracle = scan_matching_docs(&decoded, &re)?;
                assert_eq!(
                    indexed, oracle,
                    "corpus {label} strategy {strategy:?} pattern `{p}`: store index != scan"
                );
                let fast = search_streaming(&reader, &c, p, KeyScope::default(), &NullSink)?.hits;
                assert_eq!(
                    fast, oracle,
                    "corpus {label} strategy {strategy:?} pattern `{p}`: files-only path != scan"
                );
            }
        }
    }
    Ok(())
}
