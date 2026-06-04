mod common;

use common::{corpus, PATTERNS};
use holys3_core::{scan_matching_docs, Corpus, DocId, LocalBlobStore, Strategy};
use holys3_index::{build_to_store, compute_build_id, search, StoreIndexReader};
use std::collections::BTreeSet;

#[test]
fn store_index_equals_scan_for_many_patterns() -> anyhow::Result<()> {
    let c = corpus();
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
        for p in PATTERNS {
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
