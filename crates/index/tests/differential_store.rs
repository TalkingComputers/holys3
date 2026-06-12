mod common;

use common::{corpus, decoded_corpus, encoded_corpus, gzipped_corpus, PATTERNS};
use holys3_core::{
    scan_matching_docs, testutil::MemCorpus, Corpus, LocalBlobStore, MatchOptions, Strategy,
};
use holys3_index::{
    search_collect, search_streaming, update_index, KeyScope, NullSink, SegmentedReader,
};

/// The store-backed (segmented) index must agree with a full scan of
/// decompressed bodies for both strategies and both corpora.
#[test]
fn store_index_equals_scan_for_many_patterns() -> anyhow::Result<()> {
    for (label, c) in [
        ("plain", corpus()),
        ("gzipped", gzipped_corpus()),
        ("encoded", encoded_corpus()),
    ] {
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            eprintln!("differential_store corpus={label} strategy={strategy:?}");
            let store_dir = tempfile::tempdir()?;
            let cache_dir = tempfile::tempdir()?;
            let store = LocalBlobStore::new(store_dir.path());
            let listing = c
                .docs()
                .iter()
                .map(|doc| (doc.key.clone(), format!("etag-{}", doc.key), doc.size))
                .collect::<Vec<_>>();
            update_index(
                &store,
                cache_dir.path(),
                strategy,
                &listing,
                false,
                &|shard| {
                    let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
                    let bodies = keys
                        .iter()
                        .map(|key| {
                            let idx = c
                                .docs()
                                .iter()
                                .position(|doc| doc.key == *key)
                                .expect("listed key exists");
                            c.fetch(idx)
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    Ok(Box::new(MemCorpus::new(keys, bodies)))
                },
            )?;
            let reader = SegmentedReader::open(
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
                let fast = search_streaming(
                    &reader,
                    &c,
                    p,
                    KeyScope::default(),
                    MatchOptions::default(),
                    &NullSink,
                )?
                .hits;
                assert_eq!(
                    fast, oracle,
                    "corpus {label} strategy {strategy:?} pattern `{p}`: files-only path != scan"
                );
            }
        }
    }
    Ok(())
}
