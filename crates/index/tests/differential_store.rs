mod common;

use common::{corpus, encoded_corpus, gzipped_corpus, PATTERNS};
use holys3_core::{
    scan_matching_docs, testutil::MemCorpus, Corpus, LocalBlobStore, MatchOptions, Strategy,
};
use holys3_index::{
    search_collect, search_streaming, update_index, KeyScope, NullSink, SegmentedReader,
    SourceIdentity, UpdateOptions,
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
            let source = SourceIdentity::Local {
                prefix: "/test/".into(),
            };
            let listing = c
                .sources()
                .iter()
                .map(|source| {
                    (
                        source.key.clone(),
                        source.version.clone(),
                        source.encoded_size,
                    )
                })
                .collect::<Vec<_>>();
            update_index(
                &store,
                cache_dir.path(),
                &source,
                Some(strategy),
                &listing,
                UpdateOptions::default(),
                &|shard| {
                    let keys: Vec<String> = shard.iter().map(|(key, _, _)| key.clone()).collect();
                    let bodies = keys
                        .iter()
                        .map(|key| {
                            let idx = c
                                .sources()
                                .iter()
                                .position(|source| source.key == *key)
                                .expect("listed key exists");
                            Ok(c.fetch(idx)?.to_vec())
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    Ok(Box::new(MemCorpus::new(keys, bodies)))
                },
            )?;
            let reader = SegmentedReader::open(
                Box::new(LocalBlobStore::new(store_dir.path())),
                cache_dir.path(),
                &source,
            )?;
            for p in PATTERNS {
                let indexed: Vec<String> = search_collect(&reader, p)?.1.hits;
                let re = regex::bytes::Regex::new(p)?;
                let oracle = scan_matching_docs(&c, &re)?;
                assert_eq!(
                    indexed, oracle,
                    "corpus {label} strategy {strategy:?} pattern `{p}`: store index != scan"
                );
                let fast = search_streaming(
                    &reader,
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
