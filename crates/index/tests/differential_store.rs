mod common;

use common::{corpus, decoded_corpus, gzipped_corpus, PATTERNS};
use holys3_core::{scan_matching_docs, testutil::MemCorpus, Corpus, LocalBlobStore, Strategy};
use holys3_index::{
    search_collect, search_streaming, update_index, KeyScope, NullSink, SegmentedReader,
};

/// The store-backed (segmented) index must agree with a full scan of
/// decompressed bodies for both strategies and both corpora.
#[test]
fn store_index_equals_scan_for_many_patterns() -> anyhow::Result<()> {
    for (label, c) in [("plain", corpus()), ("gzipped", gzipped_corpus())] {
        for strategy in [Strategy::Trigram, Strategy::Sparse] {
            eprintln!("differential_store corpus={label} strategy={strategy:?}");
            let store_dir = tempfile::tempdir()?;
            let cache_dir = tempfile::tempdir()?;
            let store = LocalBlobStore::new(store_dir.path());
            let listing = c
                .docs()
                .iter()
                .map(|(_, key)| (key.clone(), format!("etag-{key}")))
                .collect::<Vec<_>>();
            update_index(
                &store,
                cache_dir.path(),
                strategy,
                &listing,
                false,
                &|keys| {
                    let docs = keys
                        .iter()
                        .enumerate()
                        .map(|(i, key)| (i as u32, key.clone()))
                        .collect();
                    let bodies = keys
                        .iter()
                        .map(|key| {
                            let (id, _) = c
                                .docs()
                                .iter()
                                .find(|(_, k)| k == key)
                                .expect("listed key exists");
                            c.fetch(*id)
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    Ok(Box::new(MemCorpus::new(docs, bodies)))
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
