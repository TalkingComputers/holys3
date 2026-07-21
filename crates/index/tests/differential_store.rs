mod common;

use bytes::Bytes;
use common::{corpus, encoded_corpus, gzipped_corpus, PATTERNS};
use seagrep_core::{
    grep_doc, scan_matching_docs, testutil::MemCorpus, BlobStore, Corpus, LineEvent,
    LocalBlobStore, MatchOptions, Strategy,
};
use seagrep_index::{
    search_collect, search_streaming, update_index, DocResult, KeyScope, MatchSink, NullSink,
    SegmentedReader, SinkFlow, SourceIdentity, UpdateOptions,
};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

struct CountingRangeStore {
    inner: LocalBlobStore,
    reads: Arc<AtomicUsize>,
}

impl BlobStore for CountingRangeStore {
    fn put(&self, name: &str, bytes: &[u8]) -> anyhow::Result<()> {
        self.inner.put(name, bytes)
    }

    fn put_file(&self, name: &str, path: &Path) -> anyhow::Result<()> {
        self.inner.put_file(name, path)
    }

    fn get(&self, name: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(name)
    }

    fn get_range(&self, name: &str, start: u64, len: u64) -> anyhow::Result<Vec<u8>> {
        self.inner.get_range(name, start, len)
    }

    fn get_ranges(&self, name: &str, ranges: &[(u64, u64)]) -> anyhow::Result<Vec<Bytes>> {
        if !name.starts_with("packs/") {
            return self.inner.get_ranges(name, ranges);
        }
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get_ranges(name, ranges)
    }

    fn delete(&self, name: &str) -> anyhow::Result<()> {
        self.inner.delete(name)
    }

    fn get_versioned(&self, name: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
        self.inner.get_versioned(name)
    }

    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> anyhow::Result<bool> {
        self.inner.put_if(name, bytes, expected)
    }
}

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

#[derive(Default)]
struct EventSink {
    events: Mutex<Vec<LineEvent>>,
}

impl MatchSink for EventSink {
    fn on_doc(&self, _key: &str, document: &DocResult<'_>) -> anyhow::Result<SinkFlow> {
        self.events
            .lock()
            .unwrap()
            .extend_from_slice(document.events);
        Ok(SinkFlow::Continue)
    }
}

#[derive(Default)]
struct CountEventSink {
    events: Mutex<Vec<LineEvent>>,
}

impl MatchSink for CountEventSink {
    fn wants_line_text(&self) -> bool {
        false
    }

    fn on_doc(&self, _key: &str, document: &DocResult<'_>) -> anyhow::Result<SinkFlow> {
        self.events
            .lock()
            .unwrap()
            .extend_from_slice(document.events);
        Ok(SinkFlow::Continue)
    }
}

#[test]
fn regional_verification_matches_whole_document_scanning() -> anyhow::Result<()> {
    const BLOCK: usize = seagrep_core::CANDIDATE_BLOCK_BYTES;
    let mut body = Vec::new();
    while body.len() < 40 * BLOCK {
        body.extend_from_slice(b"ordinary line with padding................................\n");
    }
    body[..10].copy_from_slice(b"FIRSTTOKEN");
    let long_start = 2 * BLOCK;
    let long_end = 30 * BLOCK;
    body[long_start..long_end].fill(b'x');
    let boundary = 12 * BLOCK - 4;
    body[boundary..boundary + 13].copy_from_slice(b"BOUNDARYTOKEN");
    body[18 * BLOCK + 17..18 * BLOCK + 30].copy_from_slice(b"LONGLINETOKEN");
    body[5 * BLOCK + 17..5 * BLOCK + 25].copy_from_slice(b"DUPTOKEN");
    body[25 * BLOCK + 17..25 * BLOCK + 25].copy_from_slice(b"DUPTOKEN");
    body[long_end] = b'\n';
    let context_at = 35 * BLOCK + 17;
    body[context_at..context_at + 12].copy_from_slice(b"CONTEXTTOKEN");
    let tail = body.len() - 9;
    body[tail..].copy_from_slice(b"LASTTOKEN");

    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let store = LocalBlobStore::new(store_dir.path());
    let source = SourceIdentity::Local {
        prefix: "/test/".into(),
    };
    let corpus = MemCorpus::new(vec!["large.log".into()], vec![body.clone()]);
    let listing = corpus
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
        Some(Strategy::Trigram),
        &listing,
        UpdateOptions::default(),
        &|_| {
            Ok(Box::new(MemCorpus::new(
                vec!["large.log".into()],
                vec![body.clone()],
            )))
        },
    )?;
    let reader = SegmentedReader::open(
        Box::new(LocalBlobStore::new(store_dir.path())),
        cache_dir.path(),
        &source,
    )?;

    for (pattern, options) in [
        ("FIRSTTOKEN", MatchOptions::default()),
        ("BOUNDARYTOKEN", MatchOptions::default()),
        ("LONGLINETOKEN", MatchOptions::default()),
        ("DUPTOKEN", MatchOptions::default()),
        (
            "CONTEXTTOKEN",
            MatchOptions {
                before_context: 2,
                after_context: 2,
                max_count: None,
            },
        ),
        ("LASTTOKEN", MatchOptions::default()),
        (
            "TOKEN",
            MatchOptions {
                before_context: 1,
                after_context: 1,
                max_count: Some(2),
            },
        ),
    ] {
        let regex = regex::bytes::Regex::new(pattern)?;
        let expected = grep_doc(&body, &regex, options);
        let sink = EventSink::default();
        let stats = search_streaming(&reader, pattern, KeyScope::default(), options, &sink)?;
        let actual = sink.events.into_inner().unwrap();
        assert_eq!(actual, expected, "pattern {pattern}");
        assert_eq!(stats.hit_count, usize::from(!expected.is_empty()));
        let files = search_streaming(&reader, pattern, KeyScope::default(), options, &NullSink)?;
        assert_eq!(files.hit_count, usize::from(!expected.is_empty()));
        if pattern == "BOUNDARYTOKEN" {
            assert!(stats.bytes_fetched < body.len());
            assert_eq!(stats.regional_docs, 1);
            assert_eq!(stats.whole_docs, 0);
            assert_eq!(stats.candidate_bytes, stats.bytes_fetched);
            assert_eq!(stats.decoded_bytes, body.len());
            assert!(files.bytes_fetched < body.len());
        }
    }

    let regex = regex::bytes::Regex::new("DUPTOKEN")?;
    let expected = grep_doc(&body, &regex, MatchOptions::default());
    let sink = CountEventSink::default();
    let stats = search_streaming(
        &reader,
        "DUPTOKEN",
        KeyScope::default(),
        MatchOptions::default(),
        &sink,
    )?;
    let actual = sink.events.into_inner().unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].line, expected[0].line);
    assert_eq!(actual[0].submatches.len(), 2);
    assert!(actual[0].text.is_empty());
    assert_eq!(stats.regional_docs, 1);
    assert_eq!(stats.whole_docs, 0);
    assert!(stats.candidate_bytes < 8 * BLOCK);
    Ok(())
}

#[test]
fn candidate_fetches_union_across_documents() -> anyhow::Result<()> {
    const DOCUMENTS: usize = 8;
    const BLOCK: usize = seagrep_core::CANDIDATE_BLOCK_BYTES;
    let mut body = Vec::new();
    while body.len() < 4 * BLOCK {
        body.extend_from_slice(b"ordinary line with padding................................\n");
    }
    let token = 2 * BLOCK + 17;
    body[token..token + 13].copy_from_slice(b"PARALLELTOKEN");

    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let source = SourceIdentity::Local {
        prefix: "/test/".into(),
    };
    let keys = (0..DOCUMENTS)
        .map(|index| format!("doc-{index}.log"))
        .collect::<Vec<_>>();
    let bodies = vec![body; DOCUMENTS];
    let corpus = MemCorpus::new(keys.clone(), bodies.clone());
    let listing = corpus
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
    let report = update_index(
        &LocalBlobStore::new(store_dir.path()),
        cache_dir.path(),
        &source,
        Some(Strategy::Trigram),
        &listing,
        UpdateOptions::default(),
        &|shard| {
            let selected = shard
                .iter()
                .map(|(key, _, _)| keys.iter().position(|candidate| candidate == key).unwrap())
                .collect::<Vec<_>>();
            Ok(Box::new(MemCorpus::new(
                selected.iter().map(|index| keys[*index].clone()).collect(),
                selected
                    .iter()
                    .map(|index| bodies[*index].clone())
                    .collect(),
            )))
        },
    )?;
    assert_eq!(report.segments, 1);
    let reads = Arc::new(AtomicUsize::new(0));
    let reader = SegmentedReader::open(
        Box::new(CountingRangeStore {
            inner: LocalBlobStore::new(store_dir.path()),
            reads: Arc::clone(&reads),
        }),
        cache_dir.path(),
        &source,
    )?;

    let (matches, stats) = search_collect(&reader, "PARALLELTOKEN")?;
    assert_eq!(matches.len(), DOCUMENTS);
    assert_eq!(stats.hits.len(), DOCUMENTS);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    Ok(())
}
