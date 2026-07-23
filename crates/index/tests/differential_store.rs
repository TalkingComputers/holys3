mod common;

use bytes::Bytes;
use common::{corpus, encoded_corpus, gzipped_corpus, PATTERNS};
use seagrep_core::{
    grep_doc, parse_pattern, scan_matching_docs, testutil::MemCorpus, BlobStore, Corpus,
    DocAddress, DocFetcher, DocumentBody, LineEvent, LocalBlobStore, MatchOptions, PatternProgram,
    Strategy, CANDIDATE_BLOCK_BYTES,
};
use seagrep_index::{
    search_collect, search_patterns, search_streaming, update_index, DocResult, IndexReader,
    IndexStats, KeyScope, MatchData, MatchSink, MatchWindow, NullSink, SearchDetail,
    SegmentedReader, SinkFlow, SourceIdentity, UpdateOptions,
};
use seagrep_query::Query;
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
                let hir = parse_pattern(p)?;
                let program = PatternProgram::compile(std::slice::from_ref(&hir), &[0])?;
                let oracle = scan_matching_docs(&c, &program)?;
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
    fn detail(&self) -> SearchDetail {
        SearchDetail::FullLines
    }

    fn on_doc(&self, _key: &str, document: &DocResult<'_>) -> anyhow::Result<SinkFlow> {
        let MatchData::Lines(events) = document.data else {
            anyhow::bail!("event sink requires line data");
        };
        self.events.lock().unwrap().extend_from_slice(events);
        Ok(SinkFlow::Continue)
    }
}

#[derive(Default)]
struct CountEventSink {
    events: Mutex<Vec<LineEvent>>,
}

impl MatchSink for CountEventSink {
    fn detail(&self) -> SearchDetail {
        SearchDetail::MatchCount
    }

    fn on_doc(&self, _key: &str, document: &DocResult<'_>) -> anyhow::Result<SinkFlow> {
        let MatchData::Lines(events) = document.data else {
            anyhow::bail!("count event sink requires line data");
        };
        self.events.lock().unwrap().extend_from_slice(events);
        Ok(SinkFlow::Continue)
    }
}

fn open_indexed(
    store_dir: &Path,
    cache_dir: &Path,
    keys: Vec<String>,
    bodies: Vec<Vec<u8>>,
) -> anyhow::Result<SegmentedReader> {
    let source = SourceIdentity::Local {
        prefix: "/test/".into(),
    };
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
    update_index(
        &LocalBlobStore::new(store_dir),
        cache_dir,
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
    SegmentedReader::open(Box::new(LocalBlobStore::new(store_dir)), cache_dir, &source)
}

#[test]
fn regional_verification_matches_whole_document_scanning() -> anyhow::Result<()> {
    const BLOCK: usize = CANDIDATE_BLOCK_BYTES;
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
    let reader = open_indexed(
        store_dir.path(),
        cache_dir.path(),
        vec!["large.log".into()],
        vec![body.clone()],
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
        let hir = parse_pattern(pattern)?;
        let program = PatternProgram::compile(std::slice::from_ref(&hir), &[0])?;
        let expected = grep_doc(&body, &program, options);
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

    let hir = parse_pattern("DUPTOKEN")?;
    let program = PatternProgram::compile(std::slice::from_ref(&hir), &[0])?;
    let expected = grep_doc(&body, &program, MatchOptions::default());
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
fn whole_document_max_count_carries_submatches_into_after_context() -> anyhow::Result<()> {
    let body = b"hit\nhit\nrest\n".to_vec();
    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let reader = open_indexed(
        store_dir.path(),
        cache_dir.path(),
        vec!["small.log".into()],
        vec![body.clone()],
    )?;
    let options = MatchOptions {
        after_context: 1,
        max_count: Some(1),
        ..Default::default()
    };
    let hir = parse_pattern("hit")?;
    let program = PatternProgram::compile(std::slice::from_ref(&hir), &[0])?;
    let expected = grep_doc(&body, &program, options);
    let sink = EventSink::default();
    let stats = search_streaming(&reader, "hit", KeyScope::default(), options, &sink)?;
    assert_eq!(stats.whole_docs, 1);
    assert_eq!(sink.events.into_inner().unwrap(), expected);
    Ok(())
}

#[test]
fn candidate_fetches_union_across_documents() -> anyhow::Result<()> {
    const DOCUMENTS: usize = 8;
    const BLOCK: usize = CANDIDATE_BLOCK_BYTES;
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum CapturedData {
    Documents,
    Lines(Vec<LineEvent>),
    Windows(Vec<MatchWindow>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedDocument {
    key: String,
    data: CapturedData,
    bytes_searched: u64,
}

struct CaptureSink {
    detail: SearchDetail,
    documents: Mutex<Vec<CapturedDocument>>,
}

impl MatchSink for CaptureSink {
    fn detail(&self) -> SearchDetail {
        self.detail
    }

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow> {
        let data = match &doc.data {
            MatchData::Documents => CapturedData::Documents,
            MatchData::Lines(events) => CapturedData::Lines(events.to_vec()),
            MatchData::Windows(windows) => CapturedData::Windows(windows.to_vec()),
        };
        self.documents
            .lock()
            .expect("capture sink lock")
            .push(CapturedDocument {
                key: key.to_owned(),
                data,
                bytes_searched: doc.bytes_searched,
            });
        Ok(SinkFlow::Continue)
    }
}

struct WholeReader<'a> {
    reader: &'a SegmentedReader,
}

impl DocFetcher for WholeReader<'_> {
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, DocumentBody) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.reader.fetch_each(documents, consume)
    }
}

impl IndexReader for WholeReader<'_> {
    fn strategy(&self) -> Strategy {
        self.reader.strategy()
    }

    fn total_docs(&self) -> usize {
        self.reader.total_docs()
    }

    fn candidate_docs(
        &self,
        query: &Query,
        key_prefix: Option<&str>,
    ) -> anyhow::Result<Vec<DocAddress>> {
        self.reader.candidate_docs(query, key_prefix)
    }

    fn stats(&self) -> IndexStats {
        self.reader.stats()
    }
}

#[test]
fn mixed_patterns_match_whole_document_oracle_on_giant_line() -> anyhow::Result<()> {
    const BLOCK: usize = CANDIDATE_BLOCK_BYTES;
    let mut giant = vec![b'x'; 40 * BLOCK];
    giant[..10].copy_from_slice(b"FIRSTTOKEN");
    let cross = BLOCK - 4;
    giant[cross..cross + 15].copy_from_slice(b"CROSSBLOCKTOKEN");
    giant[2 * BLOCK + 17..2 * BLOCK + 25].copy_from_slice(b"DUPTOKEN");
    giant[36 * BLOCK + 17..36 * BLOCK + 25].copy_from_slice(b"DUPTOKEN");
    giant[37 * BLOCK + 17..37 * BLOCK + 42].copy_from_slice(b"BEGINABCDEFGHIJKLMNOPQRST");
    giant[38 * BLOCK + 17..38 * BLOCK + 40].copy_from_slice(b"ABCDEFGHIJKLMNOPQRSTEND");
    let last = giant.len() - 9;
    giant[last..].copy_from_slice(b"LASTTOKEN");

    let store_dir = tempfile::tempdir()?;
    let cache_dir = tempfile::tempdir()?;
    let reader = open_indexed(
        store_dir.path(),
        cache_dir.path(),
        vec!["giant.log".into(), "line.log".into(), "doc.log".into()],
        vec![
            giant,
            b"LINEFALLBACK hello world\n".to_vec(),
            b"DOCFALLBACK anchored body".to_vec(),
        ],
    )?;
    let whole = WholeReader { reader: &reader };
    let patterns = [
        "FIRSTTOKEN",
        "CROSSBLOCKTOKEN",
        "DUPTOKEN",
        "LASTTOKEN",
        "BEGIN[A-Z0-9]{20,}",
        "[A-Z0-9]{20,}END",
        "(?m)^LINEFALLBACK.*$",
        r"\ADOCFALLBACK.*",
    ];
    let hirs = patterns
        .iter()
        .map(|pattern| parse_pattern(pattern))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut saw_regional = false;
    for detail in [
        SearchDetail::Documents,
        SearchDetail::MatchingLines,
        SearchDetail::MatchCount,
        SearchDetail::FullLines,
        SearchDetail::MatchWindows { max_bytes: 64 },
    ] {
        let production = CaptureSink {
            detail,
            documents: Mutex::new(Vec::new()),
        };
        let oracle = CaptureSink {
            detail,
            documents: Mutex::new(Vec::new()),
        };
        let prod_stats = search_patterns(
            &reader,
            &hirs,
            KeyScope::default(),
            MatchOptions::default(),
            &production,
        )?;
        let oracle_stats = search_patterns(
            &whole,
            &hirs,
            KeyScope::default(),
            MatchOptions::default(),
            &oracle,
        )?;
        let mut prod_docs = production.documents.into_inner().unwrap();
        let mut oracle_docs = oracle.documents.into_inner().unwrap();
        prod_docs.sort_by(|left, right| left.key.cmp(&right.key));
        oracle_docs.sort_by(|left, right| left.key.cmp(&right.key));
        assert_eq!(prod_docs, oracle_docs, "detail {detail:?}");
        assert_eq!(prod_stats.hits, oracle_stats.hits, "detail {detail:?}");
        assert_eq!(
            prod_stats.hit_count, oracle_stats.hit_count,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.candidates, oracle_stats.candidates,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.total_docs, oracle_stats.total_docs,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.decoded_bytes, oracle_stats.decoded_bytes,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.patterns, oracle_stats.patterns,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.exact_patterns, oracle_stats.exact_patterns,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.proof_patterns, oracle_stats.proof_patterns,
            "detail {detail:?}"
        );
        assert_eq!(
            prod_stats.fallback_patterns, oracle_stats.fallback_patterns,
            "detail {detail:?}"
        );
        saw_regional |= prod_stats.regional_docs > 0;
        if detail != SearchDetail::MatchCount {
            assert!(
                prod_stats.regional_docs > 0,
                "detail {detail:?} regional={} whole={}",
                prod_stats.regional_docs,
                prod_stats.whole_docs
            );
        }
        if let SearchDetail::MatchWindows { max_bytes } = detail {
            for document in &prod_docs {
                if let CapturedData::Windows(windows) = &document.data {
                    for window in windows {
                        assert!(window.text.len() <= max_bytes);
                    }
                }
            }
        }
    }
    assert!(saw_regional);

    let proof_hir = parse_pattern("BEGIN[A-Z0-9]{20,}")?;
    let production = CaptureSink {
        detail: SearchDetail::MatchWindows { max_bytes: 8 },
        documents: Mutex::new(Vec::new()),
    };
    let stats = search_patterns(
        &reader,
        std::slice::from_ref(&proof_hir),
        KeyScope::default(),
        MatchOptions::default(),
        &production,
    )?;
    assert!(stats.regional_docs > 0);
    let documents = production.documents.into_inner().unwrap();
    assert_eq!(documents.len(), 1);
    let CapturedData::Windows(windows) = &documents[0].data else {
        panic!("window detail must capture windows");
    };
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].matches.len(), 1);
    assert!(!windows[0].matches[0].canonical_span_known);
    assert!(windows[0].matches[0].right_clipped);
    Ok(())
}
