//! Streaming search engine: packed snapshot fetch, parallel decompress+verify,
//! per-doc result sinks. Documents are addressed by key throughout.

use crate::{CandidateBatchLimits, CandidatePlan, IndexReader, SearchStats};
use anyhow::{Context, Result};
use rayon::iter::{IntoParallelIterator, ParallelBridge, ParallelIterator};
use seagrep_core::{DocAddress, DocFetcher, FetchedDocument, LineEvent, MatchOptions};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

mod plan;
mod verify;

use plan::*;
use verify::*;

const SEARCH_CANDIDATE_BATCH: usize = 16_384;
const INCREMENTAL_CANDIDATE_BATCH: usize = 8;
const FILE_MATCH_CHUNK: usize = 1024 * 1024;
const FILE_MATCH_OVERLAP_MAX: usize = 1024 * 1024;

/// Key-level search scope. `prefix` is authoritative for both segment
/// pruning in readers and per-key filtering here; `matches` carries any
/// finer predicate (regex, time windows).
#[derive(Default, Clone, Copy)]
pub struct KeyScope<'a> {
    pub prefix: Option<&'a str>,
    pub matches: Option<&'a (dyn Fn(&str) -> bool + Sync)>,
}

impl KeyScope<'_> {
    fn admits(&self, key: &str) -> bool {
        if let Some(prefix) = self.prefix {
            if !key.starts_with(prefix) {
                return false;
            }
        }
        match self.matches {
            Some(matches) => matches(key),
            None => true,
        }
    }
}

/// Whether to keep streaming results after a sink call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkFlow {
    Continue,
    /// End the search early and report success (e.g. downstream pipe closed).
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchDetail {
    Documents,
    MatchingLines,
    MatchCount,
    MatchWindows { max_bytes: usize },
    FullLines,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchWindow {
    pub line: u64,
    pub line_offset: u64,
    pub window_offset: u64,
    pub text: bytes::Bytes,
    pub matches: Vec<WindowMatch>,
    pub left_clipped: bool,
    pub right_clipped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowMatch {
    pub witness: Range<u64>,
    pub visible: Range<usize>,
    pub left_clipped: bool,
    pub right_clipped: bool,
    pub canonical_span_known: bool,
}

#[derive(Debug)]
pub enum MatchData<'a> {
    Documents,
    Lines(&'a [LineEvent]),
    Windows(&'a [MatchWindow]),
}

#[derive(Debug)]
pub struct DocResult<'a> {
    pub data: MatchData<'a>,
    pub bytes_searched: u64,
    pub elapsed: std::time::Duration,
}

pub trait MatchSink: Sync {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool {
        true
    }

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow>;
}

/// Sentinel error that short-circuits verification on `SinkFlow::Stop`.
#[derive(Debug)]
struct StopEarly;

impl std::fmt::Display for StopEarly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("search stopped early by sink")
    }
}

impl std::error::Error for StopEarly {}

fn lock<'a, T>(mutex: &'a Mutex<T>) -> Result<MutexGuard<'a, T>> {
    mutex
        .lock()
        .map_err(|_| anyhow::anyhow!("a search worker panicked"))
}

struct BatchResult {
    hits: Vec<String>,
    hit_count: usize,
    regional_docs: usize,
    whole_docs: usize,
    candidate_bytes: usize,
    decoded_bytes: usize,
    stopped: bool,
}

fn search_batch(
    documents: &[DocAddress],
    fetcher: &dyn DocFetcher,
    context: SearchContext<'_>,
    sink: &dyn MatchSink,
) -> Result<BatchResult> {
    let batch = fetcher.start_candidate_batch(documents)?;
    let jobs = if documents.len() <= 1 {
        documents.len().max(1)
    } else {
        rayon::current_num_threads().min(documents.len())
    };
    let bytes_fetched = AtomicUsize::new(0);
    let regional_docs = AtomicUsize::new(0);
    let whole_docs = AtomicUsize::new(0);
    let decoded_bytes = AtomicUsize::new(0);
    let hit_count = AtomicUsize::new(0);
    let recorded = (0..documents.len())
        .map(|_| AtomicBool::new(false))
        .collect::<Vec<_>>();
    let hits: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let wants_hit_keys = sink.wants_hit_keys();
    let documents_ref = documents;
    let verify = |cache: &mut WorkerCache, idx: usize, body: FetchedDocument| -> Result<bool> {
        let key = &documents_ref[idx].display_key;
        let started = std::time::Instant::now();
        let Some(verified) = verify_document(batch.as_ref(), idx, body, context, cache)? else {
            return Ok(false);
        };
        bytes_fetched.fetch_add(verified.extra_fetched_bytes, Ordering::Relaxed);
        hit_count.fetch_add(1, Ordering::Relaxed);
        if wants_hit_keys {
            lock(&hits)?.push(key.clone());
        }
        let data = match &verified.data {
            OwnedMatchData::Documents => MatchData::Documents,
            OwnedMatchData::Lines(events) => MatchData::Lines(events),
            OwnedMatchData::Windows(windows) => MatchData::Windows(windows),
        };
        let doc = DocResult {
            data,
            bytes_searched: verified.bytes_searched,
            elapsed: started.elapsed(),
        };
        if sink.on_doc(key, &doc)? == SinkFlow::Stop {
            return Err(anyhow::Error::new(StopEarly));
        }
        Ok(true)
    };
    let verify_caught = |cache: &mut WorkerCache, idx: usize, body: FetchedDocument| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verify(cache, idx, body)))
            .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")))
    };
    let record_fetch = |idx: usize, body: &FetchedDocument| -> Result<()> {
        bytes_fetched.fetch_add(usize::try_from(body.fetched_size())?, Ordering::Relaxed);
        if recorded[idx].swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        decoded_bytes.fetch_add(usize::try_from(body.decoded_size())?, Ordering::Relaxed);
        match body {
            FetchedDocument::Whole(_) => {
                whole_docs.fetch_add(1, Ordering::Relaxed);
            }
            FetchedDocument::Regions { .. } => {
                regional_docs.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    };
    let (feed_result, verify_result) =
        if context.options.max_count == Some(1) && batch.can_fetch_documents() && jobs > 1 {
            let verified = (0..documents.len()).into_par_iter().try_for_each_init(
                || WorkerCache::create(context.programs),
                |cache, idx| {
                    batch.fetch_document_until(idx, &mut |body| {
                        record_fetch(idx, &body)?;
                        verify_caught(cache, idx, body)
                    })
                },
            );
            (Ok(()), verified)
        } else if context.options.max_count == Some(1) {
            let mut cache = WorkerCache::create(context.programs);
            let mut verified = Ok(());
            let feed = batch.fetch_until(&mut |idx, body| {
                record_fetch(idx, &body)?;
                match verify_caught(&mut cache, idx, body) {
                    Ok(matched) => Ok(matched),
                    Err(error) => {
                        verified = Err(error);
                        Err(anyhow::Error::new(StopEarly))
                    }
                }
            });
            (feed, verified)
        } else if jobs == 1 {
            let mut cache = WorkerCache::create(context.programs);
            let mut verified = Ok(());
            let feed = batch.fetch_initial(&mut |idx, body| {
                record_fetch(idx, &body)?;
                match verify_caught(&mut cache, idx, body) {
                    Ok(_) => Ok(()),
                    Err(error) => {
                        verified = Err(error);
                        Err(anyhow::Error::new(StopEarly))
                    }
                }
            });
            (feed, verified)
        } else {
            let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, FetchedDocument)>(jobs * 2);
            std::thread::scope(|scope| {
                let consumer = scope.spawn(|| {
                    rx.into_iter().par_bridge().try_for_each_init(
                        || WorkerCache::create(context.programs),
                        |cache, (idx, body)| verify_caught(cache, idx, body).map(|_| ()),
                    )
                });
                let feed = batch.fetch_initial(&mut |idx, body| {
                    record_fetch(idx, &body)?;
                    tx.send((idx, body))
                        .map_err(|_| anyhow::Error::new(StopEarly))
                });
                drop(tx);
                let verified = consumer
                    .join()
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")));
                (feed, verified)
            })
        };
    let stopped = match verify_result {
        Err(err) if err.is::<StopEarly>() => {
            if let Err(err) = feed_result {
                if !err.is::<StopEarly>() {
                    return Err(err);
                }
            }
            true
        }
        Err(err) => return Err(err),
        Ok(()) => {
            feed_result?;
            false
        }
    };
    let hits = hits
        .into_inner()
        .map_err(|_| anyhow::anyhow!("a search worker panicked"))?;
    Ok(BatchResult {
        hits,
        hit_count: hit_count.into_inner(),
        regional_docs: regional_docs.into_inner(),
        whole_docs: whole_docs.into_inner(),
        candidate_bytes: bytes_fetched.into_inner(),
        decoded_bytes: decoded_bytes.into_inner(),
        stopped,
    })
}

fn count_pattern_kind(plans: &[PatternPlan], kind: PatternKind) -> usize {
    plans.iter().filter(|plan| plan.kind == kind).count()
}

pub fn search_patterns(
    reader: &dyn IndexReader,
    hirs: &[regex_syntax::hir::Hir],
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<SearchStats> {
    let detail = sink.detail();
    let plans = build_plans(hirs, reader.strategy(), detail)?;
    let programs = SearchPrograms::compile(hirs, &plans)?;
    let stream_overlap = get_stream_overlap(&plans);
    let patterns = plans.len();
    let exact_patterns = count_pattern_kind(&plans, PatternKind::Exact);
    let proof_patterns = count_pattern_kind(&plans, PatternKind::Proof);
    let fallback_patterns = count_pattern_kind(&plans, PatternKind::Fallback);
    let total_docs = reader.total_docs();
    let excluded_objects = reader.excluded_objects();
    if options.max_count == Some(0) {
        return Ok(SearchStats {
            hits: Vec::new(),
            hit_count: 0,
            candidates: 0,
            total_docs,
            bytes_fetched: 0,
            regional_docs: 0,
            whole_docs: 0,
            candidate_bytes: 0,
            decoded_bytes: 0,
            excluded_objects,
            patterns,
            exact_patterns,
            proof_patterns,
            fallback_patterns,
        });
    }
    let context = SearchContext {
        plans: &plans,
        programs: &programs,
        stream_overlap,
        options,
        detail,
    };
    let candidate_plans = plans
        .iter()
        .map(|plan| CandidatePlan {
            query: &plan.query,
            extent: plan.extent,
        })
        .collect::<Vec<_>>();
    let mut hits = Vec::new();
    let mut hit_count = 0usize;
    let mut candidates = 0usize;
    let mut bytes_fetched = 0usize;
    let mut regional_docs = 0usize;
    let mut whole_docs = 0usize;
    let mut decoded_bytes = 0usize;
    let candidate_limits = if options.max_count == Some(1) {
        CandidateBatchLimits {
            documents: INCREMENTAL_CANDIDATE_BATCH,
            decoded_bytes: u64::MAX,
        }
    } else {
        CandidateBatchLimits {
            documents: SEARCH_CANDIDATE_BATCH,
            decoded_bytes: 64 * 1024 * 1024,
        }
    };
    let visited = reader.visit_candidates(
        &candidate_plans,
        scope.prefix,
        candidate_limits,
        &mut |mut documents| {
            documents.retain(|document| scope.admits(&document.display_key));
            candidates = candidates
                .checked_add(documents.len())
                .context("candidate count overflows usize")?;
            if documents.is_empty() {
                return Ok(true);
            }
            let batch = search_batch(&documents, reader, context, sink)?;
            hits.extend(batch.hits);
            hit_count = hit_count
                .checked_add(batch.hit_count)
                .context("hit count overflows usize")?;
            bytes_fetched = bytes_fetched
                .checked_add(batch.candidate_bytes)
                .context("fetched byte count overflows usize")?;
            regional_docs = regional_docs
                .checked_add(batch.regional_docs)
                .context("regional document count overflows usize")?;
            whole_docs = whole_docs
                .checked_add(batch.whole_docs)
                .context("whole document count overflows usize")?;
            decoded_bytes = decoded_bytes
                .checked_add(batch.decoded_bytes)
                .context("decoded byte count overflows usize")?;
            Ok(!batch.stopped)
        },
    );
    if let Err(error) = visited {
        if candidates > 0 && error.is::<crate::IndexChanged>() {
            anyhow::bail!(
                "index changed after candidate batches began; rerun the search to get a clean snapshot"
            );
        }
        return Err(error);
    }
    hits.sort_unstable();
    Ok(SearchStats {
        hits,
        hit_count,
        candidates,
        total_docs,
        bytes_fetched,
        regional_docs,
        whole_docs,
        candidate_bytes: bytes_fetched,
        decoded_bytes,
        excluded_objects,
        patterns,
        exact_patterns,
        proof_patterns,
        fallback_patterns,
    })
}

pub fn search_streaming(
    reader: &dyn IndexReader,
    pattern: &str,
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<SearchStats> {
    let hir = seagrep_core::parse_pattern(pattern)?;
    search_patterns(reader, std::slice::from_ref(&hir), scope, options, sink)
}

pub struct NullSink;

impl MatchSink for NullSink {
    fn detail(&self) -> SearchDetail {
        SearchDetail::Documents
    }

    fn on_doc(&self, _key: &str, _doc: &DocResult<'_>) -> Result<SinkFlow> {
        Ok(SinkFlow::Continue)
    }
}

#[derive(Default)]
struct CollectSink {
    matches: Mutex<Vec<(String, LineEvent)>>,
}

impl MatchSink for CollectSink {
    fn detail(&self) -> SearchDetail {
        SearchDetail::FullLines
    }

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let MatchData::Lines(events) = doc.data else {
            anyhow::bail!("collect sink requires line data");
        };
        let mut collected = lock(&self.matches)?;
        collected.extend(events.iter().map(|event| (key.to_owned(), event.clone())));
        Ok(SinkFlow::Continue)
    }
}

pub fn search_collect(
    reader: &dyn IndexReader,
    pattern: &str,
) -> Result<(Vec<(String, LineEvent)>, SearchStats)> {
    let sink = CollectSink::default();
    let stats = search_streaming(
        reader,
        pattern,
        KeyScope::default(),
        MatchOptions::default(),
        &sink,
    )?;
    let mut matches = sink
        .matches
        .into_inner()
        .map_err(|_| anyhow::anyhow!("a search worker panicked"))?;
    matches.sort_by(|(a_key, a), (b_key, b)| {
        (a_key, a.line, a.submatches.first().map(|s| s.start)).cmp(&(
            b_key,
            b.line,
            b.submatches.first().map(|s| s.start),
        ))
    });
    Ok((matches, stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IndexReader, IndexStats};
    use seagrep_core::{
        DocAddress, DocumentBody, DocumentRegion, PatternProgram, RegionProgram, SearchExtent,
        SourceEncoding, Strategy, CANDIDATE_BLOCK_BYTES,
    };
    use seagrep_query::Query;

    #[test]
    fn eligibility_programs_prevent_same_start_masking() {
        let hirs = ["\\Afoo", "(?m)^foo", "foo"]
            .into_iter()
            .map(seagrep_core::parse_pattern)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let plans = build_plans(&hirs, Strategy::Trigram, SearchDetail::FullLines).unwrap();
        assert_eq!(
            plans
                .iter()
                .map(|plan| (plan.id, plan.extent))
                .collect::<Vec<_>>(),
            [
                (0, SearchExtent::Document),
                (1, SearchExtent::Lines),
                (2, SearchExtent::Bytes { span: 3 }),
            ]
        );
        let programs = SearchPrograms::compile(&hirs, &plans).unwrap();
        let mut cache = WorkerCache::create(&programs);
        let body = b"foo";
        assert_eq!(
            find_program_matches(body, &programs.whole, &mut cache.whole)
                .into_iter()
                .map(|matched| matched.pattern)
                .collect::<Vec<_>>(),
            vec![0]
        );
        let (lines, lines_cache) =
            get_region_program(&programs, &mut cache, RegionProgram::Full).unwrap();
        assert_eq!(
            find_program_matches(body, lines, lines_cache)
                .into_iter()
                .map(|matched| matched.pattern)
                .collect::<Vec<_>>(),
            vec![1]
        );
        let (regional, regional_cache) =
            get_region_program(&programs, &mut cache, RegionProgram::Regional).unwrap();
        assert_eq!(
            find_program_matches(body, regional, regional_cache)
                .into_iter()
                .map(|matched| matched.pattern)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn max_count_keeps_earliest_union_lines_and_caps_large_exact_spans() {
        let hirs = ["second", "first"]
            .into_iter()
            .map(seagrep_core::parse_pattern)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let plans = build_plans(&hirs, Strategy::Trigram, SearchDetail::FullLines).unwrap();
        let programs = SearchPrograms::compile(&hirs, &plans).unwrap();
        let mut cache = WorkerCache::create(&programs);
        let body = b"first\nsecond\n";
        let matches = find_whole_matches(body, &plans, &programs, &mut cache, Some(1));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line, 1);
        assert_eq!(matches[0].pattern, 1);
        let huge = format!("a{{{}}}", CANDIDATE_BLOCK_BYTES + 1);
        let huge_hir = seagrep_core::parse_pattern(&huge).unwrap();
        let huge_plans = build_plans(
            std::slice::from_ref(&huge_hir),
            Strategy::Trigram,
            SearchDetail::FullLines,
        )
        .unwrap();
        assert_eq!(huge_plans[0].kind, PatternKind::Fallback);
        assert!(!matches!(huge_plans[0].extent, SearchExtent::Bytes { .. }));
    }

    #[test]
    fn bounded_file_search_matches_in_memory_across_chunks() {
        use std::io::{Seek, Write};
        let mut bytes = vec![b'x'; FILE_MATCH_CHUNK * 2 + 17];
        let at = FILE_MATCH_CHUNK - 3;
        bytes[at..at + 6].copy_from_slice(b"needle");
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        for pattern in ["needle", "missing", "x{16}needle", "needle|other"] {
            file.rewind().unwrap();
            let hir = seagrep_core::parse_pattern(pattern).unwrap();
            let program = PatternProgram::compile(std::slice::from_ref(&hir), &[0]).unwrap();
            let mut cache = program.create_cache();
            let overlap = match build_plans(
                std::slice::from_ref(&hir),
                Strategy::Trigram,
                SearchDetail::Documents,
            )
            .unwrap()[0]
                .extent
            {
                SearchExtent::Bytes { span } => span.saturating_sub(1),
                _ => continue,
            };
            let streamed = has_stream_match(
                &mut file,
                u64::try_from(bytes.len()).unwrap(),
                &program,
                &mut cache,
                overlap,
            )
            .unwrap();
            let memory = program.find_iter(&mut cache, &bytes).next().is_some();
            assert_eq!(streamed, memory, "{pattern}");
        }
    }

    #[test]
    fn regional_matches_share_a_line_across_an_unfetched_zero_newline_gap() {
        let regions = vec![
            DocumentRegion {
                start: 100,
                line: 7,
                line_offset: 80,
                bytes: bytes::Bytes::from_static(b"needle-left"),
                program: RegionProgram::Regional,
            },
            DocumentRegion {
                start: 1_000,
                line: 7,
                line_offset: 80,
                bytes: bytes::Bytes::from_static(b"right-needle\n"),
                program: RegionProgram::Regional,
            },
        ];
        let hir = seagrep_core::parse_pattern("needle").unwrap();
        let plans = build_plans(
            std::slice::from_ref(&hir),
            Strategy::Trigram,
            SearchDetail::FullLines,
        )
        .unwrap();
        let programs = SearchPrograms::compile(std::slice::from_ref(&hir), &plans).unwrap();
        let mut cache = WorkerCache::create(&programs);
        let matches =
            find_region_matches(&regions, 1_013, &plans, &programs, &mut cache, None).unwrap();
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0].line, 7);
        assert_eq!(matches[1].line, 7);
    }

    struct BatchReader {
        documents: Vec<DocAddress>,
        largest: AtomicUsize,
    }

    impl DocFetcher for BatchReader {
        fn fetch_each(
            &self,
            documents: &[DocAddress],
            consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
        ) -> Result<()> {
            self.largest.fetch_max(documents.len(), Ordering::Relaxed);
            for index in 0..documents.len() {
                consume(
                    index,
                    DocumentBody::from_bytes(bytes::Bytes::from_static(b"needle\n")),
                )?;
            }
            Ok(())
        }
    }

    impl IndexReader for BatchReader {
        fn strategy(&self) -> Strategy {
            Strategy::Trigram
        }

        fn total_docs(&self) -> usize {
            self.documents.len()
        }

        fn candidate_docs(
            &self,
            _query: &Query,
            _key_prefix: Option<&str>,
        ) -> Result<Vec<DocAddress>> {
            panic!("search should consume candidate batches")
        }

        fn visit_candidates(
            &self,
            plans: &[CandidatePlan<'_>],
            key_prefix: Option<&str>,
            limits: CandidateBatchLimits,
            visit: &mut dyn FnMut(Vec<DocAddress>) -> Result<bool>,
        ) -> Result<()> {
            anyhow::ensure!(plans.len() == 1, "expected one candidate plan");
            let _ = (key_prefix, limits);
            for chunk in self.documents.chunks(2) {
                if !visit(chunk.to_vec())? {
                    break;
                }
            }
            Ok(())
        }

        fn stats(&self) -> IndexStats {
            IndexStats {
                distinct_grams: 0,
                terms_fst_bytes: 0,
                postings_bytes: 0,
            }
        }
    }

    #[test]
    fn single_candidate_does_not_start_rayon_pool() {
        const PROBE: &str = "SEAGREP_SINGLE_CANDIDATE_RAYON_PROBE";
        if std::env::var_os(PROBE).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "search::tests::single_candidate_does_not_start_rayon_pool",
                    "--test-threads=1",
                ])
                .env(PROBE, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        let documents = [DocAddress {
            display_key: "doc".into(),
            source_key: "doc".into(),
            source_version: "v1".into(),
            encoded_size: 7,
            encoding: SourceEncoding::Raw,
            member_path: None,
            index: None,
        }];
        let fetcher = BatchReader {
            documents: Vec::new(),
            largest: AtomicUsize::new(0),
        };
        let hir = seagrep_core::parse_pattern("needle").unwrap();
        let plans = build_plans(
            std::slice::from_ref(&hir),
            Strategy::Trigram,
            SearchDetail::Documents,
        )
        .unwrap();
        let programs = SearchPrograms::compile(std::slice::from_ref(&hir), &plans).unwrap();
        let context = SearchContext {
            plans: &plans,
            programs: &programs,
            stream_overlap: get_stream_overlap(&plans),
            options: MatchOptions::default(),
            detail: SearchDetail::Documents,
        };
        let batch = search_batch(&documents, &fetcher, context, &NullSink).unwrap();
        assert_eq!(batch.hits, ["doc"]);
        assert_eq!(batch.hit_count, 1);
        assert_eq!(batch.candidate_bytes, 7);
        assert_eq!(batch.decoded_bytes, 7);
        assert_eq!(batch.whole_docs, 1);
        assert_eq!(batch.regional_docs, 0);
        assert!(!batch.stopped);
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build_global()
            .expect("single-candidate search initialized Rayon's global pool");
    }

    #[test]
    fn search_consumes_candidate_batches() {
        let reader = BatchReader {
            documents: (0..5)
                .map(|index| DocAddress {
                    display_key: format!("doc-{index}"),
                    source_key: format!("doc-{index}"),
                    source_version: "v1".into(),
                    encoded_size: 7,
                    encoding: SourceEncoding::Raw,
                    member_path: None,
                    index: None,
                })
                .collect(),
            largest: AtomicUsize::new(0),
        };
        let stats = search_streaming(
            &reader,
            "needle",
            KeyScope::default(),
            MatchOptions::default(),
            &NullSink,
        )
        .unwrap();
        assert_eq!(stats.hits.len(), 5);
        assert_eq!(reader.largest.load(Ordering::Relaxed), 2);
    }

    struct ChangingReader {
        document: DocAddress,
    }

    impl DocFetcher for ChangingReader {
        fn fetch_each(
            &self,
            documents: &[DocAddress],
            consume: &mut dyn FnMut(usize, DocumentBody) -> Result<()>,
        ) -> Result<()> {
            anyhow::ensure!(
                documents.len() == 1,
                "expected one changing-reader document"
            );
            consume(
                0,
                DocumentBody::from_bytes(bytes::Bytes::from_static(b"needle\n")),
            )
        }
    }

    impl IndexReader for ChangingReader {
        fn strategy(&self) -> Strategy {
            Strategy::Trigram
        }

        fn total_docs(&self) -> usize {
            1
        }

        fn candidate_docs(
            &self,
            _query: &Query,
            _key_prefix: Option<&str>,
        ) -> Result<Vec<DocAddress>> {
            unreachable!()
        }

        fn visit_candidates(
            &self,
            plans: &[CandidatePlan<'_>],
            key_prefix: Option<&str>,
            limits: CandidateBatchLimits,
            visit: &mut dyn FnMut(Vec<DocAddress>) -> Result<bool>,
        ) -> Result<()> {
            anyhow::ensure!(plans.len() == 1, "expected one candidate plan");
            let _ = (key_prefix, limits);
            visit(vec![self.document.clone()])?;
            Err(anyhow::Error::new(crate::IndexChanged))
        }

        fn stats(&self) -> IndexStats {
            IndexStats {
                distinct_grams: 0,
                terms_fst_bytes: 0,
                postings_bytes: 0,
            }
        }
    }

    #[test]
    fn index_change_after_a_batch_is_not_retryable() {
        let reader = ChangingReader {
            document: DocAddress {
                display_key: "doc".into(),
                source_key: "doc".into(),
                source_version: "v1".into(),
                encoded_size: 7,
                encoding: SourceEncoding::Raw,
                member_path: None,
                index: None,
            },
        };
        let error = search_streaming(
            &reader,
            "needle",
            KeyScope::default(),
            MatchOptions::default(),
            &NullSink,
        )
        .expect_err("late index change should fail the partial search");
        assert!(!error.is::<crate::IndexChanged>());
        assert!(error.to_string().contains("candidate batches began"));
    }
}
