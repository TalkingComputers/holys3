//! Streaming search engine: packed snapshot fetch, parallel decompress+verify,
//! per-doc result sinks. Documents are addressed by key throughout.

use crate::{CandidateBatchLimits, CandidatePlan, IndexReader, SearchStats};
use anyhow::{Context, Result};
use rayon::iter::{ParallelBridge, ParallelIterator};
#[cfg(test)]
use seagrep_core::DocumentBody;
use seagrep_core::{
    bounded_match_len, can_search_as_document, grep_bytes, grep_bytes_fast, has_line_match,
    has_line_match_fast, DocAddress, DocFetcher, DocumentRegion, FetchedDocument, LineEvent,
    MatchOptions, RegionRead, SearchExtent,
};
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

const SEARCH_CANDIDATE_BATCH: usize = 16_384;
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

/// Everything a sink learns about one matching doc.
#[derive(Debug)]
pub struct DocResult<'a> {
    /// Empty when the sink declined match positions.
    pub events: &'a [LineEvent],
    /// Decoded doc length.
    pub bytes_searched: u64,
    /// Decode + verify wall time for this doc.
    pub elapsed: std::time::Duration,
}

/// Receives verified results per doc, possibly from several threads at once.
pub trait MatchSink: Sync {
    /// Whether this sink uses match positions. Returning false lets the
    /// engine stop at the first match per doc (files-only behavior); `on_doc`
    /// then sees empty `events`.
    fn wants_matches(&self) -> bool {
        true
    }

    /// Whether `SearchStats.hits` should carry every matching key. Sinks
    /// that only need `hit_count` return false so a query matching millions
    /// of docs does not materialize and sort millions of strings.
    fn wants_hit_keys(&self) -> bool {
        true
    }

    fn wants_line_text(&self) -> bool {
        true
    }

    /// Called once per doc with at least one verified match.
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

fn has_bounded_reader_match(
    reader: &mut impl Read,
    len: u64,
    re: &regex::bytes::Regex,
    match_len: usize,
) -> Result<bool> {
    if len == 0 {
        return Ok(false);
    }
    if match_len == 0 {
        return Ok(true);
    }
    let overlap = match_len - 1;
    anyhow::ensure!(
        overlap <= FILE_MATCH_OVERLAP_MAX,
        "streaming regex overlap exceeds its limit"
    );
    let chunk_bytes = usize::try_from(len.min(u64::try_from(FILE_MATCH_CHUNK)?))?;
    let mut chunk = vec![0u8; chunk_bytes + overlap];
    let mut carry = 0usize;
    let mut remaining = len;
    while remaining > 0 {
        let read = usize::try_from(remaining.min(u64::try_from(chunk_bytes)?))?;
        reader.read_exact(&mut chunk[carry..carry + read])?;
        let end = carry + read;
        if re.is_match(&chunk[..end]) {
            return Ok(true);
        }
        carry = end.min(overlap);
        chunk.copy_within(end - carry..end, 0);
        remaining -= u64::try_from(read)?;
    }
    Ok(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegionMatch {
    start: u64,
    end: u64,
    line: u64,
}

fn find_region_matches(
    regions: &[DocumentRegion],
    re: &regex::bytes::Regex,
    decoded_size: u64,
    max_count: Option<u64>,
) -> Vec<RegionMatch> {
    let mut matches = Vec::new();
    let mut last_line = None;
    let mut matched_lines = 0u64;
    'regions: for region in regions {
        let mut scanned = 0usize;
        let mut line = region.line;
        for matched in re.find_iter(&region.bytes) {
            line += region.bytes[scanned..matched.start()]
                .iter()
                .filter(|byte| **byte == b'\n')
                .count() as u64;
            scanned = matched.start();
            let start = region.start + matched.start() as u64;
            if start >= decoded_size {
                continue;
            }
            if last_line != Some(line) {
                if max_count == Some(matched_lines) {
                    break 'regions;
                }
                matched_lines += 1;
                last_line = Some(line);
            }
            matches.push(RegionMatch {
                start,
                end: region.start + matched.end() as u64,
                line,
            });
        }
    }
    matches
}

fn match_count_events(matches: &[RegionMatch]) -> Vec<LineEvent> {
    let mut events: Vec<LineEvent> = Vec::new();
    for matched in matches {
        if let Some(event) = events.last_mut().filter(|event| event.line == matched.line) {
            event
                .submatches
                .push(seagrep_core::SubMatch { start: 0, end: 0 });
            continue;
        }
        events.push(LineEvent {
            line: matched.line,
            kind: seagrep_core::LineKind::Match,
            offset: matched.start,
            text: bytes::Bytes::new(),
            submatches: vec![seagrep_core::SubMatch { start: 0, end: 0 }],
        });
    }
    events
}

fn grep_fetched_document(
    body: FetchedDocument,
    re: &regex::bytes::Regex,
    whole_document: bool,
    options: MatchOptions,
) -> Result<Vec<LineEvent>> {
    match body {
        FetchedDocument::Whole(body) => {
            let text = body.into_bytes()?;
            Ok(if whole_document {
                grep_bytes_fast(text, re, options)
            } else {
                grep_bytes(text, re, options)
            })
        }
        FetchedDocument::Regions { regions, .. } => {
            let mut events = Vec::new();
            let mut matched = 0u64;
            for region in regions {
                let max_count = options.max_count.map(|limit| limit.saturating_sub(matched));
                if max_count == Some(0) {
                    break;
                }
                let regional = MatchOptions {
                    max_count,
                    ..options
                };
                let mut found = if whole_document {
                    grep_bytes_fast(region.bytes, re, regional)
                } else {
                    grep_bytes(region.bytes, re, regional)
                };
                matched += found
                    .iter()
                    .filter(|event| event.kind == seagrep_core::LineKind::Match)
                    .count() as u64;
                for event in &mut found {
                    event.line += region.line - 1;
                    event.offset += region.start;
                }
                events.extend(found);
            }
            Ok(events)
        }
    }
}

fn search_batch(
    documents: &[DocAddress],
    fetcher: &dyn DocFetcher,
    re: &regex::bytes::Regex,
    whole_document: bool,
    bounded_len: Option<usize>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<BatchResult> {
    let batch = fetcher.start_candidate_batch(documents)?;
    let workers = std::thread::available_parallelism()?
        .get()
        .min(documents.len());
    let bytes_fetched = AtomicUsize::new(0);
    let regional_docs = AtomicUsize::new(0);
    let whole_docs = AtomicUsize::new(0);
    let decoded_bytes = AtomicUsize::new(0);
    let hit_count = AtomicUsize::new(0);
    let hits: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let wants_matches = sink.wants_matches();
    let wants_hit_keys = sink.wants_hit_keys();
    let wants_line_text = sink.wants_line_text();
    let regional_bounded_len =
        bounded_len.filter(|len| *len <= seagrep_core::CANDIDATE_BLOCK_BYTES);
    let documents_ref = documents;
    let verify = |re: &regex::bytes::Regex, idx: usize, body: FetchedDocument| -> Result<()> {
        let key = &documents_ref[idx].display_key;
        let started = std::time::Instant::now();
        let bytes_searched = body.decoded_size();
        let events = match body {
            FetchedDocument::Regions {
                decoded_size,
                regions,
            } if regional_bounded_len.is_some() => {
                let matches = find_region_matches(&regions, re, decoded_size, options.max_count);
                if matches.is_empty() {
                    return Ok(());
                }
                if !wants_matches {
                    Vec::new()
                } else if wants_line_text {
                    let ranges = matches
                        .iter()
                        .map(|matched| matched.start..matched.end)
                        .collect::<Vec<_>>();
                    let lines = batch.fetch_regions(
                        idx,
                        &ranges,
                        RegionRead::Lines {
                            before_context: options.before_context,
                            after_context: options.after_context,
                        },
                    )?;
                    bytes_fetched
                        .fetch_add(usize::try_from(lines.fetched_size())?, Ordering::Relaxed);
                    let events = grep_fetched_document(lines, re, whole_document, options)?;
                    if events.is_empty() {
                        return Ok(());
                    }
                    events
                } else {
                    match_count_events(&matches)
                }
            }
            body if wants_matches => {
                let events = grep_fetched_document(body, re, whole_document, options)?;
                if events.is_empty() {
                    return Ok(());
                }
                events
            }
            body => {
                let matched = match body {
                    FetchedDocument::Whole(body) => {
                        let can_stream = body.is_file()
                            && bounded_len.is_some_and(|len| len <= FILE_MATCH_OVERLAP_MAX + 1);
                        if can_stream {
                            let len = body.len();
                            let mut reader = body.into_reader();
                            has_bounded_reader_match(
                                &mut reader,
                                len,
                                re,
                                bounded_len.expect("bounded length"),
                            )?
                        } else {
                            let text = body.into_bytes()?;
                            if whole_document {
                                has_line_match_fast(&text, re)
                            } else {
                                has_line_match(&text, re)
                            }
                        }
                    }
                    FetchedDocument::Regions { regions, .. } => regions.iter().any(|region| {
                        if whole_document {
                            has_line_match_fast(&region.bytes, re)
                        } else {
                            has_line_match(&region.bytes, re)
                        }
                    }),
                };
                if !matched {
                    return Ok(());
                }
                Vec::new()
            }
        };
        hit_count.fetch_add(1, Ordering::Relaxed);
        if wants_hit_keys {
            lock(&hits)?.push(key.clone());
        }
        let doc = DocResult {
            events: &events,
            bytes_searched,
            elapsed: started.elapsed(),
        };
        if sink.on_doc(key, &doc)? == SinkFlow::Stop {
            return Err(anyhow::Error::new(StopEarly));
        }
        Ok(())
    };
    let verify_caught = |re: &regex::bytes::Regex, idx: usize, body: FetchedDocument| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verify(re, idx, body)))
            .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")))
    };
    let fetch = |consume: &mut dyn FnMut(usize, FetchedDocument) -> Result<()>| {
        batch.fetch_initial(consume)
    };
    let record_fetch = |body: &FetchedDocument| -> Result<()> {
        bytes_fetched.fetch_add(usize::try_from(body.fetched_size())?, Ordering::Relaxed);
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
    let (feed_result, verify_result) = if workers == 1 {
        let mut verified = Ok(());
        let feed = fetch(&mut |idx, body| {
            record_fetch(&body)?;
            match verify_caught(re, idx, body) {
                Ok(()) => Ok(()),
                Err(error) => {
                    verified = Err(error);
                    Err(anyhow::Error::new(StopEarly))
                }
            }
        });
        (feed, verified)
    } else {
        let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, FetchedDocument)>(workers * 2);
        std::thread::scope(|scope| {
            let consumer = scope.spawn(|| {
                rx.into_iter().par_bridge().try_for_each_init(
                    || re.clone(),
                    |re, (idx, body)| verify_caught(re, idx, body),
                )
            });
            let feed = fetch(&mut |idx, body| {
                record_fetch(&body)?;
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
    let candidate_bytes = bytes_fetched.into_inner();
    Ok(BatchResult {
        hits,
        hit_count: hit_count.into_inner(),
        regional_docs: regional_docs.into_inner(),
        whole_docs: whole_docs.into_inner(),
        candidate_bytes,
        decoded_bytes: decoded_bytes.into_inner(),
        stopped,
    })
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

/// Streaming search: candidate docs are fetched concurrently, decompressed
/// and regex-verified on a worker pool, and reported to `sink` per doc as
/// they complete (unordered across docs; in-order within a doc). Memory is
/// bounded by one candidate batch, fetch concurrency, and worker count.
///
/// `scope` prunes candidates by key before anything is fetched. When the
/// sink does not want match positions, verification stops at the first
/// match per doc.
pub fn search_streaming(
    reader: &dyn IndexReader,
    pattern: &str,
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<SearchStats> {
    let total_docs = reader.total_docs();
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
            excluded_objects: reader.excluded_objects(),
        });
    }
    let hir = seagrep_core::parse_pattern(pattern)?;
    let query = seagrep_query::plan_hir(&hir, reader.strategy());
    let re = regex::bytes::Regex::new(pattern)?;
    let whole_document = can_search_as_document(&hir);
    let bounded_len = bounded_match_len(&hir);
    let extent = bounded_len
        .filter(|len| *len > 0 && *len <= seagrep_core::CANDIDATE_BLOCK_BYTES)
        .map_or(SearchExtent::Lines, |span| SearchExtent::Bytes { span });
    let plan = CandidatePlan {
        query: &query,
        extent,
    };
    let mut hits = Vec::new();
    let mut hit_count = 0usize;
    let mut candidates = 0usize;
    let mut bytes_fetched = 0usize;
    let mut regional_docs = 0usize;
    let mut whole_docs = 0usize;
    let mut decoded_bytes = 0usize;
    let visited = reader.visit_candidates(
        std::slice::from_ref(&plan),
        scope.prefix,
        CandidateBatchLimits {
            documents: SEARCH_CANDIDATE_BATCH,
            decoded_bytes: 64 * 1024 * 1024,
        },
        &mut |mut documents| {
            documents.retain(|document| scope.admits(&document.display_key));
            candidates = candidates
                .checked_add(documents.len())
                .context("candidate count overflows usize")?;
            if documents.is_empty() {
                return Ok(true);
            }
            let batch = search_batch(
                &documents,
                reader,
                &re,
                whole_document,
                bounded_len,
                options,
                sink,
            )?;
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
        excluded_objects: reader.excluded_objects(),
    })
}

/// Discards results; pairs with `SearchStats.hits` when only hit docs
/// matter. `wants_matches` is false, so the engine early-exits per doc.
pub struct NullSink;

impl MatchSink for NullSink {
    fn wants_matches(&self) -> bool {
        false
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
    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let mut collected = lock(&self.matches)?;
        collected.extend(
            doc.events
                .iter()
                .map(|event| (key.to_owned(), event.clone())),
        );
        Ok(SinkFlow::Continue)
    }
}

/// Convenience for tests and benchmarks: collect every match, globally
/// sorted by (key, line, col, text).
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
    use seagrep_core::{DocAddress, DocumentRegion, SourceEncoding, Strategy};
    use seagrep_query::Query;

    #[test]
    fn bounded_file_search_matches_in_memory_across_chunks() {
        use std::io::{Seek, Write};
        let mut bytes = vec![b'x'; FILE_MATCH_CHUNK * 2 + 17];
        let at = FILE_MATCH_CHUNK - 3;
        bytes[at..at + 6].copy_from_slice(b"needle");
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        for pattern in ["needle", "missing", "x{16}needle", "", "needle|other"] {
            file.rewind().unwrap();
            let re = regex::bytes::Regex::new(pattern).unwrap();
            let match_len =
                bounded_match_len(&seagrep_core::parse_pattern(pattern).unwrap()).unwrap();
            assert_eq!(
                has_bounded_reader_match(
                    &mut file,
                    u64::try_from(bytes.len()).unwrap(),
                    &re,
                    match_len,
                )
                .unwrap(),
                has_line_match_fast(&bytes, &re),
                "{pattern}"
            );
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
                program: seagrep_core::RegionProgram::Regional,
            },
            DocumentRegion {
                start: 1_000,
                line: 7,
                line_offset: 80,
                bytes: bytes::Bytes::from_static(b"right-needle\n"),
                program: seagrep_core::RegionProgram::Regional,
            },
        ];
        let re = regex::bytes::Regex::new("needle").unwrap();

        let matches = find_region_matches(&regions, &re, 1_013, None);

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

    struct RecordingFetcher {
        largest: AtomicUsize,
    }

    impl DocFetcher for RecordingFetcher {
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
        let fetcher = RecordingFetcher {
            largest: AtomicUsize::new(0),
        };
        let re = regex::bytes::Regex::new("needle").unwrap();
        let batch = search_batch(
            &documents,
            &fetcher,
            &re,
            true,
            Some(6),
            MatchOptions::default(),
            &NullSink,
        )
        .unwrap();
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
