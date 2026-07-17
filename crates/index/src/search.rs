//! Streaming search engine: packed snapshot fetch, parallel decompress+verify,
//! per-doc result sinks. Documents are addressed by key throughout.

use crate::{IndexReader, SearchStats};
use anyhow::{Context, Result};
use rayon::iter::{ParallelBridge, ParallelIterator};
use seagrep_core::{
    bounded_match_len, can_search_as_document, grep_bytes, grep_bytes_fast, has_line_match,
    has_line_match_fast, DocAddress, DocFetcher, DocumentBody, LineEvent, MatchOptions,
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

fn search_batch(
    documents: &[DocAddress],
    fetcher: &dyn DocFetcher,
    re: &regex::bytes::Regex,
    whole_document: bool,
    bounded_len: Option<usize>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<(Vec<String>, usize, bool)> {
    let workers = std::thread::available_parallelism()?
        .get()
        .min(documents.len());

    let bytes_fetched = AtomicUsize::new(0);
    let hits: Mutex<Vec<String>> = Mutex::new(Vec::new());

    let wants_matches = sink.wants_matches();
    let documents_ref = documents;
    // `re` is cloned per rayon split: the meta engine's shared scratch Cache
    // contends under exactly this all-threads-search workload.
    let verify = |re: &regex::bytes::Regex, idx: usize, body: DocumentBody| -> Result<()> {
        let key = &documents_ref[idx].display_key;
        let started = std::time::Instant::now();
        let bytes_searched = body.len();
        let events = if wants_matches {
            let text = body.into_bytes()?;
            let events = if whole_document {
                grep_bytes_fast(text.clone(), re, options)
            } else {
                grep_bytes(text.clone(), re, options)
            };
            if events.is_empty() {
                return Ok(());
            }
            events
        } else {
            let can_stream =
                body.is_file() && bounded_len.is_some_and(|len| len <= FILE_MATCH_OVERLAP_MAX + 1);
            let matched = if can_stream {
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
            };
            if !matched {
                return Ok(());
            }
            Vec::new()
        };
        lock(&hits)?.push(key.clone());
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

    let verify_caught = |re: &regex::bytes::Regex, idx: usize, body: DocumentBody| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verify(re, idx, body)))
            .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")))
    };

    let (feed_result, verify_result) = if workers == 1 {
        let mut verified = Ok(());
        let feed = fetcher.fetch_each(documents_ref, &mut |idx, body| {
            bytes_fetched.fetch_add(usize::try_from(body.len())?, Ordering::Relaxed);
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
        let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, DocumentBody)>(workers * 2);
        // One consumer thread drives the rayon pool over the channel. When any
        // doc errors, the bridge drops `rx`, so the feeder cannot deadlock.
        std::thread::scope(|scope| {
            let consumer = scope.spawn(|| {
                rx.into_iter().par_bridge().try_for_each_init(
                    || re.clone(),
                    |re, (idx, bytes)| verify_caught(re, idx, bytes),
                )
            });
            let feed = fetcher.fetch_each(documents_ref, &mut |idx, body| {
                bytes_fetched.fetch_add(usize::try_from(body.len())?, Ordering::Relaxed);
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
            // A concurrent fetch or decode failure must still fail the
            // search; only the send-into-closed-channel sentinel is the
            // expected side effect of stopping early.
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
    Ok((hits, bytes_fetched.into_inner(), stopped))
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
            candidates: 0,
            total_docs,
            bytes_fetched: 0,
        });
    }
    let query = seagrep_query::plan(pattern, reader.strategy())?;
    let re = regex::bytes::Regex::new(pattern)?;
    let whole_document = can_search_as_document(pattern)?;
    let bounded_len = bounded_match_len(pattern)?;
    let mut hits = Vec::new();
    let mut candidates = 0usize;
    let mut bytes_fetched = 0usize;
    let visited = reader.visit_candidates(
        &query,
        scope.prefix,
        SEARCH_CANDIDATE_BATCH,
        &mut |mut documents| {
            documents.retain(|document| scope.admits(&document.display_key));
            candidates = candidates
                .checked_add(documents.len())
                .context("candidate count overflows usize")?;
            if documents.is_empty() {
                return Ok(true);
            }
            let (batch_hits, batch_bytes, stopped) = search_batch(
                &documents,
                reader,
                &re,
                whole_document,
                bounded_len,
                options,
                sink,
            )?;
            hits.extend(batch_hits);
            bytes_fetched = bytes_fetched
                .checked_add(batch_bytes)
                .context("fetched byte count overflows usize")?;
            Ok(!stopped)
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
        candidates,
        total_docs,
        bytes_fetched,
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
    use seagrep_core::{DocAddress, SourceEncoding, Strategy};
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
            let match_len = bounded_match_len(pattern).unwrap().unwrap();
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
            _query: &Query,
            _key_prefix: Option<&str>,
            _batch_size: usize,
            visit: &mut dyn FnMut(Vec<DocAddress>) -> Result<bool>,
        ) -> Result<()> {
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
        let (hits, bytes, stopped) = search_batch(
            &documents,
            &fetcher,
            &re,
            true,
            Some(6),
            MatchOptions::default(),
            &NullSink,
        )
        .unwrap();
        assert_eq!(hits, ["doc"]);
        assert_eq!(bytes, 7);
        assert!(!stopped);
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
            _query: &Query,
            _key_prefix: Option<&str>,
            _batch_size: usize,
            visit: &mut dyn FnMut(Vec<DocAddress>) -> Result<bool>,
        ) -> Result<()> {
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
