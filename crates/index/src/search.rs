//! Streaming search engine: concurrent fetch, parallel decompress+verify,
//! per-doc result sinks. Documents are addressed by key throughout.

use crate::{IndexReader, SearchStats};
use anyhow::{Context, Result};
use bytes::Bytes;
use holys3_core::{
    can_search_as_document, grep_bytes, grep_bytes_fast, has_line_match, has_line_match_fast,
    DocAddress, DocFetcher, LineEvent, MatchOptions,
};
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

const SEARCH_CANDIDATE_BATCH: usize = 16_384;

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

fn search_batch(
    documents: &[DocAddress],
    fetcher: &dyn DocFetcher,
    re: &regex::bytes::Regex,
    whole_document: bool,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> Result<(Vec<String>, usize, bool)> {
    let workers = std::thread::available_parallelism()?
        .get()
        .min(documents.len());

    let bytes_fetched = AtomicUsize::new(0);
    let hits: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, Bytes)>(workers * 2);

    let wants_matches = sink.wants_matches();
    let documents_ref = documents;
    // `re` is cloned per rayon split: the meta engine's shared scratch Cache
    // contends under exactly this all-threads-search workload.
    let verify = |re: &regex::bytes::Regex, idx: usize, text: Bytes| -> Result<()> {
        let key = &documents_ref[idx].display_key;
        let started = std::time::Instant::now();
        let events = if wants_matches {
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
            // line semantics, same as grep_doc: no lines in an empty doc
            let matched = if whole_document {
                has_line_match_fast(&text, re)
            } else {
                has_line_match(&text, re)
            };
            if !matched {
                return Ok(());
            }
            Vec::new()
        };
        lock(&hits)?.push(key.clone());
        let doc = DocResult {
            events: &events,
            bytes_searched: text.len() as u64,
            elapsed: started.elapsed(),
        };
        if sink.on_doc(key, &doc)? == SinkFlow::Stop {
            return Err(anyhow::Error::new(StopEarly));
        }
        Ok(())
    };

    // One consumer thread drives the rayon pool over the channel. When any
    // doc errors (Stop, sink error, panic), try_for_each short-circuits,
    // the bridge drops `rx`, and the feeder's next blocking send fails —
    // so the feeder can never deadlock against dead consumers.
    let (feed_result, verify_result) = std::thread::scope(|scope| {
        let consumer = scope.spawn(|| {
            rx.into_iter().par_bridge().try_for_each_init(
                || re.clone(),
                |re, (idx, bytes)| {
                    // map panics to errors so rayon short-circuits: queued
                    // docs are discarded, not handed to a broken sink
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        verify(re, idx, bytes)
                    }))
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")))
                },
            )
        });
        let feed = fetcher.fetch_each(documents_ref, &mut |idx, bytes| {
            bytes_fetched.fetch_add(bytes.len(), Ordering::Relaxed);
            tx.send((idx, bytes))
                .map_err(|_| anyhow::anyhow!("search workers exited early"))
        });
        drop(tx);
        let verified = consumer
            .join()
            .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")));
        (feed, verified)
    });

    let stopped = match verify_result {
        Err(err) if err.is::<StopEarly>() => true,
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
    fetcher: &dyn DocFetcher,
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
    let query = holys3_query::plan(pattern, reader.strategy())?;
    let re = regex::bytes::Regex::new(pattern)?;
    let whole_document = can_search_as_document(pattern)?;
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
            let (batch_hits, batch_bytes, stopped) =
                search_batch(&documents, fetcher, &re, whole_document, options, sink)?;
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
    fetcher: &dyn DocFetcher,
    pattern: &str,
) -> Result<(Vec<(String, LineEvent)>, SearchStats)> {
    let sink = CollectSink::default();
    let stats = search_streaming(
        reader,
        fetcher,
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
    use holys3_core::{DocAddress, SourceEncoding, Strategy};
    use holys3_query::Query;

    struct BatchReader {
        documents: Vec<DocAddress>,
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
            consume: &mut dyn FnMut(usize, Bytes) -> Result<()>,
        ) -> Result<()> {
            self.largest.fetch_max(documents.len(), Ordering::Relaxed);
            for index in 0..documents.len() {
                consume(index, Bytes::from_static(b"needle\n"))?;
            }
            Ok(())
        }
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
                })
                .collect(),
        };
        let fetcher = RecordingFetcher {
            largest: AtomicUsize::new(0),
        };
        let stats = search_streaming(
            &reader,
            &fetcher,
            "needle",
            KeyScope::default(),
            MatchOptions::default(),
            &NullSink,
        )
        .unwrap();
        assert_eq!(stats.hits.len(), 5);
        assert_eq!(fetcher.largest.load(Ordering::Relaxed), 2);
    }

    struct ChangingReader {
        document: DocAddress,
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
            },
        };
        let fetcher = RecordingFetcher {
            largest: AtomicUsize::new(0),
        };
        let error = search_streaming(
            &reader,
            &fetcher,
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
