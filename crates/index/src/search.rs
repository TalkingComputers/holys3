//! Streaming search engine: concurrent fetch, parallel decompress+verify,
//! per-doc result sinks. Documents are addressed by key throughout.

use crate::{IndexReader, SearchStats};
use anyhow::Result;
use holys3_core::{decode_body, matches_in, DocFetcher, Match};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

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

/// Receives verified results per doc, possibly from several threads at once.
pub trait MatchSink: Sync {
    /// Whether this sink uses match positions. Returning false lets the
    /// engine stop at the first match per doc (files-only behavior); `on_doc`
    /// is then called with empty `matches`.
    fn wants_matches(&self) -> bool {
        true
    }

    /// Called once per doc with at least one verified match.
    fn on_doc(&self, key: &str, matches: &[Match]) -> Result<SinkFlow>;
}

/// Sentinel threaded through `fetch_each` to unwind an early stop.
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

/// Streaming search: candidate docs are fetched concurrently, decompressed
/// and regex-verified on a worker pool, and reported to `sink` per doc as
/// they complete (unordered across docs; in-order within a doc). Memory is
/// bounded by fetch concurrency + worker count, not corpus size.
///
/// `scope` prunes candidates by key before anything is fetched. When the
/// sink does not want match positions, verification stops at the first
/// match per doc.
pub fn search_streaming(
    reader: &dyn IndexReader,
    fetcher: &dyn DocFetcher,
    pattern: &str,
    scope: KeyScope<'_>,
    sink: &dyn MatchSink,
) -> Result<SearchStats> {
    let q = holys3_query::plan(pattern, reader.strategy())?;
    let mut keys = reader.candidate_keys(&q, scope.prefix)?;
    keys.retain(|key| scope.admits(key));
    let candidates = keys.len();
    let re = regex::bytes::Regex::new(pattern)?;
    if keys.is_empty() {
        return Ok(SearchStats {
            hits: Vec::new(),
            candidates,
            total_docs: reader.total_docs(),
            bytes_fetched: 0,
        });
    }
    let workers = std::thread::available_parallelism()?.get().min(keys.len());

    let bytes_fetched = AtomicUsize::new(0);
    let stopped = AtomicBool::new(false);
    let hits: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let worker_error: Mutex<Option<anyhow::Error>> = Mutex::new(None);
    let (tx, rx) = std::sync::mpsc::sync_channel::<(usize, Vec<u8>)>(workers * 2);
    let rx = Mutex::new(rx);

    // Workers never exit while the channel is open: on error they record it,
    // flip `stopped`, and keep draining, so the feeder's blocking send always
    // has a consumer (otherwise an all-workers-failed state would deadlock
    // the feeder).
    let record_error = |err: anyhow::Error| {
        if let Ok(mut slot) = worker_error.lock() {
            slot.get_or_insert(err);
        }
        stopped.store(true, Ordering::Relaxed);
    };
    let wants_matches = sink.wants_matches();
    let keys_ref = &keys;
    let verify = |idx: usize, bytes: Vec<u8>| -> Result<()> {
        let key = &keys_ref[idx];
        let text = match decode_body(key, bytes) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("warning: {err:#}; skipping");
                return Ok(());
            }
        };
        let matches = if wants_matches {
            let matches = matches_in(&text, &re);
            if matches.is_empty() {
                return Ok(());
            }
            matches
        } else {
            if !re.is_match(&text) {
                return Ok(());
            }
            Vec::new()
        };
        lock(&hits)?.push(key.clone());
        if sink.on_doc(key, &matches)? == SinkFlow::Stop {
            stopped.store(true, Ordering::Relaxed);
        }
        Ok(())
    };

    let feed_result = std::thread::scope(|scope| -> Result<()> {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let received = match lock(&rx) {
                    Ok(guard) => guard.recv(),
                    Err(err) => {
                        record_error(err);
                        return;
                    }
                };
                let Ok((idx, bytes)) = received else {
                    return;
                };
                if stopped.load(Ordering::Relaxed) {
                    continue;
                }
                // catch_unwind keeps a panicking verify (or sink) from
                // breaking the drain invariant — and from poisoning the rx
                // mutex, since the panic never crosses the recv lock.
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verify(idx, bytes)))
                {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => record_error(err),
                    Err(_) => record_error(anyhow::anyhow!("a search worker panicked")),
                }
            });
        }
        let feed = fetcher.fetch_each(keys_ref, &mut |idx, bytes| {
            if stopped.load(Ordering::Relaxed) {
                return Err(anyhow::Error::new(StopEarly));
            }
            bytes_fetched.fetch_add(bytes.len(), Ordering::Relaxed);
            tx.send((idx, bytes))
                .map_err(|_| anyhow::anyhow!("search workers exited early"))
        });
        drop(tx);
        feed
    });

    if let Some(err) = lock(&worker_error)?.take() {
        return Err(err);
    }
    match feed_result {
        Err(err) if err.is::<StopEarly>() => {}
        other => other?,
    }

    let mut hits = hits
        .into_inner()
        .map_err(|_| anyhow::anyhow!("a search worker panicked"))?;
    hits.sort_unstable();
    Ok(SearchStats {
        hits,
        candidates,
        total_docs: reader.total_docs(),
        bytes_fetched: bytes_fetched.into_inner(),
    })
}

/// Discards results; pairs with `SearchStats.hits` when only hit docs
/// matter. `wants_matches` is false, so the engine early-exits per doc.
pub struct NullSink;

impl MatchSink for NullSink {
    fn wants_matches(&self) -> bool {
        false
    }

    fn on_doc(&self, _key: &str, _matches: &[Match]) -> Result<SinkFlow> {
        Ok(SinkFlow::Continue)
    }
}

#[derive(Default)]
struct CollectSink {
    matches: Mutex<Vec<(String, Match)>>,
}

impl MatchSink for CollectSink {
    fn on_doc(&self, key: &str, matches: &[Match]) -> Result<SinkFlow> {
        let mut collected = lock(&self.matches)?;
        collected.extend(
            matches
                .iter()
                .map(|matched| (key.to_owned(), matched.clone())),
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
) -> Result<(Vec<(String, Match)>, SearchStats)> {
    let sink = CollectSink::default();
    let stats = search_streaming(reader, fetcher, pattern, KeyScope::default(), &sink)?;
    let mut matches = sink
        .matches
        .into_inner()
        .map_err(|_| anyhow::anyhow!("a search worker panicked"))?;
    matches.sort_by(|(a_key, a), (b_key, b)| {
        (a_key, a.line, a.col, a.text.as_str()).cmp(&(b_key, b.line, b.col, b.text.as_str()))
    });
    Ok((matches, stats))
}
