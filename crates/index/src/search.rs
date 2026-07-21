//! Streaming search engine: packed snapshot fetch, parallel decompress+verify,
//! per-doc result sinks. Documents are addressed by key throughout.

use crate::{CandidateBatchLimits, CandidatePlan, IndexReader, SearchStats};
use anyhow::{Context, Result};
use rayon::iter::{ParallelBridge, ParallelIterator};
#[cfg(test)]
use seagrep_core::DocumentBody;
use seagrep_core::{
    analyze_patterns, grep_matches, CandidateBatch, DocAddress, DocFetcher, DocumentRegion,
    FallbackExtent, FetchedDocument, LineEvent, LineKind, MatchBounds, MatchOptions, MatchWitness,
    PatternCache, PatternMatch, PatternProgram, RegionProgram, RegionRead, SearchExtent, SubMatch,
    CANDIDATE_BLOCK_BYTES,
};
use std::io::Read;
use std::ops::Range;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternKind {
    Exact,
    Proof,
    Fallback,
}

struct PatternPlan {
    id: usize,
    query: seagrep_query::Query,
    bounds: MatchBounds,
    extent: SearchExtent,
    kind: PatternKind,
}

struct SearchPrograms {
    whole: PatternProgram,
    lines: Option<PatternProgram>,
    regional: Option<PatternProgram>,
}

struct WorkerCache {
    whole: PatternCache,
    lines: Option<PatternCache>,
    regional: Option<PatternCache>,
}

#[derive(Clone, Copy)]
struct SearchContext<'a> {
    plans: &'a [PatternPlan],
    programs: &'a SearchPrograms,
    stream_overlap: Option<usize>,
    options: MatchOptions,
    detail: SearchDetail,
}

enum SelectedBound {
    Exact(usize),
    Proof(usize),
}

fn build_plans(
    hirs: &[regex_syntax::hir::Hir],
    strategy: seagrep_core::Strategy,
    detail: SearchDetail,
) -> Result<Vec<PatternPlan>> {
    anyhow::ensure!(!hirs.is_empty(), "search requires at least one pattern");
    if let SearchDetail::MatchWindows { max_bytes } = detail {
        anyhow::ensure!(max_bytes > 0, "match window must be greater than 0");
    }
    Ok(hirs
        .iter()
        .zip(analyze_patterns(hirs))
        .enumerate()
        .map(|(id, (hir, bounds))| {
            let query = seagrep_query::plan_hir(hir, strategy);
            let selected = match detail {
                SearchDetail::MatchCount => bounds.exact_bytes.map(SelectedBound::Exact),
                SearchDetail::Documents
                | SearchDetail::MatchingLines
                | SearchDetail::MatchWindows { .. }
                | SearchDetail::FullLines => bounds.witness.as_ref().map(|witness| match witness {
                    MatchWitness::Exact { bytes } => SelectedBound::Exact(*bytes),
                    MatchWitness::Proven { bytes, .. } => SelectedBound::Proof(*bytes),
                }),
            };
            let finite = selected.as_ref().is_some_and(|selected| {
                let bytes = match selected {
                    SelectedBound::Exact(bytes) | SelectedBound::Proof(bytes) => *bytes,
                };
                bytes > 0 && bytes <= CANDIDATE_BLOCK_BYTES
            });
            let (extent, kind) = if query == seagrep_query::Query::All || !finite {
                let extent = match bounds.fallback {
                    FallbackExtent::Lines => SearchExtent::Lines,
                    FallbackExtent::Document => SearchExtent::Document,
                };
                (extent, PatternKind::Fallback)
            } else {
                match selected.expect("finite bound exists") {
                    SelectedBound::Exact(span) => {
                        (SearchExtent::Bytes { span }, PatternKind::Exact)
                    }
                    SelectedBound::Proof(span) => {
                        (SearchExtent::Bytes { span }, PatternKind::Proof)
                    }
                }
            };
            PatternPlan {
                id,
                query,
                bounds,
                extent,
                kind,
            }
        })
        .collect())
}

fn get_stream_overlap(plans: &[PatternPlan]) -> Option<usize> {
    let mut max_span = 0usize;
    for plan in plans {
        let SearchExtent::Bytes { span } = plan.extent else {
            return None;
        };
        max_span = max_span.max(span);
    }
    let overlap = max_span.checked_sub(1)?;
    (overlap <= FILE_MATCH_OVERLAP_MAX).then_some(overlap)
}

impl SearchPrograms {
    fn compile(hirs: &[regex_syntax::hir::Hir], plans: &[PatternPlan]) -> Result<SearchPrograms> {
        anyhow::ensure!(
            hirs.len() == plans.len(),
            "pattern HIR count {} differs from plan count {}",
            hirs.len(),
            plans.len()
        );
        for (index, plan) in plans.iter().enumerate() {
            anyhow::ensure!(
                plan.id == index,
                "pattern plan {index} carries ID {}",
                plan.id
            );
        }
        let ids = plans.iter().map(|plan| plan.id).collect::<Vec<_>>();
        let whole = PatternProgram::compile(hirs, &ids)
            .with_context(|| format!("compiling {}-pattern whole verifier", hirs.len()))?;
        let compile_subset = |include: &dyn Fn(SearchExtent) -> bool,
                              name: &str|
         -> Result<Option<PatternProgram>> {
            let selected = hirs
                .iter()
                .zip(plans)
                .filter(|(_, plan)| include(plan.extent))
                .collect::<Vec<_>>();
            if selected.is_empty() {
                return Ok(None);
            }
            let subset_hirs = selected
                .iter()
                .map(|(hir, _)| (*hir).clone())
                .collect::<Vec<_>>();
            let subset_ids = selected.iter().map(|(_, plan)| plan.id).collect::<Vec<_>>();
            PatternProgram::compile(&subset_hirs, &subset_ids)
                .with_context(|| format!("compiling {}-pattern {name} verifier", selected.len()))
                .map(Some)
        };
        let lines = compile_subset(&|extent| extent != SearchExtent::Document, "line")?;
        let regional = compile_subset(
            &|extent| matches!(extent, SearchExtent::Bytes { .. }),
            "regional",
        )?;
        Ok(SearchPrograms {
            whole,
            lines,
            regional,
        })
    }
}

impl WorkerCache {
    fn create(programs: &SearchPrograms) -> WorkerCache {
        WorkerCache {
            whole: programs.whole.create_cache(),
            lines: programs.lines.as_ref().map(PatternProgram::create_cache),
            regional: programs.regional.as_ref().map(PatternProgram::create_cache),
        }
    }
}

fn get_region_program<'a>(
    programs: &'a SearchPrograms,
    cache: &'a mut WorkerCache,
    region: RegionProgram,
) -> Result<(&'a PatternProgram, &'a mut PatternCache)> {
    match region {
        RegionProgram::Regional => Ok((
            programs
                .regional
                .as_ref()
                .context("regional verifier is missing")?,
            cache
                .regional
                .as_mut()
                .context("regional verifier cache is missing")?,
        )),
        RegionProgram::Full => Ok((
            programs
                .lines
                .as_ref()
                .context("line verifier is missing")?,
            cache
                .lines
                .as_mut()
                .context("line verifier cache is missing")?,
        )),
    }
}

fn has_stream_match(
    reader: &mut impl Read,
    len: u64,
    program: &PatternProgram,
    cache: &mut PatternCache,
    overlap: usize,
) -> Result<bool> {
    if len == 0 {
        return Ok(false);
    }
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
        if program.find_iter(cache, &chunk[..end]).next().is_some() {
            return Ok(true);
        }
        carry = end.min(overlap);
        chunk.copy_within(end - carry..end, 0);
        remaining -= u64::try_from(read)?;
    }
    Ok(false)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegionMatch {
    pattern: usize,
    witness: Range<u64>,
    line: u64,
    line_offset: u64,
    canonical_span_known: bool,
}

fn find_program_matches(
    bytes: &[u8],
    program: &PatternProgram,
    cache: &mut PatternCache,
) -> Vec<PatternMatch> {
    program.find_iter(cache, bytes).collect()
}

fn sort_region_matches(mut matches: Vec<RegionMatch>, max_count: Option<u64>) -> Vec<RegionMatch> {
    matches.sort_unstable_by_key(|matched| {
        (matched.pattern, matched.witness.start, matched.witness.end)
    });
    matches.dedup_by(|left, right| left.pattern == right.pattern && left.witness == right.witness);
    matches.sort_unstable_by_key(|matched| {
        (
            matched.line,
            matched.line_offset,
            matched.witness.start,
            matched.witness.end,
            matched.pattern,
        )
    });
    let Some(limit) = max_count else {
        return matches;
    };
    let mut lines = 0u64;
    let mut previous = None;
    let mut keep = 0usize;
    for matched in &matches {
        let current = (matched.line, matched.line_offset);
        if previous != Some(current) {
            if lines == limit {
                break;
            }
            previous = Some(current);
            lines += 1;
        }
        keep += 1;
    }
    matches.truncate(keep);
    matches
}

fn find_region_matches(
    regions: &[DocumentRegion],
    decoded_size: u64,
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    max_count: Option<u64>,
) -> Result<Vec<RegionMatch>> {
    if max_count == Some(0) {
        return Ok(Vec::new());
    }
    let mut found = Vec::new();
    for region in regions {
        let (program, program_cache) = get_region_program(programs, cache, region.program)?;
        let matches = find_program_matches(&region.bytes, program, program_cache);
        let mut scanned = 0usize;
        let mut line = region.line;
        let mut line_offset = region.line_offset;
        for matched in matches {
            let plan = &plans[matched.pattern];
            let (relative, canonical_span_known) = match region.program {
                RegionProgram::Regional => {
                    let witness = plan
                        .bounds
                        .witness
                        .as_ref()
                        .expect("regional pattern has a finite witness");
                    (
                        witness.find_witness(&region.bytes, matched)?,
                        matches!(witness, MatchWitness::Exact { .. }),
                    )
                }
                RegionProgram::Full => (matched.start..matched.end, true),
            };
            for (index, byte) in region.bytes[scanned..relative.start].iter().enumerate() {
                if *byte == b'\n' {
                    line += 1;
                    line_offset = region.start
                        + u64::try_from(scanned + index + 1).expect("document offsets fit u64");
                }
            }
            scanned = relative.start;
            let start =
                region.start + u64::try_from(relative.start).expect("document offsets fit u64");
            if start >= decoded_size {
                continue;
            }
            found.push(RegionMatch {
                pattern: matched.pattern,
                witness: start
                    ..region.start + u64::try_from(relative.end).expect("document offsets fit u64"),
                line,
                line_offset,
                canonical_span_known,
            });
        }
    }
    Ok(sort_region_matches(found, max_count))
}

fn find_whole_matches(
    bytes: &[u8],
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    max_count: Option<u64>,
) -> Vec<RegionMatch> {
    if max_count == Some(0) {
        return Vec::new();
    }
    let matches = find_program_matches(bytes, &programs.whole, &mut cache.whole);
    let mut found = Vec::with_capacity(matches.len());
    let mut scanned = 0usize;
    let mut line = 1u64;
    let mut line_offset = 0u64;
    for matched in matches {
        if matched.start > bytes.len()
            || (matched.start == bytes.len() && bytes.last().is_none_or(|byte| *byte == b'\n'))
        {
            continue;
        }
        for (index, byte) in bytes[scanned..matched.start].iter().enumerate() {
            if *byte == b'\n' {
                line += 1;
                line_offset = u64::try_from(scanned + index + 1).expect("document offsets fit u64");
            }
        }
        scanned = matched.start;
        debug_assert_eq!(plans[matched.pattern].id, matched.pattern);
        found.push(RegionMatch {
            pattern: matched.pattern,
            witness: u64::try_from(matched.start).expect("document offsets fit u64")
                ..u64::try_from(matched.end).expect("document offsets fit u64"),
            line,
            line_offset,
            canonical_span_known: true,
        });
    }
    sort_region_matches(found, max_count)
}

fn build_count_events(matches: &[RegionMatch], count_matches: bool) -> Vec<LineEvent> {
    let mut events: Vec<LineEvent> = Vec::new();
    for matched in matches {
        if let Some(event) = events.last_mut().filter(|event| event.line == matched.line) {
            if count_matches {
                debug_assert!(matched.canonical_span_known);
                event.submatches.push(SubMatch { start: 0, end: 0 });
            }
            continue;
        }
        events.push(LineEvent {
            line: matched.line,
            kind: LineKind::Match,
            offset: matched.line_offset,
            text: bytes::Bytes::new(),
            submatches: vec![SubMatch { start: 0, end: 0 }],
        });
    }
    events
}

enum OwnedMatchData {
    Documents,
    Lines(Vec<LineEvent>),
    Windows(Vec<MatchWindow>),
}

struct VerifiedDocument {
    data: OwnedMatchData,
    bytes_searched: u64,
    extra_fetched_bytes: usize,
}

fn merge_line_events(mut events: Vec<LineEvent>) -> Vec<LineEvent> {
    events.sort_by_key(|event| {
        (
            event.line,
            event.offset,
            matches!(event.kind, LineKind::Context),
        )
    });
    let mut merged: Vec<LineEvent> = Vec::new();
    for event in events {
        if let Some(previous) = merged
            .last_mut()
            .filter(|previous| previous.line == event.line)
        {
            if previous.kind == LineKind::Match && event.kind == LineKind::Match {
                previous.submatches.extend(event.submatches);
                previous.submatches.sort_by_key(|sub| (sub.start, sub.end));
                previous.submatches.dedup();
                if previous.text.is_empty() && !event.text.is_empty() {
                    previous.text = event.text;
                    previous.offset = event.offset;
                }
            } else if previous.kind == LineKind::Context && event.kind == LineKind::Match {
                *previous = event;
            }
            continue;
        }
        merged.push(event);
    }
    merged
}

fn collect_line_events(
    body: FetchedDocument,
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    options: MatchOptions,
) -> Result<Vec<LineEvent>> {
    let _ = plans;
    match body {
        FetchedDocument::Whole(body) => {
            let bytes = body.into_bytes()?;
            let matches = find_program_matches(&bytes, &programs.whole, &mut cache.whole);
            Ok(grep_matches(bytes, &matches, options))
        }
        FetchedDocument::Regions { regions, .. } => {
            let mut events = Vec::new();
            let mut matched_lines = 0u64;
            for region in regions {
                let max_count = options
                    .max_count
                    .map(|limit| limit.saturating_sub(matched_lines));
                if max_count == Some(0) {
                    break;
                }
                let (program, program_cache) = get_region_program(programs, cache, region.program)?;
                let matches = find_program_matches(&region.bytes, program, program_cache);
                let regional = MatchOptions {
                    max_count,
                    ..options
                };
                let mut found = grep_matches(region.bytes.clone(), &matches, regional);
                matched_lines += found
                    .iter()
                    .filter(|event| event.kind == LineKind::Match)
                    .count() as u64;
                for event in &mut found {
                    event.line = event
                        .line
                        .checked_add(region.line.saturating_sub(1))
                        .expect("line numbers fit u64");
                    event.offset = event
                        .offset
                        .checked_add(region.start)
                        .expect("document offsets fit u64");
                }
                events.extend(found);
            }
            Ok(merge_line_events(events))
        }
    }
}

fn fetch_full_lines(
    batch: &dyn CandidateBatch,
    document: usize,
    matches: &[RegionMatch],
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    options: MatchOptions,
) -> Result<(Vec<LineEvent>, usize)> {
    if matches.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let ranges = matches
        .iter()
        .map(|matched| {
            if matched.witness.start == matched.witness.end {
                matched.witness.start..matched.witness.start + 1
            } else {
                matched.witness.clone()
            }
        })
        .collect::<Vec<_>>();
    let lines = batch.fetch_regions(
        document,
        &ranges,
        RegionRead::Lines {
            before_context: options.before_context,
            after_context: options.after_context,
        },
    )?;
    let fetched = usize::try_from(lines.fetched_size())?;
    let events = collect_line_events(lines, plans, programs, cache, options)?;
    Ok((events, fetched))
}

fn window_clip_range(
    line_start: u64,
    line_end: u64,
    witness: &Range<u64>,
    max_bytes: usize,
) -> Result<(u64, u64)> {
    let max_bytes = u64::try_from(max_bytes).context("match window range overflows")?;
    anyhow::ensure!(max_bytes > 0, "match window must be greater than 0");
    let witness_len = witness
        .end
        .checked_sub(witness.start)
        .context("match window range overflows")?;
    let (start, end) = if witness_len <= max_bytes {
        let spare = max_bytes - witness_len;
        let left = spare / 2;
        let right = spare - left;
        let mut start = witness.start.saturating_sub(left).max(line_start);
        let mut end = witness.end.saturating_add(right).min(line_end);
        let used = end
            .checked_sub(start)
            .context("match window range overflows")?;
        if used < max_bytes {
            let extra = max_bytes - used;
            if start > line_start {
                let shift = extra.min(start - line_start);
                start -= shift;
            }
            let used = end
                .checked_sub(start)
                .context("match window range overflows")?;
            if used < max_bytes && end < line_end {
                end = end
                    .checked_add((max_bytes - used).min(line_end - end))
                    .context("match window range overflows")?;
            }
        }
        (start, end)
    } else {
        let end = witness
            .start
            .checked_add(max_bytes)
            .context("match window range overflows")?
            .min(line_end);
        (witness.start, end)
    };
    Ok((start, end))
}

fn build_windows(
    batch: &dyn CandidateBatch,
    document: usize,
    matches: &[RegionMatch],
    decoded_size: u64,
    max_bytes: usize,
    whole: Option<&bytes::Bytes>,
) -> Result<Vec<MatchWindow>> {
    anyhow::ensure!(max_bytes > 0, "match window must be greater than 0");
    if let Some(bytes) = whole {
        anyhow::ensure!(
            u64::try_from(bytes.len()).context("match window range overflows")? == decoded_size,
            "whole document length {} differs from decoded size {decoded_size}",
            bytes.len()
        );
    }
    let mut windows = Vec::new();
    let mut index = 0usize;
    while index < matches.len() {
        let line = matches[index].line;
        let line_offset = matches[index].line_offset;
        let mut end = index + 1;
        while end < matches.len() && matches[end].line == line {
            end += 1;
        }
        let line_matches = &matches[index..end];
        let anchor = line_matches
            .iter()
            .map(|matched| matched.witness.start)
            .min()
            .expect("line group is non-empty");
        let (text, window_offset, line_end) = if let Some(bytes) = whole {
            let line_start =
                usize::try_from(line_offset).context("match window range overflows")?;
            let absolute_end = bytes[line_start..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map(|relative| line_start + relative + 1)
                .unwrap_or(bytes.len());
            let line_end = u64::try_from(absolute_end).context("match window range overflows")?;
            let lowest = line_matches
                .iter()
                .map(|matched| matched.witness.clone())
                .min_by_key(|witness| (witness.start, witness.end))
                .expect("line group is non-empty");
            let (start, end) = window_clip_range(line_offset, line_end, &lowest, max_bytes)?;
            let start_usize = usize::try_from(start).context("match window range overflows")?;
            let end_usize = usize::try_from(end).context("match window range overflows")?;
            (bytes.slice(start_usize..end_usize), start, line_end)
        } else {
            let before = u64::try_from(max_bytes).context("match window range overflows")?;
            let seed_start = anchor.saturating_sub(before);
            let anchor_bytes = line_matches
                .iter()
                .map(|matched| matched.witness.end.saturating_sub(matched.witness.start))
                .max()
                .unwrap_or(0)
                .max(1)
                .min(before);
            let seed_end = anchor
                .checked_add(anchor_bytes)
                .context("match window range overflows")?
                .checked_add(before)
                .context("match window range overflows")?
                .min(decoded_size);
            let seed = seed_start..seed_end;
            let fetched =
                batch.fetch_regions(document, std::slice::from_ref(&seed), RegionRead::Bytes)?;
            let FetchedDocument::Regions { regions, .. } = fetched else {
                anyhow::bail!("candidate region fetch returned no document");
            };
            let region = regions
                .into_iter()
                .next()
                .context("candidate region fetch returned no document")?;
            let relative_line = usize::try_from(line_offset.saturating_sub(region.start))
                .context("match window range overflows")?;
            let line_end = match region.bytes[relative_line.min(region.bytes.len())..]
                .iter()
                .position(|byte| *byte == b'\n')
            {
                Some(relative) => region
                    .start
                    .checked_add(
                        u64::try_from(relative_line + relative + 1)
                            .context("match window range overflows")?,
                    )
                    .context("match window range overflows")?,
                None => decoded_size,
            };
            let lowest = line_matches
                .iter()
                .map(|matched| matched.witness.clone())
                .min_by_key(|witness| (witness.start, witness.end))
                .expect("line group is non-empty");
            let (start, end) = window_clip_range(line_offset, line_end, &lowest, max_bytes)?;
            let clip_start = start.max(region.start);
            let clip_end = end.min(
                region
                    .start
                    .checked_add(
                        u64::try_from(region.bytes.len())
                            .context("match window range overflows")?,
                    )
                    .context("match window range overflows")?,
            );
            let start_usize = usize::try_from(
                clip_start
                    .checked_sub(region.start)
                    .context("match window range overflows")?,
            )
            .context("match window range overflows")?;
            let end_usize = usize::try_from(
                clip_end
                    .checked_sub(region.start)
                    .context("match window range overflows")?,
            )
            .context("match window range overflows")?;
            (
                region.bytes.slice(start_usize..end_usize),
                clip_start,
                line_end,
            )
        };
        let window_end = window_offset
            .checked_add(u64::try_from(text.len()).context("match window range overflows")?)
            .context("match window range overflows")?;
        let visible_matches = line_matches
            .iter()
            .map(|matched| {
                let visible_start = matched
                    .witness
                    .start
                    .saturating_sub(window_offset)
                    .min(u64::try_from(text.len()).expect("text length fits u64"));
                let visible_end = matched
                    .witness
                    .end
                    .saturating_sub(window_offset)
                    .min(u64::try_from(text.len()).expect("text length fits u64"));
                WindowMatch {
                    witness: matched.witness.clone(),
                    visible: usize::try_from(visible_start).expect("offsets fit usize")
                        ..usize::try_from(visible_end).expect("offsets fit usize"),
                    left_clipped: matched.witness.start < window_offset,
                    right_clipped: matched.witness.end > window_end,
                    canonical_span_known: matched.canonical_span_known,
                }
            })
            .collect();
        windows.push(MatchWindow {
            line,
            line_offset,
            window_offset,
            text,
            matches: visible_matches,
            left_clipped: window_offset > line_offset,
            right_clipped: window_end < line_end,
        });
        index = end;
    }
    Ok(windows)
}

fn verify_document(
    batch: &dyn CandidateBatch,
    document: usize,
    body: FetchedDocument,
    context: SearchContext<'_>,
    cache: &mut WorkerCache,
) -> Result<Option<VerifiedDocument>> {
    let bytes_searched = body.decoded_size();
    match body {
        FetchedDocument::Whole(body) => {
            if context.detail == SearchDetail::Documents {
                if let Some(overlap) = context.stream_overlap.filter(|_| body.is_file()) {
                    let len = body.len();
                    let mut reader = body.into_reader();
                    let matched = has_stream_match(
                        &mut reader,
                        len,
                        &context.programs.whole,
                        &mut cache.whole,
                        overlap,
                    )?;
                    return Ok(matched.then_some(VerifiedDocument {
                        data: OwnedMatchData::Documents,
                        bytes_searched,
                        extra_fetched_bytes: 0,
                    }));
                }
            }
            let bytes = body.into_bytes()?;
            let matches = find_whole_matches(
                &bytes,
                context.plans,
                context.programs,
                cache,
                context.options.max_count,
            );
            if matches.is_empty() {
                return Ok(None);
            }
            let data = match context.detail {
                SearchDetail::Documents => OwnedMatchData::Documents,
                SearchDetail::MatchingLines => {
                    OwnedMatchData::Lines(build_count_events(&matches, false))
                }
                SearchDetail::MatchCount => {
                    OwnedMatchData::Lines(build_count_events(&matches, true))
                }
                SearchDetail::FullLines => {
                    let pattern_matches = matches
                        .iter()
                        .map(|matched| PatternMatch {
                            pattern: matched.pattern,
                            start: usize::try_from(matched.witness.start)
                                .expect("document offsets fit usize"),
                            end: usize::try_from(matched.witness.end)
                                .expect("document offsets fit usize"),
                        })
                        .collect::<Vec<_>>();
                    OwnedMatchData::Lines(grep_matches(
                        bytes.clone(),
                        &pattern_matches,
                        context.options,
                    ))
                }
                SearchDetail::MatchWindows { max_bytes } => OwnedMatchData::Windows(build_windows(
                    batch,
                    document,
                    &matches,
                    bytes_searched,
                    max_bytes,
                    Some(&bytes),
                )?),
            };
            Ok(Some(VerifiedDocument {
                data,
                bytes_searched,
                extra_fetched_bytes: 0,
            }))
        }
        FetchedDocument::Regions {
            decoded_size,
            regions,
        } => {
            let matches = find_region_matches(
                &regions,
                decoded_size,
                context.plans,
                context.programs,
                cache,
                context.options.max_count,
            )?;
            if matches.is_empty() {
                return Ok(None);
            }
            match context.detail {
                SearchDetail::Documents => Ok(Some(VerifiedDocument {
                    data: OwnedMatchData::Documents,
                    bytes_searched,
                    extra_fetched_bytes: 0,
                })),
                SearchDetail::MatchingLines => Ok(Some(VerifiedDocument {
                    data: OwnedMatchData::Lines(build_count_events(&matches, false)),
                    bytes_searched,
                    extra_fetched_bytes: 0,
                })),
                SearchDetail::MatchCount => Ok(Some(VerifiedDocument {
                    data: OwnedMatchData::Lines(build_count_events(&matches, true)),
                    bytes_searched,
                    extra_fetched_bytes: 0,
                })),
                SearchDetail::FullLines => {
                    let (events, extra_fetched_bytes) = fetch_full_lines(
                        batch,
                        document,
                        &matches,
                        context.plans,
                        context.programs,
                        cache,
                        context.options,
                    )?;
                    if events.is_empty() {
                        return Ok(None);
                    }
                    Ok(Some(VerifiedDocument {
                        data: OwnedMatchData::Lines(events),
                        bytes_searched,
                        extra_fetched_bytes,
                    }))
                }
                SearchDetail::MatchWindows { max_bytes } => {
                    let windows =
                        build_windows(batch, document, &matches, decoded_size, max_bytes, None)?;
                    let extra_fetched_bytes = windows.iter().map(|window| window.text.len()).sum();
                    Ok(Some(VerifiedDocument {
                        data: OwnedMatchData::Windows(windows),
                        bytes_searched,
                        extra_fetched_bytes,
                    }))
                }
            }
        }
    }
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
    let hits: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let wants_hit_keys = sink.wants_hit_keys();
    let documents_ref = documents;
    let verify = |cache: &mut WorkerCache, idx: usize, body: FetchedDocument| -> Result<()> {
        let key = &documents_ref[idx].display_key;
        let started = std::time::Instant::now();
        let Some(verified) = verify_document(batch.as_ref(), idx, body, context, cache)? else {
            return Ok(());
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
        Ok(())
    };
    let verify_caught = |cache: &mut WorkerCache, idx: usize, body: FetchedDocument| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| verify(cache, idx, body)))
            .unwrap_or_else(|_| Err(anyhow::anyhow!("a search worker panicked")))
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
    let (feed_result, verify_result) = if jobs == 1 {
        let mut cache = WorkerCache::create(context.programs);
        let mut verified = Ok(());
        let feed = batch.fetch_initial(&mut |idx, body| {
            record_fetch(&body)?;
            match verify_caught(&mut cache, idx, body) {
                Ok(()) => Ok(()),
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
                    |cache, (idx, body)| verify_caught(cache, idx, body),
                )
            });
            let feed = batch.fetch_initial(&mut |idx, body| {
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

fn pattern_kind_counts(plans: &[PatternPlan]) -> (usize, usize, usize, usize) {
    let mut exact = 0usize;
    let mut proof = 0usize;
    let mut fallback = 0usize;
    for plan in plans {
        match plan.kind {
            PatternKind::Exact => exact += 1,
            PatternKind::Proof => proof += 1,
            PatternKind::Fallback => fallback += 1,
        }
    }
    (plans.len(), exact, proof, fallback)
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
    let (patterns, exact_patterns, proof_patterns, fallback_patterns) = pattern_kind_counts(&plans);
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
    let visited = reader.visit_candidates(
        &candidate_plans,
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
    use seagrep_core::{DocAddress, DocumentRegion, SourceEncoding, Strategy};
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
