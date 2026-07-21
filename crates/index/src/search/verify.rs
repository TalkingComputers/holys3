use super::{
    get_region_program, MatchWindow, PatternPlan, SearchContext, SearchDetail, SearchPrograms,
    WindowMatch, WorkerCache, FILE_MATCH_CHUNK, FILE_MATCH_OVERLAP_MAX,
};
use anyhow::{Context, Result};
use seagrep_core::{
    grep_matches, CandidateBatch, DocumentRegion, FetchedDocument, LineEvent, LineKind,
    MatchOptions, MatchWitness, PatternCache, PatternMatch, PatternProgram, RegionProgram,
    RegionRead, SubMatch,
};
use std::io::Read;
use std::ops::Range;

pub(super) fn has_stream_match(
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
pub(super) struct RegionMatch {
    pub(super) pattern: usize,
    pub(super) witness: Range<u64>,
    pub(super) line: u64,
    pub(super) line_offset: u64,
    pub(super) canonical_span_known: bool,
}

pub(super) fn find_program_matches(
    bytes: &[u8],
    program: &PatternProgram,
    cache: &mut PatternCache,
) -> Vec<PatternMatch> {
    program.find_iter(cache, bytes).collect()
}

pub(super) fn sort_region_matches(
    mut matches: Vec<RegionMatch>,
    max_count: Option<u64>,
) -> Vec<RegionMatch> {
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

pub(super) fn find_region_matches(
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

pub(super) fn find_whole_matches(
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

pub(super) fn build_count_events(matches: &[RegionMatch], count_matches: bool) -> Vec<LineEvent> {
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

pub(super) enum OwnedMatchData {
    Documents,
    Lines(Vec<LineEvent>),
    Windows(Vec<MatchWindow>),
}

pub(super) struct VerifiedDocument {
    pub(super) data: OwnedMatchData,
    pub(super) bytes_searched: u64,
    pub(super) extra_fetched_bytes: usize,
}

pub(super) fn merge_line_events(mut events: Vec<LineEvent>) -> Vec<LineEvent> {
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

pub(super) fn collect_line_events(
    body: FetchedDocument,
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    options: MatchOptions,
) -> Result<Vec<LineEvent>> {
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

pub(super) fn fetch_full_lines(
    batch: &dyn CandidateBatch,
    document: usize,
    matches: &[RegionMatch],
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
    let events = collect_line_events(lines, programs, cache, options)?;
    Ok((events, fetched))
}

pub(super) fn window_clip_range(
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

pub(super) fn build_windows(
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
        index = end;
        let lowest = line_matches
            .iter()
            .map(|matched| matched.witness.clone())
            .min_by_key(|witness| (witness.start, witness.end))
            .expect("line group is non-empty");
        let (source, source_offset) = match whole {
            Some(bytes) => (bytes.clone(), 0u64),
            None => {
                let budget = u64::try_from(max_bytes).context("match window range overflows")?;
                let seed_start = lowest.start.saturating_sub(budget);
                let anchor_bytes = line_matches
                    .iter()
                    .map(|matched| matched.witness.end.saturating_sub(matched.witness.start))
                    .max()
                    .unwrap_or(0)
                    .max(1)
                    .min(budget);
                let seed_end = lowest
                    .start
                    .checked_add(anchor_bytes)
                    .and_then(|end| end.checked_add(budget))
                    .context("match window range overflows")?
                    .min(decoded_size);
                let seed = seed_start..seed_end;
                let fetched = batch.fetch_regions(
                    document,
                    std::slice::from_ref(&seed),
                    RegionRead::Bytes,
                )?;
                let FetchedDocument::Regions { regions, .. } = fetched else {
                    anyhow::bail!("candidate region fetch returned no document");
                };
                let region = regions
                    .into_iter()
                    .next()
                    .context("candidate region fetch returned no document")?;
                (region.bytes, region.start)
            }
        };
        let source_end = source_offset
            .checked_add(u64::try_from(source.len()).context("match window range overflows")?)
            .context("match window range overflows")?;
        let line_relative = usize::try_from(line_offset.saturating_sub(source_offset))
            .context("match window range overflows")?;
        let line_end = match source[line_relative.min(source.len())..]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            Some(relative) => source_offset
                .checked_add(
                    u64::try_from(line_relative + relative + 1)
                        .context("match window range overflows")?,
                )
                .context("match window range overflows")?,
            None => decoded_size,
        };
        let (start, end) = window_clip_range(line_offset, line_end, &lowest, max_bytes)?;
        let window_offset = start.max(source_offset);
        let window_end = end.min(source_end);
        let text = source.slice(
            usize::try_from(window_offset - source_offset)
                .context("match window range overflows")?
                ..usize::try_from(window_end - source_offset)
                    .context("match window range overflows")?,
        );
        let text_len = u64::try_from(text.len()).expect("text length fits u64");
        let visible_matches = line_matches
            .iter()
            .filter(|matched| {
                if matched.witness.start == matched.witness.end {
                    matched.witness.start >= window_offset && matched.witness.start <= window_end
                } else {
                    matched.witness.start < window_end && matched.witness.end > window_offset
                }
            })
            .map(|matched| {
                let visible_start = matched
                    .witness
                    .start
                    .saturating_sub(window_offset)
                    .min(text_len);
                let visible_end = matched
                    .witness
                    .end
                    .saturating_sub(window_offset)
                    .min(text_len);
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
    }
    Ok(windows)
}

pub(super) fn verify_document(
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
            let (data, extra_fetched_bytes) = match context.detail {
                SearchDetail::Documents => (OwnedMatchData::Documents, 0),
                SearchDetail::MatchingLines => (
                    OwnedMatchData::Lines(build_count_events(&matches, false)),
                    0,
                ),
                SearchDetail::MatchCount => {
                    (OwnedMatchData::Lines(build_count_events(&matches, true)), 0)
                }
                SearchDetail::FullLines => {
                    let (events, fetched) = fetch_full_lines(
                        batch,
                        document,
                        &matches,
                        context.programs,
                        cache,
                        context.options,
                    )?;
                    if events.is_empty() {
                        return Ok(None);
                    }
                    (OwnedMatchData::Lines(events), fetched)
                }
                SearchDetail::MatchWindows { max_bytes } => {
                    let windows =
                        build_windows(batch, document, &matches, decoded_size, max_bytes, None)?;
                    let fetched = windows.iter().map(|window| window.text.len()).sum();
                    (OwnedMatchData::Windows(windows), fetched)
                }
            };
            Ok(Some(VerifiedDocument {
                data,
                bytes_searched,
                extra_fetched_bytes,
            }))
        }
    }
}
