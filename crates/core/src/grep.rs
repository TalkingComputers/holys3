use std::collections::VecDeque;

use crate::{PatternCache, PatternMatch, PatternProgram};

/// One occurrence within a line: byte offsets into `LineEvent.text`,
/// half-open, clamped to the line's content (pre-`\n`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubMatch {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineKind {
    Match,
    Context,
}

/// One output line of a search: a matching line or a context line around
/// one. The owning object's key travels alongside, not inside.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineEvent {
    /// 1-based line number.
    pub line: u64,
    pub kind: LineKind,
    /// Byte offset of the line start in the decoded doc.
    pub offset: u64,
    /// Exact line bytes INCLUDING the trailing `\n` when present.
    pub text: bytes::Bytes,
    /// Ordered by start; non-empty for Match. A Context line past a
    /// `max_count` cap can also carry submatches (rg behavior).
    pub submatches: Vec<SubMatch>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MatchOptions {
    pub before_context: usize,
    pub after_context: usize,
    /// Cap on MATCHING lines per doc (`rg -m`). After-context still drains.
    pub max_count: Option<u64>,
}

pub fn grep_bytes(
    bytes: bytes::Bytes,
    program: &PatternProgram,
    cache: &mut PatternCache,
    options: MatchOptions,
) -> Vec<LineEvent> {
    let matches = program.find_iter(cache, &bytes).collect::<Vec<_>>();
    grep_matches(bytes, &matches, options)
}

pub fn grep_matches(
    bytes: bytes::Bytes,
    matches: &[PatternMatch],
    options: MatchOptions,
) -> Vec<LineEvent> {
    if options.max_count == Some(0) {
        return Vec::new();
    }
    debug_assert!(matches.windows(2).all(|pair| {
        pair[0].start <= pair[0].end && pair[0].end <= pair[1].start && pair[1].start <= pair[1].end
    }));
    debug_assert!(matches
        .last()
        .is_none_or(|matched| matched.end <= bytes.len()));
    let mut out = Vec::new();
    let mut finder = matches.iter().peekable();
    let mut ring: VecDeque<(u64, usize, usize)> = VecDeque::new();
    let mut line_no: u64 = 0;
    let mut pos = 0usize;
    let mut after_remaining = 0usize;
    let mut matched_lines: u64 = 0;
    let mut done = false;
    while pos < bytes.len() {
        line_no += 1;
        let (content_end, span_end) = match memchr::memchr(b'\n', &bytes[pos..]) {
            Some(off) => (pos + off, pos + off + 1),
            None => (bytes.len(), bytes.len()),
        };
        let mut subs = Vec::new();
        while finder
            .peek()
            .is_some_and(|matched| matched.start <= content_end)
        {
            let matched = finder.next().expect("peeked");
            debug_assert!(matched.start >= pos);
            // A match may cross the newline (unsanitized library patterns):
            // clamp its end to this line's content per the SubMatch contract.
            subs.push(SubMatch {
                start: matched.start - pos,
                end: matched.end.min(content_end).max(matched.start) - pos,
            });
        }
        if !subs.is_empty() && !done {
            // ring never holds an emitted line: pushes require
            // after_remaining == 0 and every match drains fully
            while let Some((l, s, e)) = ring.pop_front() {
                out.push(LineEvent {
                    line: l,
                    kind: LineKind::Context,
                    offset: s as u64,
                    text: bytes.slice(s..e),
                    submatches: Vec::new(),
                });
            }
            out.push(LineEvent {
                line: line_no,
                kind: LineKind::Match,
                offset: pos as u64,
                text: bytes.slice(pos..span_end),
                submatches: subs,
            });
            matched_lines += 1;
            after_remaining = options.after_context;
            if options.max_count == Some(matched_lines) {
                done = true;
            }
        } else if after_remaining > 0 {
            out.push(LineEvent {
                line: line_no,
                kind: LineKind::Context,
                offset: pos as u64,
                text: bytes.slice(pos..span_end),
                submatches: subs,
            });
            after_remaining -= 1;
        } else if options.before_context > 0 {
            if ring.len() == options.before_context {
                ring.pop_front();
            }
            ring.push_back((line_no, pos, span_end));
        }
        if (done || finder.peek().is_none()) && after_remaining == 0 {
            break;
        }
        pos = span_end;
    }
    out
}

pub fn grep_doc(bytes: &[u8], program: &PatternProgram, options: MatchOptions) -> Vec<LineEvent> {
    let mut cache = program.create_cache();
    grep_bytes(
        bytes::Bytes::copy_from_slice(bytes),
        program,
        &mut cache,
        options,
    )
}

pub fn has_line_match(bytes: &[u8], program: &PatternProgram, cache: &mut PatternCache) -> bool {
    !bytes.is_empty()
        && program.find_iter(cache, bytes).any(|matched| {
            matched.start < bytes.len() || bytes.last().is_some_and(|byte| *byte != b'\n')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn program(pattern: &str) -> PatternProgram {
        PatternProgram::compile(&[crate::parse_pattern(pattern).unwrap()], &[0]).unwrap()
    }

    fn is_match(bytes: &[u8], pattern: &str) -> bool {
        let program = program(pattern);
        let mut cache = program.create_cache();
        has_line_match(bytes, &program, &mut cache)
    }

    type EventShape = (u64, LineKind, Vec<(usize, usize)>);

    fn shape(events: &[LineEvent]) -> Vec<EventShape> {
        events
            .iter()
            .map(|e| {
                (
                    e.line,
                    e.kind,
                    e.submatches.iter().map(|s| (s.start, s.end)).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn match_line_col() {
        let events = grep_doc(b"foo\nbar baz", &program("baz"), MatchOptions::default());
        assert_eq!(
            events,
            vec![LineEvent {
                line: 2,
                kind: LineKind::Match,
                offset: 4,
                text: bytes::Bytes::from_static(b"bar baz"),
                submatches: vec![SubMatch { start: 4, end: 7 }],
            }]
        );
    }

    #[test]
    fn grep_doc_merges_per_line_and_tracks_lines() {
        // x appears twice on line 3: ONE event with two submatches
        let bytes = b"alpha x\nbeta\nx gamma x\nx";
        let events = grep_doc(bytes, &program("x"), MatchOptions::default());
        assert_eq!(
            shape(&events),
            vec![
                (1, LineKind::Match, vec![(6, 7)]),
                (3, LineKind::Match, vec![(0, 1), (8, 9)]),
                (4, LineKind::Match, vec![(0, 1)]),
            ]
        );
        assert_eq!(events[1].text, b"x gamma x\n".to_vec());
        assert_eq!(events[2].text, b"x".to_vec());
    }

    #[test]
    fn grep_doc_context_merges_overlaps() {
        // matches on lines 3 and 5 with C=2: lines 1-7 once each, 3+5 Match
        let bytes = b"l1\nl2\nhit\nl4\nhit\nl6\nl7\nl8\n";
        let opts = MatchOptions {
            before_context: 2,
            after_context: 2,
            max_count: None,
        };
        let events = grep_doc(bytes, &program("hit"), opts);
        let lines: Vec<(u64, LineKind)> = events.iter().map(|e| (e.line, e.kind)).collect();
        assert_eq!(
            lines,
            vec![
                (1, LineKind::Context),
                (2, LineKind::Context),
                (3, LineKind::Match),
                (4, LineKind::Context),
                (5, LineKind::Match),
                (6, LineKind::Context),
                (7, LineKind::Context),
            ]
        );
    }

    #[test]
    fn grep_doc_independent_before_after() {
        let bytes = b"a\nb\nhit\nc\nd\n";
        let only_after = MatchOptions {
            after_context: 1,
            ..Default::default()
        };
        let events = grep_doc(bytes, &program("hit"), only_after);
        assert_eq!(
            events.iter().map(|e| e.line).collect::<Vec<_>>(),
            vec![3, 4]
        );
        let only_before = MatchOptions {
            before_context: 1,
            ..Default::default()
        };
        let events = grep_doc(bytes, &program("hit"), only_before);
        assert_eq!(
            events.iter().map(|e| e.line).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn grep_doc_max_count_caps_but_drains_after_context() {
        let bytes = b"hit\nmid\nhit\ntail\n";
        let opts = MatchOptions {
            after_context: 1,
            max_count: Some(1),
            ..Default::default()
        };
        let events = grep_doc(bytes, &program("hit"), opts);
        // one Match, then line 2 as after-context; the capped line-3 match
        // never surfaces because after-context ran out before it
        assert_eq!(
            shape(&events),
            vec![
                (1, LineKind::Match, vec![(0, 3)]),
                (2, LineKind::Context, vec![]),
            ]
        );
        assert!(grep_doc(
            bytes,
            &program("hit"),
            MatchOptions {
                max_count: Some(0),
                ..Default::default()
            }
        )
        .is_empty());
    }

    #[test]
    fn grep_doc_post_cap_match_in_context_carries_submatches() {
        let bytes = b"hit\nhit\nrest\n";
        let opts = MatchOptions {
            after_context: 1,
            max_count: Some(1),
            ..Default::default()
        };
        let events = grep_doc(bytes, &program("hit"), opts);
        assert_eq!(
            shape(&events),
            vec![
                (1, LineKind::Match, vec![(0, 3)]),
                (2, LineKind::Context, vec![(0, 3)]),
            ]
        );
    }

    #[test]
    fn grep_doc_eof_line_without_newline() {
        let events = grep_doc(
            b"no newline tail",
            &program("tail"),
            MatchOptions::default(),
        );
        assert_eq!(events[0].text, b"no newline tail".to_vec());
        assert_eq!(events[0].submatches, vec![SubMatch { start: 11, end: 15 }]);
    }

    #[test]
    fn line_safe_program_matches_cannot_cross_lines() {
        let program = program(r"foo[^\n]+bar");
        assert!(grep_doc(b"foo\nbar\n", &program, MatchOptions::default()).is_empty());
        let mut cache = program.create_cache();
        assert!(!has_line_match(b"foo\nbar\n", &program, &mut cache));
        let events = grep_doc(b"foo\nbar\nfoo bar\n", &program, MatchOptions::default());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].line, 3);
        assert_eq!(events[0].submatches, vec![SubMatch { start: 0, end: 7 }]);
    }

    #[test]
    fn line_match_probe_matches_grep_results() {
        for bytes in [
            b"foo\nbar\nfoo bar".as_slice(),
            b"foo\nbar\nfoo bar\n".as_slice(),
        ] {
            for pattern in ["(?m)^foo", "(?m)bar$", "(?m)^$", "foo", "(?m)$"] {
                assert_eq!(
                    !grep_doc(bytes, &program(pattern), MatchOptions::default()).is_empty(),
                    is_match(bytes, pattern)
                );
            }
        }
    }

    #[test]
    fn grep_matches_ignores_pattern_ids() {
        let bytes = bytes::Bytes::from_static(b"alpha beta\n");
        assert_eq!(
            grep_matches(
                bytes,
                &[
                    PatternMatch {
                        pattern: 9,
                        start: 0,
                        end: 5,
                    },
                    PatternMatch {
                        pattern: 3,
                        start: 6,
                        end: 10,
                    },
                ],
                MatchOptions::default(),
            )[0]
            .submatches,
            [
                SubMatch { start: 0, end: 5 },
                SubMatch { start: 6, end: 10 }
            ]
        );
    }
}
