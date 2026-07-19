use std::collections::VecDeque;

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

/// Run `re` over one decoded doc, producing the rg-ordered, overlap-merged
/// line event stream: events sorted by line, each line present at most once,
/// matches preferred over context. Empty result == zero matching lines.
pub fn grep_bytes(
    bytes: bytes::Bytes,
    re: &regex::bytes::Regex,
    options: MatchOptions,
) -> Vec<LineEvent> {
    grep_bytes_inner(bytes, re, options, false)
}

pub fn grep_bytes_fast(
    bytes: bytes::Bytes,
    re: &regex::bytes::Regex,
    options: MatchOptions,
) -> Vec<LineEvent> {
    grep_bytes_inner(bytes, re, options, true)
}

fn grep_bytes_inner(
    bytes: bytes::Bytes,
    re: &regex::bytes::Regex,
    options: MatchOptions,
    whole_document: bool,
) -> Vec<LineEvent> {
    if options.max_count == Some(0) {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut finder = whole_document.then(|| re.find_iter(&bytes).peekable());
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
        let subs = match &mut finder {
            Some(finder) => {
                let mut subs = Vec::new();
                while finder
                    .peek()
                    .is_some_and(|matched| matched.start() <= content_end)
                {
                    let matched = finder.next().expect("peeked");
                    subs.push(SubMatch {
                        start: matched.start() - pos,
                        end: matched.end().min(content_end).max(matched.start()) - pos,
                    });
                }
                subs
            }
            None => re
                .find_iter(&bytes[pos..content_end])
                .map(|matched| SubMatch {
                    start: matched.start(),
                    end: matched.end(),
                })
                .collect(),
        };
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
        if (done
            || finder
                .as_mut()
                .is_some_and(|finder| finder.peek().is_none()))
            && after_remaining == 0
        {
            break;
        }
        pos = span_end;
    }
    out
}

pub fn grep_doc(bytes: &[u8], re: &regex::bytes::Regex, options: MatchOptions) -> Vec<LineEvent> {
    grep_bytes(bytes::Bytes::copy_from_slice(bytes), re, options)
}

/// Line-semantics match test (rg behavior): a doc matches iff some match
/// STARTS before EOF — an empty doc has no lines and never matches, and an
/// empty match at EOF belongs to no line.
pub fn has_line_match(bytes: &[u8], re: &regex::bytes::Regex) -> bool {
    let mut pos = 0;
    while pos < bytes.len() {
        let span_end = match memchr::memchr(b'\n', &bytes[pos..]) {
            Some(offset) => pos + offset + 1,
            None => bytes.len(),
        };
        let content_end = span_end - usize::from(bytes[span_end - 1] == b'\n');
        if re.is_match(&bytes[pos..content_end]) {
            return true;
        }
        pos = span_end;
    }
    false
}

pub fn has_line_match_fast(bytes: &[u8], re: &regex::bytes::Regex) -> bool {
    !bytes.is_empty()
        && re.find(bytes).is_some_and(|matched| {
            matched.start() < bytes.len() || bytes.last().is_some_and(|byte| *byte != b'\n')
        })
}

/// The one place a pattern is parsed. `utf8(false)` matches the verifier
/// (`regex::bytes`): patterns like `(?-u)\xff` are valid there and must
/// parse here too, not be rejected. Planning and the grep-mode analyses
/// below all consume this same Hir, so they can never disagree with each
/// other about what the pattern means.
pub fn parse_pattern(pattern: &str) -> anyhow::Result<regex_syntax::hir::Hir> {
    Ok(regex_syntax::ParserBuilder::new()
        .utf8(false)
        .build()
        .parse(pattern)?)
}

pub fn can_search_as_document(hir: &regex_syntax::hir::Hir) -> bool {
    !needs_line_isolation(hir)
}

pub fn bounded_match_len(hir: &regex_syntax::hir::Hir) -> Option<usize> {
    if needs_line_isolation(hir) || has_look(hir) {
        return None;
    }
    hir.properties().maximum_len()
}

fn has_look(hir: &regex_syntax::hir::Hir) -> bool {
    use regex_syntax::hir::HirKind;
    match hir.kind() {
        HirKind::Look(_) => true,
        HirKind::Repetition(repetition) => has_look(&repetition.sub),
        HirKind::Capture(capture) => has_look(&capture.sub),
        HirKind::Concat(children) | HirKind::Alternation(children) => children.iter().any(has_look),
        HirKind::Empty | HirKind::Literal(_) | HirKind::Class(_) => false,
    }
}

fn needs_line_isolation(hir: &regex_syntax::hir::Hir) -> bool {
    use regex_syntax::hir::{Class, HirKind, Look};
    match hir.kind() {
        HirKind::Literal(literal) => literal.0.contains(&b'\n'),
        HirKind::Class(Class::Bytes(class)) => class
            .ranges()
            .iter()
            .any(|range| range.start() <= b'\n' && b'\n' <= range.end()),
        HirKind::Class(Class::Unicode(class)) => class
            .ranges()
            .iter()
            .any(|range| range.start() <= '\n' && '\n' <= range.end()),
        HirKind::Look(Look::Start | Look::End | Look::StartCRLF | Look::EndCRLF) => true,
        HirKind::Repetition(repetition) => needs_line_isolation(&repetition.sub),
        HirKind::Capture(capture) => needs_line_isolation(&capture.sub),
        HirKind::Concat(children) | HirKind::Alternation(children) => {
            children.iter().any(needs_line_isolation)
        }
        HirKind::Empty | HirKind::Look(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn re(p: &str) -> regex::bytes::Regex {
        regex::bytes::Regex::new(p).unwrap()
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
        let events = grep_doc(b"foo\nbar baz", &re("baz"), MatchOptions::default());
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
        let events = grep_doc(bytes, &re("x"), MatchOptions::default());
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
        let events = grep_doc(bytes, &re("hit"), opts);
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
        let events = grep_doc(bytes, &re("hit"), only_after);
        assert_eq!(
            events.iter().map(|e| e.line).collect::<Vec<_>>(),
            vec![3, 4]
        );
        let only_before = MatchOptions {
            before_context: 1,
            ..Default::default()
        };
        let events = grep_doc(bytes, &re("hit"), only_before);
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
        let events = grep_doc(bytes, &re("hit"), opts);
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
            &re("hit"),
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
        let events = grep_doc(bytes, &re("hit"), opts);
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
        let events = grep_doc(b"no newline tail", &re("tail"), MatchOptions::default());
        assert_eq!(events[0].text, b"no newline tail".to_vec());
        assert_eq!(events[0].submatches, vec![SubMatch { start: 11, end: 15 }]);
    }

    #[test]
    fn regex_matches_cannot_cross_lines() {
        let regex = re(r"foo\s+bar");
        assert!(grep_doc(b"foo\nbar\n", &regex, MatchOptions::default()).is_empty());
        assert!(!has_line_match(b"foo\nbar\n", &regex));
        let events = grep_doc(b"foo\nbar\nfoo bar\n", &regex, MatchOptions::default());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].line, 3);
        assert_eq!(events[0].submatches, vec![SubMatch { start: 0, end: 7 }]);
    }

    #[test]
    fn whole_document_fast_path_is_conservative_and_equivalent() {
        assert!(can_search_as_document(
            &parse_pattern("(?m)^foo.*bar$").unwrap()
        ));
        assert!(!can_search_as_document(
            &parse_pattern(r"foo\s+bar").unwrap()
        ));
        assert!(!can_search_as_document(&parse_pattern(r"\Afoo").unwrap()));
        for bytes in [
            b"foo\nbar\nfoo bar".as_slice(),
            b"foo\nbar\nfoo bar\n".as_slice(),
        ] {
            for pattern in ["(?m)^foo", "(?m)bar$", "(?m)^$", "foo", "(?m)$"] {
                let bytes = bytes::Bytes::copy_from_slice(bytes);
                let regex = re(pattern);
                assert_eq!(
                    grep_bytes(bytes.clone(), &regex, MatchOptions::default()),
                    grep_bytes_fast(bytes.clone(), &regex, MatchOptions::default())
                );
                assert_eq!(
                    has_line_match(&bytes, &regex),
                    has_line_match_fast(&bytes, &regex)
                );
            }
        }
    }

    #[test]
    fn bounded_match_lengths_exclude_unbounded_and_contextual_patterns() {
        assert_eq!(
            bounded_match_len(&parse_pattern("needle").unwrap()),
            Some(6)
        );
        assert_eq!(
            bounded_match_len(&parse_pattern("a{2,5}").unwrap()),
            Some(5)
        );
        assert_eq!(bounded_match_len(&parse_pattern("a+").unwrap()), None);
        assert_eq!(
            bounded_match_len(&parse_pattern(r"\bword\b").unwrap()),
            None
        );
        assert_eq!(bounded_match_len(&parse_pattern("^word").unwrap()), None);
        assert_eq!(
            bounded_match_len(&parse_pattern("line\\nnext").unwrap()),
            None
        );
    }
}
