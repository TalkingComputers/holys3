//! Stdout sinks rendering rg's output formats: standard (heading or
//! key:line:text), -l paths, -c counts, -q quiet. A closed downstream pipe
//! stops the search instead of erroring.

use crate::ColorArg;
use anyhow::Result;
use seagrep_core::{LineEvent, LineKind, SubMatch};
use seagrep_index::{DocResult, MatchData, MatchSink, MatchWindow, SearchDetail, SinkFlow};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use termcolor::{BufferedStandardStream, ColorChoice, ColorSpec, WriteColor};

const CLIP_MARK: &[u8] = "…".as_bytes();

pub(crate) fn resolve_color(arg: ColorArg, is_tty: bool) -> ColorChoice {
    match arg {
        ColorArg::Never => ColorChoice::Never,
        ColorArg::Always => ColorChoice::Always,
        ColorArg::Ansi => ColorChoice::AlwaysAnsi,
        // termcolor's Auto handles TERM unset/dumb and NO_COLOR itself; the
        // tty check is ours.
        ColorArg::Auto => {
            if is_tty {
                ColorChoice::Auto
            } else {
                ColorChoice::Never
            }
        }
    }
}

fn flow_of(result: std::io::Result<()>) -> Result<SinkFlow> {
    match result {
        Ok(()) => Ok(SinkFlow::Continue),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(SinkFlow::Stop),
        Err(err) => Err(err.into()),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>> {
    mutex
        .lock()
        .map_err(|_| anyhow::anyhow!("output writer poisoned"))
}

// rg's default palette (crates/printer/src/color.rs).
fn path_spec() -> ColorSpec {
    let mut spec = ColorSpec::new();
    spec.set_fg(Some(termcolor::Color::Magenta));
    spec
}

fn line_spec() -> ColorSpec {
    let mut spec = ColorSpec::new();
    spec.set_fg(Some(termcolor::Color::Green));
    spec
}

fn match_spec() -> ColorSpec {
    let mut spec = ColorSpec::new();
    spec.set_fg(Some(termcolor::Color::Red));
    spec.set_bold(true);
    spec
}

fn write_colored(wtr: &mut impl WriteColor, spec: &ColorSpec, bytes: &[u8]) -> std::io::Result<()> {
    wtr.set_color(spec)?;
    wtr.write_all(bytes)?;
    wtr.reset()
}

/// Line content minus one trailing newline; rendering re-adds it.
fn content_of(text: &[u8]) -> &[u8] {
    text.strip_suffix(b"\n").unwrap_or(text)
}

fn write_text_highlighted(
    wtr: &mut impl WriteColor,
    text: &[u8],
    submatches: &[SubMatch],
) -> std::io::Result<()> {
    let content = content_of(text);
    let mut cursor = 0;
    for sub in submatches {
        let start = sub.start.min(content.len());
        let end = sub.end.min(content.len());
        wtr.write_all(&content[cursor..start])?;
        write_colored(wtr, &match_spec(), &content[start..end])?;
        cursor = end;
    }
    wtr.write_all(&content[cursor..])?;
    wtr.write_all(b"\n")
}

pub(crate) struct RenderConfig {
    pub(crate) heading: bool,
    pub(crate) line_numbers: bool,
    pub(crate) column: bool,
    pub(crate) context_active: bool,
    /// `-a/--text`: print matches from binary documents instead of the
    /// rg-style suppression notice.
    pub(crate) text: bool,
    /// `--match-window BYTES`: render bounded match-centered windows
    /// instead of complete lines.
    pub(crate) match_window: Option<usize>,
}

/// Absolute byte offset of the first NUL in any fetched output content,
/// rg's binary heuristic: content with NULs is suppressed unless -a/--text
/// is set. Windows check only their fetched bytes, so an unfetched NUL
/// elsewhere on a giant line cannot suppress a window.
fn binary_nul_offset(data: &MatchData<'_>) -> Option<u64> {
    match data {
        MatchData::Lines(events) => events.iter().find_map(|event| {
            event
                .text
                .iter()
                .position(|&byte| byte == 0)
                .map(|position| event.offset + position as u64)
        }),
        MatchData::Windows(windows) => windows.iter().find_map(|window| {
            window
                .text
                .iter()
                .position(|&byte| byte == 0)
                .map(|position| window.window_offset + position as u64)
        }),
        MatchData::Documents => None,
    }
}

fn binary_notice(offset: u64) -> String {
    format!("binary file matches (found \"\\0\" byte around offset {offset})")
}

/// Render one bounded window: uncolored `…` marks a clipped line-window
/// edge; a highlighted `…` marks each clipped witness edge; one extra
/// highlighted trailing `…` marks a non-canonical (proven-witness) span —
/// when the right edge is also clipped, the two ellipses are intentional
/// and distinguish the two facts by count. Markers never count toward the
/// window's content budget.
fn write_window_highlighted(
    wtr: &mut impl WriteColor,
    window: &MatchWindow,
) -> std::io::Result<()> {
    if window.left_clipped {
        wtr.write_all(CLIP_MARK)?;
    }
    let content = content_of(&window.text);
    let mut cursor = 0usize;
    for matched in &window.matches {
        let start = matched.visible.start.min(content.len()).max(cursor);
        let end = matched.visible.end.min(content.len()).max(start);
        wtr.write_all(&content[cursor..start])?;
        if matched.left_clipped {
            write_colored(wtr, &match_spec(), CLIP_MARK)?;
        }
        write_colored(wtr, &match_spec(), &content[start..end])?;
        if matched.right_clipped {
            write_colored(wtr, &match_spec(), CLIP_MARK)?;
        }
        if !matched.canonical_span_known {
            write_colored(wtr, &match_spec(), CLIP_MARK)?;
        }
        cursor = end;
    }
    wtr.write_all(&content[cursor..])?;
    if window.right_clipped {
        wtr.write_all(CLIP_MARK)?;
    }
    wtr.write_all(b"\n")
}

/// One output line per window with the existing heading/path and optional
/// line-number prefixes. Window mode has no context separator, and Clap
/// rejects --column; `window_offset` stays typed metadata for the binary
/// heuristic rather than becoming a printed column.
fn write_window(
    wtr: &mut impl WriteColor,
    key: &str,
    window: &MatchWindow,
    config: &RenderConfig,
) -> std::io::Result<()> {
    if !config.heading {
        write_colored(wtr, &path_spec(), key.as_bytes())?;
        wtr.write_all(b":")?;
    }
    if config.line_numbers {
        write_colored(wtr, &line_spec(), window.line.to_string().as_bytes())?;
        wtr.write_all(b":")?;
    }
    write_window_highlighted(wtr, window)
}

pub(crate) struct StandardSink {
    state: Mutex<StandardState>,
    config: RenderConfig,
}

struct StandardState {
    wtr: BufferedStandardStream,
    printed_any_doc: bool,
}

impl StandardSink {
    pub(crate) fn new(config: RenderConfig, color: ColorChoice) -> StandardSink {
        StandardSink {
            state: Mutex::new(StandardState {
                wtr: BufferedStandardStream::stdout(color),
                printed_any_doc: false,
            }),
            config,
        }
    }
}

enum StandardPayload<'a> {
    Lines(&'a [LineEvent]),
    Windows(&'a [MatchWindow]),
}

impl MatchSink for StandardSink {
    fn detail(&self) -> SearchDetail {
        match self.config.match_window {
            Some(max_bytes) => SearchDetail::MatchWindows { max_bytes },
            None => SearchDetail::FullLines,
        }
    }

    fn wants_hit_keys(&self) -> bool {
        false
    }

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let payload = match (&doc.data, self.config.match_window) {
            (MatchData::Lines(events), None) => StandardPayload::Lines(events),
            (MatchData::Windows(windows), Some(_)) => StandardPayload::Windows(windows),
            _ => anyhow::bail!("standard sink received incompatible search data"),
        };
        let mut state = lock(&self.state)?;
        let state = &mut *state;
        let config = &self.config;
        let write = |wtr: &mut BufferedStandardStream,
                     printed_any_doc: bool|
         -> std::io::Result<()> {
            if config.heading {
                if printed_any_doc {
                    wtr.write_all(b"\n")?;
                }
                write_colored(wtr, &path_spec(), key.as_bytes())?;
                wtr.write_all(b"\n")?;
            } else if config.context_active && printed_any_doc {
                wtr.write_all(b"--\n")?;
            }
            if !config.text {
                if let Some(offset) = binary_nul_offset(&doc.data) {
                    if !config.heading {
                        write_colored(wtr, &path_spec(), key.as_bytes())?;
                        wtr.write_all(b": ")?;
                    }
                    wtr.write_all(binary_notice(offset).as_bytes())?;
                    wtr.write_all(b"\n")?;
                    return wtr.flush();
                }
            }
            match payload {
                StandardPayload::Windows(windows) => {
                    for window in windows {
                        write_window(wtr, key, window, config)?;
                    }
                }
                StandardPayload::Lines(events) => {
                    let mut prev_line: Option<u64> = None;
                    for event in events {
                        // rg emits group separators only when context is enabled
                        if config.context_active && prev_line.is_some_and(|p| event.line > p + 1) {
                            wtr.write_all(b"--\n")?;
                        }
                        prev_line = Some(event.line);
                        let sep: &[u8] = match event.kind {
                            LineKind::Match => b":",
                            LineKind::Context => b"-",
                        };
                        if !config.heading {
                            write_colored(wtr, &path_spec(), key.as_bytes())?;
                            wtr.write_all(sep)?;
                        }
                        if config.line_numbers {
                            write_colored(wtr, &line_spec(), event.line.to_string().as_bytes())?;
                            wtr.write_all(sep)?;
                        }
                        if config.column && event.kind == LineKind::Match {
                            let col = event.submatches.first().map_or(0, |s| s.start) + 1;
                            wtr.write_all(col.to_string().as_bytes())?;
                            wtr.write_all(sep)?;
                        }
                        write_text_highlighted(wtr, &event.text, &event.submatches)?;
                    }
                }
            }
            wtr.flush()
        };
        let printed_any_doc = state.printed_any_doc;
        state.printed_any_doc = true;
        flow_of(write(&mut state.wtr, printed_any_doc))
    }
}

pub(crate) struct PathSink {
    state: Mutex<BufferedStandardStream>,
}

impl PathSink {
    pub(crate) fn new(color: ColorChoice) -> PathSink {
        PathSink {
            state: Mutex::new(BufferedStandardStream::stdout(color)),
        }
    }
}

impl MatchSink for PathSink {
    fn detail(&self) -> SearchDetail {
        SearchDetail::Documents
    }

    fn wants_hit_keys(&self) -> bool {
        false
    }

    fn on_doc(&self, key: &str, _doc: &DocResult<'_>) -> Result<SinkFlow> {
        let mut wtr = lock(&self.state)?;
        flow_of(
            write_colored(&mut *wtr, &path_spec(), key.as_bytes())
                .and_then(|()| wtr.write_all(b"\n"))
                .and_then(|()| wtr.flush()),
        )
    }
}

pub(crate) struct CountSink {
    state: Mutex<BufferedStandardStream>,
    count_matches: bool,
}

impl CountSink {
    pub(crate) fn new(count_matches: bool, color: ColorChoice) -> CountSink {
        CountSink {
            state: Mutex::new(BufferedStandardStream::stdout(color)),
            count_matches,
        }
    }
}

impl MatchSink for CountSink {
    fn detail(&self) -> SearchDetail {
        if self.count_matches {
            SearchDetail::MatchCount
        } else {
            SearchDetail::MatchingLines
        }
    }

    fn wants_hit_keys(&self) -> bool {
        false
    }

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let MatchData::Lines(events) = doc.data else {
            anyhow::bail!("count sink requires line data");
        };
        let n = if self.count_matches {
            events.iter().map(|e| e.submatches.len()).sum::<usize>()
        } else {
            events.iter().filter(|e| e.kind == LineKind::Match).count()
        };
        let mut wtr = lock(&self.state)?;
        flow_of(
            write_colored(&mut *wtr, &path_spec(), key.as_bytes())
                .and_then(|()| writeln!(wtr, ":{n}"))
                .and_then(|()| wtr.flush()),
        )
    }
}

pub(crate) struct QuietSink {
    stop: bool,
    matched: AtomicBool,
}

impl QuietSink {
    /// `stop` = end the search at the first match (off when --stats needs
    /// the full traversal).
    pub(crate) fn new(stop: bool) -> QuietSink {
        QuietSink {
            stop,
            matched: AtomicBool::new(false),
        }
    }

    pub(crate) fn matched(&self) -> bool {
        self.matched.load(Ordering::Relaxed)
    }
}

impl MatchSink for QuietSink {
    fn detail(&self) -> SearchDetail {
        SearchDetail::Documents
    }

    fn wants_hit_keys(&self) -> bool {
        false
    }

    fn on_doc(&self, _key: &str, _doc: &DocResult<'_>) -> Result<SinkFlow> {
        self.matched.store(true, Ordering::Relaxed);
        Ok(if self.stop {
            SinkFlow::Stop
        } else {
            SinkFlow::Continue
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seagrep_index::WindowMatch;

    fn event(line: u64, offset: u64, text: &[u8]) -> LineEvent {
        LineEvent {
            line,
            kind: LineKind::Match,
            offset,
            text: bytes::Bytes::copy_from_slice(text),
            submatches: vec![SubMatch { start: 0, end: 1 }],
        }
    }

    fn window(window_offset: u64, text: &[u8]) -> MatchWindow {
        MatchWindow {
            line: 1,
            line_offset: 0,
            window_offset,
            text: bytes::Bytes::copy_from_slice(text),
            matches: Vec::new(),
            left_clipped: false,
            right_clipped: false,
        }
    }

    #[test]
    fn binary_nul_offset_reports_first_nul_across_events() {
        let events = [
            event(3, 100, b"clean line\n"),
            event(9, 400, b"ab\x00cd\x00\n"),
            event(12, 900, b"\x00\n"),
        ];
        assert_eq!(binary_nul_offset(&MatchData::Lines(&events)), Some(402));
    }

    #[test]
    fn binary_nul_offset_is_none_for_text_lines() {
        let events = [event(1, 0, b"hello world\n")];
        assert_eq!(binary_nul_offset(&MatchData::Lines(&events)), None);
        assert_eq!(binary_nul_offset(&MatchData::Documents), None);
    }

    #[test]
    fn window_binary_check_uses_absolute_fetched_offset() {
        let with_nul = [window(5_000, b"ab\x00cd")];
        assert_eq!(
            binary_nul_offset(&MatchData::Windows(&with_nul)),
            Some(5_002)
        );
        // A NUL outside the fetched window cannot suppress the output.
        let clean = [window(5_000, b"clean window")];
        assert_eq!(binary_nul_offset(&MatchData::Windows(&clean)), None);
    }

    #[test]
    fn binary_notice_matches_rg_wording() {
        assert_eq!(
            binary_notice(402),
            "binary file matches (found \"\\0\" byte around offset 402)"
        );
    }

    fn ansi_window(window: &MatchWindow) -> String {
        let mut buffer = termcolor::Buffer::ansi();
        write_window_highlighted(&mut buffer, window).unwrap();
        String::from_utf8(buffer.into_inner()).unwrap()
    }

    const HIGHLIGHT: &str = "\u{1b}[0m\u{1b}[1m\u{1b}[31m";
    const RESET: &str = "\u{1b}[0m";

    #[test]
    fn window_render_marks_each_clip_kind() {
        // Plain window, full witness: no markers, exact content width.
        let mut plain = window(0, b"before MATCH after");
        plain.matches = vec![WindowMatch {
            witness: 7..12,
            visible: 7..12,
            left_clipped: false,
            right_clipped: false,
            canonical_span_known: true,
        }];
        assert_eq!(
            ansi_window(&plain),
            format!("before {HIGHLIGHT}MATCH{RESET} after\n")
        );

        // Both window edges clipped, witness right-clipped, non-canonical
        // span: uncolored edge marks, one highlighted mark per clipped
        // witness edge plus one for the unknown canonical span.
        let mut clipped = window(100, b"xxMXYZ");
        clipped.left_clipped = true;
        clipped.right_clipped = true;
        clipped.matches = vec![WindowMatch {
            witness: 102..150,
            visible: 2..6,
            left_clipped: false,
            right_clipped: true,
            canonical_span_known: false,
        }];
        assert_eq!(
            ansi_window(&clipped),
            format!("…xx{HIGHLIGHT}MXYZ{RESET}{HIGHLIGHT}…{RESET}{HIGHLIGHT}…{RESET}…\n")
        );

        // Witness left-clipped only.
        let mut left = window(100, b"TCH tail");
        left.left_clipped = true;
        left.matches = vec![WindowMatch {
            witness: 90..103,
            visible: 0..3,
            left_clipped: true,
            right_clipped: false,
            canonical_span_known: true,
        }];
        assert_eq!(
            ansi_window(&left),
            format!("…{HIGHLIGHT}…{RESET}{HIGHLIGHT}TCH{RESET} tail\n")
        );
    }
}
