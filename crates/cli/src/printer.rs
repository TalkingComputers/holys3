//! Stdout sinks rendering rg's output formats: standard (heading or
//! key:line:text), -l paths, -c counts, -q quiet. A closed downstream pipe
//! stops the search instead of erroring.

use crate::ColorArg;
use anyhow::Result;
use seagrep_core::{LineKind, SubMatch};
use seagrep_index::{DocResult, MatchSink, SinkFlow};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use termcolor::{BufferedStandardStream, ColorChoice, ColorSpec, WriteColor};

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

fn write_colored(
    wtr: &mut BufferedStandardStream,
    spec: &ColorSpec,
    bytes: &[u8],
) -> std::io::Result<()> {
    wtr.set_color(spec)?;
    wtr.write_all(bytes)?;
    wtr.reset()
}

/// Line content minus one trailing newline; rendering re-adds it.
fn content_of(text: &[u8]) -> &[u8] {
    text.strip_suffix(b"\n").unwrap_or(text)
}

fn write_text_highlighted(
    wtr: &mut BufferedStandardStream,
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
}

/// Absolute byte offset of the first NUL in any output line, rg's binary
/// heuristic: content with NULs is suppressed unless -a/--text is set.
fn binary_nul_offset(events: &[seagrep_core::LineEvent]) -> Option<u64> {
    events.iter().find_map(|event| {
        event
            .text
            .iter()
            .position(|&byte| byte == 0)
            .map(|position| event.offset + position as u64)
    })
}

fn binary_notice(offset: u64) -> String {
    format!("binary file matches (found \"\\0\" byte around offset {offset})")
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

impl MatchSink for StandardSink {
    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let mut state = lock(&self.state)?;
        let state = &mut *state;
        let config = &self.config;
        let write =
            |wtr: &mut BufferedStandardStream, printed_any_doc: bool| -> std::io::Result<()> {
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
                    if let Some(offset) = binary_nul_offset(doc.events) {
                        if !config.heading {
                            write_colored(wtr, &path_spec(), key.as_bytes())?;
                            wtr.write_all(b": ")?;
                        }
                        wtr.write_all(binary_notice(offset).as_bytes())?;
                        wtr.write_all(b"\n")?;
                        return wtr.flush();
                    }
                }
                let mut prev_line: Option<u64> = None;
                for event in doc.events {
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
    fn wants_matches(&self) -> bool {
        false // engine stops at first match per doc (rg -l)
    }

    fn on_doc(&self, key: &str, _doc: &DocResult<'_>) -> Result<SinkFlow> {
        let mut wtr = lock(&self.state)?;
        flow_of(
            write_colored(&mut wtr, &path_spec(), key.as_bytes())
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
    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let n = if self.count_matches {
            doc.events.iter().map(|e| e.submatches.len()).sum::<usize>()
        } else {
            doc.events
                .iter()
                .filter(|e| e.kind == LineKind::Match)
                .count()
        };
        let mut wtr = lock(&self.state)?;
        flow_of(
            write_colored(&mut wtr, &path_spec(), key.as_bytes())
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
    fn wants_matches(&self) -> bool {
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
    use seagrep_core::LineEvent;

    fn event(line: u64, offset: u64, text: &[u8]) -> LineEvent {
        LineEvent {
            line,
            kind: LineKind::Match,
            offset,
            text: bytes::Bytes::copy_from_slice(text),
            submatches: vec![SubMatch { start: 0, end: 1 }],
        }
    }

    #[test]
    fn binary_nul_offset_reports_first_nul_across_events() {
        let events = [
            event(3, 100, b"clean line\n"),
            event(9, 400, b"ab\x00cd\x00\n"),
            event(12, 900, b"\x00\n"),
        ];
        assert_eq!(binary_nul_offset(&events), Some(402));
    }

    #[test]
    fn binary_nul_offset_is_none_for_text_lines() {
        let events = [event(1, 0, b"hello world\n")];
        assert_eq!(binary_nul_offset(&events), None);
    }

    #[test]
    fn binary_notice_matches_rg_wording() {
        assert_eq!(
            binary_notice(402),
            "binary file matches (found \"\\0\" byte around offset 402)"
        );
    }
}
