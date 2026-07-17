//! ripgrep-compatible JSON Lines output: begin/match/context/end per doc,
//! one summary message last. Wire format per rg's grep-printer.

use anyhow::Result;
use base64::Engine;
use seagrep_core::LineKind;
use seagrep_index::{DocResult, MatchSink, SinkFlow};
use serde::Serialize;
use std::borrow::Cow;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Serialize)]
#[serde(tag = "type", content = "data", rename_all = "lowercase")]
enum JsonMsg<'a> {
    Begin(BeginData<'a>),
    Match(LineData<'a>),
    Context(LineData<'a>),
    End(EndData<'a>),
    Summary(SummaryData),
}

/// rg's arbitrary-data encoding: {"text": ...} iff valid UTF-8 else
/// {"bytes": base64}.
#[derive(Serialize)]
#[serde(untagged)]
enum Data<'a> {
    Text { text: Cow<'a, str> },
    Bytes { bytes: String },
}

fn data_from(bytes: &[u8]) -> Data<'_> {
    match std::str::from_utf8(bytes) {
        Ok(text) => Data::Text {
            text: Cow::Borrowed(text),
        },
        Err(_) => Data::Bytes {
            bytes: base64::engine::general_purpose::STANDARD.encode(bytes),
        },
    }
}

#[derive(Serialize)]
struct BeginData<'a> {
    path: Data<'a>,
}

#[derive(Serialize)]
struct SubMatchData<'a> {
    #[serde(rename = "match")]
    matched: Data<'a>,
    start: usize,
    end: usize,
}

#[derive(Serialize)]
struct LineData<'a> {
    path: Data<'a>,
    lines: Data<'a>,
    line_number: Option<u64>,
    absolute_offset: u64,
    submatches: Vec<SubMatchData<'a>>,
}

#[derive(Serialize)]
struct Elapsed {
    secs: u64,
    nanos: u32,
    human: String,
}

fn elapsed_from(d: std::time::Duration) -> Elapsed {
    Elapsed {
        secs: d.as_secs(),
        nanos: d.subsec_nanos(),
        human: format!("{:.6}s", d.as_secs_f64()),
    }
}

#[derive(Serialize)]
struct StatsData {
    elapsed: Elapsed,
    searches: u64,
    searches_with_match: u64,
    bytes_searched: u64,
    bytes_printed: u64,
    matched_lines: u64,
    matches: u64,
}

#[derive(Serialize)]
struct EndData<'a> {
    path: Data<'a>,
    binary_offset: Option<u64>,
    stats: StatsData,
}

#[derive(Serialize)]
struct SummaryData {
    elapsed_total: Elapsed,
    stats: StatsData,
}

pub(crate) struct JsonSink {
    out: Mutex<std::io::BufWriter<std::io::Stdout>>,
    matched_lines: AtomicU64,
    matches: AtomicU64,
    docs_with_match: AtomicU64,
    bytes_searched: AtomicU64,
    bytes_printed: AtomicU64,
    elapsed_nanos: AtomicU64,
}

impl JsonSink {
    pub(crate) fn new() -> JsonSink {
        JsonSink {
            out: Mutex::new(std::io::BufWriter::new(std::io::stdout())),
            matched_lines: AtomicU64::new(0),
            matches: AtomicU64::new(0),
            docs_with_match: AtomicU64::new(0),
            bytes_searched: AtomicU64::new(0),
            bytes_printed: AtomicU64::new(0),
            elapsed_nanos: AtomicU64::new(0),
        }
    }

    /// The final JSON line. `searches` = candidate docs actually verified.
    pub(crate) fn write_summary(
        &self,
        stats: &seagrep_index::SearchStats,
        elapsed_total: std::time::Duration,
    ) -> Result<()> {
        let msg = JsonMsg::Summary(SummaryData {
            elapsed_total: elapsed_from(elapsed_total),
            stats: StatsData {
                elapsed: elapsed_from(std::time::Duration::from_nanos(
                    self.elapsed_nanos.load(Ordering::Relaxed),
                )),
                searches: stats.candidates as u64,
                searches_with_match: self.docs_with_match.load(Ordering::Relaxed),
                bytes_searched: self.bytes_searched.load(Ordering::Relaxed),
                bytes_printed: self.bytes_printed.load(Ordering::Relaxed),
                matched_lines: self.matched_lines.load(Ordering::Relaxed),
                matches: self.matches.load(Ordering::Relaxed),
            },
        });
        let mut out = self
            .out
            .lock()
            .map_err(|_| anyhow::anyhow!("output writer poisoned"))?;
        serde_json::to_writer(&mut *out, &msg)?;
        out.write_all(b"\n")?;
        out.flush()?;
        Ok(())
    }
}

impl MatchSink for JsonSink {
    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> Result<SinkFlow> {
        let path = || data_from(key.as_bytes());
        let mut m_lines = 0u64;
        let mut m_total = 0u64;
        let mut printed = 0u64;
        let mut buffer = Vec::new();
        fn push(buffer: &mut Vec<u8>, printed: &mut u64, msg: &JsonMsg<'_>) -> Result<()> {
            let start = buffer.len();
            serde_json::to_writer(&mut *buffer, msg)?;
            buffer.push(b'\n');
            *printed += (buffer.len() - start) as u64;
            Ok(())
        }
        push(
            &mut buffer,
            &mut printed,
            &JsonMsg::Begin(BeginData { path: path() }),
        )?;
        for event in doc.events {
            let line = LineData {
                path: path(),
                lines: data_from(&event.text),
                line_number: Some(event.line),
                absolute_offset: event.offset,
                submatches: event
                    .submatches
                    .iter()
                    .map(|s| SubMatchData {
                        matched: data_from(&event.text[s.start..s.end]),
                        start: s.start,
                        end: s.end,
                    })
                    .collect(),
            };
            match event.kind {
                LineKind::Match => {
                    m_lines += 1;
                    m_total += event.submatches.len() as u64;
                    push(&mut buffer, &mut printed, &JsonMsg::Match(line))?;
                }
                LineKind::Context => push(&mut buffer, &mut printed, &JsonMsg::Context(line))?,
            }
        }
        let bytes_printed_so_far = printed;
        push(
            &mut buffer,
            &mut printed,
            &JsonMsg::End(EndData {
                path: path(),
                binary_offset: None,
                stats: StatsData {
                    elapsed: elapsed_from(doc.elapsed),
                    searches: 1,
                    searches_with_match: 1,
                    bytes_searched: doc.bytes_searched,
                    bytes_printed: bytes_printed_so_far,
                    matched_lines: m_lines,
                    matches: m_total,
                },
            }),
        )?;
        self.matched_lines.fetch_add(m_lines, Ordering::Relaxed);
        self.matches.fetch_add(m_total, Ordering::Relaxed);
        self.docs_with_match.fetch_add(1, Ordering::Relaxed);
        self.bytes_searched
            .fetch_add(doc.bytes_searched, Ordering::Relaxed);
        self.bytes_printed.fetch_add(printed, Ordering::Relaxed);
        self.elapsed_nanos
            .fetch_add(doc.elapsed.as_nanos() as u64, Ordering::Relaxed);
        let mut out = self
            .out
            .lock()
            .map_err(|_| anyhow::anyhow!("output writer poisoned"))?;
        let written = out.write_all(&buffer).and_then(|()| out.flush());
        match written {
            Ok(()) => Ok(SinkFlow::Continue),
            Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Ok(SinkFlow::Stop),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn render(msg: &JsonMsg<'_>) -> String {
        serde_json::to_string(msg).unwrap()
    }

    #[test]
    fn wire_format_matches_rg_shapes() {
        assert_eq!(
            render(&JsonMsg::Begin(BeginData {
                path: data_from(b"logs/a")
            })),
            r#"{"type":"begin","data":{"path":{"text":"logs/a"}}}"#
        );
        assert_eq!(
            render(&JsonMsg::Match(LineData {
                path: data_from(b"k"),
                lines: data_from(b"hit me\n"),
                line_number: Some(5),
                absolute_offset: 100,
                submatches: vec![SubMatchData {
                    matched: data_from(b"hit"),
                    start: 0,
                    end: 3
                }],
            })),
            r#"{"type":"match","data":{"path":{"text":"k"},"lines":{"text":"hit me\n"},"line_number":5,"absolute_offset":100,"submatches":[{"match":{"text":"hit"},"start":0,"end":3}]}}"#
        );
        let end = render(&JsonMsg::End(EndData {
            path: data_from(b"k"),
            binary_offset: None,
            stats: StatsData {
                elapsed: elapsed_from(std::time::Duration::from_nanos(1500)),
                searches: 1,
                searches_with_match: 1,
                bytes_searched: 10,
                bytes_printed: 20,
                matched_lines: 1,
                matches: 1,
            },
        }));
        assert!(
            end.starts_with(r#"{"type":"end","data":{"path":{"text":"k"},"binary_offset":null,"#)
        );
        // non-UTF8 falls back to base64 bytes
        assert_eq!(
            render(&JsonMsg::Begin(BeginData {
                path: data_from(&[0xff, 0xfe])
            })),
            r#"{"type":"begin","data":{"path":{"bytes":"//4="}}}"#
        );
    }
}
