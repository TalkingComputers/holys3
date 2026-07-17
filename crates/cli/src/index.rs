use anyhow::{Context, Result};
use seagrep_index::UpdateReport;
use serde::Serialize;
use std::io::Write;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(crate) struct IndexConfig<'a> {
    pub target: &'a str,
    pub interval: Option<Duration>,
    pub rebuild: bool,
    pub json: bool,
}

pub(crate) struct IndexResult {
    pub report: UpdateReport,
    pub location: String,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum IndexEvent<'a> {
    Indexed {
        cycle: u64,
        target: &'a str,
        duration_ms: u64,
        added: usize,
        removed: usize,
        total_docs: usize,
        segments: usize,
        compacted: bool,
        up_to_date: bool,
    },
    Error {
        cycle: u64,
        target: &'a str,
        duration_ms: u64,
        error: String,
    },
    Stopped {
        cycle: u64,
        target: &'a str,
    },
}

pub(crate) fn run_index(
    config: IndexConfig<'_>,
    mut build: impl FnMut(bool) -> Result<IndexResult>,
) -> Result<()> {
    let mut output = std::io::stdout().lock();
    match config.interval {
        Some(interval) => {
            anyhow::ensure!(!interval.is_zero(), "watch interval must be greater than 0");
            let stop = install_stop_channel()?;
            run_cycles(config, Some(&stop), &mut output, &mut build)
        }
        None => run_cycles(config, None, &mut output, &mut build),
    }
}

pub(crate) fn write_start_error(
    target: &str,
    json: bool,
    duration: Duration,
    error: &anyhow::Error,
) -> Result<()> {
    if !json {
        return Ok(());
    }
    let mut output = std::io::stdout().lock();
    write_event(
        &mut output,
        &IndexEvent::Error {
            cycle: 1,
            target,
            duration_ms: elapsed_ms(duration)?,
            error: format!("{error:#}"),
        },
    )
}

fn run_cycles(
    config: IndexConfig<'_>,
    stop: Option<&Receiver<()>>,
    output: &mut dyn Write,
    build: &mut dyn FnMut(bool) -> Result<IndexResult>,
) -> Result<()> {
    anyhow::ensure!(
        config.interval.is_some() == stop.is_some(),
        "watch interval and stop receiver must be paired"
    );
    let watched = stop.is_some();
    let mut cycle = 0u64;
    let mut succeeded = false;
    loop {
        let mut fail_fast_error = None;
        cycle = cycle.checked_add(1).context("index cycle overflow")?;
        let started = Instant::now();
        match build(config.rebuild && cycle == 1) {
            Ok(result) => {
                let duration_ms = elapsed_ms(started.elapsed())?;
                if config.json {
                    let report = &result.report;
                    write_event(
                        output,
                        &IndexEvent::Indexed {
                            cycle,
                            target: config.target,
                            duration_ms,
                            added: report.added,
                            removed: report.removed,
                            total_docs: report.total_docs,
                            segments: report.segments,
                            compacted: report.compacted,
                            up_to_date: report.up_to_date,
                        },
                    )?;
                } else {
                    print_report(watched.then_some(cycle), &result);
                }
                succeeded = true;
            }
            Err(error) => {
                let duration_ms = elapsed_ms(started.elapsed())?;
                if config.json {
                    write_event(
                        output,
                        &IndexEvent::Error {
                            cycle,
                            target: config.target,
                            duration_ms,
                            error: format!("{error:#}"),
                        },
                    )?;
                }
                if !succeeded || !watched {
                    fail_fast_error = Some(error);
                } else if !config.json {
                    eprintln!("cycle {cycle}: index failed: {error:#}");
                }
            }
        }
        let Some(stop) = stop else {
            return match fail_fast_error {
                Some(error) => Err(error),
                None => Ok(()),
            };
        };
        if stop.try_recv().is_ok() {
            if let Some(error) = fail_fast_error.as_ref().filter(|_| !config.json) {
                eprintln!("cycle {cycle}: index failed: {error:#}");
            }
            write_stopped(config, cycle, output)?;
            return match fail_fast_error {
                Some(error) => Err(error),
                None => Ok(()),
            };
        }
        if let Some(error) = fail_fast_error {
            return Err(error);
        }
        match stop.recv_timeout(config.interval.context("watch interval missing")?) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => {
                return write_stopped(config, cycle, output);
            }
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn install_stop_channel() -> Result<Receiver<()>> {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    ctrlc::set_handler(move || {
        let _ = sender.try_send(());
    })
    .context("installing termination signal handler")?;
    Ok(receiver)
}

fn write_event(output: &mut dyn Write, event: &IndexEvent<'_>) -> Result<()> {
    serde_json::to_writer(&mut *output, event).context("writing index status JSON")?;
    output
        .write_all(b"\n")
        .context("writing index status newline")?;
    output.flush().context("flushing index status")
}

fn write_stopped(config: IndexConfig<'_>, cycle: u64, output: &mut dyn Write) -> Result<()> {
    if config.json {
        write_event(
            output,
            &IndexEvent::Stopped {
                cycle,
                target: config.target,
            },
        )
    } else {
        eprintln!("index watch stopped after cycle {cycle}");
        Ok(())
    }
}

fn print_report(cycle: Option<u64>, result: &IndexResult) {
    let prefix = match cycle {
        Some(cycle) => format!("cycle {cycle}: "),
        None => String::new(),
    };
    let report = &result.report;
    if report.up_to_date {
        eprintln!(
            "{prefix}index up to date: {} docs in {} segments at {}",
            report.total_docs, report.segments, result.location
        );
    } else {
        eprintln!(
            "{prefix}indexed +{} -{} -> {} docs in {} segments{} at {}",
            report.added,
            report.removed,
            report.total_docs,
            report.segments,
            if report.compacted { " (compacted)" } else { "" },
            result.location
        );
    }
}

fn elapsed_ms(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).context("index attempt duration exceeds u64 milliseconds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use seagrep_index::UpdateReport;
    use serde_json::Value;
    use std::sync::mpsc;
    use std::time::Duration;

    fn sample_result(up_to_date: bool) -> IndexResult {
        IndexResult {
            report: UpdateReport {
                added: if up_to_date { 0 } else { 1 },
                removed: 0,
                total_docs: 1,
                segments: 1,
                compacted: false,
                up_to_date,
            },
            location: "index-dir".to_owned(),
        }
    }

    fn parse_events(output: &[u8]) -> Vec<Value> {
        String::from_utf8_lossy(output)
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn one_shot_json_emits_one_indexed_event() {
        let mut output = Vec::new();
        let mut build = |rebuild: bool| {
            assert!(!rebuild);
            Ok(sample_result(false))
        };
        run_cycles(
            IndexConfig {
                target: "./logs",
                interval: None,
                rebuild: false,
                json: true,
            },
            None,
            &mut output,
            &mut build,
        )
        .unwrap();
        let events = parse_events(&output);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "indexed");
        assert_eq!(events[0]["cycle"], 1);
        assert_eq!(events[0]["target"], "./logs");
        assert!(events[0]["duration_ms"].as_u64().is_some());
        assert_eq!(events[0]["added"], 1);
        assert_eq!(events[0]["removed"], 0);
        assert_eq!(events[0]["total_docs"], 1);
        assert_eq!(events[0]["segments"], 1);
        assert_eq!(events[0]["compacted"], false);
        assert_eq!(events[0]["up_to_date"], false);
    }

    #[test]
    fn rebuild_only_applies_to_first_cycle() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut rebuilds = Vec::new();
        let mut build = |rebuild| {
            rebuilds.push(rebuild);
            if rebuilds.len() == 2 {
                sender.try_send(()).unwrap();
            }
            Ok(sample_result(false))
        };
        let mut output = Vec::new();
        run_cycles(
            IndexConfig {
                target: "./logs",
                interval: Some(Duration::ZERO),
                rebuild: true,
                json: true,
            },
            Some(&receiver),
            &mut output,
            &mut build,
        )
        .unwrap();
        assert_eq!(rebuilds, [true, false]);
        assert_eq!(
            parse_events(&output)
                .iter()
                .map(|event| event["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["indexed", "indexed", "stopped"]
        );
    }

    #[test]
    fn first_error_is_emitted_and_returned() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut output = Vec::new();
        let mut build = |_| anyhow::bail!("offline");
        let error = run_cycles(
            IndexConfig {
                target: "./logs",
                interval: Some(Duration::ZERO),
                rebuild: false,
                json: true,
            },
            Some(&receiver),
            &mut output,
            &mut build,
        )
        .unwrap_err();
        drop(sender);
        assert_eq!(error.to_string(), "offline");
        let events = parse_events(&output);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "error");
        assert_eq!(events[0]["cycle"], 1);
        assert_eq!(events[0]["error"], "offline");
    }

    #[test]
    fn first_error_with_pending_stop_emits_error_then_stopped() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut output = Vec::new();
        let mut build = |_| {
            sender.try_send(()).unwrap();
            anyhow::bail!("offline")
        };
        let error = run_cycles(
            IndexConfig {
                target: "./logs",
                interval: Some(Duration::from_secs(60)),
                rebuild: false,
                json: true,
            },
            Some(&receiver),
            &mut output,
            &mut build,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "offline");
        assert_eq!(
            parse_events(&output)
                .iter()
                .map(|event| event["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["error", "stopped"]
        );
    }

    #[test]
    fn post_start_error_retries() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut attempts = 0;
        let mut build = |_| {
            attempts += 1;
            match attempts {
                1 => Ok(sample_result(false)),
                2 => anyhow::bail!("temporary outage"),
                3 => {
                    sender.try_send(()).unwrap();
                    Ok(sample_result(true))
                }
                _ => unreachable!(),
            }
        };
        let mut output = Vec::new();
        run_cycles(
            IndexConfig {
                target: "./logs",
                interval: Some(Duration::ZERO),
                rebuild: false,
                json: true,
            },
            Some(&receiver),
            &mut output,
            &mut build,
        )
        .unwrap();
        assert_eq!(attempts, 3);
        assert_eq!(
            parse_events(&output)
                .iter()
                .map(|event| event["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["indexed", "error", "indexed", "stopped"]
        );
    }

    #[test]
    fn pending_stop_emits_completed_cycle_before_stopped() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut build = |_| {
            sender.try_send(()).unwrap();
            Ok(sample_result(false))
        };
        let mut output = Vec::new();
        run_cycles(
            IndexConfig {
                target: "./logs",
                interval: Some(Duration::from_secs(60)),
                rebuild: false,
                json: true,
            },
            Some(&receiver),
            &mut output,
            &mut build,
        )
        .unwrap();
        assert_eq!(
            parse_events(&output)
                .iter()
                .map(|event| event["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["indexed", "stopped"]
        );
    }
}
