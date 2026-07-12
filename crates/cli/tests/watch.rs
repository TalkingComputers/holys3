#![cfg(unix)]

use anyhow::{Context, Result};
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

fn receive_event(
    receiver: &Receiver<String>,
    event_type: &str,
    minimum_cycle: u64,
) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .with_context(|| format!("timed out waiting for {event_type} event"))?;
        let line = receiver
            .recv_timeout(remaining)
            .with_context(|| format!("receiving {event_type} event"))?;
        let event = serde_json::from_str::<Value>(&line).context("parsing index status JSON")?;
        if event["type"] == event_type
            && event["cycle"]
                .as_u64()
                .is_some_and(|cycle| cycle >= minimum_cycle)
        {
            return Ok(event);
        }
    }
}

#[test]
fn watch_indexes_changes_and_stops_on_sigterm() -> Result<()> {
    let target = tempfile::tempdir()?;
    let index = tempfile::tempdir()?;
    std::fs::write(target.path().join("first.log"), "first\n")?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_holys3"))
        .arg("index")
        .arg(target.path())
        .arg("--out")
        .arg(index.path())
        .args(["--watch", "--interval", "1", "--json"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("starting index watch")?;
    let stdout = child.stdout.take().context("taking index watch stdout")?;
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if sender.send(line).is_err() {
                break;
            }
        }
    });

    let test_result = (|| -> Result<()> {
        let initial = receive_event(&receiver, "indexed", 1)?;
        anyhow::ensure!(
            initial["total_docs"] == 1,
            "unexpected initial event: {initial}"
        );
        std::fs::write(target.path().join("second.log"), "WATCHNEEDLE\n")?;
        let changed = loop {
            let event = receive_event(&receiver, "indexed", 2)?;
            if event["added"] == 1 && event["total_docs"] == 2 {
                break event;
            }
        };
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .context("sending SIGTERM to index watch")?;
        anyhow::ensure!(status.success(), "kill -TERM failed with {status}");
        let stopped = receive_event(
            &receiver,
            "stopped",
            changed["cycle"].as_u64().context("missing changed cycle")?,
        )?;
        anyhow::ensure!(
            stopped["target"].as_str() == target.path().to_str(),
            "unexpected stopped event"
        );
        let status = child.wait().context("waiting for index watch")?;
        anyhow::ensure!(status.success(), "index watch exited with {status}");
        let output = Command::new(env!("CARGO_BIN_EXE_holys3"))
            .arg("WATCHNEEDLE")
            .arg(target.path())
            .arg("--index")
            .arg(index.path())
            .output()
            .context("searching watched index")?;
        anyhow::ensure!(
            output.status.success(),
            "search failed with {}",
            output.status
        );
        Ok(())
    })();

    if child.try_wait()?.is_none() {
        child.kill()?;
        child.wait()?;
    }
    reader
        .join()
        .map_err(|_| anyhow::anyhow!("index stdout reader panicked"))?;
    test_result
}
