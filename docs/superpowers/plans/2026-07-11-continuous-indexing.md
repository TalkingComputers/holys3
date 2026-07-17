# Continuous Indexing Implementation Plan

> [!NOTE]
> Historical planning record from 2026-07-11. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add provider-neutral continuous indexing with strict status events, graceful termination, post-start retries, crash-consistency coverage, and deterministic churn performance gates.

**Architecture:** Keep source listing and index mutation in the existing local/S3 paths. Add one CLI-owned control-loop module around `update_index`; it serializes attempts, owns signal/wait behavior, and emits status without changing the atomic index protocol. Extend the existing benchmark binary with steady-cardinality local log churn and gate it in the established benchmark workflow.

**Tech Stack:** Rust 1.88, clap derive, ctrlc 3.5.2 with `termination`, serde/serde_json, std mpsc, existing seagrep index/store APIs, GitHub Actions, MinIO/local fixtures.

## Global Constraints

- Do not access live AWS; tests use local files or MinIO only.
- Do not add provider-specific notification or daemon state.
- `--watch` and `--interval SECONDS` require each other; zero is invalid and no interval default exists.
- The first attempt fails fast; only post-success failures retry.
- `--rebuild` is true only for cycle 1.
- Cycles never overlap, and waits begin after attempts complete.
- SIGINT, SIGTERM, SIGHUP, Windows Ctrl-C, and Windows Ctrl-Break stop the loop.
- No new source comments, fallback behavior, `any`, underscore-prefixed names, or function names containing `resolve`, `ensure`, or `handle`.
- Preserve current one-shot human output and exit codes.
- JSON Lines event schemas are exact and additive only through a future versioned change.
- Keep the S3 boundary in `seagrep-s3`; no network IO enters core/query/index.

---

### Task 1: Continuous Index Control Loop

**Files:**
- Create: `crates/cli/src/index.rs`
- Modify: `crates/cli/src/main.rs` by adding `mod index;` only.
- Modify: `crates/cli/Cargo.toml` by adding `ctrlc = { version = "3.5.2", features = ["termination"] }`.
- Modify: `Cargo.lock` through `cargo check --locked` after dependency resolution.

**Interfaces:**
- Consumes: `seagrep_index::UpdateReport` and a caller closure `FnMut(bool) -> anyhow::Result<IndexResult>`.
- Produces:

```rust
pub(crate) struct IndexConfig<'a> {
    pub target: &'a str,
    pub interval: Option<std::time::Duration>,
    pub rebuild: bool,
    pub json: bool,
}

pub(crate) struct IndexResult {
    pub report: seagrep_index::UpdateReport,
    pub location: String,
}

pub(crate) fn run_index(
    config: IndexConfig<'_>,
    build: impl FnMut(bool) -> anyhow::Result<IndexResult>,
) -> anyhow::Result<()>;
```

`IndexConfig` input schema:

```rust
pub(crate) struct IndexConfig<'a> {
    pub target: &'a str,
    pub interval: Option<Duration>,
    pub rebuild: bool,
    pub json: bool,
}
```

`IndexResult` output schema:

```rust
pub(crate) struct IndexResult {
    pub report: UpdateReport,
    pub location: String,
}
```

`run_index` output schema: `Ok(())` after one successful one-shot attempt or clean watched shutdown. Errors preserve the full `anyhow` chain from signal installation, duration conversion, status serialization/write, first attempt, or one-shot attempt. Post-success build errors are emitted and not returned.

Transformation:

1. If `interval` is `None`, call `run_cycles` without a stop receiver.
2. If `interval` is `Some`, reject zero, install one bounded stop channel through `ctrlc::try_set_handler`, and call `run_cycles` with its receiver.
3. `run_cycles` increments a checked `u64` cycle counter, calls `build(config.rebuild && cycle == 1)`, measures elapsed time, and emits one success or error event.
4. A first or one-shot error is emitted in JSON mode and returned. A post-success watched error is emitted and retained only as status.
5. After every watched attempt, consume a pending stop immediately; otherwise wait exactly `interval` with `recv_timeout`.
6. A received or disconnected stop channel emits `stopped` and returns `Ok(())`; timeout starts the next cycle.

Every function in the new file:

```rust
pub(crate) fn run_index(
    config: IndexConfig<'_>,
    build: impl FnMut(bool) -> Result<IndexResult>,
) -> Result<()>;

fn run_cycles(
    config: IndexConfig<'_>,
    stop: Option<&std::sync::mpsc::Receiver<()>>,
    output: &mut dyn std::io::Write,
    build: &mut dyn FnMut(bool) -> Result<IndexResult>,
) -> Result<()>;

fn install_stop_channel() -> Result<std::sync::mpsc::Receiver<()>>;

fn write_event(output: &mut dyn std::io::Write, event: &IndexEvent<'_>) -> Result<()>;

fn write_stopped(
    config: IndexConfig<'_>,
    cycle: u64,
    output: &mut dyn std::io::Write,
) -> Result<()>;

fn print_report(cycle: Option<u64>, result: &IndexResult);

fn elapsed_ms(duration: std::time::Duration) -> Result<u64>;
```

Function schemas and transformations:

- `run_cycles`: inputs are immutable config, paired optional receiver, mutable status writer, and mutable build closure. Output is `Ok(())` or the exact errors described above. It transforms attempts into ordered tagged events and never overlaps closures.
- `install_stop_channel`: no input. Output is one `Receiver<()>`; error is `ctrlc::Error` with context `installing termination signal handler`. It creates `sync_channel(1)` and uses nonblocking `try_send(())` so signals coalesce.
- `write_event`: input is one event and writer. Output is one compact JSON object plus `\n`, flushed; serialization and IO errors retain context.
- `write_stopped`: input is config, last attempted cycle, writer. Output is a JSON `stopped` event or human stderr line.
- `print_report`: input is optional watched cycle and complete `IndexResult`; output is existing human wording on stderr, with `cycle N: ` only when watched. It cannot throw.
- `elapsed_ms`: input is elapsed duration; output is checked `u64` milliseconds; overflow returns `index attempt duration exceeds u64 milliseconds`.

Exact private event type:

```rust
#[derive(serde::Serialize)]
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
```

- [ ] **Step 1: Add failing state-machine tests at the bottom of `index.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::sync::mpsc;

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
        let mut build = |rebuild| {
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
```

Each test uses `Duration::ZERO` only inside `run_cycles`, keeps the channel sender alive for timeout cases, sends a stop token from the final closure invocation, and asserts exact event ordering and fields.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p seagrep index::tests -- --nocapture`

Expected: compile failure because `IndexConfig`, `IndexResult`, `IndexEvent`, and `run_cycles` do not exist.

- [ ] **Step 3: Implement the module exactly from the contracts above**

```rust
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
                    return Err(error);
                }
                if !config.json {
                    eprintln!("cycle {cycle}: index failed: {error:#}");
                }
            }
        }
        let Some(stop) = stop else {
            return Ok(());
        };
        if stop.try_recv().is_ok() {
            return write_stopped(config, cycle, output);
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
    ctrlc::try_set_handler(move || {
        let _ = sender.try_send(());
    })
    .context("installing termination signal handler")?;
    Ok(receiver)
}

fn write_event(output: &mut dyn Write, event: &IndexEvent<'_>) -> Result<()> {
    serde_json::to_writer(&mut *output, event).context("writing index status JSON")?;
    output.write_all(b"\n").context("writing index status newline")?;
    output.flush().context("flushing index status")
}

fn write_stopped(
    config: IndexConfig<'_>,
    cycle: u64,
    output: &mut dyn Write,
) -> Result<()> {
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

fn elapsed_ms(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .context("index attempt duration exceeds u64 milliseconds")
}
```

```rust
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
```

- [ ] **Step 4: Resolve and lock the dependency**

Run: `cargo check -p seagrep`

Expected: Cargo.lock records `ctrlc 3.5.2`; check succeeds on Rust 1.88+.

- [ ] **Step 5: Run focused tests**

Run: `cargo test -p seagrep index::tests -- --nocapture`

Expected: 5 passed, 0 failed.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/cli/Cargo.toml crates/cli/src/index.rs crates/cli/src/main.rs
git commit -m "feat: add continuous index control loop"
```

---

### Task 2: CLI Wiring and Real Process Coverage

**Files:**
- Modify: `crates/cli/src/main.rs` command schema, `build_local`, `build_s3`, and `run`.
- Modify: `crates/cli/tests/cli.rs` with argument and one-shot JSON tests.
- Create: `crates/cli/tests/watch.rs` with a Unix real-process SIGTERM test.

**Interfaces:**
- Consumes: `index::IndexConfig`, `index::IndexResult`, `index::run_index` from Task 1.
- Produces the CLI contract `seagrep index TARGET [--watch --interval SECONDS] [--json]`.

Changed `Cmd::Index` field schema:

```rust
Index {
    target: String,
    out: PathBuf,
    strategy: StrategyArg,
    rebuild: bool,
    watch: bool,
    interval: Option<u64>,
    json: bool,
    connect: ConnectArgs,
}
```

Changed function contracts:

```rust
fn build_local(
    dir: &Path,
    out: &Path,
    strategy: Strategy,
    rebuild: bool,
) -> Result<index::IndexResult>;

fn build_s3(
    src: &S3Source,
    strategy: Strategy,
    rebuild: bool,
) -> Result<index::IndexResult>;

fn run() -> Result<bool>;
```

- `build_local` input/output: required source directory, index directory, strategy, and per-cycle rebuild flag become the existing listing/update pipeline. Output wraps `UpdateReport` with `out.display().to_string()`. Existing canonicalization, target/index containment error, listing errors, update errors, and cache errors are unchanged. It no longer prints.
- `build_s3` input/output: borrowed opened S3 source, strategy, and per-cycle rebuild flag become a fresh filtered listing and existing update pipeline. Output wraps `UpdateReport` with `s3://{bucket}/{build_index_namespace(prefix)}`. It no longer consumes the source or prints.
- `run` input is parsed process arguments/environment. For index mode, it opens the source once, validates `--out` for S3 before starting, converts interval seconds to `Duration`, then calls `run_index` with a closure borrowing the opened source. Output remains `Ok(true)` after clean completion; errors retain current exit-code mapping in `main`.

Transformation in `run`:

1. Parse the new clap fields.
2. Convert `interval.map(Duration::from_secs)`; clap has already rejected zero and invalid pairing.
3. Open `Source` once so SSO credential refresh remains active across cycles.
4. Validate S3 `--out` once before installing the signal handler.
5. Call `index::run_index(IndexConfig { target: &target, interval, rebuild, json }, |cycle_rebuild| ...)`.
6. The closure calls `build_local` or borrowed `build_s3` and returns `IndexResult`.

Every new test function:

```rust
#[test]
fn index_watch_flags_are_paired();

#[test]
fn index_json_reports_update();
```

`index_watch_flags_are_paired` invokes three invalid forms: `--watch` alone, `--interval 1` alone, and `--watch --interval 0`; each exits 2 and names the missing/invalid argument. `index_json_reports_update` indexes one local file with `--json`, parses exactly one stdout line, and asserts the complete `indexed` schema.

Every function in `crates/cli/tests/watch.rs`:

```rust
fn receive_event(
    receiver: &std::sync::mpsc::Receiver<String>,
    event_type: &str,
    minimum_cycle: u64,
) -> anyhow::Result<serde_json::Value>;

#[test]
fn watch_indexes_changes_and_stops_on_sigterm() -> anyhow::Result<()>;
```

- `receive_event` input is the line channel, required event tag, and minimum cycle. Output is the first matching parsed object within 15 seconds. Timeout/disconnect/JSON errors are returned with context. It repeatedly computes remaining deadline time; no fixed sleep.
- `watch_indexes_changes_and_stops_on_sigterm` creates one source, starts the real binary with `--watch --interval 1 --json`, forwards stdout lines from a reader thread, waits for cycle 1, adds a second source, waits for an `indexed` event with `cycle >= 2`, `added == 1`, and `total_docs == 2`, sends `kill -TERM PID`, requires `stopped`, requires process success, then searches the new token through the resulting index. Cleanup kills/waits the child even when assertions fail.

- [ ] **Step 1: Write the failing clap and JSON tests**

Add the two exact tests to `cli.rs`; use `tempfile`, existing `seagrep()`, and `serde_json::Value`.

- [ ] **Step 2: Run tests to verify failure**

Run: `cargo test -p seagrep --test cli index_ -- --nocapture`

Expected: clap rejects unknown `--watch`/`--json`, so tests fail.

- [ ] **Step 3: Add CLI fields and wire `run_index`**

Use these clap relations:

```rust
#[arg(long, requires = "interval", help = "Continuously update the index")]
watch: bool,
#[arg(
    long,
    value_name = "SECONDS",
    requires = "watch",
    value_parser = parse_positive_u64,
    help = "Wait SECONDS after each index attempt"
)]
interval: Option<u64>,
#[arg(long, help = "Emit one JSON status object per line")]
json: bool,
```

Refactor only the report printing out of `build_local` and `build_s3`; preserve all existing listing/update operations.

- [ ] **Step 4: Run clap and JSON tests**

Run: `cargo test -p seagrep --test cli index_ -- --nocapture`

Expected: both tests pass.

- [ ] **Step 5: Write the real watch SIGTERM test**

Create `watch.rs` with `#![cfg(unix)]`, spawn `env!("CARGO_BIN_EXE_seagrep")`, use a reader thread and `recv_timeout`, invoke the system `kill` command with `-TERM`, and perform the exact assertions in the contract.

- [ ] **Step 6: Run the real process test**

Run: `cargo test -p seagrep --test watch -- --nocapture`

Expected: 1 passed in roughly 1-3 seconds; child exits 0 and final search succeeds.

- [ ] **Step 7: Run all CLI tests**

Run: `cargo test -p seagrep`

Expected: all CLI unit/integration/doc tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/cli/src/main.rs crates/cli/tests/cli.rs crates/cli/tests/watch.rs
git commit -m "feat: expose continuous indexing CLI"
```

---

### Task 3: Interrupted Root-Swap Recovery Proof

**Files:**
- Modify: `crates/index/tests/segmented.rs` with one store wrapper and one integration test.

**Interfaces:**
- Consumes: unchanged `BlobStore`, `LocalBlobStore`, `update_index`, and existing `assert_matches_oracle`.
- Produces no library API; it proves current crash consistency.

New test-only type:

```rust
struct RejectSwapStore {
    inner: LocalBlobStore,
}
```

Every method and exact forwarding contract:

```rust
impl BlobStore for RejectSwapStore {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()>;
    fn put_file(&self, name: &str, path: &Path) -> Result<()>;
    fn get(&self, name: &str) -> Result<Option<Vec<u8>>>;
    fn get_range(&self, name: &str, start: u64, len: u64) -> Result<Vec<u8>>;
    fn delete(&self, name: &str) -> Result<()>;
    fn get_versioned(&self, name: &str) -> Result<Option<(Vec<u8>, String)>>;
    fn put_if(&self, name: &str, bytes: &[u8], expected: Option<&str>) -> Result<bool>;
}
```

All methods forward input and output unchanged to `inner` except `put_if`: when `name == "segments.bin"`, return `Ok(false)` without mutating the root; otherwise forward. No method swallows IO errors.

New test:

```rust
#[test]
fn interrupted_root_swap_preserves_old_index_and_restart_converges() -> Result<()>;
```

Input schema: temporary store/cache directories; old bucket with `old.log = "OLD_NEEDLE"`; new bucket clone that adds `new.log = "NEW_NEEDLE"`.

Output schema: `Ok(())`; any build, search, or assertion failure fails the test. The rejected update must contain `concurrently`; old snapshot search must still find only `OLD_NEEDLE`; normal restart must match the new bucket oracle for both tokens.

Transformation:

1. Build and verify the old bucket.
2. Save the exact `segments.bin` bytes.
3. Run the new listing through `RejectSwapStore`; segment writes happen, root swap returns false.
4. Assert the root bytes are unchanged.
5. Search through the unchanged root with the old bucket snapshot.
6. Rerun normal `reindex` with the new bucket.
7. Assert indexed search equals the new full-scan oracle.

- [ ] **Step 1: Write the test and wrapper**

Place the wrapper near existing `RacingStore` coverage and write the exact flow above.

- [ ] **Step 2: Run the focused test**

Run: `cargo test -p seagrep-index --test segmented interrupted_root_swap -- --nocapture`

Expected: pass against the existing atomic CAS implementation. If it fails, fix the root cause in `update_index` before proceeding and add the changed function contract to this plan.

- [ ] **Step 3: Run segmented lifecycle tests**

Run: `cargo test -p seagrep-index --test segmented`

Expected: all lifecycle, race, GC, and recovery tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/index/tests/segmented.rs
git commit -m "test: prove interrupted index recovery"
```

---

### Task 4: Deterministic Churn Benchmark

**Files:**
- Create: `crates/xbench/src/churn.rs`.
- Modify: `crates/xbench/src/main.rs` by adding `mod churn`, `Command::Churn`, one match arm, and making `percentile_ms` `pub(crate)`.
- Modify: `crates/xbench/src/gen.rs` by adding `churn_run_path`.

**Interfaces:**
- Consumes: existing generated manifest/corpus, local index, `update_index`, `search_streaming`, `LocalFetcher`, and `percentile_ms`.
- Produces:

```rust
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ChurnSummary {
    pub cycles: usize,
    pub changes_per_cycle: usize,
    pub total_docs: usize,
    pub listing_p50_ms: f64,
    pub listing_p95_ms: f64,
    pub update_p50_ms: f64,
    pub update_p95_ms: f64,
    pub final_segments: usize,
}

pub(crate) fn run(cycles: usize, changes: usize) -> anyhow::Result<ChurnSummary>;

pub(crate) fn churn_run_path() -> PathBuf;

pub(crate) fn percentile_ms(values: &[Duration], percentile: usize) -> f64;
```

`Command::Churn` input schema:

```rust
Churn {
    #[arg(long)]
    cycles: usize,
    #[arg(long)]
    changes: usize,
}
```

`run` input schema: required positive cycle count and positive changed-source count; `changes <= manifest.objects`. Existing seed manifest, source files, and local index must exist. Output is the exact `ChurnSummary`; missing state, arithmetic overflow, IO errors, unexpected update counts, search mismatch, or JSON write failure returns an error with context.

Every function in `churn.rs`:

```rust
pub(crate) fn run(cycles: usize, changes: usize) -> Result<ChurnSummary>;

fn build_churn_path(sequence: usize) -> PathBuf;

fn build_churn_body(seed: u64, sequence: usize, size: usize) -> Result<Vec<u8>>;

fn write_churn_source(path: &Path, bytes: &[u8]) -> Result<()>;
```

- `build_churn_path`: input sequence; output `objects_dir()/year=2026/month=07/day=12/hour={sequence % 24:02}/churn-{sequence:08}.jsonl`; cannot throw.
- `build_churn_body`: inputs seed, sequence, exact byte size. Output is one deterministic JSONL record containing timestamp, level, service, request ID, `CHURN_NEEDLE`, and sequence; it pads the JSON string message with ASCII spaces so the closing quote, brace, and newline end exactly at `size`. It rejects a size shorter than the fixed prefix plus suffix and checked-capacity overflow.
- `write_churn_source`: input path and complete body. Output `Ok(())`; creates the parent directories and writes the entire body, preserving IO errors.
- `run`: validates inputs, initializes a FIFO from manifest document paths, and for each cycle deletes exactly `changes` oldest paths, writes exactly `changes` new paths/bodies, lists the corpus, calls `update_index`, validates counts/cardinality, and records durations. It then opens the final index, searches `CHURN_NEEDLE`, requires `min(cycles * changes, total_docs)` hit documents, sorts timing vectors through `percentile_ms`, writes pretty JSON plus newline to `churn_run_path`, and returns the same summary.

Changed existing function:

```rust
pub(crate) fn percentile_ms(values: &[Duration], percentile: usize) -> f64;
```

Input schema: nonempty sorted durations and percentile 1..=100. Output schema: selected nearest-rank duration in milliseconds. Transformation remains the existing saturating nearest-rank calculation; only visibility changes.

New tests in `churn.rs`:

```rust
#[test]
fn churn_body_is_deterministic_and_exact() -> Result<()>;

#[test]
fn churn_paths_are_date_partitioned();
```

- The body test calls the function twice with `(7, 42, 4096)`, requires equality, exact 4096 length, valid UTF-8 prefix, and `CHURN_NEEDLE`.
- The path test requires the exact path suffix `year=2026/month=07/day=12/hour=18/churn-00000042.jsonl`.

- [ ] **Step 1: Write failing generation tests and command parse test**

Add the two unit tests and extend the existing clap debug assertion through the new enum variant.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `cargo test -p seagrep-bench churn -- --nocapture`

Expected: compile failure because the churn module/functions do not exist.

- [ ] **Step 3: Implement deterministic generation and benchmark flow**

Use `VecDeque<PathBuf>` for live-source FIFO, `Instant` for separate listing/update durations, existing local index/cache paths, and `serde_json::to_writer_pretty` for the summary. Do not introduce a random or time dependency.

- [ ] **Step 4: Run unit tests**

Run: `cargo test -p seagrep-bench churn -- --nocapture`

Expected: 2 passed, 0 failed.

- [ ] **Step 5: Run a small end-to-end churn benchmark**

```bash
cargo run --locked -p seagrep-bench -- seed --seed 1 --objects 100 --size 4096
cargo run --locked -p seagrep-bench -- upload --target dir
cargo run --locked -p seagrep-bench -- churn --cycles 3 --changes 10
```

Expected: summary reports `cycles=3`, `changes_per_cycle=10`, `total_docs=100`; `crates/xbench/runs/churn.json` parses and final search validates 30 `CHURN_NEEDLE` documents.

- [ ] **Step 6: Commit**

```bash
git add crates/xbench/src/churn.rs crates/xbench/src/gen.rs crates/xbench/src/main.rs
git commit -m "bench: measure incremental index churn"
```

---

### Task 5: CI Churn and Workflow Gates

**Files:**
- Modify: `.github/workflows/bench.yml` scale job and artifact list.

**Interfaces:**
- Consumes: `seagrep-bench churn --cycles 10 --changes 250` and `crates/xbench/runs/churn.json` from Task 4.
- Produces artifacts `bench-churn.txt`, `bench-churn-rss.txt`, and `bench-churn.json`; failures feed the existing `benchmarks-success` gate.

Workflow input/output schema:

```json
{
  "cycles": 10,
  "changes_per_cycle": 250,
  "total_docs": 25000,
  "listing_p50_ms": "positive finite number",
  "listing_p95_ms": "number <= 2000",
  "update_p50_ms": "positive finite number",
  "update_p95_ms": "number <= 5000",
  "final_segments": "positive integer"
}
```

Transformation:

1. Reuse the 25,000-object corpus and index produced by `Profile 25,000-object index`.
2. Run churn under `/usr/bin/time -v` and tee human output.
3. Copy the exact JSON summary to an artifact path.
4. Extend the existing Python memory/performance gate to parse JSON, validate all exact fields/limits, and require churn peak RSS <= 300 MiB.
5. Upload all three files with `if-no-files-found: error` through the existing scale artifact action.

- [ ] **Step 1: Add the benchmark step**

```yaml
      - name: Profile incremental churn
        run: |
          /usr/bin/time -v -o bench-churn-rss.txt target/release/seagrep-bench churn --cycles 10 --changes 250 | tee bench-churn.txt
          cp crates/xbench/runs/churn.json bench-churn.json
```

- [ ] **Step 2: Extend the Python gate with exact checks**

Load `bench-churn.json`; require the exact integer fields above, finite positive timings, `listing_p95_ms <= 2000`, `update_p95_ms <= 5000`, and `final_segments > 0`. Parse `bench-churn-rss.txt` with the existing maximum-resident-set-size logic and require `<= 300 * 1024` KiB.

- [ ] **Step 3: Add artifact paths**

Add `bench-churn.txt`, `bench-churn-rss.txt`, and `bench-churn.json` under the scale upload action.

- [ ] **Step 4: Validate workflow syntax**

Run: `actionlint`

Expected: exit 0 with no output.

- [ ] **Step 5: Run the full local scale workload once**

```bash
cargo build --locked --release -p seagrep -p seagrep-bench
target/release/seagrep-bench seed --seed 1 --objects 25000 --size 4096
target/release/seagrep-bench upload --target dir
/usr/bin/time -l target/release/seagrep-bench churn --cycles 10 --changes 250
```

Expected: exact summary checks pass; listing p95 <= 2,000 ms and update p95 <= 5,000 ms. On macOS, inspect `maximum resident set size` in bytes and require <= 314,572,800.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/bench.yml
git commit -m "ci: gate continuous index churn"
```

---

### Task 6: User and Architecture Documentation

**Files:**
- Modify: `README.md` quickstart, index behavior, performance coverage, and limitations.
- Modify: `CHANGELOG.md` Unreleased section and two incorrect format references.
- Modify: `ARCHITECTURE.md` index pipeline and cross-cutting execution behavior.

**Interfaces:**
- No code interfaces.
- Produces public CLI documentation matching the exact Task 2 contract and correct format 9 references.

Documentation transformation:

1. In README quickstart, add `seagrep index ./logs --out seagrep.idxdir --watch --interval 30` and an S3 equivalent using a generic profile.
2. State that startup failures exit, post-start failures retry, cycles do not overlap, and `--rebuild` affects cycle 1 only.
3. Document `--json` tagged `indexed`, `error`, and `stopped` events without adding fields outside the approved schema.
4. Add the 25,000-object/1%-per-cycle churn gate to continuous benchmark coverage.
5. Replace README `format-8` with `format-9`.
6. Under CHANGELOG Unreleased, add watch/status/churn coverage and correct both historical `index format 8` references to `index format 9`, because released code already writes 9.
7. In Architecture, state that continuous mode reuses the same listing/diff/CAS transaction, opens one refreshing S3 client, serializes cycles, and adds no daemon database.

- [ ] **Step 1: Edit the three documents**

Use only exact behavior verified by tests; do not promise notification-driven updates, background daemonization, or live AWS results.

- [ ] **Step 2: Verify command/help agreement**

Run: `cargo run --locked -p seagrep -- index --help`

Expected help contains `--watch`, `--interval <SECONDS>`, and `--json`; README examples use exactly those forms.

- [ ] **Step 3: Scan for stale format references**

Run: `rg -n "format-8|format 8|INDEX_FORMAT" README.md CHANGELOG.md ARCHITECTURE.md crates/index/src/lib.rs`

Expected: user-facing docs and `INDEX_FORMAT` all state 9; no format-8 reference remains.

- [ ] **Step 4: Commit**

```bash
git add README.md CHANGELOG.md ARCHITECTURE.md
git commit -m "docs: document continuous indexing"
```

---

### Task 7: Full Verification and Delivery

**Files:**
- Modify only files required by failures rooted in Tasks 1-6.
- Do not change versions manually; release-plz determines the next release from conventional commits.

**Interfaces:**
- Consumes the complete branch.
- Produces a clean, reviewable branch with no warnings, stale dependencies, benchmark regressions, or uncommitted generated files.

- [ ] **Step 1: Format**

Run: `cargo fmt --all`

Expected: exit 0.

- [ ] **Step 2: Run workspace tests**

Run: `cargo test --locked --workspace --all-features`

Expected: all unit, integration, real watch, crash recovery, and doc tests pass.

- [ ] **Step 3: Run release tests**

Run: `cargo test --locked --release --workspace --all-features`

Expected: all tests pass in release mode.

- [ ] **Step 4: Run strict lint and docs**

```bash
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --workspace --all-features --no-deps
```

Expected: both exit 0 with no warnings.

- [ ] **Step 5: Run dependency and workflow checks**

```bash
cargo machete
cargo deny check
actionlint
```

Expected: all exit 0.

- [ ] **Step 6: Verify clean packages**

Run the repository's existing package workspace CI command from `.github/workflows/ci.yml` exactly.

Expected: all six public crates package from a clean registry without missing files.

- [ ] **Step 7: Inspect branch state**

```bash
git diff --check
git status --short --branch
git log --oneline origin/main..HEAD
```

Expected: no whitespace errors, no generated churn corpus/index files, and only intentional commits/files.

- [ ] **Step 8: Push and open a PR**

Push `feat/continuous-indexing`, open a PR describing the exact CLI contract and benchmark evidence, then wait for CI, security, benchmark, and automated review completion before merge.
