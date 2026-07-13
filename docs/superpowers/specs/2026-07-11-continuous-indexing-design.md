# Continuous Indexing Design

> [!NOTE]
> Historical design record from 2026-07-11. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).

## Goal

Add a provider-neutral continuous indexing mode that repeatedly applies the existing incremental index update, reports each attempt in human or JSON form, survives post-start transient failures, exits cleanly on process termination, and remains covered by deterministic churn and interrupted-update tests.

## CLI Contract

```text
holys3 index TARGET --watch --interval SECONDS [--json]
```

The `index` subcommand gains three options:

```rust
watch: bool
interval: Option<u64>
json: bool
```

- `--watch` requires `--interval`.
- `--interval` requires `--watch`, is expressed as positive integer seconds, and rejects zero.
- `--json` works for one-shot and watched indexing.
- `--rebuild` applies only to the first successful-or-failed attempt. A watched process never rebuilds the entire index repeatedly.
- Existing one-shot commands and human output remain compatible.

## Execution Model

One source object and one credential-refreshing S3 client are opened before the loop. Each cycle performs a fresh listing and calls the existing `update_index` function. Cycles never overlap, and the interval begins after an attempt completes.

The first attempt is fail-fast. An invalid target, unusable credentials, malformed index, or unavailable backend exits with code 2 instead of creating a silently unhealthy daemon. After one successful attempt, later failures are reported and retried after the configured interval. The process therefore recovers from temporary listing, fetch, upload, and compare-and-swap failures without hiding startup configuration errors.

`--rebuild` is passed only to cycle 1. Every later cycle passes `false` and uses the committed root as its incremental base.

## Termination

The CLI uses `ctrlc` 3.5.2 with its `termination` feature. One bounded stop channel receives SIGINT, SIGTERM, SIGHUP, Windows Ctrl-C, and Windows Ctrl-Break notifications.

- A signal received while waiting ends the wait immediately and exits 0.
- A signal received during an index attempt lets that attempt reach its existing atomic root-swap boundary, emits its result, then exits 0.
- Repeated signals coalesce into one stop request.
- One-shot indexing does not install a signal handler and retains normal process behavior.

## Status Output

Human mode keeps the existing summaries on stderr. Watched summaries are prefixed with their cycle number. Errors after startup are printed on stderr, and shutdown prints the final attempted cycle count.

`--json` emits one JSON object per line to stdout and suppresses human summaries. Notes and warnings from lower layers remain on stderr.

Successful attempt:

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

The serialized output shapes are exact:

```json
{"type":"indexed","cycle":1,"target":"./logs","duration_ms":42,"added":100,"removed":0,"total_docs":100,"segments":1,"compacted":false,"up_to_date":false}
{"type":"error","cycle":2,"target":"./logs","duration_ms":5,"error":"listing source objects: permission denied"}
{"type":"stopped","cycle":2,"target":"./logs"}
```

Serialization or stdout failures terminate with code 2. Durations are checked conversions from elapsed milliseconds to `u64`.

## Crash Consistency

Continuous mode does not introduce a second journal or mutable daemon database. It relies on the existing index protocol:

1. New segment blobs are content-addressed and immutable.
2. The old root remains authoritative while a cycle builds.
3. `segments.bin` changes only through compare-and-swap.
4. A process failure before the swap leaves the prior root readable.
5. A restarted process lists current sources and reruns the incremental diff.
6. Rebuilt deterministic segment blobs reuse their content addresses, and a successful swap makes the new state authoritative.

A fault-injection integration test will reject the root swap after segment writes, verify the prior index remains searchable, rerun with the normal store, and verify convergence to the new source state.

## Churn Benchmark

`holys3-bench` gains a local `churn` command:

```text
holys3-bench churn --cycles CYCLES --changes CHANGES
```

Both values are required positive integers. The command consumes the existing generated corpus and local index. For each cycle it removes exactly `CHANGES` oldest sources and writes exactly `CHANGES` deterministic JSONL log sources under date-partitioned paths, keeping source cardinality constant.

Each generated body is one valid JSONL record containing timestamp, level, service, request ID, and message fields. The message is deterministically padded so the record plus newline has the seed manifest's exact object size. New source names and contents derive only from the seed and sequence number.

The benchmark times listing and incremental update separately, validates `added == CHANGES`, `removed == CHANGES`, constant `total_docs`, and final indexed search equivalence for a planted churn token. It writes this schema to `crates/xbench/runs/churn.json`:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct ChurnSummary {
    cycles: usize,
    changes_per_cycle: usize,
    total_docs: usize,
    listing_p50_ms: f64,
    listing_p95_ms: f64,
    update_p50_ms: f64,
    update_p95_ms: f64,
    final_segments: usize,
}
```

CI runs ten cycles over the existing 25,000-object, 4 KiB scale corpus with 250 additions and deletions per cycle. It requires listing p95 at or below 2,000 ms, update p95 at or below 5,000 ms, peak RSS at or below 300 MiB, the exact summary schema, and a final indexed hit for `CHURN_NEEDLE`.

## Tests

- Clap rejects either half of the `--watch`/`--interval` pair and rejects zero seconds.
- One-shot JSON emits exactly one `indexed` event.
- The loop applies `--rebuild` only to cycle 1.
- A first-attempt error is emitted and returned.
- An error after a successful attempt is emitted and retried.
- A stop request interrupts waiting and emits one `stopped` event.
- A stop request during work emits the completed attempt before `stopped`.
- A failed root swap preserves the old searchable index and restart converges.
- Churn generation is deterministic, bounded to the requested body size, and reports exact additions/removals.
- Linux CI starts the real watch command against a local corpus, mutates the corpus, observes the second indexed event, sends SIGTERM, and requires a clean stopped event.

## Documentation

README usage, supported behavior, performance coverage, and limitations will describe watch mode and its fail-fast/retry semantics. README and CHANGELOG references to index format 8 will be corrected to the implemented format 9. Architecture documentation will describe continuous indexing without changing the existing ownership boundaries.

## Non-Goals

- SQS, EventBridge, bucket-notification, or provider-specific event ingestion.
- Background daemonization, PID files, service installation, or a mutable state database.
- Overlapping index cycles.
- Automatic interval defaults.
- Live AWS validation; CI uses local files and MinIO only.
