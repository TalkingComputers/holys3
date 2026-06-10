<div align="center">

# holys3

Indexed regex search for local files and private S3 buckets.

[![CI](https://github.com/TalkingComputers/holys3/actions/workflows/ci.yml/badge.svg)](https://github.com/TalkingComputers/holys3/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/holys3.svg)](https://crates.io/crates/holys3)
[![docs.rs](https://docs.rs/holys3/badge.svg)](https://docs.rs/holys3)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](Cargo.toml)
[![downloads](https://img.shields.io/crates/d/holys3.svg)](https://crates.io/crates/holys3)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

</div>

## Why

S3 has no native grep. holys3 builds a compact index next to the data, uses it to narrow candidate objects, then verifies matches with Rust regexes over the original bytes. Gzip and zstd objects (ALB, CloudTrail, CloudFront, VPC Flow Logs, Vector/Fluentd sinks) are decompressed transparently at both index and search time, and searches can be scoped by key prefix, key regex, or the timestamps embedded in log keys.

Use holys3 when:

- You need regex search over many text objects in a private S3 bucket — including gzipped logs.
- You want the index stored in the same bucket, under `.holys3/`.
- You want candidate narrowing without trusting the index as the answer.

## Why not

Do not use holys3 when:

- You need a managed search service with ranking, analyzers, or faceting.
- You need to search encrypted object bodies.
- You need a stable library API; the publishable surface is the CLI.

## Installation

```sh
cargo install holys3
```

## Quickstart

Local directory:

```sh
holys3 index --local-dir ./fixtures --out holys3.idxdir --strategy trigram
holys3 search 'TODO|FIXME' --local-dir ./fixtures --index holys3.idxdir --stats
holys3 stats --index holys3.idxdir
```

S3 bucket:

```sh
AWS_ACCESS_KEY_ID=<access-key> \
AWS_SECRET_ACCESS_KEY=<secret-key> \
AWS_SESSION_TOKEN=<session-token> \
holys3 index --bucket holys3-test-381235349110-ue2 --region us-east-2 --strategy trigram

AWS_ACCESS_KEY_ID=<access-key> \
AWS_SECRET_ACCESS_KEY=<secret-key> \
AWS_SESSION_TOKEN=<session-token> \
holys3 search 'TODO|FIXME' --bucket holys3-test-381235349110-ue2 --region us-east-2 --stats
```

## Usage

```text
holys3 index (--local-dir <LOCAL_DIR> | --bucket <BUCKET>) [--prefix <PREFIX>] [--region <REGION>] [--endpoint <URL>] [--concurrency <N>] [--out <OUT>] [--strategy trigram|sparse]
holys3 search (--local-dir <LOCAL_DIR> | --bucket <BUCKET>) [--prefix <PREFIX>] [--region <REGION>] [--endpoint <URL>] [--concurrency <N>] [--index <INDEX>] [--key-prefix <P>] [--key-regex <RE>] [--since <T>] [--until <T>] [--files-only] [--stats] <PATTERN>
holys3 stats [--index <INDEX>]
```

### `index`

Builds an index for either a local directory or an S3 bucket prefix.

- `--local-dir <LOCAL_DIR>`: directory to index.
- `--bucket <BUCKET>`: S3 bucket to index.
- `--prefix <PREFIX>`: S3 prefix with directory semantics (`logs` matches `logs/...`, never `logs-old/...`). Defaults to empty.
- `--region <REGION>`: AWS region. If omitted, `AWS_REGION` is required.
- `--endpoint <URL>`: S3-compatible endpoint (MinIO, R2, ...). Defaults to AWS.
- `--concurrency <N>`: peak S3 fetch concurrency. Defaults to 750.
- `--out <OUT>`: local index directory. Defaults to `holys3.idxdir`.
- `--strategy trigram|sparse`: index strategy. Defaults to `trigram`.

For S3, the index is written in-bucket under `.holys3/` or `<prefix>/.holys3/` as content-addressed segments. Index runs are incremental: only new or changed objects are fetched and indexed, deletions take effect immediately, and a run against an unchanged bucket costs one listing and nothing else. Small segments merge automatically to keep per-query overhead flat.

### `search`

Searches with a prebuilt index and verifies matches against the original bytes.

- `<PATTERN>`: Rust regex pattern.
- `--local-dir <LOCAL_DIR>`: local directory to read candidates from.
- `--bucket <BUCKET>`: S3 bucket to read candidates from.
- `--prefix <PREFIX>`: S3 prefix with directory semantics. Defaults to empty.
- `--region <REGION>`: AWS region. If omitted, `AWS_REGION` is required.
- `--endpoint <URL>`: S3-compatible endpoint (MinIO, R2, ...). Defaults to AWS.
- `--concurrency <N>`: peak S3 fetch concurrency. Defaults to 750.
- `--index <INDEX>`: local index directory. Defaults to `holys3.idxdir`.
- `--key-prefix <P>`: only search objects whose key starts with `P`.
- `--key-regex <RE>`: only search objects whose key matches `RE`.
- `--since <T>` / `--until <T>`: only search objects whose key-embedded timestamp overlaps the window. `T` is `2026-06-09`, `2026-06-09T14:30[:00][Z]`, or relative like `6h` / `2d` (ago, UTC). Recognized key shapes: `2026/06/09[/14]` paths, `year=2026/month=06/day=09[/hour=14]`, `dt=`/`date=2026-06-09`, ALB/CloudTrail filename stamps (`20260609T2300Z`), and CloudFront/S3-access-log dashed stamps (`2026-06-09-23`). Keys without a recognizable timestamp are searched anyway (with a note on stderr).
- `--files-only`: print only matching file or object keys (early-exit per object).
- `--stats`: print candidate and index statistics to stderr.

Results stream per object as verification completes (unordered across objects, like grep over many files). Objects deleted between indexing and searching are skipped with a warning, and gzip/zstd bodies are decompressed transparently. Output is pipe-friendly (`holys3 search ... | head` terminates cleanly).

### `stats`

Prints local index statistics:

- `--index <INDEX>`: local index directory. Defaults to `holys3.idxdir`.

## How it works

1. The query planner extracts grams from the regex literal set.
2. The term dictionary (an fst) maps each gram to its postings offset _and_ doc count, so selectivity is known before any postings fetch: absent grams answer instantly, and only the rarest grams per AND group are fetched at all.
3. For S3 indexes, holys3 fetches every needed postings block concurrently — one ranged GET each — from `.holys3/`. Candidates are then pruned by any `--key-prefix`/`--key-regex`/`--since`/`--until` scope before a single object is fetched.
4. Candidate objects are fetched concurrently with adaptive (AIMD) concurrency, retries, and request hedging; bodies are decompressed (multi-member gzip, zstd) and verified on a worker pool as fetches complete, with results streamed per object; deleted objects are skipped as stale.
5. The regex verifier runs against original bytes and produces the final answer.

The index narrows candidates. The verifier decides matches.

## Benchmarks

**Real-world S3** — end-to-end search latency over 200 synthetic 4 KiB objects, indexed search (concurrency 64) vs a sequential (`--concurrency 1`) baseline. Two effects compound: the trigram index **prunes** the candidate set, and concurrent ranged fetch **fans out** the survivors.

| scenario      | pattern                     | hits | candidates/total | p50 ms | seq p50 ms |               speedup |
| ------------- | --------------------------- | ---: | ---------------: | -----: | ---------: | --------------------: |
| no_match      | `UNMATCHABLE_TOKEN`         |    0 |            0/200 |    0.0 |        0.0 | index fetches nothing |
| QAll          | `.*`                        |  200 |          200/200 |    328 |      14290 |                 43.5x |
| short_literal | `needle`                    |  100 |          100/200 |    814 |       7593 |                  9.3x |
| alternation   | `alpha\|beta`               |   63 |           63/200 |   1045 |       5371 |                  5.1x |
| long_literal  | `longliteralbenchmarktoken` |   67 |           67/200 |   3537 |       7927 |                  2.2x |
| anchored      | `^ANCHOR_START`             |   19 |           19/200 |   1710 |       3008 |                  1.8x |

A non-matching query fetches **zero** objects; `.*` over all 200 is **43.5x** faster than sequential. Caveat: laptop → `us-east-2` over the public internet (per-object RTT dominates; in-region EC2 is far lower), fixed seed 42, 5 iterations; `long_literal`'s 3.5 s is real-network tail variance. Reproduce against your bucket with `make bench-s3`.

**Continuous (CI)** — the table below is regenerated on every push to `main` against a local MinIO (deterministic, reproducible with `make bench-minio`); it tracks regressions rather than headline latency.

<!-- BENCH:START -->
| scenario | hits | candidates/total | prune ratio | bytes | p50 ms | p95 ms | p99 ms | concurrency=1 p50 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| short_literal | 50 | 50/100 | 0.500 | 204800 | 50.016 | 51.018 | 51.018 | 71.222 |
| long_literal | 34 | 34/100 | 0.340 | 139264 | 38.030 | 38.715 | 38.715 | 55.196 |
| alternation | 32 | 32/100 | 0.320 | 131072 | 35.886 | 37.331 | 37.331 | 52.504 |
| anchored | 10 | 10/100 | 0.100 | 40960 | 18.890 | 19.168 | 19.168 | 23.027 |
| no_match | 0 | 0/100 | 0.000 | 0 | 0.082 | 0.090 | 0.090 | 0.104 |
| QAll | 100 | 100/100 | 1.000 | 409600 | 130.517 | 133.118 | 133.118 | 149.750 |
<!-- BENCH:END -->

Microbenchmarks (`make bench-micro`): trigram extraction ~330 us, query plan ~0.7 us, postings decode ~44 ns.

## Security

holys3 signs S3 requests with its own SigV4 implementation and tests it against AWS signature vectors. It reads credentials from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, or an AWS credentials profile.

Use private buckets. The index lives in the same account and bucket namespace as the data and is stored under `.holys3/`. holys3 does not send the index to an external service.

## Contributing

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing index, query, S3, or SigV4 behavior. CI and contributor scaffolding live in the repository.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
