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

S3 has no native grep. holys3 builds a compact index next to the data, uses it to narrow candidate objects, then verifies matches with Rust regexes over the original bytes. Compressed objects — gzip, zstd, bzip2, xz, snappy (framed), lz4 (ALB, CloudTrail, CloudFront, VPC Flow Logs, Vector/Fluentd/Kafka sinks) — are decompressed transparently at both index and search time; Parquet and Avro objects are searched as a JSON-Lines projection (one row per line). Format detection is by magic bytes only, never file extensions. Searches can be scoped by key prefix, key regex, glob, or the timestamps embedded in log keys.

Use holys3 when:

- You need regex search over many objects in a private S3 bucket — text, compressed logs, or Parquet/Avro row data.
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

The CLI follows ripgrep: `holys3 PATTERN TARGET`, where TARGET is
`s3://bucket[/prefix]` or a local path.

Local directory:

```sh
holys3 index ./fixtures --out holys3.idxdir
holys3 'TODO|FIXME' ./fixtures --index holys3.idxdir
holys3 stats --index holys3.idxdir
```

S3 bucket (with an `aws sso login` session — holys3 reads SSO profiles directly):

```sh
AWS_PROFILE=my-sso-profile holys3 index s3://my-log-bucket/prod --region us-east-2
AWS_PROFILE=my-sso-profile holys3 'ERROR' s3://my-log-bucket/prod --region us-east-2
holys3 -i 'timeout' s3://my-log-bucket -g '*.gz' -C2 --since 6h       # rg flags work
holys3 'req-[0-9a-f]+' s3://my-log-bucket --json | jq .               # rg JSON wire format
```

or with static credentials:

```sh
AWS_ACCESS_KEY_ID=<access-key> \
AWS_SECRET_ACCESS_KEY=<secret-key> \
AWS_SESSION_TOKEN=<session-token> \
holys3 'TODO|FIXME' s3://my-log-bucket --region us-east-2
```

## Usage

```text
holys3 PATTERN TARGET [FLAGS]            search (TARGET = s3://bucket[/prefix] or a local path)
holys3 -e PAT [-e PAT ...] TARGET        multiple patterns (OR), like rg
holys3 index TARGET [--strategy trigram|sparse] [--rebuild] [--out <DIR>]
holys3 stats [--index <DIR>]
```

Search flags follow ripgrep exactly where the concept maps:

- `-i` / `-S` / `-s` case handling, `-F` fixed strings, `-w` word boundaries
- `-l` files with matches, `-c` count lines, `--count-matches`, `-m NUM` max per object
- `-A`/`-B`/`-C NUM` context lines with rg's `-`/`--` separators
- `-n`/`-N` line numbers, `--column`, `--heading`/`--no-heading` (tty defaults match rg)
- `-g GLOB` include/`!`exclude key globs (gitignore-style, last match wins)
- `-q` quiet, `--color WHEN`, `--json` (rg-compatible JSON Lines wire format)
- exit codes: 0 match, 1 no match, 2 error

holys3-specific: `--key-prefix`, `--key-regex`, `--since`/`--until` (key-embedded
timestamps), `--stats`, `--region`, `--endpoint`, `--concurrency`, `--index`.

### `index`

Builds or incrementally updates the index for TARGET.

- `s3://bucket[/prefix]`: the index is written in-bucket under `.holys3/` or `<prefix>/.holys3/` as content-addressed segments. Index runs are incremental: only new or changed objects are fetched and indexed, deletions take effect immediately, and a run against an unchanged bucket costs one listing and nothing else. Small segments merge automatically; large index blobs upload as concurrent multipart parts.
- Local directory: the index is written to `--out` (default `holys3.idxdir`).
- `--strategy trigram|sparse` picks the gram strategy; `--rebuild` ignores any existing index.
- `--region` (or `AWS_REGION`), `--endpoint` for S3-compatible stores (MinIO, R2), `--concurrency` for fetch parallelism.

### `search`

Searches with a prebuilt index and verifies matches against the original bytes.
Results stream per object as verification completes (unordered across objects,
like rg's parallel mode). Objects deleted between indexing and searching are
skipped with a warning; gzip/zstd bodies are decompressed transparently; output
is pipe-friendly (`holys3 ... | head` terminates cleanly).

`--since <T>` / `--until <T>` scope by the timestamps embedded in keys. `T` is
`2026-06-09`, `2026-06-09T14:30[:00][Z]`, or relative like `6h` / `2d` (ago, UTC).
Recognized key shapes: `2026/06/09[/14]` paths, `year=2026/month=06/day=09[/hour=14]`,
`dt=`/`date=2026-06-09`, ALB/CloudTrail filename stamps (`20260609T2300Z`), and
CloudFront/S3-access-log dashed stamps (`2026-06-09-23`). Keys without a
recognizable timestamp are searched anyway (with a note on stderr).

### `stats`

Prints local index statistics:

- `--index <INDEX>`: local index directory. Defaults to `holys3.idxdir`.

## How it works

1. The query planner extracts grams from the regex literal set.
2. The term dictionary (an fst) maps each gram to its postings offset _and_ doc count, so selectivity is known before any postings fetch: absent grams answer instantly, and only the rarest grams per AND group are fetched at all.
3. For S3 indexes, holys3 fetches every needed postings block concurrently from `.holys3/`, coalescing nearby blocks into single ranged GETs. Candidates are then pruned by any `--key-prefix`/`--key-regex`/`--since`/`--until` scope before a single object is fetched.
4. Candidate objects are fetched concurrently with adaptive (AIMD) concurrency, retries, and request hedging; bodies are decompressed (multi-member gzip, zstd) and verified on a worker pool as fetches complete, with results streamed per object; deleted objects are skipped as stale.
5. The regex verifier runs against original bytes and produces the final answer.

The index narrows candidates. The verifier decides matches.

## Benchmarks

**Real-world S3** — end-to-end search latency over 1000 synthetic 4 KiB objects, indexed search (concurrency 64) vs a sequential (`--concurrency 1`) baseline. Two effects compound: the trigram index **prunes** the candidate set, and concurrent ranged fetch **fans out** the survivors.

| scenario      | pattern                     | hits | candidates/total | p50 ms | seq p50 ms |               speedup |
| ------------- | --------------------------- | ---: | ---------------: | -----: | ---------: | --------------------: |
| no_match      | `UNMATCHABLE_TOKEN`         |    0 |           0/1000 |    0.0 |        0.0 | index fetches nothing |
| QAll          | `.*`                        | 1000 |        1000/1000 |   1176 |      68261 |                 58.1x |
| short_literal | `needle`                    |  500 |         500/1000 |    701 |      32973 |                 47.0x |
| alternation   | `alpha\|beta`               |  314 |         314/1000 |    525 |      19875 |                 37.9x |
| long_literal  | `longliteralbenchmarktoken` |  334 |         334/1000 |    628 |      21124 |                 33.6x |
| anchored      | `^ANCHOR_START`             |   91 |          91/1000 |    216 |       5821 |                 27.0x |
| dot_star_gap  | `(?s)needle.*alpha`         |  100 |         100/1000 |    279 |       6858 |                 24.6x |

A non-matching query fetches **zero** objects; `.*` over all 1000 is **58x** faster than sequential, and `(?s)needle.*alpha` shows the planner constraining on both sides of the gap (100 candidates, not the 500 containing `needle`). Caveat: laptop → `us-east-2` over the public internet via an SSO profile (per-object RTT dominates; in-region EC2 is far lower), fixed seed 1, 5 iterations. Reproduce against your bucket with `make bench-s3`.

**Continuous (CI)** — the table below is regenerated on every push to `main` against a local MinIO (deterministic, reproducible with `make bench-minio`); it tracks regressions rather than headline latency.

<!-- BENCH:START -->
| scenario | hits | candidates/total | prune ratio | bytes | p50 ms | p95 ms | p99 ms | concurrency=1 p50 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| short_literal | 50 | 50/100 | 0.500 | 204800 | 45.894 | 49.888 | 49.888 | 70.281 |
| long_literal | 34 | 34/100 | 0.340 | 139264 | 33.072 | 36.559 | 36.559 | 48.690 |
| alternation | 32 | 32/100 | 0.320 | 131072 | 34.623 | 35.194 | 35.194 | 48.154 |
| anchored | 10 | 10/100 | 0.100 | 40960 | 12.254 | 43.730 | 43.730 | 16.379 |
| no_match | 0 | 0/100 | 0.000 | 0 | 0.129 | 0.184 | 0.184 | 0.110 |
| QAll | 100 | 100/100 | 1.000 | 409600 | 135.008 | 138.927 | 138.927 | 159.922 |
| dot_star_gap | 10 | 10/100 | 0.100 | 40960 | 15.842 | 16.760 | 16.760 | 19.127 |
<!-- BENCH:END -->

Microbenchmarks (`make bench-micro`): trigram extraction ~330 us, query plan ~0.7 us, postings decode ~44 ns.

## Security

holys3 signs S3 requests with its own SigV4 implementation and tests it against AWS signature vectors. Credentials resolve in order: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (with optional `AWS_SESSION_TOKEN`) from the environment, then the `AWS_PROFILE` config profile — including AWS IAM Identity Center (SSO) profiles, whose cached `aws sso login` tokens are exchanged for role credentials and refreshed automatically before expiry — then static profile keys.

Use private buckets. The index lives in the same account and bucket namespace as the data and is stored under `.holys3/`. holys3 does not send the index to an external service.

## Contributing

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing index, query, S3, or SigV4 behavior. CI and contributor scaffolding live in the repository.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
