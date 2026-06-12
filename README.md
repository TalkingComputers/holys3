<div align="center">

# holys3

**grep for S3.** Indexed regex search over buckets — fetches matches, not corpora.

[![CI](https://github.com/TalkingComputers/holys3/actions/workflows/ci.yml/badge.svg)](https://github.com/TalkingComputers/holys3/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/holys3.svg)](https://crates.io/crates/holys3)
[![docs.rs](https://docs.rs/holys3/badge.svg)](https://docs.rs/holys3)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](Cargo.toml)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

</div>

```sh
holys3 index s3://my-logs/prod                     # build the index, in-bucket, once
holys3 'req-7f3e9a2c1b' s3://my-logs/prod          # then grep it in ~a second
holys3 -i 'timeout' s3://my-logs -g '*.gz' -C2 --since 6h
```

S3 has no native grep. The alternatives scan: download-everything-and-rg pays for
every object on every query, and Athena bills per byte scanned. holys3 builds a
compact trigram index _next to the data_ (under `.holys3/` in the same bucket),
uses it to narrow each query to candidate objects, then fetches only those and
verifies with real Rust regexes against the original bytes. **The index narrows
candidates; the verifier decides matches** — results are always exact, never
index-approximated.

Measured against the fastest DIY alternative (s5cmd at 64 workers + `rg -z`) on
a 100,000-object log corpus in S3: holys3 answered needle queries in **1.5 s**
fetching ~100 objects; the download-and-grep path takes **~4.3 minutes and
100,000 GETs — every query**. The gap grows with bucket size, because scan time
is O(corpus) and holys3 is O(matches).

## Install

Prebuilt binaries for Linux (x86_64, arm64), macOS (Intel, Apple Silicon), and
Windows ship with every [GitHub release](https://github.com/TalkingComputers/holys3/releases):

```sh
cargo binstall holys3   # fetches the prebuilt binary for your platform
cargo install holys3    # or build from source (Rust 1.88+)
```

## Quickstart

```sh
# S3, with an `aws sso login` session — holys3 reads SSO profiles directly
AWS_PROFILE=my-sso holys3 index s3://my-log-bucket/prod --region us-east-2
AWS_PROFILE=my-sso holys3 'level":"ERROR' s3://my-log-bucket/prod --region us-east-2

# rg-style flags work
holys3 'req-[0-9a-f]+' s3://my-log-bucket --json | jq .
holys3 -w -F 'foo(' s3://my-code-bucket -l

# local directories work too
holys3 index ./logs --out holys3.idxdir
holys3 'TODO|FIXME' ./logs --index holys3.idxdir
```

The CLI follows ripgrep: `holys3 PATTERN TARGET`, where TARGET is
`s3://bucket[/prefix]` or a local path. To search for a pattern named like a
subcommand, use `-e`: `holys3 -e index s3://bucket`.

## What's supported

### Object formats

Format detection is by **magic bytes only** — file extensions are never
trusted, and every check is an exact byte comparison from the format's
official specification. An object either matches a magic or it is searched as
raw text.

| format                           | detection                      | behavior                                                                                                                                                                                              |
| -------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| plain text / anything            | no magic matched               | searched as-is (JSONL, CSV, syslog, …)                                                                                                                                                                |
| gzip                             | `1f 8b 08`                     | transparent decompress, incl. multi-member concatenations (how ALB/CloudTrail/CloudFront deliver)                                                                                                     |
| zstd                             | `28 b5 2f fd`                  | transparent decompress, incl. multi-frame and skippable frames                                                                                                                                        |
| bzip2                            | `BZh` + level + block magic    | transparent decompress, incl. multi-stream concatenations                                                                                                                                             |
| xz                               | `fd 37 7a 58 5a 00`            | transparent decompress, incl. multi-stream + stream padding                                                                                                                                           |
| snappy (framing format)          | `ff 06 00 00 sNaPpY`           | transparent decompress, incl. concatenated streams                                                                                                                                                    |
| lz4 (frame format)               | `04 22 4d 18`                  | transparent decompress, incl. concatenated frames and skippable frames                                                                                                                                |
| **Parquet**                      | `PAR1` head + validated footer | each row projected to one JSON line and searched as text — RFC3339 timestamps (incl. `tz="UTC"` files from pyarrow/pandas/Spark), unquoted decimals, hex binary, explicit nulls, nested structs/lists |
| **Avro** (Object Container File) | `Obj` `01`                     | each record projected to one JSON line — null/deflate/snappy/zstd/bzip2/xz codecs, decimals rendered as decimal strings (`"123.45"`), NaN/Infinity as `null`                                          |

Because the Parquet/Avro projection happens at the same layer as
decompression, the index and the verifier always see identical text — search
results over columnar data are exact, line numbers refer to rows.

Truncated or corrupt-tailed streams **salvage**: the cleanly decoded prefix is
searched and a warning names the object. Undecodable objects are excluded
loudly, never silently mis-searched.

### Search flags (ripgrep semantics)

```text
-e PATTERN        multiple patterns, OR              -n / -N      line numbers on/off
-F                fixed strings                      --column     1-based match column
-i / -S / -s      ignore / smart / sensitive case    --heading    group under key (tty default)
-w                word boundaries (rg half-bounds)   --no-heading key:line:text (pipe default)
-l                files with matches                 -g GLOB      include/!exclude key globs
-c                count matching lines               -q           quiet, exit at first match
--count-matches   count individual matches           --color WHEN auto/always/never/ansi
-m NUM            max matching lines per object      --json       rg-compatible JSON Lines
-A/-B/-C NUM      context lines with -/-- separators --stats      candidate stats to stderr
```

Exit codes are rg's: `0` match found, `1` no match, `2` error. Patterns are
line-oriented like rg: `^`/`$` anchor at every line, character classes never
match the line terminator, and a literal `\n` in a pattern is an error.

### holys3-specific scoping

- `--key-prefix P` — only keys starting with `P` (prunes whole index segments
  before any fetch)
- `--key-regex RE` — only keys matching `RE`
- `--since T` / `--until T` — only objects whose **key-embedded timestamp**
  overlaps the window. `T` is `2026-06-09`, `2026-06-09T14:30[:00][Z]`, or
  relative `30s`/`15m`/`6h`/`2d`/`1w` (ago, UTC). Recognized key shapes:
  `2026/06/09` paths, `year=2026/month=06/day=09[/hour=14]` hive partitions,
  `dt=`/`date=2026-06-09`, ALB/CloudTrail filename stamps (`20260609T2300Z`),
  and CloudFront/S3-access-log dashed stamps (`2026-06-09-23[-59-59]`). Keys
  without a recognizable timestamp are searched anyway, with a note on stderr —
  time scoping never silently hides data.
- `--region`, `--endpoint` (MinIO, R2, any S3-compatible store),
  `--concurrency` (default 750), `--index` (local index dir)

### Credentials

Resolved in order: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (+ optional
`AWS_SESSION_TOKEN`) from the environment → the `AWS_PROFILE` config profile,
including **AWS IAM Identity Center (SSO)** profiles whose cached
`aws sso login` tokens are exchanged for role credentials and auto-refreshed
before expiry → static profile keys. Requests are signed by holys3's own
SigV4 implementation, tested against AWS signature vectors.

## How it works

1. The query planner extracts gram constraints from the regex — prefix,
   suffix, **and required inner literals** (Cox-style), so `.*ERROR.*` prunes
   instead of scanning.
2. The term dictionary (an FST) maps each gram to its postings offset _and_
   doc count, so selectivity is known before any fetch: absent grams answer
   instantly, only the rarest grams per AND-group are fetched, and grams
   present in every doc cost zero bytes on disk and zero fetches.
3. Posting blocks are read with coalesced ranged GETs; candidates are pruned
   by key scope before a single object is fetched.
4. Candidate objects stream through concurrent fetches (adaptive AIMD
   concurrency, retries, request hedging), get decoded, and are
   regex-verified on a worker pool — results print as they complete,
   unordered across objects like rg's parallel mode, pipe-friendly
   (`holys3 ... | head` terminates cleanly).

### The index

`holys3 index s3://bucket/prefix` maintains content-addressed segments under
`.holys3/`. Runs are **incremental diffs**: only new or changed objects are
fetched and indexed, deletions take effect immediately, an unchanged bucket
costs one listing (~seconds), and small segments merge automatically. Posting
lists are density-classed and bit-packed — on a 100K-object log corpus the
index is ~260 MB against 217 MB of gzipped data, and grams shared by every
document cost nothing. Replaced segments are garbage-collected; the root
pointer swap is a **compare-and-swap** (S3 conditional writes), so a racing
concurrent index run fails loudly instead of corrupting anything. Large index
blobs upload as concurrent multipart parts.

## Performance

**Real-world S3** — 1,000 synthetic 4 KiB objects, indexed search
(concurrency 64) vs a sequential (`--concurrency 1`) baseline, laptop →
us-east-2 over the public internet (per-object RTT dominates; in-region is
far lower). Reproduce with `make bench-s3`.

| scenario | pattern | hits | candidates/total | p50 ms | seq p50 ms | speedup |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| no_match | `UNMATCHABLE_TOKEN` | 0 | 0/1000 | 0 | 0 | index fetches nothing |
| QAll | `.*` | 1000 | 1000/1000 | 1288 | 66828 | 51.9x |
| short_literal | `needle` | 500 | 500/1000 | 702 | 34825 | 49.6x |
| alternation | `alpha\|beta` | 314 | 314/1000 | 467 | 22267 | 47.6x |
| long_literal | `longliteralbenchmarktoken` | 334 | 334/1000 | 547 | 23593 | 43.1x |
| anchored | `^ANCHOR_START` | 91 | 91/1000 | 214 | 5832 | 27.2x |
| dot_star_gap | `(?s)needle.*alpha` | 100 | 100/1000 | 299 | 7699 | 25.7x |

At 100K objects the same needle queries hold at ~1.5 s with exact candidate
counts (the planted-needle suite returns precisely 1/5/100 matching objects
out of 100,000).

**Continuous (CI)** — regenerated on every push against a local MinIO
(`make bench-minio`); tracks regressions rather than headline latency.

<!-- BENCH:START -->
| scenario | hits | candidates/total | prune ratio | bytes | p50 ms | p95 ms | p99 ms | concurrency=1 p50 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| short_literal | 50 | 50/100 | 0.500 | 204800 | 33.672 | 33.868 | 33.868 | 63.729 |
| long_literal | 34 | 34/100 | 0.340 | 139264 | 23.799 | 23.805 | 23.805 | 44.528 |
| alternation | 32 | 32/100 | 0.320 | 131072 | 23.820 | 24.993 | 24.993 | 42.595 |
| anchored | 10 | 10/100 | 0.100 | 40960 | 8.813 | 9.426 | 9.426 | 14.893 |
| no_match | 0 | 0/100 | 0.000 | 0 | 0.151 | 0.163 | 0.163 | 0.115 |
| QAll | 100 | 100/100 | 1.000 | 409600 | 80.834 | 81.583 | 81.583 | 133.350 |
| dot_star_gap | 10 | 10/100 | 0.100 | 40960 | 11.101 | 11.359 | 11.359 | 16.128 |
<!-- BENCH:END -->

Microbenchmarks: `make bench-micro` (CI-gated against
[`benches/baseline.json`](benches/baseline.json)).

## Limitations

- **Raw (unframed) snappy** has no magic bytes and is undetectable by design —
  unsupported as an object format (it still decodes fine _inside_ Avro files,
  where the container names the codec). **lz4 legacy frames** (`lz4 -l`
  output) are detected and rejected loudly rather than decoded.
- No multiline mode: patterns that would match across line boundaries are
  line-restricted exactly like rg without `-U`.
- Decompression is in-memory and unbounded: a pathological archive expands to
  its full decoded size during indexing/search.
- Concurrent `holys3 index` runs over one prefix are safe (the loser errors
  cleanly and retries), but the design assumes occasional writers, not a
  write-heavy pipeline.
- The library crates are not a stable API; the CLI is the supported surface.

## Security

Use private buckets. The index lives in the same account and bucket namespace
as the data, under `.holys3/`; holys3 talks only to your S3 endpoint and
never sends data anywhere else.

## Contributing

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing index, query, S3, or
SigV4 behavior. The differential test suites are the correctness contract:
indexed search must exactly equal a decoded full scan, for every format, both
gram strategies, and both readers.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
