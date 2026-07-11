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

In the tracked real-S3 benchmark below, a literal query over 1,000 objects
finished in **1.5 s** at concurrency 64 versus **54.7 s** sequentially. A
no-match query fetched zero objects. Every benchmark corpus, planted hit count,
candidate count, and byte count is deterministic and checked before timing.

## Install

Prebuilt binaries for Linux (x86_64, arm64), macOS (Intel, Apple Silicon), and
Windows ship with every [GitHub release](https://github.com/TalkingComputers/holys3/releases):

```sh
cargo binstall holys3   # fetches the prebuilt binary for your platform
cargo install holys3    # or build from source (Rust 1.88+)
```

Release archives include SHA-256 checksums and GitHub build-provenance
attestations. Verify an archive with
`gh attestation verify -R TalkingComputers/holys3 <archive>`.

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

Format detection is magic-first. Brotli and zlib have no reliable container
magic, so only `.br`, `.zlib`, and `.zz` select those decoders, and the entire
stream must validate. Other extensions are never trusted.

| format                           | detection                      | behavior                                                                                                                                                                                              |
| -------------------------------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| plain text / anything            | no magic matched               | searched as-is (JSONL, CSV, syslog, …)                                                                                                                                                                |
| gzip                             | `1f 8b 08`                     | transparent decompress, incl. multi-member concatenations (how ALB/CloudTrail/CloudFront deliver)                                                                                                     |
| zstd                             | `28 b5 2f fd`                  | transparent decompress, incl. multi-frame and skippable frames                                                                                                                                        |
| bzip2                            | `BZh` + level + block magic    | transparent decompress, incl. multi-stream concatenations                                                                                                                                             |
| xz                               | `fd 37 7a 58 5a 00`            | transparent decompress, incl. multi-stream + stream padding                                                                                                                                           |
| snappy (framing format)          | `ff 06 00 00 sNaPpY`           | transparent decompress, incl. concatenated streams                                                                                                                                                    |
| lz4 (frame format)               | `04 22 4d 18`                  | transparent decompress, incl. concatenated frames and skippable frames                                                                                                                                |
| Brotli                           | validated `.br` hint           | transparent strict decompression                                                                                                                                                                      |
| zlib                             | validated `.zlib`/`.zz` hint   | transparent strict decompression                                                                                                                                                                      |
| ZIP                              | ZIP signatures                 | every regular member is a searchable document at `object.zip!/member/path`; directories and links are skipped, while encrypted members reject the source                                             |
| TAR                              | validated `ustar` header        | every regular member is a searchable document; nested archives/compression recurse to four layers                                                                                                    |
| **Parquet**                      | `PAR1` head + validated footer | each row projected to one JSON line and searched as text — RFC3339 timestamps (incl. `tz="UTC"` files from pyarrow/pandas/Spark), unquoted decimals, hex binary, explicit nulls, nested structs/lists |
| **Avro** (Object Container File) | `Obj` `01`                     | each record projected to one JSON line — null/deflate/snappy/zstd/bzip2/xz codecs, decimals rendered as decimal strings (`"123.45"`), NaN/Infinity as `null`                                          |
| Arrow IPC file / stream / Feather | validated file or stream framing | each record batch is projected to canonical JSON Lines; continuation-marker and legacy streams are supported                                                                                         |
| ORC                               | validated postscript + footer    | Arrow-backed rows are projected to canonical JSON Lines                                                                                                                                               |

Projection and decompression happen at one canonical decoder boundary, so the
index and verifier see identical bytes. Columnar line numbers refer to rows.
Archive member paths are normalized without filesystem extraction; unsafe
paths fail the source, and duplicate normalized names receive `#2`, `#3`, etc.

Truncated or corrupt-tailed streams **salvage**: the cleanly decoded prefix is
searched and a warning names the object. Undecodable objects are excluded
loudly, never silently searched incorrectly.

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
- `--object-cache DIR --object-cache-cap BYTES` — explicit private cache for
  immutable S3 source bodies. Both flags are required; files are checksummed,
  content-addressed by endpoint/bucket/key/ETag, owner-only, atomically
  replaced, and size-bounded with FIFO eviction.

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
2. The term dictionary (an FST, adaptively prefix-sharded for dense trigram
   spaces) maps each gram to its postings offset _and_ doc count, so
   selectivity is known before any fetch: absent grams answer instantly, only
   the rarest grams per AND-group are fetched, and grams present in every doc
   cost zero bytes on disk and zero fetches.
3. Posting blocks are read with coalesced ranged GETs; candidates are pruned
   by key scope before a single object is fetched.
4. Candidate physical sources stream through concurrent conditional GETs
   (`If-Match` against the indexed ETag), with adaptive AIMD concurrency,
   retries, request hedging, and a 512 MiB in-flight byte budget. Sources of
   at least 64 MiB use four concurrent streamed 8 MiB conditional ranges. One archive
   is fetched and decoded once even when many members are candidates. Sources
   are then regex-verified on a worker pool — results print as they complete,
   unordered across objects like rg's parallel mode, pipe-friendly
   (`holys3 ... | head` terminates cleanly).

### The index

`holys3 index s3://bucket/prefix` maintains content-addressed segments under
`.holys3/`. Runs are **incremental diffs**: only new or changed objects are
fetched and indexed, deletions take effect immediately, an unchanged bucket
costs one listing (~seconds), and small segments merge automatically. Posting
lists are density-classed and bit-packed, and grams shared by every document
cost nothing. Index construction uses bounded sorted runs instead of a global
in-memory postings map. Replaced segments are garbage-collected; the root
pointer swap is a **compare-and-swap** (S3 conditional writes), so a racing
concurrent index run fails loudly instead of corrupting anything. Large index
blobs upload as concurrent multipart parts. Every immutable segment blob has
its own SHA-256 length/hash contract; readers reject truncation, corruption,
duplicate segment IDs, and malformed metadata before using it.

Local directories use the same format-8 physical-source/logical-document
tables, written to `--out` (default `holys3.idxdir`) with BLAKE3 content
freshness tokens. Local verification rechecks the token, local runs are
incremental, and `--rebuild` re-ingests everything. Raw candidate files are
read concurrently under the same 512 MiB byte budget; expanding formats stay
serial so decompression cannot multiply peak memory.

## Performance

**Real-world S3** — 1,000 synthetic 4 KiB objects, indexed search
(concurrency 64) vs a sequential (`--concurrency 1`) baseline, laptop →
us-east-2 over the public internet (per-object RTT dominates; in-region is
far lower). Reproduce with `make bench-s3`.

| scenario | pattern | hits | candidates/total | p50 ms | seq p50 ms | speedup |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| no_match | `UNMATCHABLE_TOKEN` | 0 | 0/1000 | 0 | 0 | index fetches nothing |
| QAll | `.*` | 1000 | 1000/1000 | 2566 | 110239 | 43.0x |
| short_literal | `needle` | 500 | 500/1000 | 1501 | 54688 | 36.4x |
| alternation | `alpha\|beta` | 314 | 314/1000 | 1088 | 35729 | 32.8x |
| anchored | `^ANCHOR_START` | 91 | 91/1000 | 390 | 10204 | 26.1x |
| long_literal | `longliteralbenchmarktoken` | 334 | 334/1000 | 1810 | 37891 | 20.9x |
| dot_star_gap | `needle.*alpha` | 100 | 100/1000 | 1135 | 10984 | 9.7x |

Absolute latencies vary with the network (per-object RTT dominates from a
laptop; in-region EC2 is far lower) — the prune ratios and hits are the
stable part.

**Continuous (CI)** — measured on every push against a pinned local MinIO
image (`make bench-minio`). CI runs release binaries, rejects missing or
unbaselined microbenchmarks and hybrid sort paths more than 15% slower than
their same-run controls, validates exact end-to-end hit counts, indexes 25,000
objects, and enforces hosted-run time plus a 300 MiB peak-RSS ceiling for
high-cardinality and 256 MiB decoded workloads under both index strategies.
Segment construction also enforces its cap on logical documents, including
archive members, rather than only on physical source objects. A separate 512
MiB compressed-expansion gate caps trigram/sparse index and files-only search
RSS at 96 MiB. A 512 MiB raw-object MinIO gate applies the same ceiling to S3
indexing and files-only search with both strategies. A separate 64 MiB
high-entropy gzip gate exercises ranged source download, streaming decode,
bitmap-backed trigram construction, cold term-cache population, and mmap lookup
under the same 96 MiB ceiling. Workspace line coverage is gated at 80%.

<!-- BENCH:START -->
| scenario | hits | candidates/total | prune ratio | bytes | p50 ms | p95 ms | p99 ms | concurrency=1 p50 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| short_literal | 500 | 500/1000 | 0.500 | 2048000 | 20.887 | 210.947 | 210.947 | 102.454 |
| long_literal | 334 | 334/1000 | 0.334 | 1368064 | 12.522 | 23.286 | 23.286 | 64.968 |
| alternation | 314 | 314/1000 | 0.314 | 1286144 | 14.916 | 207.664 | 207.664 | 62.277 |
| anchored | 91 | 91/1000 | 0.091 | 372736 | 3.303 | 3.585 | 3.585 | 19.080 |
| no_match | 0 | 0/1000 | 0.000 | 0 | 0.004 | 0.004 | 0.004 | 0.003 |
| QAll | 1000 | 1000/1000 | 1.000 | 4096000 | 231.110 | 245.671 | 245.671 | 206.964 |
| dot_star_gap | 100 | 100/1000 | 0.100 | 409600 | 5.413 | 12.604 | 12.604 | 22.036 |
<!-- BENCH:END -->

Microbenchmarks: `make bench-micro`. PR CI compares the base and head revisions
on one runner and gates statistically confident regressions above 20%; the
committed [`benches/baseline.json`](benches/baseline.json) remains the reporting
reference. Refresh it only from CI's `bench-micro` artifact.

## Limitations

- **Raw (unframed) snappy** has no magic bytes and is undetectable by design —
  unsupported as an object format (it still decodes fine _inside_ Avro files,
  where the container names the codec). **lz4 legacy frames** (`lz4 -l`
  output) are detected and rejected loudly rather than decoded.
- No multiline mode: patterns that would match across line boundaries are
  line-restricted exactly like rg without `-U`.
- Decoded output above 8 MiB spills to private temporary files. Trigram indexing
  and files-only bounded regex verification stream those files; sparse indexing
  reads them through a bounded 1 MiB window. Expansion is capped at 64 GiB per
  physical source, 100,000 regular archive members, and four nested format
  layers.
- Oversized local sources, S3 sources of at least 64 MiB, and large optional
  object-cache entries remain file-backed through indexing and verification
  instead of materializing the full body in memory. File-backed gzip, zstd,
  bzip2, Snappy, Brotli, and zlib sources decode through bounded readers rather
  than whole-source mappings.
- A search that races segment garbage collection reopens the new index root
  once before emitting results. Continuous write churn can still require a
  manual rerun; no incorrect matches are returned.
- Concurrent `holys3 index` runs over one prefix are safe (the loser errors
  cleanly and retries), but the design assumes occasional writers, not a
  write-heavy pipeline.
- AWS general-purpose buckets and S3-compatible endpoints are supported. S3
  Express directory buckets require zonal endpoints and session credentials
  and are not yet supported.
- The library crates are not a stable API; the CLI is the supported surface.

## Security

Use private buckets. The index lives in the same account and bucket namespace
as the data, under `.holys3/`; holys3 talks only to your S3 endpoint and
never sends data anywhere else.

## Contributing

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing index, query, S3, or
SigV4 behavior. The differential test suites are the correctness contract:
indexed search must exactly equal a decoded full scan, for every format, both
gram strategies, and every index lifecycle state.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
