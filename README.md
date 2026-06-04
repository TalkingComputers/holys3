<div align="center">

# holys3

Indexed regex search for local files and private S3 buckets.

[![CI](https://github.com/holys3/holys3/actions/workflows/ci.yml/badge.svg)](https://github.com/holys3/holys3/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/holys3.svg)](https://crates.io/crates/holys3)
[![docs.rs](https://docs.rs/holys3/badge.svg)](https://docs.rs/holys3)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](Cargo.toml)
[![downloads](https://img.shields.io/crates/d/holys3.svg)](https://crates.io/crates/holys3)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

</div>

## Why

S3 has no native grep. holys3 builds a compact index next to the data, uses it to narrow candidate objects, then verifies matches with Rust regexes over the original bytes.

Use holys3 when:

- You need regex search over many text objects in a private S3 bucket.
- You want the index stored in the same bucket, under `.holys3/`.
- You want candidate narrowing without trusting the index as the answer.

## Why not

Do not use holys3 when:

- You need a managed search service with ranking, analyzers, or faceting.
- You need to search encrypted or compressed object bodies without extracting text first.
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
holys3 index [--local-dir <LOCAL_DIR> | --bucket <BUCKET>] [--prefix <PREFIX>] [--region <REGION>] [--out <OUT>] [--strategy trigram|sparse]
holys3 search [--local-dir <LOCAL_DIR> | --bucket <BUCKET>] [--prefix <PREFIX>] [--region <REGION>] [--index <INDEX>] [--files-only] [--stats] <PATTERN>
holys3 stats [--index <INDEX>]
```

### `index`

Builds an index for either a local directory or an S3 bucket prefix.

- `--local-dir <LOCAL_DIR>`: directory to index.
- `--bucket <BUCKET>`: S3 bucket to index.
- `--prefix <PREFIX>`: S3 prefix to index. Defaults to empty.
- `--region <REGION>`: AWS region. If omitted, `AWS_REGION` is required.
- `--out <OUT>`: local index directory. Defaults to `holys3.idxdir`.
- `--strategy trigram|sparse`: index strategy. Defaults to `trigram`.

For S3, the index is written in-bucket under `.holys3/` or `<prefix>/.holys3/`.

### `search`

Searches with a prebuilt index and verifies matches against the original bytes.

- `<PATTERN>`: Rust regex pattern.
- `--local-dir <LOCAL_DIR>`: local directory to read candidates from.
- `--bucket <BUCKET>`: S3 bucket to read candidates from.
- `--prefix <PREFIX>`: S3 prefix. Defaults to empty.
- `--region <REGION>`: AWS region. If omitted, `AWS_REGION` is required.
- `--index <INDEX>`: local index directory. Defaults to `holys3.idxdir`.
- `--files-only`: print only matching file or object keys.
- `--stats`: print candidate and index statistics to stderr.

### `stats`

Prints local index statistics:

- `--index <INDEX>`: local index directory. Defaults to `holys3.idxdir`.

## How it works

1. The query planner extracts trigrams from the regex literal set.
2. The index maps trigrams to candidate document ids and narrows the search set.
3. For S3 indexes, holys3 reads only the needed postings ranges from `.holys3/`.
4. holys3 fetches candidate objects with ranged GETs where applicable and full GETs for verification.
5. The regex verifier runs against original bytes and produces the final answer.

The index narrows candidates. The verifier decides matches.

## Benchmarks

<!-- BENCH:START -->
| scenario | hits | candidates/total | prune ratio | bytes | p50 ms | p95 ms | p99 ms | concurrency=1 p50 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| short_literal | pending | pending | pending | pending | pending | pending | pending | pending |
| long_literal | pending | pending | pending | pending | pending | pending | pending | pending |
| alternation | pending | pending | pending | pending | pending | pending | pending | pending |
| anchored | pending | pending | pending | pending | pending | pending | pending | pending |
| no_match | pending | pending | pending | pending | pending | pending | pending | pending |
| QAll | pending | pending | pending | pending | pending | pending | pending | pending |
<!-- BENCH:END -->

Methodology: one machine, one region, fixed synthetic corpus, fixed seed, object count and object size recorded in `crates/xbench/runs/*.json`, holys3 SHA from `git rev-parse HEAD`, exact command recorded by the runner; reproduce with `make bench`.

## Security

holys3 signs S3 requests with its own SigV4 implementation and tests it against AWS signature vectors. It reads credentials from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`, or an AWS credentials profile.

Use private buckets. The index lives in the same account and bucket namespace as the data and is stored under `.holys3/`. holys3 does not send the index to an external service.

## Contributing

Read [ARCHITECTURE.md](ARCHITECTURE.md) before changing index, query, S3, or SigV4 behavior. CI and contributor scaffolding live in the repository.

## License

Licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
