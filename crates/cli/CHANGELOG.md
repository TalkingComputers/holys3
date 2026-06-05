# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/TalkingComputers/holys3/releases/tag/holys3-v0.1.0) - 2026-06-05

### Added

- *(cli)* --concurrency flag wires FetchConfig
- *(cli)* index/search against an S3 bucket
- *(cli)* --strategy + candidate --stats
- *(cli)* use on-disk FST index dir
- *(cli)* index/search/stats over local corpus

### Other

- Drop unused regex dependencies
- Route S3 clients through timed builder
- Dedup CLI result emission
- Restore single-fetch parallel search verify
- C9 honor XDG cache fallback
- C7 compile CLI search regex once
- C4 share S3 setup helpers
- C3 use trait-only index readers
- real S3 benchmark numbers in README (43.5x fan-out, index prunes to zero)
- Add benchmark README targets
- Add S3 custom endpoint support
- Share prefix and query encoding
- Deduplicate query evaluation
- *(cli)* use unified index search
- Add docsrs metadata
- Rewrite README for current CLI
- Add workspace lints and fix warnings
- Add Cargo publish metadata
- CI + contributor scaffolding (research-backed)
- *(cli)* env-gated real-S3 index+search e2e; stage 3b green
- Stage 2 sparse-gram measurement; confirm §5 Option B
- stage 1 README
