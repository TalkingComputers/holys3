# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

### Added

- `--index` accepts a local path or an independent prefixed S3 location, with separate `--index-region` and `--index-endpoint` controls for read-only source buckets and cross-region or S3-compatible index storage.

### Changed

- Index format 10 binds every root to its canonical local source or S3 endpoint, bucket, and prefix; searches may narrow that scope but reject broader or different sources.
- The legacy `--out` flag remains accepted for local index builds and is rejected for S3 targets; `--index` is the unambiguous storage-location flag.
- The workspace version is now 0.6.0 for the index-format and public library API break.
- S3 now uses the official AWS SDK credential, endpoint, signing, and operation implementations while retaining holys3's adaptive concurrency, retries, hedging, range coalescing, and bounded body storage.
- The minimum supported Rust version is now 1.94.1, required by the current AWS SDK.

### Removed

- The internal `holys3-sigv4` crate and custom IAM Identity Center credential exchange.

## [0.5.1] - 2026-07-12

### Fixed

- Watch mode now starts from shell background jobs that inherit ignored signal dispositions.

## [0.5.0] - 2026-07-12

### Added

- Continuous local and S3 indexing with paired `--watch --interval SECONDS` controls, graceful signal shutdown, and fail-fast startup followed by retry after a successful cycle.
- Machine-readable `indexed`, `error`, and `stopped` JSON Lines for one-shot and watched indexing.
- Deterministic incremental churn benchmarks and CI gates over 25,000 objects for listing latency, update latency, exact cardinality, and peak memory.

## [0.4.0] - 2026-07-12

### Added

- Dual MIT OR Apache-2.0 licensing.
- Cargo package metadata, publish scope, workspace lint configuration, and formatter/editor configuration.
- README, architecture notes, code of conduct, and security reporting policy.
- docs.rs package metadata and library crate documentation setup.
- Cross-platform CI, release tests, package verification, CodeQL, dependency review, benchmark memory gates, release checksums, and build-provenance attestations.
- ZIP, TAR, nested compression/archive members, Arrow IPC file/stream/Feather, ORC, Brotli, and zlib search support.
- Typed virtual member paths, bounded recursive decoding, conditional S3 verification, grouped source fetches, and an opt-in private source cache.

### Changed

- README now documents the actual `index`, `search`, and `stats` CLI surface.
- Index construction uses bounded external posting runs; large trigram and sparse inputs no longer materialize corpus-wide posting maps.
- Index format 9 separates physical source identity from logical searchable documents and authenticates every immutable segment blob with its own length and SHA-256 digest.
- Local freshness uses parallel BLAKE3 content tokens, and local index writes use advisory locks plus atomic replacement.
- S3 prefix, XML, multipart, retry, addressing, redirect, and credential handling now fail loudly on malformed or ambiguous protocol state.
- Search and index bodies use shared `Bytes` ownership; match lines are zero-copy slices and S3 source concurrency is byte-bounded.
- Decoded output above 8 MiB spills to temporary files; trigram indexing, sparse indexing, and files-only bounded regex verification stream large expansions with bounded memory.
- Sparse builds deduplicate grams before sorting, expanding formats build serially, and large gzip buffers use bounded trailer sizing to reduce peak memory.
- Posting runs retain closed temporary paths and use a 64-run merge fan-in, preventing large builds from exhausting file descriptors.
- S3 listings strictly validate complete XML, decode AWS percent escapes and MinIO space encoding, and preserve opaque continuation tokens.
- Prefix pruning validates segment key bounds against source tables before skipping data, and regex verification cannot cross line boundaries.
- Candidate delivery is batch-bounded and grouped by physical source; local raw-file reads and source-cache probes run concurrently under explicit limits.
- Oversized local source bodies are hash-verified and file-backed; S3 source objects of at least 64 MiB download as four concurrent streamed 8 MiB `If-Match` ranges, and large source-cache entries are file-backed too.
- Object-cache writes no longer rescan or synchronously flush disposable entries, healthy reads are lock-free, and interrupted size accounting is recovered before eviction.
- FST and postings SHA-256 digests are computed during their existing write instead of rereading temporary blobs.
- Large term dictionaries populate the local cache through bounded ranges and stay memory-mapped; verified warm-cache entries no longer rehash or materialize the entire dictionary.
- High-cardinality trigram files stream fixed-size bitmaps directly into posting runs, avoiding flattened radix allocations.
- File-backed gzip, zstd, bzip2, Snappy, Brotli, and zlib sources decode through bounded readers instead of scanning whole-source mappings.
- Segment sharding enforces its limit on decoded logical documents, including archive members, and compaction arithmetic is overflow-safe.
- Workspace version is now 0.4.0 because index format 9 and the expanded library APIs intentionally break compatibility with 0.3.0.
