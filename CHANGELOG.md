# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

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
- Index format 8 separates physical source identity from logical searchable documents and authenticates every immutable segment blob with its own length and SHA-256 digest.
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
- Workspace version is now 0.4.0 because index format 8 and the expanded library APIs intentionally break compatibility with 0.3.0.
