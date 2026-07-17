# seagrep Engine Expansion Design

> [!NOTE]
> Historical design record from 2026-07-10. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).

## Objective

Make seagrep materially faster and more scalable while preserving its exact-search contract, then expand the decoder from one searchable document per object to multiple logical documents per source object.

The work covers three coordinated areas:

1. Reduce index-build CPU, allocation, and peak memory.
2. Add archive and columnar formats without duplicating indexing and verification semantics.
3. Reduce S3 transfer copies, prevent stale-object verification, and accelerate repeated searches without caching private object data unless the user opts in.

## Baseline

The measured local release baseline on 2026-07-10 is 25,000 objects of 4,096 bytes each:

- Wall time: 7.83 seconds.
- User time: 5.61 seconds.
- System time: 2.25 seconds.
- Peak RSS: 500,629,504 bytes.
- The sampling profile attributes the largest actionable CPU share to the two comparison sorts in trigram construction: per-document gram sorting and flattened posting-run sorting.
- Sequential local content hashing and file-open/read work is the next large source-owned cost.

The existing 1,000-object MinIO benchmark and 25,000-object scale benchmark remain correctness and regression gates.

## Success Criteria

### Indexing

- The 25,000 by 4 KiB local benchmark completes in at most 4.5 seconds on the baseline machine.
- Peak RSS for that benchmark is at most 300 MiB.
- A synthetic source with 1 GiB of decoded text completes indexing without holding the decoded body or final index blobs entirely in memory.
- Existing trigram and sparse candidate-superset invariants remain unchanged.

### Search

- Existing cold local and S3-compatible benchmarks regress by no more than 10 percent at p50 or p95.
- Ranged index reads return shared byte slices instead of copying every requested subrange.
- An enabled object cache performs zero source-body GETs on an unchanged warm query.
- Search fetches use the indexed ETag as an `If-Match` precondition and report a stale index on HTTP 412.

### Formats

- ZIP, TAR, nested compression/archive chains, Arrow IPC file/Feather v2, ORC, Brotli, and zlib are searchable.
- ZIP and TAR regular-file members are independent logical documents with deterministic virtual paths.
- Indexed and scan-oracle results are identical for every supported format and nesting combination.
- Archive traversal, encryption, expansion-limit, malformed-container, duplicate-name, and truncation behavior is deterministic and tested.

### Distribution

- Rust 1.88 remains the MSRV.
- The stripped release binary remains at most 25 MiB.
- All crates still package independently and pass semver checks for the selected release version.

## Chosen Approach

Evolve the current exact document-level index instead of replacing it with a positional block index.

A positional index could reduce cold transfer for large uncompressed objects, but it would enlarge the index substantially and cannot safely range-decode common gzip, zstd, ZIP, or TAR sources. The selected design improves the dominant measured costs while retaining one source fetch for exact verification.

## Data Model

### Source objects

A source object is the physical local file or S3 object used for incremental freshness and fetching:

```rust
pub struct SourceObject {
    pub key: String,
    pub version: String,
    pub encoded_size: u64,
}
```

`version` is the S3 ETag for S3 sources and a BLAKE3 content hash for local files.

### Logical documents

A logical document is one independently searchable decoded byte stream:

```rust
pub struct LogicalDocument {
    pub display_key: String,
    pub source_key: String,
    pub member_path: Option<String>,
    pub decoded_size: u64,
}
```

Normal, compressed, Parquet, Avro, Arrow IPC, and ORC sources emit one logical document. ZIP and TAR sources emit one logical document per regular-file member. Nested archive paths append `!/member` at each archive boundary.

Duplicate archive member names receive a deterministic `#<ordinal>` suffix after the first occurrence. Absolute paths, parent traversal, and NUL bytes fail the complete source. ZIP directory entries and TAR directories, links, devices, FIFOs, and other non-regular entries are ignored. No member is extracted to a filesystem path.

### Indexed addresses

Candidate lookup returns typed addresses rather than bare strings:

```rust
pub struct DocAddress {
    pub display_key: String,
    pub source_key: String,
    pub source_version: String,
    pub member_path: Option<String>,
}
```

Fetchers group addresses by `(source_key, source_version)`, fetch each source once, decode it once, and emit only requested logical members. Output, glob filtering, key regexes, and hit lists use `display_key`. Source-level prefix pruning remains valid because every virtual display key starts with its source key.

## Segment Format

Index format 8 stores two tables per segment:

1. A source table keyed by physical source key, containing source version, encoded size, detected encoding, decode status, and the logical-document ID range.
2. A logical-document table keyed by local document ID, containing display key, source-table ID, optional member path, and decoded size.

Posting IDs reference logical-document IDs. Incremental comparison and tombstoning operate on source keys. Replacing or deleting one source tombstones all logical documents emitted by that source. Empty archives and permanent decode failures remain represented in the source table so unchanged sources are not fetched on every update.

An older format fails with the existing explicit rebuild error. There is no in-place migration because rebuilding is simpler and avoids retaining incompatible source/document identities.

## Decoder Pipeline

The decoder becomes a recursive source-to-logical-document pipeline:

```text
source bytes
  -> exact magic detection or validated key hint
  -> streaming decompressor or container reader
  -> zero or more logical documents
  -> canonical byte projection
  -> index gram sink or search line sink
```

The same pipeline and canonical projection are used during indexing and verification. This preserves the core invariant that the index can only remove impossible candidates and the regex verifier decides matches.

### Detection

- Existing magic-based gzip, zstd, bzip2, xz, Snappy framed, LZ4 framed, Parquet, and Avro detection remains.
- ZIP requires a valid ZIP signature and successful central-directory parsing.
- TAR requires a checksum-valid header and recognized ustar, PAX, or GNU metadata. Printable `ustar` text alone is insufficient.
- Arrow IPC file requires `ARROW1` framing and a valid footer. Arrow stream input is not auto-detected because the continuation marker is not a reliable standalone signature.
- ORC requires its magic plus successful PostScript/footer parsing.
- Brotli and zlib have no sufficiently reliable universal magic. They are attempted only for `.br` and `.zz`/`.zlib` key hints, and decoder validation must consume a valid stream.

### Recursion and limits

- Maximum nested decode depth: 4.
- Maximum logical members per source: 100,000.
- Maximum total expanded bytes per source: 64 GiB.
- Limits count canonical projected bytes, not only archive member headers.
- Exceeding a limit fails the source loudly and records the failure state.
- ZIP encryption fails explicitly. No password callback or silent member skip exists.

### Canonical structured projection

Parquet, Avro, Arrow IPC, and ORC batches project to one compact JSON object per row followed by `\n`. Null, decimal, binary, timestamp, NaN, and nested-value behavior remains aligned with the existing Parquet/Avro policy.

Arrow IPC uses Arrow 59 directly. `orc-rust` 0.8 depends on Arrow 58, so ORC lives behind a narrow adapter that accepts bytes and returns canonical JSONL bytes without exposing Arrow-58 types to the rest of the workspace. ORC disables its default async feature because seagrep already owns transport and scheduling.

## Index Construction

### Gram extraction

The first implementation benchmarks these candidates on identical corpora:

1. Stable radix sort for per-document `u32` trigrams and flattened `(gram, doc_id)` entries.
2. K-way merge of already-sorted per-document gram vectors.
3. The current comparison-sort implementation as the control.

The selected implementation preserves sorted unique grams and ascending document IDs. Bounded batches use stable radix sorting. Oversized packed batches switch to the byte-identical k-way merge, and high-cardinality file-backed inputs retain a fixed trigram bitmap that streams directly into a posting run.

### Local freshness

Local files are hashed in parallel with BLAKE3. Result ordering is restored to lexical path order before segment construction, so concurrency cannot change document IDs or index bytes.

### Streaming output

FST and postings builders write to temporary files. The build result contains paths, lengths, and streaming hashes instead of complete `Vec<u8>` blobs. Segment identity is computed by streaming all immutable segment files in their canonical order.

`BlobStore` gains a file-backed write operation:

- Local storage writes and atomically renames within the destination filesystem.
- S3 storage uploads small files once and large files as concurrent multipart parts read from fixed file offsets.
- Temporary files live until all immutable blobs and the root pointer are committed or the build fails.

No output blob is duplicated solely to satisfy a storage API.

### Streaming input

Compression readers feed chunks into consumers:

- Trigram indexing retains only the previous two bytes between chunks.
- Search verification retains one line plus the required context ring.
- Structured readers emit one projected record batch at a time.
- Sparse indexing spools decoded bytes to a temporary file and uses a bounded file window with resumable gram state because sparse grams can span beyond a fixed-size boundary.

Single pathological lines remain exact: the line buffer can grow to the line length, but the full source body is not retained.

## Search and S3 Transport

### Shared response buffers

Small S3 response bodies use shared `bytes::Bytes`; large source ranges and cache entries use sealed file-backed `DocumentBody` values. Coalesced index reads retain bounded shared buffers, while immutable term dictionaries populate the local cache through ranged reads and remain memory-mapped for lookup.

### Request identity

Every candidate carries the indexed source version. S3 `GetObject` sends `If-Match` with that ETag. HTTP 412 becomes a typed stale-index error that tells the user to rerun `seagrep index`; verification never silently runs against a different object version.

### Connections, retries, and ranges

The existing reusable reqwest client, horizontal concurrency, adaptive throttling, request hedging, and range coalescing remain. Transport changes require cold MinIO and representative in-region S3 evidence because higher concurrency or forced HTTP/2 is not universally faster across S3-compatible endpoints.

Large file-backed index uploads use concurrent multipart parts. Sources of at least 64 MiB use four concurrent 8 MiB range GETs whose response chunks spool to sealed temporary files. Every range carries the indexed `If-Match` precondition. The canonical decoder consumes the assembled file-backed body, so raw and encoded sources share one bounded transport path.

### Object cache

Private source-body caching is opt-in:

```text
--object-cache <DIR> --object-cache-cap <SIZE>
```

Both options are required together. Entries are raw encoded source bodies keyed by endpoint, bucket, source key, and indexed version. Writes are atomic, reads verify the content length, and least-recently-used eviction runs before admitting a new object. Disabling the option leaves no source-body data on disk.

Immutable index-blob caching remains automatic and separate.

## Module Boundaries

The change must reduce large-file concentration rather than append branches to existing modules:

```text
crates/core/src/codec/
  mod.rs
  detect.rs
  compression.rs
  archive.rs
  arrow.rs
  avro.rs
  projection.rs

crates/index/src/build/
  mod.rs
  grams.rs
  runs.rs
  output.rs

crates/index/src/
  format.rs
  local.rs

crates/s3/src/
  cache.rs
  request.rs
  transfer.rs
```

Public ownership remains unchanged: core owns decoding, index owns index format and candidate planning, S3 owns network/storage, and CLI owns options and rendering.

## Error Semantics

- Corrupt compressed tails retain the existing clean-prefix salvage policy only when the decoder can prove it emitted a valid prefix.
- Container metadata errors fail the complete source because member identity would be unreliable.
- A corrupt archive member fails that source rather than silently producing an incomplete index.
- Missing sources during indexing remain retryable.
- Missing sources during search warn and skip as stale deletions.
- ETag mismatch during search fails with a stale-index error.
- Object-cache corruption evicts the entry and fails that cache read; it does not silently substitute unverified bytes.

## Tests

### Unit and property tests

- Radix and k-way posting runs must byte-match the comparison-sort control over randomized corpora.
- Streaming trigram extraction must equal whole-buffer extraction for every split point.
- Streaming line verification must equal `grep_doc` for randomized lines, context, counts, and split points.
- Each detector receives valid, truncated, text-lookalike, and malformed inputs.
- Archive paths cover traversal, absolute paths, links, duplicate names, nested archives, and all limits.
- Arrow/ORC projection tests cover nulls, decimals, timestamps, binary, lists, structs, and non-finite floats.

### Differential tests

For every format, indexed search and the decoded scan oracle run the same pattern matrix. Archive results compare virtual display keys and line events, not only hit counts.

### Integration tests

- Local incremental add, modify, delete, and archive-member replacement.
- S3-compatible MinIO cold and warm object-cache behavior.
- One source GET for multiple matching archive members.
- `If-Match` success and 412 stale-index failure.
- Multipart file-backed index upload and abort-on-failure.

### Scale and performance gates

- 25,000 by 4 KiB high-cardinality corpus.
- 100,000 small objects.
- 1 GiB decoded gzip and zstd sources.
- 10,000-member ZIP and TAR sources.
- Nested archive depth and expansion-limit boundaries.
- Cold and warm search scenarios, candidate counts, source GET counts, bytes transferred, p50, p95, p99, CPU time, and peak RSS.

Criterion tracks gram extraction, run construction, posting merge/decode, query planning, line verification, archive projection, and Arrow/ORC projection. CI rejects missing benchmarks, incorrect hit counts, memory-limit failures, and statistically material regressions.

## Delivery Order

1. Add benchmark coverage and retain the current implementation as a byte-for-byte control.
2. Optimize gram sorting, local hashing, response-buffer ownership, and file-backed index output.
3. Introduce source/logical-document tables and format-7 reader/writer support.
4. Split the codec pipeline and add ZIP/TAR plus nested decoding.
5. Add Arrow IPC, Brotli, zlib, and isolated ORC support.
6. Add grouped archive fetching, ETag preconditions, ordered large-raw-object range reads, and the opt-in object cache.
7. Run all correctness, performance, package, security, and cross-platform gates.
8. Re-profile, repeat current-source research, and implement only additional changes supported by measured evidence and the exact-search contract.

## Primary References

- AWS S3 performance guidelines: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html
- AWS S3 GetObject and `If-Match`: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
- Apache Arrow IPC format: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
- Arrow Rust IPC reader: https://docs.rs/arrow-ipc/59.0.0/arrow_ipc/reader/
- ORC Rust 0.8: https://docs.rs/orc-rust/0.8.0/orc_rust/
- ZIP Rust 8.6: https://docs.rs/zip/8.6.0/zip/
- TAR Rust 0.4.46: https://docs.rs/tar/0.4.46/tar/
- Radix sort 0.1.1: https://docs.rs/radsort/0.1.1/radsort/
- BLAKE3 Rust 1.8.5: https://docs.rs/blake3/1.8.5/blake3/
