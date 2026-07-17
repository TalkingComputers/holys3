# Logical Documents and Formats Implementation Plan

> [!NOTE]
> Historical planning record from 2026-07-10. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one source object produce multiple exact searchable documents and add ZIP, TAR, nested compression, Arrow IPC, ORC, Brotli, and zlib support.

**Architecture:** Introduce physical source identity, logical document identity, and a streaming decoder sink. Index format 8 stores source and document tables separately; archive members become typed candidate addresses and are fetched by parent source only once.

**Tech Stack:** Rust 1.88, bytes 1.x, zip 8.6.0, tar 0.4.46, arrow-ipc 59.1, arrow-json 59.1, orc-rust 0.8.0 with isolated Arrow 58 types, brotli 8.0.4, flate2 1.1.9.

## Global Constraints

- The bytes sent to gram extraction and regex verification must be identical.
- Detection remains magic-first. Only Brotli and zlib use validated key hints.
- Never extract archive members to user-controlled filesystem paths.
- Maximum decode depth is 4, maximum members per source is 100,000, and maximum projected bytes per source is 64 GiB.
- Preserve Rust 1.88 and a stripped binary no larger than 25 MiB.
- Do not expose Arrow-58 types outside the ORC adapter module.

---

## File Map

- Modify `Cargo.toml`: add exact workspace format dependencies.
- Modify `crates/core/Cargo.toml`: add ZIP/TAR/Arrow IPC/ORC/Brotli dependencies with minimal features.
- Delete `crates/core/src/codec.rs` after moving all behavior.
- Create `crates/core/src/codec/mod.rs`: public decoder contracts and recursive dispatcher.
- Create `crates/core/src/codec/detect.rs`: magic and validated-hint detection.
- Create `crates/core/src/codec/compression.rs`: streaming compression decoders and salvage state.
- Create `crates/core/src/codec/archive.rs`: ZIP/TAR member traversal and virtual paths.
- Create `crates/core/src/codec/projection.rs`: canonical Arrow JSONL projection policy.
- Create `crates/core/src/codec/parquet.rs`: existing Parquet adapter.
- Create `crates/core/src/codec/avro.rs`: existing Avro adapter.
- Create `crates/core/src/codec/arrow_ipc.rs`: Arrow IPC file/Feather adapter.
- Create `crates/core/src/codec/orc.rs`: isolated ORC/Arrow-58 adapter.
- Modify `crates/core/src/store.rs`: replace bare document keys with source objects and typed addresses.
- Modify `crates/core/src/lib.rs`: export new source, address, decoder, and sink contracts.
- Create `crates/index/src/format.rs`: source/document tables for format 8.
- Modify `crates/index/src/build/mod.rs`: decode sources into logical-document postings and segment batches.
- Modify `crates/index/src/segment.rs`: incremental source replacement, source-level tombstones, compaction, and typed candidates.
- Modify `crates/index/src/search.rs`: typed candidate filtering and grouped document fetching.
- Modify `crates/index/src/lib.rs`: set `INDEX_FORMAT = 8` and expose typed APIs.
- Modify `crates/index/src/local.rs`: source listings and virtual-member fetching.
- Modify `crates/s3/src/lib.rs`: source listings and grouped virtual-member fetching.
- Modify `crates/cli/src/main.rs`: use typed candidates while preserving displayed virtual keys.
- Modify `crates/cli/src/scope.rs`: evaluate virtual display keys while retaining source-prefix pruning.
- Modify `crates/core/src/testutil.rs`: in-memory source and address fixtures.
- Modify `crates/index/tests/differential_store.rs`: format matrix and virtual-key oracle.
- Modify `crates/index/tests/segmented.rs`: source/member lifecycle and format-7 compaction.
- Modify `crates/cli/tests/cli.rs`: virtual paths, globs, JSON, count, and context output.

## Interface Contracts

### `SourceObject`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceObject {
    pub key: String,
    pub version: String,
    pub encoded_size: u64,
}
```

Input schema:

- `key`: required physical local path or S3 key; valid UTF-8; unique within one listing.
- `version`: required opaque freshness token; BLAKE3 hex locally and ETag for S3.
- `encoded_size`: required encoded body length in bytes.

The type has no fallible constructor; listing boundaries validate uniqueness and key shape.

### `SourceEncoding`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SourceEncoding {
    Raw,
    Gzip,
    Zstd,
    Bzip2,
    Xz,
    SnappyFrame,
    Lz4Frame,
    Parquet,
    Avro,
    Zip,
    Tar,
    ArrowIpc,
    Orc,
    Brotli,
    Zlib,
}
```

Output meaning: the successful top-level decoder selected for a source. Nested member encodings belong to their recursive decode frames, not this source-table value.

### `DocAddress`

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DocAddress {
    pub display_key: String,
    pub source_key: String,
    pub source_version: String,
    pub encoded_size: u64,
    pub encoding: SourceEncoding,
    pub member_path: Option<String>,
}
```

Input schema:

- `display_key`: required user-visible key; equals `source_key` for a single-document source or begins with `source_key + "!/"` for archive members.
- `source_key`: required physical fetch key.
- `source_version`: required indexed freshness token.
- `encoded_size`: required physical source body size.
- `encoding`: required top-level source encoding.
- `member_path`: absent for one-document sources; otherwise the complete normalized nested member chain after the first `!/`.

### `DecodeLimits`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_depth: u8,
    pub max_members: u32,
    pub max_expanded_bytes: u64,
}

pub const DECODE_LIMITS: DecodeLimits = DecodeLimits {
    max_depth: 4,
    max_members: 100_000,
    max_expanded_bytes: 64 * 1024 * 1024 * 1024,
};
```

All fields are required and nonzero. Internal validation rejects a zero field with `anyhow::Error` before decoding.

### `LogicalDocumentMeta`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDocumentMeta {
    pub display_key: String,
    pub member_path: Option<String>,
}
```

The source key/version live outside this type because one decoder invocation already owns one source.

### `DecodeSink`

```rust
pub trait DecodeSink {
    fn begin(&mut self, document: &LogicalDocumentMeta) -> anyhow::Result<()>;
    fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()>;
    fn finish(&mut self) -> anyhow::Result<()>;
}
```

State schema:

- `begin` is called exactly once before chunks for one document.
- `write` is called zero or more times with ordered nonempty chunks.
- `finish` is called exactly once after successful writes.
- A sink error aborts the source and no later callback occurs.

Output/error schema: every method returns `()` or `anyhow::Error`; decoder context adds source key and nested member path.

### `DecodeSummary`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeSummary {
    pub encoding: SourceEncoding,
    pub documents: u32,
    pub expanded_bytes: u64,
}
```

All counters include only completed logical documents. A failed decode returns no summary.

### `decode_source`

```rust
pub fn decode_source(
    source_key: &str,
    bytes: bytes::Bytes,
    limits: DecodeLimits,
    sink: &mut dyn DecodeSink,
) -> anyhow::Result<DecodeSummary>;
```

Input schema: `source_key` is required valid UTF-8; `bytes` is the complete encoded physical object; `limits` is fully specified; `sink` begins idle.

Output schema: summary of the top-level encoding and completed output. Errors include a typed message for unsupported encryption, unsafe path, depth/member/byte limits, metadata corruption, invalid hinted compression, and sink failure.

Transformation:

1. Validate limits.
2. Detect top-level encoding by exact magic, then permitted key hint.
3. Stream a one-document encoding to the sink or iterate archive members.
4. Recursively decode regular members with the nested display path.
5. Count projected chunks before forwarding; fail before exceeding a limit.

### `Corpus`

```rust
pub trait Corpus {
    fn sources(&self) -> &[SourceObject];
    fn fetch(&self, index: usize) -> anyhow::Result<bytes::Bytes>;
    fn fetch_many(
        &self,
        sources: std::ops::Range<usize>,
    ) -> anyhow::Result<Vec<(usize, bytes::Bytes)>>;
}
```

Input schema: indices refer to `sources()` positions. Implementations may omit a vanished source from `fetch_many`; other errors fail the batch.

Output schema: complete encoded bodies with their source index; completion order is unspecified.

### `DocFetcher`

```rust
pub trait DocFetcher {
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, bytes::Bytes) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;
}
```

Input schema: addresses may share source identity; each slice index is stable for the call.

Output schema: `consume(index, canonical_document_bytes)` is invoked once for every found requested logical document, in unspecified order. One parent source is fetched and decoded at most once. Vanished sources may be omitted with a warning; other errors fail the operation.

### Format-7 tables

```rust
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub(crate) struct SourceEntry {
    pub key: String,
    pub version: String,
    pub encoded_size: u64,
    pub encoding: SourceEncoding,
    pub first_doc: u32,
    pub doc_count: u32,
    pub failed: bool,
    pub retry: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub(crate) struct DocEntry {
    pub display_key: String,
    pub source_id: u32,
    pub member_path: Option<String>,
    pub decoded_size: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub(crate) struct SegmentTables {
    pub sources: Vec<SourceEntry>,
    pub documents: Vec<DocEntry>,
}
```

Invariants:

- Sources are ascending by key and unique.
- Documents for one source are contiguous and ascending by display key.
- `first_doc..first_doc + doc_count` lies within `documents`.
- Every document's `source_id` points to the owning source.
- Failed zero-document sources remain represented.

### `IndexReader`

```rust
pub trait IndexReader {
    fn strategy(&self) -> Strategy;
    fn total_docs(&self) -> usize;
    fn candidate_docs(
        &self,
        query: &seagrep_query::Query,
        key_prefix: Option<&str>,
    ) -> anyhow::Result<Vec<DocAddress>>;
    fn stats(&self) -> IndexStats;
}
```

Output schema: candidates are a superset of true matching logical documents. Every address is complete and display-key sorted. Missing/corrupt blobs and root changes return contextual typed errors.

## Task 1: Source and Decoder Contracts

**Files:**
- Modify: `crates/core/src/store.rs`
- Modify: `crates/core/src/lib.rs`
- Modify: `crates/core/src/testutil.rs`
- Modify: every `Corpus` and `DocFetcher` implementation until the workspace compiles.

- [ ] **Step 1: Add compile-time contract tests**

Construct one `SourceObject`, one `DocAddress`, and a recording `DecodeSink`; assert every required field and callback transition.

- [ ] **Step 2: Replace `Doc` and bare-key fetch APIs**

Apply the exact contracts above. Convert `Vec<u8>` bodies to `bytes::Bytes` at fetch boundaries.

- [ ] **Step 3: Preserve current one-source/one-document behavior**

Implement a temporary adapter that calls existing `decode_body`, then emits one begin/write/finish sequence. This step changes types only.

- [ ] **Step 4: Verify the workspace**

```bash
cargo +1.96.1 check --locked --workspace --all-targets
cargo +1.96.1 nextest run --locked --workspace --all-features --profile ci
```

- [ ] **Step 5: Commit typed source contracts**

```bash
git add crates/core crates/index crates/s3 crates/cli crates/xbench
git commit -m "refactor: separate source and document identity"
```

## Task 2: Split Existing Codec Ownership

**Files:**
- Delete: `crates/core/src/codec.rs`
- Create: `crates/core/src/codec/mod.rs`
- Create: `crates/core/src/codec/detect.rs`
- Create: `crates/core/src/codec/compression.rs`
- Create: `crates/core/src/codec/projection.rs`
- Create: `crates/core/src/codec/parquet.rs`
- Create: `crates/core/src/codec/avro.rs`

- [ ] **Step 1: Move tests before moving code**

Keep every existing codec test name and assertion. Place detector tests in `detect.rs`, compression tests in `compression.rs`, and structured projection tests beside their adapters.

- [ ] **Step 2: Move code without semantic changes**

Expose only `decode_source`, `DecodeLimits`, `DecodeSink`, `DecodeSummary`, `LogicalDocumentMeta`, `SourceEncoding`, and `DECODE_LIMITS` from `mod.rs`. All format helpers remain module-private.

- [ ] **Step 3: Verify byte-identical existing projections**

```bash
cargo +1.96.1 nextest run --locked -p seagrep-core --all-features
```

- [ ] **Step 4: Commit codec decomposition**

```bash
git add crates/core/src/codec.rs crates/core/src/codec
git commit -m "refactor: split codec pipeline by format"
```

## Task 3: Recursive ZIP and TAR

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/core/Cargo.toml`
- Create: `crates/core/src/codec/archive.rs`
- Modify: `crates/core/src/codec/detect.rs`
- Modify: `crates/core/src/codec/mod.rs`

- [ ] **Step 1: Add failing archive tests**

Generate ZIP and TAR fixtures in memory and assert virtual keys, nested `tar.gz`, duplicate names, empty members, directories, traversal failure, links ignored, encrypted ZIP failure, four-layer success, five-layer failure, 100,000-member boundary, and expanded-byte boundary.

- [ ] **Step 2: Add minimal dependency features**

Use:

```toml
zip = { version = "8.6.0", default-features = false, features = ["bzip2", "deflate-flate2-zlib-rs", "deflate64", "lzma", "xz", "zstd"] }
tar = { version = "0.4.46", default-features = false }
```

Do not enable ZIP AES because encrypted entries must fail.

- [ ] **Step 3: Implement normalized virtual paths**

Reject absolute, prefix, parent, and NUL components. Convert separators to `/`. Track duplicate full normalized paths in insertion order and append `#2`, `#3`, and onward.

- [ ] **Step 4: Implement recursive member dispatch**

ZIP iterates central-directory order. TAR iterates stream order. Only regular files recurse; ignored entries do not count toward member limits. Count each completed leaf document once.

- [ ] **Step 5: Verify differential archive search**

Run core tests, index differential tests, and CLI glob/JSON tests with virtual keys.

- [ ] **Step 6: Commit archives**

```bash
git add Cargo.toml Cargo.lock crates/core/Cargo.toml crates/core/src/codec
git commit -m "feat: search nested ZIP and TAR members"
```

## Task 4: Arrow IPC, Brotli, zlib, and ORC

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/core/Cargo.toml`
- Create: `crates/core/src/codec/arrow_ipc.rs`
- Create: `crates/core/src/codec/orc.rs`
- Modify: `crates/core/src/codec/compression.rs`
- Modify: `crates/core/src/codec/detect.rs`
- Modify: `crates/core/src/codec/projection.rs`

- [ ] **Step 1: Add failing format fixtures**

Cover Arrow IPC file/Feather with dictionaries and IPC compression; ORC with primitive, decimal, timestamp, list, struct, null, and compression variants; Brotli `.br`; zlib `.zlib` and `.zz`; text lookalikes and wrong-extension corruption.

- [ ] **Step 2: Add current dependencies**

Use Arrow 59 for IPC. Add `orc-rust = { version = "0.8.0", default-features = false }`, package-renamed Arrow-58 JSON dependencies only inside `orc.rs`, and `brotli = { version = "8.0.4", default-features = true }`.

- [ ] **Step 3: Implement Arrow IPC file detection**

Require `ARROW1` at the start and end plus successful `FileReader::try_new`. Do not guess IPC streams from `0xffffffff`.

- [ ] **Step 4: Implement canonical projection**

Feed each RecordBatch through the version-matched Arrow JSON writer into a bounded scratch vector, forward it to `DecodeSink::write`, then clear it. Preserve current compact JSONL policy.

- [ ] **Step 5: Implement validated hinted compression**

Only `.br`, `.zlib`, and `.zz` select these codecs without magic. Require successful stream completion; an error before completion fails instead of falling back to raw bytes.

- [ ] **Step 6: Verify format matrix and binary size**

```bash
cargo +1.96.1 nextest run --locked -p seagrep-core -p seagrep-index --all-features
cargo +1.96.1 build --locked --release -p seagrep
test "$(stat -f %z target/release/seagrep)" -le 26214400
```

- [ ] **Step 7: Commit structured formats**

```bash
git add Cargo.toml Cargo.lock crates/core/Cargo.toml crates/core/src/codec
git commit -m "feat: search Arrow ORC Brotli and zlib data"
```

## Task 5: Index Format 8

**Files:**
- Create: `crates/index/src/format.rs`
- Modify: `crates/index/src/build/mod.rs`
- Modify: `crates/index/src/segment.rs`
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/index/tests/segmented.rs`

- [ ] **Step 1: Add format invariant tests**

Reject unsorted/duplicate sources, noncontiguous document ranges, wrong source IDs, display keys outside their source, out-of-range counts, and format-6 roots.

- [ ] **Step 2: Implement source/document table serialization**

Set `INDEX_FORMAT` to 8. Parse and validate `SegmentTables` before exposing entries.

- [ ] **Step 3: Build logical-document postings**

Assign document IDs at `DecodeSink::begin`. Stream bytes into the selected gram extractor. Finalize grams at `finish`, append `DocEntry`, and record the source's contiguous range only after the source completes.

- [ ] **Step 4: Preserve failed zero-document sources**

Append a `SourceEntry` with `doc_count = 0`; mark transient misses retryable and permanent format failures nonretryable until the source version changes.

- [ ] **Step 5: Change incremental replacement to source keys**

Build the live map from source tables. A changed/deleted source tombstones its complete logical range. Group new source results into segments without splitting one source and flush before the 4,000,000-document cap.

- [ ] **Step 6: Update compaction**

Merge source tables in source-key order, append each source's live documents contiguously, build old-to-new document ID maps, and merge postings through those maps.

- [ ] **Step 7: Verify lifecycle tests**

Cover adding/removing/replacing one member, changing nested archive paths, empty archives, failed archives, compaction, garbage collection, and stale reader retry.

- [ ] **Step 8: Commit format 8**

```bash
git add crates/index/src crates/index/tests/segmented.rs
git commit -m "feat: index logical documents per source"
```

## Task 6: Typed Candidate Search

**Files:**
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/index/src/segment.rs`
- Modify: `crates/index/src/search.rs`
- Modify: `crates/index/src/local.rs`
- Modify: `crates/s3/src/lib.rs`
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/cli/src/scope.rs`
- Modify: `crates/cli/tests/cli.rs`

- [ ] **Step 1: Add candidate-address tests**

Assert complete addresses, display-key ordering, prefix pruning, archive-member globs, key regexes, time scopes, JSON paths, counts, context, and files-only output.

- [ ] **Step 2: Replace `candidate_keys` with `candidate_docs`**

Construct addresses by joining selected document entries to their source entry. Validate table references during load, not during every query.

- [ ] **Step 3: Group fetches by source**

Local and S3 fetchers build a map from `(source_key, source_version)` to requested slice indices. Fetch once, decode once, and route each completed logical document by exact `member_path`.

- [ ] **Step 4: Preserve output semantics**

Pass `display_key` to sinks and `SearchStats.hits`. Apply prefix, regex, time, and glob predicates to display keys after segment source-prefix pruning.

- [ ] **Step 5: Verify scan equivalence**

The scan oracle must call `decode_source` and compare `(display_key, LineEvent)` output with indexed search across the full format matrix.

- [ ] **Step 6: Commit typed search**

```bash
git add crates/core crates/index crates/s3 crates/cli
git commit -m "feat: search virtual archive documents"
```

## Task 7: Format Acceptance

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`
- Modify: `CHANGELOG.md`
- Modify: `.github/workflows/bench.yml`

- [ ] **Step 1: Run large synthetic cases**

Run 10,000-member ZIP and TAR, 1 GiB decoded gzip/zstd, nested depth boundaries, and expansion failures. Require exact hit keys and bounded RSS.

- [ ] **Step 2: Run complete verification**

```bash
cargo +1.96.1 fmt --all --check
cargo +1.96.1 nextest run --locked --workspace --all-features --profile ci
cargo +1.96.1 test --locked --release --workspace --all-features
cargo +1.96.1 clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.88 check --locked --workspace --all-targets
cargo +1.96.1 package --locked --workspace --allow-dirty
```

- [ ] **Step 3: Document exact behavior**

Update format detection, virtual path syntax, recursion limits, unsupported encryption, rebuild requirement, benchmark methods, and ORC dependency isolation.

- [ ] **Step 4: Commit documentation and gates**

```bash
git add README.md ARCHITECTURE.md CHANGELOG.md .github/workflows/bench.yml
git commit -m "docs: document logical format search"
```
