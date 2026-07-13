# S3 Transport And Object Cache Implementation Plan

> [!NOTE]
> Historical planning record from 2026-07-10. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make exact verification over S3 fast and race-safe by fetching each physical source once, binding reads to the indexed object version, using measured bounded range concurrency for large raw objects, and exposing a private opt-in object cache.

**Architecture:** Keep the existing signed reqwest client, AIMD limiter, retries, hedging, and range coalescing. Add signed `If-Match` to read requests, carry immutable source identities from the index into verification, group logical documents by physical source, and put the cache between immutable source fetch and decoding. Cache entries are content-addressed by endpoint, bucket, key, and indexed version and are never enabled implicitly.

**Tech Stack:** Rust 1.88, bytes 1.10, reqwest 0.13, Tokio 1.48, BLAKE3 1.8.5, tempfile 3.27, fs4 0.13, Clap 4.5.

## Global Constraints

- Never make a live provider request without an explicit operator-approved profile assignment.
- Never inspect, source, or use unrelated cloud profiles.
- Sign `if-match` and `range` exactly as transmitted.
- Treat HTTP 412 as a typed stale-index result; never retry it.
- Fetch one physical source once per search batch even when several logical documents reference it.
- Bound in-flight source bytes and range request count.
- Keep the cache disabled unless both its directory and byte cap are explicitly supplied.
- Store cache files with owner-only permissions and never store credentials or signed URLs.

---

## File Map

- Modify `Cargo.toml`: expose workspace BLAKE3 and bytes dependencies to the transport and CLI crates.
- Modify `crates/core/src/store.rs`: replace search keys with typed immutable source requests and use `bytes::Bytes` for fetched bodies.
- Modify `crates/core/src/lib.rs`: export source request and stale-source types.
- Modify `crates/s3/Cargo.toml`: consume workspace BLAKE3, bytes, fs4, and tempfile dependencies.
- Modify `crates/s3/src/client.rs`: retain public client construction while moving request and transfer ownership into focused modules.
- Create `crates/s3/src/request.rs`: own signed attempts, response classification, conditional GET state, retry, AIMD, and hedging.
- Create `crates/s3/src/transfer.rs`: own ordered concurrent full-object and range reads plus file-backed uploads.
- Create `crates/s3/src/cache.rs`: own private immutable cache lookup, commit, LRU accounting, and cap enforcement.
- Modify `crates/s3/src/lib.rs`: expose cache configuration and adapt `S3Corpus`, `S3Fetcher`, and `S3BlobStore` to typed source requests and bytes.
- Modify `crates/index/src/search.rs`: group candidate logical documents by physical source and decode each fetched source once.
- Modify `crates/index/src/lib.rs`: export typed candidate documents instead of key-only candidates.
- Modify `crates/index/src/segment.rs`: supply source key, version, encoded length, and logical address from format-7 tables.
- Modify `crates/cli/src/main.rs`: add paired cache flags, pass cache configuration only to S3 search, and print stale-index diagnostics.
- Modify `crates/cli/tests/cli.rs`: cover paired flag validation and stable exit/error behavior.
- Modify `crates/s3/tests/minio.rs`: cover conditional reads, grouped fetches, range reads, cache hit behavior, and stale versions.
- Modify `crates/xbench/src/main.rs`: record body GET count, bytes downloaded, cache hits, and cache misses.
- Modify `crates/xbench/scenarios/queries.toml`: add repeated-source archive and large-raw-object scenarios.
- Modify `.github/workflows/bench.yml`: gate S3 cold-path regression and warm-cache zero-body-GET behavior.

## Interface Contracts

### `SourceRequest`

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceRequest {
    pub key: String,
    pub version: String,
    pub encoded_len: u64,
}
```

Input schema:

```rust
pub struct SourceRequest {
    pub key: String,       // required; exact S3 object key or local source key
    pub version: String,   // required; opaque ETag or local content digest captured by the index
    pub encoded_len: u64,  // required; complete encoded source length in bytes
}
```

Output schema: values are immutable source identities. Equality and hashing include all three fields. Construction itself cannot throw.

### `StaleSource`

```rust
#[derive(Debug, thiserror::Error)]
#[error("indexed source changed: {key} expected version {expected}")]
pub struct StaleSource {
    pub key: String,
    pub expected: String,
}
```

Output schema: `key` is the exact source key and `expected` is the indexed opaque version. It is returned only after the origin rejects `If-Match`; it is never wrapped as a transient transport failure.

### `DocFetcher::fetch_each`

```rust
fn fetch_each(
    &self,
    sources: &[SourceRequest],
    consume: &mut dyn FnMut(usize, bytes::Bytes) -> anyhow::Result<()>,
) -> anyhow::Result<()>;
```

Input schema:

```rust
type FetchInput = Vec<SourceRequest>;
```

- Every source is unique by `(key, version)` and ordered by first candidate occurrence.
- `consume` accepts the source index and complete immutable encoded body.

Output schema: success means each still-current source was consumed exactly once; a missing source is skipped under the existing stale-deletion policy; a changed source returns `anyhow::Error` containing `StaleSource`; consumer errors stop new work and propagate unchanged.

Transformation:

1. Look up each source in the configured cache.
2. Fetch misses concurrently with `If-Match` and bounded request concurrency.
3. Commit successful misses to the cache before consumption.
4. Invoke `consume` as sources complete; do not reorder for transport completion.

### `S3Client::get_if_match`

```rust
pub fn get_if_match(
    &self,
    bucket: &str,
    key: &str,
    etag: &str,
) -> anyhow::Result<Option<bytes::Bytes>>;
```

Input schema: `bucket` and `key` are exact unsigned resource identifiers; `etag` is the opaque listing ETag including any quotes emitted by S3.

Output schema: `Ok(Some(bytes))` is the complete object matching `etag`; `Ok(None)` is HTTP 404; HTTP 412 returns `Err(StaleSource { key, expected: etag })`; retry exhaustion and fatal HTTP failures preserve operation context.

Transformation:

1. Build a GET request with precondition `Some(etag)` and no range.
2. Add `if-match` to canonical signed headers and the reqwest request.
3. Execute through the existing hedge/retry/AIMD path.
4. Retain response storage as `Bytes` without copying to `Vec<u8>`.

### `S3Client::get_ranges_if_match`

```rust
pub fn get_ranges_if_match(
    &self,
    bucket: &str,
    key: &str,
    etag: &str,
    ranges: &[(u64, u64)],
) -> anyhow::Result<Option<Vec<bytes::Bytes>>>;
```

Input schema: each range is `(start, len)`, `len > 0`, and `start + len` must not overflow. Input order is meaningful and may overlap.

Output schema: one exact byte slice per input range in input order; `None` is HTTP 404; any 412 is `StaleSource`; empty input returns `Some(Vec::new())` without an HTTP call.

Transformation:

1. Validate and sort range indexes without changing output order.
2. Coalesce nearby ranges using the existing gap constant.
3. Fetch coalesced ranges concurrently with signed `range` and `if-match` headers.
4. Slice `Bytes` views from merged responses without copying and restore input order.

### `S3Client::get_ordered_ranges_if_match`

```rust
pub fn get_ordered_ranges_if_match(
    &self,
    bucket: &str,
    key: &str,
    etag: &str,
    encoded_len: u64,
    part_len: u64,
    consume: &mut dyn FnMut(bytes::Bytes) -> anyhow::Result<()>,
) -> anyhow::Result<bool>;
```

Input schema: `encoded_len` is the indexed complete object length; `part_len` is a positive measured transfer size; `consume` accepts contiguous bytes in increasing source offset.

Output schema: `true` means all `encoded_len` bytes from a current object were consumed in order; `false` means HTTP 404 before any bytes were consumed. A later missing part, 412, invalid response length, or consumer error throws with source/range context.

Transformation:

1. Partition `[0, encoded_len)` into exact non-overlapping ranges.
2. Keep at most the configured transfer concurrency in flight.
3. Buffer completed out-of-order parts in a bounded index map.
4. Drain contiguous ready parts into `consume`; release each part immediately.

### `CacheKey`

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CacheKey<'a> {
    pub endpoint: &'a str,
    pub bucket: &'a str,
    pub key: &'a str,
    pub version: &'a str,
}
```

Input schema: all fields are required exact strings; an AWS endpoint is the canonical regional origin, while a custom endpoint is its configured origin URL.

Output schema: cache paths are the lowercase BLAKE3 hex digest of length-prefixed field bytes. No source key or credentials appear in a cache filename.

Transformation: feed each field length as little-endian `u64`, then field bytes, to one BLAKE3 hasher.

### `ObjectCache::open`

```rust
pub fn open(root: &std::path::Path, cap_bytes: u64) -> anyhow::Result<ObjectCache>;
```

Input schema: `root` is an explicit cache directory; `cap_bytes > 0`.

Output schema: an initialized cache with root mode `0700`, entry file mode `0600`, and an exclusive mutation lock. Invalid caps, permission failures, unreadable metadata, and lock failures throw with the cache path.

Transformation:

1. Create the root and object subdirectory.
2. Set owner-only permissions on Unix.
3. Open the accounting lock and load the binary entry ledger.
4. Remove ledger rows whose files are absent and temporary files with no committed row.
5. Evict oldest entries until tracked bytes are at most `cap_bytes`.

### `ObjectCache::get`

```rust
pub fn get(&self, key: &CacheKey<'_>) -> anyhow::Result<Option<bytes::Bytes>>;
```

Input schema: `key` is a complete immutable cache identity.

Output schema: `Some(bytes)` is the exact committed body and updates access order; `None` means no complete entry. Corrupt length/digest metadata deletes the entry and returns an error instead of serving bytes.

Transformation:

1. Hash the key and lock the ledger.
2. Locate and validate the committed entry metadata and file length.
3. Read the file into `Bytes`, update monotonic access sequence, persist the ledger atomically, and unlock.

### `ObjectCache::put`

```rust
pub fn put(&self, key: &CacheKey<'_>, body: &bytes::Bytes) -> anyhow::Result<()>;
```

Input schema: `key` is the immutable source identity; `body` is the complete object and may exceed the cap.

Output schema: success means either the complete entry is atomically committed and accounted, or the body was deliberately not cached because it exceeds `cap_bytes`. Errors leave no committed partial entry.

Transformation:

1. Skip bodies larger than the cap.
2. Write and sync an owner-only temporary file under the cache root.
3. Lock the ledger, atomically persist the file, insert its length/access row, and evict oldest other entries to the cap.
4. Atomically rewrite and sync the ledger before unlocking.

### `ObjectCacheArgs::config`

```rust
fn config(&self) -> anyhow::Result<Option<ObjectCacheConfig>>;
```

Input schema:

```rust
#[derive(clap::Args)]
struct ObjectCacheArgs {
    #[arg(long, requires = "object_cache_cap")]
    object_cache: Option<std::path::PathBuf>,
    #[arg(long, requires = "object_cache")]
    object_cache_cap: Option<u64>,
}

pub struct ObjectCacheConfig {
    pub root: std::path::PathBuf,
    pub cap_bytes: u64,
}
```

Output schema: `None` when both flags are absent; `Some` with the exact supplied values when both are present and cap is positive; invalid combinations are Clap usage errors and zero cap is `anyhow::Error` with no implicit value.

Transformation: pattern-match the two options, reject zero, and construct the config without consulting environment variables.

## Task 1: Conditional Immutable Reads

**Files:**
- Modify: `crates/core/src/store.rs`
- Modify: `crates/core/src/lib.rs`
- Modify: `crates/s3/src/client.rs`
- Create: `crates/s3/src/request.rs`
- Modify: `crates/s3/src/lib.rs`
- Modify: `crates/s3/tests/minio.rs`

**Interfaces:**
- Produces: `SourceRequest`, `StaleSource`, `S3Client::get_if_match`, and changed `DocFetcher::fetch_each`.

- [ ] **Step 1: Add failing signature, signing, and HTTP behavior tests**

Assert `if-match` appears in signed headers and transmitted headers, a matching ETag returns exact `Bytes`, a mismatch is `StaleSource`, 412 is attempted once, and 404 remains `None`.

- [ ] **Step 2: Run the focused tests before implementation**

```bash
cargo +1.96.1 test --locked -p holys3-s3 conditional_get
```

Expected: compilation or assertions fail because conditional immutable reads are absent.

- [ ] **Step 3: Extract request ownership without behavior changes**

Move request construction, signing, attempt classification, retries, AIMD, and hedging to `request.rs`; preserve existing tests and public API byte-for-byte before adding the new precondition.

- [ ] **Step 4: Add conditional reads and typed stale errors**

Carry `Bytes` through `Outcome`, `send_resilient`, and fetch methods. Sign both range and precondition headers and classify only GET 412 as `StaleSource`; retain `PreconditionFailed` for conditional writes.

- [ ] **Step 5: Verify request behavior**

```bash
cargo +1.96.1 test --locked -p holys3-sigv4 -p holys3-s3 conditional_get
cargo +1.96.1 clippy --locked -p holys3-s3 --all-targets -- -D warnings
```

Expected: all focused tests pass and clippy is clean.

## Task 2: Grouped Source Verification

**Files:**
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/index/src/segment.rs`
- Modify: `crates/index/src/search.rs`
- Modify: `crates/s3/src/lib.rs`
- Modify: `crates/index/tests/segmented.rs`
- Modify: `crates/s3/tests/minio.rs`

**Interfaces:**
- Consumes: format-7 candidate logical documents and `SourceRequest`.
- Produces: one fetch per unique `(key, version)` and ordered logical verification events.

- [ ] **Step 1: Add a counting-fetcher regression test**

Create candidates for three members in one ZIP source plus one raw source. Assert two physical fetches, all member hits, correct virtual paths, and unchanged line/submatch ordering.

- [ ] **Step 2: Verify the test fails before grouping**

```bash
cargo +1.96.1 test --locked -p holys3-index grouped_sources_fetch_once
```

Expected: fetch count exceeds two or typed candidates are not yet available.

- [ ] **Step 3: Implement stable grouping**

Build an insertion-ordered source vector and source-to-logical-index map from candidates. Fetch sources once, decode once, route decoded members only to requested logical addresses, and retain the existing match sink stop/error behavior.

- [ ] **Step 4: Verify grouped behavior and deterministic output**

```bash
cargo +1.96.1 test --locked -p holys3-index grouped_sources_fetch_once
cargo +1.96.1 test --locked -p holys3-cli --test cli archive
```

Expected: exact output is stable across fetch completion order and each source is fetched once.

## Task 3: Ordered Range Transfers

**Files:**
- Create: `crates/s3/src/transfer.rs`
- Modify: `crates/s3/src/client.rs`
- Modify: `crates/s3/src/request.rs`
- Modify: `crates/s3/src/lib.rs`
- Modify: `crates/s3/tests/minio.rs`
- Modify: `crates/xbench/src/main.rs`

**Interfaces:**
- Produces: `S3Client::get_ranges_if_match` and `S3Client::get_ordered_ranges_if_match`.

- [ ] **Step 1: Add range property and fault tests**

For deterministic bodies around every part boundary, assert exact reassembly for 8 MiB and 16 MiB parts, bounded in-flight count, correct ordering after reversed completion, one-byte final parts, empty objects, 404, 412, short responses, and consumer cancellation.

- [ ] **Step 2: Verify focused failures**

```bash
cargo +1.96.1 test --locked -p holys3-s3 ordered_ranges
```

Expected: methods are absent.

- [ ] **Step 3: Implement bounded ordered drains**

Use `FuturesUnordered` capped by `FetchConfig.cap`; retain completed parts only until their predecessors arrive; release bytes immediately after callback consumption.

- [ ] **Step 4: Benchmark the transfer threshold and part length**

Run MinIO loopback and latency-injected tests for 16 MiB, 64 MiB, 256 MiB, and 1 GiB objects with full GET, 8 MiB ranges, and 16 MiB ranges. Select ranges only above the measured crossover and retain one part constant.

- [ ] **Step 5: Verify exact transfer behavior**

```bash
cargo +1.96.1 test --locked -p holys3-s3 ordered_ranges
cargo +1.96.1 test --locked -p holys3-s3 --test minio range
```

Expected: all bodies are byte-identical, peak retained range bytes stay within the configured bound, and the chosen mode wins or is within 10 percent of full GET.

## Task 4: Private Opt-In Object Cache

**Files:**
- Modify: `crates/s3/Cargo.toml`
- Create: `crates/s3/src/cache.rs`
- Modify: `crates/s3/src/lib.rs`
- Modify: `crates/cli/src/main.rs`
- Modify: `crates/cli/tests/cli.rs`
- Modify: `crates/s3/tests/minio.rs`

**Interfaces:**
- Produces: `CacheKey`, `ObjectCache`, `ObjectCache::open`, `ObjectCache::get`, `ObjectCache::put`, `ObjectCacheArgs::config`.

- [ ] **Step 1: Add cache contract tests**

Cover disabled-by-absence, paired flags, zero cap, key separation for endpoint/bucket/key/version, owner-only modes, atomic concurrent puts, hit bytes, version misses, oversize bypass, oldest-entry eviction, corrupt entry rejection, and recovery after interrupted temporary writes.

- [ ] **Step 2: Verify focused failures**

```bash
cargo +1.96.1 test --locked -p holys3-s3 cache
cargo +1.96.1 test --locked -p holys3-cli --test cli object_cache
```

Expected: cache types and flags are absent.

- [ ] **Step 3: Implement immutable cache storage**

Use one binary ledger with fixed-width length/access fields and hashed names, fs4 locking, same-directory temporary files, atomic persist, file and directory syncing, and deterministic oldest-access eviction.

- [ ] **Step 4: Insert cache before S3 source transfer**

Return hits directly; on misses fetch with `If-Match`, atomically cache complete bodies, then consume. Never cache 404, 412, partial ranges, failed decodes, or consumer failures.

- [ ] **Step 5: Verify privacy and behavior**

```bash
cargo +1.96.1 test --locked -p holys3-s3 cache
cargo +1.96.1 test --locked -p holys3-cli --test cli object_cache
cargo +1.96.1 clippy --locked --workspace --all-targets -- -D warnings
```

Expected: all cache tests pass, no cache path contains object keys, and no cache is created without both flags.

## Task 5: S3 Performance Gates

**Files:**
- Modify: `crates/xbench/src/main.rs`
- Modify: `crates/xbench/scenarios/queries.toml`
- Modify: `crates/xbench/README.md`
- Modify: `.github/workflows/bench.yml`

**Interfaces:**
- Produces benchmark fields `body_gets`, `range_gets`, `downloaded_bytes`, `cache_hits`, `cache_misses`, `peak_rss_bytes`, and `wall_ms` as unsigned JSON numbers.

- [ ] **Step 1: Instrument the local S3 test server**

Count only source-body GETs and bytes; exclude index metadata and posting ranges using the reserved `.holys3` namespace.

- [ ] **Step 2: Add deterministic scenarios**

Add one archive with 10,000 candidate members and one 256 MiB raw object, each searched cold twice and warm twice with exact expected hit hashes.

- [ ] **Step 3: Run the S3 benchmark matrix**

```bash
cargo +1.96.1 run --locked --release -p holys3-xbench -- s3 --profile local
```

Expected: cold grouped archive uses one body GET, warm cache uses zero body GETs, stale versions fail before verification, and result hashes equal the cache-disabled control.

- [ ] **Step 4: Add CI regression checks**

Reject missing fields, warm-cache body GETs above zero, grouped-source body GETs above source count, result-hash drift, peak RSS above 300 MiB for the standard workload, and cold wall-time regression above 10 percent.

- [ ] **Step 5: Run transport acceptance**

```bash
cargo +1.96.1 test --locked --workspace --all-targets
cargo +1.96.1 clippy --locked --workspace --all-targets -- -D warnings
cargo +1.96.1 run --locked --release -p holys3-xbench -- s3 --profile local
```

Expected: tests and clippy pass, result hashes are unchanged, cold performance passes its gate, and warm verification performs zero source-body GETs.
