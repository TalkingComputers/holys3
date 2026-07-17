# Index Performance Implementation Plan

> [!NOTE]
> Historical planning record from 2026-07-10. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut default trigram index wall time and peak RSS by removing comparison-sort and whole-output-buffer bottlenecks without changing index/search results.

**Architecture:** Keep document-level trigram semantics and the segmented index. Benchmark stable radix sorting against a k-way merge control, parallelize deterministic local hashing, then write final FST/posting blobs to temporary files and stream them through `BlobStore`.

**Tech Stack:** Rust 1.88, Rayon 1.12, radsort 0.1.1, BLAKE3 1.8.5, fst 0.4.7, tempfile 3.27, Criterion 0.8.2.

## Global Constraints

- Preserve `index candidates` as a superset of exact regex matches.
- Preserve deterministic segment bytes across fetch completion order and thread count.
- Keep Rust 1.88 as MSRV.
- Do not make a real AWS call; local and MinIO tests only.
- Run the 25,000 by 4 KiB benchmark before and after each accepted optimization.
- Reject any optimization that regresses wall time by more than 10 percent without a larger verified memory gain.

---

## File Map

- Modify `Cargo.toml`: add workspace dependencies `blake3 = "1.8.5"` and `radsort = "0.1.1"`.
- Modify `crates/core/Cargo.toml`: consume workspace BLAKE3 and radsort dependencies.
- Modify `crates/index/Cargo.toml`: consume workspace radsort dependency.
- Modify `crates/core/src/grams.rs`: add the measured packed-gram sort and retain byte-identical output.
- Modify `crates/index/src/lib.rs`: replace flattened comparison sorting, parallelize local hashing, and move build ownership into focused modules.
- Create `crates/index/src/build/mod.rs`: own corpus-to-temporary-index orchestration.
- Create `crates/index/src/build/runs.rs`: own temporary posting records and external merge.
- Create `crates/index/src/build/output.rs`: own file-backed FST/posting output and streaming hashes.
- Create `crates/index/src/local.rs`: own local corpus walking, deterministic listing, and BLAKE3 freshness tokens.
- Modify `crates/core/src/store.rs`: add file-backed immutable blob writes.
- Modify `crates/s3/src/client.rs`: add bounded-memory file and multipart upload.
- Modify `crates/s3/src/lib.rs`: wire `S3BlobStore::put_file`.
- Modify `crates/index/src/segment.rs`: consume file-backed build outputs.
- Modify `crates/index/benches/hot_paths.rs`: add end-to-end build and run-construction benchmarks.
- Modify `crates/index/tests/segmented.rs`: verify deterministic file-backed segments.
- Modify `.github/workflows/bench.yml`: enforce wall-time/RSS and benchmark-presence gates.

## Interface Contracts

### `generated_corpus`

```rust
fn generated_corpus(objects: usize, size: usize) -> MemCorpus;
```

Input schema: `objects` is the exact document count and `size` is the exact encoded bytes per document. Both are positive benchmark constants.

Output schema: a deterministic in-memory corpus with lexical keys and exactly `objects * size` body bytes. It cannot throw.

Transformation: render repeatable production-shaped log records containing timestamp, level, service, request ID, and message fields for each object; repeat and truncate each body to `size` bytes.

### `bench_index_build`

```rust
fn bench_index_build(c: &mut criterion::Criterion);
```

Input schema: `c` is Criterion's mutable benchmark registry.

Output schema: registered benchmark IDs `index_build_1024x4096_trigram` and `index_build_1024x4096_sparse`; setup failures panic with `benchmark setup failed` context.

Transformation: create a fresh deterministic corpus, listing, blob directory, and cache directory outside each timed iteration; time one complete `update_index` call for each strategy.

### `bench_packed_sort_crossover`

```rust
fn bench_packed_sort_crossover(c: &mut criterion::Criterion);
```

Input schema: `c` is Criterion's mutable benchmark registry.

Output schema: paired `packed_sort_control/<length>` and `packed_sort_hybrid/<length>` benchmark IDs for 128, 256, 512, 1,024, 4,096, and 65,536 trigram windows.

Transformation: generate one deterministic byte input per length; benchmark direct integer extraction plus standard sort/dedup as the control and `pack_trigram_grams` as the candidate.

### `sort_packed_grams`

```rust
fn sort_packed_grams(grams: &mut Vec<u32>);
```

Input schema:

```rust
type PackedGramInput = Vec<u32>;
```

- Each value uses only its low 24 bits and represents one three-byte gram in big-endian lexical order.
- Values may be unsorted and duplicated.

Output schema: the same vector is ascending and deduplicated. It cannot return an error or panic for valid `u32` input.

Transformation:

1. Use standard unstable sort below the benchmarked crossover length.
2. Use `radsort::sort` at or above the crossover.
3. Call `dedup` once.

### `write_trigram_run_radix`

```rust
fn write_trigram_run_radix(grammed: Vec<(usize, Vec<u32>)>) -> anyhow::Result<std::fs::File>;
```

Input schema:

```rust
type TrigramDocument = (usize, Vec<u32>);
```

- `usize` is the segment-local document ID and must fit `u32`.
- Each gram vector is ascending and unique.
- Documents may arrive in any order.

Output schema: a temporary file of repeated seven-byte records: three-byte big-endian gram followed by four-byte big-endian document ID. Records are ascending by `(gram, document_id)` with duplicates removed. Errors are `anyhow::Error` preserving integer conversion, temporary-file, seek, and write context.

Transformation:

1. Pack each record as `(gram, DocId)`.
2. Stable-radix-sort by `(gram, DocId)` using a scalar `u64` key.
3. Deduplicate records.
4. Write fixed-width records and seek to byte zero.

### `write_trigram_run_merge`

```rust
fn write_trigram_run_merge(grammed: Vec<(usize, Vec<u32>)>) -> anyhow::Result<std::fs::File>;
```

Input and output schemas equal `write_trigram_run_radix`.

Transformation:

1. Sort documents by converted `DocId`.
2. Seed a min-heap with the first gram from every document.
3. Pop `(gram, id, document_index, gram_index)`, write it unless equal to the previous record, then push that document's next gram.
4. Seek the completed file to byte zero.

### `TempBlob`

```rust
pub(crate) struct TempBlob {
    file: tempfile::NamedTempFile,
    len: u64,
}

impl TempBlob {
    pub(crate) fn path(&self) -> &std::path::Path;
    pub(crate) fn len(&self) -> u64;
}
```

Output schema: `path()` remains valid for the lifetime of `TempBlob`; `len()` exactly equals filesystem metadata length. No method throws.

### `BuiltIndexFiles`

```rust
pub(crate) struct BuiltIndexFiles {
    pub fst: TempBlob,
    pub postings: TempBlob,
    pub failed: Vec<DocId>,
    pub retryable: Vec<DocId>,
}
```

- `failed` and `retryable` are ascending, unique segment-local IDs.
- Every `retryable` ID is also present in `failed`.

### `build_index_files`

```rust
pub(crate) fn build_index_files(
    corpus: &dyn Corpus,
    strategy: Strategy,
) -> anyhow::Result<BuiltIndexFiles>;
```

Input schema: `corpus.docs()` is the complete stable segment ordering; `fetch_many` may return those IDs in any order or omit vanished documents. `strategy` is `Trigram` or `Sparse`.

Output schema: two complete temporary blobs plus sorted failure IDs. Errors retain fetch, decode, run-write, merge, FST, and posting-encode context.

Transformation:

1. Partition source IDs by existing count/encoded-byte caps.
2. Fetch and decode each partition in parallel.
3. Produce sorted posting runs using the benchmark-selected trigram algorithm or bounded sparse runs.
4. K-way merge runs directly into file-backed FST and postings writers.
5. Flush and sync both blobs; return their owned temporary files.

### `BlobStore::put_file`

```rust
fn put_file(&self, name: &str, path: &std::path::Path) -> anyhow::Result<()>;
```

Input schema: `name` is a validated store-relative immutable blob key; `path` references a readable regular file that remains unchanged for the call.

Output schema: success means the complete source file is committed under `name`. Errors identify the source path and destination blob. No partial destination is observable after an error.

Transformation for local storage: copy into a named temporary file under the destination parent, flush, sync, and persist atomically.

Transformation for S3 storage: files no larger than the multipart threshold use one buffered PUT; larger files read fixed parts from offsets, upload at bounded concurrency, complete only after every part succeeds, and abort the multipart upload after any failure.

### `hash_file`

```rust
fn hash_file(path: &std::path::Path) -> anyhow::Result<String>;
```

Input schema: `path` is a local regular file.

Output schema: lowercase 64-character BLAKE3 hexadecimal digest. Errors include the displayed path and underlying open/read failure.

Transformation: stream the file through `blake3::Hasher::update_reader`; never read the complete file into memory.

### `LocalCorpus::listing`

```rust
pub fn listing(&self) -> anyhow::Result<Vec<(String, String, u64)>>;
```

Input schema: `self.paths` and `self.docs` have equal lengths and lexical path ordering.

Output schema: one `(key, BLAKE3 digest, encoded size)` tuple per document in exactly `self.docs()` order. Any worker error aborts the complete result.

Transformation: parallel-map indexed paths to digests, collect through Rayon’s indexed iterator, and zip with existing keys/sizes without a second sort.

### `LocalCorpus::fetch_many`

```rust
fn fetch_many(&self, docs: std::ops::Range<usize>) -> anyhow::Result<Vec<(usize, Vec<u8>)>>;
```

Input schema: `docs` is a contiguous in-bounds range of corpus positions.

Output schema: one `(position, complete file bytes)` tuple per position in ascending order. Any open/read failure aborts the batch with its path.

Transformation: parallel-map the indexed position range through `std::fs::read`, then collect through Rayon’s indexed iterator so transport completion does not change output order.

### `upload_dir`

```rust
fn upload_dir() -> anyhow::Result<()>;
```

Input schema: the deterministic generated corpus already exists at `objects_dir()`.

Output schema: a complete local index at `local_index_dir()` and its path on stdout. Filesystem, listing, build, or write failures retain context.

Transformation: remove the old benchmark index, enumerate the generated directory directly, build its listing, and run a fresh trigram update. The timed local path does not parse the unrelated expected-hit manifest.

## Task 1: Benchmark Controls

**Files:**
- Modify: `crates/index/benches/hot_paths.rs`
- Modify: `crates/xbench/src/main.rs`
- Modify: `.github/workflows/bench.yml`

**Interfaces:**
- Consumes: existing `update_index`, `LocalBlobStore`, `MemCorpus`.
- Produces: Criterion IDs `index_build_1024x4096_trigram` and `index_build_1024x4096_sparse`; xbench phase fields `listing_ms`, `build_ms`, `peak_rss_bytes`.

- [ ] **Step 1: Add deterministic end-to-end benchmark fixtures**

Create 1,024 in-memory 4 KiB documents from the existing deterministic generator rules and benchmark a fresh store per iteration with `iter_batched` and `BatchSize::LargeInput`.

- [ ] **Step 2: Verify the new benchmarks execute**

Run:

```bash
cargo +1.96.1 bench --locked -p seagrep-index --bench hot_paths -- index_build_1024x4096 --noplot
```

Expected: both benchmark IDs produce positive medians and the existing benchmark comparator reports them as new, not missing.

- [ ] **Step 3: Record the unchanged control**

Run the 25,000-object xbench three times under `/usr/bin/time -l`; retain median wall time and maximum RSS in the benchmark report.

- [ ] **Step 4: Commit benchmark controls**

```bash
git add crates/index/benches/hot_paths.rs crates/xbench/src/main.rs .github/workflows/bench.yml
git commit -m "bench: cover full index construction"
```

## Task 2: Packed-Gram Sorting

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/core/Cargo.toml`
- Modify: `crates/core/src/grams.rs`

**Interfaces:**
- Produces: `sort_packed_grams(grams: &mut Vec<u32>) -> ()`.

- [ ] **Step 1: Add an equivalence property test**

For lengths `0, 1, 2, 3, 31, 255, 256, 4096, 600000`, generate deterministic bytes, compute the existing comparison-sorted result and the new function result, and assert equality.

- [ ] **Step 2: Verify the test fails before implementation**

```bash
cargo +1.96.1 test --locked -p seagrep-core grams::tests::packed_sort_matches_control
```

Expected: compilation fails because `sort_packed_grams` is absent.

- [ ] **Step 3: Implement the minimal hybrid sort**

Add radsort at the workspace and core manifests. Benchmark crossover candidates `128`, `256`, `512`, and `1024`; retain one private constant and remove benchmark-only branches.

- [ ] **Step 4: Verify behavior and speed**

```bash
cargo +1.96.1 test --locked -p seagrep-core grams::tests
cargo +1.96.1 bench --locked -p seagrep-index --bench hot_paths -- grams_index_trigram --noplot
```

Expected: all tests pass and trigram extraction does not regress by more than 10 percent.

- [ ] **Step 5: Commit packed sorting**

```bash
git add Cargo.toml Cargo.lock crates/core/Cargo.toml crates/core/src/grams.rs
git commit -m "perf: radix-sort packed trigrams"
```

## Task 3: Posting-Run Algorithm Selection

**Files:**
- Create: `crates/index/src/build/mod.rs`
- Create: `crates/index/src/build/runs.rs`
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/index/benches/hot_paths.rs`

**Interfaces:**
- Produces: `write_trigram_run_radix`, `write_trigram_run_merge`, and one selected `write_trigram_run` with the same input/output contract.

- [ ] **Step 1: Write byte-equivalence tests**

Cover ordered, reverse-completion, duplicate, empty-gram, repeated-text, random-text, and maximum `u32` document IDs. Read both temporary files fully and assert equality with the current comparison-sort output.

- [ ] **Step 2: Run the tests and confirm missing implementations**

```bash
cargo +1.96.1 test --locked -p seagrep-index build::runs::tests
```

- [ ] **Step 3: Implement radix and k-way candidates**

Use explicit integer conversion before writing. Flush and seek every temporary file. Add context `writing trigram posting run` to I/O errors.

- [ ] **Step 4: Benchmark both candidates**

Benchmark low-cardinality repeated logs, the current 18-character synthetic alphabet, and high-cardinality random bytes at 1,024 documents by 4 KiB.

- [ ] **Step 5: Keep one implementation**

Retain the implementation with the lowest median unless it uses more than 1.5 times the peak allocation of the other; in that case retain the lower-memory implementation when wall time is within 10 percent. Delete the losing implementation and its benchmark-only selector.

- [ ] **Step 6: Verify full index equivalence**

```bash
cargo +1.96.1 nextest run --locked -p seagrep-index --all-features
```

Expected: every index, segmented lifecycle, and differential test passes.

- [ ] **Step 7: Commit run construction**

```bash
git add crates/index/src/build crates/index/src/lib.rs crates/index/benches/hot_paths.rs
git commit -m "perf: remove comparison-sorted posting runs"
```

## Task 4: Parallel BLAKE3 Local Freshness

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/core/Cargo.toml`
- Create: `crates/index/src/local.rs`
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/cli/tests/cli.rs`

**Interfaces:**
- Produces: `hash_file` and unchanged public `LocalCorpus` API.

- [ ] **Step 1: Add digest and deterministic-order tests**

Assert a known BLAKE3 vector, listing order under varied file sizes, and detection of a same-size/same-mtime rewrite.

- [ ] **Step 2: Move local ownership into `local.rs`**

Move `LocalCorpus`, `LocalFetcher`, key conversion, walking, and hashing without behavior changes; re-export public types from `lib.rs`.

- [ ] **Step 3: Implement parallel BLAKE3 listing**

Use `par_iter` over the already sorted path vector. Preserve indexed collection order and exact path context on errors.

- [ ] **Step 4: Verify tests and benchmark listing phase**

```bash
cargo +1.96.1 nextest run --locked -p seagrep-index -p seagrep
```

Expected: same-size/same-mtime rewrite remains detected and listing output is deterministic.

- [ ] **Step 5: Commit local hashing**

```bash
git add Cargo.toml Cargo.lock crates/core/Cargo.toml crates/index/src/local.rs crates/index/src/lib.rs crates/cli/tests/cli.rs
git commit -m "perf: parallelize local freshness hashing"
```

## Task 5: File-Backed Index Output

**Files:**
- Create: `crates/index/src/build/output.rs`
- Modify: `crates/index/src/build/mod.rs`
- Modify: `crates/index/src/build/runs.rs`
- Modify: `crates/index/src/lib.rs`
- Modify: `crates/core/src/store.rs`
- Modify: `crates/index/src/segment.rs`
- Modify: `crates/s3/src/client.rs`
- Modify: `crates/s3/src/lib.rs`
- Modify: `crates/index/tests/segmented.rs`

**Interfaces:**
- Produces: `TempBlob`, `BuiltIndexFiles`, `build_index_files`, and `BlobStore::put_file` exactly as specified above.

- [ ] **Step 1: Add failing local file-write tests**

Verify a multi-megabyte source file round-trips through `LocalBlobStore::put_file`, an existing destination is replaced atomically, and a missing source returns an error without changing the destination.

- [ ] **Step 2: Implement local `put_file`**

Copy through a destination-parent `NamedTempFile`, call `sync_all`, and persist. Do not read the source into a `Vec`.

- [ ] **Step 3: Add file-backed output equivalence tests**

For trigram and sparse strategies, compare FST bytes, postings bytes, failed IDs, and search results between the old in-memory control and `build_index_files`.

- [ ] **Step 4: Write FST/postings directly to files**

Track the posting offset as `u64`; encode one posting block into a reusable bounded scratch vector, write it, then clear it. Finish and sync each writer before constructing `TempBlob`.

- [ ] **Step 5: Wire fresh segment writes**

Stream-hash `terms.fst`, `postings.bin`, and serialized `docs.bin` in canonical order. Call `put_file` for immutable large blobs and retain `put_if` only for the small root pointer.

- [ ] **Step 6: Implement S3 file upload**

Use one bounded buffer for single PUTs. For multipart files, each async task opens the file, seeks to its part offset, reads exactly one part, uploads it through the existing retry engine, and releases the buffer before taking another part. Abort on any part failure.

- [ ] **Step 7: Verify local, MinIO, and failure paths**

```bash
cargo +1.96.1 nextest run --locked --workspace --all-features --profile ci
```

Then run the existing pinned MinIO benchmark and a forced multipart index blob.

- [ ] **Step 8: Commit file-backed output**

```bash
git add crates/core/src/store.rs crates/index/src/build crates/index/src/lib.rs crates/index/src/segment.rs crates/index/tests/segmented.rs crates/s3/src/client.rs crates/s3/src/lib.rs
git commit -m "perf: stream index blobs through storage"
```

## Task 6: Performance Acceptance

**Files:**
- Modify: `.github/workflows/bench.yml`
- Modify: `crates/xbench/README.md`
- Modify: `README.md`

- [ ] **Step 1: Run repeated local measurements**

Run five fresh 25,000-object builds. Compare medians and maximum RSS against 7.83 seconds and 500,629,504 bytes.

- [ ] **Step 2: Run 100,000-object and 1 GiB decoded cases**

Require exact scenario hit counts, no process failure, and bounded output-memory behavior.

- [ ] **Step 3: Profile the optimized binary**

Build release with debug symbols, sample five seconds, and confirm comparison sorting is no longer a top stack. Investigate any new project-owned function above 15 percent of active samples.

- [ ] **Step 4: Set CI gates from measured evidence**

Record the accepted relative wall-time threshold and absolute 300 MiB RSS gate. Do not encode this workstation's absolute time as a shared-runner requirement.

- [ ] **Step 5: Run release verification**

```bash
cargo +1.96.1 fmt --all --check
cargo +1.96.1 nextest run --locked --workspace --all-features --profile ci
cargo +1.96.1 test --locked --release --workspace --all-features
cargo +1.96.1 clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo +1.96.1 doc --locked --no-deps --document-private-items --workspace
cargo +1.88 check --locked --workspace --all-targets
```

- [ ] **Step 6: Commit benchmark gates and results**

```bash
git add .github/workflows/bench.yml crates/xbench/README.md README.md
git commit -m "bench: gate index throughput and memory"
```
