# Architecture

## Bird's Eye View

holys3 is a grep-style CLI for local files and private S3 buckets. It builds a compact gram index, uses that index only to reduce the candidate set, and runs the final regex over source bytes. The correctness model is deliberately simple: indexed search must return the same document set as scanning every document.

The main boundary is IO. `holys3-core`, `holys3-query`, `holys3-index`, and `holys3-sigv4` are mostly pure format and planning code. `holys3-s3` owns AWS network calls. `holys3` wires those pieces into a user-facing CLI.

## Entry Points

### Index pipeline

`holys3 index s3://bucket[/prefix]` lists the prefix, filters out the exact index namespace, diffs (key, etag) pairs against the union of existing segment doc tables, builds one or more bounded content-addressed segments over the changes (tombstoning superseded docs), optionally merges small adjacent segments, and atomically swaps `.holys3/segments.bin`. Large segment blobs upload as concurrent multipart parts.

`holys3 index <DIR>` is the same pipeline over a local blob store rooted at `--out`: it walks the canonicalized directory, computes BLAKE3 content tokens, and runs the identical incremental diff — there is exactly one index format.

### Search pipeline

`holys3 <PATTERN> <DIR> --index ...` opens the segmented index in the local index directory, plans a gram query from the regex (prefix, suffix, AND inner literals), reads candidate ids, fetches local files, and renders rg-style verified results.

`holys3 <PATTERN> s3://bucket[/prefix]` opens the in-bucket segmented index through the S3 blob store, caches immutable segment blobs locally, reads posting blocks with coalesced ranged GETs, groups logical candidates by physical source, and fetches each source with an ETag-bound conditional GET. Sources of at least 64 MiB are reconstructed from four concurrent 16 MiB conditional ranges. An optional explicit source-object cache sits before decoding.

## Code Map

`crates/core` defines physical `SourceObject` identity, logical `DocAddress` identity, bounded recursive decoding, gram extraction, the `Corpus`, `DocFetcher`, and `BlobStore` IO traits, local blob storage, the line-oriented match engine, and the format-aware scan oracle. Architectural Invariant: core must not perform network IO or know about S3.

`crates/query` turns a regex pattern into a gram query using regex-syntax literal extraction. It chooses candidate constraints, not matches. Architectural Invariant: query must not read corpus bytes, fetch indexes, or decide final answers.

`crates/index` builds and reads the FST term dictionary, postings blocks, format-8 source/document tables, segment lists, local corpus, and the store-backed segmented index reader. One physical archive may emit many logical posting IDs. The reader joins candidates back to typed source/member addresses and delegates final verification to canonical decoded bytes. Architectural Invariant: index must not treat candidates as answers.

Index construction emits bounded sorted posting runs to temporary files and k-way merges them into the final FST/postings pair while computing each blob's SHA-256 digest in the same write. Corpus cardinality does not require one global in-memory postings map. Trigrams use packed `u32` values or a fixed bitmap; sparse grams deduplicate before entering bounded external-sort runs. Raw sources extract grams in parallel, while formats that can expand are decoded and flushed one source at a time. The segment cap applies to logical documents: an oversized multi-source shard is bisected at source boundaries and rebuilt, while one physical source that alone exceeds the cap fails explicitly.

`crates/sigv4` implements AWS SigV4 canonicalization, signing, and credential loading from env or credentials files. The signer is pure and vector-tested. Architectural Invariant: sigv4 must not perform HTTP requests.

`crates/s3` is the AWS S3 boundary: list, conditional/full/ranged GET, PUT and multipart upload, XML parsing, S3 blob storage, index key layout, grouped source fetching, adaptive request limits, and the private opt-in object cache. The cache validates BLAKE3-framed entries on every read, uses lock-free healthy reads, serializes mutations across processes, recovers interrupted accounting at open, and performs bounded concurrent probes. Architectural Invariant: s3 must be the only crate that performs S3 network IO.

`crates/cli` owns argument parsing, env reads, rg-style output rendering (stdout sinks, JSON wire format), and composition of local or S3 pipelines (the async runtime lives inside `S3Client`). Architectural Invariant: cli must not contain index format logic or signing logic.

## Cross-Cutting Concerns

### Correctness and the differential test

The contract is `index == scan`: indexed search must return the same logical documents as recursively decoding and scanning every physical source. `differential_store` covers the format matrix and both gram strategies; `segmented` covers source/member lifecycle, 10,000-member archives, compaction, stale readers, and garbage collection.

### SigV4 vector conformance

SigV4 changes are gated by deterministic AWS signature-vector tests. Signing should stay concrete because it is one pure algorithm with no second implementation.

### Error handling

Fallible boundaries return `anyhow::Result`. Format checks use explicit validation before trusting stored metadata. Environment variables are read at the CLI or credential boundary and fail loudly when required values are missing.

### The index lives in the bucket

For S3, index data is written under `.holys3/` or `<prefix>/.holys3/` in the same bucket namespace as the searched objects. The search path reads `.holys3/segments.bin` (the root pointer), opens each live segment, then uses coalesced ranged GETs against postings data to find candidates.

### Reader consistency

The root swap is atomic and concurrent writers use compare-and-swap. Segment blobs are immutable and format 8 records the length and SHA-256 digest of each FST, postings, and document-table blob. Garbage collection runs after the root swap; readers detect a missing old segment as an `IndexChanged` error, and the CLI reopens the new root once before emitting any result.
