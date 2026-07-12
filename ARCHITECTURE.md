# Architecture

## Bird's Eye View

holys3 is a grep-style CLI for local files and private S3 buckets. It builds a compact gram index, uses that index only to reduce the candidate set, and runs the final regex over source bytes. The correctness model is deliberately simple: indexed search must return the same document set as scanning every document.

The main boundary is IO. `holys3-core`, `holys3-query`, and `holys3-index` are mostly pure format and planning code. `holys3-s3` owns AWS network calls. `holys3` wires those pieces into a user-facing CLI.

## Entry Points

### Index pipeline

`holys3 index s3://bucket[/prefix]` lists the prefix, filters out any co-located index namespace, diffs (key, etag) pairs against the union of existing segment doc tables, builds one or more bounded content-addressed segments over the changes (tombstoning superseded docs), optionally merges small adjacent segments, and atomically swaps the index root pointer. The index defaults to `<prefix>/.holys3/`; `--index s3://other-bucket/path` selects an independent bucket and namespace. Large segment blobs upload as concurrent multipart parts.

`holys3 index <DIR>` is the same pipeline over the selected local or S3 index blob store: it walks the canonicalized directory, computes BLAKE3 content tokens, and runs the identical incremental diff — there is exactly one index format.

`--watch --interval SECONDS` opens the target once, retains one refreshing S3 client when applicable, and serializes fresh listing/diff/CAS cycles through the same pipeline. The interval begins after an attempt completes. Continuous mode adds no daemon database, event-consumer path, or second index transaction.

### Search pipeline

`holys3 <PATTERN> <DIR> --index ...` opens the segmented index from its local or S3 location, plans a gram query from the regex (prefix, suffix, AND inner literals), reads candidate ids, fetches local files, and renders rg-style verified results.

`holys3 <PATTERN> s3://bucket[/prefix]` opens the default in-bucket or explicit independent segmented index through its blob store, caches immutable segment blobs locally, reads posting blocks with coalesced ranged GETs, groups logical candidates by physical source, and fetches each source with an ETag-bound conditional GET. Sources of at least 64 MiB are reconstructed from four concurrent 8 MiB conditional ranges whose response chunks spool directly to private temporary files. An optional explicit source-object cache sits before decoding.

## Code Map

`crates/core` defines physical `SourceObject` identity, logical `DocAddress` identity, bounded recursive decoding, gram extraction, the `Corpus`, `DocFetcher`, and `BlobStore` IO traits, local blob storage, the line-oriented match engine, and the format-aware scan oracle. Architectural Invariant: core must not perform network IO or know about S3.

`crates/query` turns a regex pattern into a gram query using regex-syntax literal extraction. It chooses candidate constraints, not matches. Architectural Invariant: query must not read corpus bytes, fetch indexes, or decide final answers.

`crates/index` builds and reads the FST term dictionary, postings blocks, format-10 source/document tables, source-bound segment lists, local corpus, and the store-backed segmented index reader. One physical archive may emit many logical posting IDs. The reader joins candidates back to typed source/member addresses and delegates final verification to canonical decoded bytes. Architectural Invariant: index must not treat candidates as answers.

Index construction emits bounded sorted posting runs to temporary files and k-way merges them into the final FST/postings pair while computing each blob's SHA-256 digest in the same write. Trigram dictionaries above one million temporary posting records use 256 independently streamed first-byte FST shards in one immutable term blob, bounding builder state for dense three-byte keyspaces; smaller trigram dictionaries and sparse dictionaries remain one FST. Completed runs retain closed temporary paths; merge fan-in opens at most 64 runs. Corpus cardinality does not require one global in-memory postings map. Oversized local and S3 source bodies and decoded output above 8 MiB remain file-backed. Trigram and sparse extraction read those files in bounded 1 MiB chunks; high-cardinality trigram inputs retain a fixed 2 MiB bitmap and stream it directly to a posting run, while sparse grams use exact short-gram bitmaps and bounded external-sort runs. Raw sources extract grams in parallel, while formats that can expand are decoded and flushed one source at a time. The segment cap applies to logical documents: an oversized multi-source shard is bisected at source boundaries and rebuilt, while one physical source that alone exceeds the cap fails explicitly.

Immutable term dictionaries download into the content-addressed cache through bounded ranged reads, receive a streaming SHA-256 check, and remain memory-mapped for lookup. Owner-only verification markers bind the validated hash to file length and modification time, so warm opens avoid rehashing the dictionary while externally modified entries are revalidated and repaired. Search validates only posting ranges selected by the query, so opening an index is independent of total dictionary size.

`crates/s3` is the AWS S3 boundary: official SDK configuration and credential providers, list, conditional/full/ranged GET, PUT and multipart upload, S3 blob storage, index key layout, grouped source fetching, adaptive request limits, and the private opt-in object cache. Large ranged responses and cache entries use sealed file-backed bodies. The cache validates BLAKE3-framed entries on every read, uses lock-free healthy reads, serializes mutations across processes, recovers interrupted accounting at open, and performs bounded concurrent probes. Architectural Invariant: s3 must be the only crate that performs S3 network IO.

`crates/cli` owns argument parsing, env reads, rg-style output rendering (stdout sinks, JSON wire format), and composition of local or S3 pipelines (the async runtime lives inside `S3Client`). Architectural Invariant: cli must not contain index format or AWS transport logic.

## Cross-Cutting Concerns

### Correctness and the differential test

The contract is `index == scan`: indexed search must return the same logical documents as recursively decoding and scanning every physical source. `differential_store` covers the format matrix and both gram strategies; `segmented` covers source/member lifecycle, 10,000-member archives, compaction, stale readers, and garbage collection.

### AWS transport

The official AWS SDK owns credential discovery, refresh, endpoint rules, and request signing. holys3 owns operation scheduling, adaptive concurrency, retry jitter, hedging, range coalescing, and bounded body storage above that transport.

### Error handling

Fallible boundaries return `anyhow::Result`. Format checks use explicit validation before trusting stored metadata. Environment variables are read at the CLI or credential boundary and fail loudly when required values are missing.

Continuous indexing fails its first cycle so invalid targets, credentials, and index state cannot become a silently unhealthy process. After one successful cycle, later errors are emitted and retried after the configured interval. `--rebuild` is passed only to cycle 1. A stop signal interrupts the wait or lets an active cycle reach the atomic root-swap boundary before clean exit.

### Index storage

S3 sources default to index data under `.holys3/` or `<prefix>/.holys3/` in the source bucket. `--index` may instead name any local path or prefixed S3 location; `--index-region` and `--index-endpoint` independently configure that S3 client. The search path reads the selected root pointer, verifies its source identity, opens each live segment, then uses coalesced ranged GETs against postings data to find candidates. Searches may narrow the recorded source prefix but cannot broaden it or change its endpoint or bucket. Co-located explicit namespaces are excluded from source listings only when endpoint and bucket both match, and namespaces that contain the source prefix are rejected.

### Reader consistency

The root swap is atomic and concurrent writers use compare-and-swap. Format 10 binds the root to its source identity and records the length and SHA-256 digest of each immutable FST, postings, and document-table blob. Garbage collection runs after the root swap; readers detect a missing old segment as an `IndexChanged` error, and the CLI reopens the new root once before emitting any result.
