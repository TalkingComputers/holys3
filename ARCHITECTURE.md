# Architecture

## Bird's Eye View

seagrep is a grep-style CLI for private S3 buckets. It builds a compact gram index and immutable compressed content snapshot, uses the grams only to reduce the candidate set, and runs the final regex over snapshot bytes. The correctness model is deliberately simple: indexed search must return the same document set as scanning the canonical bytes captured by that index generation.

The main boundary is IO. `seagrep-core`, `seagrep-query`, and `seagrep-index` are mostly pure format and planning code. `seagrep-s3` owns AWS network calls. `seagrep` wires those pieces into a user-facing CLI.

## Entry Points

### Index pipeline

`seagrep index s3://bucket[/prefix]` lists the prefix, filters out any co-located index namespace, diffs (key, etag) pairs against the union of existing segment doc tables, writes immutable tombstones and bounded content-addressed segments over the changes, optionally repacks or merges segments, and atomically swaps the index root pointer. The index defaults to `<prefix>/.seagrep/`; `--index s3://other-bucket/path` selects an independent bucket and namespace. Large blobs upload as concurrent multipart parts.

`--watch --interval SECONDS` opens the target once, retains one refreshing S3 client, and serializes fresh listing/diff/CAS cycles through the same pipeline. The interval begins after an attempt completes. Continuous mode adds no daemon database, event-consumer path, or second index transaction.

### Search pipeline

The CLI opens the selected segmented index, verifies its S3 source binding, plans a gram query from the regex, reads candidate IDs from posting ranges, fetches only the required compressed pack ranges, verifies each block hash, decompresses canonical bytes, and renders rg-style verified results. Search does not read mutable source objects.

## Code Map

`crates/core` defines physical `SourceObject` identity, logical `DocAddress` identity, bounded recursive decoding, gram extraction, the `Corpus`, `DocFetcher`, and `BlobStore` IO traits, benchmark/test-only local blob storage, the line-oriented match engine, and the format-aware scan oracle. Architectural Invariant: core must not perform network IO or know about S3.

`crates/query` turns a regex pattern into a gram query using regex-syntax literal extraction. It chooses candidate constraints, not matches. Architectural Invariant: query must not read corpus bytes, fetch indexes, or decide final answers.

`crates/index` builds and reads the FST term dictionary, postings blocks, format-11 source/document/block tables, source-bound segment lists, content packs, and the store-backed segmented index reader. Its local corpus adapter supports tests and the unpublished benchmark harness; the product CLI does not expose it. One physical archive may emit many logical posting IDs. The reader joins candidates to snapshot coordinates and supplies canonical decoded bytes for final verification. Architectural Invariant: index must not treat candidates as answers.

Index construction emits bounded sorted posting runs to temporary files and k-way merges them into the final FST/postings pair while computing each blob's SHA-256 digest in the same write. The same canonical decoder output streams into 128 KiB independently checksummed zstd frames grouped into content-addressed packs capped near 256 MiB. Document tables store the first block, intra-block offset, and decoded length; block tables bind pack ID, compressed range, decoded length, and SHA-256. Trigram dictionaries above one million temporary posting records use 256 independently streamed first-byte FST shards in one immutable term blob, bounding builder state for dense three-byte keyspaces; smaller trigram dictionaries and sparse dictionaries remain one FST. Completed runs and packs retain closed temporary paths; merge fan-in opens at most 64 runs. Corpus cardinality does not require one global in-memory postings map. Oversized source bodies and decoded output above 8 MiB remain file-backed. File-backed TAR and ZIP inputs decode from streaming or seekable handles. Trigram and sparse extraction read files in bounded 1 MiB chunks; high-cardinality trigram inputs retain a fixed 2 MiB bitmap and stream it directly to a posting run, while sparse grams use exact short-gram bitmaps and bounded external-sort runs. Raw sources extract grams in parallel. Formats that can expand are decoded one source at a time: canonical member bytes go to one content spool, documents retain only spool coordinates, and posting runs materialize grams under a fixed memory budget after member-key sorting. The segment cap applies to logical documents: an oversized multi-source shard is bisected at source boundaries and rebuilt, while one physical source that alone exceeds the cap fails explicitly.

Search groups adjacent selected frames into ranges capped at 8 MiB and processes candidate windows capped at 64 MiB decoded or 1,024 documents. All ranges for one pack in a bounded window are submitted together so the blob store can fetch them concurrently. Single-block documents are zero-copy `Bytes` slices. A document larger than the window cap streams into a temp-backed spool. Candidate IDs are filtered against each segment's immutable dead set before any pack fetch. Dead documents disappear from search immediately. A segment is physically repacked when dead documents or decoded bytes reach 25%, when compaction selects it, or when `--purge-deleted` is supplied; fully dead segments drop without pack reads. This keeps ordinary updates proportional to the delta while bounding stale snapshot bytes below one third of live bytes per segment.

Immutable term dictionaries download into the content-addressed cache through bounded ranged reads, receive a streaming SHA-256 check, and remain memory-mapped for lookup. Owner-only verification markers bind the validated hash to file length and modification time, so warm opens avoid rehashing the dictionary while externally modified entries are revalidated and repaired. Search validates only posting ranges selected by the query, so opening an index is independent of total dictionary size.

`crates/s3` is the AWS S3 boundary: official SDK configuration and credential providers, list, conditional/full/ranged GET, PUT and multipart upload, S3 blob storage, index key layout, build-side source fetching, adaptive request limits, and retry scheduling. Large source responses use sealed file-backed bodies. Architectural Invariant: s3 must be the only crate that performs S3 network IO.

`crates/cli` owns argument parsing, env reads, rg-style output rendering (stdout sinks, JSON wire format), and composition of S3 source and index storage pipelines (the async runtime lives inside `S3Client`). Architectural Invariant: cli must not contain index format or AWS transport logic.

`crates/xbench` owns deterministic corpus generation, local engine and S3 transport benchmarks, report rendering, and the incremental churn benchmark. It is unpublished and is not part of the product CLI.

## Cross-Cutting Concerns

### Correctness and the differential test

The contract is `index == captured scan`: indexed search must return the same logical documents as scanning every canonical document captured by the root generation. `differential_store` covers the format matrix and both gram strategies; `segmented` covers source/member lifecycle, source removal after indexing, 10,000-member archives, compaction, stale readers, pack rewrites, and garbage collection.

### AWS transport

The official AWS SDK owns credential discovery, refresh, endpoint rules, and request signing. seagrep owns operation scheduling, adaptive concurrency, retry jitter, hedging, range coalescing, and bounded body storage above that transport.

### Error handling

Fallible boundaries return `anyhow::Result`. Format checks use explicit validation before trusting stored metadata. Environment variables are read at the CLI or credential boundary and fail loudly when required values are missing.

Continuous indexing fails its first cycle so invalid targets, credentials, and index state cannot become a silently unhealthy process. After one successful cycle, later errors are emitted and retried after the configured interval. `--rebuild` is passed only to cycle 1. A stop signal interrupts the wait or lets an active cycle reach the atomic root-swap boundary before clean exit.

### Index storage

S3 sources default to index data under `.seagrep/` or `<prefix>/.seagrep/` in the source bucket. `--index` may instead name any prefixed S3 location; `--index-region` and `--index-endpoint` independently configure that S3 client. The search path reads the selected root pointer, verifies its source identity, opens each live segment, then uses coalesced ranged GETs against postings and content packs. Searches may narrow the recorded source prefix but cannot broaden it or change its endpoint or bucket. Co-located explicit namespaces are excluded from source listings only when endpoint and bucket both match, and namespaces that contain the source prefix are rejected. Because packs contain canonical decoded content, the index storage boundary has the same confidentiality requirements as the source. Large index blobs stream to their final keys as multipart uploads during the build; failure paths abort the upload, but a hard crash (release builds abort on panic and skip destructors) can strand an incomplete multipart that accrues storage invisibly. Configure an `AbortIncompleteMultipartUpload` lifecycle rule (one day is plenty) scoped to the index namespace — the `.seagrep/` prefix for co-located indexes, or the `--index` prefix — so unrelated multipart uploads in a shared bucket are never aborted.

### Reader consistency

The root swap is atomic and concurrent writers use compare-and-swap. Format 11 binds the root to its source identity and records the length and SHA-256 digest of each immutable FST, postings, document table, and content pack. Garbage collection runs after the root swap; readers detect a missing old segment or pack as an `IndexChanged` error. The CLI reopens once if this happens before candidate processing; after result batches begin, it errors and may have emitted partial valid output.
