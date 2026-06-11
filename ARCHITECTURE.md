# Architecture

## Bird's Eye View

holys3 is a grep-style CLI for local files and private S3 buckets. It builds a compact gram index, uses that index only to reduce the candidate set, and runs the final regex over source bytes. The correctness model is deliberately simple: indexed search must return the same document set as scanning every document.

The main boundary is IO. `holys3-core`, `holys3-query`, `holys3-index`, and `holys3-sigv4` are mostly pure format and planning code. `holys3-s3` owns AWS network calls. `holys3` wires those pieces into a user-facing CLI.

## Entry Points

### Index pipeline

`holys3 index <DIR>` walks a local directory, assigns document ids, extracts grams for the selected strategy, writes `terms.fst`, `postings.bin`, and `manifest.bin` to `--out`, and exits.

`holys3 index s3://bucket[/prefix]` lists the prefix, filters out `.holys3/`, diffs (key, etag) pairs against the union of existing segment doc tables, builds ONE new content-addressed segment over the changes (tombstoning superseded docs), optionally merges small adjacent segments, and atomically swaps `.holys3/segments.bin`. Large segment blobs upload as concurrent multipart parts.

### Search pipeline

`holys3 <PATTERN> <DIR> --index ...` opens the local index, plans a gram query from the regex (prefix, suffix, AND inner literals), reads candidate ids, fetches local files, and renders rg-style verified results.

`holys3 <PATTERN> s3://bucket[/prefix]` opens the in-bucket segmented index through the S3 blob store, caches immutable segment blobs locally, reads posting blocks with coalesced ranged GETs, fetches candidate objects concurrently, and renders only regex-verified results (rg-compatible output, JSON wire format, context lines, globs).

## Code Map

`crates/core` defines `DocId`, `Strategy`, gram extraction, the `Corpus` and `BlobStore` IO traits, local blob storage for tests, the line-oriented `grep_doc` match engine, and the scan oracle. Architectural Invariant: core must not perform network IO or know about S3.

`crates/query` turns a regex pattern into a gram query using regex-syntax literal extraction. It chooses candidate constraints, not matches. Architectural Invariant: query must not read corpus bytes, fetch indexes, or decide final answers.

`crates/index` builds and reads the FST term dictionary, postings file, manifests, local corpus, and store-backed index reader. It produces candidate document ids and delegates final verification to regex over source bytes. Architectural Invariant: index must not treat candidates as answers.

`crates/sigv4` implements AWS SigV4 canonicalization, signing, and credential loading from env or credentials files. The signer is pure and vector-tested. Architectural Invariant: sigv4 must not perform HTTP requests.

`crates/s3` is the AWS S3 boundary: list, GET, ranged GET, PUT, XML parsing, S3 blob storage, index key layout, and S3 corpus fetching. Architectural Invariant: s3 must be the only crate that performs S3 network IO.

`crates/cli` owns argument parsing, env reads, stdout and stderr output, Tokio runtime entry, and composition of local or S3 pipelines. Architectural Invariant: cli must not contain index format logic or signing logic.

## Cross-Cutting Concerns

### Correctness and the differential test

The contract is `index == scan`: indexed search must return the same documents as scanning every source byte. `differential` covers the local reader and `differential_store` covers the blob-store reader. Trigram is the default strategy because it fit the S3 dictionary bake-off; sparse remains available behind `--strategy sparse`.

### SigV4 vector conformance

SigV4 changes are gated by deterministic AWS signature-vector tests. Signing should stay concrete because it is one pure algorithm with no second implementation.

### Error handling

Fallible boundaries return `anyhow::Result`. Format checks use explicit validation before trusting stored metadata. Environment variables are read at the CLI or credential boundary and fail loudly when required values are missing.

### The index lives in the bucket

For S3, index data is written under `.holys3/` or `<prefix>/.holys3/` in the same bucket namespace as the searched objects. The search path reads `.holys3/segments.bin` (the root pointer), opens each live segment, then uses coalesced ranged GETs against postings data to find candidates.

### Planned trait seams (not yet implemented)

`TermDict`: FST today, sorted-table later.

`PostingsCodec`: raw little-endian doc-id blocks today, another codec later.

`Extractor`: direct UTF-8 or bytes today, archive and codec extraction later.
