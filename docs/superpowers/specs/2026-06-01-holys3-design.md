# holys3 — Design Spec

**Date:** 2026-06-01
**Status:** Design approved, pre-implementation-plan
**One line:** `ripgrep` over a private S3 bucket, accelerated by a sparse-n-gram index stored in S3 with a small mmap'd footer (the Quickwit storage trick). Single Rust binary, hand-rolled SigV4, indexes objects in place across text/compressed/Parquet formats.

---

## 1. Problem & positioning

Existing grep-over-S3 tools (`s3grep`, `s5cmd`, `barnybug/s3`) **stream and re-scan every object on every query** — no persistent index. AWS S3 Select is closed to new customers (2024-07-25) and does SQL-on-CSV/JSON, not regex. Athena/Object-Lambda full-scan per query with no inverted index.

holys3 builds a persistent **sparse-n-gram inverted index** so a query touches the index, gets a small candidate set, and **ranged-GETs only those byte ranges** from S3 — then verifies with the real regex. It **strictly dominates** a streaming grep: worst case (a pattern with no literal) it does exactly what streaming grep does; every other case it is 1–2 orders faster.

### Not Quickwit

Quickwit/tantivy is a **tokenized** index (term-level `RegexQuery`, cannot match arbitrary substrings/regex over raw bytes), ingests data into its own **split** format, and runs as a distributed service with a metastore. holys3 is an **n-gram** index (arbitrary regex, ripgrep semantics), indexes objects **already in the bucket in their native format**, and is a **single CLI binary** with no services. The only thing borrowed from Quickwit is the **footer/hotcache storage pattern**. Closest true sibling: `querymt/qndx` (sparse-n-gram regex index for local files) — holys3 is qndx with an S3 corpus, SigV4 auth, and format extraction.

---

## 2. Cost regime (the honest claim)

The index wins in the **repeated-query** regime. Building it is one full streaming pass over every object (≥ the cost of one whole-bucket grep). So:

- **Build is amortized** across many later queries against a stable bucket.
- **Warm selective queries** are 1–2 orders faster than any re-scanning tool.
- **The S3 binding constraint is sequential GET round-trips (TTFB ~20–80 ms) and GET request cost ($0.0004/1000), not bandwidth.** A cold query is a critical chain (~4 sequential reads ≈ 150 ms, Quickwit-measured). Design minimizes _sequential_ round-trips and fans out _parallel_ ones.

---

## 3. The single search pipeline (no fallback)

Confirmed canonical across Google codesearch, Zoekt, pg_trgm, regrams, qndx, Cursor: regex → trigram/n-gram query is a **total function** into a query algebra `{QAll, QNone, QAnd, QOr, gram}`. When a regex has no usable literal (`.*`, `\d{3}`, bare char-class), the result is **`QAll` (match everything)** — a normal value, not an error and not a separate code path. Every candidate is **re-verified by running the real regex on real bytes** ⇒ the index yields false positives but **never false negatives**.

```
regex
  → parse (regex-syntax HIR)
  → extract required literals (Seq), OR across alternations, AND within
  → build_covering sparse n-grams per literal   (QAll if none)
  → resolve n-gram → posting byte-ranges via footer (1 GET, cached)
  → ranged-GET + intersect/union postings → candidate segment ids
  → resolve segment ids → FetchRecipes
  → fetch candidate bytes (fan-out, coalesced): byte-ranges for seekable
    codecs, WHOLE-OBJECT for non-seekable (gzip/vanilla zstd)
  → decode per codec → run real regex (verify) → emit locator:text
```

`QAll` simply makes the candidate set = all segments → a full scan, same pipeline. This is why there is **one command**, not a `search`/`grep` split.

**Locator format:** line-oriented codecs (raw text, gzip, zstd) emit `object_key:line:col:text` (ripgrep-style). Structured codecs (Parquet/ORC) have no line:col — emit `object_key#rg<row_group>/col<col>/row<row>:text`. The locator is codec-determined, carried by the Segment.

**Fetch-granularity honesty:** the byte-range win applies to seekable codecs (raw text, zstd-seekable frame, Parquet page/column-chunk). For non-seekable codecs (plain gzip, vanilla zstd) a candidate fetch is the **whole object** — same as a streaming tool _for that object_. The holys3 win there is **skipping non-candidate objects entirely**, not sub-object ranges.

**CLI**

- `holys3 index [--bucket B --prefix P]` — build/update the index, upload to S3.
- `holys3 "<regex>" [--bucket B --prefix P] [--stats] [--files-only]` — search.
- `holys3 plan "<regex>"` — show the query plan (n-grams, lookups, estimated candidates/cost) without searching.

---

## 4. Architecture (crates)

```
holys3-core      shared types, hashing (rapidhash portable v3), file-format headers, errors
holys3-sigv4     hand-rolled SigV4 signer + credential chain (env → profile → IMDSv2)
holys3-s3        HTTP client (reqwest/hyper+rustls): list, ranged GET, PUT, retry/concurrency
holys3-extract   format → decoded text + segment map (raw, gzip, zstd, parquet, strings)
holys3-index     sparse-n-gram builder, footer reader (mmap), postings codec, S3 layout
holys3-query     regex decomposition, query planner, candidate resolution, verification
holys3-cli       index / search / plan entrypoints, cost guardrail, output formatting
```

---

## 5. Index storage (in S3 + hot footer)

### Term-dictionary storage — OPEN, decided empirically in Stage 1

The footer cannot hold the **full** sparse-n-gram lookup table at scale: ~20 B/entry × tens of millions of distinct sparse grams for a multi-GB bucket ⇒ hundreds of MB–GB, not the ~10 MB a "tiny footer, one GET" implies. The full-table-resident and small-stateless-footer goals conflict. **Stage 1 builds the index on a representative bucket and measures distinct-gram count and term-dict bytes**, then picks:

- **Option A — full dict cached locally:** source-of-truth index in S3; pull the term dict once, cache + mmap locally; zero per-query gram hops (fastest repeated CLI use); cold machine pays a one-time download.
- **Option B — FST blueprint footer, dict in S3:** term dict is an **FST** (tantivy/Lucene-style, `fst` crate, keyed on real gram bytes so it compresses); footer stays ~MBs and fully stateless (Lambda/CI cold); costs **+1 sequential ranged-GET per query** to fetch FST blocks.

Everything below is common to both; only term-dict residency differs.

### Layout

Index lives under a known prefix in the **same bucket**, in **immutable per-build directories** with an atomically-overwritten pointer (readers never see a half-written reindex — §10):

```
s3://<bucket>/<prefix>/.holys3/builds/<build_id>/{footer.bin, postings.dat, segments.bin, manifest.bin, termdict.*}
s3://<bucket>/<prefix>/.holys3/CURRENT          # tiny object: the current build_id (last write wins, atomic)
```

| File           | Contents                                                                                                                                                                                                                                                               | Access pattern                                                               |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `footer.bin`   | Section-offset map + term-dict location/blueprint + segment-table location + per-shard pruning metadata + magic/version. In **Option A** it points at the locally-cached full dict; in **Option B** it carries the small FST blueprint. Size is what Stage 1 measures. | **One GET per session, mmap'd + cached locally.**                            |
| `termdict.*`   | Gram → postings-byte-range map. **Option A:** sorted `(ngram_hash u64, postings_offset u64, len u32)`, downloaded once + mmap'd. **Option B:** `fst::Map` of gram-bytes → offset, fetched by range.                                                                    | A: whole-download, cached. B: ranged-GET FST blocks.                         |
| `postings.dat` | Concatenated posting blocks. 1-byte tag per block: `0x02` varint(LEB128) delta for lists ≤64, `0x03` Roaring for lists >64. Posting entries are `segment_id` (u32/u64).                                                                                                | Ranged-GET only the blocks named by the term dict; coalesce adjacent ranges. |
| `segments.bin` | Segment table: per `segment_id` a `Segment{ object_key, etag, decoded_origin, decoded_len, FetchRecipe }`.                                                                                                                                                             | Ranged-GET the slice for candidate ids (or whole-load if small).             |
| `manifest.bin` | Index metadata: bucket/prefix, build time, format version, per-object ETags, enumeration source (ListObjectsV2 vs S3 Inventory snapshot id), n-gram strategy params, frequency-weight table id. `postcard`.                                                            | One GET; cached.                                                             |

**Local cache:** footer + manifest cached under `~/.cache/holys3/<bucket>/<prefix>/` keyed by build id; mmap'd. Repeat sessions skip the footer GET. `postings.dat`/`segments.bin` ranges may be LRU-cached on disk (optional, Elasticsearch frozen-tier style).

**Statelessness:** any machine with read access to the bucket can grep it — fetch footer, then ranged-GET. No per-machine rebuild.

### On-disk file framing

Every file: 24-byte header `[magic u8;4][version u32][payload_len u64][checksum u64 (rapidhash-v3)]`. Footer parsed for a section-offset map so one fetch yields the blueprint.

---

## 6. Sparse n-grams

Deterministic `pair_weight(b0, b1) -> u16` from a **frequency table over a large code/text corpus** (rare byte-pairs → high weight; beats CRC32). A sparse n-gram is a substring whose **two boundary pair-weights strictly exceed every interior pair-weight**.

- **Index time — `build_all`:** emit every qualifying substring (superset, ≈1.5–2× trigram count). At most `2n-2` grams.
- **Query time — `build_covering`:** monotone-stack minimal cover (fewer, longer, more selective grams). At most `n-2` grams.
- **Subset invariant (correctness crux):** `covering(L) ⊆ all(F)` for any literal `L` inside file `F` ⇒ no false negatives even though index and query use different extraction modes. Guarded by a regression test.
- **Strategy:** planner computes both a trigram plan and a sparse plan, picks lower estimated cost (`cost ≈ Σ 3/gram_len`, optionally weighted by document frequency). Over S3, fewer candidate objects = fewer round-trips, so covering-mode sparse is favored.

Hash, never store the gram (collisions only widen candidates; verify catches them). `rapidhash` portable v3 for on-disk stability.

---

## 7. Regex → query (decomposition)

`regex-syntax` `Parser` → `Hir` → `hir::literal::Extractor` (Prefix/Suffix `Seq`) plus a custom inner-literal walk (reference: `regex-automata::meta::reverse_inner`). Combine:

- concatenation → AND of grams,
- alternation `|` → OR of branch queries,
- char-class / `.*` / `<3`-char literal with no gram → **`QAll`** for that node.
  Boolean-simplify (`a OR (a AND b)` → `a`; `QAll AND x` → `x`). Result is a query over n-gram hashes.

---

## 8. Format extraction → Segment + FetchRecipe

Index offsets address **decoded text**; S3 ranges address **raw bytes**. The map is codec-specific. Unit = **Segment** (smallest independently-decodable unit); each carries a **FetchRecipe**.

```rust
struct Posting { segment_id: u64, intra_segment_offset: u64, len: u32 }

struct Segment {
    id: u64,
    object_key: String,
    etag: String,
    decoded_origin: u64,   // start in the object's global decoded-text space
    decoded_len: u64,
    recipe: FetchRecipe,
}

struct FetchRecipe {
    raw_ranges: Vec<ByteRange>,   // MULTI-RANGE (e.g. Parquet dict page + data page)
    codec: Codec,
    intra_segment_skip: u64,      // decode whole unit, then discard to reach offset
    column: Option<ColumnRef>,    // typed col=value matching for Parquet/ORC
}
struct ByteRange { start: u64, len: u64 }

enum Codec {
    RawText,                                          // identity, no decode
    Gzip,                                             // whole-object, decode from 0
    Zstd,                                             // whole-object, decode from 0
    ZstdSeekable { frame_index: u32 },                // frame range; set_offset within
    Bgzf { virtual_position: u64 },                   // block range; gzi-indexed
    ParquetPage { row_group: u32, col: u32, page: u32, dict_page: Option<ByteRange> },
    ParquetColumnChunk { row_group: u32, col: u32 },  // when no PageIndex
    StringsRun,                                       // identity over a printable run
}
```

**Per-codec fetch granularity (v1 scope: text + gzip/zstd + Parquet; binary via strings):**

| Format                 | Segment                | FetchRecipe                                        | Crate                      |
| ---------------------- | ---------------------- | -------------------------------------------------- | -------------------------- |
| raw text               | span                   | byte-range (identity)                              | —                          |
| gzip                   | whole object           | whole-object, decode from 0 (NOT seekable)         | `flate2`                   |
| zstd (vanilla)         | whole object           | whole-object                                       | `zstd`/`async-compression` |
| zstd-seekable          | frame (~2 MiB)         | frame range + skip                                 | `zeekstd`                  |
| BGZF                   | block (<64 KiB)        | block range + skip                                 | `noodles-bgzf`             |
| Parquet (PageIndex)    | data page (+dict page) | multi-range                                        | `parquet`+`arrow`          |
| Parquet (no PageIndex) | column chunk           | `[dict_offset, data_offset+total_compressed_size)` | `parquet`+`arrow`          |
| binary                 | printable run          | byte-range (identity)                              | `strings`-style scan       |

Routing: `infer` (magic-byte sniff) + `content_inspector` (text/binary via NUL + UTF-8). **Columnar bonus:** carry `column` so Parquet/ORC can do typed `col=value` matching beyond blind regex.

---

## 9. S3 client (hand-rolled, no AWS crates)

**SigV4 signer** (`holys3-sigv4`): canonical request → string-to-sign → daily-cached signing key (`HMAC-SHA256` chain over date/region/`s3`/`aws4_request`) → `AWS4-HMAC-SHA256` Authorization header. `UNSIGNED-PAYLOAD` on GETs (TLS covers integrity); send `x-amz-content-sha256`, `x-amz-date`, `host` (+ `x-amz-security-token` for temp creds, + `range`). S3 quirks: single URI-encoding, **no path normalization** (keep `//`). Unit-tested against AWS published SigV4 test vectors.

**Credential chain:** env (`AWS_ACCESS_KEY_ID`/`SECRET`/`SESSION_TOKEN`) → `~/.aws/credentials`+`config` by `AWS_PROFILE` → **IMDSv2** (PUT token w/ TTL header, then GET role creds). Refresh temp creds ~5 min before `Expiration`. (STS AssumeRole / ECS container creds: post-v1.)

**HTTP** (`holys3-s3`): `reqwest`/`hyper` + `rustls`. S3 data API is HTTP/1.1 (one request per connection) ⇒ concurrency = connections. Connection pool sized to concurrency; keep-alive; periodic DNS re-resolve to spread across S3 IPs. **Fan out thousands of concurrent ranged GETs** (`Arc<Semaphore>` + `futures::stream::buffer_unordered`) to hide TTFB; **AIMD** concurrency (ramp on success, ×0.5 on 503 bursts); **full-jitter** exponential backoff on 503 SlowDown; **tail hedging** (reissue slowest ~1% on a fresh connection). Coalesce adjacent byte ranges (gaps < ~1 MB). In-region + VPC gateway endpoint assumed (egress free). Optional S3 Express One Zone target (single-digit-ms, `CreateSession`, service name `s3express`) — post-v1.

---

## 10. Freshness (incremental reindex)

`holys3 index` re-run: enumerate objects (`ListObjectsV2`; **S3 Inventory** manifest for very large buckets) → diff ETags against the current build's `manifest.bin` → re-extract changed/new objects, drop deleted → write a **new immutable build dir** `builds/<new_build_id>/` → **atomically overwrite `CURRENT`** with the new build_id. Readers resolve `CURRENT` once at session start, so an in-flight reindex never yields a torn read. Old build dirs are GC'd after a grace period (no reader still pointing at them).

---

## 11. Cost guardrail

Before a `QAll`/large scan, estimate and print:
`requests = candidate_segments` , `cost = requests × $0.0004/1000 + bytes × egress_rate` (egress free in-region). Hard-confirm above a threshold (default > $50 or > N requests). Transparency, never a silent block. `--stats` always shows candidate count, lookups, bytes fetched, stage timings.

---

## 12. Correctness invariant & testing

- **Invariant:** the index only ever _narrows_; verification on real bytes is the source of truth ⇒ `index results ⊇ scan results`, equality after verify.
- **Differential test** (the main guarantee): for arbitrary regexes over a fixture corpus, `holys3` (indexed) results == a from-scratch streaming-scan oracle (`holys3-core` scan). Mirrors qndx `tests/differential.rs`.
- SigV4 against AWS test vectors. Per-format extractor golden fixtures. MinIO/localstack end-to-end. Sparse-n-gram subset-invariant regression test.

---

## 13. Recommended crates

`regex-syntax`, `regex-automata`, `grep-searcher`+`grep-regex`, `memchr`; `roaring`, `integer-encoding`, `memmap2`, `rapidhash`, `postcard`, `rkyv`; `parquet`+`arrow`, `flate2`, `zeekstd`, `noodles-bgzf`, `infer`, `content_inspector`; `reqwest`/`hyper`/`rustls`, `ring`/`hmac`+`sha2`+`hex`, `tokio`, `futures`. (`bincode` is unmaintained — avoid.)

---

## 14. Out of scope (v1)

Documents (PDF/docx via Tika), ORC/Avro (format supports it; deferred), STS/ECS creds, SigV4a/MRAP, S3 Express, multi-writer index concurrency (single writer per build; readers safe via immutable builds + `CURRENT`), presigned-URL mode.

---

## 15. Build sequence (staged — YAGNI, thin vertical slice first)

Each stage is end-to-end runnable and verifiable before the next. **Stage 1 doubles as the measurement that resolves the §5 term-dict decision.**

1. **Vertical slice (text-only, trigram, local).** Hand-rolled SigV4 GET + IMDSv2/env/profile creds → list a prefix → build a **trigram** index over **raw-text** objects (local files first, then S3) → regex-decompose → intersect postings → ranged-GET candidates → verify with `grep-regex`/`regex-automata` → `object_key:line:col:text`. Differential test (`index ⊇ scan`, equality after verify) green. **Measure distinct-gram count + term-dict bytes on a representative bucket → decide §5 Option A vs B.**
2. **Sparse n-grams.** Add frequency-weighted `build_all`/`build_covering`, planner (trigram vs sparse), subset-invariant test. Re-measure term-dict size.
3. **In-S3 index + footer.** Immutable `builds/<id>/` + `CURRENT`; footer/term-dict per the A/B decision; local cache; statelessness test from a second machine/profile.
4. **Concurrency + resilience.** Fan-out ranged GETs (Semaphore + `buffer_unordered`), AIMD, full-jitter 503 backoff, tail hedging, range coalescing. Cost guardrail + `--stats`.
5. **Format extraction.** gzip → zstd(+seekable) → Parquet (PageIndex/column-chunk) → binary `strings`. Per-format golden fixtures; extend differential corpus.
6. **Freshness.** Incremental reindex (ETag diff; S3 Inventory for large buckets).
