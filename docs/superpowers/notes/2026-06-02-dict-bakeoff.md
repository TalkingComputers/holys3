# holys3 Dict Bake-off: Trigram vs Sparse

## Corpus

| Field | Value |
|---|---:|
| Path | `/Users/parsabahraminejad/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/tracing-0.1.44` |
| Bytes | 9,818,210 |
| Files | 94 |

Normal pure-Rust crate, not a `*-sys` crate, and not a tiny facade.

## Index size

| Strategy | distinct_grams | terms_fst_bytes | postings_bytes | manifest_bytes | total_index_bytes | index/corpus | terms.fst at 10 GiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| Trigram | 16,549 | 93,078 | 459,428 | 11,276 | 563,782 | 5.74% | 101,792,222 bytes (97.08 MiB) |
| Sparse | 423,112 | 20,405,736 | 4,816,284 | 11,276 | 25,233,296 | 257.01% | 22,316,177,992 bytes (20.78 GiB) |

`ls -l`:

| Strategy | manifest.bin | postings.bin | terms.fst |
|---|---:|---:|---:|
| Trigram | 11,276 | 459,428 | 93,078 |
| Sparse | 11,276 | 4,816,284 | 20,405,736 |

## Query selectivity

| Query | Trigram candidates/total | Sparse candidates/total |
|---|---:|---:|
| `fn` | 94/94 | 87/94 |
| `struct` | 66/94 | 66/94 |
| `pub fn` | 7/94 | 7/94 |
| `impl` | 66/94 | 65/94 |
| `TODO` | 2/94 | 2/94 |
| `async fn` | 5/94 | 5/94 |
| `Result<` | 4/94 | 4/94 |
| `\w+\(` | 94/94 | 94/94 |

## Recommendation

Use Trigram for the S3-resident term dict.

Sparse gives smaller candidate sets and fewer lookups, but the dict is much bigger: 20.4 MB on a 9.8 MB corpus, extrapolating to 20.78 GiB of `terms.fst` for a 10 GiB bucket. That is too large for a cacheable footer-style S3 dict.

Trigram gives a small cacheable dict: 93 KB on this corpus, extrapolating to 97.08 MiB for a 10 GiB bucket. The tradeoff is more lookups and larger candidate sets on short patterns (`fn` is 94/94 vs sparse 87/94), but longer literals are effectively identical in this bake-off (`pub fn`, `async fn`, `Result<`, and `TODO`). For Stage 3b, the cacheability and S3-read profile dominate; use Trigram and accept less selectivity for very short patterns.
