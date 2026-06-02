# holys3 Stage 3a (On-disk FST term dict) Implementation Plan

> Implement task-by-task. `- [ ]` steps. Stage 2 (sparse n-grams, in-memory `BTreeMap` index) is committed and green. This stage replaces the in-memory index with an **on-disk FST term dictionary keyed on gram BYTES + a postings file**, queried via mmap. Stage 3b adds S3.

**Goal:** Build the index as on-disk files — `terms.fst` (an `fst::Map` from gram bytes → byte-offset into `postings.bin`) + `postings.bin` + `manifest.bin` — and query it by mmap. Keying the FST on **gram bytes** (not hashes) gives real prefix/suffix compression over the millions of overlapping variable-length grams and removes hash-collision false positives. Then **measure the real `terms.fst` size** (the number that validates Option B is affordable).

**Architecture:** `holys3-core` gains byte-returning sparse-gram functions (the existing hash functions delegate to them, so Stage 2 tests stay valid). `holys3-index` gains `build_to_dir()` (writes the three files) and `IndexReader::open()` (mmaps them). `Query` carries gram bytes. The Stage 1/2 differential test (`index == scan`) is rewritten to go through the on-disk FST path and remains the correctness gate.

**Tech Stack:** add `fst` and `memmap2` to `holys3-index`.

> Prior art: the `mmap-cache` crate (fst::Map<bytes→u64 offset> into a mmap'd values file) is exactly this pattern. `fst::MapBuilder` requires keys inserted in lexicographic order; a `BTreeMap<Vec<u8>, _>` gives that ordering for free.

---

## Task 1: Byte-returning sparse grams in `holys3-core`

**Files:** Modify `crates/core/src/lib.rs`

- [ ] **Step 1: Add byte-returning primitives and make the hash fns delegate**

Add, and refactor the existing `extract_sparse_ngrams_all`/`_covering` to call these:

```rust
/// build_all as raw gram byte strings (sorted, deduped). Index-time.
pub fn sparse_grams_all_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() < 2 {
        return out;
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let n = weights.len();
    for i in 0..n {
        out.push(data[i..i + 2].to_vec());
        let mut interior_max: u32 = 0;
        for j in (i + 1)..n {
            if j > i + 1 {
                interior_max = interior_max.max(weights[j - 1]);
            }
            if interior_max >= weights[i] {
                break;
            }
            if weights[j] > interior_max {
                let end = j + 2;
                if end <= data.len() {
                    out.push(data[i..end].to_vec());
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// build_covering as raw gram byte strings (sorted, deduped). Query-time.
pub fn sparse_grams_covering_bytes(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    if data.len() < 2 {
        return out;
    }
    let weights: Vec<u32> = data.windows(2).map(|w| pair_weight(w[0], w[1])).collect();
    let mut stack: Vec<usize> = Vec::new();
    for i in 0..weights.len() {
        while let Some(&top) = stack.last() {
            if weights[top] <= weights[i] {
                let end = i + 2;
                if end <= data.len() {
                    out.push(data[top..end].to_vec());
                }
                if weights[top] == weights[i] {
                    stack.pop();
                    break;
                }
                stack.pop();
            } else {
                break;
            }
        }
        stack.push(i);
    }
    while stack.len() > 1 {
        let top = stack.pop().unwrap();
        if let Some(&prev) = stack.last() {
            let end = top + 2;
            if end <= data.len() {
                out.push(data[prev..end].to_vec());
            }
        }
    }
    if let Some(&pos) = stack.last() {
        let end = pos + 2;
        if end <= data.len() {
            out.push(data[pos..end].to_vec());
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}
```

Refactor the existing hash functions to delegate (keeps Stage 2 tests passing):

```rust
pub fn extract_sparse_ngrams_all(data: &[u8]) -> Vec<(u64, usize)> {
    sparse_grams_all_bytes(data).iter().map(|g| (hash_ngram(g), g.len())).collect()
}
pub fn extract_sparse_ngrams_covering(data: &[u8]) -> Vec<(u64, usize)> {
    sparse_grams_covering_bytes(data).iter().map(|g| (hash_ngram(g), g.len())).collect()
}
```

- [ ] **Step 2: Add a byte-level subset-invariant test**

```rust
#[test]
fn covering_bytes_subset_of_all_bytes() {
    use std::collections::HashSet;
    let pattern = b"MODIFIED_CONSTANT";
    let content = b"fn main() {\n let x = MODIFIED_CONSTANT;\n}\n";
    let all: HashSet<Vec<u8>> = sparse_grams_all_bytes(content).into_iter().collect();
    let cov: HashSet<Vec<u8>> = sparse_grams_covering_bytes(pattern).into_iter().collect();
    assert!(cov.is_subset(&all), "covering bytes must be subset of all bytes");
}
```

- [ ] **Step 3: Run + commit**

`cargo test -p holys3-core` (Stage 2 tests + this new one pass). `cargo fmt --all`.

```bash
git add crates/core && git commit -m "feat(core): byte-returning sparse grams (FST keys)"
```

---

## Task 2: `Query::Gram(Vec<u8>)` in `holys3-query`

**Files:** Modify `crates/query/src/lib.rs`

- [ ] **Step 1: Switch the gram payload to bytes**

```rust
use holys3_core::sparse_grams_covering_bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    All,
    None,
    And(Vec<Query>),
    Or(Vec<Query>),
    Gram(Vec<u8>),
}

fn lit_query(lit: &[u8]) -> Query {
    let grams = sparse_grams_covering_bytes(lit);
    if grams.is_empty() {
        Query::All
    } else {
        Query::And(grams.into_iter().map(Query::Gram).collect())
    }
}

pub fn plan(pattern: &str) -> anyhow::Result<Query> {
    let hir = regex_syntax::parse(pattern)?;
    let seq = regex_syntax::hir::literal::Extractor::new().extract(&hir);
    match seq.literals() {
        None => Ok(Query::All),
        Some([]) => Ok(Query::All),
        Some(lits) => {
            let branches: Vec<Query> = lits.iter().map(|l| lit_query(l.as_bytes())).collect();
            if branches.contains(&Query::All) { Ok(Query::All) } else { Ok(Query::Or(branches)) }
        }
    }
}
```

Remove `matches_grams` (the in-memory `HashSet<u64>` evaluator) — it is superseded by the reader's `candidates`. If any query unit test used it, drop that test; keep `wildcard_is_all`, `single_char_literal_is_all`, and `literal_yields_gram_conjunction` (they only call `plan`).

- [ ] **Step 2: Run + commit**

`cargo test -p holys3-query`.

```bash
git add crates/query && git commit -m "feat(query): Query::Gram carries gram bytes (FST keys)"
```

---

## Task 3: On-disk FST index + reader in `holys3-index`

**Files:** Modify `crates/index/Cargo.toml`, `crates/index/src/lib.rs`

- [ ] **Step 1: Deps**

`cargo add -p holys3-index fst memmap2`. Keep `holys3-core`, `holys3-query`, `anyhow`, `serde`, `postcard`.

- [ ] **Step 2: Replace the in-memory `Index` with an on-disk builder + reader**

Replace the body of `crates/index/src/lib.rs` with:

```rust
use anyhow::Result;
use holys3_core::{sparse_grams_all_bytes, Corpus, DocId};
use holys3_query::Query;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct Manifest {
    docs: Vec<(DocId, String)>,
}

/// Write terms.fst + postings.bin + manifest.bin into `dir`.
pub fn build_to_dir(corpus: &dyn Corpus, dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    // gram bytes -> sorted doc ids
    let mut postings: BTreeMap<Vec<u8>, Vec<DocId>> = BTreeMap::new();
    for &(id, _) in corpus.docs() {
        let bytes = corpus.fetch(id)?;
        for gram in sparse_grams_all_bytes(&bytes) {
            postings.entry(gram).or_default().push(id);
        }
    }
    // postings.bin: per gram block = [u32 count][count x u32 docid] (LE). FST value = offset.
    let mut postings_buf: Vec<u8> = Vec::new();
    let mut builder = fst::MapBuilder::new(Vec::new())?;
    for (gram, ids) in &postings {
        let mut ids = ids.clone();
        ids.sort_unstable();
        ids.dedup();
        let offset = postings_buf.len() as u64;
        postings_buf.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for id in &ids {
            postings_buf.extend_from_slice(&id.to_le_bytes());
        }
        builder.insert(gram, offset)?; // BTreeMap iteration is lexicographic => valid FST order
    }
    let fst_bytes = builder.into_inner()?;
    std::fs::write(dir.join("terms.fst"), &fst_bytes)?;
    std::fs::write(dir.join("postings.bin"), &postings_buf)?;
    let manifest = Manifest { docs: corpus.docs().to_vec() };
    std::fs::write(dir.join("manifest.bin"), postcard::to_allocvec(&manifest)?)?;
    Ok(())
}

pub struct IndexReader {
    map: fst::Map<memmap2::Mmap>,
    postings: memmap2::Mmap,
    docs: Vec<(DocId, String)>,
}

impl IndexReader {
    pub fn open(dir: &Path) -> Result<IndexReader> {
        let fst_file = std::fs::File::open(dir.join("terms.fst"))?;
        let map = fst::Map::new(unsafe { memmap2::Mmap::map(&fst_file)? })?;
        let post_file = std::fs::File::open(dir.join("postings.bin"))?;
        let postings = unsafe { memmap2::Mmap::map(&post_file)? };
        let manifest: Manifest = postcard::from_bytes(&std::fs::read(dir.join("manifest.bin"))?)?;
        Ok(IndexReader { map, postings, docs: manifest.docs })
    }

    pub fn docs(&self) -> &[(DocId, String)] { &self.docs }

    fn all_docs(&self) -> BTreeSet<DocId> { self.docs.iter().map(|&(id, _)| id).collect() }

    fn read_block(&self, offset: u64) -> BTreeSet<DocId> {
        let o = offset as usize;
        let count = u32::from_le_bytes(self.postings[o..o + 4].try_into().unwrap()) as usize;
        let mut set = BTreeSet::new();
        let base = o + 4;
        for k in 0..count {
            let p = base + k * 4;
            set.insert(u32::from_le_bytes(self.postings[p..p + 4].try_into().unwrap()));
        }
        set
    }

    pub fn candidates(&self, q: &Query) -> BTreeSet<DocId> {
        match q {
            Query::All => self.all_docs(),
            Query::None => BTreeSet::new(),
            Query::Gram(g) => match self.map.get(g) {
                Some(off) => self.read_block(off),
                None => BTreeSet::new(),
            },
            Query::And(subs) => {
                let mut it = subs.iter().map(|s| self.candidates(s));
                match it.next() {
                    None => self.all_docs(),
                    Some(first) => it.fold(first, |a, s| a.intersection(&s).copied().collect()),
                }
            }
            Query::Or(subs) => subs.iter().flat_map(|s| self.candidates(s)).collect(),
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            distinct_grams: self.map.len(),
            terms_fst_bytes: self.map.as_fst().as_bytes().len(),
            postings_bytes: self.postings.len(),
        }
    }
}

#[derive(Debug)]
pub struct Stats {
    pub distinct_grams: usize,
    pub terms_fst_bytes: usize,
    pub postings_bytes: usize,
}

/// Full indexed search via the on-disk reader: plan -> candidates -> verify.
pub fn search_matching_docs(reader: &IndexReader, corpus: &dyn Corpus, pattern: &str) -> Result<BTreeSet<DocId>> {
    let q = holys3_query::plan(pattern)?;
    let re = regex::bytes::Regex::new(pattern)?;
    let mut hits = BTreeSet::new();
    for id in reader.candidates(&q) {
        if re.is_match(&corpus.fetch(id)?) {
            hits.insert(id);
        }
    }
    Ok(hits)
}
```

Add `regex` to `crates/index/Cargo.toml` deps (used by `search_matching_docs`). Keep `LocalCorpus` from Stage 1 (move it here unchanged if it lived in this file; it must still exist for the CLI and differential test).

> Note: `Mmap::map` is `unsafe` (UB if the file is mutated concurrently). For our immutable build dirs that is sound. Keep the `unsafe` blocks minimal and commented as the plan shows.

- [ ] **Step 3: Rewrite the index unit tests for the on-disk path**

Replace the Stage 2 `Index` unit tests with on-disk equivalents (build to a temp dir, open, query). Example:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct MemCorpus(Vec<(DocId, String)>, Vec<Vec<u8>>);
    impl Corpus for MemCorpus {
        fn docs(&self) -> &[(DocId, String)] { &self.0 }
        fn fetch(&self, id: DocId) -> Result<Vec<u8>> { Ok(self.1[id as usize].clone()) }
    }

    fn build_tmp(c: &MemCorpus) -> (tempfile::TempDir, IndexReader) {
        let dir = tempfile::tempdir().unwrap();
        build_to_dir(c, dir.path()).unwrap();
        let r = IndexReader::open(dir.path()).unwrap();
        (dir, r)
    }

    #[test]
    fn candidate_superset_then_verify() {
        let c = MemCorpus(
            vec![(0, "x".into()), (1, "y".into())],
            vec![b"world".to_vec(), b"word".to_vec()],
        );
        let (_d, r) = build_tmp(&c);
        let cands = r.candidates(&holys3_query::plan("world").unwrap());
        assert!(cands.contains(&0));
        assert!(cands.is_subset(&BTreeSet::from([0, 1])));
    }

    #[test]
    fn all_returns_every_doc() {
        let c = MemCorpus(vec![(0, "x".into())], vec![b"abcdef".to_vec()]);
        let (_d, r) = build_tmp(&c);
        assert_eq!(r.candidates(&Query::All), BTreeSet::from([0]));
    }
}
```

Add `tempfile` as a dev-dependency: `cargo add -p holys3-index --dev tempfile`.

- [ ] **Step 4: Update the differential test to the on-disk path (THE GATE)**

In `crates/index/tests/differential.rs`, replace the in-memory build with:

```rust
use holys3_index::{build_to_dir, search_matching_docs, IndexReader};
// ... same MemCorpus + corpus() ...
#[test]
fn index_equals_scan_for_many_patterns() {
    let c = corpus();
    let dir = tempfile::tempdir().unwrap();
    build_to_dir(&c, dir.path()).unwrap();
    let reader = IndexReader::open(dir.path()).unwrap();
    let patterns = [ /* same list as Stage 1 */ ];
    for p in patterns {
        let indexed = search_matching_docs(&reader, &c, p).unwrap();
        let re = regex::bytes::Regex::new(p).unwrap();
        let oracle = holys3_core::scan_matching_docs(&c, &re).unwrap();
        assert_eq!(indexed, oracle, "pattern `{p}`: index != scan");
    }
}
```

Add `tempfile` dev-dep already covers the test crate. Keep the exact Stage 1 pattern list.

- [ ] **Step 5: Run the gate**

`cargo test -p holys3-index` and `cargo test -p holys3-index --test differential -- --nocapture`. Every pattern must pass through the FST path. If a pattern fails, the FST lookup of a covering gram is missing from build_all's set — confirm `build_to_dir` uses `sparse_grams_all_bytes` and `plan` uses `sparse_grams_covering_bytes` (same byte grams, subset invariant). Do not weaken the test.

- [ ] **Step 6: Commit**

```bash
git add crates/index && git commit -m "feat(index): on-disk FST term dict (gram bytes) + mmap reader; differential green"
```

---

## Task 4: Wire the CLI to the on-disk index

**Files:** Modify `crates/cli/src/main.rs`

- [ ] **Step 1: Use a directory, not a single .idx file**

Change `index` to call `build_to_dir(&corpus, &out_dir)`, `search` to `IndexReader::open(&index_dir)` + `reader.candidates` + verify + print (reuse `matches_in`), and `stats` to print `distinct_grams`, `terms_fst_bytes`, `postings_bytes`. Replace the `--out FILE`/`--index FILE` args with `--out DIR`/`--index DIR` (default `holys3.idxdir`). Keep `build_local`/`search_local` shapes; only the index handle type changes.

- [ ] **Step 2: Smoke test**

```bash
cargo run -p holys3 -- index  --local-dir crates --out /tmp/h.idxdir
cargo run -p holys3 -- search "fn build" --local-dir crates --index /tmp/h.idxdir
cargo run -p holys3 -- stats  --index /tmp/h.idxdir
```

`search` prints path:line:col:text; `stats` prints the three numbers incl. `terms_fst_bytes`.

- [ ] **Step 3: Commit**

```bash
git add crates/cli && git commit -m "feat(cli): use on-disk FST index dir"
```

---

## Task 5: Measure the real FST size (the payoff number)

**Files:** Modify `docs/superpowers/notes/2026-06-01-termdict-measurement.md`

- [ ] **Step 1: Index the SAME corpus, capture FST size**

```bash
cargo run --release -p holys3 -- index --local-dir <same cargo-registry corpus> --out /tmp/fst.idxdir
cargo run --release -p holys3 -- stats --index /tmp/fst.idxdir
ls -l /tmp/fst.idxdir
```

- [ ] **Step 2: Append a Stage 3a section**

Record `distinct_grams`, **`terms_fst_bytes`** (the compressed FST term dict), `postings_bytes`, and the on-disk file sizes. Compare `terms_fst_bytes` against the Stage 2 flat estimate (`distinct_grams * 16` = 76 MiB) — report the FST compression ratio. Re-extrapolate the FST term-dict to a 10 GiB bucket. State whether the compressed FST makes the in-S3 dict comfortably affordable (it should be far smaller than the flat estimate).

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/notes && git commit -m "docs: Stage 3a FST term-dict size measurement"
```

---

## Task 6: Workspace green

- [ ] **Step 1:** `cargo test` ; `cargo clippy --all-targets -- -D warnings` ; `cargo fmt --all -- --check`. All pass; fix inline.
- [ ] **Step 2:** `git add -A && git commit -m "chore: stage 3a workspace green" || echo "nothing to commit"`

---

## Self-Review (against the spec)

- **Spec §15.3 (partial — local FST half):** on-disk FST term dict keyed on gram bytes (Tasks 1–3) ✓; mmap reader ✓; differential test through the FST path ✓ (correctness gate preserved); real FST-size measurement (Task 5) ✓. **Deferred to Stage 3b:** S3 upload/download, immutable `builds/<id>/` + `CURRENT` pointer, ranged-GET of postings, local hot-footer cache, statelessness test.
- **Why bytes not hashes:** FST compresses shared substrings of the 5M overlapping grams (no compression on random hashes), and exact byte keys eliminate hash-collision false positives. This is the change that makes Option B affordable; Task 5 measures by how much.
- **Type consistency:** `Query::Gram(Vec<u8>)` (query) ↔ `IndexReader::candidates` (index); `build_to_dir`/`IndexReader::open`/`search_matching_docs` replace the Stage 2 in-memory `Index`; CLI updated to dirs.
- **Unsafe:** two `Mmap::map` calls, sound because build dirs are immutable; commented.
