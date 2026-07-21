# Multi-Pattern Regional Search Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one repeated-`-e` search plan, fetch, verify, and render mixed bounded/unbounded patterns without bound poisoning, repeated index I/O, giant-line materialization in bounded-output mode, or accuracy loss.

**Architecture:** Parse one HIR per user pattern, formally classify each pattern's exact/witness/fallback extent, union all term and posting reads per segment, carry tagged candidate ranges into one batch-scoped pack-block map, and verify with ordered whole/line/regional multi-pattern programs plus job-local caches. Default and JSON output still lazily fetch and re-verify complete lines; `--match-window` is the only clipped output mode.

**Tech Stack:** Rust 1.94.1, `regex-syntax` 0.8.11, `regex-automata` 0.4.16, Rayon 1.12, `bytes`, existing S3/blob-store and pack formats, Clap 4, termcolor 1.4.

## Global Constraints

- Preserve the existing unstaged `skills/seagrep/SKILL.md` cost-model edit. Integrate it only in Task 6; never discard or overwrite it wholesale.
- Do not change the index format or `INDEX_FORMAT`; every new range/proof type is query-runtime state only.
- Do not add benchmark strings, bucket names, credential names, RCAEval names, special-case regexes, native matchers, a test crate, or a fixture framework.
- Do not add source comments. Preserve existing comments unless a changed contract makes one false.
- Do not introduce names containing `resolve`, `ensure`, or `handle`; existing functions with those names remain untouched unless this plan explicitly says otherwise.
- Keep `Query` I/O-free, S3 transport-only, core network-free, and the regex verifier authoritative.
- Treat every proof construction failure as `MatchWitness=None` plus an exact fallback. Only invalid user HIR/program compilation, corrupt index/pack data, stale snapshots, transport failures, and sink failures are fatal.
- Preserve leftmost-first, ordered repeated-pattern semantics by compiling the HIRs in original `-e` order and using `WhichCaptures::Implicit`.
- Choose pattern eligibility before searching: whole documents use all patterns, full-line regions exclude `Document`, and truncated regions include only `Bytes`. Never post-filter a match from a broader program.
- Use `rayon::current_num_threads()` only to size the bounded search channel. `try_for_each_init` caches are job-local; code and tests make no one-cache-per-thread claim.
- Delete the duplicate dead `crates/core/src/grep.rs::parse_pattern`; `crates/core/src/pattern.rs::parse_pattern` remains the single parser and `search_streaming` calls it in Task 4.
- Task 4 must add no Clippy warning. The existing `candidate.rs::single_match` and `pack.rs::{too_many_arguments,single_range_in_vec_init}` warnings are outside this task and must not increase; deleting the duplicate grep parser removes its `dead_code`/`unreachable_pub` warnings.
- Apply `-m` after union/deduplication by matching line. It must not alter `SearchDetail` or exact `--count-matches` span iteration.
- Keep existing default text and JSON bytes unchanged. Clipping is opt-in through `--match-window`.
- Performance gates use the existing pinned app-log inventory and oracle; production code must not contain either hash.

## Shared Runtime Schemas

The following exact types are used across Tasks 1–5. Define each type only in the file named by its task.

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FallbackExtent {
    Lines,
    Document,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SearchExtent {
    Bytes { span: usize },
    Lines,
    Document,
}

#[derive(Clone)]
pub enum MatchWitness {
    Exact {
        bytes: usize,
    },
    Proven {
        bytes: usize,
        direction: ProofDirection,
        machine: std::sync::Arc<regex_automata::dfa::sparse::DFA<Vec<u8>>>,
    },
}

#[derive(Clone)]
pub struct MatchBounds {
    pub exact_bytes: Option<usize>,
    pub witness: Option<MatchWitness>,
    pub fallback: FallbackExtent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PatternMatch {
    pub pattern: usize,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateRange {
    pub blocks: std::ops::Range<u32>,
    pub extent: SearchExtent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionProgram {
    Regional,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchWindow {
    pub line: u64,
    pub line_offset: u64,
    pub window_offset: u64,
    pub text: bytes::Bytes,
    pub matches: Vec<WindowMatch>,
    pub left_clipped: bool,
    pub right_clipped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowMatch {
    pub witness: std::ops::Range<u64>,
    pub visible: std::ops::Range<usize>,
    pub left_clipped: bool,
    pub right_clipped: bool,
    pub canonical_span_known: bool,
}
```

Schema invariants:

- `PatternMatch.pattern` is the original zero-based `-e` index, never a regional-program-local ID.
- `PatternMatch.start..end` is half-open and relative to the searched slice.
- `SearchExtent::Bytes.span` is positive and at most `CANDIDATE_BLOCK_BYTES`.
- `CandidateRange.blocks` is non-empty, half-open, document-local, sorted after merging, and never carries `SearchExtent::Document`.
- `IndexAddress.ranges=None` means whole document. `Some` is non-empty and contains the exact tagged ranges above.
- `WindowMatch.witness` is a complete accepted absolute byte range. `visible` is its intersection with `MatchWindow.text` and is relative to `text`.
- `canonical_span_known=false` is permitted only for `MatchWitness::Proven`; it does not weaken existence, line number, witness range, or window offset.

---

## Task 1: Add the formal pattern engine

**Files:**

- Modify `Cargo.toml`: pin workspace `regex = "1.13.1"`; add `regex-automata = "0.4.16"`.
- Modify `Cargo.lock`: update only the dependency graph implied by those two workspace changes.
- Modify `crates/core/Cargo.toml`: add `regex-automata.workspace = true`; retain `regex.workspace = true` until Task 4 removes the legacy matcher calls.
- Create `crates/core/src/pattern.rs`: pattern parsing, multi-pattern program/cache/iterator, exact-bound analysis, formal finite-witness proof, and focused tests.
- Modify `crates/core/src/lib.rs`: add `mod pattern;` and re-export the public Task 1 types/functions.

### Public type additions in `crates/core/src/pattern.rs`

Add the shared `ProofDirection`, `FallbackExtent`, `SearchExtent`, `MatchWitness`, `MatchBounds`, and `PatternMatch` schemas verbatim. Add these opaque program types:

```rust
pub struct PatternProgram {
    regex: regex_automata::meta::Regex,
    pattern_ids: Box<[usize]>,
}

pub struct PatternCache {
    cache: regex_automata::meta::Cache,
}

pub struct PatternMatches<'p, 'c, 'h> {
    program: &'p PatternProgram,
    cache: &'c mut PatternCache,
    searcher: regex_automata::util::iter::Searcher<'h>,
}
```

`PatternProgram` input schema is an ordered HIR list plus an equally long list of original pattern IDs. Output is one immutable leftmost-first verifier. `PatternCache` is mutable scratch for one sequential verifier or one Rayon job state at a time. Neither exposes the underlying regex/cache.

### Function contracts in `crates/core/src/pattern.rs`

```rust
pub fn parse_pattern(pattern: &str) -> anyhow::Result<regex_syntax::hir::Hir>;
```

- Input: `pattern`, required UTF-8 regex source; byte escapes remain legal.
- Output: one HIR parsed by `ParserBuilder::new().utf8(false)`.
- Errors: return the original `regex_syntax::Error` through `anyhow`; add no pattern index here because the CLI owns that context.
- Transformation: build the parser, disable UTF-8-only matching, parse once, return the HIR without stringifying/reparsing it.

```rust
pub fn analyze_patterns(hirs: &[regex_syntax::hir::Hir]) -> Vec<MatchBounds>;
```

- Input: ordered HIR slice; empty is valid and returns an empty vector.
- Output: exactly one `MatchBounds` per input, same order. `exact_bytes` is a finite canonical maximum only when the HIR has no look assertion and cannot match newline. `witness` is `Exact` for a positive finite maximum, otherwise the shorter valid forward/reverse `Proven`, otherwise `None`. `fallback` is always populated.
- Errors: none. DFA build limits, graph limits, quit states, empty matches, oversized proofs, sparse conversion failures, and retained-memory exhaustion all become `witness=None`.
- Transformation: carry one `retained_bytes` counter across the slice; call `analyze_pattern`; retain sparse proof machines only while the total is at most 32 MiB.

```rust
impl PatternProgram {
    pub fn compile(
        hirs: &[regex_syntax::hir::Hir],
        pattern_ids: &[usize],
    ) -> anyhow::Result<PatternProgram>;

    pub fn create_cache(&self) -> PatternCache;

    pub fn find_iter<'p, 'c, 'h>(
        &'p self,
        cache: &'c mut PatternCache,
        haystack: &'h [u8],
    ) -> PatternMatches<'p, 'c, 'h>;
}
```

- `compile` input: non-empty HIR slice and same-length IDs; IDs may be non-contiguous but must be unique.
- `compile` output: `meta::Regex` built with `Regex::config().which_captures(WhichCaptures::Implicit)` and `build_many_from_hir`, plus boxed ID map.
- `compile` errors:
  - empty HIRs: `pattern program must contain at least one HIR`;
  - unequal lengths: `pattern HIR count {N} differs from pattern ID count {M}`;
  - duplicate ID: `pattern ID {ID} appears more than once`;
  - regex-automata build failures propagate as query-level `anyhow::Error`.
- `create_cache` output: one cache created by `self.regex.create_cache()`; no error.
- `find_iter` input: cache created for this program and borrowed haystack; output owns `Searcher::new(Input::new(haystack))` and borrows both program/cache.

```rust
impl Iterator for PatternMatches<'_, '_, '_> {
    type Item = PatternMatch;

    fn next(&mut self) -> Option<PatternMatch>;
}
```

- Input state: current `Searcher`, program, mutable cache.
- Output: next non-overlapping leftmost-first match; local `Match::pattern()` maps through `pattern_ids`, and `start/end` copy exactly.
- Errors: none; call `Searcher::advance` with `program.regex.search_with(&mut cache.cache, input)` so the configured infallible meta engine owns empty-match advancement.

```rust
impl MatchWitness {
    pub fn find_witness(
        &self,
        haystack: &[u8],
        matched: PatternMatch,
    ) -> anyhow::Result<std::ops::Range<usize>>;
}
```

- Input: searched bytes plus a meta-verifier match relative to those bytes.
- Output: `Exact` returns `matched.start..matched.end`; `Proven` returns the shortest concrete accepted prefix from `matched.start` or suffix ending at `matched.end`, never exceeding `bytes`.
- Errors:
  - invalid match bounds: `verifier match {START}..{END} is outside a {LEN}-byte region`;
  - retained proof does not accept: `finite witness for pattern {ID} did not accept within {BYTES} bytes`.
- Transformation: validate bounds; create the anchored start state (`start_state_forward` or `start_state_reverse`); transition one byte at a time in the proof direction; after each byte, call `next_eoi_state` and accept only `is_match_state`; return the consumed half-open range. Dead/quit states or exhaustion use the second error.

### Private proof schemas and functions

```rust
struct ProofCandidate {
    bytes: usize,
    direction: ProofDirection,
    machine: regex_automata::dfa::sparse::DFA<Vec<u8>>,
    retained_bytes: usize,
}

struct ProofGraph {
    start: usize,
    states: Vec<regex_automata::util::primitives::StateID>,
    accepting: Vec<bool>,
    edges: Vec<Vec<usize>>,
}
```

`ProofGraph.states[index]` is the dense-DFA state represented by `index`; `accepting` and `edges` use the same index space. Edges contain unique representative-byte transitions, never dead/quit states, and accepting nodes have no outgoing edges.

```rust
fn analyze_pattern(
    hir: &regex_syntax::hir::Hir,
    retained_bytes: &mut usize,
) -> MatchBounds;
```

- Input: one HIR and current retained sparse-DFA bytes.
- Output: fallback from `choose_fallback`; positive exact maximum becomes both `exact_bytes=Some` and `MatchWitness::Exact`; otherwise choose the shorter valid proof, charge its `memory_usage`, and retain it only within 32 MiB.
- Errors: none; all checked failures return a fallback-only bound.

```rust
fn find_exact_bytes(hir: &regex_syntax::hir::Hir) -> Option<usize>;
```

- Return `hir.properties().maximum_len()` only when the look set is empty, the HIR cannot match newline, and the maximum is positive; otherwise `None`.

```rust
fn choose_fallback(hir: &regex_syntax::hir::Hir) -> FallbackExtent;
```

- Return `Document` when `hir.properties().look_set().contains_anchor_haystack()` or the HIR can match newline; return `Lines` for line anchors, word boundaries, and newline-free unbounded HIR.

```rust
fn can_match_newline(hir: &regex_syntax::hir::Hir) -> bool;
```

- Traverse HIR nodes. Literal/class membership of `\n` returns true; repetition/capture recurse; concat/alternation use `any`; empty/look return false.

```rust
fn find_proof(
    hir: &regex_syntax::hir::Hir,
    direction: ProofDirection,
) -> Option<ProofCandidate>;
```

- Reject any look, possible empty match, newline match, or finite exact maximum.
- Build one dense DFA, derive a proof graph, compute the longest accepting path, reject zero or `> CANDIDATE_BLOCK_BYTES`, convert the same dense DFA to sparse, record `memory_usage`, and return it. Drop the dense DFA before attempting the other direction.

```rust
fn build_proof_dfa(
    hir: &regex_syntax::hir::Hir,
    direction: ProofDirection,
) -> Option<regex_automata::dfa::dense::DFA<Vec<u32>>>;
```

- Compile a capture-free Thompson NFA from the HIR with `WhichCaptures::None` and `reverse(direction == Reverse)`.
- Determinize with `StartKind::Anchored`, `MatchKind::All`, `dfa_size_limit(Some(8 * 1024 * 1024))`, and `determinize_size_limit(Some(16 * 1024 * 1024))`.
- Return `None` on every build error.

```rust
fn build_proof_graph(
    dfa: &regex_automata::dfa::dense::DFA<Vec<u32>>,
    direction: ProofDirection,
) -> Option<ProofGraph>;
```

- Start with anchored `Input::new(&[])`; use `start_state_forward` for forward proof and `start_state_reverse` for reverse proof.
- BFS reachable states using `dfa.byte_classes().representatives(..)`; deduplicate next states; reject quit states; omit dead states; mark acceptance through `next_eoi_state` and stop expanding accepted states.
- Reverse-reach from accepted nodes, remove nodes that cannot reach acceptance, remap indices, and return `None` when start cannot reach acceptance or graph/state accounting exceeds the same 16 MiB scratch limit.

```rust
fn find_longest_accepting_path(graph: &ProofGraph) -> Option<usize>;
```

- DFS/topologically color only co-accessible non-accepting nodes. A gray-to-gray edge is an unbounded delaying cycle and returns `None`.
- Otherwise memoize `1 + child_distance`; accepting nodes contribute zero; return the exact longest start-to-acceptance byte count.

```rust
fn is_accepting<A: regex_automata::dfa::Automaton>(
    dfa: &A,
    state: regex_automata::util::primitives::StateID,
) -> bool;
```

- Return true only when `next_eoi_state(state)` succeeds and is a match state; dead/quit/error is false.

### Focused tests in `pattern.rs`

- [ ] Add `fn analyze_pattern_table()` covering exact (`foo.{2}`), forward proof (`[A-Z0-9]{20,}` and `foo.*`), reverse proof (`.*token`), unsafe middle gap (`foo.*bar`), unsafe alternation (`foo|bar.*baz`), line fallback (`^foo$`, `\bfoo`), document fallback (`\Afoo`, `foo\z`), empty match, proof larger than one candidate block, and a DFA-limit fallback. Assert exact enum fields and proof direction/bytes.
- [ ] Add `fn program_preserves_pattern_ids_and_empty_match_progress()` compiling two HIRs with IDs `[7, 3]`; assert ordered original IDs/spans and termination across an empty match.
- [ ] Add `fn proof_witness_returns_concrete_absolute_slice()` for forward and reverse machines; assert returned bytes are accepted and lie within the configured bound.
- [ ] Run `cargo test -p seagrep-core pattern` and expect all Task 1 tests to pass.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Commit: `git add Cargo.toml Cargo.lock crates/core/Cargo.toml crates/core/src/lib.rs crates/core/src/pattern.rs && git commit -m "feat: add multi-pattern proof engine"`.

---

## Task 2: Union candidate planning and tagged runtime addresses

**Files:**

- Create `crates/index/src/candidate.rs`: multi-query bind/fetch/evaluate orchestration, block expansion, selection folding, range merging, and batch-size estimation.
- Modify `crates/core/src/store.rs`: replace `IndexAddress.blocks` with tagged `ranges`.
- Modify `crates/core/src/lib.rs`: re-export `CandidateRange` and the revised address type.
- Modify `crates/index/src/eval.rs`: add shared gram collection and multi-query binding; leave existing `resolve`, `blocks_needed`, `eval`, and set algebra unchanged.
- Modify `crates/index/src/remote_terms.rs`: accept an already-unioned gram slice and remove query-tree knowledge.
- Modify `crates/index/src/lib.rs`: register/re-export candidate types, widen `IndexReader::visit_candidates`, and remove `candidates_with` after `segment.rs` migrates.
- Modify `crates/index/src/segment.rs`: preload one term map, fetch one posting union, evaluate/fold one pattern at a time, emit each document once, and honor both batch limits.
- Modify `crates/index/Cargo.toml`: add `regex-syntax.workspace = true` for the public multi-HIR search signature added in Task 4.
- Modify `crates/index/tests/sparse_remote.rs`: one focused multi-plan read-count test.

### Public index schemas

Add in `crates/index/src/candidate.rs` and re-export from `lib.rs`:

```rust
#[derive(Debug, Clone, Copy)]
pub struct CandidatePlan<'a> {
    pub query: &'a seagrep_query::Query,
    pub extent: seagrep_core::SearchExtent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CandidateBatchLimits {
    pub documents: usize,
    pub decoded_bytes: u64,
}
```

- Both limits are required and positive.
- Search passes `documents=16_384` and `decoded_bytes=64 * 1024 * 1024`.
- A single whole document larger than the byte limit is emitted alone; no other batch may exceed either estimate.

Modify in `crates/core/src/store.rs` during this task:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexAddress {
    pub segment: u32,
    pub document: u32,
    pub ranges: Option<Vec<CandidateRange>>,
}
```

Remove `IndexAddress.blocks`. Re-export `CandidateRange` and `IndexAddress` from core `lib.rs`.

### Added functions in `crates/index/src/eval.rs`

```rust
pub(crate) fn collect_query_grams(queries: &[&Query]) -> Vec<Vec<u8>>;
```

- Input: ordered query references; every node is required and may be `Query::All`.
- Output: owned gram bytes, lexicographically sorted and deduplicated; `All` contributes none.
- Errors: none.
- Transformation: iterative stack traversal; clone only `Query::Gram`; push children for `And`/`Or`; sort/dedup once.

```rust
pub(crate) fn bind_queries(
    queries: &[&Query],
    id_space: u32,
    strategy: Strategy,
    lookup: &dyn Fn(&[u8]) -> anyhow::Result<Option<TermValue>>,
) -> anyhow::Result<Vec<Resolved>>;
```

- Output length/order exactly matches input; each item is existing `resolve(query, id_space, strategy, lookup)`.
- Errors: preserve existing corrupt singleton, dictionary, and lookup errors; add no wrapping that hides the pattern position.

### New functions in `crates/index/src/candidate.rs`

```rust
pub(crate) fn validate_candidate_plans(
    plans: &[CandidatePlan<'_>],
    limits: CandidateBatchLimits,
) -> anyhow::Result<()>;
```

- Errors: `candidate plans must not be empty`, `candidate document batch limit must be positive`, `candidate decoded-byte batch limit must be positive`, or `candidate byte span must be positive`.
- Reject `SearchExtent::Bytes { span: 0 }`; other extents are valid.

```rust
pub(crate) fn visit_candidate_selections(
    plans: &[CandidatePlan<'_>],
    id_space: u32,
    strategy: Strategy,
    lookup: &dyn Fn(&[u8]) -> anyhow::Result<Option<crate::eval::TermValue>>,
    expand: &dyn Fn(usize, seagrep_core::DocId) -> std::ops::RangeInclusive<seagrep_core::DocId>,
    fetch_blocks: impl FnOnce(
        &std::collections::BTreeMap<u64, (u32, u64)>,
    ) -> anyhow::Result<std::collections::BTreeMap<u64, Vec<seagrep_core::DocId>>>,
    visit: &mut dyn FnMut(usize, crate::eval::Selection) -> anyhow::Result<()>,
) -> anyhow::Result<()>;
```

- Input schema: validated plans; one segment ID space/strategy; shared pure lookup; plan-indexed expansion; one posting fetch closure; streaming selection visitor.
- Output: unit after visiting exactly one selection per plan, in plan order.
- Errors: propagate binding, posting fetch/decode, eval, or visitor errors.
- Transformation: bind all queries; union every `blocks_needed` into one `BTreeMap`; invoke `fetch_blocks` once (use an empty map without I/O when no blocks); evaluate each bound query with an expansion closure capturing that plan index; immediately visit and drop each `Selection`.

```rust
pub(crate) fn build_block_bases(tables: &crate::format::SegmentTables) -> anyhow::Result<Vec<u32>>;
```

- Move the existing implementation from `segment.rs`; output is the cumulative first candidate-block ID for each document plus terminal total. Preserve its overflow/corruption errors.

```rust
fn get_block_document(id: u32, bases: &[u32]) -> usize;
```

- Map one segment-global candidate block to the containing document via `partition_point`; caller validates `id < terminal`.

```rust
pub(crate) fn expand_candidate_block(
    id: u32,
    bases: &[u32],
    tables: &crate::format::SegmentTables,
    extent: SearchExtent,
) -> std::ops::RangeInclusive<u32>;
```

- `Bytes { span }`: expand by `span - 1` bytes converted to candidate-block slack.
- `Lines`: expand by the document's indexed `max_line_len` slack.
- `Document`: return the complete block interval for the containing document.
- Clamp every result to that document's first/last block.

```rust
fn group_candidate_blocks(
    ids: Vec<u32>,
    bases: &[u32],
    extent: SearchExtent,
) -> anyhow::Result<Vec<(u32, Vec<CandidateRange>)>>;
```

- Input IDs must be sorted, deduplicated, and inside the terminal base; otherwise return `candidate block {ID} is outside 0..{TOTAL}`.
- Convert to document-local contiguous half-open ranges tagged with `extent`; `Document` is rejected with `document extent cannot be stored as a candidate range`.

```rust
fn pick_broader_extent(left: SearchExtent, right: SearchExtent) -> SearchExtent;
```

- Dominance is `Document > Lines > Bytes`; two byte extents retain the larger span.

```rust
fn merge_candidate_ranges(current: &mut Vec<CandidateRange>, incoming: Vec<CandidateRange>);
```

- Sort by block start/end; merge overlaps using `pick_broader_extent`; merge adjacent ranges only when extents are equal; leave a sorted non-empty vector.

```rust
pub(crate) fn add_candidate_selection(
    documents: &mut std::collections::BTreeMap<u32, Option<Vec<CandidateRange>>>,
    selection: crate::eval::Selection,
    extent: SearchExtent,
    strategy: Strategy,
    document_count: u32,
    block_bases: Option<&[u32]>,
) -> anyhow::Result<()>;
```

- `Selection::All`: insert every document as `None`.
- Sparse IDs: validate `< document_count`, insert `None`.
- Trigram + `Document`: map selected blocks to documents, insert `None`.
- Trigram + finite extent: group, attach, and merge ranges.
- Existing `None` always dominates later regional ranges. Invalid IDs and missing trigram bases are fatal corruption errors.

```rust
pub(crate) fn estimate_candidate_bytes(
    decoded_size: u64,
    ranges: Option<&[CandidateRange]>,
) -> anyhow::Result<u64>;
```

- Whole document returns `decoded_size`.
- Regional ranges convert block endpoints to decoded byte endpoints, clamp to `decoded_size`, union overlaps, and sum with checked arithmetic. This is the deterministic decoded-block estimate used only to close reader batches; pack planning performs the exact physical-block union.

### Changed functions in `crates/index/src/remote_terms.rs`

```rust
pub(crate) fn fetch_gram_values(
    store: &dyn BlobStore,
    blob: &str,
    index: &SparseTableIndex,
    grams: &[Vec<u8>],
    cache_dir: &std::path::Path,
    seg_id: &str,
) -> anyhow::Result<rapidhash::RapidHashMap<u64, (u64, Option<u64>)>>;
```

- Replace `fetch_query_gram_values(..., q: &Query, ...)`.
- Hash every supplied gram, sort/dedup hashes, identify unique sparse blocks, read/verify each cache hit once, fetch all misses in one `get_ranges`, decode each block once, and binary-search all hashes.
- Preserve every existing length/hash/cache error verbatim.

```rust
fn collect_gram_hashes(grams: &[Vec<u8>]) -> Vec<u64>;
```

- Replace recursive query traversal; map `hash_ngram`, sort, dedup, return.

### Changed `IndexReader` contract in `crates/index/src/lib.rs`

```rust
fn visit_candidates(
    &self,
    plans: &[CandidatePlan<'_>],
    key_prefix: Option<&str>,
    limits: CandidateBatchLimits,
    visit: &mut dyn FnMut(Vec<seagrep_core::DocAddress>) -> anyhow::Result<bool>,
) -> anyhow::Result<()>;
```

- Default implementation validates plans/limits, clones the queries into one `Query::Or`, calls `candidate_docs` once, forces every indexed `ranges` to `None` for safe degradation, and chunks only by `limits.documents`.
- `candidate_docs(&self, q, key_prefix)` remains source-compatible.
- Visitor false stops successfully; errors propagate.

### Changed functions in `crates/index/src/segment.rs`

```rust
fn read_term_values(
    &self,
    segment: &Segment,
    queries: &[&Query],
) -> anyhow::Result<std::collections::BTreeMap<Vec<u8>, Option<crate::eval::TermValue>>>;
```

- Collect unique grams once. Sparse-remote mode calls `fetch_gram_values` once and maps hashes back to every gram; local modes call `segment.map.get` once per unique gram. Store absent grams as `None` so binding performs no hidden reread.

```rust
fn read_posting_blocks(
    &self,
    segment: &Segment,
    needed: &std::collections::BTreeMap<u64, (u32, u64)>,
    id_space: u32,
) -> anyhow::Result<std::collections::BTreeMap<u64, Vec<seagrep_core::DocId>>>;
```

- Extract the current posting-range path unchanged in behavior: validate logical ranges, union trusted verification blocks, read cache hits, one ranged fetch for misses, hash-check, cache, assemble each logical posting, and decode by strategy.
- Empty `needed` returns an empty map without reading the posting table/blob.

```rust
fn read_candidate_batches(
    &self,
    plans: &[CandidatePlan<'_>],
    key_prefix: Option<&str>,
    limits: CandidateBatchLimits,
    visit: &mut dyn FnMut(Vec<DocAddress>) -> anyhow::Result<bool>,
) -> anyhow::Result<()>;
```

- Validate once, prefix-prune each segment once, load tables/bases once, preload term values once, call `visit_candidate_selections`, and fold each selection immediately through `add_candidate_selection`.
- Filter dead documents once. Construct one `DocAddress` per surviving document with `IndexAddress.ranges`.
- Build batches in display/document order. Before adding a document, compute `estimate_candidate_bytes`; flush when adding it would exceed either limit. Emit a whole document alone when it is at least `LARGE_DOCUMENT_BYTES` or exceeds the decoded-byte limit, preserving the file-backed large-body path and preventing its pack blocks from overlapping another request in the same batch. Preserve early stop and `IndexChanged` classification.

```rust
fn read_candidate_docs(
    &self,
    q: &Query,
    key_prefix: Option<&str>,
) -> anyhow::Result<Vec<DocAddress>>;
```

- Wrap `q` as one `CandidatePlan { query: q, extent: SearchExtent::Lines }`, use limits `{ documents: 16_384, decoded_bytes: 64 MiB }`, collect/sort exactly as today.

Delete moved `build_block_bases`, `block_document`, `single_range`, `expand_block`, `blocks_to_doc_ranges`, and the now-unused `candidates_with` in `lib.rs`.

### Focused verification

- [ ] Move existing block expansion/grouping tests from `segment.rs` to `candidate.rs`; keep assertions but replace `Option<usize>` with explicit `SearchExtent`.
- [ ] Add `fn selections_merge_extents_and_documents_once()` covering overlapping `Bytes` spans, `Lines` dominance, `Document` dominance, disjoint ranges, sparse IDs, and one output entry per document.
- [ ] Change `remote_terms::query_grams_resolve_with_one_ranged_read()` to `fn gram_sets_fetch_each_sparse_block_once()` with two overlapping gram sets and one `get_ranges` assertion.
- [ ] Add `fn repeated_plans_fetch_unique_term_and_posting_ranges_once()` in `crates/index/tests/sparse_remote.rs`; pass two plans with overlapping grams and assert unique display keys, one term ranged read, one postings data ranged read after the table tail, and no duplicate physical range.
- [ ] Continue directly into Task 3 without compiling or committing: replacing `IndexAddress.blocks` and `IndexReader::visit_candidates` is one atomic producer/consumer migration. Task 3 updates the only production consumer plus the `search.rs` test readers, then runs every Task 2 focused command.

---

## Task 3: Fetch every pack block once per candidate batch

**Files:**

- Modify `crates/core/src/store.rs`: tagged regions, scoped candidate-batch trait, and safe whole-document default adapter.
- Modify `crates/core/src/lib.rs`: re-export the new fetch/runtime types.
- Modify `crates/index/src/pack.rs`: replace per-document regional fetches with `PackBatch`, two-phase initial planning, shared decoded block state, and lazy single-flight reads.
- Modify `crates/index/src/segment.rs`: replace independent regional workers with one `SegmentedCandidateBatch` owning one `PackBatch` per segment.
- Modify `crates/index/src/search.rs`: compile-only adapter to use `start_candidate_batch`, one `CandidatePlan`, and the existing verifier; Task 4 replaces its verification logic in the same branch before behavior is released.

### Core store schemas and traits

Modify `DocumentRegion` exactly:

```rust
#[derive(Debug, Clone)]
pub struct DocumentRegion {
    pub start: u64,
    pub line: u64,
    pub line_offset: u64,
    pub bytes: bytes::Bytes,
    pub program: RegionProgram,
}
```

- `start`: absolute decoded offset of `bytes`.
- `line`: one-based line containing `start`.
- `line_offset`: absolute start of that logical line, even when it lies before `bytes`.
- `program`: `Regional` only for safe finite byte extents; `Full` for exact full-line regions.

Keep `FetchedDocument::{Whole, Regions { decoded_size, regions }}`. The only changed implementation is the mechanically expanded regional byte sum:

```rust
impl FetchedDocument {
    pub fn decoded_size(&self) -> u64;

    pub fn fetched_size(&self) -> u64;
}
```

- `decoded_size`: `Whole.body.len()` or the stored `Regions.decoded_size`; no errors.
- `fetched_size`: `Whole.body.len()` or the existing `u64` sum of `region.bytes.len()` across all regions; no I/O and no semantic change.

Add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRead {
    Bytes,
    Lines {
        before_context: usize,
        after_context: usize,
    },
}

pub trait CandidateBatch: Sync {
    fn fetch_initial(
        &self,
        consume: &mut dyn FnMut(usize, FetchedDocument) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;

    fn fetch_regions(
        &self,
        document: usize,
        ranges: &[std::ops::Range<u64>],
        read: RegionRead,
    ) -> anyhow::Result<FetchedDocument>;
}
```

- `document` is the zero-based position in the exact `DocAddress` slice passed to `start_candidate_batch`.
- Ranges are absolute decoded, non-empty, and within the selected document. `Bytes` returns exactly their merged union. `Lines` expands seeds to exact line plus requested context boundaries.
- Errors: `candidate document {INDEX} is outside a batch of {LEN}`, `candidate byte range {START}..{END} is outside a {LEN}-byte document`, plus existing fetch/decode/integrity errors.

Replace the two candidate methods on `DocFetcher` while preserving `fetch_each` exactly:

```rust
struct WholeCandidateBatch<'a, F: DocFetcher + ?Sized> {
    fetcher: &'a F,
    documents: &'a [DocAddress],
}

impl<F: DocFetcher + ?Sized> CandidateBatch for WholeCandidateBatch<'_, F> {
    fn fetch_initial(
        &self,
        consume: &mut dyn FnMut(usize, FetchedDocument) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;

    fn fetch_regions(
        &self,
        document: usize,
        ranges: &[std::ops::Range<u64>],
        read: RegionRead,
    ) -> anyhow::Result<FetchedDocument>;
}

pub trait DocFetcher: Sync {
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, DocumentBody) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;

    fn start_candidate_batch<'a>(
        &'a self,
        documents: &'a [DocAddress],
    ) -> anyhow::Result<Box<dyn CandidateBatch + 'a>> {
        Ok(Box::new(WholeCandidateBatch {
            fetcher: self,
            documents,
        }))
    }
}
```

- `WholeCandidateBatch` inputs are the exact borrowed fetcher and address slice; its only state is those two references.
- `fetch_initial` delegates the complete address slice to `fetch_each`, preserves callback indices/order semantics, wraps each body as `FetchedDocument::Whole`, and propagates fetch/callback errors unchanged.
- `fetch_regions` validates `document < documents.len()` or errors `candidate document {INDEX} is outside a batch of {LEN}`. It intentionally ignores `ranges` and `read`, fetches only `documents[document]` through `fetch_each`, captures the body as `Whole`, and errors `candidate region fetch returned no document` if the callback never fires. This is exact degradation for non-segmented/test fetchers.
- The generic `F: DocFetcher + ?Sized` keeps the default method object-safe for both concrete and `dyn DocFetcher` receivers; do not cast `self` to a sized type.
- Remove `fetch_candidate_each` and `fetch_candidate_lines`.

### Pack request/state schemas in `crates/index/src/pack.rs`

Replace `RegionFetchOptions` and the old `PackRegionRequest` with:

Change `LARGE_DOCUMENT_BYTES` visibility from private to `pub(crate)` so candidate batching and pack spooling use one threshold.

```rust
#[derive(Debug, Clone)]
pub(crate) enum PackRange {
    Regional {
        bytes: std::ops::Range<u64>,
        span: usize,
    },
    FullLines {
        bytes: std::ops::Range<u64>,
        before_context: usize,
        after_context: usize,
    },
}

pub(crate) struct PackRegionRequest<'a> {
    pub index: usize,
    pub slice: PackSlice,
    pub decoded_size: u64,
    pub ranges: &'a [PackRange],
    pub block_newlines: &'a [u32],
}

enum BlockEntry {
    Loading,
    Ready(bytes::Bytes),
}

struct BlockState {
    entries: std::collections::BTreeMap<usize, BlockEntry>,
}

pub(crate) struct PackBatch<'a> {
    store: &'a dyn BlobStore,
    packs: &'a [PackMeta],
    blocks: &'a [PackBlock],
    state: std::sync::Mutex<BlockState>,
    ready: std::sync::Condvar,
}
```

Failures are not cached. The loader removes only its still-`Loading` claims and wakes waiters before returning the original `anyhow::Error`; a waiter may retry in a query that is already failing. This preserves the complete original error/type chain, while the at-most-once invariant remains exact for successful batches.

### `PackBatch` contracts

```rust
impl<'a> PackBatch<'a> {
    pub(crate) fn create(
        store: &'a dyn BlobStore,
        packs: &'a [PackMeta],
        blocks: &'a [PackBlock],
    ) -> PackBatch<'a>;

    pub(crate) fn fetch_documents(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        requests: &[PackRequest],
        consume: &mut dyn FnMut(usize, DocumentBody) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;

    pub(crate) fn fetch_regions(
        &self,
        cache: Option<&PackBlockCache<'_>>,
        requests: &[PackRegionRequest<'_>],
        consume: &mut dyn FnMut(usize, FetchedDocument) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;
}
```

- `create` initializes empty state/condvar; no I/O.
- `fetch_documents`: group small whole documents into one block union and load through `load_blocks`; construct each `DocumentBody` from shared bytes. Preserve existing file-backed `fetch_large` for any single document at least `LARGE_DOCUMENT_BYTES`.
- `fetch_regions`: first compute provisional logical ranges for the complete request slice (`Regional` expands by `span - 1`; `FullLines` already carries the index's maximum-line-length slack), merge with full-line dominance, apply the existing 512-range and 50%-coverage whole-document fallback, union every surviving physical block ID, and call `load_blocks` once. Only after that shared load, scan the already-loaded boundary blocks to trim/extend `FullLines` to exact line/context boundaries; any unexpectedly missing adjacent block uses the same single-flight map. Construct separate `DocumentRegion` values without concatenating gaps and call `consume` once per request. A coverage fallback calls the whole-document path and emits `FetchedDocument::Whole`, never a full-sized region.
- Existing pack/hash/cache/stale errors propagate unchanged.

Retain the existing free compaction entry point with its exact signature:

```rust
pub(crate) fn fetch_documents(
    store: &dyn BlobStore,
    cache: Option<&PackBlockCache<'_>>,
    packs: &[PackMeta],
    blocks: &[PackBlock],
    requests: &[PackRequest],
    consume: &mut dyn FnMut(usize, DocumentBody) -> anyhow::Result<()>,
) -> anyhow::Result<()>;
```

- Input/output/errors remain identical for `segment/compact.rs`.
- Transformation: create one temporary `PackBatch::create(store, packs, blocks)` and delegate the complete request slice to `PackBatch::fetch_documents(cache, requests, consume)`. Do not change `compact.rs`; its outer `request_windows` loop continues to bound compaction memory.

```rust
fn load_blocks(
    &self,
    cache: Option<&PackBlockCache<'_>>,
    block_ids: &std::collections::BTreeSet<usize>,
) -> anyhow::Result<()>;
```

- Lock state; skip `Ready`; claim absent IDs as `Loading`; wait on already-`Loading` IDs.
- For claimed IDs, call existing `block_runs` and `visit_blocks` once. Insert each decoded `Bytes` as `Ready`.
- On any fetch/decompress error, remove every still-claimed `Loading` entry, notify all, and return that exact error unchanged. On success notify all and verify every requested ID is `Ready`.

```rust
fn read_range(
    &self,
    cache: Option<&PackBlockCache<'_>>,
    parts: &[DocumentBlock],
    range: std::ops::Range<u64>,
) -> anyhow::Result<bytes::Bytes>;
```

- Determine intersecting physical block IDs, call `load_blocks` (lazy single-flight), copy only requested document bytes into one output, and preserve `document range is incomplete` on length mismatch.

```rust
fn extend_line_range(
    &self,
    cache: Option<&PackBlockCache<'_>>,
    parts: &[DocumentBlock],
    block_newlines: &[u32],
    decoded_size: u64,
    range: std::ops::Range<u64>,
    before_context: usize,
    after_context: usize,
) -> anyhow::Result<std::ops::Range<u64>>;
```

- Walk outward by document blocks, using newline counts to skip zero-newline blocks and reading only boundary blocks needed to locate exact delimiters. Return `[0, decoded_size]` clamps. Preserve complete lines and requested context.

```rust
fn locate_line(
    &self,
    cache: Option<&PackBlockCache<'_>>,
    parts: &[DocumentBlock],
    block_newlines: &[u32],
    offset: u64,
) -> anyhow::Result<(u64, u64)>;
```

- Output `(one_based_line, absolute_line_start)` for `offset`.
- Sum indexed newline counts before the containing block. To find line start, identify the nearest preceding block whose newline count is non-zero, read only that block, and use its last newline; intervening zero-newline blocks are skipped without reads. Offset zero returns `(1, 0)`.

```rust
fn collect_request_blocks(
    slice: PackSlice,
    decoded_size: u64,
    ranges: &[std::ops::Range<u64>],
    blocks: &[PackBlock],
    output: &mut std::collections::BTreeSet<usize>,
) -> anyhow::Result<()>;
```

- Translate document-relative byte ranges through `document_blocks` and insert intersecting physical IDs. This is used by both two-phase initial planning and lazy reads.

Delete free single-document `fetch_regions`, `load_document_block`, `RegionSource`, and sequential `extend_region` in `pack.rs`; delete `candidate_byte_ranges` in `segment.rs` after its last caller migrates. Keep the compaction-only free `fetch_documents` adapter, `block_span`, `block_runs`, `visit_blocks`, `visit_compressed_block`, cache verification, `fetch_large`, and spool logic.

Remove this exact obsolete conversion; tagged logical expansion, range merging, and the 512-range/50%-coverage decision now occur once in `PackBatch::fetch_regions`:

```rust
fn candidate_byte_ranges(
    decoded_size: u64,
    blocks: &[std::ops::Range<u32>],
) -> Option<Vec<std::ops::Range<u64>>>;
```

### Segment-scoped batch contracts

Add in `crates/index/src/segment.rs`:

```rust
struct SegmentBatch<'a> {
    segment: &'a Segment,
    tables: &'a SegmentTables,
    pack: crate::pack::PackBatch<'a>,
}

struct SegmentedCandidateBatch<'a> {
    reader: &'a SegmentedReader,
    documents: &'a [DocAddress],
    segments: std::collections::BTreeMap<usize, SegmentBatch<'a>>,
}
```

```rust
impl CandidateBatch for SegmentedCandidateBatch<'_> {
    fn fetch_initial(
        &self,
        consume: &mut dyn FnMut(usize, FetchedDocument) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;

    fn fetch_regions(
        &self,
        document: usize,
        ranges: &[std::ops::Range<u64>],
        read: RegionRead,
    ) -> anyhow::Result<FetchedDocument>;
}
```

- `fetch_initial`: validate every address against its segment table/display key; group by segment; whole addresses become `PackRequest`; tagged finite addresses become one `PackRegionRequest` each; call each segment's one `PackBatch` once and map outputs back to original batch indices.
- For each `CandidateRange`, convert `range.blocks` through the document's block table into a clamped, non-empty decoded byte range, then match `range.extent`: `SearchExtent::Bytes { span }` becomes `PackRange::Regional`; `SearchExtent::Lines` becomes `PackRange::FullLines` with zero context for discovery; `SearchExtent::Document` inside `IndexAddress.ranges=Some` errors `document extent cannot appear inside candidate ranges`.
- Incomplete newline tables degrade that document to whole fetch, preserving exactness.
- `fetch_regions`: validate the document index; `Bytes` creates exact regional ranges; `Lines` creates full-line ranges with supplied context; call the same segment `PackBatch`, capture its single result, and error if absent.

Replace the segmented `DocFetcher` candidate methods with:

```rust
fn start_candidate_batch<'a>(
    &'a self,
    documents: &'a [DocAddress],
) -> anyhow::Result<Box<dyn CandidateBatch + 'a>>;
```

- Build one `SegmentBatch` per referenced segment after loading/classifying tables; each owns a `PackBatch` borrowing the existing store/metadata. Return boxed `SegmentedCandidateBatch`.
- Remove `RegionFetch`, `fetch_region`, `fetch_regions_parallel`, `fetch_candidate_each`, and `fetch_candidate_lines`.

### Transitional single-pattern adapter in `crates/index/src/search.rs`

Keep the public signature until Task 4 replaces it:

```rust
pub fn search_streaming(
    reader: &dyn IndexReader,
    pattern: &str,
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> anyhow::Result<SearchStats>;
```

- Preserve parsing, legacy regex compilation, sink behavior, stats, stale-index handling, and every error.
- After computing `query` and `bounded_len`, construct one `CandidatePlan`: `Bytes { span }` only for a positive `bounded_len <= CANDIDATE_BLOCK_BYTES`, otherwise `Lines`; the segmented candidate fold still upgrades `Query::All` to `Document`.
- Replace the old `visit_candidates` call with the one-element plan slice and `CandidateBatchLimits { documents: 16_384, decoded_bytes: 64 * 1024 * 1024 }`. Scope filtering and stop behavior stay in the visitor.

Keep this private signature until Task 4 replaces it:

```rust
fn search_batch(
    documents: &[DocAddress],
    fetcher: &dyn DocFetcher,
    re: &regex::bytes::Regex,
    whole_document: bool,
    bounded_len: Option<usize>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> anyhow::Result<BatchResult>;
```

- Call `fetcher.start_candidate_batch(documents)` once before feeding workers.
- Replace `fetch_candidate_each` with `batch.fetch_initial` and preserve record/worker/channel/stop ordering.
- Replace the lazy `fetch_candidate_lines` call with `batch.fetch_regions(idx, &ranges, RegionRead::Lines { before_context: options.before_context, after_context: options.after_context })`; retain its byte accounting and exact re-verification.
- The regex clone worker initializer stays only through this transitional task; Task 4 replaces it with `WorkerCache`.

Change the two local `IndexReader` test implementations exactly:

```rust
impl IndexReader for BatchReader {
    fn visit_candidates(
        &self,
        plans: &[CandidatePlan<'_>],
        key_prefix: Option<&str>,
        limits: CandidateBatchLimits,
        visit: &mut dyn FnMut(Vec<DocAddress>) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()>;
}

impl IndexReader for ChangingReader {
    fn visit_candidates(
        &self,
        plans: &[CandidatePlan<'_>],
        key_prefix: Option<&str>,
        limits: CandidateBatchLimits,
        visit: &mut dyn FnMut(Vec<DocAddress>) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()>;
}
```

- `BatchReader`: require one plan, ignore prefix/limits as the fixture does today, emit the existing two-document chunks, and honor visitor false.
- `ChangingReader`: require one plan, ignore prefix/limits, visit its one document, then return the same `IndexChanged` error.
- Their `DocFetcher::fetch_each`, `strategy`, `total_docs`, `candidate_docs`, and `stats` signatures/behavior remain unchanged; they use `WholeCandidateBatch` through the default `start_candidate_batch`.

### Focused verification

- [ ] Convert existing pack regional tests to construct one `PackBatch` and preserve every prior byte/line assertion.
- [ ] Move `segment.rs::candidate_ranges_coalesce_and_fall_back_on_majority_coverage` to `pack.rs` as `fn batch_ranges_coalesce_and_fall_back_after_expansion()`; preserve the disjoint-coalescing, 50%-coverage whole fallback, 64-sparse-range, and saturated-line whole-fallback assertions against actual `PackBatch` results. Remove the obsolete `candidate_byte_ranges` assertions from expansion tests migrated in Task 2.
- [ ] Add `fn batch_and_lazy_reads_load_each_pack_block_once()` with two documents sharing a physical block, overlapping initial ranges, and repeated concurrent lazy reads. The counting store must observe one physical range fetch and every result must be byte-exact.
- [ ] Update existing `search.rs` mock fetchers to use the default whole adapter and migrate both `visit_candidates` implementations exactly as above; do not add a new mock framework.
- [ ] Run `cargo test -p seagrep-index candidate`.
- [ ] Run `cargo test -p seagrep-index remote_terms`.
- [ ] Run `cargo test -p seagrep-index --test sparse_remote repeated_plans_fetch_unique_term_and_posting_ranges_once`.
- [ ] Run `cargo test -p seagrep-index pack`.
- [ ] Run `cargo test -p seagrep-index search` to prove the transitional single-pattern adapter remains exact.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Commit Tasks 2–3 together: `git add crates/core/src/store.rs crates/core/src/lib.rs crates/index/Cargo.toml crates/index/src/candidate.rs crates/index/src/eval.rs crates/index/src/lib.rs crates/index/src/pack.rs crates/index/src/remote_terms.rs crates/index/src/search.rs crates/index/src/segment.rs crates/index/tests/sparse_remote.rs && git commit -m "perf: union candidate and pack reads"`.

---

## Task 4: Verify multiple patterns with typed result detail

**Files:**

- Modify `crates/core/src/grep.rs`: line assembly consumes `PatternMatch` spans; remove regex-owned analysis, duplicate fast paths, and the duplicate dead `parse_pattern`.
- Modify `crates/core/src/store.rs`: migrate `scan_matching_docs` to `PatternProgram` and one reusable cache.
- Modify `crates/core/src/codec.rs`: mechanically update the one grep oracle test.
- Modify `crates/core/src/lib.rs`: revise grep exports; stop exporting removed bound/isolation functions.
- Modify `crates/core/Cargo.toml`: move `regex.workspace = true` from production dependencies to dev-dependencies; only the independent witness-oracle assertions in `pattern.rs` retain it.
- Modify `crates/index/src/search.rs`: independent plan selection, ordered whole/line/regional programs, job-local caches, tagged verification, lazy full lines, bounded windows, deduplication, and compatibility wrapper.
- Modify `crates/index/src/lib.rs`: re-export typed result/search APIs and add exact/proof/fallback pattern counters to `SearchStats`.
- Modify `crates/index/Cargo.toml`: remove `regex.workspace = true` after search migration.
- Modify `crates/index/tests/differential_store.rs` and `crates/index/tests/segmented.rs`: use HIR/program oracles and add the one mixed giant-line differential case.
- Modify `crates/cli/src/printer.rs`: migrate existing standard/path/count/quiet sinks to typed detail/data without changing rendered bytes; Task 5 adds window rendering.
- Modify `crates/cli/src/json.rs`: migrate the JSON sink to typed full-line data without changing its wire bytes.

### Core line assembly contracts

Replace regex-specific functions with:

```rust
pub fn grep_matches(
    bytes: bytes::Bytes,
    matches: &[PatternMatch],
    options: MatchOptions,
) -> Vec<LineEvent>;
```

- Input schema: `bytes` owns the exact decoded byte range whose offsets start at zero; `matches` is required, sorted in leftmost-first non-overlap order, and every `PatternMatch { pattern: usize, start: usize, end: usize }` satisfies `start <= end <= bytes.len()` and stays within one logical line's content because programs compile sanitized HIRs; `options` is exactly `MatchOptions { before_context: usize, after_context: usize, max_count: Option<u64> }`. Pattern IDs do not change line assembly.
- Output schema: `Vec<LineEvent>` in ascending line order, with at most one event per line. Each event is exactly `LineEvent { line: u64, kind: LineKind, offset: u64, text: bytes::Bytes, submatches: Vec<SubMatch> }`; match-line submatches are line-content-relative half-open offsets, `text` keeps its trailing newline when present, and context merge, EOF empty-match, and `max_count` after-context drain semantics remain byte-for-byte compatible. No runtime errors are returned; invalid/out-of-order spans are debug assertions because only `PatternProgram` constructs them.
- Transformation: return empty immediately for `max_count=Some(0)`; wrap `matches.iter()` in a peekable iterator; walk `bytes` line-by-line using the current `memchr` boundary logic; consume spans whose start belongs to the current content line, clamp their ends to content, and build `SubMatch`; retain the existing before-context ring, after-context countdown, matching-line counter, and early-stop conditions.

```rust
pub fn grep_bytes(
    bytes: bytes::Bytes,
    program: &PatternProgram,
    cache: &mut PatternCache,
    options: MatchOptions,
) -> Vec<LineEvent>;
```

- Input schema: `bytes` and `options` have the `grep_matches` schemas; `program` is a required compiled ordered program built from line-sanitized HIRs; `cache` is the exclusive mutable cache created by that same program.
- Output schema: exactly the `grep_matches` `Vec<LineEvent>` schema; no errors.
- Transformation: collect `program.find_iter(cache, &bytes)` once into `Vec<PatternMatch>` and pass ownership of `bytes`, the borrowed matches, and `options` to `grep_matches`.

```rust
pub fn grep_doc(
    bytes: &[u8],
    program: &PatternProgram,
    options: MatchOptions,
) -> Vec<LineEvent>;
```

- Input schema: `bytes` is one complete decoded document; `program` is a required compiled ordered program; `options` has the `grep_matches` schema.
- Output schema: exactly the `grep_matches` `Vec<LineEvent>` schema; no errors.
- Transformation: copy `bytes` once into `bytes::Bytes`, create exactly one `PatternCache` with `program.create_cache()`, and call `grep_bytes`.

```rust
pub fn has_line_match(
    bytes: &[u8],
    program: &PatternProgram,
    cache: &mut PatternCache,
) -> bool;
```

- Input schema: `bytes` is one complete decoded document or exact full-line body; `program` is eligibility-correct for that body; `cache` is the exclusive mutable cache created by that program.
- Output schema: `true` iff `program.find_iter` yields a match whose start belongs to a logical line; otherwise `false`. No errors. An empty document never matches, and an empty match at EOF after a terminal newline belongs to no line.
- Transformation: return false for empty input; pull matches from one iterator until finding `start < bytes.len()` or `start == bytes.len()` with a non-newline final byte; return false when the iterator is exhausted.

Delete `grep_bytes_fast`, `grep_bytes_inner`, `has_line_match_fast`, `can_search_as_document`, `bounded_match_len`, `has_look`, `needs_line_isolation`, and the duplicate `grep.rs::parse_pattern`. Keep only `pattern.rs::parse_pattern`, already re-exported from `crates/core/src/lib.rs`; Task 4's `search_streaming` calls that parser before `search_patterns`, so it is production-used and no `dead_code`/`unreachable_pub` warning remains. Remove the deleted grep symbols from `crates/core/src/lib.rs`; keep the pattern-module `parse_pattern` export unchanged.

Set the grep re-export exactly:

```rust
pub use grep::{
    grep_bytes, grep_doc, grep_matches, has_line_match, LineEvent, LineKind, MatchOptions, SubMatch,
};
```

Change the oracle:

```rust
pub fn scan_matching_docs(
    corpus: &dyn Corpus,
    program: &PatternProgram,
) -> anyhow::Result<Vec<String>>;
```

- Input schema: `corpus` is the required source collection to decode in source order; `program` is the required compiled ordered verifier shared immutably across documents.
- Output schema: sorted `Vec<String>` containing each decoded logical document key exactly once when any eligible match belongs to a line. Errors preserve `Corpus::fetch` and `decode_source` chains unchanged.
- Transformation: create one nested `ScanSink { program: &PatternProgram, cache: PatternCache, key: String, bytes: Vec<u8>, hits: Vec<String> }`; reuse its cache sequentially across every decoded document; fetch and decode each corpus source; sort `hits` with `sort_unstable`; return it.
- `fn begin(&mut self, document: &LogicalDocumentMeta) -> anyhow::Result<()>`: input is the required logical-document metadata; copy `document.display_key` into `key`, clear `bytes`, and return `Ok(())`; no added error.
- `fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()>`: input is the next decoded chunk; append it to the sink buffer and return `Ok(())`; no added error.
- `fn finish(&mut self) -> anyhow::Result<()>`: input is the buffered current document in sink state; call `has_line_match(&self.bytes, self.program, &mut self.cache)`, clone `key` into `hits` only on true, and return `Ok(())`; no added error.

### Public result contracts in `crates/index/src/search.rs`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchDetail {
    Documents,
    MatchingLines,
    MatchCount,
    MatchWindows { max_bytes: usize },
    FullLines,
}

#[derive(Debug)]
pub enum MatchData<'a> {
    Documents,
    Lines(&'a [LineEvent]),
    Windows(&'a [MatchWindow]),
}

#[derive(Debug)]
pub struct DocResult<'a> {
    pub data: MatchData<'a>,
    pub bytes_searched: u64,
    pub elapsed: std::time::Duration,
}

pub trait MatchSink: Sync {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool {
        true
    }

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- Remove `wants_matches` and `wants_line_text`.
- `Documents` never carries fake empty events. `Lines` is used for both matching-line and exact-match counts; `Windows` only for the explicit window detail.
- `fn detail(&self) -> SearchDetail`: input is the shared sink; output is one immutable detail selection for the entire search; no errors. `search_patterns` reads it once before candidate I/O and passes it unchanged through every batch.
- `fn wants_hit_keys(&self) -> bool`: input is the shared sink; output `true` means collect matching keys in `SearchStats.hits`, while `false` keeps only `hit_count`; no errors and the default remains `true`.
- `fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>`: `key` is the required matching display key and `doc` is exactly `DocResult { data: MatchData<'_>, bytes_searched: u64, elapsed: Duration }` for the selected detail. Output `Continue` accepts more results and `Stop` ends successfully; sink-defined errors propagate unchanged. The payload borrows job-owned data only for this call and is never retained by the engine.

Add the shared `MatchWindow` and `WindowMatch` schemas verbatim in this file and re-export them.

Set the search re-export in `crates/index/src/lib.rs` exactly:

```rust
pub use search::{
    search_collect, search_patterns, search_streaming, DocResult, KeyScope, MatchData, MatchSink,
    MatchWindow, NullSink, SearchDetail, SinkFlow, WindowMatch,
};
```

Extend `SearchStats` in `crates/index/src/lib.rs` exactly:

```rust
pub patterns: usize,
pub exact_patterns: usize,
pub proof_patterns: usize,
pub fallback_patterns: usize,
```

- Every return path in `search_patterns` populates all four fields. `patterns=plans.len()`; the other three count `PatternPlan.kind` and sum exactly to `patterns`, including the `max_count=0` early return. Existing counters retain their meanings and errors.

### Private search schemas

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatternKind {
    Exact,
    Proof,
    Fallback,
}

struct PatternPlan {
    id: usize,
    query: seagrep_query::Query,
    bounds: MatchBounds,
    extent: SearchExtent,
    kind: PatternKind,
}

struct SearchPrograms {
    whole: PatternProgram,
    lines: Option<PatternProgram>,
    regional: Option<PatternProgram>,
}

struct WorkerCache {
    whole: PatternCache,
    lines: Option<PatternCache>,
    regional: Option<PatternCache>,
}

#[derive(Clone, Copy)]
struct SearchContext<'a> {
    plans: &'a [PatternPlan],
    programs: &'a SearchPrograms,
    stream_overlap: Option<usize>,
    options: MatchOptions,
    detail: SearchDetail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegionMatch {
    pattern: usize,
    witness: std::ops::Range<u64>,
    line: u64,
    line_offset: u64,
    canonical_span_known: bool,
}

struct BatchResult {
    hits: Vec<String>,
    hit_count: usize,
    regional_docs: usize,
    whole_docs: usize,
    candidate_bytes: usize,
    decoded_bytes: usize,
    stopped: bool,
}

enum OwnedMatchData {
    Documents,
    Lines(Vec<LineEvent>),
    Windows(Vec<MatchWindow>),
}

struct VerifiedDocument {
    data: OwnedMatchData,
    bytes_searched: u64,
    extra_fetched_bytes: usize,
}
```

`OwnedMatchData` owns exactly the payload selected by `SearchDetail`; `VerifiedDocument.extra_fetched_bytes` counts lazy line/window bytes in addition to the initial candidate body's `fetched_size`.

`SearchPrograms.whole` is always present and preserves every original pattern in input order. `lines` preserves the ordered `Bytes`/`Lines` subset and is `None` only when every plan is `Document`. `regional` preserves the ordered `Bytes` subset and is `None` when no finite regional plan exists. `WorkerCache` has exactly the same optional shape; each value is owned by one sequential verifier or one `try_for_each_init` job state.

`SearchContext` is the immutable per-search input passed into batch/document verification. `plans` and `programs` are the exact matching tables compiled by `search_patterns`; `stream_overlap` is computed once from those plans; `options` and `detail` are the one request-wide values. It is `Copy` because it contains only shared references and copyable values. Grouping these fields keeps both verification functions below Clippy's argument-count threshold without an `allow`.

### Search planning/program functions

```rust
fn build_plans(
    hirs: &[regex_syntax::hir::Hir],
    strategy: Strategy,
    detail: SearchDetail,
) -> anyhow::Result<Vec<PatternPlan>>;
```

- Input schema: `hirs` is the required non-empty ordered sanitized HIR slice; `strategy` is the reader's exact immutable `Strategy`; `detail` is the one sink-selected result shape. Neither HIRs nor strategy are mutated.
- Output schema: `Vec<PatternPlan>` of exactly `hirs.len()` entries in input order. Entry `index` is exactly `PatternPlan { id: index, query: Query, bounds: MatchBounds, extent: SearchExtent, kind: PatternKind }`, with positive/capped byte extents and one mutually exclusive kind.
- Errors: empty input returns exactly `search requires at least one pattern`; `SearchDetail::MatchWindows { max_bytes: 0 }` returns exactly `match window must be greater than 0`; no other new errors.
- Transformation: validate the two request invariants; call `analyze_patterns(hirs)` once so the retained proof budget spans the whole query; zip HIRs with bounds; call `seagrep_query::plan_hir(hir, strategy)` independently; choose the detail-specific bound; then derive extent/kind by the rules below.
- Select finite span by detail: witness for `Documents`, `MatchingLines`, `MatchWindows`, and `FullLines`; `exact_bytes` for `MatchCount`.
- `Query::All` always maps to `Document`. A positive selected bound maps to `Bytes` only when it is at most `CANDIDATE_BLOCK_BYTES`; a larger or absent bound maps through `FallbackExtent::{Lines,Document}`. This cap applies equally to large finite exact matches and finite proofs, so every constructed `SearchExtent::Bytes.span` satisfies its schema invariant.
- Set `kind=Exact` when the selected in-cap source is `exact_bytes` or `MatchWitness::Exact`, `kind=Proof` when it is `MatchWitness::Proven`, and `kind=Fallback` whenever the final extent is `Lines` or `Document` (including `Query::All` and over-cap finite matches).
- Move the owned `query` and `bounds` into the output plan; assign `id=index`; collect without reordering.

Keep `FILE_MATCH_CHUNK = 1 MiB` and `FILE_MATCH_OVERLAP_MAX = 1 MiB`. Add:

```rust
fn get_stream_overlap(plans: &[PatternPlan]) -> Option<usize>;
```

- Input: the non-empty ordered plans returned by `build_plans`.
- Output: `Some(max_span - 1)` only when every `plan.extent` is `SearchExtent::Bytes { span }` and the resulting overlap is at most `FILE_MATCH_OVERLAP_MAX`; otherwise `None`. No errors.
- Transformation: return `None` on the first `Lines`/`Document` extent, compute the maximum positive span, subtract one exactly once, apply the 1 MiB ceiling, and return it. `search_patterns` computes this once and passes it unchanged to every `search_batch`/`verify_document` call.

```rust
impl SearchPrograms {
    fn compile(
        hirs: &[regex_syntax::hir::Hir],
        plans: &[PatternPlan],
    ) -> anyhow::Result<SearchPrograms>;
}
```

- Input schema: `hirs` is the required non-empty ordered sanitized HIR slice; `plans` is required, has the same length/order, and every `plans[index].id == index`. Each plan supplies `extent: SearchExtent` as the eligibility source. Neither input is mutated.
- Output schema: `SearchPrograms { whole, lines, regional }`. `whole` compiles every HIR with IDs from `plans`. `lines` compiles the ordered subset where `extent != Document`, or `None` when empty. `regional` compiles the ordered subset where `extent` is `Bytes`, or `None` when empty. Subsets keep original IDs; no local renumbering occurs.
- Errors: mismatched lengths error `pattern HIR count {HIRS} differs from plan count {PLANS}`. Invalid IDs error `pattern plan {INDEX} carries ID {ID}`. Program compilation preserves the underlying regex error and adds exactly `compiling {N}-pattern whole verifier`, `compiling {N}-pattern line verifier`, or `compiling {N}-pattern regional verifier` for the program being built.
- Transformation: validate lengths and IDs once; compile `whole` from the borrowed full HIR slice; iterate `hirs.zip(plans)` twice to clone only the line/regional subset HIRs and copy their original IDs; skip compilation for an empty subset; return all three immutable programs. Never compile a broad program and attach an eligibility bitmap for post-filtering.

```rust
impl WorkerCache {
    fn create(programs: &SearchPrograms) -> WorkerCache;
}
```

- Input schema: `programs` is a required compiled `SearchPrograms` reference and outlives the returned cache's use; it is not mutated.
- Output schema: `WorkerCache { whole, lines, regional }`, with one cache from `whole.create_cache()` and optional caches present iff their matching optional programs are present. No errors and no shared mutex/pool.
- Transformation: create the whole cache, then map `lines.as_ref()` and `regional.as_ref()` through `PatternProgram::create_cache`. `search_batch` calls this once for the sequential path or from `try_for_each_init`; Rayon controls job-state cardinality.

```rust
fn get_region_program<'a>(
    programs: &'a SearchPrograms,
    cache: &'a mut WorkerCache,
    region: RegionProgram,
) -> anyhow::Result<(&'a PatternProgram, &'a mut PatternCache)>;
```

- Input schema: `programs` and `cache` are required and must have identical optional shapes; `region=Regional` means a truncated finite region, while `region=Full` means an exact full-line region. `whole` is deliberately not selectable here.
- Output schema: the immutable eligible program plus its exclusive mutable job-local cache. `Regional` returns the regional pair; `Full` returns the line pair.
- Errors: `regional verifier is missing`, `regional verifier cache is missing`, `line verifier is missing`, or `line verifier cache is missing`. These indicate an internal program/body invariant violation and retain `anyhow::Error` type.
- Transformation: match `region`, borrow the corresponding optional program and cache, return the pair without compiling, cloning, filtering, or allocating.

### Verification functions

```rust
fn has_stream_match(
    reader: &mut impl std::io::Read,
    len: u64,
    program: &PatternProgram,
    cache: &mut PatternCache,
    overlap: usize,
) -> anyhow::Result<bool>;
```

- Input schema: `reader` is positioned at byte zero of one whole decoded body and yields exactly `len` bytes; `program` is the ordered eligibility-correct program for the complete document; `cache` is its exclusive mutable job-local cache; `overlap` is the exact value returned by `get_stream_overlap(plans)` and is at most `FILE_MATCH_OVERLAP_MAX`.
- Output schema: `Ok(true)` at the first eligible match in any carry/chunk body, `Ok(false)` after consuming a nonmatching body or immediately for `len=0`. Read/conversion errors preserve their source chain; `overlap > FILE_MATCH_OVERLAP_MAX` returns exactly `streaming regex overlap exceeds its limit`.
- Transformation: validate the overlap before allocation; allocate one `FILE_MATCH_CHUNK + overlap` buffer; read at most 1 MiB per iteration; search the combined carry plus new chunk once with the passed `program/cache`; retain the last `overlap` bytes with `copy_within`; stop at the first match. The only production caller passes `programs.whole/cache.whole`, and only for `Documents` when every plan is `Bytes` and a whole body is file-backed.

```rust
fn find_program_matches(
    bytes: &[u8],
    program: &PatternProgram,
    cache: &mut PatternCache,
) -> Vec<PatternMatch>;
```

- Input schema: `bytes` is the exact searched slice; `program` is already eligibility-correct for that slice; `cache` is its exclusive mutable job-local cache. Pattern IDs inside the program are original `PatternPlan.id` values.
- Output schema: leftmost-first, non-overlapping `PatternMatch` values relative to `bytes`, preserving program order for equal-start alternatives. Empty input and empty matches retain `PatternProgram::find_iter` semantics. No errors.
- Transformation: collect `program.find_iter(cache, bytes)` once. Do not inspect plans, filter IDs, merge another program, or rescan per pattern.

```rust
fn find_region_matches(
    regions: &[DocumentRegion],
    decoded_size: u64,
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    max_count: Option<u64>,
) -> anyhow::Result<Vec<RegionMatch>>;
```

- Input schema: `regions` contains sorted, disjoint document-relative regions with `program=Regional|Full`; every region starts on one-based `line`, has an exact absolute `line_offset`, and satisfies `region.start + region.bytes.len() <= decoded_size`; `decoded_size` is the complete document length; `plans[id]` exists for every original match ID; `programs/cache` have identical optional shapes; `max_count` limits distinct matching `(line,line_offset)` pairs and `Some(0)` returns empty.
- Output schema: identity-deduplicated `RegionMatch` values with original pattern IDs, absolute complete witnesses, exact one-based line/line-start metadata, and `canonical_span_known=false` only for `MatchWitness::Proven` on a regional body. Added invariant errors come only from `get_region_program`; proof/body errors preserve their source chain.
- Select the eligible regional/line pair with `get_region_program` before searching each `DocumentRegion`; call `find_program_matches` once. Call that pattern's `MatchWitness::find_witness` for regional matches and use the canonical meta span for full-line matches. Never run `whole` on a region and never post-filter an ineligible winner.
- Convert to absolute witness offsets. Compute exact line/line start using region metadata plus newlines before the witness. Drop starts at/after decoded EOF.
- Sort/dedup first by identity `(pattern, witness.start, witness.end)`. Then sort by `(line, line_offset, witness.start, witness.end, pattern)` and enforce `max_count` on the earliest distinct `(line, line_offset)` values, retaining all accepted patterns/spans on every retained line. Pattern order never decides which lines survive `-m`.
- `canonical_span_known` is true for exact bounds/full matches and false only for proven regional witnesses.

```rust
fn find_whole_matches(
    bytes: &[u8],
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    max_count: Option<u64>,
) -> Vec<RegionMatch>;
```

- Input schema: `bytes` is the complete document starting at absolute offset zero; every `plans[id]` exists; `programs.whole` and `cache.whole` are the matching pair; `max_count` limits distinct matching lines and `Some(0)` returns empty.
- Output schema: identity-deduplicated canonical `RegionMatch` values with original pattern IDs, absolute spans, exact line metadata, and `canonical_span_known=true`. No new errors.
- Run `find_program_matches(bytes, &programs.whole, &mut cache.whole)` once, so every pattern ID is eligible and absolute anchors see real document boundaries.
- Walk newlines once while consuming canonical `PatternMatch` spans to populate exact one-based line and absolute line start; mark every span canonical; identity-dedup, line/offset-sort, and apply `max_count` with the same earliest-distinct-line rule as `find_region_matches`.

```rust
fn build_count_events(
    matches: &[RegionMatch],
    count_matches: bool,
) -> Vec<LineEvent>;
```

- Input schema: `matches` is the sorted, identity-deduplicated output of `find_region_matches` or `find_whole_matches`; `count_matches=false` requests one matching-line unit per line; `count_matches=true` requires every input to have `canonical_span_known=true` and requests one unit per canonical match.
- Output schema: ordered zero-text `LineEvent` values grouped by exact `line`. Each event is `LineKind::Match`, has the group's absolute `line_offset` in `offset`, has `Bytes::new()` text, and carries either one zero-width `SubMatch` for matching-line count or one zero-width `SubMatch` per canonical match for exact-match count. Empty input returns empty; no errors.
- Transformation: iterate already-sorted matches once; start a new event when `line` changes; for `count_matches=false`, keep exactly one submatch on the event; for `count_matches=true`, append one submatch for every match and debug-assert canonical spans. Proof-only matches never enter exact-match mode because `build_plans` selects a fallback extent first.

```rust
fn collect_line_events(
    body: FetchedDocument,
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    options: MatchOptions,
) -> anyhow::Result<Vec<LineEvent>>;
```

- Input schema: `body` is one owned whole document or sorted tagged regions satisfying the `find_region_matches` metadata/range invariants; `plans` maps every original pattern ID; `programs/cache` have identical optional shapes; `options` carries exact before/after context and matching-line `max_count`.
- Output schema: ordered `LineEvent` values with exact document-relative offsets, one-based lines, merged context, original line bytes, and line-relative submatches. No match returns an empty vector.
- Errors: whole-body materialization preserves its source chain; missing eligible region program/cache returns the exact `get_region_program` invariant error; validated region metadata makes line/offset adjustment infallible and adds no arithmetic error.
- Transformation for `Whole`: materialize once, collect matches with `programs.whole/cache.whole`, call `grep_matches` once.
- Transformation for `Regions`: call `get_region_program` for each region tag before searching; collect only that ordered eligible program's matches; call `grep_matches` with remaining `max_count`; adjust line/offset; sort and merge duplicate match/context lines exactly like current regional output. Never run `whole` on a partial region and never post-filter IDs.

```rust
fn fetch_full_lines(
    batch: &dyn CandidateBatch,
    document: usize,
    matches: &[RegionMatch],
    plans: &[PatternPlan],
    programs: &SearchPrograms,
    cache: &mut WorkerCache,
    options: MatchOptions,
) -> anyhow::Result<Vec<LineEvent>>;
```

- Input schema: `batch` is the live candidate batch; `document` indexes its immutable address slice; `matches` contains confirmed absolute witnesses; `plans/programs/cache` are the same query objects used for discovery; `options` carries requested context and `max_count`.
- Output schema: exact re-verified full-line/context `LineEvent` values. Empty `matches` returns an empty vector without I/O.
- Errors: candidate-range validation/fetch errors preserve their source chain; missing line verifier/cache is the exact `get_region_program` error reached through `collect_line_events`; `candidate region fetch returned no document` remains unchanged.
- Transformation: convert every non-empty witness to its unchanged absolute range; convert an empty witness at `start < decoded_size` to `start..start + 1` (all starts at/after EOF were already dropped); call the same batch's `fetch_regions(document, seeds, RegionRead::Lines { before_context, after_context })`; re-run `collect_line_events`, whose `RegionProgram::Full` branch selects the ordered line program. This is mandatory for default/JSON proof hits.

```rust
fn build_windows(
    batch: &dyn CandidateBatch,
    document: usize,
    matches: &[RegionMatch],
    decoded_size: u64,
    max_bytes: usize,
    whole: Option<&bytes::Bytes>,
) -> anyhow::Result<Vec<MatchWindow>>;
```

- Input schema: `batch` is the live candidate batch; `document` is its valid document index; `matches` is sorted by exact `(line,line_offset,witness)` and contains complete absolute witnesses within `decoded_size`; `decoded_size` is the complete document length; `max_bytes` is required and positive; `whole=Some(Bytes)` contains exactly `decoded_size` bytes, while `None` authorizes bounded lazy reads.
- Output schema: at most one `MatchWindow` per distinct matching line, in line order. Every `text.len() <= max_bytes`; `window_offset` and `line_offset` are absolute; each `visible` range is the witness/text intersection relative to `text` and may be empty only for a zero-width witness; edge/per-match clip flags are exact.
- Errors: zero `max_bytes` returns `match window must be greater than 0`; whole-length mismatch returns `whole document length {ACTUAL} differs from decoded size {EXPECTED}`; arithmetic errors use `match window range overflows`; lazy candidate fetch/validation errors preserve their source chain.
- Group matches by exact line; choose the lowest witness start as the line anchor; emit one window per retained line.
- When `whole=Some`, create `MatchWindow.text` with `whole.slice(start..end)` so ownership and zero-copy slicing survive; never convert the owner to `&[u8]`. Otherwise fetch at most `max_bytes` before and after the anchor plus at most `max_bytes` of the anchor through `RegionRead::Bytes`; never request the complete witness merely because it is longer than the budget.
- Locate newline/EOF inside fetched bytes, center a window of at most `max_bytes` around the complete witness when it fits, otherwise start at the witness start; shift left when an early line end leaves spare budget.
- Set exact `window_offset`, `line_offset`, edge clip flags, and one `WindowMatch` for every confirmed witness intersecting text. `visible` is clamped relative to text; per-match left/right flags compare visible absolute endpoints to complete witness endpoints.

```rust
fn verify_document(
    batch: &dyn CandidateBatch,
    document: usize,
    body: FetchedDocument,
    context: SearchContext<'_>,
    cache: &mut WorkerCache,
) -> anyhow::Result<Option<VerifiedDocument>>;
```

- Input schema: `batch/document` identify the live candidate; `body` is the one initial fetch result; `context` contains the exact plans/programs/precomputed overlap/request options/fixed sink detail; `cache` is the matching exclusive job-local state.
- Output schema: `None` for no verified match; otherwise `VerifiedDocument { data, bytes_searched, extra_fetched_bytes }`, where `data` has the exact variant required by `context.detail`, `bytes_searched=body.decoded_size()`, and `extra_fetched_bytes` is only lazy line/window bytes beyond `body.fetched_size()`.
- Errors: proof errors, body materialization, candidate-batch errors, and added exact helper invariant messages propagate unchanged; this function never calls the sink.
- Record decoded/fetched sizes before consuming the body. For `Whole`, stream only `Documents` when `context.stream_overlap=Some` and the body is file-backed; `has_stream_match` uses `context.programs.whole/cache.whole` and receives that exact overlap. Otherwise materialize once into owned `bytes::Bytes`, call `find_whole_matches`/`grep_matches` according to `context.detail`, and call `build_windows(..., Some(&bytes))` for windows so returned slices retain ownership. `None` never enters chunked verification because a line/document extent could cross an arbitrary chunk boundary.
- For `Regions`, pass `context.plans/context.programs/context.options.max_count` to `find_region_matches`; no matches returns `None`; `Documents` returns owned document data; counts call `build_count_events`; `FullLines` calls `fetch_full_lines` with context plans/programs/options; windows call `build_windows(..., None)`.
- Return `None` for no verified hit. Return exact lazy fetched-byte accounting for line/window reads. Propagate verifier proof, body materialization, candidate-batch, and sink-independent errors.

```rust
fn search_batch(
    documents: &[DocAddress],
    fetcher: &dyn DocFetcher,
    context: SearchContext<'_>,
    sink: &dyn MatchSink,
) -> anyhow::Result<BatchResult>;
```

- Input schema: `documents` is a required non-empty candidate batch in reader order; `fetcher` creates one scoped batch for exactly those addresses; `context` contains the non-empty plans, their compiled programs, their one stream overlap, immutable options, and the single detail value read before candidate I/O; `sink` is safe for concurrent calls.
- Output schema: `BatchResult { hits: Vec<String>, hit_count: usize, regional_docs: usize, whole_docs: usize, candidate_bytes: usize, decoded_bytes: usize, stopped: bool }`. `hits` contains each verified key iff `sink.wants_hit_keys()`; `hit_count` counts every verified document; regional/whole counts classify every initial body; byte fields sum initial and explicit lazy reads exactly as currently defined; `stopped=true` only after `SinkFlow::Stop`.
- Errors: `start_candidate_batch`, initial/lazy fetch, verification, and sink errors preserve their source chains; worker or poisoned-result panics map to `a search worker panicked`; internal `StopEarly` remains private and never escapes as a user error. Removing `std::thread::available_parallelism()` removes its possible I/O error from this function.
- Transformation: start one scoped `CandidateBatch`; `fetch_initial` performs the unioned pack preload; record initial fetched/decoded bytes and body kind before queueing; call `verify_document` with unchanged `context`; borrow its owned payload into the matching `MatchData` variant; invoke the sink exactly once for a verified document; then drop the payload.
- Compute `let jobs = rayon::current_num_threads().min(documents.len());`. Use `jobs` only to choose the sequential path and size the bounded channel to `jobs * 2`; do not infer cache cardinality from it.
- When `jobs == 1`, create one `WorkerCache` from `context.programs` and reuse it sequentially. Otherwise retain the current scoped feed/channel/stop/panic protocol and run `rx.into_iter().par_bridge().try_for_each_init(|| WorkerCache::create(context.programs), ...)`; every initializer result is job-local state owned by one sequential Rayon job, and Rayon may create a different number of states than OS or pool threads.
- Per body/detail:
  - `Documents`: first verified match, no payload;
  - `MatchingLines`: witness/full matches converted to line events;
  - `MatchCount`: canonical exact matches only;
  - `FullLines`: discover, lazily fetch/reverify complete lines/context;
  - `MatchWindows`: build bounded windows from confirmed witnesses.
- Deduplicate before applying `-m`; collect hit keys only when requested; preserve byte/document counters and `StopEarly` behavior.
- Merge the feed and verifier results with the existing precedence: a real feed error wins over the stop sentinel, a verifier `StopEarly` produces `stopped=true`, every other verifier error returns, and success requires the feed to succeed. Extract atomics/mutex contents and return the exact `BatchResult` schema.

### Public search functions

```rust
pub fn search_patterns(
    reader: &dyn IndexReader,
    hirs: &[regex_syntax::hir::Hir],
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> anyhow::Result<SearchStats>;
```

- Input schema: `reader` is the immutable index/candidate source; `hirs` is a required non-empty ordered slice with one sanitized HIR per original CLI `-e`; `scope` is exactly `KeyScope { prefix: Option<&str>, matches: Option<&(dyn Fn(&str) -> bool + Sync)> }`; `options` is the request `MatchOptions`; `sink` is the shared result consumer and its `detail()` value is fixed for this call.
- Output schema: `SearchStats { hits: Vec<String>, hit_count: usize, candidates: usize, total_docs: usize, bytes_fetched: usize, regional_docs: usize, whole_docs: usize, candidate_bytes: usize, decoded_bytes: usize, excluded_objects: usize, patterns: usize, exact_patterns: usize, proof_patterns: usize, fallback_patterns: usize }`. `hits` is globally sorted; category counts derive from mutually exclusive final `PatternPlan.kind` values and sum exactly to `patterns=hirs.len()`; all existing counter meanings remain unchanged.
- Errors: propagate exact `build_plans`, `SearchPrograms::compile`, candidate-reader, batch, verifier, and sink errors. Preserve counter contexts `candidate count overflows usize`, `hit count overflows usize`, `fetched byte count overflows usize`, `regional document count overflows usize`, `whole document count overflows usize`, and `decoded byte count overflows usize`. If `IndexChanged` arrives after at least one candidate was admitted, replace it with exactly `index changed after candidate batches began; rerun the search to get a clean snapshot`; otherwise preserve it.
- Transformation: read `detail=sink.detail()` once; build plans and all three programs once; count plan kinds; compute `stream_overlap` once; construct `SearchContext { plans: &plans, programs: &programs, stream_overlap, options, detail }`; if `options.max_count==Some(0)`, return valid pattern/reader metadata with all search-work counters zero before candidate I/O.
- Convert every plan to a borrowed `CandidatePlan`; call `reader.visit_candidates` once with `CandidateBatchLimits { documents: 16_384, decoded_bytes: 64 * 1024 * 1024 }`; apply `scope.admits` before fetch; skip empty filtered batches; call `search_batch` with the unchanged context; checked-add all counters; stop visits when a batch reports `stopped`; sort hits; construct the exact output.

```rust
pub fn search_streaming(
    reader: &dyn IndexReader,
    pattern: &str,
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> anyhow::Result<SearchStats>;
```

- Input schema: `pattern` is one required already line-sanitized compatibility regex string; `reader`, `scope`, `options`, and `sink` have the exact `search_patterns` schemas.
- Output schema: exactly the `SearchStats` returned by `search_patterns`, with `patterns=1`; errors are the unchanged `seagrep_core::parse_pattern` syntax error or a downstream `search_patterns` error.
- Transformation: execute `let hir = seagrep_core::parse_pattern(pattern)?;` and then `search_patterns(reader, std::slice::from_ref(&hir), scope, options, sink)`. This is the CLI's sole per-pattern parser entry. Do not compile `pattern` through `regex`, concatenate it with another expression, or retain a parser in `grep.rs`.

```rust
pub fn search_collect(
    reader: &dyn IndexReader,
    pattern: &str,
) -> anyhow::Result<(Vec<(String, LineEvent)>, SearchStats)>;
```

- Input schema: `reader` is the immutable index source and `pattern` is one required already line-sanitized compatibility regex string.
- Output schema: `(matches, stats)`, where `matches` owns every full-line event paired with its document key and is globally sorted by `(key, line, first_submatch_start)`; `stats` is the exact `search_streaming` output. Errors preserve parser/search/sink errors, and a poisoned result mutex maps to `a search worker panicked`.
- Transformation: create `CollectSink::default()`, call `search_streaming` with `KeyScope::default()` and `MatchOptions::default()`, take the sink vector, apply the existing global sort unchanged, and return it with stats.

### Exact index and test sink migrations

In `crates/index/src/search.rs`:

```rust
impl MatchSink for NullSink {
    fn detail(&self) -> SearchDetail;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}

impl MatchSink for CollectSink {
    fn detail(&self) -> SearchDetail;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `NullSink::detail` returns `Documents`; `on_doc` ignores both required inputs and returns `Continue` without error. It inherits `wants_hit_keys=true` because `SearchStats.hits` is this sink's output.
- `CollectSink::detail` returns `FullLines`; `on_doc` accepts only `MatchData::Lines`, locks `matches`, appends `(key.to_owned(), event.clone())` in event order, and returns `Continue`. Other data errors `collect sink requires line data`; lock errors remain `a search worker panicked`.

In the existing local test sinks in `crates/index/src/lib.rs`:

```rust
impl MatchSink for CountOnlySink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}

impl MatchSink for StopAfterFirst {
    fn detail(&self) -> SearchDetail;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `CountOnlySink`: `detail=Documents`, `wants_hit_keys=false`, ignore key/data, return `Continue`; no error.
- `StopAfterFirst`: `detail=Documents`, inherit `wants_hit_keys=true`, ignore key/data, return `Stop`; no error. The existing cooperative-stop assertions stay unchanged.

In `crates/index/tests/differential_store.rs`:

```rust
impl MatchSink for EventSink {
    fn detail(&self) -> SearchDetail;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}

impl MatchSink for CountEventSink {
    fn detail(&self) -> SearchDetail;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `EventSink`: `detail=FullLines`; require `MatchData::Lines` or error `event sink requires line data`; ignore key, lock `events`, clone the slice into it, return `Continue`. Preserve the existing test-only poisoned-lock `unwrap`.
- `CountEventSink`: `detail=MatchCount`; require `MatchData::Lines` or error `count event sink requires line data`; ignore key, append events exactly as above, return `Continue`. Remove `wants_line_text`.

### Downstream typed sink migration

In `crates/cli/src/printer.rs`, keep every constructor and rendering function signature unchanged in Task 4 and change only these trait methods:

```rust
impl MatchSink for StandardSink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}

impl MatchSink for PathSink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}

impl MatchSink for CountSink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}

impl MatchSink for QuietSink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `StandardSink::detail`: no inputs beyond `self`; return `FullLines`; no error. `wants_hit_keys` remains false. `on_doc` requires `doc.data=MatchData::Lines(events)` or errors exactly `standard sink requires line data`; pass `events` to the unchanged binary/heading/context/line renderer, preserve every output byte and lock/I/O/`flow_of` behavior.
- `PathSink::detail`: no inputs beyond `self`; return `Documents`; no error. `wants_hit_keys` remains false. `on_doc` uses required `key`, deliberately ignores `doc`, and preserves the exact colored-key/newline/flush transformation and existing lock/I/O result.
- `CountSink::detail`: no inputs beyond `self`; return `MatchCount` iff `self.count_matches`, else `MatchingLines`; no error. `wants_hit_keys` remains false. `on_doc` requires `doc.data=MatchData::Lines(events)` or errors exactly `count sink requires line data`; sum submatch lengths in match-count mode or count `LineKind::Match` events in line-count mode; preserve exact `key:{count}\n`, flush, lock, and `flow_of` behavior.
- `QuietSink::detail`: no inputs beyond `self`; return `Documents`; no error. `wants_hit_keys` remains false. `on_doc` deliberately ignores required `key/doc`, sets `matched=true`, and returns `Stop` iff `self.stop`, otherwise `Continue`; no new errors. Remove `wants_matches`/`wants_line_text` implementations.

In `crates/cli/src/json.rs`:

```rust
impl MatchSink for JsonSink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `detail`: no inputs beyond `self`; return `FullLines`; no error. `wants_hit_keys` remains false.
- `on_doc`: `key` is the matching display key and `doc` must contain `MatchData::Lines(events)`; otherwise return exactly `JSON sink requires line data`. Replace only `doc.events` reads with `events`; preserve begin/match/context/end order, every JSON field, all counters, buffered single-lock write, newline framing, flush, existing serde/I/O/poison errors, and `Continue` output.

### Differential test-only contracts

Add these narrow types in `crates/index/tests/differential_store.rs`; they support only the one required regional-versus-whole differential and are not a reusable fixture framework:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
enum CapturedData {
    Documents,
    Lines(Vec<LineEvent>),
    Windows(Vec<MatchWindow>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedDocument {
    key: String,
    data: CapturedData,
    bytes_searched: u64,
}

struct CaptureSink {
    detail: SearchDetail,
    documents: Mutex<Vec<CapturedDocument>>,
}

struct WholeReader<'a> {
    reader: &'a SegmentedReader,
}
```

```rust
impl MatchSink for CaptureSink {
    fn detail(&self) -> SearchDetail;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `detail`: input is `self`; output is the stored request detail; no errors.
- `on_doc`: `key/doc` are one verified result; clone `MatchData::Documents`, `Lines`, or `Windows` into the matching owned `CapturedData`, copy `bytes_searched`, append one `CapturedDocument`, and return `Continue`. The test-only mutex uses `expect("capture sink lock")`; it panics on poison and otherwise returns no error. Elapsed time is deliberately excluded from the comparison.

```rust
impl DocFetcher for WholeReader<'_> {
    fn fetch_each(
        &self,
        documents: &[DocAddress],
        consume: &mut dyn FnMut(usize, DocumentBody) -> anyhow::Result<()>,
    ) -> anyhow::Result<()>;
}

impl IndexReader for WholeReader<'_> {
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

- Every method delegates its exact input/output/error to the wrapped `SegmentedReader`; `fetch_each` forwards bodies and callback results unchanged.
- Do not override `visit_candidates` or `start_candidate_batch`. The public defaults union the same candidate queries, clear address ranges, and fetch canonical whole bodies, creating an oracle that differs from the production reader only at regional-versus-whole retrieval.

### Focused verification

- [ ] In `grep.rs`, replace `fn re(p: &str) -> regex::bytes::Regex` with `fn compile_program(pattern: &str) -> PatternProgram`. Input is one test-only line-sanitized pattern; output is a one-HIR program with original ID `0`; parse/compile failures panic through `unwrap`. Update `fn match_line_col()`, `fn grep_doc_merges_per_line_and_tracks_lines()`, `fn grep_doc_context_merges_overlaps()`, `fn grep_doc_independent_before_after()`, `fn grep_doc_max_count_caps_but_drains_after_context()`, `fn grep_doc_post_cap_match_in_context_carries_submatches()`, and `fn grep_doc_eof_line_without_newline()`; each has no input, returns `()`, compiles its fixture, and preserves every existing output assertion. Replace `fn regex_matches_cannot_cross_lines()` with `fn line_safe_program_matches_cannot_cross_lines() -> ()`, using `foo[^\n]+bar` so the program satisfies the sanitized-HIR contract while preserving the cross-line/same-line assertions. Delete the two tests for removed legacy functions: `whole_document_fast_path_is_conservative_and_equivalent` and `bounded_match_lengths_exclude_unbounded_and_contextual_patterns`.
- [ ] In `store.rs`, update `fn scan_finds_matching_docs() -> ()` and `fn scan_finds_archive_members() -> ()`: each has no input/output beyond assertions; parse its existing pattern with the sole core parser, compile one ID-0 program, pass it to `scan_matching_docs`, and keep expected sorted keys unchanged.
- [ ] In `codec.rs`, update `fn parquet_named_timezone_utc_decodes() -> ()`: no input/output beyond assertions; replace its regex fixture with one parsed ID-0 `PatternProgram`, pass it to `grep_doc`, and keep all decoded timestamp/match assertions unchanged.
- [ ] In `differential_store.rs`, update `fn store_index_equals_scan_for_many_patterns() -> anyhow::Result<()>` and `fn regional_verification_matches_whole_document_scanning() -> anyhow::Result<()>`: no input; output `Ok(())` after existing assertions; for each existing oracle pattern, parse one HIR, compile one ID-0 program, and pass it to `scan_matching_docs`/`grep_doc` without changing expected events or fetch assertions.
- [ ] In `segmented.rs`, change only the body of `fn assert_matches_oracle(bucket: &Bucket, store_dir: &Path, cache_dir: &Path, patterns: &[&str], label: &str) -> anyhow::Result<()>`. Inputs retain their current bucket/path/pattern/diagnostic meanings; output remains `Ok(())` or the existing open/decode/search/assertion error. For each pattern, parse one HIR, compile one ID-0 program, and pass it to `scan_matching_docs`; keep all callers and comparison messages unchanged.
- [ ] Add `fn eligibility_programs_prevent_same_start_masking() -> ()` in `search.rs`. It has no inputs/output beyond assertions and panics on fixture compilation failure. Parse ordered patterns `\Afoo`, `(?m)^foo`, and `foo`; call `build_plans(..., SearchDetail::FullLines)` and assert original IDs `0/1/2` plus extents `Document/Lines/Bytes`; compile `SearchPrograms`; create one `WorkerCache`; search the same `b"foo"` body with `whole`, `lines`, and `regional`; assert the sole winner is respectively original pattern ID `0`, `1`, and `2`. This is the exact regression for leftmost-first same-start masking; do not run a broad program plus an ID filter.
- [ ] Add `fn max_count_keeps_earliest_union_lines_and_caps_large_exact_spans() -> ()` in `search.rs`. It has no inputs/output beyond assertions. For two small compiled patterns, reverse pattern order relative to line order and assert `max_count=Some(1)` keeps the earliest line and every match on that retained line, matching the whole-document program oracle. Separately parse `a{CANDIDATE_BLOCK_BYTES+1}`, run only `build_plans`, and assert `kind=Fallback` plus a non-`Bytes` extent; do not compile or allocate a body for that 128-KiB repetition.
- [ ] Migrate `fn bounded_file_search_matches_in_memory_across_chunks() -> ()`, `fn regional_matches_share_a_line_across_an_unfetched_zero_newline_gap() -> ()`, and `fn single_candidate_does_not_start_rayon_pool() -> ()` in `search.rs` to the new programs/caches/detail signatures. They have no inputs/output beyond assertions; preserve the chunk-boundary, absolute-line/dedup, and sequential-path assertions respectively. Do not assert the number of `try_for_each_init` initializations or equate it to Rayon thread count.
- [ ] Add `fn mixed_patterns_match_whole_document_oracle_on_giant_line() -> anyhow::Result<()>` to `differential_store.rs`. It has no input and returns `Ok(())` after comparisons. Build a corpus with one multi-megabyte single-line `giant.log`, one small line-fallback document, and one small document-fallback document. Use this ordered pattern table: `FIRSTTOKEN`, `CROSSBLOCKTOKEN`, `DUPTOKEN`, `LASTTOKEN`, `BEGIN[A-Z0-9]{20,}`, `[A-Z0-9]{20,}END`, `(?m)^LINEFALLBACK.*$`, `\ADOCFALLBACK.*`. Place `FIRSTTOKEN` at offset zero, start `CROSSBLOCKTOKEN` at `CANDIDATE_BLOCK_BYTES - 4`, place `DUPTOKEN` twice on the giant line, end `LASTTOKEN` at EOF, and place the two proof sequences elsewhere on that line; put only the line/document fallback strings in their small documents so fallback coverage does not force the giant candidate whole. Parse the ordered HIRs once. For each `Documents`, `MatchingLines`, `MatchCount`, `FullLines`, and `MatchWindows { max_bytes: 64 }`, run `search_patterns` against the production reader and `WholeReader` with separate `CaptureSink`s; sort captured documents by key and assert exact payload/`bytes_searched` equality. Compare semantic stats (`hits`, hit/candidate/total/decoded counts, and all pattern-kind counts) while asserting production retrieval includes regional work and window text never exceeds 64 bytes; do not compare intentionally different regional/whole/fetched-byte counters.
- [ ] Run `rg -n "pub fn parse_pattern" crates/core/src`; require exactly `crates/core/src/pattern.rs`. Run `rg -n "regex::|regex::bytes|bounded_match_len|can_search_as_document|grep_bytes_fast|has_line_match_fast" crates/core/src/grep.rs crates/core/src/lib.rs`; require no matches. Run `rg -n "regex::|regex::bytes" crates/index`; require no matches. Confirm the only remaining core `regex` dependency is under `[dev-dependencies]` for `pattern.rs` witness-oracle tests.
- [ ] Run `cargo test -p seagrep-core grep`.
- [ ] Run `cargo test -p seagrep-core store`.
- [ ] Run `cargo test -p seagrep-index search`.
- [ ] Run `cargo test -p seagrep-index --test differential_store mixed_patterns_match_whole_document_oracle_on_giant_line`.
- [ ] Run `cargo test -p seagrep -- --test-threads=1`; existing CLI output/JSON tests must pass unchanged.
- [ ] Run `cargo check --locked --workspace --all-targets --all-features --offline` so the public trait migration leaves the complete workspace compiling at the Task 4 boundary.
- [ ] Run `cargo clippy --locked --workspace --all-targets --all-features --offline`. Compare with the recorded pre-Task-4 inventory and require a strict subset: duplicate `parse_pattern` `dead_code`/`unreachable_pub` warnings are gone, no new warning appears, and only unchanged warnings in files outside Task 4 remain. Do not add an `allow` to silence new code.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Commit: `git add crates/core/Cargo.toml crates/core/src/codec.rs crates/core/src/grep.rs crates/core/src/lib.rs crates/core/src/store.rs crates/index/Cargo.toml crates/index/src/lib.rs crates/index/src/search.rs crates/index/tests/differential_store.rs crates/index/tests/segmented.rs crates/cli/src/printer.rs crates/cli/src/json.rs && git commit -m "feat: verify mixed patterns in one pass"`.

---

## Task 5: Keep CLI patterns separate and render bounded windows

**Files:**

- Modify `crates/cli/src/patterns.rs`: return one sanitized HIR per raw pattern with indexed errors.
- Modify `crates/cli/src/main.rs`: add `--match-window`, pass HIR slices to `search_patterns`, select typed sink detail, and print pattern stats.
- Modify `crates/cli/src/printer.rs`: extend the typed standard sink with line/window rendering and window-only binary suppression; Task 4 already migrated all non-window sinks.

### Pattern transformation contracts in `patterns.rs`

Replace `build_pattern` with:

```rust
pub(crate) fn build_patterns(
    patterns: &[String],
    fixed_strings: bool,
    word_regexp: bool,
    ignore_case: bool,
    smart_case: bool,
) -> anyhow::Result<Vec<regex_syntax::hir::Hir>>;
```

- Input patterns are required in user order and must be non-empty; error `at least one pattern is required` otherwise.
- Escape every raw pattern independently for `-F`; parse each escaped form once with `seagrep_core::parse_pattern` for smart-case literal inspection with the original index/raw value attached to errors; compute one smart-case decision across those HIRs; compose each final pattern, parse it with the same sole core parser, sanitize it, and return one HIR per input in order.
- Any parse/sanitize error is wrapped exactly `invalid pattern {ONE_BASED_INDEX} {RAW_DEBUG}: {SOURCE}`.
- Never join patterns and never stringify sanitized HIR.

```rust
fn compose_pattern(
    pattern: &str,
    word_regexp: bool,
    insensitive: bool,
) -> String;
```

- Wrap only this pattern with rg half-word boundaries when requested; prefix `(?m)` or `(?mi)`; no alternation.

```rust
fn sanitize_line_terminators(
    hir: &regex_syntax::hir::Hir,
) -> anyhow::Result<regex_syntax::hir::Hir>;
```

- Call `strip_line_terminators`; return HIR directly without stringification.

```rust
fn strip_line_terminators(
    hir: &regex_syntax::hir::Hir,
) -> anyhow::Result<regex_syntax::hir::Hir>;
```

- Recursively clone HIR; reject literal newline with `the literal '\n' is not allowed in a regex`; subtract newline from byte/Unicode classes; preserve repetition/capture/look metadata.

```rust
fn smart_case_insensitive(hirs: &[regex_syntax::hir::Hir]) -> bool;
```

- Traverse the already-parsed escaped HIRs. Return true iff at least one literal exists and none is uppercase; parsing failures have already returned from `build_patterns`, so there is no fallback.

```rust
fn collect_literal_chars(hir: &regex_syntax::hir::Hir, output: &mut Vec<char>);
```

- Keep signature/recursive transformation; rename parameter `out` to `output` only if touched to match the plan.

### CLI argument/wiring contracts in `main.rs`

Add to `SearchArgs`:

```rust
#[arg(
    long,
    value_name = "BYTES",
    value_parser = parse_positive_usize,
    conflicts_with_all = [
        "json",
        "after_context",
        "before_context",
        "context",
        "column",
        "count",
        "count_matches",
        "files_with_matches",
        "files",
        "quiet"
    ]
)]
match_window: Option<usize>,
```

Help text: `Print at most BYTES of content around the first confirmed match on each matching line.`

```rust
fn parse_positive_usize(value: &str) -> std::result::Result<usize, String>;
```

- Parse `usize`; parse errors use `ParseIntError::to_string`; zero errors `value must be greater than 0`; positive returns unchanged.

Change:

```rust
#[derive(Clone, Copy)]
struct SearchExecution<'a> {
    hirs: &'a [regex_syntax::hir::Hir],
    scope: Option<&'a Scope>,
    options: MatchOptions,
    stats_line: bool,
}
```

`pattern` is removed.

```rust
fn search_with_reopen(
    mut open: impl FnMut() -> anyhow::Result<SegmentedReader>,
    hirs: &[regex_syntax::hir::Hir],
    scope: KeyScope<'_>,
    options: MatchOptions,
    sink: &dyn MatchSink,
) -> anyhow::Result<SearchStats>;
```

- Initial and one `IndexChanged` retry both call `search_patterns`; retry behavior/error shape stays unchanged.

`execute_search` and `execute_with_discovery` signatures remain unchanged; replace `execution.pattern` use with `execution.hirs`.

```rust
fn execute_search(
    source: &S3Source,
    index: &IndexStorage,
    execution: SearchExecution<'_>,
    sink: &dyn MatchSink,
) -> anyhow::Result<SearchStats>;

fn execute_with_discovery(
    source: &S3Source,
    index: &mut IndexStorage,
    index_args: &IndexArgs,
    concurrency: usize,
    execution: SearchExecution<'_>,
    sink: &dyn MatchSink,
) -> anyhow::Result<SearchStats>;
```

- `execute_search` keeps the exact target/scope predicate, opens/reopens the same index, passes `execution.hirs`, reports excluded objects, and prints the extended plain stats line.
- `execute_with_discovery` keeps its explicit-index memory and one fallback discovery attempt; the same HIR slice/sink is reused only because `IndexMissing` occurs before any sink callback.

```rust
fn run_search(args: SearchArgs) -> anyhow::Result<bool>;
```

- Split raw patterns/target; call `build_patterns`; remove joined debug error context; calculate existing scope/options/output defaults.
- Build sinks exactly: quiet `Documents`; JSON `FullLines`; files `Documents`; count `MatchingLines`; count-matches `MatchCount`; standard `FullLines` or `MatchWindows { max_bytes }` via `RenderConfig.match_window`.
- `-m` remains in `MatchOptions` for every detail.
- Text stats line becomes:

```text
patterns={} exact_patterns={} proof_patterns={} fallback_patterns={} candidates={} total={} hits={} regional={} whole={} candidate_bytes={} decoded_bytes={}
```

### Printer contracts in `printer.rs`

Change `RenderConfig`:

```rust
pub(crate) struct RenderConfig {
    pub(crate) heading: bool,
    pub(crate) line_numbers: bool,
    pub(crate) column: bool,
    pub(crate) context_active: bool,
    pub(crate) text: bool,
    pub(crate) match_window: Option<usize>,
}
```

Generalize existing writers:

```rust
fn write_colored(
    output: &mut impl termcolor::WriteColor,
    spec: &termcolor::ColorSpec,
    bytes: &[u8],
) -> std::io::Result<()>;

fn write_text_highlighted(
    output: &mut impl termcolor::WriteColor,
    text: &[u8],
    submatches: &[SubMatch],
) -> std::io::Result<()>;
```

- Only genericize the writer; current line bytes/colors/newline behavior stays unchanged.

```rust
fn binary_nul_offset(data: &seagrep_index::MatchData<'_>) -> Option<u64>;
```

- `Lines`: first `event.offset + local NUL`; `Windows`: first `window.window_offset + local NUL`; `Documents`: `None`. Thus an unfetched NUL cannot suppress a window.

```rust
fn write_window_highlighted(
    output: &mut impl termcolor::WriteColor,
    window: &MatchWindow,
) -> std::io::Result<()>;
```

- Emit uncolored UTF-8 `…` before/after text for clipped line-window edges.
- Emit normal bytes between ordered visible ranges.
- Highlight visible witness bytes with `match_spec`.
- Emit one highlighted `…` immediately before/after a visible witness for each clipped witness edge.
- Emit one additional highlighted trailing `…` when `canonical_span_known=false`; if the right edge is also clipped, two highlighted ellipses are intentional and distinguish the two facts by count.
- Emit one terminal newline. Never exceed `max_bytes` in `window.text`; markers do not count toward the content budget.

```rust
fn write_window(
    output: &mut impl termcolor::WriteColor,
    key: &str,
    window: &MatchWindow,
    config: &RenderConfig,
) -> std::io::Result<()>;
```

- Use the existing heading/path and optional line-number prefixes; window mode has no context separator and no column because Clap rejects it. Call `write_window_highlighted`. Do not invent a new byte-offset column; `window_offset` remains typed metadata and powers binary offsets/tests.

Use these exact changed implementations in `printer.rs`:

```rust
impl MatchSink for StandardSink {
    fn detail(&self) -> SearchDetail;

    fn wants_hit_keys(&self) -> bool;

    fn on_doc(&self, key: &str, doc: &DocResult<'_>) -> anyhow::Result<SinkFlow>;
}
```

- `StandardSink::detail`: `MatchWindows { max_bytes }` from `config.match_window`, else `FullLines`. `wants_hit_keys=false`. `on_doc` runs `binary_nul_offset` on `doc.data`, preserves the binary notice/heading/context state machine, renders ordered lines in default mode or ordered windows through `write_window` in window mode, and returns the existing `flow_of` result. A detail/data mismatch errors `standard sink received incompatible search data`; writer/lock errors propagate unchanged.

### Focused verification

- [ ] Replace `patterns.rs::compose_matrix` with `fn builds_one_ordered_hir_per_pattern()`; assert two inputs remain two HIRs and compiled matches preserve IDs/order.
- [ ] Keep fixed/smart-case/newline tests, changing only HIR/program construction. Add an assertion for exact 1-based raw-pattern error context.
- [ ] Add a `main.rs` parse table for valid `--match-window 256`, zero rejection, and each listed conflict including `--column`.
- [ ] Add `printer.rs::window_render_marks_each_clip_kind()` using `termcolor::Buffer::ansi`; assert exact content width, plain edge-marker bytes, ANSI-delimited highlighted witness/clip-marker bytes, and no default-line rendering change.
- [ ] Add `printer.rs::window_binary_check_uses_absolute_fetched_offset()`; place NUL inside/outside fetched window and assert only inside returns `window_offset + local_position`.
- [ ] Run `cargo test -p seagrep patterns`.
- [ ] Run `cargo test -p seagrep printer`.
- [ ] Run `cargo test -p seagrep -- --test-threads=1`.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Commit: `git add crates/cli/src/patterns.rs crates/cli/src/main.rs crates/cli/src/printer.rs && git commit -m "feat: add bounded multi-pattern output"`.

---

## Task 6: Update user guidance and run the exact gates

**Files:**

- Modify `README.md`: document repeated `-e` one-pass behavior, `--match-window`, conflicts, giant-line/default-output distinction, and extended stats.
- Modify `skills/seagrep/SKILL.md`: integrate—not overwrite—the existing unstaged cost-model paragraph and replace obsolete serial/preflight advice.
- No production Rust file changes are allowed in this task unless a verification gate exposes a real defect; fix such a defect in its owning earlier task and rerun that task's focused test first.

### Exact README changes

- Add `--match-window BYTES` to the flag summary as `bounded match-centered line preview`.
- Add one example with repeated `-e` and `--match-window 512` over logs.
- State: repeated `-e` values are planned together in one segment/posting/pack pass; default/JSON still print exact complete matching lines; `--match-window` is the explicit bounded alternative for giant structured rows; it conflicts with JSON, context, column, counts, files, and quiet.
- State that `--stats` now reports pattern exact/proof/fallback categories before existing counters.

### Exact skill changes

- Flag map: add `--match-window N  bounded match-centered content per matching line`.
- Cost model: preserve the current gram-commonness examples and case-sensitivity advice. Remove only the sentence instructing `-c` preflight as an engine optimization; replace it with `Keep --stats visible while timing broad sweeps, then scope or sharpen only when the evidence calls for it.`
- Turn economy: replace shell batching/serial-query guidance with one repeated-`-e` command. State that the engine shares index and pack work across those patterns.
- Investigation recipe: replace the broad `-c -e 'error|...'` preflight with repeated `-e error -e exception -e fatal -e timeout --match-window 512 --stats`; reserve default full lines/context for the final narrow ID/service query.
- Do not include benchmark patterns, bucket names beyond existing generic examples, or claims about a fixed warm latency.

### Regression and performance gates

The oracle is an independent source-value scan of the existing local copy of the same 84 Parquet objects. For these eight ASCII, prefix-disjoint expressions it is count-equivalent to scanning Seagrep's canonical JSON rows: every accepted byte is preserved inside JSON string values, the pinned primitive/list/struct schema names match none of the prefixes, and JSON field names, escaping, and delimiters can neither create nor split a match. Its canonical output is UTF-8 TSV sorted by name, one `name<TAB>decimal<LF>` row for every pattern followed by `union`. The prior `fe2856ca...` artifact is not a valid gate for this workload: it hashes raw findings from 19 boundary-aware expressions, not counts from these eight exact expressions.

Run the following block as one `zsh` subshell. It verifies the remote inventory including ETags, verifies that the local oracle corpus has the same key/size inventory, derives the exact oracle counts, performs cold and warm timing with an isolated cache, compares indexed per-pattern and union counts byte-for-byte with the oracle, then runs both Claude comparison prompts. The patterns deliberately mix finite-exact and finite-proof forms; neither hash may enter production code.

```zsh
(
setopt ERR_EXIT NO_UNSET PIPE_FAIL

benchmark_bucket=open-swe-traces
benchmark_region=us-east-2
benchmark_profile=speedtrain
benchmark_inventory_hash=9c5ed518fc369cc3d6f036103f59066c2646c5fa1a123d1110682fbe61113ff2
benchmark_size_hash=5d0b283c0036bb161fd33f817b7382f152f9a0d91e29bf86f71ff9c831850a67
benchmark_oracle_hash=fe9cd7d99f2a6a6757975d8e6a30bc77a163bde495274a6ab65ffe81b41a1a54
oracle_data=/private/tmp/open-swe-traces.VH5GcG

benchmark_names=(
  anthropic_key
  aws_akia
  aws_asia
  github_pat
  github_token
  google_api_key
  private_key_block
  slack_token
)
benchmark_regexes=(
  'sk-ant-[A-Za-z0-9_-]{24,}'
  'AKIA[A-Z0-9]{16}'
  'ASIA[A-Z0-9]{16}'
  'github_pat_[A-Za-z0-9]{22}_[A-Za-z0-9]{59}'
  'gh[pousr]_[A-Za-z0-9]{36}'
  'AIza[0-9A-Za-z_-]{35}'
  '-----BEGIN (RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY( BLOCK)?-----'
  'xox[baprs]-[0-9A-Za-z-]{10,}'
)
benchmark_patterns=()
for benchmark_regex in "${benchmark_regexes[@]}"; do
  benchmark_patterns+=("-e=$benchmark_regex")
done

inventory_hash=$(
  AWS_PROFILE="$benchmark_profile" aws s3api list-objects-v2 \
    --region "$benchmark_region" \
    --bucket "$benchmark_bucket" \
    --query 'Contents[?starts_with(Key, `.seagrep`) == `false`].[Key,Size,ETag]' \
    --output text |
    LC_ALL=C sort |
    shasum -a 256 |
    awk '{print $1}'
)
test "$inventory_hash" = "$benchmark_inventory_hash"

remote_size_hash=$(
  AWS_PROFILE="$benchmark_profile" aws s3api list-objects-v2 \
    --region "$benchmark_region" \
    --bucket "$benchmark_bucket" \
    --query 'Contents[?starts_with(Key, `.seagrep`) == `false`].[Key,Size]' \
    --output text |
    LC_ALL=C sort |
    shasum -a 256 |
    awk '{print $1}'
)
test "$remote_size_hash" = "$benchmark_size_hash"

oracle_files=("${(@f)$(rg --files -g '*.parquet' "$oracle_data" | LC_ALL=C sort)}")
test "${#oracle_files[@]}" -eq 84
local_size_hash=$(
  while IFS= read -r oracle_file; do
    oracle_key=${oracle_file#"$oracle_data"/}
    printf '%s\t%s\n' "$oracle_key" "$(stat -f '%z' "$oracle_file")"
  done < <(printf '%s\n' "${oracle_files[@]}") |
    LC_ALL=C sort |
    shasum -a 256 |
    awk '{print $1}'
)
test "$local_size_hash" = "$benchmark_size_hash"
test "$(python3 -c 'import pyarrow; print(pyarrow.__version__)')" = 24.0.0

oracle_counts=$(mktemp /tmp/seagrep-mixed-oracle-counts.XXXXXX)
python3 - "${oracle_files[@]}" >"$oracle_counts" <<'PY'
import re
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

PATTERNS: dict[str, str] = {
    "anthropic_key": r"sk-ant-[A-Za-z0-9_-]{24,}",
    "aws_akia": r"AKIA[A-Z0-9]{16}",
    "aws_asia": r"ASIA[A-Z0-9]{16}",
    "github_pat": r"github_pat_[A-Za-z0-9]{22}_[A-Za-z0-9]{59}",
    "github_token": r"gh[pousr]_[A-Za-z0-9]{36}",
    "google_api_key": r"AIza[0-9A-Za-z_-]{35}",
    "private_key_block": r"-----BEGIN (RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY( BLOCK)?-----",
    "slack_token": r"xox[baprs]-[0-9A-Za-z-]{10,}",
}
PROGRAM = re.compile("|".join(f"(?P<{name}>{pattern})" for name, pattern in PATTERNS.items()))
ARROW_PROGRAM = "|".join(PATTERNS.values())

counts: dict[str, int] = {name: 0 for name in PATTERNS}
union = 0
for path in sys.argv[1:]:
    parquet = pq.ParquetFile(path)
    for row_group in range(parquet.num_row_groups):
        table = parquet.read_row_group(row_group)
        for column in table.column_names:
            pending: list[pa.Array | pa.ChunkedArray] = [table.column(column)]
            while pending:
                values = pending.pop()
                if isinstance(values, pa.ChunkedArray):
                    pending.extend(reversed(values.chunks))
                    continue
                value_type = values.type
                if pa.types.is_list(value_type) or pa.types.is_large_list(value_type):
                    pending.append(values.flatten())
                    continue
                if pa.types.is_struct(value_type):
                    for field in reversed(range(value_type.num_fields)):
                        pending.append(values.field(field))
                    continue
                if not (pa.types.is_string(value_type) or pa.types.is_large_string(value_type)):
                    continue
                mask = pc.match_substring_regex(values, ARROW_PROGRAM)
                if pc.any(mask).as_py():
                    for text in values.filter(mask).to_pylist():
                        for matched in PROGRAM.finditer(text):
                            name = matched.lastgroup
                            if name is None:
                                raise RuntimeError("oracle match has no pattern name")
                            counts[name] += 1
                            union += 1

for name in sorted(counts):
    print(f"{name}\t{counts[name]}")
print(f"union\t{union}")
PY
test "$(shasum -a 256 "$oracle_counts" | awk '{print $1}')" = "$benchmark_oracle_hash"

benchmark_cache=$(mktemp -d /tmp/seagrep-mixed-cache.XXXXXX)
XDG_CACHE_HOME="$benchmark_cache" AWS_PROFILE="$benchmark_profile" \
  /usr/bin/time -l -o /tmp/seagrep-mixed-cold.time \
  ./target/release/seagrep "${benchmark_patterns[@]}" \
  --match-window 256 -m 1 --stats --region "$benchmark_region" \
  "s3://$benchmark_bucket" \
  >/tmp/seagrep-mixed-cold.out 2>/tmp/seagrep-mixed-cold.err
cold_seconds=$(awk '/ real / {print $1}' /tmp/seagrep-mixed-cold.time)
cold_rss=$(awk '/maximum resident set size/ {print $1}' /tmp/seagrep-mixed-cold.time)
cold_stdout=$(wc -c </tmp/seagrep-mixed-cold.out | awk '{print $1}')
test -n "$cold_seconds"
test -n "$cold_rss"
test -n "$cold_stdout"
awk -v seconds="$cold_seconds" 'BEGIN { exit !(seconds <= 60) }'
test "$cold_rss" -le 314572800
test "$cold_stdout" -le 65536

XDG_CACHE_HOME="$benchmark_cache" AWS_PROFILE="$benchmark_profile" \
  /usr/bin/time -l -o /tmp/seagrep-mixed-warm.time \
  ./target/release/seagrep "${benchmark_patterns[@]}" \
  --match-window 256 -m 1 --stats --region "$benchmark_region" \
  "s3://$benchmark_bucket" \
  >/tmp/seagrep-mixed-warm.out 2>/tmp/seagrep-mixed-warm.err
warm_seconds=$(awk '/ real / {print $1}' /tmp/seagrep-mixed-warm.time)
test -n "$warm_seconds"
awk -v seconds="$warm_seconds" 'BEGIN { exit !(seconds <= 15) }'

parity_cache=$(mktemp -d /tmp/seagrep-mixed-parity-cache.XXXXXX)
engine_counts=$(mktemp /tmp/seagrep-mixed-engine-counts.XXXXXX)
parity_errors=$(mktemp /tmp/seagrep-mixed-parity-errors.XXXXXX)
for (( pattern_index = 1; pattern_index <= ${#benchmark_names[@]}; pattern_index++ )); do
  parity_status=0
  parity_output=$(
    XDG_CACHE_HOME="$parity_cache" AWS_PROFILE="$benchmark_profile" \
      ./target/release/seagrep \
      "-e=${benchmark_regexes[$pattern_index]}" \
      --count-matches --color never --region "$benchmark_region" \
      "s3://$benchmark_bucket" \
      2>>"$parity_errors"
  ) || parity_status=$?
  test "$parity_status" -eq 0 || test "$parity_status" -eq 1
  match_count=$(printf '%s\n' "$parity_output" | awk -F: '{ matches += $NF } END { printf "%.0f\n", matches }')
  printf '%s\t%s\n' "${benchmark_names[$pattern_index]}" "$match_count" >>"$engine_counts"
done
union_status=0
union_output=$(
  XDG_CACHE_HOME="$parity_cache" AWS_PROFILE="$benchmark_profile" \
    ./target/release/seagrep "${benchmark_patterns[@]}" \
    --count-matches --color never --region "$benchmark_region" \
    "s3://$benchmark_bucket" \
    2>>"$parity_errors"
) || union_status=$?
test "$union_status" -eq 0
union_count=$(printf '%s\n' "$union_output" | awk -F: '{ matches += $NF } END { printf "%.0f\n", matches }')
printf 'union\t%s\n' "$union_count" >>"$engine_counts"
test "$(shasum -a 256 "$engine_counts" | awk '{print $1}')" = "$benchmark_oracle_hash"
diff -u "$oracle_counts" "$engine_counts"

agent_question='any real api keys or tokens in s3://open-swe-traces? about to finetune on it. speedtrain profile, us-east-2'
with_seagrep_prompt="Use ./target/release/seagrep and read-only commands to answer this question with concrete findings, not a plan: $agent_question Do not use another seagrep binary and do not edit files."
without_seagrep_prompt="Do not use seagrep in any form. Use read-only commands to answer this question with concrete findings, not a plan: $agent_question Do not edit files."

XDG_CACHE_HOME="$benchmark_cache" AWS_PROFILE="$benchmark_profile" AWS_REGION="$benchmark_region" \
  /usr/bin/time -p -o /tmp/seagrep-agent.time \
  claude --safe-mode --no-session-persistence --model claude-fable-5 \
  --permission-mode dontAsk --tools Bash \
  --allowed-tools 'Bash(./target/release/seagrep *)' \
  --append-system-prompt-file skills/seagrep/SKILL.md \
  --print "$with_seagrep_prompt" \
  >/tmp/seagrep-agent.out 2>/tmp/seagrep-agent.err
agent_seconds=$(awk '/^real / {print $2}' /tmp/seagrep-agent.time)
test -n "$agent_seconds"
awk -v seconds="$agent_seconds" 'BEGIN { exit !(seconds <= 120) }'
test -s /tmp/seagrep-agent.out

AWS_PROFILE="$benchmark_profile" AWS_REGION="$benchmark_region" \
  /usr/bin/time -p -o /tmp/no-seagrep-agent.time \
  claude --safe-mode --no-session-persistence --model claude-fable-5 \
  --permission-mode bypassPermissions --tools Bash \
  --disallowed-tools 'Bash(*seagrep*)' \
  --print "$without_seagrep_prompt" \
  >/tmp/no-seagrep-agent.out 2>/tmp/no-seagrep-agent.err
test -s /tmp/no-seagrep-agent.out
)
```

- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo test --workspace` and require zero failures.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings` and require zero warnings.
- [ ] Run `cargo build --release -p seagrep`.
- [ ] With the existing parity MinIO fixture running, run `python3 scripts/rg-parity/run.py`; require every case to pass and do not create a new harness.
- [ ] Run the exact live-gate subshell above; require every `test`, `awk`, and `diff` to exit zero. The Seagrep agent is the only agent with a time threshold; the no-Seagrep run is the comparison control.
- [ ] Confirm `git diff --check` is clean and `git status --short` contains only intentional implementation/doc changes plus the now-integrated skill edit.
- [ ] Commit docs/skill without staging unrelated files: `git add README.md skills/seagrep/SKILL.md && git commit -m "docs: teach one-pass bounded search"`.

## Completion Criteria

- One repeated-`-e` invocation parses and plans every pattern independently.
- Each segment, unique term block, posting verification block, physical pack block, and lazy adjacent block is read/decompressed at most once per successful query batch.
- Finite proof construction is formal, resource-bounded, and never required for correctness.
- Unsafe line/document patterns cannot run against an artificial regional boundary.
- Default and JSON output remain exact; bounded windows are explicit, content-bounded, and carry exact accepted witnesses.
- All focused/workspace/parity/performance gates above pass with no benchmark-specific production behavior.
