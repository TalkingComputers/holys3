# Multi-Pattern Regional Search Design

Status: proposed for review. This is an architecture decision, not the implementation plan.

## Objective

Make a mixed repeated-pattern search complete in one CLI invocation and fast enough for an agent to finish in one to two minutes, without changing Seagrep's exact-search contract or adding workload-specific rules.

The engine must:

- plan every `-e` pattern independently;
- fetch each term, posting block, pack block, and document region at most once per query batch;
- regionally verify unbounded regexes when a finite accepting witness can be proven;
- retain exact whole-document or whole-line behavior when no proof exists;
- offer an explicit bounded-output mode for giant lines;
- keep local, S3, trigram, and sparse search on the same path.

## Measured Failure

The CLI currently converts repeated `-e` values into one alternation. `search_streaming` parses that alternation once and computes one `bounded_match_len`. One unbounded branch therefore changes the bound for every branch to `None`.

For Parquet trajectories projected as one large JSON line, `None` selects line-length slack. A single matching line can be hundreds of megabytes, so candidate expansion covers the document, regional coverage crosses the whole-document threshold, and verification materializes the row. Standard output then fetches and prints that entire line. The tested bounded queries finish in seconds; the mixed query is dominated by the one unbounded branch and oversized output.

Running one CLI command per pattern avoids bound poisoning but repeats segment traversal, term reads, posting reads, pack reads, decompression, and process startup. That is a workflow workaround, not an engine fix.

## Success Criteria

### Exactness

- Candidate planning remains a superset and the original regex remains the authority.
- Default text and JSON output remain byte-for-byte compatible with the current behavior.
- Existing `-q`, `-l`, `-c`, `--count-matches`, `-m`, context, binary detection, line numbers, byte offsets, and repeated-pattern order retain their current semantics. The new clipped mode has the explicit witness and bounded binary contract below.
- A proof optimization that cannot be established returns to the existing exact path. It never substitutes a minimum length, clamps a quantifier, or treats a partial regex as the verifier.
- No candidate or verifier contains credential names, RCAEval assumptions, bucket paths, or benchmark-specific literals.

### Performance

On the pinned 84-object app-log corpus and the fixed mixed-pattern set:

- cold release CLI: at most 60 seconds;
- warm release CLI: at most 15 seconds;
- peak RSS: at most 300 MiB;
- bounded-output stdout: at most 64 KiB;
- full agent run after the index exists: at most 120 seconds;
- result counts equal the whole-document oracle for every individual pattern and their union.

The benchmark inventory and oracle hashes live in benchmark notes, not production code. The current inventory SHA-256 is `9c5ed518fc369cc3d6f036103f59066c2646c5fa1a123d1110682fbe61113ff2`; the current canonical per-pattern-plus-union count SHA-256 is `fe9cd7d99f2a6a6757975d8e6a30bc77a163bde495274a6ab65ffe81b41a1a54`.

## Non-Goals

- No index-format rebuild.
- No approximate regex engine.
- No native C or C++ matcher.
- No arbitrary discontiguous regex stream.
- No implicit clipping of existing output.
- No rules command, secret taxonomy, or specialized detector.

## Library Decision

### Adopt `regex-automata` 0.4.16 directly

The workspace locks `regex-automata` 0.4.16 directly, `regex` 1.13.1, and `regex-syntax` 0.8.11. Their relevant public low-level APIs are:

- `meta::Regex::builder().build_many_from_hir(&hirs)` compiles the already-sanitized HIRs into one multi-pattern verifier. Configure `WhichCaptures::Implicit` because Seagrep needs only each overall match span, not user capture groups.
- `Match::pattern`, `Match::start`, and `Match::end` preserve pattern identity and byte spans.
- `Regex::create_cache` and `Regex::search_with` provide scratch owned by each `try_for_each_init` job state instead of contending on the internal cache pool. The contract is job-local, not one cache per operating-system thread.
- `util::iter::Searcher` provides correct non-overlapping iteration, including empty-match advancement.
- `dfa::dense`, `nfa::thompson`, and `dfa::Automaton` expose the state graph needed for a formal finite-witness proof.

`regex-syntax` 0.8.11 remains the only parser. CLI transforms produce one HIR per input pattern. Query planning, proof analysis, and verifier compilation all consume those same HIR values; no component reparses a joined string.

Official APIs:

- <https://docs.rs/regex-automata/0.4.16/regex_automata/meta/struct.Regex.html>
- <https://docs.rs/regex-automata/0.4.16/regex_automata/dfa/trait.Automaton.html>
- <https://docs.rs/regex-syntax/0.8.11/regex_syntax/hir/struct.Properties.html>

### Do not adopt the alternatives

| Library                | Decision             | Reason                                                                                                                                                                     |
| ---------------------- | -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `grep-searcher` 0.1.17 | Reject               | It owns a contiguous reader starting at byte zero, has no regional base-offset API, and its memory lower bound remains the longest line.                                   |
| `grep-printer` 0.3.1   | Reject               | `only_matching` reshapes already-buffered line output; it does not prevent the giant line fetch. Its preview is line-prefix based, not match-centered.                     |
| `grep-regex` 0.1.14    | Reject               | `build_many` joins alternatives and does not expose per-pattern HIR, identity, proof spans, or regional planning.                                                          |
| `regex-cursor` 0.1.5   | Reject               | Its docs call it a prototype, it requires backtracking into retained chunks, and its cursor transitions cannot propagate an S3 fetch error.                                |
| `aho-corasick` 1.1.4   | No direct dependency | It is excellent for literal sets, but `regex-automata` already uses literal prefilters and Seagrep's index is the first-stage prefilter. It cannot verify general regexes. |
| VectorScan/Hyperscan   | Reject               | Native build complexity and different regex, greediness, ordering, and start-of-match semantics make it unsuitable as the correctness engine.                              |

## Pattern Model

The CLI keeps patterns separate through the entire search:

```rust
pub struct PatternPlan {
    pub hir: regex_syntax::hir::Hir,
    pub query: seagrep_query::Query,
    pub bounds: MatchBounds,
}

pub struct MatchBounds {
    pub exact_bytes: Option<usize>,
    pub witness: Option<MatchWitness>,
    pub fallback: FallbackExtent,
}

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

pub enum ProofDirection {
    Forward,
    Reverse,
}

pub enum SearchExtent {
    Bytes { span: usize },
    Lines,
    Document,
}

pub enum FallbackExtent {
    Lines,
    Document,
}
```

`exact_bytes` is the existing finite maximum match length after rejecting line-sensitive and look-around HIR. It is valid for exact match endpoints.

`witness` is `Exact` when the finite maximum is sufficient. `Proven` is a finite prefix or suffix that is itself accepted by an otherwise unbounded original pattern. It proves match existence and matching-line identity, but it does not claim to be the canonical greedy endpoint of the full document match.

`fallback` is `Lines` for line anchors, word boundaries, and newline-excluding unbounded patterns. It is `Document` for absolute text anchors or any construct whose truth depends on document context. This prevents a sliced line from turning its artificial boundary into `\A` or `\z`.

The selected regional span depends on the requested result:

| Result                         | Regional span                                                    |
| ------------------------------ | ---------------------------------------------------------------- |
| document existence, `-q`, `-l` | `witness`                                                        |
| matching-line count, `-c`      | `witness`                                                        |
| explicit match window          | `witness`                                                        |
| default or JSON full lines     | `witness`, followed by exact lazy line fetch and re-verification |
| exact match count              | `exact_bytes`; otherwise whole-line or whole-document path       |

The planner maps a selected finite bound to `SearchExtent::Bytes`. When the required bound is absent, it uses that pattern's `FallbackExtent`; a `Query::All` selection becomes `SearchExtent::Document`. Exact fallback applies only to that pattern and does not poison other patterns.

`-m` is orthogonal to `SearchDetail`: it caps matching lines for documents, full lines, and windows, while `--count-matches -m` still uses exact match spans.

## Formal Finite-Witness Proof

`minimum_len` is not a proof. For example, the minimum length of `foo.*bar` is finite while the distance to `bar` is unbounded. String inspection and quantifier special cases are also rejected.

For an unbounded, non-empty HIR without look assertions or newline matches:

1. Compile the HIR to a capture-free Thompson NFA.
2. Determinize it to an anchored `MatchKind::All` dense DFA. Limit each stored DFA to 8 MiB, determinization scratch to 16 MiB, and retained sparse proof automata to 32 MiB across the complete query; forward and reverse dense automata are built and dropped sequentially.
3. Start from the anchored DFA start state.
4. Treat a state as accepting when its end-of-input transition is a match. This removes the DFA searcher's one-byte match delay and asks the language question directly.
5. Traverse byte transitions, excluding dead and quit states. Stop traversal at accepting states.
6. Remove non-accepting states that cannot reach acceptance.
7. If the remaining non-accepting graph contains a cycle, no finite bound exists: the cycle can delay the first accepted prefix arbitrarily.
8. Otherwise, the longest path from the start state to acceptance is the exact finite prefix proof.
9. Build a reverse Thompson NFA, enter it with `start_state_reverse`, traverse haystack bytes backward with `next_state`, and apply the same end-of-input acceptance test. This produces a suffix proof; keep the shorter direction.

Examples:

| Pattern                        | Proof                                                               |
| ------------------------------ | ------------------------------------------------------------------- |
| `[A-Z0-9]{20,}`                | forward, 20 bytes                                                   |
| `foo.*`                        | forward, 3 bytes                                                    |
| `.*token`                      | reverse, 5 bytes                                                    |
| `foo.*bar`                     | none                                                                |
| <code>foo&#124;bar.*baz</code> | none because one accepting branch has an unbounded pre-accept cycle |

The graph visits one representative byte from each DFA byte class and retains only unique state edges. After analysis, convert the chosen dense DFA to a sparse DFA and retain it with the proof; drop the construction graph and the other direction. The proof compiler is an optimization only. A look assertion, empty match, quit state, DFA build limit, graph limit, or proof larger than one candidate block produces `None` and records the fallback in search stats. The meta verifier still compiles normally, so proof resource limits cannot reject a valid user regex.

This is language analysis, not a replacement verifier. Every regional hit is first produced by the original HIR compiled through `meta::Regex`. For a proof-only hit, traverse the retained proof DFA from the concrete meta-match start in the forward direction or from its end in the reverse direction until end-of-input acceptance. The resulting absolute range is the exact accepted witness used by bounded output; an artificial region boundary never becomes the window anchor merely because it is the buffer edge.

## Search Detail Contract

Replace the interacting sink booleans with one explicit capability:

```rust
pub enum SearchDetail {
    Documents,
    MatchingLines,
    MatchCount,
    MatchWindows { max_bytes: usize },
    FullLines,
}
```

`PathSink` and `QuietSink` request `Documents`. Line count requests `MatchingLines`; match count requests `MatchCount`. Standard and JSON output request `FullLines` unless the new window flag is active.

`MatchSink::wants_hit_keys` remains independent because it controls only result-key retention.

The enum makes the exactness decision visible before candidate planning. It prevents an output sink from accidentally selecting an unsafe proof path through a combination of booleans.

## One-Pass Candidate Planning

The index reader accepts all pattern queries together:

```rust
pub struct CandidatePlan<'a> {
    pub query: &'a seagrep_query::Query,
    pub extent: SearchExtent,
}
```

For each segment:

1. Collect the union of query grams across every plan.
2. Read local or sparse-remote term values once per unique gram.
3. Bind every query to the shared term-value map.
4. Collect the union of required posting blocks.
5. Fetch, hash-check, and decode each posting block once.
6. Evaluate every query independently with its own block expansion span.
7. Convert selected block IDs to per-document candidate ranges.
8. Union and coalesce ranges across patterns, then emit each document once.

The current pure query evaluator remains set algebra over sorted posting IDs. Its API is widened from one bound query to a slice; index storage and posting encoding do not change. After the shared posting fetch, selections are evaluated and folded into the per-document range map one pattern at a time so the reader does not retain one full candidate vector per pattern.

`Query::All` and a selection whose expanded ranges exceed the existing range-count or coverage limits retain whole-document behavior. A non-finite witness changes only that pattern's selected ranges to its exact `Lines` or `Document` fallback; it does not change the finite spans of the other plans.

The exactness link between the proof and the index is direct: the proof prefix or suffix is itself a complete string in the regex language. The query planner's required grams must therefore occur inside that witness just as they must occur inside any other complete match. Expanding that pattern's posting blocks by the proof span cannot separate the witness grams or remove the match.

## Per-Range Fetch Extents

One global `bounded_len` cannot represent mixed patterns. Runtime candidate addresses instead carry range-local extents:

```rust
pub struct CandidateRange {
    pub blocks: std::ops::Range<u32>,
    pub extent: SearchExtent,
}
```

`Bytes` expands the block-aligned byte range by `span - 1` bytes on both sides and selects the regional program. `Lines` extends to exact line boundaries and selects the line program. `Document` discards ranges and selects the whole program. Overlapping byte ranges merge after expansion; overlapping finite ranges use the larger span; a line range dominates overlapping byte ranges; a document extent dominates the document. Disjoint byte and line ranges retain their extent tag.

The 512-range and 50-percent-coverage fallbacks are applied after expansion and merging. Pack blocks are then grouped, range-fetched, decompressed, and cached once. Discontiguous regions stay separate and are never concatenated across a gap.

No index bytes change. `CandidateRange` exists only in the in-memory `DocAddress` produced by the reader. Fetched `DocumentRegion` values retain a regional/full tag derived from the extent so an unsafe pattern is never evaluated on a truncated finite region for an exact match-count request.

Pack work is two-phase across the complete candidate batch, before per-document verification starts:

1. Translate every document's tagged logical ranges into physical pack-block IDs.
2. Union IDs and coalesce compressed byte ranges across all documents.
3. Fetch, hash-check, and decompress each unique block into one batch-scoped shared byte map.
4. Construct document-region views from that map and feed them to the Rayon workers.

Lazy full-line and window expansion uses the same batch map. A missing adjacent block is loaded through a keyed single-flight entry, so concurrent jobs cannot fetch or decompress it twice. A fetch batch closes at 16,384 documents or 64 MiB of unique decoded physical pack blocks spanned by its candidate documents, whichever comes first. Documents whose physical span exceeds the byte limit are emitted alone. A large regional-to-whole fallback reuses already-ready blocks, stores missing decoded blocks in batch-local file-backed entries, and returns the existing file-backed body. The map is dropped when that batch completes. This replaces independent per-document `fetch_regions_parallel` calls rather than wrapping them in another loop.

## Multi-Pattern Verification

Compile three immutable ordered programs from the same pattern table:

- the whole program contains every pattern;
- the line program contains patterns whose selected extent is `Bytes` or `Lines`;
- the regional program contains only patterns whose selected extent is `Bytes`.

Each subset preserves relative input order and original pattern IDs. All three use default leftmost-first semantics and implicit captures only, preserving the current ordered-alternation behavior while retaining overall match spans. Program eligibility is chosen before searching: whole documents use the whole program, exact full-line regions use the line program, and truncated finite regions use the regional program. An ineligible earlier alternative therefore cannot win a combined search and hide an eligible match before post-filtering.

Each `try_for_each_init` job state owns one cache per compiled program. It iterates matches with `util::iter::Searcher` and `search_with`; the compiled programs are shared. This removes internal pool contention and avoids compiling or cloning a regex per document. Rayon may create more job states than operating-system worker threads, so cache construction and tests make no per-thread cardinality claim.

A whole-document selection dominates every range for that document. A full-line range dominates only overlapping regional ranges. Disjoint regional and full-line ranges may coexist, but their physical pack-block requests are unioned before I/O. Run only the program eligible for each body or region. Candidate completeness guarantees that a matching pattern's own query selected the document and its required region.

Regional results are deduplicated by absolute byte span and then by line where the output contract is line-based. Existing newline-block prefixes supply exact line numbers across unfetched gaps.

## Result Data

`DocResult` carries one typed result instead of overloading empty `LineEvent.text` values:

```rust
pub enum MatchData<'a> {
    Documents,
    Lines(&'a [seagrep_core::LineEvent]),
    Windows(&'a [MatchWindow]),
}

pub struct MatchWindow {
    pub line: u64,
    pub line_offset: u64,
    pub window_offset: u64,
    pub text: bytes::Bytes,
    pub matches: Vec<WindowMatch>,
    pub left_clipped: bool,
    pub right_clipped: bool,
}

pub struct WindowMatch {
    pub witness: std::ops::Range<u64>,
    pub visible: std::ops::Range<usize>,
    pub left_clipped: bool,
    pub right_clipped: bool,
    pub canonical_span_known: bool,
}
```

`line_offset` is the absolute start of the logical line; `window_offset` is the absolute start of `text`. `witness` is the complete absolute accepted range even when it is longer than the output budget; `visible` is only its intersection with `text`. The per-match clip flags prevent a partial visible range from masquerading as a complete occurrence. Whole-document window construction borrows the owned `bytes::Bytes` and creates `text` with zero-copy slices; it does not erase ownership to `&[u8]` before slicing. The newline block table identifies the preceding line boundary without reading the intervening giant line. `canonical_span_known=false` means the accepted witness is real but the full-line leftmost-first start or greedy end may differ.

## Bounded Match Windows

Add an opt-in standard-output flag:

```text
--match-window <BYTES>
```

It prints at most `BYTES` content bytes for each matching line while keeping a confirmed match visible. It is incompatible with JSON, `-A`, `-B`, `-C`, `--column`, counts, file lists, and quiet mode. `BYTES=0` is invalid. Existing default output is unchanged.

For a finite exact match, the window is centered around that match when space permits. For a proof-only unbounded match, the accepted witness anchors the window. The output separately marks clipped window edges, clipped witness edges, and a witness whose canonical full-line span is unknown. Detection, line identity, line number, complete absolute witness range, and window byte offset remain exact. The visible highlight can be only part of that witness; this explicit clipped mode does not claim the same leftmost-first start or greedy end as a complete-line search.

The engine uses already-fetched candidate bytes when they contain the full requested window. Otherwise it asks the pack reader for the missing adjacent byte range, which is coalesced with other windows for that document. It never fetches the enclosing giant line merely to render a window.

One window is emitted per matching line, anchored by the lowest absolute accepted-witness offset produced for that line. This retains Seagrep's line-oriented result model and prevents a line with thousands of matches from producing unbounded output. `-m` still limits matching lines per document.

Without `--text`, binary suppression examines the fetched window only. A NUL inside the window emits the existing notice with its absolute offset; an unfetched NUL elsewhere on the line does not suppress the bounded window. Full-line modes retain their existing binary behavior.

## Default Full-Line Output

Default and JSON output preserve exact current behavior:

1. Regionally discover the matching line with a finite proof when available.
2. Fetch the complete matching line and requested context only for a true hit.
3. Run the ordered line program over the fetched line.
4. Emit the existing `LineEvent` stream.

A genuinely matching 755 MiB one-line document therefore remains expensive when the user explicitly requests its complete line. The bounded-output flag is the intentional way to avoid that cost; silently clipping default output would be an accuracy regression.

## Module Boundaries

- `crates/cli` transforms each raw `-e` value into one sanitized HIR, selects `SearchDetail`, validates `--match-window`, and renders typed results.
- `crates/core/src/pattern.rs` is a new focused module for `MatchBounds`, DFA proof analysis, and multi-pattern program construction. It depends only on regex libraries and has no index or I/O knowledge.
- `crates/core/src/grep.rs` retains line assembly and exact rg semantics, consuming match spans from the compiled program instead of owning pattern parsing.
- `crates/query` remains pure and plans one HIR at a time.
- `crates/index/src/eval.rs` binds and evaluates several queries against one shared posting map.
- `crates/index/src/candidate.rs` is a new focused module extracted from the oversized segment reader. It batches term/posting reads and constructs tagged candidate ranges.
- `crates/index/src/segment.rs` retains snapshot loading, segment metadata, and reader integration.
- `crates/index/src/pack.rs` unions tagged byte requests, fetches each physical block once, and returns separate logical regions.
- `crates/index/src/search.rs` chooses spans from `SearchDetail`, owns worker caches, verifies regions, deduplicates absolute results, and invokes sinks.
- `skills/seagrep/SKILL.md` changes only after the CLI contract is implemented.

S3 remains transport-only, the index remains candidate/fetch/verify, query remains I/O-free, and core remains network-free.

## Errors and Observability

- Per-pattern transform and parse failures are fatal and identify the original pattern index. A failure while compiling any eligibility program from the already-valid HIRs is fatal at the query level; it is not falsely attributed to one pattern.
- Invalid `--match-window` combinations fail in CLI argument validation.
- Stale index, pack corruption, range verification, and transport errors remain fatal.
- Proof construction failure is an exact fallback, not a query failure. `--stats` reports total patterns, finite-exact patterns, finite-proof patterns, and fallback patterns.
- Existing candidate, fetched, regional, whole, and decoded byte counters remain authoritative after range union.
- A changed index root after candidate streaming begins retains the current retry error.

## Skill Update

After the CLI behavior exists, update the Seagrep skill without overwriting the current uncommitted edits:

- issue one repeated-`-e` command instead of serial shell searches;
- use `--match-window` for broad discovery over giant structured rows;
- reserve full-line output for the final narrow query;
- keep stderr and `--stats` visible while timing;
- remove advice that treats `-c` preflight or shell batching as an engine optimization.

The skill remains dataset-agnostic. It does not ship the benchmark pattern set.

## Focused Verification

Testing stays concentrated in existing modules:

1. A table test covers forward proof, reverse proof, finite maximum, unsafe middle gaps, alternation with an unsafe branch, line versus absolute look fallback, empty matches, DFA limits, and the one-block cap.
2. One differential store case uses a multi-megabyte single line, mixed bounded/proof/fallback patterns, cross-block matches, duplicate matches on one line, and first/last-byte matches. It compares all existing result modes with the whole-document oracle.
3. Eligibility tests place an ineligible earlier pattern at the same start as an eligible later pattern and prove that regional, full-line, and whole-document searches select the correct ordered subset without changing repeated-pattern semantics.
4. One reader instrumentation test proves that repeated patterns traverse each segment once and fetch each unique term/posting range once.
5. One CLI test verifies window width, clip markers, line number, window byte offset, `--column` rejection, other incompatibilities, and unchanged default output.
6. Existing workspace tests, rg parity, formatting, clippy, and the release build remain the regression gate.
7. The live pinned benchmark validates the performance thresholds and per-pattern oracle counts.

No new test crate, broad fixture framework, or benchmark-only production switch is introduced.

## Delivery Boundary

Implementation starts only after this design is reviewed. The implementation plan will enumerate every touched file and exact function/type transformation required by the repository planning standard.
