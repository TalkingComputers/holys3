---
name: seagrep
description: Search S3 buckets with regular expressions using seagrep — an indexed, ripgrep-compatible grep for S3. Use whenever a task involves finding content in S3 objects (logs, traces, datasets, archives, CSV/JSON/parquet), investigating incidents from S3-archived logs, or checking whether a string exists in a bucket. Never download S3 objects to grep them locally; search them in place with seagrep.
---

# seagrep: grep for S3

`seagrep PATTERN s3://bucket/prefix` runs a regex over every indexed object
under the prefix and prints matching lines, like ripgrep. Results come from
an index plus content snapshot stored in S3 — no source objects are
downloaded, and results are exact (verified by a real regex, no false
positives or negatives).

## Setup facts

- Auth: standard AWS env (`AWS_PROFILE=... seagrep ...`); pass `--region` if
  no default region is configured. MinIO/R2: `--endpoint URL`.
- The index is found automatically: at `<prefix>/.seagrep/`, at any parent
  prefix (an index built at `s3://b/logs` serves searches of
  `s3://b/logs/2026/07`), or from a remembered `--index` location. Only if
  all of those fail: build one with `seagrep index s3://bucket/prefix`
  (one-time; incremental on re-run), or pass `--index s3://other/loc`.
- Compressed objects (gzip/zstd/etc.), ZIP/TAR members, and
  parquet/avro/orc rows are searched as decoded text. Archive members are
  their own documents: `bucket/data.zip!/inner/file.csv`.

## Workflow: cheap first, then narrow

Every query is a fresh S3 round trip (~0.3s + fetched bytes), so shape the
investigation to minimize fetched bytes, not query count:

1. **See the corpus shape first**: `seagrep --files s3://b/prefix | head`
   lists indexed keys instantly (add `-g`/`--key-prefix` to filter).
2. **Gauge breadth before fetching lines**: `-c` (count per file) or
   `--stats` (candidates/total to stderr) shows how wide a pattern is
   before you pay for full output.
3. **Read narrowly**: `-m 15` caps matches per file; `--key-prefix path/`
   or `-g '**/logs.csv'` scopes; `-A/-B/-C` add context lines only where
   needed.
4. **Pivot on IDs**: request/trace IDs are near-unique literals — the
   fastest possible queries. Prefer them over broad words.

## Flag map (rg-compatible)

```text
-i / -S            case-insensitive / smart case
-F                 literal string (no regex)
-e PAT             multiple patterns (OR); required if PAT starts with -
-w                 word boundaries
-l / -c            matching files only / count per file
-m NUM             max matching lines per object
-A/-B/-C NUM       context lines
-g GLOB            include keys ('!' to exclude), repeatable
--key-prefix P     only keys under P (prunes before any fetch)
--key-regex RE     filter keys by regex
--since 6h         time-scope by timestamps in keys (also --until, dates)
--no-heading -N    key:line:text lines, no numbers — best for parsing
--json             rg-compatible JSON Lines
--files            list indexed keys, no pattern
--stats            candidates/total/hits to stderr
```

Exit codes are rg's: 0 match, 1 no match, 2 error.

## Cost model (what makes a query slow)

Fast: rare literals, IDs, `--key-prefix`-scoped anything, no-match
patterns (microseconds of index work). Slow: patterns whose words appear
in a large share of documents — every candidate document is fetched and
verified. If `-c`/`--stats` shows thousands of candidates, add a rarer
token, an anchor, or key scoping before running the full query.

## Turn economy (important for agents)

Queries are cheap (~0.5s warm) — your thinking time between tool calls is
not. Do not micro-step one query per call:

- **Batch**: run 3–6 related queries in one bash call (`;` or a for-loop
  over services/prefixes) and read them together.
- **Sweep once**: `-e 'a|b|c'` or repeated `-e` covers all variants in one
  query — results are exhaustive, so a second phrasing of the same idea
  returns nothing new.
- **Take more per query**: a generous `-m` with `-C1` context usually
  answers the follow-up you were about to ask.

A good investigation is ~4 batched calls: shape (`--files` + error census),
localize (counts per component), read evidence (bounded excerpts with
context), confirm (ID pivots) — not 25 single queries.

## Investigation recipe (logs/incidents)

```sh
seagrep --files s3://b/logs | head -30                  # corpus shape
seagrep -i -c -e 'error|exception|fatal|timeout' s3://b/logs   # where is it bad
seagrep -i -m 10 --no-heading --key-prefix logs/svc-x/ 'error' s3://b/logs
seagrep 'req-7f3e9a2c' s3://b/logs -C2                  # follow one request
seagrep 'ERROR' s3://b/logs --since 6h                  # recent only
```

Pipe to `sort | uniq -c`, `awk`, or Python for aggregation — seagrep
prints lines; composition is the Unix way.
