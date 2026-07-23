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
2. **Bucket-wide discovery is always bounded**: run one repeated-`-e`
   command with `--match-window`, `--stats`, and exactly `-m 1`. Never use
   `-c`, omit `-m`, or use a larger `-m` before key/pattern scoping.
3. **Read more only after scoping**: use `--key-prefix path/`, a confirmed
   distinctive pattern, or `-g '**/logs.csv'` before increasing `-m`,
   counting, or adding `-A/-B/-C` context.
4. **Pivot on IDs**: request/trace IDs are near-unique literals — the
   fastest possible queries. Prefer them over broad words.

For a bucket-wide API key or token screen, copy this first-pass command
exactly and replace only the bucket:

```sh
seagrep \
  -e 'sk-ant-[A-Za-z0-9_-]{24,}' \
  -e 'AKIA[A-Z0-9]{16}' \
  -e 'ASIA[A-Z0-9]{16}' \
  -e 'github_pat_[A-Za-z0-9]{22}_[A-Za-z0-9]{59}' \
  -e 'gh[pousr]_[A-Za-z0-9]{36}' \
  -e 'AIza[0-9A-Za-z_-]{35}' \
  -e '-----BEGIN (RSA |EC |DSA |OPENSSH |PGP )?PRIVATE KEY( BLOCK)?-----' \
  -e 'xox[baprs]-[0-9A-Za-z-]{10,}' \
  --match-window 256 --stats -m 1 s3://<bucket>
```

Do not add broader formats to this first pass. Inspect the bounded findings,
then run a second query only for a confirmed key prefix or pattern.

## Flag map (rg-compatible)

```text
-i / -S            case-insensitive / smart case
-F                 literal string (no regex)
-e PAT             multiple patterns (OR); required if PAT starts with -
-w                 word boundaries
-l / -c            matching files only / count per file
-m NUM             max matching lines per object
-A/-B/-C NUM       context lines
--match-window N   bounded match-centered content per matching line
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
verified. Never use `-c` or `--count-matches` as a broad preflight: counts
must exhaust the full candidate union and disable `-m` early stopping.
Every bucket-wide discovery sweep must use `--match-window`, `--stats`,
and exactly `-m 1`. Increase `-m` or count only after narrowing by key
prefix or a confirmed distinctive pattern.

Cost tracks the commonness of the pattern's 3-char substrings, not the
whole string's rarity: on a code corpus `git push --force` is slow (every
trigram is everywhere) while `AKIA` or `ghp_` is fast. Prefer distinctive
anchored fragments over short common ones (`AKIA` beats `key`; `ECONNREFUSED`
beats `error`). Stay case-sensitive unless case truly varies — `-i`
multiplies candidates. Keep --stats visible while timing broad sweeps, then
scope or sharpen only when the evidence calls for it.

## Turn economy (important for agents)

Queries are cheap — your thinking time between tool calls is not. Do not
micro-step one query per call:

- **Sweep once, one command**: pass every variant as its own `-e` pattern
  in a single invocation — the engine plans all the patterns together and
  shares index, posting, and snapshot work across them. Candidate and
  verification cost still grows with the union of the supplied patterns.
  Results are exhaustive for that exact pattern set; different literals or
  regexes can find additional matches.
- **Run one search at a time**: never launch concurrent Seagrep sweeps over
  the same bucket. Put the patterns into one repeated-`-e` invocation.
- **Take more only after scoping**: once a key prefix or distinctive pattern
  is confirmed, a larger `-m` with `-C1` can answer the next follow-up.

A good investigation is ~4 calls: shape (`--files` + one multi-pattern
sweep), localize (bounded key-scoped pivots), read evidence (bounded
excerpts with context), confirm (ID pivots) — not 25 single queries.

## Investigation recipe (logs/incidents)

```sh
seagrep --files s3://b/logs | head -30                  # corpus shape
seagrep -e error -e exception -e fatal -e timeout \
  --match-window 512 --stats -m 1 s3://b/logs           # one bounded sweep
seagrep -i -m 10 --no-heading --key-prefix logs/svc-x/ 'error' s3://b/logs
seagrep 'req-7f3e9a2c' s3://b/logs -C2                  # final narrow query: full lines + context
seagrep 'ERROR' s3://b/logs --since 6h                  # recent only
```

Pipe to `sort | uniq -c`, `awk`, or Python for aggregation — seagrep
prints lines; composition is the Unix way.
