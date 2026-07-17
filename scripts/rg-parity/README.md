# ripgrep parity harness

Differential check that seagrep's matching semantics equal ripgrep's on a
fixture corpus covering the axes where greps diverge: UTF-16 (LE/BE, BOM),
CRLF and lone-CR line endings, invalid UTF-8, NUL-bearing binary, empty
files, missing trailing newlines, huge single lines, and container formats
(gzip, zip incl. nested members, tar.gz) compared against their decoded
twins.

Needs a local MinIO (docker compose -f docker-compose.bench.yml) and rg on
PATH:

    python3 scripts/rg-parity/build_fixtures.py
    # upload /tmp/parity/bucket to a MinIO bucket, index it, then
    python3 scripts/rg-parity/run.py

Last run 2026-07-17: PARITY CLEAN across 8 pattern classes after adding
UTF-16 BOM transcoding.
