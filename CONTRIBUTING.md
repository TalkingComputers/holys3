# Contributing to seagrep

Bug reports, documentation fixes, features, and questions are all welcome.
File bugs and feature requests as
[issues](https://github.com/TalkingComputers/seagrep/issues); general questions
belong in
[discussions](https://github.com/TalkingComputers/seagrep/discussions). For a
large change, open a discussion or an issue before writing the code, so we can
agree on the approach first.

## Project layout

seagrep is a Cargo workspace:

| Crate          | Path            | Responsibility                                 |
| -------------- | --------------- | ---------------------------------------------- |
| `seagrep`       | `crates/cli`    | S3-only user-facing CLI                        |
| `seagrep-core`  | `crates/core`   | Canonical decoding, matching, and shared types |
| `seagrep-query` | `crates/query`  | Regex-to-gram query planning                   |
| `seagrep-index` | `crates/index`  | Segmented snapshot index build and search      |
| `seagrep-s3`    | `crates/s3`     | AWS SDK transport and S3 storage               |
| `seagrep-bench` | `crates/xbench` | Unpublished deterministic benchmark harness    |

## Development setup

1. Install Rust via [rustup](https://rustup.rs/). The minimum supported Rust
   version (MSRV) is declared once in `Cargo.toml` as
   `[workspace.package] rust-version`; `cargo` will refuse to build on older
   toolchains.
2. Add the components used by the checks below:
   ```console
   $ rustup component add rustfmt clippy
   ```

## Build and test

```console
$ cargo build --workspace
$ cargo test --workspace
```

`Cargo.lock` is committed (this is a binary crate), so all commands run with
`--locked` in CI. If you change dependencies, commit the updated lockfile.

### Live S3 tests

`crates/s3/tests/live_s3.rs` and `crates/cli/tests/live_index.rs` exercise a
real bucket. They **self-skip** unless you point them at one:

Use a dedicated disposable bucket, never a production bucket. The CLI test
indexes the bucket root and leaves its `.seagrep/` index in place. The S3 test
briefly writes under `.seagrep-live-test/`, and an interrupted run may leave that
object behind. Seed these non-empty fixtures before running the suite:

- `a.rs` containing `handleClick`;
- `b.txt` containing `world`;
- `c/d.log` containing `EMAIL`.

```console
$ AWS_PROFILE=my-test-profile SEAGREP_TEST_BUCKET=my-test-bucket AWS_REGION=us-east-1 cargo test --locked --workspace --all-features
```

CI never sets `SEAGREP_TEST_BUCKET`, so these tests are skipped there — run them
locally if you touch the `s3` crate.

## Before opening a PR

Run the local equivalents of the CI checks:

```bash
make check
cargo machete --with-metadata
cargo semver-checks --workspace
cargo package --locked --workspace
cargo llvm-cov --locked --workspace --all-features --lcov --output-path lcov.info --fail-under-lines 80
zizmor --offline .
```

CI also tests the declared MSRV and all supported operating systems, packages
from a clean target directory, and enforces the 32 MiB release-binary limit.

In short, your change must:

- be formatted with `rustfmt` (`cargo fmt --all --check` is clean);
- be clippy-clean with warnings treated as errors (`-D warnings`);
- build and pass tests on the workspace;
- build docs with no rustdoc warnings;
- pass `cargo deny` (no new advisories, only allowed licenses);
- have no unused dependencies according to `cargo machete`;
- preserve the public libraries' SemVer compatibility;
- keep line coverage at or above 80%;
- pass `actionlint`, `typos`, and `zizmor` workflow checks.

### Useful standalone tooling

- [`typos`](https://github.com/crate-ci/typos) — `typos` to catch spelling
  mistakes (`cargo install typos-cli`).
- [`cargo-msrv`](https://github.com/foresterre/cargo-msrv) — `cargo msrv verify`
  to confirm the declared MSRV still builds; `cargo msrv find` to discover the
  real floor when you bump dependencies.
- [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) —
  `cargo semver-checks` to catch accidental breaking changes in the libraries.
- [`cargo-machete`](https://github.com/bnjbvr/cargo-machete) —
  `cargo machete --with-metadata` to catch unused dependencies.
- [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) — coverage and
  the 80% line-coverage gate.
- [`zizmor`](https://github.com/zizmorcore/zizmor) — static analysis for GitHub
  Actions workflows.
- A pre-commit runner such as [`lefthook`](https://github.com/evilmartians/lefthook)
  or [`rusty-hook`](https://github.com/swellaby/rusty-hook) to run `fmt` +
  `clippy` on every commit.

## Commits and PRs

- We use [Conventional Commits](https://www.conventionalcommits.org)
  (`feat:`, `fix:`, `docs:`, `chore:`, …). This drives automated changelog
  generation and version bumps at release time, so it matters.
- Keep commits **atomic**: each should build, pass tests, and have a single
  responsibility. A clean, readable history helps reviewers.
- Reference the issue you are addressing in the PR body (e.g. `Closes #123`).
- Add tests for new behavior and bug fixes.

## Sign-off (DCO)

seagrep uses the [Developer Certificate of Origin](https://developercertificate.org/).
There is **no CLA**. Certify that you wrote (or have the right to submit) your
change by signing off every commit:

```console
$ git commit -s -m "fix: handle empty bucket listing"
```

This appends a `Signed-off-by: Your Name <you@example.com>` trailer.

## Good first issues

Issues labeled [`good first issue`](https://github.com/TalkingComputers/seagrep/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
are scoped to be approachable without deep knowledge of the codebase — a good
place to start. [`help wanted`](https://github.com/TalkingComputers/seagrep/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
issues are larger but still up for grabs. Comment on an issue to claim it.

## Versioning

seagrep follows [Semantic Versioning](https://semver.org/). The libraries are
public API surface. Before 1.0, breaking library changes require a minor bump;
after 1.0, they require a major bump. Flag every breaking change in your PR.

## Releasing

Every push to `main` verifies release tests and packages before the protected
`crates-io` environment can publish registry-missing workspace versions through
trusted publishing. A successful release creates the shared version tag,
GitHub release, checksums, attestations, and binaries for Linux, macOS, and
Windows.

Release PR automation is opt-in because some organizations prohibit PRs created
by `GITHUB_TOKEN`:

1. Add a repository secret named `RELEASE_PLZ_TOKEN` from a fine-grained PAT or
   GitHub App with Contents and Pull requests read/write access.
2. Set the repository variable `RELEASE_PLZ_PR_ENABLED` to `true`.

Leave the variable `false` when no scoped token is configured. Direct releases
from reviewed version bumps on `main` remain enabled.
