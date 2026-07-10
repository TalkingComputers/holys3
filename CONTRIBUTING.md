# Contributing to holys3

Thanks for your interest in improving holys3! Contributions of every size are
welcome — bug reports, documentation fixes, new features, and questions all
help. **No contribution is too small.**

## Where to ask questions

- **Bugs / feature requests:** open an [issue](../../issues).
- **Design discussion / "is this a good idea?":** open a
  [discussion](../../discussions) (or an issue) _before_ a large PR, so we can
  agree on the approach first. This saves you from reworking a big change.

## Project layout

holys3 is a Cargo workspace. The `holys3` binary is the CLI; the rest are
libraries:

| Crate          | Path           | Responsibility                           |
| -------------- | -------------- | ---------------------------------------- |
| `holys3`       | `crates/cli`   | CLI binary (`holys3`)                    |
| `holys3-core`  | `crates/core`  | Shared types (corpus, doc ids, strategy) |
| `holys3-query` | `crates/query` | Query / regex parsing                    |
| `holys3-index` | `crates/index` | FST-backed index build + search          |
| `holys3-sigv4` | `crates/sigv4` | AWS SigV4 request signing                |
| `holys3-s3`    | `crates/s3`    | S3 client + blob store                   |

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

```console
$ export AWS_PROFILE=my-test-profile
$ export HOLYS3_TEST_BUCKET=my-test-bucket
$ export AWS_REGION=us-east-1
$ cargo test --workspace
```

CI never sets `HOLYS3_TEST_BUCKET`, so these tests are skipped there — run them
locally if you touch the `s3` or `sigv4` crates.

## The PR gauntlet

Every PR is run through the same checks CI runs. Run them locally first:

```console
$ cargo fmt --all --check
$ cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
$ cargo test --locked --workspace --all-features
$ cargo test --locked --release --workspace --all-features
$ RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --workspace
$ cargo deny check          # advisories, licenses, bans, sources
$ cargo machete --with-metadata
$ cargo package --locked --workspace
$ actionlint
$ typos
```

In short, your change must:

- be **formatted** with `rustfmt` (`cargo fmt --all --check` is clean);
- be **clippy-clean** with warnings treated as errors (`-D warnings`);
- **build and pass tests** on the workspace;
- **build docs** with no rustdoc warnings;
- pass **`cargo deny`** (no new advisories, only allowed licenses);
- have no unused dependencies according to **`cargo machete`**.

### Optional tooling we recommend

- [`typos`](https://github.com/crate-ci/typos) — `typos` to catch spelling
  mistakes (`cargo install typos-cli`).
- [`cargo-msrv`](https://github.com/foresterre/cargo-msrv) — `cargo msrv verify`
  to confirm the declared MSRV still builds; `cargo msrv find` to discover the
  real floor when you bump dependencies.
- [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks) —
  `cargo semver-checks` to catch accidental breaking changes in the libraries.
- [`cargo-machete`](https://github.com/bnjbvr/cargo-machete) —
  `cargo machete --with-metadata` to catch unused dependencies.
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

holys3 uses the [Developer Certificate of Origin](https://developercertificate.org/).
There is **no CLA**. Certify that you wrote (or have the right to submit) your
change by signing off every commit:

```console
$ git commit -s -m "fix: handle empty bucket listing"
```

This appends a `Signed-off-by: Your Name <you@example.com>` trailer.

## Good first issues

Issues labeled [`good first issue`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22)
are scoped to be approachable without deep knowledge of the codebase — a good
place to start. [`help wanted`](../../issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22)
issues are larger but still up for grabs. Comment on an issue to claim it.

## Versioning

holys3 follows [Semantic Versioning](https://semver.org/). The libraries are
public API surface: breaking changes require a major bump and should be flagged
in your PR.

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
