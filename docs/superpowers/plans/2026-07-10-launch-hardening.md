# Launch Hardening Implementation Plan

> [!NOTE]
> Historical planning record from 2026-07-10. It does not describe the current CLI or architecture. See [README](../../../README.md), [Architecture](../../../ARCHITECTURE.md), and [Changelog](../../../CHANGELOG.md).

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Land, validate, publish, and verify seagrep 0.4.0 through one protected release path.

**Architecture:** GitHub pull requests provide the immutable verification boundary. Release-plz owns version publication, tag creation, the GitHub release, binaries, checksums, and attestations; a protected GitHub environment gates crates.io publication. Live AWS validation uses only process-local configuration and a disposable bucket created for the run.

**Tech Stack:** Cargo, GitHub Actions, release-plz, crates.io trusted publishing, GitHub CLI, AWS CLI, Amazon S3.

## Global Constraints

- Never commit AWS profile, account, bucket, credential, or organization identifiers.
- Every AWS command must explicitly select the approved local profile and verify its STS identity before mutation.
- Never query or modify any other AWS profile or account.
- Never force-push or overwrite remote history.
- Publish only after local verification, live S3 verification, and every required GitHub check pass.

---

### Task 1: Consolidate Release Automation

**Files:**
- Modify: `.github/workflows/release-plz.yml`
- Delete: `.github/workflows/release.yml`

**Interfaces:**
- Consumes: pushes to `main`, crates.io OIDC, and the repository `GITHUB_TOKEN`.
- Produces: one `v{{ version }}` tag, one GitHub release, six published crates, five platform archives, SHA-256 checksums, and build attestations.

- Add `environment: crates-io` to `release-plz-release` so only the crate-publication job crosses the protected deployment boundary.
- Set `release_always = true` so the already-reviewed `0.4.0` workspace version is published after merge; environment approval remains mandatory.
- Keep `id-token: write`; do not add `CARGO_REGISTRY_TOKEN`.
- Delete the tag-triggered release workflow because `release-plz-release` and `release-assets` already own its complete behavior.
- Verify with `actionlint -color` and `rg -n 'create-release|upload-assets' .github/workflows` returning no duplicate release jobs.

### Task 2: Add Stable Required Checks

**Files:**
- Modify: `.github/workflows/bench.yml`
- Modify: `.github/workflows/security.yml`

**Interfaces:**
- Consumes: terminal states from each workflow's existing jobs.
- Produces: `benchmarks-success` and `security-success` check runs with a zero exit status only when all mandatory jobs succeed.

- Add `benchmarks-success` with `if: always()`, `needs: [micro, e2e, scale]`, and `jq --exit-status 'all(.result == "success")'`.
- Add `security-success` with `if: always()`, `needs: [dependency-review, codeql]`, and a predicate accepting only `success` or `skipped`; dependency review is intentionally skipped outside pull requests.
- Verify with `actionlint -color` and `typos`.

### Task 3: Reconcile and Verify the Branch

**Files:**
- Modify: the current Git branch and commit graph only.

- Create `feat/seagrep-0.4.0` without discarding the dirty worktree.
- Commit the complete reviewed implementation, merge `origin/main`, and resolve only genuine conflicts.
- Run formatting, check, tests, release tests, Clippy, docs, packaging, cargo-deny, actionlint, typos, and `git diff --check`.
- Scan the repository for forbidden organization identifiers and credential patterns before pushing.

### Task 4: Run the Live S3 Gate

**Files:**
- Modify: `crates/xbench/runs/s3.json` only if the fresh result is free of environment identifiers.

- Read the approved profile's configured account locally, then compare it to `aws sts get-caller-identity` with that profile explicitly selected.
- Create a globally unique disposable canary bucket in the profile's configured region.
- Run live integration tests, a 1,000-object deterministic benchmark, index/search parity checks, and a 64 MiB ranged-fetch check.
- Empty and delete only the bucket created by this task, then verify it no longer exists.

### Task 5: Land and Protect

**Files:**
- Modify: GitHub repository configuration only.

- Push `feat/seagrep-0.4.0`, open a pull request, and wait for `ci-success`, `benchmarks-success`, and `security-success`.
- Create the `crates-io` environment with a required reviewer and protected-branch deployment policy.
- Configure crates.io trusted publishing for every published workspace crate against `.github/workflows/release-plz.yml` and environment `crates-io`.
- Protect `main`: require pull requests, strict required checks, conversation resolution, linear history, no force pushes, and no deletion.
- Merge only after all required checks pass.

### Task 6: Verify the Release

**Files:**
- Modify: no repository files.

- Approve the protected `crates-io` deployment after confirming the release commit and version.
- Wait for crates.io publication and all five release-asset jobs.
- Verify `cargo install seagrep --version 0.4.0`, `cargo binstall seagrep`, SHA-256 checksum files, GitHub attestations, docs.rs, and `seagrep --version`.
- Confirm the repository contains no environment-specific identifiers and the release is backed by the merged commit.
