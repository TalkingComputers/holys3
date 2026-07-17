# seagrep-bench

`seagrep-bench` is the unpublished deterministic performance harness. Its local
backend exercises the index engine only; the `seagrep` product CLI remains
S3-only.

## MinIO

Run the complete seed, upload, index, query, and render workflow:

```sh
make bench-minio
make bench-minio BENCH_OBJECTS=25000 BENCH_ITERATIONS=3
```

The Makefile uses the canonical `http://127.0.0.1:9000` endpoint identity.
Results are written to `crates/xbench/runs/latest.json` and
`crates/xbench/runs/minio.json`.

## AWS S3

Use a dedicated benchmark bucket. The harness writes source objects under
`xbench/` and index data under `xbench/.seagrep/`.

```sh
AWS_PROFILE=your-profile \
SEAGREP_BENCH_BUCKET=your-bucket \
SEAGREP_BENCH_REGION=us-east-1 \
make bench-s3 BENCH_OBJECTS=25000 BENCH_ITERATIONS=3
```

Results are written to `crates/xbench/runs/latest.json` and
`crates/xbench/runs/s3.json`. Every timed scenario validates its expected hit,
candidate, and byte counts first.

## Engine microbenchmarks

```sh
make bench-micro
```

## Incremental churn

The churn benchmark is local and deterministic. It requires a freshly seeded
and locally indexed benchmark corpus:

```sh
cargo run --locked --release -p seagrep-bench -- seed --seed 1 --objects 25000 --size 4096
cargo run --locked --release -p seagrep-bench -- upload --target dir
cargo run --locked --release -p seagrep-bench -- churn --cycles 30 --changes 250
```

It writes `crates/xbench/runs/churn.json` and mutates the generated corpus;
reseed before another run.

## Prose corpus

Random-byte corpora make trigrams unrealistically selective. `--corpus prose`
seeds Zipf-sampled common-English text (hard-wrapped like Gutenberg files)
where phrase queries collapse trigram pruning — the workload the sparse
strategy exists for. `make bench-prose` runs the same scenarios against a
trigram and a sparse index and compares them; CI gates on the deterministic
candidate counts, not timings.

```sh
make bench-prose
```
