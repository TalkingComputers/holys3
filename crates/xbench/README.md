# holys3-bench

`holys3-bench` is the unpublished deterministic performance harness. Its local
backend exercises the index engine only; the `holys3` product CLI remains
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
`xbench/` and index data under `xbench/.holys3/`.

```sh
AWS_PROFILE=your-profile \
HOLYS3_BENCH_BUCKET=your-bucket \
HOLYS3_BENCH_REGION=us-east-1 \
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
cargo run --locked --release -p holys3-bench -- seed --seed 1 --objects 25000 --size 4096
cargo run --locked --release -p holys3-bench -- upload --target dir
cargo run --locked --release -p holys3-bench -- churn --cycles 30 --changes 250
```

It writes `crates/xbench/runs/churn.json` and mutates the generated corpus;
reseed before another run.
