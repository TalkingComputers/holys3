# holys3-bench

Local MinIO run:

```sh
docker compose -f docker-compose.bench.yml up -d
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
HOLYS3_BENCH_BUCKET=holys3-bench \
HOLYS3_BENCH_REGION=us-east-1 \
HOLYS3_BENCH_ENDPOINT=http://localhost:9000 \
cargo run -p holys3-bench -- seed --seed 1 --objects 1000 --size 4096
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
HOLYS3_BENCH_BUCKET=holys3-bench \
HOLYS3_BENCH_REGION=us-east-1 \
HOLYS3_BENCH_ENDPOINT=http://localhost:9000 \
cargo run -p holys3-bench -- upload --target s3
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
HOLYS3_BENCH_BUCKET=holys3-bench \
HOLYS3_BENCH_REGION=us-east-1 \
HOLYS3_BENCH_ENDPOINT=http://localhost:9000 \
cargo run -p holys3-bench -- run --scenarios crates/xbench/scenarios/queries.toml --iterations 5 --warmup 1 --concurrency 64
cargo run -p holys3-bench -- render --input crates/xbench/runs/latest.json
```

Real S3 run:

```sh
HOLYS3_BENCH_BUCKET=your-bucket \
HOLYS3_BENCH_REGION=us-east-1 \
cargo run -p holys3-bench -- seed --seed 1 --objects 1000 --size 4096
HOLYS3_BENCH_BUCKET=your-bucket \
HOLYS3_BENCH_REGION=us-east-1 \
cargo run -p holys3-bench -- upload --target s3
HOLYS3_BENCH_BUCKET=your-bucket \
HOLYS3_BENCH_REGION=us-east-1 \
cargo run -p holys3-bench -- run --scenarios crates/xbench/scenarios/queries.toml --iterations 5 --warmup 1 --concurrency 64
cargo run -p holys3-bench -- report --out crates/xbench/runs/s3.json
cargo run -p holys3-bench -- render --input crates/xbench/runs/s3.json
```
