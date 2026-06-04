BENCH_SCENARIOS=crates/xbench/scenarios/queries.toml
BENCH_SEED=1
BENCH_OBJECTS=1000
BENCH_SIZE=4096
BENCH_ITERATIONS=5
BENCH_WARMUP=1
BENCH_CONCURRENCY=64
MINIO_ENV=AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin HOLYS3_BENCH_BUCKET=holys3-bench HOLYS3_BENCH_REGION=us-east-1 HOLYS3_BENCH_ENDPOINT=http://localhost:9000

.PHONY: bench bench-micro bench-s3 bench-minio

bench: bench-micro

bench-micro:
	cargo bench -p holys3-index

bench-s3:
	cargo run -p holys3-bench -- seed --seed $(BENCH_SEED) --objects $(BENCH_OBJECTS) --size $(BENCH_SIZE)
	cargo run -p holys3-bench -- upload --target s3
	cargo run -p holys3-bench -- run --scenarios $(BENCH_SCENARIOS) --iterations $(BENCH_ITERATIONS) --warmup $(BENCH_WARMUP) --concurrency $(BENCH_CONCURRENCY)
	cargo run -p holys3-bench -- report --out crates/xbench/runs/s3.json
	cargo run -p holys3-bench -- render --input crates/xbench/runs/s3.json

bench-minio:
	docker compose -f docker-compose.bench.yml up -d
	$(MINIO_ENV) cargo run -p holys3-bench -- seed --seed $(BENCH_SEED) --objects $(BENCH_OBJECTS) --size $(BENCH_SIZE)
	$(MINIO_ENV) cargo run -p holys3-bench -- upload --target s3
	$(MINIO_ENV) cargo run -p holys3-bench -- run --scenarios $(BENCH_SCENARIOS) --iterations $(BENCH_ITERATIONS) --warmup $(BENCH_WARMUP) --concurrency $(BENCH_CONCURRENCY)
	cargo run -p holys3-bench -- report --out crates/xbench/runs/minio.json
	cargo run -p holys3-bench -- render --input crates/xbench/runs/minio.json
