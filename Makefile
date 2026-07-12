BENCH_SCENARIOS=crates/xbench/scenarios/queries.toml
BENCH_SEED=1
BENCH_OBJECTS=1000
BENCH_SIZE=4096
BENCH_ITERATIONS=5
BENCH_WARMUP=1
BENCH_CONCURRENCY=64
MINIO_ENV=env -u AWS_PROFILE AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin HOLYS3_BENCH_BUCKET=holys3-bench HOLYS3_BENCH_REGION=us-east-1 HOLYS3_BENCH_ENDPOINT=http://127.0.0.1:9000
XBENCH=cargo run --locked --release -p holys3-bench --

.PHONY: check package bench bench-micro bench-s3 bench-minio

check:
	cargo fmt --all --check
	cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
	cargo test --locked --workspace --all-features
	cargo test --locked --release --workspace --all-features
	RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --document-private-items --workspace
	cargo deny check
	actionlint
	typos

package:
	cargo package --locked --workspace

bench: bench-micro

bench-micro:
	cargo bench --locked -p holys3-index

bench-s3:
	$(XBENCH) seed --seed $(BENCH_SEED) --objects $(BENCH_OBJECTS) --size $(BENCH_SIZE)
	$(XBENCH) upload --target s3
	$(XBENCH) run --scenarios $(BENCH_SCENARIOS) --iterations $(BENCH_ITERATIONS) --warmup $(BENCH_WARMUP) --concurrency $(BENCH_CONCURRENCY)
	cp crates/xbench/runs/latest.json crates/xbench/runs/s3.json
	$(XBENCH) render --input crates/xbench/runs/s3.json

bench-minio:
	docker compose -f docker-compose.bench.yml up -d
	$(MINIO_ENV) $(XBENCH) seed --seed $(BENCH_SEED) --objects $(BENCH_OBJECTS) --size $(BENCH_SIZE)
	$(MINIO_ENV) $(XBENCH) upload --target s3
	$(MINIO_ENV) $(XBENCH) run --scenarios $(BENCH_SCENARIOS) --iterations $(BENCH_ITERATIONS) --warmup $(BENCH_WARMUP) --concurrency $(BENCH_CONCURRENCY)
	cp crates/xbench/runs/latest.json crates/xbench/runs/minio.json
	$(XBENCH) render --input crates/xbench/runs/minio.json
