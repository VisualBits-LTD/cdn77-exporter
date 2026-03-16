.PHONY: build test test-docker clean help bench bench-compare profile bench-rust

IMAGE_NAME := cdn77-exporter
IMAGE_TAG := latest

BENCH_SCALE ?= medium
VENV := .venv/bin
CDN_EXPORTER := cdn-exporter/target/release/cdn-exporter
CDN_EXPORTER_POLARS := cdn-exporter-polars/target/release/cdn-exporter-polars

help:
	@echo "CDN77 Prometheus Exporter - Available targets:"
	@echo "  make build         - Build production Docker image"
	@echo "  make test          - Run tests locally with Docker"
	@echo "  make test-docker   - Run tests in Docker build (faster)"
	@echo "  make bench         - Run performance benchmarks (BENCH_SCALE=small|medium|large|xl)"
	@echo "  make bench-compare - Run all implementations head-to-head with output validation"
	@echo "  make bench-rust    - Build and benchmark cdn-exporter (Rust)"
	@echo "  make profile       - Generate py-spy CPU flamegraph (requires sudo)"
	@echo "  make clean         - Remove test artifacts and cache"
	@echo "  make help          - Show this help message"

build:
	@echo "Building Docker image: $(IMAGE_NAME):$(IMAGE_TAG)"
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) -f Dockerfile .

test:
	@echo "Running test suite..."
	@docker run --rm -v "$(PWD)":/app python:3.11-alpine sh -c "\
		cd /app && \
		apk add --no-cache gcc musl-dev libffi-dev snappy-dev >/dev/null 2>&1 && \
		pip install -q -r requirements.txt && \
		python -m pytest test_exporter.py -v"

test-docker:
	@echo "Running test suite in Docker build..."
	@docker build --target test -f Dockerfile -t $(IMAGE_NAME):test . >/dev/null 2>&1 && \
	echo "✓ All tests passed!"

bench:
	@echo "Running benchmarks at scale: $(BENCH_SCALE)"
	$(VENV)/python bench_exporter.py --scale $(BENCH_SCALE) --output bench_results/

$(CDN_EXPORTER): cdn-exporter/src/*.rs cdn-exporter/Cargo.toml
	@echo "Building cdn-exporter (Rust)..."
	cd cdn-exporter && cargo build --release

$(CDN_EXPORTER_POLARS): cdn-exporter-polars/src/*.rs cdn-exporter-polars/Cargo.toml
	@echo "Building cdn-exporter-polars (Rust + Polars)..."
	cd cdn-exporter-polars && cargo build --release

bench-rust: $(CDN_EXPORTER) $(CDN_EXPORTER_POLARS)
	@echo "Running Rust benchmarks at scale: $(BENCH_SCALE)"
	$(VENV)/python bench_exporter.py --scale $(BENCH_SCALE) --only end_to_end_cdn_exporter,end_to_end_cdn_exporter_polars --output bench_results/

bench-compare: $(CDN_EXPORTER) $(CDN_EXPORTER_POLARS)
	@echo "Running all implementations at scale: $(BENCH_SCALE)"
	$(VENV)/python bench_exporter.py --scale $(BENCH_SCALE) \
		--only end_to_end_baseline,end_to_end,end_to_end_hybrid,end_to_end_native,end_to_end_cdn_exporter,end_to_end_cdn_exporter_polars,aggregate,aggregate_polars \
		--output bench_results/

profile:
	@echo "Generating CPU flamegraph at scale: $(BENCH_SCALE) (requires sudo)"
	sudo $(VENV)/py-spy record -o bench_results/flamegraph_$(BENCH_SCALE).svg -- $(VENV)/python bench_exporter.py --scale $(BENCH_SCALE)
	@echo "Flamegraph saved to bench_results/flamegraph_$(BENCH_SCALE).svg"

clean:
	@echo "Cleaning up test artifacts..."
	@rm -rf .pytest_cache __pycache__ test_exporter.pyc
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Clean complete"
