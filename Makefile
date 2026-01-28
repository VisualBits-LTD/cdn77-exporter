.PHONY: build test test-docker clean help

IMAGE_NAME := cdn77-exporter
IMAGE_TAG := latest

help:
	@echo "CDN77 Prometheus Exporter - Available targets:"
	@echo "  make build       - Build production Docker image"
	@echo "  make test        - Run tests locally with Docker"
	@echo "  make test-docker - Run tests in Docker build (faster)"
	@echo "  make clean       - Remove test artifacts and cache"
	@echo "  make help        - Show this help message"

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

clean:
	@echo "Cleaning up test artifacts..."
	@rm -rf .pytest_cache __pycache__ test_exporter.pyc
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Clean complete"
