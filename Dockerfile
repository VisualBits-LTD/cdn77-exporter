FROM python:3.11-alpine AS base

LABEL maintainer="VisualBits LTD"
LABEL description="CDN77 to Prometheus metrics exporter daemon"

WORKDIR /app

# Install build dependencies for Python packages
RUN apk add --no-cache \
    gcc \
    musl-dev \
    libffi-dev \
    snappy-dev \
    && rm -rf /var/cache/apk/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy exporter scripts and protobuf definitions
COPY exporter.py remote_pb2.py .

# Test stage - runs test suite
FROM base AS test
COPY test_exporter.py .
RUN python -m pytest test_exporter.py -v

# Production stage
FROM base AS production
RUN chmod +x exporter.py

# Create data directory for state persistence
RUN mkdir -p /data

# Volume for state file
VOLUME ["/data"]

# Health check
HEALTHCHECK --interval=5m --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default to exporter daemon mode with 360s (6 minute) delay flush
ENTRYPOINT ["python", "-u", "exporter.py"]
CMD ["--poll-interval", "60", "--flush-delay", "360"]
