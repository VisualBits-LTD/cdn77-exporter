# Copilot Context: CDN77 Exporter

## Scope
- Repository: `cdn77-exporter`
- Runtime: Python exporter that reads CDN77 real-time access logs from S3 and remote-writes Prometheus metrics.

## Required Workflow
- Run everything from Docker containers.
- Do not rely on host Python for build, test, or runtime commands.
- Prefer Docker multi-stage targets defined in `Dockerfile`.

## Canonical Commands
- Build production image:
  - `docker build --target production -t cdn77-exporter:prod .`
- Run tests in container:
  - `docker build --target test -t cdn77-exporter:test .`
- Run exporter daemon from production image:
  - `docker run --rm --name cdn77-exporter \
    -e S3_ENDPOINT=https://eu-1.cdn77-storage.com \
    -e S3_BUCKET=real-time-logs-synwudjt \
    -e S3_PREFIX=real-time-logs/all-logs \
    -e S3_ACCESS_KEY=xxx \
    -e S3_SECRET_KEY=xxx \
    -e PROMETHEUS_URL=https://prometheus.example.com/api/v1/write \
    cdn77-exporter:prod`

## Codebase Notes
- `cdn77_users_total` is computed as per-minute unique client IP count per stream.
- All metric series must use timestamps aligned to processed log-event minute boundaries (UTC), with millisecond offsets only for duplicate-sample avoidance.
- Parsing drops invalid/non-stream lines; there is no synthetic fallback metric emission.

## Documentation Rules
- Keep README container-first.
- If command examples are added, provide Docker-based examples first.