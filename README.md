# CDN77 Prometheus Exporter

Daemon service that polls CDN77 S3-compatible storage for real-time access logs and exports transfer metrics to Prometheus.

Copilot context for this repository: [`.github/copilot-instructions.md`](.github/copilot-instructions.md)

## Features

- Polls S3 endpoint every 60 seconds for new log files
- Processes real-time NDJSON logs (30-second chunks)
- Aggregates bytes transferred per stream per minute
- Exports to Prometheus remote write endpoint with authentication
- Stateless operation - files deleted after processing
- Handles gzipped NDJSON format efficiently

## Metrics Exported

Metric names are prefixed by `METRIC_PREFIX` (default: `cdn_`).
Examples below use the `cdn_` prefix.

The exporter generates 27 metrics with rich dimensional labels (stream, cdn_id, cache_status, pop, response_status, device_type, country, region, track, ip_version):

### Counters (use with `rate()` or `increase()`)

**`cdn_transfer_bytes_total`**
- Total bytes transferred (counter)
- Labels: `stream`, `cdn_id`, `cache_status`, `pop`
- Query bandwidth (bits/sec): `sum(rate(cdn_transfer_bytes_total{stream="..."}[5m])) * 8`
- Query total bytes in 5 minutes: `sum(increase(cdn_transfer_bytes_total{stream="..."}[5m]))`
- Query by location: `sum by (pop) (rate(cdn_transfer_bytes_total{stream="..."}[5m]))`

**`cdn_requests_total`**
- Total number of requests (counter)
- Labels: `stream`, `cdn_id`, `cache_status`, `pop`
- Query requests/sec: `sum(rate(cdn_requests_total{stream="..."}[5m]))`
- Query total requests: `sum(increase(cdn_requests_total{stream="..."}[1h]))`
- Query by cache status: `sum by (cache_status) (rate(cdn_requests_total{stream="..."}[5m]))`

**`cdn_responses_total`**
- Requests by HTTP status code (counter)
- Labels: `stream`, `cdn_id`, `cache_status`, `pop`, `response_status`
- Query 4xx error rate: `sum(rate(cdn_responses_total{stream="...", response_status=~"4.."}[5m]))`
- Query success rate: `sum(rate(cdn_responses_total{stream="...", response_status="200"}[5m]))`

**`cdn_users_total`**
- Count of unique IP addresses per stream (counter)
- Labels: `stream`
- Query unique users per stream: `cdn_users_total{stream="..."}`
- Query total unique users across all streams: `sum(cdn_users_total)`
- Query unique users rate: `rate(cdn_users_total{stream="..."}[5m])`

**`cdn_users_by_device_total`**
- Count of unique IP addresses per stream by device type (counter)
- Labels: `stream`, `device_type`
- Device types: `android`, `ios`, `apple_tv`, `roku`, `firestick`, `web`, `bots`, `streamology`, `other`
- Query mobile vs OTT: `cdn_users_by_device_total{stream="...", device_type=~"android|ios|apple_tv|roku|firestick"}`
- Query by platform: `sum by (device_type) (cdn_users_by_device_total{stream="..."})`
- Compare platforms: `cdn_users_by_device_total{stream="...", device_type="roku"}` vs `device_type="apple_tv"`

**`cdn_users_by_country_total`**
- Count of unique IP addresses per stream by country (counter)
- Labels: `stream`, `country`
- Country from CDN77 logs (ISO 3166-1 alpha-2)
- Query unique users per country: `cdn_users_by_country_total{stream="...", country="US"}`
- Query top countries: `topk(10, cdn_users_by_country_total{stream="..."})`
- Query growth rate: `rate(cdn_users_by_country_total{stream="...", country="US"}[5m])`

**`cdn_users_by_resolution_total`**
- Count of unique IP addresses per stream by track/resolution segment (counter)
- Labels: `stream`, `track`
- `track` parsed from path segment `tracks-vX` as `vX` (example: `tracks-v3` -> `track="v3"`)
- Query unique users per track: `cdn_users_by_resolution_total{stream="...", track="v3"}`

### Gauges (use with `max_over_time()` or direct value)

**`cdn_viewers`**
- Unique viewers from the latest processed file/flush (gauge)
- Labels: `stream`
- Value is recalculated from the current processed batch, not a rolling session window
- Query current unique viewers: `cdn_viewers{stream="..."}`
- Query peak during broadcast: `max_over_time(cdn_viewers{stream="..."}[3h])`
- Query average concurrent: `avg_over_time(cdn_viewers{stream="..."}[1h])`

**`cdn_viewers_by_device`**
- Unique viewers by device type from the latest processed file/flush (gauge)
- Labels: `stream`, `device_type`
- Query by platform: `cdn_viewers_by_device{stream="...", device_type="roku"}`
- Baron vs OTT: `sum by (device_type) (cdn_viewers_by_device{stream="..."})`

**`cdn_viewers_by_ip_version`**
- Unique viewers by IP version from the latest processed file/flush (gauge)
- Labels: `stream`, `ip_version` (`ipv4`, `ipv6`)
- Query split: `sum by (ip_version) (cdn_viewers_by_ip_version{stream="..."})`

**`cdn_viewers_by_country`**
- Unique viewers by country from the latest processed file/flush (gauge)
- Labels: `stream`, `country`
- Requires GeoIP database (see setup below)
- Query top countries: `topk(10, cdn_viewers_by_country{stream="..."})`
- Query specific country: `cdn_viewers_by_country{stream="...", country="US"}`

**`cdn_viewers_by_resolution`**
- Unique viewers by track/resolution from latest processed file/flush (gauge)
- Labels: `stream`, `track`
- `track` parsed from path segment `tracks-vX` as `vX` (example: `tracks-v3` -> `track="v3"`)
- Query by track: `cdn_viewers_by_resolution{stream="...", track="v3"}`

**`cdn_viewers_by_region`**
- Unique viewers by state/region from the latest processed file/flush (gauge)
- Labels: `stream`, `country`, `region`
- Requires GeoIP database (see setup below)
- Query top regions: `topk(10, cdn_viewers_by_region{stream="..."})`
- Query US states: `cdn_viewers_by_region{stream="...", country="US"}`

**`cdn_viewers_unique`**
- Unique viewers from rolling session state for configured window (default: 1h) (gauge)
- Labels: `stream`, `window`
- Query 1h unique viewers: `cdn_viewers_unique{stream="...", window="1h"}`

**`cdn_viewers_unique_by_device`**
- Rolling unique viewers by device for configured window (default: 1h) (gauge)
- Labels: `stream`, `device_type`, `window`
- Query platform mix: `sum by (device_type) (cdn_viewers_unique_by_device{stream="...", window="1h"})`

**`cdn_viewers_unique_by_ip_version`**
- Rolling unique viewers by IP version for configured window (default: 1h) (gauge)
- Labels: `stream`, `ip_version`, `window`
- Query split: `sum by (ip_version) (cdn_viewers_unique_by_ip_version{stream="...", window="1h"})`

**`cdn_viewers_unique_by_country`**
- Rolling unique viewers by country for configured window (default: 1h) (gauge)
- Labels: `stream`, `country`, `window`
- Query top countries: `topk(10, cdn_viewers_unique_by_country{stream="...", window="1h"})`

**`cdn_viewers_unique_by_region`**
- Rolling unique viewers by state/region for configured window (default: 1h) (gauge)
- Labels: `stream`, `country`, `region`, `window`
- Requires GeoIP database (see setup below)
- Query US states: `cdn_viewers_unique_by_region{stream="...", country="US", window="1h"}`

**`cdn_watch_sessions_total`**
- Total completed watch sessions by stream (counter)
- Labels: `stream`, `device_type`
- Sessions close after `SESSION_GAP_SECONDS` of inactivity (or on hard expiry)
- Query completed sessions (all devices): `sum by (stream) (increase(cdn_watch_sessions_total{stream="..."}[1h]))`
- Query completed sessions by device: `sum by (stream, device_type) (increase(cdn_watch_sessions_total{stream="..."}[1h]))`

**`cdn_watch_time_seconds_total`**
- Total watch time accumulated from completed sessions (counter)
- Labels: `stream`
- Query watch hours: `increase(cdn_watch_time_seconds_total{stream="..."}[1h]) / 3600`

**`cdn_watch_time_seconds_by_device_total`**
- Total watch time accumulated from completed sessions, segmented by device (counter)
- Labels: `stream`, `device_type`
- Query watch hours by device: `sum by (stream, device_type) (increase(cdn_watch_time_seconds_by_device_total[1h])) / 3600`

**`cdn_watch_time_seconds_by_device_band_total`**
- Total watch time accumulated from completed sessions by device and watch-time band (counter)
- Labels: `stream`, `device_type`, `band` (`0_1m`, `1_5m`, `5_10m`, `10_20m`, `20_60m`, `60_plus`)
- Query watch hours by device and band: `sum by (stream, device_type, band) (increase(cdn_watch_time_seconds_by_device_band_total[1h])) / 3600`

**`cdn_watch_session_duration_seconds_bucket`** (+`_sum`, +`_count`)
- Histogram of completed session durations by stream
- Labels: `stream`, `le`
- Query p90 session duration: `histogram_quantile(0.90, sum by (stream, le) (rate(cdn_watch_session_duration_seconds_bucket[30m])))`

**`cdn_watch_session_duration_band_total`**
- Product-friendly session duration band totals by stream (counter)
- Labels: `stream`, `band` (`0_1m`, `1_5m`, `5_10m`, `10_20m`, `20_60m`, `60_plus`)
- Query engagement mix: `sum by (stream, band) (increase(cdn_watch_session_duration_band_total[1h]))`
- Query short-session ratio: `sum(increase(cdn_watch_session_duration_band_total{band="0_1m"}[1h])) / sum(increase(cdn_watch_sessions_total[1h]))`

**`cdn_time_to_first_byte_ms`**
- Average time to first byte in milliseconds (gauge)
- Labels: `stream`, `cdn_id`, `cache_status`, `pop`
- Query average TTFB: `avg_over_time(cdn_time_to_first_byte_ms{stream="..."}[5m])`
- Query by location: `avg by (pop) (avg_over_time(cdn_time_to_first_byte_ms{stream="..."}[5m]))`

**`cdn_file_process_time_seconds`**
- Time to download, decompress, parse, and delete one log file (gauge)
- Labels: `result` (`success` or `failure`)
- Query average processing time: `avg_over_time(cdn_file_process_time_seconds{result="success"}[5m])`
- Query failure processing time: `avg_over_time(cdn_file_process_time_seconds{result="failure"}[15m])`

**`cdn_file_process_delay_seconds`**
- Parse lag in seconds between wall-clock now and the newest parsed event timestamp in a processed file (gauge)
- Labels: `result` (`success` when delay can be computed)
- Query current ingest lag: `max_over_time(cdn_file_process_delay_seconds{result="success"}[5m])`
- Query lag trend: `avg_over_time(cdn_file_process_delay_seconds{result="success"}[30m])`

**`cdn_file_lines_processed_total`**
- Number of NDJSON lines processed per file (gauge-like per-file sample)
- Labels: `result` (`success` or `failure`)
- Query average lines/file: `avg_over_time(cdn_file_lines_processed_total{result="success"}[5m])`
- Query processing throughput estimate: `sum_over_time(cdn_file_lines_processed_total{result="success"}[5m]) / 300`

**`cdn_viewer_undefined_devices_total`**
- Total unmatched device user-agent events observed in a poll cycle (gauge-like per-poll sample)
- Labels: none
- Query recent undefined-device load: `sum_over_time(cdn_viewer_undefined_devices_total[10m])`
- Query average undefined devices per poll: `avg_over_time(cdn_viewer_undefined_devices_total[30m])`

### Label Dimensions

All metrics include these labels for filtering and grouping:
- `stream` - 32-character hex stream ID extracted from path
- `stream_name` - Legacy compatibility mirror of `stream` (temporary during migration)
- `cdn_id` - CDN77 resource ID
- `cache_status` - HIT, MISS, EXPIRED, etc.
- `pop` - Edge location (e.g., "losangelesUSCA", "frankfurtDEHE")
- `response_status` - HTTP status code (only on `cdn_responses_total`)

### Legacy `stream_name` compatibility (temporary)

To preserve dashboards and alerts during label migration, the exporter emits both `stream` and `stream_name` on stream-scoped series by default.

- Control with `EMIT_LEGACY_STREAM_NAME_LABEL` (default: `true`)
- Recommended plan: keep enabled through migration/backfill, then disable around June 2026 once historical `stream_name` dependence has aged out.

## Log Format

The exporter processes real-time CDN77 access logs in NDJSON format (newline-delimited JSON), delivered as gzipped files every ~30 seconds.

**JSON Structure (All Available Fields):**
```json
{
  "timestamp": "2026-01-10T14:49:28.299Z",
  "startTimeMs": 1768056568299,
  "tcpRTTus": 42321,
  "requestTimeMs": 132,
  "timeToFirstByteMs": 132,
  "resourceID": 1693215245,
  "cacheAge": 2,
  "cacheStatus": "HIT",
  "clientCountry": "US",
  "clientIP": "104.192.89.228",
  "clientPort": 61825,
  "clientASN": 606,
  "clientRequestAccept": "*/*",
  "clientRequestBytes": 65,
  "clientRequestCompleted": true,
  "clientRequestConnTimeout": false,
  "clientRequestContentType": "",
  "clientRequestHost": "1693215245.rsc.cdn77.org",
  "clientRequestMethod": "GET",
  "clientRequestPath": "/78001c7e2b3ff8ca21b504883b47c44c/tracks-v3/rewind-3600.fmp4.m3u8",
  "clientRequestProtocol": "HTTP20",
  "clientRequestRange": "",
  "clientRequestReferer": "https://virtualrailfan.com/",
  "clientRequestScheme": "HTTPS",
  "clientRequestServerPush": false,
  "clientRequestUserAgent": "Mozilla/5.0 ...",
  "clientRequestVia": "",
  "clientRequestXForwardedFor": "104.192.89.0",
  "clientSSLCipher": "TLS_AES_256_GCM_SHA384",
  "clientSSLFlags": 1,
  "clientSSLProtocol": "TLS13",
  "locationID": "losangelesUSCA",
  "serverIP": "143.244.51.39",
  "serverPort": 443,
  "responseStatus": 200,
  "responseBytes": 4143,
  "responseContentType": "application/vnd.apple.mpegurl",
  "responseContentRange": "",
  "responseContentEncoding": "GZIP"
}
```

**Fields Used by Exporter:**
- `timestamp` - ISO 8601 timestamp, parsed and rounded down to the minute for aggregation
- `clientRequestPath` - Request path, stream ID extracted via regex `/([a-f0-9]{32})/`
- `responseBytes` - Bytes transferred, aggregated by stream + minute
- `clientIP` - Client IP address, used for unique user counting per stream
- `clientRequestUserAgent` - User-Agent string, parsed to detect device type (Baron mobile app, Apple TV, Roku, Firestick, etc.)

**Available But Unused Fields:**
- **Timing Metrics**: `startTimeMs`, `tcpRTTus`, `requestTimeMs`, `timeToFirstByteMs`
- **Cache Info**: `cacheAge`, `cacheStatus`
- **Client Info**: `clientCountry`, `clientIP`, `clientPort`, `clientASN`
- **Request Details**: `clientRequestAccept`, `clientRequestBytes`, `clientRequestCompleted`, `clientRequestConnTimeout`, `clientRequestContentType`, `clientRequestHost`, `clientRequestMethod`, `clientRequestProtocol`, `clientRequestRange`, `clientRequestReferer`, `clientRequestScheme`, `clientRequestServerPush`, `clientRequestUserAgent`, `clientRequestVia`, `clientRequestXForwardedFor`
- **SSL Info**: `clientSSLCipher`, `clientSSLFlags`, `clientSSLProtocol`
- **Server Info**: `locationID`, `serverIP`, `serverPort`, `resourceID`
- **Response Details**: `responseStatus`, `responseContentType`, `responseContentRange`, `responseContentEncoding`

**S3 File Structure:**
```
s3://real-time-logs-synwudjt/real-time-logs/all-logs/YYYYMMDD/HH/
  └── rtl.{uuid}+{shard}+{sequence}.ndjson.gz
```

Files contain approximately 30 seconds of log data, delivered with minimal latency.

## GeoIP Setup (Optional)

For geographic metrics (`cdn_viewers_by_country` and `cdn_viewers_by_region`), download the MaxMind GeoLite2 database:

1. **Sign up for free MaxMind account:** https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
2. **Download GeoLite2-City.mmdb** (~70MB)
3. **Mount database in Docker:**
   ```yaml
   volumes:
     - ./GeoLite2-City.mmdb:/app/GeoLite2-City.mmdb:ro
   ```
4. **Add environment variable:**
   ```yaml
   GEOIP_DB_PATH: /app/GeoLite2-City.mmdb
   ```

**Performance:** 1-5 microseconds per lookup, ~50MB memory overhead

**Without GeoIP:** Geographic metrics will show country="XX" and region="Unknown"

## Usage

### Docker Compose (Recommended)

```yaml
cdn77-s3-importer:
  build: ./docker/cdn77-exporter
  environment:
    S3_ENDPOINT: https://eu-1.cdn77-storage.com
    S3_BUCKET: real-time-logs-synwudjt
    S3_PREFIX: real-time-logs/all-logs
    S3_ACCESS_KEY: ${S3_ACCESS_KEY}
    S3_SECRET_KEY: ${S3_SECRET_KEY}
    PROMETHEUS_URL: https://prometheus.streamology.tv/api/v1/write
    PROMETHEUS_USER: ${PROMETHEUS_USER}
    PROMETHEUS_PASSWORD: ${PROMETHEUS_PASSWORD}
  restart: unless-stopped
```

### Manual Docker Run

```bash
docker build -t cdn77-s3-importer docker/cdn77-exporter/

# Run S3 importer daemon
docker run -d \
  --name cdn77-s3-importer \
  -e S3_ENDPOINT=https://eu-1.cdn77-storage.com \
  -e S3_BUCKET=real-time-logs-synwudjt \
  -e S3_PREFIX=real-time-logs/all-logs \
  -e S3_ACCESS_KEY=xxx \
  -e S3_SECRET_KEY=xxx \
  -e PROMETHEUS_URL=https://prometheus.streamology.tv/api/v1/write \
  -e PROMETHEUS_USER=admin \
  -e PROMETHEUS_PASSWORD=secret \
  cdn77-s3-importer
```

### Container-First Development

```bash
# Run tests in Docker (uses Dockerfile test stage)
docker build --target test -t cdn77-exporter:test .

# Build production image
docker build --target production -t cdn77-exporter:prod .

# Run exporter daemon from container
docker run --rm --name cdn77-exporter \
  -e S3_ENDPOINT=https://eu-1.cdn77-storage.com \
  -e S3_BUCKET=real-time-logs-synwudjt \
  -e S3_PREFIX=real-time-logs/all-logs \
  -e S3_ACCESS_KEY=xxx \
  -e S3_SECRET_KEY=xxx \
  -e METRIC_PREFIX=cdn_ \
  -e PROMETHEUS_URL=https://prometheus.streamology.tv/api/v1/write \
  cdn77-exporter:prod
```

## Configuration

**Environment Variables:**
- `S3_ENDPOINT` - S3-compatible endpoint URL (required)
- `S3_BUCKET` - S3 bucket name (required)
- `S3_PREFIX` - S3 key prefix for log files (required)
- `S3_ACCESS_KEY` - S3 access key ID (required)
- `S3_SECRET_KEY` - S3 secret access key (required)
- `PROMETHEUS_URL` - Prometheus remote write endpoint (required)
- `PROMETHEUS_USER` - Prometheus basic auth username (optional)
- `PROMETHEUS_PASSWORD` - Prometheus basic auth password (optional)
- `POLL_INTERVAL` - Polling interval in seconds (default: 60)
- `LOOKBACK_HOURS` - How far back to check for files (default: 2)
- `SESSION_WINDOW_SECONDS` - Session retention horizon used for unique-window calculations (default: 7200 = 2 hours)
- `SESSION_GAP_SECONDS` - Gap threshold to split sessions for watch-time aggregation (default: 120)
  - Also controls idle timeout used to close watch sessions when viewers stop sending events
- `UNIQUE_VIEWERS_WINDOW_SECONDS` - Window used for `cdn_viewers_unique*` metrics (default: 3600 = 1 hour)
- `SESSION_STATE_PATH` - Optional path to persisted session tracker snapshot file (example: `/app/state/session_state.json.gz`)
- `SESSION_STATE_SAVE_INTERVAL_SECONDS` - Background session snapshot interval in seconds (default: `300`)
- `GEOIP_DB_PATH` - Path to MaxMind GeoLite2-City.mmdb database (optional, for geographic metrics)
- `PROMETHEUS_RETRY_ATTEMPTS` - Retries for failed remote-write pushes in writer thread (default: 5)
- `PROMETHEUS_RETRY_BASE_DELAY_SECONDS` - Initial retry backoff delay (default: 1.0)
- `PROMETHEUS_RETRY_MAX_DELAY_SECONDS` - Max retry backoff delay cap (default: 30.0)
- `STARTUP_OFFSET_MS` - Millisecond offset added to minute-aligned samples (default: 0)
- `EMIT_LEGACY_STREAM_NAME_LABEL` - Emit legacy `stream_name` label mirroring `stream` on stream series (default: `true`)
- `METRIC_PREFIX` - Prefix for all emitted metric names (default: `cdn_`, e.g. `cdn_users_total`)
- `UNMATCHED_DEVICE_LOGGING` - Enable end-of-poll logging of user agents classified as `other` (default: `true`)
- `UNMATCHED_DEVICE_MAX_TRACKED` - Maximum unique unmatched user agents tracked in memory (default: `5000`)

### Future: Additional unique windows

Current behavior emits a single unique window via `UNIQUE_VIEWERS_WINDOW_SECONDS` (default `1h`).
To support multiple windows later (for example `1h`, `2h`, `4h`, `24h`), emit additional `cdn_viewers_unique*` snapshots with different `window` label values from the same session table, and set `SESSION_WINDOW_SECONDS` to at least the largest target window.

## How It Works

1. **Poll S3**: Every 60 seconds, polls for new `.ndjson.gz` files (current hour + 2-hour lookback)
2. **Download & Parse**: Downloads files chronologically (oldest first), decompresses, parses JSON line-by-line
3. **Event Creation**: Each JSON log entry becomes an Event object with timestamp, stream ID, bytes, TTFB, cache status, location, etc.
4. **Buffer Events**: All events from file buffered in memory
5. **Flush After Each File**: Events immediately flushed and aggregated by minute
6. **Multi-Metric Generation**: Single event stream generates multiple series.
7. **Timestamp Offset Strategy**: Prevents duplicate timestamp errors:
  - Startup offset: `STARTUP_OFFSET_MS` (default `0`, so first sample lands on exact minute boundary)
   - Batch increment: +100ms per batch for same minute
  - Example: 04:27:00.000, 04:27:00.100, 04:27:00.200 (same source minute, deduplicated by ms offset)
8. **Label Dimensions**: All metrics tagged with stream, cdn_id, cache_status, pop (location)
9. **Push to Prometheus**: Batched push via remote write API (protobuf + snappy compression)
10. **Delete & Repeat**: Removes processed file from S3, waits for next poll
11. **Session State Persistence (optional)**: Background thread snapshots session tracker state on interval and restores on startup

**Writer Retry Behavior:**
- Remote write failures (network/DNS/5xx) are retried in the writer thread with exponential backoff.
- Duplicate-sample `400` responses are still handled with timestamp increment retries.
- Retry tuning is controlled via `PROMETHEUS_RETRY_*` environment variables.

**Session Persistence Behavior:**
- When `SESSION_STATE_PATH` is set, exporter restores session state from disk at startup.
- A dedicated background thread writes compact gzip JSON snapshots every `SESSION_STATE_SAVE_INTERVAL_SECONDS` (default 5 minutes).
- Final snapshot is also written during graceful shutdown.

**Processing Flow:**
```
S3 Bucket → Download (chronological) → Parse JSON → Event Buffer → 
Aggregate by Minute+Labels → Apply Offset → Push to Prometheus → Delete from S3
```

**Overlapping File Handling:**
- Files contain overlapping timestamps (~30s each, consecutive files overlap by seconds)
- Batch offset system allows multiple flushes per minute without duplicates
- Each flush keeps minute boundary timestamp semantics and gets unique millisecond offsets
- Prometheus automatically aggregates across batches in range queries

## Performance

- **Memory**: ~50-100MB for typical 30-second log files (~16K lines)
- **Processing**: ~1 second per file (download, parse, aggregate, push)
- **Compression**: ~88% (protobuf + snappy for Prometheus remote write)
- **Latency**: ~30-90 seconds from log generation to Prometheus ingestion
- **Backfill**: 7-day window (older data filtered by Prometheus)

## Troubleshooting

**Check logs:**
```bash
docker logs -f cdn77-s3-importer
```