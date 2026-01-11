# CDN77 Prometheus Exporter

Daemon service that polls CDN77 S3-compatible storage for real-time access logs and exports transfer metrics to Prometheus.

## Features

- Polls S3 endpoint every 60 seconds for new log files
- Processes real-time NDJSON logs (30-second chunks)
- Aggregates bytes transferred per stream per minute
- Exports to Prometheus remote write endpoint with authentication
- Stateless operation - files deleted after processing
- Handles gzipped NDJSON format efficiently

## Metrics Exported

The exporter generates 4 metrics with rich dimensional labels (stream_name, cdn_id, cache_status, pop, response_status):

### Counters (use with `rate()` or `increase()`)

**`cdn77_transfer_bytes_total`**
- Total bytes transferred (counter)
- Labels: `stream_name`, `cdn_id`, `cache_status`, `pop`
- Query bandwidth (bits/sec): `sum(rate(cdn77_transfer_bytes_total{stream_name="..."}[5m])) * 8`
- Query total bytes in 5 minutes: `sum(increase(cdn77_transfer_bytes_total{stream_name="..."}[5m]))`
- Query by location: `sum by (pop) (rate(cdn77_transfer_bytes_total{stream_name="..."}[5m]))`

**`cdn77_requests_total`**
- Total number of requests (counter)
- Labels: `stream_name`, `cdn_id`, `cache_status`, `pop`
- Query requests/sec: `sum(rate(cdn77_requests_total{stream_name="..."}[5m]))`
- Query total requests: `sum(increase(cdn77_requests_total{stream_name="..."}[1h]))`
- Query by cache status: `sum by (cache_status) (rate(cdn77_requests_total{stream_name="..."}[5m]))`

**`cdn77_responses_total`**
- Requests by HTTP status code (counter)
- Labels: `stream_name`, `cdn_id`, `cache_status`, `pop`, `response_status`
- Query 4xx error rate: `sum(rate(cdn77_responses_total{stream_name="...", response_status=~"4.."}[5m]))`
- Query success rate: `sum(rate(cdn77_responses_total{stream_name="...", response_status="200"}[5m]))`

### Gauges (use with `avg_over_time()`)

**`cdn77_time_to_first_byte_ms`**
- Average time to first byte in milliseconds (gauge)
- Labels: `stream_name`, `cdn_id`, `cache_status`, `pop`
- Query average TTFB: `avg_over_time(cdn77_time_to_first_byte_ms{stream_name="..."}[5m])`
- Query by location: `avg by (pop) (avg_over_time(cdn77_time_to_first_byte_ms{stream_name="..."}[5m]))`

### Label Dimensions

All metrics include these labels for filtering and grouping:
- `stream_name` - 32-character hex stream ID extracted from path
- `cdn_id` - CDN77 resource ID
- `cache_status` - HIT, MISS, EXPIRED, etc.
- `pop` - Edge location (e.g., "losangelesUSCA", "frankfurtDEHE")
- `response_status` - HTTP status code (only on `cdn77_responses_total`)

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

### Python Script (Development)

```bash
# Install dependencies
pip install -r requirements.txt

# Run S3 importer daemon
python s3_importer.py
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

## How It Works

1. **Poll S3**: Every 60 seconds, polls for new `.ndjson.gz` files (current hour + 2-hour lookback)
2. **Download & Parse**: Downloads files chronologically (oldest first), decompresses, parses JSON line-by-line
3. **Event Creation**: Each JSON log entry becomes an Event object with timestamp, stream ID, bytes, TTFB, cache status, location, etc.
4. **Buffer Events**: All events from file buffered in memory
5. **Flush After Each File**: Events immediately flushed and aggregated by minute
6. **Multi-Metric Generation**: Single event stream generates multiple series.
7. **Timestamp Offset Strategy**: Prevents duplicate timestamp errors:
   - Random startup offset: 0-999ms (logged at startup)
   - Batch increment: +100ms per batch for same minute
   - Example: 04:27:00.437, 04:27:00.537, 04:27:00.637 (all within 1 second of actual time)
8. **Label Dimensions**: All metrics tagged with stream_name, cdn_id, cache_status, pop (location)
9. **Push to Prometheus**: Batched push via remote write API (protobuf + snappy compression)
10. **Delete & Repeat**: Removes processed file from S3, waits for next poll

**Processing Flow:**
```
S3 Bucket → Download (chronological) → Parse JSON → Event Buffer → 
Aggregate by Minute+Labels → Apply Offset → Push to Prometheus → Delete from S3
```

**Overlapping File Handling:**
- Files contain overlapping timestamps (~30s each, consecutive files overlap by seconds)
- Batch offset system allows multiple flushes per minute without duplicates
- Each flush gets unique timestamp via millisecond offsets
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

**View metrics being processed:**
```bash
docker logs cdn77-s3-importer | grep "PARSE SUCCESS"
docker logs cdn77-s3-importer | grep "Time range"
```

**Check for duplicate timestamp errors:**
```bash
docker logs cdn77-s3-importer | grep "duplicate sample"
```

**Restart container:**
```bash
docker restart cdn77-s3-importer
```

## Legacy API-Based Exporter

The original `exporter.py` script that uses the CDN77 API to fetch daily log files is still available but deprecated. Use `s3_importer.py` for real-time log processing.
