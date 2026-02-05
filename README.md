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

The exporter generates 12 metrics with rich dimensional labels (stream_name, cdn_id, cache_status, pop, response_status, device_type, country, region):

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

**`cdn77_viewers_total`**
- Count of unique IP addresses (viewers) per stream (counter)
- Labels: `stream_name`
- Query unique viewers per stream: `cdn77_viewers_total{stream_name="..."}`
- Query total unique viewers across all streams: `sum(cdn77_viewers_total)`
- Query unique viewers rate: `rate(cdn77_viewers_total{stream_name="..."}[5m])`

**`cdn77_viewers_by_device_total`**
- Count of unique IP addresses (viewers) per stream by device type (counter)
- Labels: `stream_name`, `device_type`
- Device types: `baron_mobile`, `apple_tv`, `roku`, `firestick`, `android_tv`, `web`, `other`
- Query Baron mobile vs OTT: `cdn77_viewers_by_device_total{stream_name="...", device_type=~"baron_mobile|apple_tv|roku|firestick"}`
- Query by platform: `sum by (device_type) (cdn77_viewers_by_device_total{stream_name="..."})`
- Compare platforms: `cdn77_viewers_by_device_total{stream_name="...", device_type="roku"}` vs `device_type="apple_tv"`

**`cdn77_viewers_by_country_total`**
- Count of unique IP addresses (viewers) per stream by country (counter)
- Labels: `stream_name`, `country`
- Country from CDN77 logs (ISO 3166-1 alpha-2)
- Query unique viewers per country: `cdn77_viewers_by_country_total{stream_name="...", country="US"}`
- Query top countries: `topk(10, cdn77_viewers_by_country_total{stream_name="..."})`
- Query growth rate: `rate(cdn77_viewers_by_country_total{stream_name="...", country="US"}[5m])`

**`cdn77_viewers_by_region_total`**
- Count of unique IP addresses (viewers) per stream by country and region/state (counter)
- Labels: `stream_name`, `country`, `region`
- Requires GeoIP database for region lookup (see setup below)
- Query unique viewers per region: `cdn77_viewers_by_region_total{stream_name="...", country="US", region="California"}`
- Query top regions: `topk(10, cdn77_viewers_by_region_total{stream_name="..."})`
- Query US states: `cdn77_viewers_by_region_total{stream_name="...", country="US"}`

### Gauges (use with `max_over_time()` or direct value)

**`cdn77_active_viewers`**
- Current unique viewers in rolling time window (gauge)
- Labels: `stream_name`, `window`
- Tracks unique IPs over 2-hour rolling window (handles retroactive data)
- Query current unique viewers: `cdn77_active_viewers{stream_name="...", window="2h"}`
- Query peak during broadcast: `max_over_time(cdn77_active_viewers{stream_name="...", window="2h"}[3h])`
- Query average concurrent: `avg_over_time(cdn77_active_viewers{stream_name="...", window="2h"}[1h])`

**`cdn77_active_viewers_by_device`**
- Current unique viewers by device type in rolling window (gauge)
- Labels: `stream_name`, `device_type`, `window`
- Query by platform: `cdn77_active_viewers_by_device{stream_name="...", device_type="roku", window="2h"}`
- Baron vs OTT: `sum by (device_type) (cdn77_active_viewers_by_device{stream_name="...", window="2h"})`

**`cdn77_active_viewers_by_country`**
- Current unique viewers by country in rolling window (gauge)
- Labels: `stream_name`, `country`, `window`
- Requires GeoIP database (see setup below)
- Query top countries: `topk(10, cdn77_active_viewers_by_country{stream_name="...", window="2h"})`
- Query specific country: `cdn77_active_viewers_by_country{stream_name="...", country="US", window="2h"}`

**`cdn77_active_viewers_by_region`**
- Current unique viewers by state/region in rolling window (gauge)
- Labels: `stream_name`, `country`, `region`, `window`
- Requires GeoIP database (see setup below)
- Query top regions: `topk(10, cdn77_active_viewers_by_region{stream_name="...", window="2h"})`
- Query US states: `cdn77_active_viewers_by_region{stream_name="...", country="US", window="2h"}`

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

For region-based metrics (`cdn77_viewers_by_region_total`, `cdn77_active_viewers_by_country`, `cdn77_active_viewers_by_region`), download the MaxMind GeoLite2 database:

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

**Performance Optimization:**
- Lookups happen **after deduplication** (once per unique IP, not per event)
- Results are **cached in memory** for the lifetime of the exporter
- Typical reduction: 90%+ fewer lookups vs naive per-event approach
- Example: 1000 events from 100 unique IPs = 100 lookups instead of 1000
- Memory overhead: ~70MB for database + ~120 bytes per cached IP
- Lookup speed: 1-5 microseconds per uncached IP

**Without GeoIP:** Region-based counter metrics won't be generated; gauge metrics will show country="XX" and region="Unknown"

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
- `SESSION_WINDOW_SECONDS` - Rolling window for active viewer tracking (default: 7200 = 2 hours)
- `GEOIP_DB_PATH` - Path to MaxMind GeoLite2-City.mmdb database (optional, for geographic metrics)

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
- **GeoIP Cache**: ~120 bytes per unique IP address (persistent across batches)
- **Processing**: ~1 second per file (download, parse, aggregate, push)
- **GeoIP Optimization**: 90%+ reduction via deduplication + in-memory caching
- **Compression**: ~88% (protobuf + snappy for Prometheus remote write)
- **Latency**: ~30-90 seconds from log generation to Prometheus ingestion
- **Backfill**: 7-day window (older data filtered by Prometheus)

## Troubleshooting

**Check logs:**
```bash
docker logs -f cdn77-s3-importer
```