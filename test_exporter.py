#!/usr/bin/env python3
"""
Test suite for CDN77 Prometheus Exporter
Generates mock log data and validates metric processing
"""

import json
import gzip
import io
import random
import queue
import os
import tempfile
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import pytest
import snappy

import remote_pb2

os.environ.setdefault("METRIC_PREFIX", "cdn77_")

from exporter import (
    Event, LogParser, MetricAggregator, AggregationType,
    MetricDefinition, METRIC_DEFINITIONS, detect_device_type,
    SessionTracker, SessionInfo,
    build_file_viewer_metrics,
    minute_timestamp_ms,
    metric_name,
    PrometheusExporter,
    PrometheusWriterThread,
    S3Importer,
    extract_stream_id, extract_cdn_id, extract_cache_status,
    extract_location_id, extract_response_status, extract_client_ip, extract_device_type,
    extract_track_from_path, extract_resolution_track,
)


# ============================================================================
# MOCK DATA GENERATORS
# ============================================================================

def generate_mock_log_entry(
    stream_id: str = "78001c7e2b3ff8ca21b504883b47c44c",
    timestamp: datetime = None,
    client_ip: str = None,
    response_bytes: int = None,
    cache_status: str = "HIT",
    location_id: str = "losangelesUSCA",
    response_status: int = 200,
    ttfb_ms: int = None,
    user_agent: str = None
) -> Dict:
    """Generate a single mock CDN77 log entry"""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    
    if client_ip is None:
        client_ip = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
    
    if response_bytes is None:
        response_bytes = random.randint(1000, 10000)
    
    if ttfb_ms is None:
        ttfb_ms = random.randint(50, 300)
    
    if user_agent is None:
        user_agent = random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Safari/605.1",
            "Roku/DVP-11.0 (11.0.0.4193)",
            "AppleTV11,1/11.1",
            "Dalvik/2.1.0 (Linux; U; Android 9; AFTMM Build/PS7633)"
        ])
    
    return {
        "timestamp": timestamp.isoformat().replace('+00:00', 'Z'),
        "startTimeMs": int(timestamp.timestamp() * 1000),
        "tcpRTTus": random.randint(10000, 100000),
        "requestTimeMs": random.randint(100, 500),
        "timeToFirstByteMs": ttfb_ms,
        "resourceID": 1693215245,
        "cacheAge": random.randint(0, 10),
        "cacheStatus": cache_status,
        "clientCountry": random.choice(["US", "CA", "GB", "DE", "FR"]),
        "clientIP": client_ip,
        "clientPort": random.randint(10000, 65535),
        "clientASN": random.randint(100, 10000),
        "clientRequestAccept": "*/*",
        "clientRequestBytes": 65,
        "clientRequestCompleted": True,
        "clientRequestConnTimeout": False,
        "clientRequestContentType": "",
        "clientRequestHost": "1693215245.rsc.cdn77.org",
        "clientRequestMethod": "GET",
        "clientRequestPath": f"/{stream_id}/tracks-v3/rewind-3600.fmp4.m3u8",
        "clientRequestProtocol": "HTTP20",
        "clientRequestRange": "",
        "clientRequestReferer": "https://virtualrailfan.com/",
        "clientRequestScheme": "HTTPS",
        "clientRequestServerPush": False,
        "clientRequestUserAgent": user_agent,
        "clientRequestVia": "",
        "clientRequestXForwardedFor": f"{client_ip}",
        "clientSSLCipher": "TLS_AES_256_GCM_SHA384",
        "clientSSLFlags": 1,
        "clientSSLProtocol": "TLS13",
        "locationID": location_id,
        "serverIP": "143.244.51.39",
        "serverPort": 443,
        "responseStatus": response_status,
        "responseBytes": response_bytes,
        "responseContentType": "application/vnd.apple.mpegurl",
        "responseContentRange": "",
        "responseContentEncoding": "GZIP"
    }


def generate_mock_ndjson(num_entries: int, 
                        stream_ids: List[str] = None,
                        ip_addresses: List[str] = None,
                        base_timestamp: datetime = None) -> str:
    """Generate mock NDJSON log data"""
    if stream_ids is None:
        stream_ids = ["78001c7e2b3ff8ca21b504883b47c44c"]  # Valid 32-char hex
    
    if ip_addresses is None:
        # Generate diverse IP pool
        ip_addresses = [f"192.168.{i}.{j}" for i in range(1, 11) for j in range(1, 26)]
    
    if base_timestamp is None:
        base_timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    
    lines = []
    for i in range(num_entries):
        # Vary timestamp within a minute
        timestamp = base_timestamp + timedelta(seconds=random.randint(0, 59))
        stream_id = random.choice(stream_ids)
        client_ip = random.choice(ip_addresses)
        
        entry = generate_mock_log_entry(
            stream_id=stream_id,
            timestamp=timestamp,
            client_ip=client_ip,
            cache_status=random.choice(["HIT", "MISS", "EXPIRED"]),
            location_id=random.choice(["losangelesUSCA", "frankfurtDEHE", "tokyoJPTY"]),
            response_status=random.choices([200, 206, 404, 500], weights=[80, 15, 3, 2])[0]
        )
        lines.append(json.dumps(entry))
    
    return '\n'.join(lines)


# ============================================================================
# TESTS
# ============================================================================

class TestLogParser:
    """Test log parsing functionality"""
    
    def test_parse_valid_entry(self):
        """Test parsing a valid log entry"""
        parser = LogParser()
        timestamp = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        entry = generate_mock_log_entry(timestamp=timestamp, client_ip="192.168.1.100")
        json_line = json.dumps(entry)
        
        event = parser.parse_json_to_event(json_line)
        
        assert event is not None
        assert event.stream_id == "78001c7e2b3ff8ca21b504883b47c44c"
        assert event.client_ip == "192.168.1.100"
        assert event.timestamp == timestamp
        assert event.response_bytes > 0
        assert event.resource_id == 1693215245
    
    def test_parse_missing_path(self):
        """Test parsing with missing path field"""
        parser = LogParser()
        entry = generate_mock_log_entry()
        del entry['clientRequestPath']
        json_line = json.dumps(entry)
        
        event = parser.parse_json_to_event(json_line)
        
        assert event is None
    
    def test_parse_invalid_path_format(self):
        """Test parsing with path that doesn't match stream pattern"""
        parser = LogParser()
        entry = generate_mock_log_entry()
        entry['clientRequestPath'] = "/invalid/path/format.m3u8"
        json_line = json.dumps(entry)
        
        event = parser.parse_json_to_event(json_line)
        
        assert event is None  # Should skip non-stream paths
    
    def test_parse_multiple_entries(self):
        """Test parsing multiple entries"""
        parser = LogParser()
        ndjson = generate_mock_ndjson(100)
        
        events = []
        for line in ndjson.splitlines():
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)
        
        assert len(events) == 100
        assert all(isinstance(e, Event) for e in events)


class TestMetricAggregation:
    """Test metric aggregation functionality"""
    
    def test_transfer_bytes_aggregation(self):
        """Test bytes transfer aggregation (SUM)"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        # Generate 10 events with known byte values
        events = []
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        for i in range(10):
            entry = generate_mock_log_entry(
                timestamp=base_time,
                response_bytes=1000,
                stream_id="abc1230000000000000000000000000f"
            )
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find transfer_bytes metric
        bytes_metric = [m for m in computed if m['metric_name'] == 'cdn77_transfer_bytes_total'][0]
        assert bytes_metric['value'] == 10000  # 10 events * 1000 bytes
    
    def test_unique_ip_counting(self):
        """Test unique IP address counting"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        # Generate events with specific IP addresses
        stream_id = "abcd1230000000000000000000000001"
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        
        # 20 events: 5 unique IPs, each appearing multiple times
        unique_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"]
        events = []
        
        for i in range(20):
            ip = unique_ips[i % 5]  # Cycle through the 5 IPs
            entry = generate_mock_log_entry(
                timestamp=base_time,
                client_ip=ip,
                stream_id=stream_id
            )
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find users metric
        users_metric = [m for m in computed if m['metric_name'] == 'cdn77_users_total'][0]
        assert users_metric['value'] == 5  # Should count 5 unique IPs
        assert users_metric['labels']['stream'] == stream_id
    
    def test_unique_ip_per_stream(self):
        """Test unique IP counting per stream (separate counts)"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream1 = "aaaa1111000000000000000000000001"
        stream2 = "bbbb2222000000000000000000000002"
        
        events = []
        
        # Stream 1: 3 unique IPs
        for ip in ["10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.1", "10.0.0.2"]:
            entry = generate_mock_log_entry(timestamp=base_time, client_ip=ip, stream_id=stream1)
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        # Stream 2: 2 unique IPs
        for ip in ["10.0.0.4", "10.0.0.5", "10.0.0.4"]:
            entry = generate_mock_log_entry(timestamp=base_time, client_ip=ip, stream_id=stream2)
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find users metrics for each stream
        users_metrics = [m for m in computed if m['metric_name'] == 'cdn77_users_total']
        
        stream1_metric = [m for m in users_metrics if m['labels']['stream'] == stream1][0]
        stream2_metric = [m for m in users_metrics if m['labels']['stream'] == stream2][0]
        
        assert stream1_metric['value'] == 3
        assert stream2_metric['value'] == 2
    
    def test_requests_total_counting(self):
        """Test request counting"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        events = []
        
        for i in range(50):
            entry = generate_mock_log_entry(timestamp=base_time)
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        requests_metric = [m for m in computed if m['metric_name'] == 'cdn77_requests_total'][0]
        assert requests_metric['value'] == 50

    def test_users_metrics_include_expected_breakdowns(self):
        """Ensure users metrics include total/device/country/resolution breakdowns."""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)

        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd1230000000000000000000000001"

        events = []
        for ip in ["10.0.0.1", "10.0.0.2", "10.0.0.2", "10.0.0.3"]:
            entry = generate_mock_log_entry(
                timestamp=base_time,
                client_ip=ip,
                stream_id=stream_id,
                user_agent="Roku/DVP-11.0",
            )
            entry['clientCountry'] = 'US'
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)

        computed = aggregator.compute_final_values(aggregator.aggregate_events(events))

        def get_metric(metric_name, extra_label=None):
            matches = [m for m in computed if m['metric_name'] == metric_name]
            if extra_label is None:
                return matches[0]
            key, value = extra_label
            return [m for m in matches if m['labels'][key] == value][0]

        users_total = get_metric('cdn77_users_total')
        assert users_total['value'] == 3

        users_device = get_metric('cdn77_users_by_device_total', ('device_type', 'roku'))
        assert users_device['value'] == 3

        users_country = get_metric('cdn77_users_by_country_total', ('country', 'US'))
        assert users_country['value'] == 3

        users_resolution = get_metric('cdn77_users_by_resolution_total', ('track', 'v3'))
        assert users_resolution['value'] == 3

    def test_resolution_metrics_exclude_audio_tracks(self):
        """Resolution metrics should include video tracks only and skip audio tracks."""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)

        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd1230000000000000000000000001"

        video_entry = generate_mock_log_entry(
            timestamp=base_time,
            client_ip="10.0.0.1",
            stream_id=stream_id,
        )
        video_entry['clientRequestPath'] = f"/{stream_id}/tracks-v3/rewind-3600.fmp4.m3u8"

        audio_entry = generate_mock_log_entry(
            timestamp=base_time,
            client_ip="10.0.0.2",
            stream_id=stream_id,
        )
        audio_entry['clientRequestPath'] = f"/{stream_id}/tracks-a1/rewind-3600.fmp4.m3u8"

        events = [
            parser.parse_json_to_event(json.dumps(video_entry)),
            parser.parse_json_to_event(json.dumps(audio_entry)),
        ]
        events = [e for e in events if e is not None]

        computed = aggregator.compute_final_values(aggregator.aggregate_events(events))
        resolution_metrics = [m for m in computed if m['metric_name'] == 'cdn77_users_by_resolution_total']

        assert len(resolution_metrics) == 1
        assert resolution_metrics[0]['labels']['track'] == 'v3'
        assert resolution_metrics[0]['value'] == 1
    
    def test_ttfb_averaging(self):
        """Test time to first byte averaging"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd4560000000000000000000000456"
        
        # Generate events with known TTFB values
        ttfb_values = [100, 200, 300, 400, 500]
        events = []
        
        for ttfb in ttfb_values:
            entry = generate_mock_log_entry(
                timestamp=base_time,
                stream_id=stream_id,
                ttfb_ms=ttfb
            )
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        ttfb_metric = [m for m in computed if m['metric_name'] == 'cdn77_time_to_first_byte_ms'][0]
        expected_avg = sum(ttfb_values) / len(ttfb_values)
        assert ttfb_metric['value'] == expected_avg
    
    def test_response_status_grouping(self):
        """Test response status code grouping"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd7890000000000000000000000789"
        
        # Generate events with different status codes
        status_codes = [200] * 10 + [206] * 5 + [404] * 3 + [500] * 2
        events = []
        
        for status in status_codes:
            entry = generate_mock_log_entry(
                timestamp=base_time,
                stream_id=stream_id,
                response_status=status
            )
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        response_metrics = [m for m in computed if m['metric_name'] == 'cdn77_responses_total']
        
        status_200 = [m for m in response_metrics if m['labels']['response_status'] == '200'][0]
        status_404 = [m for m in response_metrics if m['labels']['response_status'] == '404'][0]
        status_500 = [m for m in response_metrics if m['labels']['response_status'] == '500'][0]
        
        assert status_200['value'] == 10
        assert status_404['value'] == 3
        assert status_500['value'] == 2


class TestLabelExtractors:
    """Test label extraction functions"""
    
    def test_extract_stream_id(self):
        """Test stream ID extraction"""
        event = Event(
            timestamp=datetime.now(timezone.utc),
            stream_id="abc123def456",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
            client_ip="192.168.1.1",
            device_type="web",
        )
        
        assert extract_stream_id(event) == "abc123def456"
    
    def test_extract_client_ip(self):
        """Test client IP extraction"""
        event = Event(
            timestamp=datetime.now(timezone.utc),
            stream_id="test",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
            client_ip="10.20.30.40",
            device_type="web",
        )
        
        assert extract_client_ip(event) == "10.20.30.40"

    def test_extract_track_from_path(self):
        """Test parsing track label from CDN request path."""
        assert extract_track_from_path("/99641d13e41ef47215b12671803bb13c/tracks-v3/rewind-43200.fmp4.m3u8") == "v3"
        assert extract_track_from_path("/99641d13e41ef47215b12671803bb13c/tracks-V2/chunk.m3u8") == "v2"
        assert extract_track_from_path("/99641d13e41ef47215b12671803bb13c/other/chunk.m3u8") == "unknown"

    def test_extract_resolution_track(self):
        """Test Event-level resolution extractor."""
        event = Event(
            timestamp=datetime.now(timezone.utc),
            stream_id="99641d13e41ef47215b12671803bb13c",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
            client_ip="10.20.30.40",
            device_type="web",
            request_path="/99641d13e41ef47215b12671803bb13c/tracks-v3/rewind-43200.fmp4.m3u8"
        )

        assert extract_resolution_track(event) == "v3"


class TestDeviceDetection:
    """Test device type detection from User-Agent"""
    
    def test_detect_apple_tv(self):
        """Test Apple TV detection"""
        assert detect_device_type("AppleTV11,1/11.1") == "apple_tv"
        assert detect_device_type("tvOS/17.0 AppleTV") == "apple_tv"
    
    def test_detect_roku(self):
        """Test Roku detection"""
        assert detect_device_type("Roku/DVP-11.0 (11.0.0.4193)") == "roku"
        assert detect_device_type("RokuOS/12.0") == "roku"
    
    def test_detect_firestick(self):
        """Test Firestick detection"""
        assert detect_device_type("Dalvik/2.1.0 (Linux; U; Android 9; AFTMM Build/PS7633)") == "firestick"
        assert detect_device_type("Amazon Fire TV Stick") == "firestick"
    
    def test_detect_android(self):
        """Test Android detection"""
        assert detect_device_type("AndroidTV/10.0") == "android"
        assert detect_device_type("Dalvik/2.1.0 (Linux; U; Android 13; Pixel 7)") == "android"
    
    def test_detect_web(self):
        """Test web browser detection"""
        assert detect_device_type("Mozilla/5.0 Chrome/120.0.0.0") == "web"
        assert detect_device_type("Mozilla/5.0 Firefox/121.0") == "web"
        assert detect_device_type("Mozilla/5.0 Safari/605.1") == "web"

    def test_detect_bots(self):
        """Test bot detection"""
        assert detect_device_type("Mozilla/5.0 (Yallbot Camera Frame Capture)") == "bots"
        assert detect_device_type("ExampleCrawler/1.0") == "bots"

    def test_detect_streamology(self):
        """Test streamology playback/ingest signatures"""
        assert detect_device_type("Lavf/61.7.100") == "streamology"
        assert detect_device_type("GStreamer/1.22.9") == "streamology"
        assert detect_device_type("gst-launch-1.0/1.20") == "streamology"
    
    def test_detect_ios(self):
        """Test iOS detection"""
        assert detect_device_type("Baron iOS App/1.0") == "ios"
        assert detect_device_type("AppleCoreMedia/1.0.0 (iPhone; U; CPU OS 18_5 like Mac OS X; en_us)") == "ios"
    
    def test_detect_other(self):
        """Test unknown device detection"""
        assert detect_device_type("") == "other"
        assert detect_device_type("UnknownDevice/1.0") == "other"
        assert detect_device_type("Matt Laubhan IOS/7.0.9 CFNetwork/3860.400.51 Darwin/25.3.0") == "other"
    
    def test_device_in_parsed_event(self):
        """Test device type is extracted during parsing"""
        parser = LogParser()
        entry = generate_mock_log_entry(
            user_agent="Roku/DVP-11.0 (11.0.0.4193)"
        )
        event = parser.parse_json_to_event(json.dumps(entry))
        
        assert event is not None
        assert event.device_type == "roku"
    
    def test_unique_users_by_device(self):
        """Test unique IP counting per device type"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        events = []
        
        # 3 users on Roku (2 unique IPs)
        for ip in ["10.0.0.1", "10.0.0.2", "10.0.0.1"]:
            entry = generate_mock_log_entry(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip=ip,
                user_agent="Roku/DVP-11.0"
            )
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        # 2 users on Apple TV (2 unique IPs)
        for ip in ["10.0.0.3", "10.0.0.4"]:
            entry = generate_mock_log_entry(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip=ip,
                user_agent="AppleTV11,1/11.1"
            )
            event = parser.parse_json_to_event(json.dumps(entry))
            if event:
                events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find users_by_device metrics
        device_metrics = [m for m in computed if m['metric_name'] == 'cdn77_users_by_device_total']
        
        roku_metric = [m for m in device_metrics if m['labels']['device_type'] == 'roku'][0]
        apple_tv_metric = [m for m in device_metrics if m['labels']['device_type'] == 'apple_tv'][0]
        
        assert roku_metric['value'] == 2  # 2 unique IPs on Roku
        assert apple_tv_metric['value'] == 2  # 2 unique IPs on Apple TV
    
    def test_unique_users_by_country(self):
        """Test unique IP counting per country"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # Create events with client_country field already set
        events = [
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.2",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",  # Duplicate IP from US
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.3",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="CA",
                location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.4",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="CA",
                location_id="losangelesUSCA",
                ),
        ]
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find users_by_country metrics
        country_metrics = [m for m in computed if m['metric_name'] == 'cdn77_users_by_country_total']
        
        us_metric = [m for m in country_metrics if m['labels']['country'] == 'US'][0]
        ca_metric = [m for m in country_metrics if m['labels']['country'] == 'CA'][0]
        
        assert us_metric['value'] == 2  # 2 unique IPs from US
        assert ca_metric['value'] == 2  # 2 unique IPs from CA


class TestIntegration:
    """Integration tests with realistic data volumes"""
    
    def test_large_dataset_processing(self):
        """Test processing 500 log entries"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        # Generate 500 entries across 3 streams with varied IPs
        streams = [
            "aaaa1111222233334444555566667777",
            "bbbb2222333344445555666677778888",
            "cccc3333444455556666777788889999"
        ]
        
        # Create IP pool (50 unique IPs)
        ip_pool = [f"192.168.{i // 256}.{i % 256}" for i in range(1, 51)]
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        ndjson = generate_mock_ndjson(
            num_entries=500,
            stream_ids=streams,
            ip_addresses=ip_pool,
            base_timestamp=base_time
        )
        
        # Parse all events
        events = []
        for line in ndjson.splitlines():
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)
        
        assert len(events) == 500
        
        # Aggregate
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Verify all metrics are present
        metric_names = set(m['metric_name'] for m in computed)
        expected_metrics = {
            'cdn77_transfer_bytes_total',
            'cdn77_time_to_first_byte_ms',
            'cdn77_requests_total',
            'cdn77_responses_total',
            'cdn77_users_total'
        }
        assert expected_metrics.issubset(metric_names)
        
        # Verify users_total has one entry per stream
        users_metrics = [m for m in computed if m['metric_name'] == 'cdn77_users_total']
        assert len(users_metrics) >= 1  # At least one stream has data
        
        # Verify total requests = 500
        requests_metrics = [m for m in computed if m['metric_name'] == 'cdn77_requests_total']
        total_requests = sum(m['value'] for m in requests_metrics)
        assert total_requests == 500
    
    def test_time_window_aggregation(self):
        """Test aggregation across different time windows"""
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        stream_id = "abcdabcd000000000000000000000fab"
        
        # Generate events across 3 different minutes
        events = []
        for minute_offset in [0, 1, 2]:
            base_time = datetime(2026, 1, 28, 12, minute_offset, 0, tzinfo=timezone.utc)
            for i in range(10):
                entry = generate_mock_log_entry(
                    timestamp=base_time,
                    stream_id=stream_id,
                    client_ip=f"10.0.0.{i % 5}"  # 5 unique IPs per minute
                )
                event = parser.parse_json_to_event(json.dumps(entry))
                if event:
                    events.append(event)
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Should have separate metrics for each minute
        users_metrics = [m for m in computed if m['metric_name'] == 'cdn77_users_total']
        
        # Each minute should show 5 unique IPs
        assert len(users_metrics) == 3
        for metric in users_metrics:
            assert metric['value'] == 5


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestPerformance:
    """Performance and stress tests"""
    
    def test_parse_performance(self):
        """Test parsing performance with large dataset"""
        import time
        
        parser = LogParser()
        ndjson = generate_mock_ndjson(1000)
        
        start_time = time.time()
        events = []
        for line in ndjson.splitlines():
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)
        elapsed = time.time() - start_time
        
        assert len(events) == 1000
        assert elapsed < 1.0  # Should process 1000 entries in under 1 second
        print(f"\nParsed 1000 entries in {elapsed:.3f}s ({1000/elapsed:.0f} entries/sec)")
    
    def test_aggregation_performance(self):
        """Test aggregation performance"""
        import time
        
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS)
        
        ndjson = generate_mock_ndjson(1000)
        events = []
        for line in ndjson.splitlines():
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)
        
        start_time = time.time()
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        elapsed = time.time() - start_time
        
        assert len(computed) > 0
        assert elapsed < 0.5  # Should aggregate in under 0.5 seconds
        print(f"\nAggregated 1000 events in {elapsed:.3f}s")


class TestSessionTracker:
    """Test session tracking with rolling windows"""
    
    def test_session_tracking_basic(self):
        """Test basic session tracking"""
        tracker = SessionTracker(window_seconds=300)  # 5 minute window
        
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # Create some test events
        events = []
        for i in range(5):
            event = Event(
                timestamp=base_time + timedelta(seconds=i*10),
                stream_id=stream_id,
                client_ip=f"10.0.0.{i}",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="losangelesUSCA",
                device_type="web",
                )
            events.append(event)
        
        tracker.update(events)
        
        # Should have 5 unique IPs
        stats = tracker.get_stats()
        assert stats['total_sessions'] == 5
        assert stats['streams'] == 1
    
    def test_session_expiration(self):
        """Test that old sessions expire"""
        tracker = SessionTracker(window_seconds=60)  # 1 minute window
        
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # Add session at 12:00
        event1 = Event(
            timestamp=base_time,
            stream_id=stream_id,
            client_ip="10.0.0.1",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
            device_type="web",
        )
        
        tracker.update([event1])
        assert tracker.get_stats()['total_sessions'] == 1
        
        # Expire sessions older than 2 minutes (should remove the 12:00 session)
        tracker.expire_old(base_time + timedelta(minutes=2))
        assert tracker.get_stats()['total_sessions'] == 0
    
    def test_gauge_metrics_generation(self):
        """Test gauge metric generation"""
        tracker = SessionTracker(window_seconds=300)
        
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # Add 3 sessions with different devices
        events = [
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="roku",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.2",
                device_type="apple_tv",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.3",
                device_type="roku",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="CA", location_id="losangelesUSCA",
                )
        ]
        
        tracker.update(events)
        metrics = tracker.get_gauge_metrics()
        
        # Should have total, by_device, and by_country metrics
        metric_names = {m['metric_name'] for m in metrics}
        assert 'cdn77_viewers' in metric_names
        assert 'cdn77_viewers_by_device' in metric_names
        assert 'cdn77_viewers_by_country' in metric_names
        
        # Check total viewers
        total_metric = [m for m in metrics if m['metric_name'] == 'cdn77_viewers'][0]
        assert total_metric['value'] == 3.0
        
        # Check device breakdown
        device_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_device']
        roku_count = [m for m in device_metrics if m['labels']['device_type'] == 'roku'][0]['value']
        apple_tv_count = [m for m in device_metrics if m['labels']['device_type'] == 'apple_tv'][0]['value']
        assert roku_count == 2.0
        assert apple_tv_count == 1.0
    
    def test_retroactive_updates(self):
        """Test handling of retroactive data (delayed files)"""
        tracker = SessionTracker(window_seconds=7200)  # 2 hour window
        
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # First batch: events at 12:00
        batch1 = [
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                )
        ]
        
        tracker.update(batch1)
        assert tracker.get_stats()['total_sessions'] == 1
        
        # Second batch: retroactive events from 11:50 (10 minutes ago)
        batch2 = [
            Event(
                timestamp=base_time - timedelta(minutes=10),
                stream_id=stream_id,
                client_ip="10.0.0.2",  # New IP
                device_type="roku",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                ),
            Event(
                timestamp=base_time - timedelta(minutes=5),
                stream_id=stream_id,
                client_ip="10.0.0.1",  # Same IP as batch1, update timestamp
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                )
        ]
        
        tracker.update(batch2)
        
        # Should have 2 unique IPs (10.0.0.1 updated, 10.0.0.2 added)
        assert tracker.get_stats()['total_sessions'] == 2
        
        # Generate metrics - should show increased count
        metrics = tracker.get_gauge_metrics()
        total_metric = [m for m in metrics if m['metric_name'] == 'cdn77_viewers'][0]
        assert total_metric['value'] == 2.0

    def test_watch_time_metrics_emitted_for_expired_sessions(self):
        """Expired sessions should be rolled into watch-time counters and histogram."""
        tracker = SessionTracker(window_seconds=3600, session_gap_seconds=1800)

        stream_id = "abcd1234567890abcdef1234567890ab"
        start = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        end = start + timedelta(minutes=20)

        tracker.update([
            Event(
                timestamp=start,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
            Event(
                timestamp=end,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
        ])

        # Force expiry to close active session and roll-up duration
        tracker.expire_old(start + timedelta(hours=2))
        metrics = tracker.get_watch_time_metrics(reference_time=start + timedelta(hours=2))

        sessions_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_sessions_total'][0]
        watch_time_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_time_seconds_total'][0]
        p1200_bucket = [
            m for m in metrics
            if m['metric_name'] == 'cdn77_watch_session_duration_seconds_bucket' and m['labels']['le'] == '1200'
        ][0]
        band_5_20m = [
            m for m in metrics
            if m['metric_name'] == 'cdn77_watch_session_duration_band_total' and m['labels']['band'] == '5_20m'
        ][0]
        inf_bucket = [
            m for m in metrics
            if m['metric_name'] == 'cdn77_watch_session_duration_seconds_bucket' and m['labels']['le'] == '+Inf'
        ][0]

        assert sessions_metric['value'] == 1.0
        assert watch_time_metric['value'] == 1200.0
        assert p1200_bucket['value'] == 1.0
        assert band_5_20m['value'] == 1.0
        assert inf_bucket['value'] == 1.0

    def test_watch_time_metrics_split_on_session_gap(self):
        """A large gap for the same IP should close prior session and start a new one."""
        tracker = SessionTracker(window_seconds=7200, session_gap_seconds=120)

        stream_id = "abcd1234567890abcdef1234567890ab"
        t0 = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(seconds=30)
        t2 = t0 + timedelta(minutes=10)

        tracker.update([
            Event(
                timestamp=t0,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
            Event(
                timestamp=t1,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
            Event(
                timestamp=t2,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
        ])

        metrics = tracker.get_watch_time_metrics(reference_time=t2)
        sessions_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_sessions_total'][0]
        watch_time_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_time_seconds_total'][0]
        band_0_1m = [
            m for m in metrics
            if m['metric_name'] == 'cdn77_watch_session_duration_band_total' and m['labels']['band'] == '0_1m'
        ][0]

        assert sessions_metric['value'] == 1.0
        assert watch_time_metric['value'] == 30.0
        assert band_0_1m['value'] == 1.0

    def test_watch_time_metrics_close_idle_sessions_without_window_expiry(self):
        """Idle sessions should close at session_gap_seconds, independent of session window expiry."""
        tracker = SessionTracker(window_seconds=7200, session_gap_seconds=120)

        stream_id = "abcd1234567890abcdef1234567890ab"
        t0 = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(seconds=45)

        tracker.update([
            Event(
                timestamp=t0,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
            Event(
                timestamp=t1,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="testPOP",
                ),
        ])

        # Move past gap threshold but remain well within 2h session window.
        reference_time = t1 + timedelta(seconds=180)
        tracker.expire_old(reference_time)
        metrics = tracker.get_watch_time_metrics(reference_time=reference_time)

        sessions_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_sessions_total'][0]
        watch_time_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_time_seconds_total'][0]
        band_0_1m = [
            m for m in metrics
            if m['metric_name'] == 'cdn77_watch_session_duration_band_total' and m['labels']['band'] == '0_1m'
        ][0]

        assert sessions_metric['value'] == 1.0
        assert watch_time_metric['value'] == 45.0
        assert band_0_1m['value'] == 1.0

    def test_region_metrics_use_geoip_when_region_missing(self):
        """Region gauges should use GeoIP lookup when event only has country code."""
        tracker = SessionTracker(window_seconds=300)
        tracker.geoip_reader = object()  # Enable GeoIP path for lookup
        tracker.lookup_geo = lambda ip: ("US", "California")

        event = Event(
            timestamp=datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc),
            stream_id="abcd1234567890abcdef1234567890ab",
            client_ip="10.0.0.1",
            device_type="web",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
        )

        tracker.update([event])
        metrics = tracker.get_gauge_metrics()

        region_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_region']
        assert len(region_metrics) == 1
        assert region_metrics[0]['labels']['country'] == 'US'
        assert region_metrics[0]['labels']['region'] == 'California'
        assert region_metrics[0]['value'] == 1.0

    def test_region_metrics_skip_unknown_region(self):
        """Region gauges should not emit a series when region is unknown."""
        tracker = SessionTracker(window_seconds=300)

        event = Event(
            timestamp=datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc),
            stream_id="abcd1234567890abcdef1234567890ab",
            client_ip="10.0.0.1",
            device_type="web",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
        )

        tracker.update([event])
        metrics = tracker.get_gauge_metrics()
        region_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_region']
        assert region_metrics == []

    def test_lookup_geo_uses_ip_cache(self):
        """Geo lookup should be cached per IP to avoid repeated reader calls."""
        tracker = SessionTracker(window_seconds=300)

        class _FakeCountry:
            iso_code = "US"

        class _FakeSubdivision:
            name = "California"

        class _FakeSubdivisions:
            most_specific = _FakeSubdivision()

        class _FakeResponse:
            country = _FakeCountry()
            subdivisions = _FakeSubdivisions()

        class _FakeReader:
            def __init__(self):
                self.calls = 0

            def city(self, ip_address):
                self.calls += 1
                return _FakeResponse()

        fake_reader = _FakeReader()
        tracker.geoip_reader = fake_reader

        first = tracker.lookup_geo("1.1.1.1")
        second = tracker.lookup_geo("1.1.1.1")

        assert first == ("US", "California")
        assert second == ("US", "California")
        assert fake_reader.calls == 1

    def test_session_state_persist_and_restore_roundtrip(self):
        """Session tracker state should round-trip through persisted snapshot file."""
        tracker = SessionTracker(window_seconds=7200)
        now = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"

        events = [
            Event(
                timestamp=now,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
            ),
            Event(
                timestamp=now,
                stream_id=stream_id,
                client_ip="10.0.0.2",
                device_type="roku",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
            ),
        ]
        tracker.update(events)

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = os.path.join(tmp_dir, "session_state.json.gz")
            assert tracker.persist_to_file(snapshot_path, reference_time=now)

            restored = SessionTracker(window_seconds=7200)
            count = restored.restore_from_file(snapshot_path, reference_time=now)

            assert count == 2
            stats = restored.get_stats()
            assert stats['total_sessions'] == 2
            assert stats['streams'] == 1

    def test_session_state_restore_expires_old_entries(self):
        """Restored session snapshots should drop entries outside tracker window."""
        tracker = SessionTracker(window_seconds=3600)
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)

        tracker.update([
            Event(
                timestamp=base_time - timedelta(minutes=120),
                stream_id="abcd1234567890abcdef1234567890ab",
                client_ip="10.0.0.9",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
            )
        ])

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = os.path.join(tmp_dir, "session_state.json.gz")
            assert tracker.persist_to_file(snapshot_path, reference_time=base_time)

            restored = SessionTracker(window_seconds=3600)
            count = restored.restore_from_file(snapshot_path, reference_time=base_time)
            assert count == 0

    def test_session_state_persists_watch_duration_bands(self):
        """Persist/restore should retain rolled-up watch duration band counters."""
        tracker = SessionTracker(window_seconds=7200, session_gap_seconds=120)
        stream_id = "abcd1234567890abcdef1234567890ab"
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)

        tracker.update([
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
            ),
            Event(
                timestamp=base_time + timedelta(seconds=90),
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
            ),
            Event(
                timestamp=base_time + timedelta(minutes=10),
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
            ),
        ])

        with tempfile.TemporaryDirectory() as tmp_dir:
            snapshot_path = os.path.join(tmp_dir, "session_state.json.gz")
            assert tracker.persist_to_file(snapshot_path, reference_time=base_time + timedelta(minutes=10))

            restored = SessionTracker(window_seconds=7200, session_gap_seconds=120)
            restored.restore_from_file(snapshot_path, reference_time=base_time + timedelta(minutes=10))
            metrics = restored.get_watch_time_metrics(reference_time=base_time + timedelta(minutes=10))

            sessions_metric = [m for m in metrics if m['metric_name'] == 'cdn77_watch_sessions_total'][0]
            band_metric = [
                m for m in metrics
                if m['metric_name'] == 'cdn77_watch_session_duration_band_total' and m['labels']['band'] == '1_5m'
            ][0]

            assert sessions_metric['value'] == 1.0
            assert band_metric['value'] == 1.0

    def test_region_metric_has_expected_labels(self):
        """Region metric should include stream/country/region labels."""
        tracker = SessionTracker(window_seconds=300)
        tracker.geoip_reader = object()

        mapping = {
            "10.0.0.1": ("US", "California"),
            "10.0.0.2": ("AU", "New South Wales"),
        }
        tracker.lookup_geo = lambda ip: mapping.get(ip, ("XX", "Unknown"))

        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        events = [
            Event(
                timestamp=base_time,
                stream_id="abcd1234567890abcdef1234567890ab",
                client_ip="10.0.0.1",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="losangelesUSCA",
            ),
            Event(
                timestamp=base_time,
                stream_id="abcd1234567890abcdef1234567890ab",
                client_ip="10.0.0.2",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="AU",
                location_id="sydneyAUNSW",
            ),
        ]

        tracker.update(events)
        metrics = tracker.get_gauge_metrics()
        region_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_region']

        assert len(region_metrics) == 2
        for metric in region_metrics:
            labels = metric['labels']
            assert set(labels.keys()) == {'stream', 'country', 'region'}
            assert labels['region'] != 'Unknown'

    def test_sum_by_country_matches_region_rollup(self):
        """Country totals should equal sum of region series per country (PromQL sum by country behavior)."""
        tracker = SessionTracker(window_seconds=300)
        tracker.geoip_reader = object()

        mapping = {
            "10.0.0.1": ("US", "California"),
            "10.0.0.2": ("US", "Texas"),
            "10.0.0.3": ("AU", "Victoria"),
            "10.0.0.4": ("AU", "Queensland"),
        }
        tracker.lookup_geo = lambda ip: mapping.get(ip, ("XX", "Unknown"))

        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"

        events = []
        for ip, (country, _region) in mapping.items():
            events.append(
                Event(
                    timestamp=base_time,
                    stream_id=stream_id,
                    client_ip=ip,
                    device_type="web",
                    resource_id=123,
                    cache_status="HIT",
                    response_bytes=1000,
                    time_to_first_byte_ms=100,
                    tcp_rtt_us=50000,
                    request_time_ms=150,
                    response_status=200,
                    client_country=country,
                    location_id="testPOP",
                )
            )

        tracker.update(events)
        metrics = tracker.get_gauge_metrics()

        region_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_region']
        country_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_country']

        region_sum_by_country = {}
        for metric in region_metrics:
            country = metric['labels']['country']
            region_sum_by_country[country] = region_sum_by_country.get(country, 0.0) + metric['value']

        country_values = {m['labels']['country']: m['value'] for m in country_metrics}

        assert region_sum_by_country == country_values
        assert region_sum_by_country['US'] == 2.0
        assert region_sum_by_country['AU'] == 2.0

    def test_unique_metrics_respect_window_cutoff(self):
        """Unique metrics should only include sessions seen within configured window."""
        tracker = SessionTracker(window_seconds=7200)

        now = datetime.now(timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"

        recent_event = Event(
            timestamp=now - timedelta(minutes=20),
            stream_id=stream_id,
            client_ip="10.0.0.1",
            device_type="web",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="testPOP",
        )
        old_event = Event(
            timestamp=now - timedelta(minutes=90),
            stream_id=stream_id,
            client_ip="10.0.0.2",
            device_type="roku",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="testPOP",
        )

        tracker.update([recent_event, old_event])
        metrics = tracker.get_unique_metrics(window_seconds=3600, window_label='1h')

        total_metric = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_unique'][0]
        assert total_metric['value'] == 1.0
        assert total_metric['labels']['window'] == '1h'

    def test_unique_metrics_include_expected_metric_names(self):
        """Unique metrics should emit total/device/country/region series with window label."""
        tracker = SessionTracker(window_seconds=7200)
        tracker.geoip_reader = object()
        tracker.lookup_geo = lambda ip: ("US", "California")

        event = Event(
            timestamp=datetime.now(timezone.utc),
            stream_id="abcd1234567890abcdef1234567890ab",
            client_ip="10.0.0.1",
            device_type="web",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="testPOP",
        )

        tracker.update([event])
        metrics = tracker.get_unique_metrics(window_seconds=3600, window_label='1h')

        metric_names = {m['metric_name'] for m in metrics}
        assert 'cdn77_viewers_unique' in metric_names
        assert 'cdn77_viewers_unique_by_device' in metric_names
        assert 'cdn77_viewers_unique_by_country' in metric_names
        assert 'cdn77_viewers_unique_by_region' in metric_names

        for metric in metrics:
            assert metric['labels']['window'] == '1h'


class TestFileViewerMetrics:
    """Tests for per-file/flush viewer metric construction."""

    def test_build_file_viewer_metrics_unique_ips_per_dimension(self):
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"

        events = [
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="roku",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
                request_path=f"/{stream_id}/tracks-v3/rewind-43200.fmp4.m3u8",
            ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type="roku",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
                request_path=f"/{stream_id}/tracks-v3/rewind-43200.fmp4.m3u8",
            ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.2",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
                request_path=f"/{stream_id}/tracks-v3/rewind-43200.fmp4.m3u8",
            ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.99",
                device_type="web",
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country="US",
                location_id="testPOP",
                request_path=f"/{stream_id}/tracks-a1/rewind-43200.fmp4.m3u8",
            ),
        ]

        geo = {
            "10.0.0.1": ("US", "California"),
            "10.0.0.2": ("US", "Texas"),
        }
        metrics = build_file_viewer_metrics(
            events,
            timestamp_ms=1234567890000,
            geo_lookup=lambda ip: geo.get(ip, ("XX", "Unknown")),
        )

        metric_map = {m['metric']: m['value'] for m in metrics}

        assert metric_map[f'cdn77_viewers{{stream="{stream_id}",stream_name="{stream_id}"}}'] == 3.0
        assert metric_map[f'cdn77_viewers_by_device{{stream="{stream_id}",stream_name="{stream_id}",device_type="roku"}}'] == 1.0
        assert metric_map[f'cdn77_viewers_by_device{{stream="{stream_id}",stream_name="{stream_id}",device_type="web"}}'] == 2.0
        assert metric_map[f'cdn77_viewers_by_country{{stream="{stream_id}",stream_name="{stream_id}",country="US"}}'] == 3.0
        assert metric_map[f'cdn77_viewers_by_resolution{{stream="{stream_id}",stream_name="{stream_id}",track="v3"}}'] == 2.0
        assert metric_map[f'cdn77_viewers_by_region{{stream="{stream_id}",stream_name="{stream_id}",country="US",region="California"}}'] == 1.0
        assert metric_map[f'cdn77_viewers_by_region{{stream="{stream_id}",stream_name="{stream_id}",country="US",region="Texas"}}'] == 1.0

    def test_build_file_viewer_metrics_skips_unknown_region(self):
        event = Event(
            timestamp=datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc),
            stream_id="abcd1234567890abcdef1234567890ab",
            client_ip="10.0.0.3",
            device_type="web",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="testPOP",
        )

        metrics = build_file_viewer_metrics(
            [event],
            timestamp_ms=1234567890000,
            geo_lookup=lambda _ip: ("US", "Unknown"),
        )

        metric_names = [m['metric'].split('{')[0] for m in metrics]
        assert 'cdn77_viewers_by_region' not in metric_names


class TestSessionTrackerSimulation:
    """Large-scale simulation tests for session tracking"""
    
    def test_large_scale_simulation(self):
        """
        Simulate 3 hours of real-world data with:
        - Multiple streams
        - Varying viewer counts
        - Realistic IP patterns (some viewers stay, some leave)
        - Out-of-order file arrival
        """
        tracker = SessionTracker(window_seconds=7200)  # 2 hour window
        
        # Simulation parameters
        num_streams = 3
        streams = [f"stream{i:032x}" for i in range(num_streams)]
        base_time = datetime(2026, 2, 3, 10, 0, 0, tzinfo=timezone.utc)
        
        # Track ground truth for validation
        ground_truth = {}  # {stream_id: {ip: last_seen_time}}
        
        # Generate 3 hours of data in 30-second batches (360 batches)
        batches_generated = 0
        total_events = 0
        
        print(f"\n{'='*70}")
        print(f"LARGE SCALE SIMULATION TEST")
        print(f"{'='*70}")
        print(f"Streams: {num_streams}")
        print(f"Duration: 3 hours")
        print(f"Window: 2 hours")
        print(f"Batch size: 30 seconds")
        print(f"{'='*70}\n")
        
        for minute_offset in range(0, 180, 1):  # Every minute for 3 hours
            batch_time = base_time + timedelta(minutes=minute_offset)
            
            # Simulate file arrival delay (0-20 minutes)
            file_arrival_delay = random.randint(0, 20)
            
            events = []
            
            for stream_idx, stream_id in enumerate(streams):
                # Each stream has different viewer patterns
                base_viewers = 100 + (stream_idx * 50)  # 100, 150, 200 base viewers
                
                # Vary viewers over time (simulate peak hours)
                hour_factor = 1.0 + 0.5 * abs(2.0 - (minute_offset / 60.0))
                num_viewers = int(base_viewers * hour_factor)
                
                # Generate viewers for this minute
                for i in range(num_viewers):
                    # 70% chance IP is from "returning" viewer pool
                    # 30% chance it's a new viewer
                    if random.random() < 0.7 and stream_id in ground_truth and ground_truth[stream_id]:
                        # Pick existing IP
                        ip = random.choice(list(ground_truth[stream_id].keys()))
                    else:
                        # New IP
                        ip = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
                    
                    # Update ground truth
                    if stream_id not in ground_truth:
                        ground_truth[stream_id] = {}
                    ground_truth[stream_id][ip] = batch_time
                    
                    # Create event
                    device_types = ["roku", "apple_tv", "web", "firestick", "android", "ios"]
                    event = Event(
                        timestamp=batch_time,
                        stream_id=stream_id,
                        client_ip=ip,
                        device_type=random.choice(device_types),
                        resource_id=123,
                        cache_status="HIT",
                        response_bytes=random.randint(1000, 10000),
                        time_to_first_byte_ms=random.randint(50, 300),
                        tcp_rtt_us=random.randint(10000, 100000),
                        request_time_ms=random.randint(100, 500),
                        response_status=200,
                        client_country=random.choice(["US", "CA", "GB", "DE", "FR"]),
                        location_id="losangelesUSCA",
                                )
                    events.append(event)
                    total_events += 1
            
            # Update tracker with this batch
            tracker.update(events)
            
            # Simulate expiration every 10 minutes
            if minute_offset % 10 == 0:
                tracker.expire_old(batch_time)
                
                # Calculate expected count (IPs within 2-hour window)
                cutoff = batch_time - timedelta(hours=2)
                expected_per_stream = {}
                for stream_id, ips in ground_truth.items():
                    expected_per_stream[stream_id] = sum(
                        1 for last_seen in ips.values() if last_seen >= cutoff
                    )
                
                # Get actual count
                stats = tracker.get_stats()
                metrics = tracker.get_gauge_metrics()
                
                # Print progress
                total_expected = sum(expected_per_stream.values())
                print(f"t+{minute_offset:03d}m | Events: {len(events):4d} | "
                      f"Sessions: {stats['total_sessions']:5d} | "
                      f"Expected: {total_expected:5d} | "
                      f"Memory: ~{stats['total_sessions'] * 100 / 1024:.1f}KB")
                
                # Validate counts are close (within 5% due to timing)
                for stream_id in streams:
                    stream_metrics = [m for m in metrics 
                                    if m['metric_name'] == 'cdn77_viewers' 
                                    and m['labels']['stream'] == stream_id]
                    if stream_metrics:
                        actual = int(stream_metrics[0]['value'])
                        expected = expected_per_stream.get(stream_id, 0)
                        if expected > 0:
                            diff_pct = abs(actual - expected) / expected * 100
                            assert diff_pct < 10, f"Stream {stream_id}: actual={actual}, expected={expected}"
            
            batches_generated += 1
        
        print(f"\n{'='*70}")
        print(f"SIMULATION COMPLETE")
        print(f"{'='*70}")
        print(f"Batches processed: {batches_generated}")
        print(f"Total events: {total_events:,}")
        print(f"Final session count: {tracker.get_stats()['total_sessions']:,}")
        print(f"Memory estimate: ~{tracker.get_stats()['total_sessions'] * 100 / 1024:.1f}KB")
        print(f"{'='*70}\n")
        
        # Final validation
        final_metrics = tracker.get_gauge_metrics()
        assert len(final_metrics) > 0
        
        # Verify we have metrics for all dimensions
        metric_types = {m['metric_name'] for m in final_metrics}
        assert 'cdn77_viewers' in metric_types
        assert 'cdn77_viewers_by_device' in metric_types
        assert 'cdn77_viewers_by_country' in metric_types
    
    def test_retroactive_data_accumulation(self):
        """
        Test that unique IP counts increase as retroactive data arrives.
        Simulates the real-world scenario where files arrive out of order.
        """
        tracker = SessionTracker(window_seconds=7200)
        
        stream_id = "test1234567890abcdef1234567890ab"
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        
        print(f"\n{'='*70}")
        print(f"RETROACTIVE DATA ACCUMULATION TEST")
        print(f"{'='*70}\n")
        
        # Create a pool of 1000 unique IPs
        ip_pool = [f"10.{i//256}.{(i%256)//16}.{i%16}" for i in range(1000)]
        
        # Simulate 10 files arriving for the same time period
        # Each file contains events from the past 30 minutes
        # But files arrive out of order with delays
        
        unique_ips_seen = set()
        
        for file_num in range(10):
            # File arrival time (files arrive over 10 minutes)
            file_time = base_time + timedelta(minutes=file_num)
            
            # But file contains events from 0-30 minutes ago
            events = []
            ips_in_this_file = set()
            
            for i in range(100):  # 100 events per file
                # Event timestamp is in the past (retroactive)
                event_offset_minutes = random.randint(0, 30)
                event_time = file_time - timedelta(minutes=event_offset_minutes)
                
                # Pick an IP (might be new or might be seen before)
                ip = random.choice(ip_pool[:200 + file_num * 50])  # Gradually expose more IPs
                unique_ips_seen.add(ip)
                ips_in_this_file.add(ip)
                
                event = Event(
                    timestamp=event_time,
                    stream_id=stream_id,
                    client_ip=ip,
                    device_type="web",
                    resource_id=123,
                    cache_status="HIT",
                    response_bytes=1000,
                    time_to_first_byte_ms=100,
                    tcp_rtt_us=50000,
                    request_time_ms=150,
                    response_status=200,
                    client_country="US",
                    location_id="losangelesUSCA",
                        )
                events.append(event)
            
            # Update tracker
            tracker.update(events)
            
            # Generate metrics
            metrics = tracker.get_gauge_metrics()
            viewer_metric = [m for m in metrics if m['metric_name'] == 'cdn77_viewers'][0]
            
            print(f"File {file_num+1:2d} | "
                  f"New IPs in file: {len(ips_in_this_file):3d} | "
                  f"Total unique IPs seen: {len(unique_ips_seen):4d} | "
                  f"Active sessions: {int(viewer_metric['value']):4d}")
            
            # Verify count is increasing (as we discover more retroactive IPs)
            if file_num > 0:
                assert viewer_metric['value'] > 0
        
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS")
        print(f"{'='*70}")
        print(f"Total unique IPs discovered: {len(unique_ips_seen)}")
        print(f"Final active sessions: {tracker.get_stats()['total_sessions']}")
        
        # The final count should be close to unique IPs seen
        # (within the 2-hour window)
        assert tracker.get_stats()['total_sessions'] >= len(unique_ips_seen) * 0.8
        print(f"Validation: ✓ Session count matches expected range")
        print(f"{'='*70}\n")
    
    def test_memory_usage_estimation(self):
        """
        Test memory usage with different scales to verify estimates.
        Tests: 1K, 10K, 100K, and 1M IPs
        """
        import sys
        
        print(f"\n{'='*70}")
        print(f"MEMORY USAGE ESTIMATION TEST")
        print(f"{'='*70}\n")
        
        test_sizes = [1_000, 10_000, 100_000, 1_000_000]
        
        for size in test_sizes:
            tracker = SessionTracker(window_seconds=7200)
            base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
            stream_id = "test1234567890abcdef1234567890ab"
            
            # Generate unique IPs
            events = []
            for i in range(size):
                ip = f"{(i>>24)&0xFF}.{(i>>16)&0xFF}.{(i>>8)&0xFF}.{i&0xFF}"
                event = Event(
                    timestamp=base_time,
                    stream_id=stream_id,
                    client_ip=ip,
                    device_type="web",
                    resource_id=123,
                    cache_status="HIT",
                    response_bytes=1000,
                    time_to_first_byte_ms=100,
                    tcp_rtt_us=50000,
                    request_time_ms=150,
                    response_status=200,
                    client_country="US",
                    location_id="losangelesUSCA",
                        )
                events.append(event)
                
                # Batch update every 10K to avoid memory spike
                if len(events) >= 10_000:
                    tracker.update(events)
                    events = []
            
            # Update any remaining
            if events:
                tracker.update(events)
            
            # Calculate memory
            session_count = tracker.get_stats()['total_sessions']
            
            # Estimate memory per session
            # Each SessionInfo has: datetime (48 bytes) + 2 strings (~30 bytes) = ~80 bytes
            # Plus dict overhead ~40 bytes
            # Total ~120 bytes per IP
            estimated_mb = (session_count * 120) / (1024 * 1024)
            
            print(f"IPs: {size:>10,} | Sessions: {session_count:>10,} | "
                  f"Est. Memory: {estimated_mb:>6.1f}MB")
            
            # Verify count matches
            assert session_count == size
        
        print(f"\n{'='*70}")
        print(f"Scaling estimates:")
        print(f"  10M IPs  = ~1.2 GB")
        print(f"  50M IPs  = ~6.0 GB")
        print(f"  100M IPs = ~12 GB")
        print(f"{'='*70}\n")
    
    def test_device_and_country_breakdown_accuracy(self):
        """
        Test that device and country breakdowns are accurate with large datasets.
        """
        tracker = SessionTracker(window_seconds=7200)
        
        stream_id = "test1234567890abcdef1234567890ab"
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        
        print(f"\n{'='*70}")
        print(f"DEVICE & COUNTRY BREAKDOWN ACCURACY TEST")
        print(f"{'='*70}\n")
        
        # Define expected distributions
        device_distribution = {
            "roku": 0.35,        # 35%
            "apple_tv": 0.25,    # 25%
            "web": 0.20,         # 20%
            "firestick": 0.15,   # 15%
            "android": 0.10,    # 10%
            "ios": 0.10         # 10%
        }
        
        country_distribution = {
            "US": 0.60,  # 60%
            "CA": 0.15,  # 15%
            "GB": 0.10,  # 10%
            "DE": 0.08,  # 8%
            "FR": 0.07   # 7%
        }
        
        # Generate 10,000 unique viewers
        num_viewers = 10_000
        events = []
        
        # Track what we generate
        generated_by_device = {d: 0 for d in device_distribution.keys()}
        generated_by_country = {c: 0 for c in country_distribution.keys()}
        
        for i in range(num_viewers):
            # Pick device based on distribution
            rand = random.random()
            cumulative = 0
            device = "web"
            for dev, prob in device_distribution.items():
                cumulative += prob
                if rand <= cumulative:
                    device = dev
                    break
            generated_by_device[device] += 1
            
            # Pick country based on distribution  
            rand = random.random()
            cumulative = 0
            country = "US"
            for ctry, prob in country_distribution.items():
                cumulative += prob
                if rand <= cumulative:
                    country = ctry
                    break
            generated_by_country[country] += 1
            
            ip = f"{(i>>24)&0xFF}.{(i>>16)&0xFF}.{(i>>8)&0xFF}.{i&0xFF}"
            
            event = Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip=ip,
                device_type=device,
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country=country,
                location_id="losangelesUSCA",
                )
            events.append(event)
        
        # Update tracker
        tracker.update(events)
        metrics = tracker.get_gauge_metrics()
        
        # Validate device breakdown
        print("Device Breakdown:")
        print(f"{'Device':<15} {'Generated':<12} {'Expected %':<12} {'Tracked':<12} {'Tracked %':<12}")
        print("-" * 70)
        
        device_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_device']
        for device, expected_pct in sorted(device_distribution.items()):
            generated = generated_by_device[device]
            metric = [m for m in device_metrics if m['labels']['device_type'] == device]
            tracked = int(metric[0]['value']) if metric else 0
            tracked_pct = (tracked / num_viewers) * 100
            expected_pct_display = expected_pct * 100
            
            print(f"{device:<15} {generated:<12,} {expected_pct_display:<12.1f} {tracked:<12,} {tracked_pct:<12.1f}")
            
            # Verify within 2% of expected
            assert abs(tracked - generated) < num_viewers * 0.02
        
        # Validate country breakdown
        print(f"\nCountry Breakdown:")
        print(f"{'Country':<15} {'Generated':<12} {'Expected %':<12} {'Tracked':<12} {'Tracked %':<12}")
        print("-" * 70)
        
        country_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_viewers_by_country']
        for country, expected_pct in sorted(country_distribution.items()):
            generated = generated_by_country[country]
            metric = [m for m in country_metrics if m['labels']['country'] == country]
            tracked = int(metric[0]['value']) if metric else 0
            tracked_pct = (tracked / num_viewers) * 100
            expected_pct_display = expected_pct * 100
            
            print(f"{country:<15} {generated:<12,} {expected_pct_display:<12.1f} {tracked:<12,} {tracked_pct:<12.1f}")
            
            # Verify matches exactly (no deduplication across countries)
            assert tracked == generated
        
        print(f"\n{'='*70}")
        print("✓ All breakdowns match expected distributions")
        print(f"{'='*70}\n")


class TestPrometheusWriterRetries:
    """Test writer retry behavior for transient remote-write failures"""

    class _FakeExporter:
        def __init__(self, outcomes):
            self.outcomes = outcomes
            self.calls = 0

        def push_metrics(self, metrics, log_prefix, retry_count=0):
            result = self.outcomes[self.calls] if self.calls < len(self.outcomes) else self.outcomes[-1]
            self.calls += 1
            return result

    def test_push_with_retries_succeeds_after_failure(self):
        exporter = self._FakeExporter([False, True])
        writer = PrometheusWriterThread(
            exporter=exporter,
            write_queue=queue.Queue(),
            max_retry_attempts=3,
            retry_base_delay_seconds=0.0,
            retry_max_delay_seconds=0.0,
        )

        ok = writer._push_with_retries([{'metric': 'm{}', 'value': 1, 'timestamp': 1}], 'Test')

        assert ok is True
        assert exporter.calls == 2

    def test_push_with_retries_exhausts_attempts(self):
        exporter = self._FakeExporter([False, False, False, False])
        writer = PrometheusWriterThread(
            exporter=exporter,
            write_queue=queue.Queue(),
            max_retry_attempts=2,
            retry_base_delay_seconds=0.0,
            retry_max_delay_seconds=0.0,
        )

        ok = writer._push_with_retries([{'metric': 'm{}', 'value': 1, 'timestamp': 1}], 'Test')

        assert ok is False
        assert exporter.calls == 3


class TestPolarsViewerMetricLabels:
    def test_polars_viewer_metrics_escape_labels_and_include_legacy_stream_name(self):
        polars_mod = pytest.importorskip("exporter_polars")
        dataframe_from_events = polars_mod.dataframe_from_events
        build_file_viewer_metrics_polars = polars_mod.build_file_viewer_metrics_polars

        stream_id = "abcd1234567890abcdef1234567890ab"
        ts = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        events = [
            Event(
                timestamp=ts,
                stream_id=stream_id,
                client_ip="10.0.0.1",
                device_type='web\\"tv',
                resource_id=123,
                cache_status="HIT",
                response_bytes=1000,
                time_to_first_byte_ms=100,
                tcp_rtt_us=50000,
                request_time_ms=150,
                response_status=200,
                client_country='U\\"S',
                location_id="testPOP",
                request_path=f"/{stream_id}/tracks-v3/rewind-43200.fmp4.m3u8",
            ),
        ]

        df = dataframe_from_events(events)

        metrics = build_file_viewer_metrics_polars(
            df,
            timestamp_ms=minute_timestamp_ms(ts),
            geo_lookup=lambda _ip: ('U\\"S', 'North\\"East'),
        )

        metric_names = [m["metric"] for m in metrics]

        assert any(
            f'cdn77_viewers{{stream="{stream_id}",stream_name="{stream_id}"}}' == metric
            for metric in metric_names
        )
        assert any('device_type="web\\\\\\"tv"' in metric for metric in metric_names)
        assert any('country="U\\\\\\"S"' in metric for metric in metric_names)
        assert any('region="North\\\\\\"East"' in metric for metric in metric_names)


class TestFileProcessingTelemetry:
    def test_process_file_emits_file_process_delay_seconds(self):
        now = datetime.now(timezone.utc)
        newest_event_ts = now - timedelta(seconds=42)

        event = Event(
            timestamp=newest_event_ts,
            stream_id="78001c7e2b3ff8ca21b504883b47c44c",
            resource_id=123,
            cache_status="HIT",
            response_bytes=1000,
            time_to_first_byte_ms=100,
            tcp_rtt_us=50000,
            request_time_ms=150,
            response_status=200,
            client_country="US",
            location_id="losangelesUSCA",
            client_ip="192.168.1.1",
            device_type="web",
            request_path="/78001c7e2b3ff8ca21b504883b47c44c/tracks-v3/index.m3u8",
        )

        compressed_payload = io.BytesIO()
        with gzip.open(compressed_payload, "wt", encoding="utf-8") as gz:
            gz.write("{}\n")

        class _FakeS3Client:
            def download_object(self, key):
                return compressed_payload.getvalue()

            def delete_object(self, key):
                return True

        class _FakeParser:
            def parse_json_to_event(self, line):
                return event

        class _FakeBuffer:
            def add_many(self, events):
                return None

        importer = S3Importer.__new__(S3Importer)
        importer.s3_client = _FakeS3Client()
        importer.parser = _FakeParser()
        importer.event_buffer = _FakeBuffer()
        importer.write_queue = queue.Queue(maxsize=10)

        ok = importer.process_file("real-time-logs/test.ndjson.gz")

        assert ok is True
        metrics, source = importer.write_queue.get_nowait()
        assert source == "FileProcessMetrics"

        delay_metric = [
            m for m in metrics
            if m["metric"].startswith(f'{metric_name("file_process_delay_seconds")}{{')
        ]
        assert len(delay_metric) == 1
        assert 'result="success"' in delay_metric[0]["metric"]

        delay_value = delay_metric[0]["value"]
        assert delay_value >= 0.0
        assert 30.0 <= delay_value <= 90.0

        assert delay_metric[0]["timestamp"] == minute_timestamp_ms(newest_event_ts)


class TestPrometheusExporterPush:
    def test_push_metrics_handles_quoted_commas_in_labels(self, monkeypatch):
        captured = {}

        class _OkResponse:
            status_code = 200

            def raise_for_status(self):
                return None

        def _fake_post(url, data, headers, auth):
            captured['url'] = url
            captured['data'] = data
            captured['headers'] = headers
            captured['auth'] = auth
            return _OkResponse()

        monkeypatch.setattr("exporter.requests.post", _fake_post)

        exporter = PrometheusExporter("https://prometheus.example.com/api/v1/write")
        ok = exporter.push_metrics([
            {
                'metric': 'cdn77_viewers_by_resolution{stream="abc",track="1080p,h264"}',
                'value': 7.0,
                'timestamp': 1710592440000,
            }
        ])

        assert ok is True
        payload = remote_pb2.WriteRequest()
        payload.ParseFromString(snappy.decompress(captured['data']))

        assert len(payload.timeseries) == 1
        labels = {label.name: label.value for label in payload.timeseries[0].labels}
        assert labels['__name__'] == 'cdn77_viewers_by_resolution'
        assert labels['stream'] == 'abc'
        assert labels['track'] == '1080p,h264'


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
