#!/usr/bin/env python3
"""
Test suite for CDN77 Prometheus Exporter
Generates mock log data and validates metric processing
"""

import json
import gzip
import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import pytest

from exporter import (
    Event, LogParser, MetricAggregator, AggregationType,
    MetricDefinition, METRIC_DEFINITIONS, detect_device_type,
    extract_stream_id, extract_cdn_id, extract_cache_status,
    extract_location_id, extract_response_status, extract_client_ip, extract_device_type
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
        assert users_metric['labels']['stream_name'] == stream_id
    
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
        
        stream1_metric = [m for m in users_metrics if m['labels']['stream_name'] == stream1][0]
        stream2_metric = [m for m in users_metrics if m['labels']['stream_name'] == stream2][0]
        
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
            raw_data={}
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
            raw_data={}
        )
        
        assert extract_client_ip(event) == "10.20.30.40"


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
    
    def test_detect_android_tv(self):
        """Test Android TV detection"""
        assert detect_device_type("AndroidTV/10.0") == "android_tv"
    
    def test_detect_web(self):
        """Test web browser detection"""
        assert detect_device_type("Mozilla/5.0 Chrome/120.0.0.0") == "web"
        assert detect_device_type("Mozilla/5.0 Firefox/121.0") == "web"
        assert detect_device_type("Mozilla/5.0 Safari/605.1") == "web"
    
    def test_detect_baron_mobile(self):
        """Test Baron mobile app detection"""
        assert detect_device_type("Baron iOS App/1.0") == "baron_mobile"
        assert detect_device_type("VirtualRailFan Android Mobile/2.0") == "baron_mobile"
    
    def test_detect_other(self):
        """Test unknown device detection"""
        assert detect_device_type("") == "other"
        assert detect_device_type("UnknownDevice/1.0") == "other"
    
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


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
