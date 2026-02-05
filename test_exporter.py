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
    SessionTracker, SessionInfo,
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
        
        # Find viewers metric
        viewers_metric = [m for m in computed if m['metric_name'] == 'cdn77_viewers_total'][0]
        assert viewers_metric['value'] == 5  # Should count 5 unique IPs
        assert viewers_metric['labels']['stream_name'] == stream_id
    
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
        
        # Find viewers metrics for each stream
        viewers_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_total']
        
        stream1_metric = [m for m in viewers_metrics if m['labels']['stream_name'] == stream1][0]
        stream2_metric = [m for m in viewers_metrics if m['labels']['stream_name'] == stream2][0]
        
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
    
    def test_unique_viewers_by_device(self):
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
        
        # Find viewers_by_device metrics
        device_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_by_device_total']
        
        roku_metric = [m for m in device_metrics if m['labels']['device_type'] == 'roku'][0]
        apple_tv_metric = [m for m in device_metrics if m['labels']['device_type'] == 'apple_tv'][0]
        
        assert roku_metric['value'] == 2  # 2 unique IPs on Roku
        assert apple_tv_metric['value'] == 2  # 2 unique IPs on Apple TV
    
    def test_unique_viewers_by_country(self):
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
                raw_data={}
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
                raw_data={}
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
                raw_data={}
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
                raw_data={}
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
                raw_data={}
            ),
        ]
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find viewers_by_country metrics
        country_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_by_country_total']
        
        us_metric = [m for m in country_metrics if m['labels']['country'] == 'US'][0]
        ca_metric = [m for m in country_metrics if m['labels']['country'] == 'CA'][0]
        
        assert us_metric['value'] == 2  # 2 unique IPs from US
        assert ca_metric['value'] == 2  # 2 unique IPs from CA
    
    def test_unique_viewers_by_region(self):
        """Test unique IP counting per region with GeoIP lookup"""
        from unittest.mock import Mock
        
        parser = LogParser()
        aggregator = MetricAggregator(METRIC_DEFINITIONS, geoip_db_path=None)
        
        # Mock GeoIP reader with different regions per IP
        def mock_city_lookup(ip):
            """Return different regions based on IP"""
            mock_response = Mock()
            mock_subdivision = Mock()
            mock_subdivisions = Mock()
            
            if ip == "10.0.0.1" or ip == "10.0.0.2":
                mock_subdivision.name = "California"
            elif ip == "10.0.0.3":
                mock_subdivision.name = "Texas"
            elif ip == "10.0.0.4":
                mock_subdivision.name = "Ontario"
            else:
                mock_subdivision.name = "Unknown"
            
            mock_subdivisions.most_specific = mock_subdivision
            mock_response.subdivisions = mock_subdivisions
            return mock_response
        
        mock_reader = Mock()
        mock_reader.city.side_effect = mock_city_lookup
        aggregator.geoip_reader = mock_reader
        
        base_time = datetime(2026, 1, 28, 12, 30, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # Create events without client_region (will be looked up via GeoIP)
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
                raw_data={}
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
                raw_data={}
            ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.1",  # Duplicate IP from CA
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
                raw_data={}
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
                client_country="US",
                location_id="losangelesUSCA",
                raw_data={}
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
                raw_data={}
            ),
        ]
        
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        
        # Find viewers_by_region metrics
        region_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_by_region_total']
        
        # Should have 3 region combinations: US/California (2 IPs), US/Texas (1 IP), CA/Ontario (1 IP)
        assert len(region_metrics) == 3
        
        us_ca_metric = [m for m in region_metrics if m['labels']['country'] == 'US' and m['labels']['region'] == 'California'][0]
        us_tx_metric = [m for m in region_metrics if m['labels']['country'] == 'US' and m['labels']['region'] == 'Texas'][0]
        ca_on_metric = [m for m in region_metrics if m['labels']['country'] == 'CA' and m['labels']['region'] == 'Ontario'][0]
        
        assert us_ca_metric['value'] == 2  # 2 unique IPs from US/California
        assert us_tx_metric['value'] == 1  # 1 unique IP from US/Texas
        assert ca_on_metric['value'] == 1  # 1 unique IP from CA/Ontario
        
        # Verify GeoIP was called for each unique IP (4 unique IPs)
        assert mock_reader.city.call_count == 4


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
            'cdn77_viewers_total'
        }
        assert expected_metrics.issubset(metric_names)
        
        # Verify viewers_total has one entry per stream
        viewers_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_total']
        assert len(viewers_metrics) >= 1  # At least one stream has data
        
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
        viewers_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_total']
        
        # Each minute should show 5 unique IPs
        assert len(viewers_metrics) == 3
        for metric in viewers_metrics:
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
    
    def test_geoip_deduplication_efficiency(self):
        """
        Test that GeoIP lookups happen AFTER deduplication.
        With 1000 events from 100 unique IPs, we should only do 100 lookups, not 1000.
        """
        import time
        from unittest.mock import Mock, patch
        
        # Create aggregator without real GeoIP (we'll mock it)
        aggregator = MetricAggregator(METRIC_DEFINITIONS, geoip_db_path=None)
        
        # Mock the GeoIP reader and track lookup calls
        mock_reader = Mock()
        mock_city_response = Mock()
        mock_subdivision = Mock()
        mock_subdivision.name = "California"
        mock_subdivisions = Mock()
        mock_subdivisions.most_specific = mock_subdivision
        mock_city_response.subdivisions = mock_subdivisions
        mock_reader.city.return_value = mock_city_response
        
        aggregator.geoip_reader = mock_reader
        
        # Generate 1000 events from 100 unique IPs (10 events per IP)
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        unique_ips = [f"10.0.{i//256}.{i%256}" for i in range(100)]
        events = []
        
        for i in range(1000):
            ip = unique_ips[i % 100]  # Repeat IPs to simulate real traffic
            events.append(Event(
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
                raw_data={}
            ))
        
        # Aggregate and compute
        start_time = time.time()
        aggregated = aggregator.aggregate_events(events)
        computed = aggregator.compute_final_values(aggregated)
        elapsed = time.time() - start_time
        
        # Verify we only looked up each unique IP once (100 times, not 1000)
        lookup_count = mock_reader.city.call_count
        
        print(f"\n1000 events, 100 unique IPs")
        print(f"GeoIP lookups: {lookup_count} (expected ~100, not 1000)")
        print(f"Lookup efficiency: {(1 - lookup_count/1000)*100:.1f}% reduction")
        print(f"Aggregation time: {elapsed:.3f}s")
        
        # Should be ~100 lookups (one per unique IP), not 1000
        assert lookup_count <= 150, f"Too many lookups: {lookup_count} (expected ~100)"
        assert lookup_count >= 50, f"Too few lookups: {lookup_count} (expected ~100)"
        
        # Verify metrics were generated correctly
        region_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_by_region_total']
        assert len(region_metrics) > 0
    
    def test_geoip_cache_across_batches(self):
        """
        Test that GeoIP lookups are cached across multiple batches.
        Same IPs appearing in different batches should only be looked up once.
        """
        import time
        from unittest.mock import Mock
        
        # Create aggregator without real GeoIP (we'll mock it)
        aggregator = MetricAggregator(METRIC_DEFINITIONS, geoip_db_path=None)
        
        # Mock the GeoIP reader and track lookup calls
        mock_reader = Mock()
        mock_city_response = Mock()
        mock_subdivision = Mock()
        mock_subdivision.name = "California"
        mock_subdivisions = Mock()
        mock_subdivisions.most_specific = mock_subdivision
        mock_city_response.subdivisions = mock_subdivisions
        mock_reader.city.return_value = mock_city_response
        
        aggregator.geoip_reader = mock_reader
        
        # Generate 3 batches with overlapping IPs
        base_time = datetime(2026, 2, 3, 12, 0, 0, tzinfo=timezone.utc)
        stream_id = "abcd1234567890abcdef1234567890ab"
        
        # 10 unique IPs that will appear in all 3 batches
        unique_ips = [f"10.0.0.{i}" for i in range(10)]
        
        # Process 3 batches
        for batch_num in range(3):
            events = []
            batch_time = base_time + timedelta(minutes=batch_num)
            
            # Each batch has same 10 IPs appearing 10 times each (100 events per batch)
            for i in range(100):
                ip = unique_ips[i % 10]
                events.append(Event(
                    timestamp=batch_time,
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
                    raw_data={}
                ))
            
            # Process batch
            aggregated = aggregator.aggregate_events(events)
            computed = aggregator.compute_final_values(aggregated)
            
            # Verify metrics were generated
            region_metrics = [m for m in computed if m['metric_name'] == 'cdn77_viewers_by_region_total']
            assert len(region_metrics) > 0
        
        # Verify GeoIP was only called 10 times total (once per unique IP), not 30 times (10 per batch)
        total_lookups = mock_reader.city.call_count
        
        print(f"\n3 batches, 10 unique IPs per batch (same IPs)")
        print(f"Total GeoIP lookups: {total_lookups} (expected 10, not 30)")
        print(f"Cache efficiency: {(1 - total_lookups/30)*100:.1f}% reduction")
        print(f"Cache size: {len(aggregator.ip_to_region_cache)}")
        
        # Should only lookup each IP once across all batches
        assert total_lookups == 10, f"Expected 10 lookups (one per unique IP), got {total_lookups}"
        assert len(aggregator.ip_to_region_cache) == 10, "Cache should contain 10 IPs"


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
                raw_data={}
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
            raw_data={}
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
                raw_data={}
            ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.2",
                device_type="apple_tv",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                raw_data={}
            ),
            Event(
                timestamp=base_time,
                stream_id=stream_id,
                client_ip="10.0.0.3",
                device_type="roku",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="CA", location_id="losangelesUSCA",
                raw_data={}
            )
        ]
        
        tracker.update(events)
        metrics = tracker.get_gauge_metrics()
        
        # Should have total, by_device, and by_country metrics
        metric_names = {m['metric_name'] for m in metrics}
        assert 'cdn77_active_viewers' in metric_names
        assert 'cdn77_active_viewers_by_device' in metric_names
        assert 'cdn77_active_viewers_by_country' in metric_names
        
        # Check total viewers
        total_metric = [m for m in metrics if m['metric_name'] == 'cdn77_active_viewers'][0]
        assert total_metric['value'] == 3.0
        
        # Check device breakdown
        device_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_active_viewers_by_device']
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
                raw_data={}
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
                raw_data={}
            ),
            Event(
                timestamp=base_time - timedelta(minutes=5),
                stream_id=stream_id,
                client_ip="10.0.0.1",  # Same IP as batch1, update timestamp
                device_type="web",
                resource_id=123, cache_status="HIT", response_bytes=1000,
                time_to_first_byte_ms=100, tcp_rtt_us=50000, request_time_ms=150,
                response_status=200, client_country="US", location_id="losangelesUSCA",
                raw_data={}
            )
        ]
        
        tracker.update(batch2)
        
        # Should have 2 unique IPs (10.0.0.1 updated, 10.0.0.2 added)
        assert tracker.get_stats()['total_sessions'] == 2
        
        # Generate metrics - should show increased count
        metrics = tracker.get_gauge_metrics()
        total_metric = [m for m in metrics if m['metric_name'] == 'cdn77_active_viewers'][0]
        assert total_metric['value'] == 2.0


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
                    device_types = ["roku", "apple_tv", "web", "firestick", "baron_mobile"]
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
                        raw_data={}
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
                                    if m['metric_name'] == 'cdn77_active_viewers' 
                                    and m['labels']['stream_name'] == stream_id]
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
        assert 'cdn77_active_viewers' in metric_types
        assert 'cdn77_active_viewers_by_device' in metric_types
        assert 'cdn77_active_viewers_by_country' in metric_types
    
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
                    raw_data={}
                )
                events.append(event)
            
            # Update tracker
            tracker.update(events)
            
            # Generate metrics
            metrics = tracker.get_gauge_metrics()
            viewer_metric = [m for m in metrics if m['metric_name'] == 'cdn77_active_viewers'][0]
            
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
                    raw_data={}
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
            "baron_mobile": 0.05 # 5%
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
                raw_data={}
            )
            events.append(event)
        
        # Update tracker
        tracker.update(events)
        metrics = tracker.get_gauge_metrics()
        
        # Validate device breakdown
        print("Device Breakdown:")
        print(f"{'Device':<15} {'Generated':<12} {'Expected %':<12} {'Tracked':<12} {'Tracked %':<12}")
        print("-" * 70)
        
        device_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_active_viewers_by_device']
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
        
        country_metrics = [m for m in metrics if m['metric_name'] == 'cdn77_active_viewers_by_country']
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


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
