#!/usr/bin/env python3
"""
CDN77 S3 Real-Time Logs to Prometheus Metrics Importer (Multi-Metric)
Polls S3 bucket for new NDJSON log files and exports metrics to Prometheus
Files are deleted from S3 after successful processing

Architecture:
- Event-based: Each JSON entry is an Event
- Buffering: Events outside time window are buffered for next file
- Multi-metric: Declarative metric definitions with flexible labels
"""

import argparse
import gzip
import json
import logging
import os
import queue
import random
import re
import signal
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable, Any, Tuple
from enum import Enum

import boto3
from botocore.exceptions import ClientError
import requests
import snappy

try:
    import geoip2.database
    from geoip2.errors import AddressNotFoundError
    GEOIP_AVAILABLE = True
except ImportError:
    GEOIP_AVAILABLE = False
    logger.warning("GeoIP2 not available. Install with: pip install geoip2 maxminddb")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================================
# EVENT SYSTEM
# ============================================================================

@dataclass
class Event:
    """Represents a single CDN log event (JSON entry)"""
    timestamp: datetime
    stream_id: str
    resource_id: int
    cache_status: str
    response_bytes: int
    time_to_first_byte_ms: int
    tcp_rtt_us: int
    request_time_ms: int
    response_status: int
    client_country: str
    location_id: str
    client_ip: str
    device_type: str
    client_region: Optional[str] = None  # State/region from GeoIP lookup (optional)
    raw_data: Dict[str, Any] = None  # Full JSON for future extensibility
    
    @property
    def minute_key(self) -> str:
        """Return minute-rounded timestamp key"""
        return self.timestamp.strftime('%d/%b/%Y %H:%M')


class AggregationType(Enum):
    """Metric aggregation types"""
    SUM = "sum"
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    COUNT = "count"
    UNIQUE_COUNT = "unique_count"  # Count of unique values


@dataclass
class MetricDefinition:
    """Defines a Prometheus metric to be generated from events"""
    name: str
    value_extractor: Callable[[Event], Optional[float]]
    aggregation: AggregationType
    labels: Dict[str, Callable[[Event], str]]  # label_name -> extractor function
    help_text: str = ""
    
    def extract_value(self, event: Event) -> Optional[float]:
        """Extract metric value from event"""
        try:
            return self.value_extractor(event)
        except (AttributeError, TypeError, ValueError):
            return None
    
    def extract_labels(self, event: Event) -> Dict[str, str]:
        """Extract all labels from event"""
        labels = {}
        for label_name, extractor in self.labels.items():
            try:
                labels[label_name] = str(extractor(event))
            except (AttributeError, TypeError, ValueError):
                labels[label_name] = "unknown"
        return labels


@dataclass
class TimeWindow:
    """Represents the explicit time boundaries for a file"""
    start: datetime
    end: datetime
    
    def contains(self, dt: datetime) -> bool:
        """Check if datetime falls within this window"""
        return self.start <= dt < self.end
    
    @classmethod
    def from_filename(cls, filename: str) -> Optional['TimeWindow']:
        """
        Extract time window from S3 filename
        Format: rtl.{uuid}+{shard}+{sequence}.ndjson.gz
        Assumption: Files are ~30 seconds, aligned to :00/:30 boundaries
        """
        # For now, return None - will be calculated from actual event timestamps
        return None
    
    @classmethod
    def from_events(cls, events: List[Event], duration_seconds: int = 29) -> Optional['TimeWindow']:
        """
        Calculate time window from event timestamps
        Uses first event's minute boundary as start, adds duration
        """
        if not events:
            return None
        
        # Find earliest event
        min_time = min(e.timestamp for e in events)
        
        # Round down to nearest 30-second boundary
        minute = min_time.replace(second=0, microsecond=0)
        if min_time.second >= 30:
            start = minute.replace(second=30)
        else:
            start = minute
        
        end = start + timedelta(seconds=duration_seconds)
        return cls(start=start, end=end)


class EventBuffer:
    """Buffers events that fall outside the current time window"""
    
    def __init__(self):
        self.events: List[Event] = []
        self._lock = threading.Lock()
    
    def add(self, event: Event):
        """Add event to buffer"""
        with self._lock:
            self.events.append(event)
    
    def add_many(self, events: List[Event]):
        """Add multiple events to buffer"""
        with self._lock:
            self.events.extend(events)
    
    def get_and_clear(self) -> List[Event]:
        """Get all buffered events and clear the buffer"""
        with self._lock:
            events = self.events.copy()
            self.events.clear()
            return events
    
    def size(self) -> int:
        """Return number of buffered events"""
        with self._lock:
            return len(self.events)


@dataclass
class SessionInfo:
    """Information about an IP session"""
    last_seen: datetime
    device_type: str
    country: str
    region: str  # State/province


class SessionTracker:
    """
    Tracks unique IP sessions in a rolling time window for accurate concurrent viewer counts.
    
    Handles retroactive data by updating timestamps as new events arrive.
    Generates gauge metrics representing unique viewers in the window.
    """
    
    def __init__(self, window_seconds: int = 7200, geoip_db_path: Optional[str] = None):
        """
        Initialize session tracker
        
        Args:
            window_seconds: Rolling window size in seconds (default: 2 hours)
            geoip_db_path: Path to MaxMind GeoIP2 database file (optional)
        """
        self.window = timedelta(seconds=window_seconds)
        self.window_label = f"{window_seconds//3600}h" if window_seconds >= 3600 else f"{window_seconds//60}m"
        
        # Store: {stream_id: {ip_address: SessionInfo}}
        self.sessions: Dict[str, Dict[str, SessionInfo]] = defaultdict(dict)
        self._lock = threading.Lock()
        
        # GeoIP lookup
        self.geoip_reader = None
        if geoip_db_path and GEOIP_AVAILABLE:
            try:
                self.geoip_reader = geoip2.database.Reader(geoip_db_path)
                logger.info(f"GeoIP database loaded: {geoip_db_path}")
            except Exception as e:
                logger.warning(f"Failed to load GeoIP database: {e}")
    
    def lookup_geo(self, ip_address: str) -> Tuple[str, str]:
        """
        Lookup country and region for IP address
        
        Returns:
            Tuple of (country_code, region_name)
        """
        if not self.geoip_reader:
            return ("XX", "Unknown")
        
        try:
            response = self.geoip_reader.city(ip_address)
            country = response.country.iso_code or "XX"
            region = response.subdivisions.most_specific.name if response.subdivisions else "Unknown"
            return (country, region)
        except (AddressNotFoundError, AttributeError):
            return ("XX", "Unknown")
        except Exception as e:
            logger.debug(f"GeoIP lookup failed for {ip_address}: {e}")
            return ("XX", "Unknown")
    
    def update(self, events: List[Event]):
        """
        Update session tracker with new events.
        Events may be retroactive (from delayed files).
        """
        with self._lock:
            for event in events:
                # Try to get country from event first, fallback to GeoIP lookup
                country = event.client_country if hasattr(event, 'client_country') and event.client_country else None
                region = None
                
                # If not in event, try GeoIP lookup
                if not country:
                    country, region = self.lookup_geo(event.client_ip)
                
                # Update or create session info
                current_session = self.sessions[event.stream_id].get(event.client_ip)
                
                if current_session is None or event.timestamp > current_session.last_seen:
                    # New session or more recent timestamp
                    self.sessions[event.stream_id][event.client_ip] = SessionInfo(
                        last_seen=event.timestamp,
                        device_type=event.device_type,
                        country=country,
                        region=region
                    )
    
    def expire_old(self, reference_time: datetime):
        """
        Remove sessions older than the rolling window.
        
        Args:
            reference_time: Current time to calculate cutoff
        """
        cutoff = reference_time - self.window
        
        with self._lock:
            for stream_id in list(self.sessions.keys()):
                expired_ips = [
                    ip for ip, session in self.sessions[stream_id].items()
                    if session.last_seen < cutoff
                ]
                
                for ip in expired_ips:
                    del self.sessions[stream_id][ip]
                
                # Remove empty streams
                if not self.sessions[stream_id]:
                    del self.sessions[stream_id]
    
    def get_gauge_metrics(self, batch_offset_ms: int = 0) -> List[Dict]:
        """
        Generate gauge metrics for current session state.
        
        Returns metrics representing unique viewers in the rolling window:
        - cdn77_active_viewers: Total unique IPs per stream
        - cdn77_active_viewers_by_device: Unique IPs per stream per device
        - cdn77_active_viewers_by_country: Unique IPs per stream per country
        - cdn77_active_viewers_by_region: Unique IPs per stream per region
        
        Args:
            batch_offset_ms: Millisecond offset to prevent duplicate timestamps
        """
        with self._lock:
            metrics = []
            now = datetime.now(timezone.utc)
            timestamp_ms = int(now.timestamp() * 1000) + batch_offset_ms
            
            for stream_id, sessions in self.sessions.items():
                if not sessions:
                    continue
                
                # Total unique viewers
                metrics.append({
                    'metric_name': 'cdn77_active_viewers',
                    'value': float(len(sessions)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {
                        'stream_name': stream_id,
                        'window': self.window_label
                    }
                })
                
                # Group by device type
                by_device = defaultdict(set)
                by_country = defaultdict(set)
                by_region = defaultdict(set)
                
                for ip, session in sessions.items():
                    by_device[session.device_type].add(ip)
                    by_country[session.country].add(ip)
                    by_region[f"{session.country}_{session.region}"].add(ip)
                
                # Device metrics
                for device_type, ips in by_device.items():
                    metrics.append({
                        'metric_name': 'cdn77_active_viewers_by_device',
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream_name': stream_id,
                            'device_type': device_type,
                            'window': self.window_label
                        }
                    })
                
                # Country metrics
                for country, ips in by_country.items():
                    metrics.append({
                        'metric_name': 'cdn77_active_viewers_by_country',
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream_name': stream_id,
                            'country': country,
                            'window': self.window_label
                        }
                    })
                
                # Region metrics (country_region to avoid conflicts)
                for region_key, ips in by_region.items():
                    country, region = region_key.split('_', 1)
                    if region != "Unknown":  # Skip unknown regions
                        metrics.append({
                            'metric_name': 'cdn77_active_viewers_by_region',
                            'value': float(len(ips)),
                            'timestamp_ms': timestamp_ms,
                            'labels': {
                                'stream_name': stream_id,
                                'country': country,
                                'region': region,
                                'window': self.window_label
                            }
                        })
            
            return metrics
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about current sessions"""
        with self._lock:
            total_streams = len(self.sessions)
            total_sessions = sum(len(sessions) for sessions in self.sessions.values())
            return {
                'streams': total_streams,
                'total_sessions': total_sessions
            }


# ============================================================================
# DEVICE DETECTION
# ============================================================================

def detect_device_type(user_agent: str) -> str:
    """
    Detect device type from User-Agent string
    
    Returns:
        - baron_mobile: Baron iOS/Android app
        - apple_tv: Apple TV / tvOS
        - roku: Roku devices
        - firestick: Amazon Fire TV
        - android_tv: Android TV devices
        - web: Desktop/mobile browsers
        - other: Unknown devices
    """
    if not user_agent:
        return "other"
    
    ua_lower = user_agent.lower()
    
    # Baron mobile app detection (customize based on your app's UA string)
    if "baron" in ua_lower or "virtualrailfan" in ua_lower:
        if "ios" in ua_lower or "iphone" in ua_lower or "ipad" in ua_lower:
            return "baron_mobile"
        if "android" in ua_lower and "mobile" in ua_lower:
            return "baron_mobile"
    
    # OTT Platform detection
    if "appletv" in ua_lower or "apple tv" in ua_lower or "tvos" in ua_lower:
        return "apple_tv"
    
    if "roku" in ua_lower:
        return "roku"
    
    if "aft" in ua_lower or "amazon" in ua_lower and "fire" in ua_lower:
        return "firestick"
    
    if "android" in ua_lower and "tv" in ua_lower:
        return "android_tv"
    
    # Web browsers
    if any(browser in ua_lower for browser in ["chrome", "firefox", "safari", "edge", "opera"]):
        return "web"
    
    return "other"


# ============================================================================
# METRIC REGISTRY
# ============================================================================

# Standard label extractors
def extract_stream_id(event: Event) -> str:
    return event.stream_id

def extract_cdn_id(event: Event) -> str:
    return str(event.resource_id)

def extract_cache_status(event: Event) -> str:
    return event.cache_status

def extract_client_country(event: Event) -> str:
    return event.client_country

def extract_location_id(event: Event) -> str:
    return event.location_id

def extract_response_status(event: Event) -> str:
    return str(event.response_status)

def extract_client_ip(event: Event) -> str:
    return event.client_ip

def extract_device_type(event: Event) -> str:
    return event.device_type

def extract_client_region(event: Event) -> str:
    return event.client_region if event.client_region else "unknown"


# Metric definitions
METRIC_DEFINITIONS = [
    MetricDefinition(
        name="cdn77_transfer_bytes_total",
        value_extractor=lambda e: float(e.response_bytes),
        aggregation=AggregationType.SUM,
        labels={
            "stream_name": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "pop": extract_location_id,
        },
        help_text="Total bytes transferred per stream"
    ),
    MetricDefinition(
        name="cdn77_time_to_first_byte_ms",
        value_extractor=lambda e: float(e.time_to_first_byte_ms) if e.time_to_first_byte_ms > 0 else None,
        aggregation=AggregationType.AVG,
        labels={
            "stream_name": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "pop": extract_location_id,
        },
        help_text="Average time to first byte in milliseconds"
    ),
    MetricDefinition(
        name="cdn77_requests_total",
        value_extractor=lambda e: 1.0,
        aggregation=AggregationType.SUM,
        labels={
            "stream_name": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "pop": extract_location_id,
        },
        help_text="Total number of requests"
    ),
    MetricDefinition(
        name="cdn77_responses_total",
        value_extractor=lambda e: 1.0,
        aggregation=AggregationType.SUM,
        labels={
            "stream_name": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "response_status": extract_response_status,
            "pop": extract_location_id,
        },
        help_text="Request count by HTTP response status code"
    ),
    MetricDefinition(
        name="cdn77_viewers_total",
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream_name": extract_stream_id,
        },
        help_text="Count of unique IP addresses (viewers) per stream"
    ),
    MetricDefinition(
        name="cdn77_viewers_by_device_total",
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream_name": extract_stream_id,
            "device_type": extract_device_type,
        },
        help_text="Count of unique IP addresses (viewers) per stream by device type"
    ),
    MetricDefinition(
        name="cdn77_viewers_by_country_total",
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream_name": extract_stream_id,
            "country": extract_client_country,
        },
        help_text="Count of unique IP addresses (viewers) per stream by country"
    ),
    MetricDefinition(
        name="cdn77_viewers_by_region_total",
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream_name": extract_stream_id,
            "country": extract_client_country,
            # NOTE: 'region' label is added later in compute_final_values() via GeoIP lookup
            # after deduplication. This avoids doing GeoIP lookups for every event.
        },
        help_text="Count of unique IP addresses (viewers) per stream by country and region"
    ),
]


# ============================================================================
# S3 CLIENT
# ============================================================================


class S3Client:
    """S3 client for CDN77 real-time logs storage"""
    
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, 
                 bucket: str, region: str = 'eu-1'):
        """
        Initialize S3 client for CDN77 storage
        
        Args:
            endpoint_url: S3 base endpoint (e.g., https://eu-1.cdn77-storage.com)
            access_key: AWS access key ID
            secret_key: AWS secret access key
            bucket: Bucket name (e.g., real-time-logs-synwudjt)
            region: AWS region (default: eu-1)
        
        Note: CDN77 uses virtual-hosted style URLs: https://{bucket}.{region}.cdn77-storage.com
        """
        self.bucket = bucket
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        logger.info(f"Initialized S3 client: {endpoint_url}/{bucket}")
    
    def list_objects(self, prefix: str = '', max_keys: int = 1000) -> List[Dict]:
        """
        List objects in bucket with given prefix
        
        Returns list of dicts with: key, size, last_modified
        """
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' not in response:
                return []
            
            objects = []
            for obj in response['Contents']:
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
            
            return objects
            
        except ClientError as e:
            logger.error(f"Failed to list objects: {e}")
            return []
    
    def download_object(self, key: str) -> Optional[bytes]:
        """Download object and return raw bytes"""
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            logger.error(f"Failed to download {key}: {e}")
            return None
    
    def delete_object(self, key: str) -> bool:
        """Delete object from bucket"""
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=key)
            logger.info(f"Deleted: {key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete {key}: {e}")
            return False


class LogParser:
    """Parse CDN77 real-time JSON logs into Event objects"""
    
    # Regex to extract stream ID (hex hash) from path like: /HASH/filename
    STREAM_PATTERN = re.compile(r'^/([a-f0-9]+)/')
    
    def __init__(self, geoip_db_path: Optional[str] = None):
        self.debug_count = 0
        self.success_count = 0
        self.timestamp_cache = {}
        
        # GeoIP lookup
        self.geoip_reader = None
        if geoip_db_path and GEOIP_AVAILABLE:
            try:
                self.geoip_reader = geoip2.database.Reader(geoip_db_path)
                logger.info(f"GeoIP database loaded for LogParser: {geoip_db_path}")
            except Exception as e:
                logger.warning(f"Failed to load GeoIP database in LogParser: {e}")
    
    def lookup_geo(self, ip_address: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Lookup country and region for IP address
        
        Returns:
            Tuple of (country_code, region_name) or (None, None) if not available
        """
        if not self.geoip_reader:
            return (None, None)
        
        try:
            response = self.geoip_reader.city(ip_address)
            country = response.country.iso_code or None
            region = response.subdivisions.most_specific.name if response.subdivisions else None
            return (country, region)
        except Exception:
            return (None, None)
    
    def parse_json_to_event(self, line: str) -> Optional[Event]:
        """
        Parse a single JSON log line into an Event object
        
        Expected JSON fields (comprehensive):
        - timestamp: ISO 8601 timestamp
        - clientRequestPath: Request path
        - responseBytes: Bytes sent
        - resourceID: CDN resource ID
        - cacheStatus: HIT/MISS/etc
        - timeToFirstByteMs: TTFB metric
        - tcpRTTus: TCP round-trip time
        - requestTimeMs: Total request time
        - responseStatus: HTTP status code
        - clientCountry: Country code
        - locationID: Edge location
        """
        try:
            data = json.loads(line)
            
            # Extract required fields
            timestamp_str = data.get('timestamp')
            path = data.get('clientRequestPath')
            
            if not timestamp_str or not path:
                if self.debug_count < 10:
                    logger.error(f"Missing required fields. Data: {line[:200]}")
                    self.debug_count += 1
                return None
            
            # Extract stream ID from path
            match = self.STREAM_PATTERN.match(path)
            if not match:
                # Not a stream path, skip silently
                return None
            
            stream_id = match.group(1)
            
            # Parse ISO 8601 timestamp
            if timestamp_str in self.timestamp_cache:
                dt = self.timestamp_cache[timestamp_str]
            else:
                try:
                    # Parse ISO 8601 format: 2026-01-10T03:46:58.664Z
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    
                    # Limit cache size
                    if len(self.timestamp_cache) < 1000:
                        self.timestamp_cache[timestamp_str] = dt
                except ValueError as e:
                    if self.debug_count < 10:
                        logger.error(f"Invalid timestamp format '{timestamp_str}': {e}")
                        self.debug_count += 1
                    return None
            
            # Log first few successes
            if self.success_count < 5:
                logger.info(f"✓ EVENT PARSED: stream={stream_id}, path={path}, time={dt}")
                self.success_count += 1
            
            # Create Event object
            user_agent = data.get('clientRequestUserAgent', '')
            client_ip = data.get('clientIP', '0.0.0.0')
            
            # Note: GeoIP lookup for region happens during aggregation (after deduplication)
            # to avoid redundant lookups for the same IP
            
            event = Event(
                timestamp=dt,
                stream_id=stream_id,
                resource_id=data.get('resourceID', 0),
                cache_status=data.get('cacheStatus', 'UNKNOWN'),
                response_bytes=data.get('responseBytes', 0),
                time_to_first_byte_ms=data.get('timeToFirstByteMs', 0),
                tcp_rtt_us=data.get('tcpRTTus', 0),
                request_time_ms=data.get('requestTimeMs', 0),
                response_status=data.get('responseStatus', 0),
                client_country=data.get('clientCountry', 'XX'),
                location_id=data.get('locationID', 'unknown'),
                client_ip=client_ip,
                device_type=detect_device_type(user_agent),
                client_region=None,  # Will be populated during aggregation if GeoIP available
                raw_data=data
            )
            
            return event
            
        except json.JSONDecodeError as e:
            if self.debug_count < 10:
                logger.error(f"JSON decode error: {e}. Line: {line[:200]}")
                self.debug_count += 1
            return None
        except Exception as e:
            if self.debug_count < 10:
                logger.error(f"PARSE EXCEPTION: {type(e).__name__}: {e}. Line: {line[:200]}")
                self.debug_count += 1
            return None
    
    def parse_ndjson_with_buffer(self, ndjson_content: str, filename: str, 
                                  buffered_events: List[Event]) -> Tuple[List[Event], List[Event]]:
        """
        Parse NDJSON content into events and split by time window
        
        CRITICAL: Prepends buffered events from previous file to current events
        
        Args:
            ndjson_content: Full NDJSON string
            filename: Source filename for logging
            buffered_events: Events buffered from previous file
        
        Returns:
            Tuple of (events_to_flush, events_to_buffer)
        """
        # Parse all events from current file
        current_events = []
        lines_processed = 0
        invalid_lines = 0
        
        for line in ndjson_content.splitlines():
            if not line.strip():
                continue
            
            lines_processed += 1
            event = self.parse_json_to_event(line)
            
            if event:
                current_events.append(event)
            else:
                invalid_lines += 1
        
        # CRITICAL: Prepend buffered events from previous file
        all_events = buffered_events + current_events
        
        if not all_events:
            logger.info(f"Completed {filename}: {lines_processed:,} lines, {invalid_lines} invalid, 0 events")
            return ([], [])
        
        # Determine time window (29 seconds from earliest event)
        time_window = TimeWindow.from_events(all_events, duration_seconds=29)
        
        if not time_window:
            logger.warning(f"Could not determine time window for {filename}")
            return (all_events, [])
        
        # Split events into flush vs buffer
        events_to_flush = []
        events_to_buffer = []
        
        for event in all_events:
            if time_window.contains(event.timestamp):
                events_to_flush.append(event)
            else:
                events_to_buffer.append(event)
        
        # Log statistics
        min_ts = min(e.timestamp for e in all_events)
        max_ts = max(e.timestamp for e in all_events)
        
        logger.info(
            f"Completed {filename}: {lines_processed:,} lines, {invalid_lines} invalid | "
            f"Events: {len(buffered_events)} buffered + {len(current_events)} new = {len(all_events)} total | "
            f"Window: {time_window.start.strftime('%H:%M:%S')}-{time_window.end.strftime('%H:%M:%S')} | "
            f"Flushing: {len(events_to_flush)}, Buffering: {len(events_to_buffer)} | "
            f"Time range: {min_ts.strftime('%Y-%m-%d %H:%M:%S')} to {max_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        return (events_to_flush, events_to_buffer)


class MetricAggregator:
    """Aggregates events into multiple metrics based on metric definitions"""
    
    def __init__(self, metric_definitions: List[MetricDefinition], geoip_db_path: Optional[str] = None):
        self.metric_definitions = metric_definitions
        self.ip_to_region_cache = {}  # Cache: {ip: region_name or None}
        
        # GeoIP lookup for post-deduplication region enrichment
        self.geoip_reader = None
        if geoip_db_path and GEOIP_AVAILABLE:
            try:
                self.geoip_reader = geoip2.database.Reader(geoip_db_path)
                logger.info(f"GeoIP database loaded for MetricAggregator: {geoip_db_path}")
            except Exception as e:
                logger.warning(f"Failed to load GeoIP database in MetricAggregator: {e}")
    
    def lookup_region(self, ip_address: str) -> Optional[str]:
        """Lookup region for IP address with caching (called only for unique IPs after deduplication)"""
        # Check cache first
        if ip_address in self.ip_to_region_cache:
            return self.ip_to_region_cache[ip_address]
        
        # If no GeoIP reader, cache as None and return
        if not self.geoip_reader:
            self.ip_to_region_cache[ip_address] = None
            return None
        
        # Perform lookup and cache result
        try:
            response = self.geoip_reader.city(ip_address)
            region = response.subdivisions.most_specific.name if response.subdivisions else None
            self.ip_to_region_cache[ip_address] = region
            return region
        except Exception:
            self.ip_to_region_cache[ip_address] = None
            return None
    
    def aggregate_events(self, events: List[Event]) -> Dict[str, Dict[Tuple[str, ...], any]]:
        """
        Aggregate events into metrics
        
        Returns: {metric_name: {label_tuple: [values] or {unique_values}}}
        """
        # Structure: {metric_name: {(label1_val, label2_val, ...): [values] or set(values)}}
        aggregated = {}
        
        for metric_def in self.metric_definitions:
            aggregated[metric_def.name] = {}
            
            for event in events:
                # Extract value
                value = metric_def.extract_value(event)
                if value is None:
                    continue
                
                # Extract labels
                labels = metric_def.extract_labels(event)
                
                # Add minute to labels
                labels['minute'] = event.minute_key
                
                # Create label tuple for grouping (sorted for consistency)
                label_tuple = tuple(labels[k] for k in sorted(labels.keys()))
                
                # Initialize collection based on aggregation type
                if label_tuple not in aggregated[metric_def.name]:
                    if metric_def.aggregation == AggregationType.UNIQUE_COUNT:
                        aggregated[metric_def.name][label_tuple] = set()
                    else:
                        aggregated[metric_def.name][label_tuple] = []
                
                # Add value to aggregation
                if metric_def.aggregation == AggregationType.UNIQUE_COUNT:
                    aggregated[metric_def.name][label_tuple].add(value)
                else:
                    aggregated[metric_def.name][label_tuple].append(value)
        
        return aggregated
    
    def compute_final_values(self, aggregated: Dict[str, Dict[Tuple[str, ...], List[float]]]) -> List[Dict]:
        """
        Compute final metric values based on aggregation type
        
        Returns list of {metric_name, labels, value, timestamp}
        """
        results = []
        
        for metric_def in self.metric_definitions:
            metric_data = aggregated.get(metric_def.name, {})
            
            for label_tuple, values in metric_data.items():
                if not values:
                    continue
                
                # Compute aggregated value
                if metric_def.aggregation == AggregationType.SUM:
                    final_value = sum(values)
                elif metric_def.aggregation == AggregationType.AVG:
                    final_value = sum(values) / len(values)
                elif metric_def.aggregation == AggregationType.MAX:
                    final_value = max(values)
                elif metric_def.aggregation == AggregationType.MIN:
                    final_value = min(values)
                elif metric_def.aggregation == AggregationType.COUNT:
                    final_value = len(values)
                elif metric_def.aggregation == AggregationType.UNIQUE_COUNT:
                    # values is a set for UNIQUE_COUNT
                    # For region-based metrics, enrich IPs with GeoIP lookup AFTER deduplication
                    if metric_def.name == 'cdn77_viewers_by_region_total' and self.geoip_reader:
                        # values is a set of IPs - lookup region for each unique IP
                        # Group by region and count unique IPs per region
                        region_ip_sets = {}  # {region: set of IPs}
                        
                        for ip in values:
                            region = self.lookup_region(ip)
                            if region:
                                if region not in region_ip_sets:
                                    region_ip_sets[region] = set()
                                region_ip_sets[region].add(ip)
                        
                        # Reconstruct labels dict from tuple
                        all_label_keys = sorted(list(metric_def.labels.keys()) + ['minute'])
                        labels_dict = dict(zip(all_label_keys, label_tuple))
                        
                        # Generate separate metric for each region
                        for region, ip_set in region_ip_sets.items():
                            # Update labels to include region
                            region_labels = dict(labels_dict)
                            region_labels['region'] = region
                            
                            results.append({
                                'metric_name': metric_def.name,
                                'labels': region_labels,
                                'value': float(len(ip_set)),
                                'minute_key': labels_dict['minute']
                            })
                        
                        # Skip the normal append below for this metric
                        continue
                    
                    final_value = len(values)
                else:
                    logger.warning(f"Unknown aggregation type: {metric_def.aggregation}")
                    continue
                
                # Reconstruct labels dict from tuple
                # The tuple was created with sorted(labels.keys()), so we need the same order
                # labels.keys() at creation time = sorted(metric_def.labels.keys()) + ['minute']
                all_label_keys = sorted(list(metric_def.labels.keys()) + ['minute'])
                labels_dict = dict(zip(all_label_keys, label_tuple))
                
                results.append({
                    'metric_name': metric_def.name,
                    'labels': labels_dict,
                    'value': final_value,
                    'minute_key': labels_dict['minute']
                })
        
        return results


class PrometheusExporter:
    """Export metrics to Prometheus remote write endpoint"""
    
    def __init__(self, url: str, username: Optional[str] = None, password: Optional[str] = None):
        self.url = url
        self.auth = (username, password) if username and password else None
    
    def build_metrics_from_computed(self, computed_metrics: List[Dict], batch_offset_ms: int = 0) -> List[Dict]:
        """Build Prometheus metrics from computed metric values with optional batch offset in milliseconds"""
        metrics = []
        oldest_allowed = int((datetime.now(timezone.utc).timestamp() - (7 * 24 * 60 * 60)) * 1000)
        skipped_old = 0
        parse_failed = 0
        
        for computed in computed_metrics:
            metric_name = computed['metric_name']
            labels = computed['labels']
            value = computed['value']
            minute_key = computed['minute_key']
            
            # Parse datetime from minute_key and add batch offset
            try:
                dt = datetime.strptime(minute_key, '%d/%b/%Y %H:%M')
                dt = dt.replace(tzinfo=timezone.utc)
                # Add batch offset in milliseconds to prevent duplicate timestamps
                timestamp_ms = int(dt.timestamp() * 1000) + batch_offset_ms
            except Exception as e:
                parse_failed += 1
                if parse_failed <= 3:
                    logger.warning(f"Failed to parse timestamp '{minute_key}': {e}")
                continue
            
            # Skip samples older than 7 days (Prometheus backfill limit)
            if timestamp_ms < oldest_allowed:
                skipped_old += 1
                continue
            
            # Build label string (exclude 'minute' as it's just for grouping)
            label_strs = []
            for key, val in sorted(labels.items()):
                if key != 'minute':
                    label_strs.append(f'{key}="{val}"')
            label_string = ','.join(label_strs)
            
            metrics.append({
                'metric': f'{metric_name}{{{label_string}}}',
                'value': value,
                'timestamp': timestamp_ms
            })
        
        if skipped_old > 0 or parse_failed > 0:
            logger.warning(f"Skipped {skipped_old} old, {parse_failed} parse failures")
        
        return metrics
    
    def build_gauge_metrics(self, gauge_data: List[Dict]) -> List[Dict]:
        """
        Build Prometheus metrics from gauge data (already has timestamps).
        
        Args:
            gauge_data: List of dicts with metric_name, value, timestamp_ms, labels
        """
        metrics = []
        oldest_allowed = int((datetime.now(timezone.utc).timestamp() - (7 * 24 * 60 * 60)) * 1000)
        skipped_old = 0
        
        for data in gauge_data:
            metric_name = data['metric_name']
            labels = data['labels']
            value = data['value']
            timestamp_ms = data['timestamp_ms']
            
            # Skip samples older than 7 days
            if timestamp_ms < oldest_allowed:
                skipped_old += 1
                continue
            
            # Build label string
            label_strs = []
            for key, val in sorted(labels.items()):
                label_strs.append(f'{key}="{val}"')
            label_string = ','.join(label_strs)
            
            metrics.append({
                'metric': f'{metric_name}{{{label_string}}}',
                'value': value,
                'timestamp': timestamp_ms
            })
        
        if skipped_old > 0:
            logger.warning(f"Skipped {skipped_old} old gauge metrics")
        
        return metrics
    
    def push_metrics(self, metrics: List[Dict], log_prefix: str = "", retry_count: int = 0) -> bool:
        """Push metrics to Prometheus remote write endpoint with automatic retry on duplicate timestamp"""
        if not metrics:
            logger.warning(f"{log_prefix}No metrics to push")
            return False
        
        prefix = f"{log_prefix}" if log_prefix else ""
        
        try:
            # Build Prometheus remote write protobuf
            import remote_pb2
            
            write_request = remote_pb2.WriteRequest()
            
            for metric_data in metrics:
                ts = write_request.timeseries.add()
                
                # Parse metric name and labels
                metric_str = metric_data['metric']
                brace_idx = metric_str.index('{')
                metric_name = metric_str[:brace_idx]
                labels_str = metric_str[brace_idx+1:-1]
                
                # Add __name__ label
                label = ts.labels.add()
                label.name = '__name__'
                label.value = metric_name
                
                # Add other labels
                for label_pair in labels_str.split(','):
                    eq_idx = label_pair.index('=')
                    key = label_pair[:eq_idx]
                    value = label_pair[eq_idx+2:-1]
                    label = ts.labels.add()
                    label.name = key
                    label.value = value
                
                # Add sample
                sample = ts.samples.add()
                sample.value = float(metric_data['value'])
                sample.timestamp = metric_data['timestamp']
            
            # Serialize and compress
            serialized = write_request.SerializeToString()
            compressed = snappy.compress(serialized)
            
            # POST to remote write endpoint
            headers = {
                'Content-Encoding': 'snappy',
                'Content-Type': 'application/x-protobuf',
                'X-Prometheus-Remote-Write-Version': '0.1.0'
            }
            
            response = requests.post(
                self.url,
                data=compressed,
                headers=headers,
                auth=self.auth
            )
            response.raise_for_status()
            logger.info(f"{prefix}Successfully pushed {len(metrics)} metrics to Prometheus")
            return True
            
        except requests.exceptions.HTTPError as e:
            # Check if it's a duplicate timestamp error
            if (hasattr(e, 'response') and e.response is not None and 
                e.response.status_code == 400 and 
                'duplicate sample' in e.response.text and 
                retry_count < 5):
                
                logger.warning(f"{prefix}Duplicate timestamp detected, incrementing by 2s and retrying (attempt {retry_count + 1}/5)")
                
                # Increment all timestamps by 2 seconds (2000ms)
                incremented_metrics = []
                for metric_data in metrics:
                    incremented_metric = metric_data.copy()
                    incremented_metric['timestamp'] = metric_data['timestamp'] + 2000
                    incremented_metrics.append(incremented_metric)
                
                # Retry with incremented timestamps
                return self.push_metrics(incremented_metrics, log_prefix, retry_count + 1)
            
            logger.error(f"{prefix}⚠️  PROMETHEUS REJECTED PUSH: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"{prefix}Response status: {e.response.status_code}")
                logger.error(f"{prefix}Response body: {e.response.text[:500]}")
            return False
        except Exception as e:
            logger.error(f"{prefix}Failed to push metrics: {e}")
            return False


class PrometheusWriterThread(threading.Thread):
    """Background thread that handles Prometheus write operations"""
    
    def __init__(self, exporter: PrometheusExporter, write_queue: queue.Queue):
        super().__init__(daemon=True, name="PrometheusWriter")
        self.exporter = exporter
        self.write_queue = write_queue
        self.running = True
        self.metrics_written = 0
        self.metrics_failed = 0
    
    def run(self):
        """Process metrics from queue and write to Prometheus"""
        logger.info("Prometheus writer thread started")
        
        while self.running:
            try:
                try:
                    job = self.write_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                if job is None:  # Shutdown signal
                    break
                
                metrics, source = job
                log_prefix = f"[{source}] "
                
                if self.exporter.push_metrics(metrics, log_prefix):
                    self.metrics_written += len(metrics)
                else:
                    self.metrics_failed += len(metrics)
                
                self.write_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in writer thread: {e}", exc_info=True)
        
        logger.info(f"Prometheus writer thread stopped (written: {self.metrics_written}, failed: {self.metrics_failed})")
    
    def stop(self):
        """Signal thread to stop"""
        self.running = False
        self.write_queue.put(None)


class S3Importer:
    """Main S3 log importer - processes events with buffering for overlaps"""
    
    def __init__(self, s3_client: S3Client, prometheus_url: str,
                 prometheus_user: Optional[str] = None, prometheus_password: Optional[str] = None,
                 s3_prefix: str = 'real-time-logs/logs',
                 metric_definitions: List[MetricDefinition] = None,
                 flush_delay_seconds: int = 180,
                 geoip_db_path: Optional[str] = None):
        self.s3_client = s3_client
        self.s3_prefix = s3_prefix.rstrip('/')  # Remove trailing slash
        self.parser = LogParser(geoip_db_path=geoip_db_path)
        self.shutdown_requested = False
        self.aggregator = MetricAggregator(metric_definitions or METRIC_DEFINITIONS, geoip_db_path=geoip_db_path)
        self.exporter = PrometheusExporter(prometheus_url, prometheus_user, prometheus_password)
        self.write_queue = queue.Queue(maxsize=100)
        
        # Event buffer for handling file overlaps - flush immediately with batch offsets
        self.event_buffer = EventBuffer()
        self.flush_delay_seconds = flush_delay_seconds  # Still used for determining when to flush
        
        # Track flush count per minute for timestamp offsets (prevents duplicates)
        # Key: minute_key, Value: (flush_count, last_flush_time)
        self.flush_count_per_minute: Dict[str, tuple[int, datetime]] = {}
        
        # Random startup offset (0-999ms) to prevent collisions across restarts/instances
        self.startup_offset_ms = random.randint(0, 999)
        logger.info(f"Generated startup offset: {self.startup_offset_ms}ms")
        
        # Track dropped events (for statistics)
        self.dropped_events = 0
        
        # Start writer thread
        self.writer_thread = PrometheusWriterThread(self.exporter, self.write_queue)
        self.writer_thread.start()
        self.last_flush_time = datetime.now(timezone.utc)
    
    def _flush_events(self, events: List[Event], source: str):
        """Aggregate events into metrics and queue for Prometheus with batch offsets"""
        if not events:
            return
        
        now = datetime.now(timezone.utc)
        
        # Group events by minute_key
        events_by_minute = {}
        for event in events:
            minute_key = event.minute_key
            if minute_key not in events_by_minute:
                events_by_minute[minute_key] = []
            events_by_minute[minute_key].append(event)
        
        # Cleanup old flush counts (>1 hour old) to prevent memory leak
        cutoff_time = now - timedelta(hours=1)
        keys_to_remove = [k for k, (_, last_time) in self.flush_count_per_minute.items() 
                         if last_time < cutoff_time]
        for key in keys_to_remove:
            del self.flush_count_per_minute[key]
        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} old flush count entries")
        
        # Flush each minute separately with batch offset
        total_metrics = 0
        for minute_key in sorted(events_by_minute.keys()):
            minute_events = events_by_minute[minute_key]
            
            # Get or initialize flush count for this minute
            if minute_key in self.flush_count_per_minute:
                flush_count, first_flush_time = self.flush_count_per_minute[minute_key]
                flush_count += 1
                
                # Warn if flushing more than 10 minutes after original time
                time_since_first = (now - first_flush_time).total_seconds() / 60
                if time_since_first > 10:
                    logger.warning(f"Late batch for {minute_key}: {time_since_first:.1f} minutes after first flush (batch #{flush_count})")
            else:
                flush_count = 0
                first_flush_time = now
            
            self.flush_count_per_minute[minute_key] = (flush_count, first_flush_time)
            
            # Calculate batch offset (100ms per batch) + startup offset
            batch_offset_ms = (flush_count * 100) + self.startup_offset_ms
            
            # Aggregate events for this minute
            aggregated = self.aggregator.aggregate_events(minute_events)
            
            # Compute final values
            computed_metrics = self.aggregator.compute_final_values(aggregated)
            
            # Build Prometheus metrics for this minute with combined offset
            metrics = self.exporter.build_metrics_from_computed(computed_metrics, batch_offset_ms)
            
            if metrics:
                offset_str = f" [+{batch_offset_ms}ms offset]" if batch_offset_ms > 0 else ""
                logger.info(f"Flushing {len(metrics)} metrics for minute {minute_key} ({len(minute_events)} events){offset_str}")
                try:
                    self.write_queue.put((metrics, source), timeout=5.0)
                    total_metrics += len(metrics)
                except queue.Full:
                    logger.error(f"Queue full! Dropping {len(metrics)} metrics for minute {minute_key}")
    
    def _check_and_flush_buffer(self):
        """Flush all buffered events immediately (batch offsets prevent duplicates)"""
        # Get all buffered events
        all_events = self.event_buffer.get_and_clear()
        
        if not all_events:
            return
        
        # Flush all events immediately with batch offset system
        flush_minutes = set(e.minute_key for e in all_events)
        logger.info(f"Flushing buffer: {len(all_events)} events (minutes: {sorted(flush_minutes)})")
        self._flush_events(all_events, "Buffer")
    
    def process_file(self, s3_key: str) -> bool:
        """Download, decompress, parse, and buffer events (no immediate flush)"""
        try:
            logger.info(f"Processing: {s3_key}")
            
            # Download file
            compressed_data = self.s3_client.download_object(s3_key)
            if not compressed_data:
                return False
            
            # Decompress gzip
            try:
                ndjson_content = gzip.decompress(compressed_data).decode('utf-8')
            except Exception as e:
                logger.error(f"Failed to decompress {s3_key}: {e}")
                return False
            
            # Parse events from file
            events = []
            lines_processed = 0
            invalid_lines = 0
            
            for line in ndjson_content.splitlines():
                if not line.strip():
                    continue
                
                lines_processed += 1
                event = self.parser.parse_json_to_event(line)
                
                if event:
                    events.append(event)
                else:
                    invalid_lines += 1
            
            # Add all events to buffer (no immediate flush)
            if events:
                self.event_buffer.add_many(events)
                
            # Log statistics
            if events:
                min_ts = min(e.timestamp for e in events)
                max_ts = max(e.timestamp for e in events)
                time_range = f"{min_ts.strftime('%H:%M:%S')} to {max_ts.strftime('%H:%M:%S')} UTC"
            else:
                time_range = "no valid events"
            
            logger.info(
                f"Buffered {s3_key}: {lines_processed:,} lines, {invalid_lines} invalid, "
                f"{len(events)} buffered | "
                f"Time range: {time_range}"
            )
            
            # Delete from S3 after successful processing
            self.s3_client.delete_object(s3_key)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing {s3_key}: {e}", exc_info=True)
            return False
    
    def get_prefix_for_lookback(self, lookback_hours: int) -> List[str]:
        """
        Generate S3 prefixes for the lookback period
        Path format: {s3_prefix}/YYYYMMDD/HH/
        """
        prefixes = []
        now = datetime.now(timezone.utc)
        
        for hour_offset in range(lookback_hours):
            dt = now - timedelta(hours=hour_offset)
            prefix = f"{self.s3_prefix}/{dt.strftime('%Y%m%d')}/{dt.strftime('%H')}/"
            prefixes.append(prefix)
        
        return prefixes
    
    def poll_once(self, lookback_hours: int = 2):
        """Poll S3 for new files and process them (files are deleted after processing)"""
        logger.info(f"Polling S3 (lookback: {lookback_hours}h)")
        
        # Get prefixes to check (oldest first)
        prefixes = self.get_prefix_for_lookback(lookback_hours)
        prefixes.reverse()  # Process oldest hour first
        
        total_found = 0
        total_processed = 0
        
        for prefix in prefixes:
            # List objects in this prefix
            objects = self.s3_client.list_objects(prefix=prefix)
            
            # Sort by key to ensure chronological processing
            # Files have format: rtl.{uuid}+{shard}+{sequence}.ndjson.gz
            # Sorting by key ensures sequence order
            objects.sort(key=lambda x: x['key'])
            
            logger.info(f"Prefix {prefix}: {len(objects)} files")
            
            for obj in objects:
                key = obj['key']
                total_found += 1
                
                # Process file (buffers events, no immediate flush)
                if self.process_file(key):
                    total_processed += 1
                    
                # Check if it's time to flush buffered events
                self._check_and_flush_buffer()
        
        # Final flush check after processing all files
        self._check_and_flush_buffer()
        
        # Wait for queue to empty
        logger.info("Waiting for writer thread to finish...")
        self.write_queue.join()
        
        # Report buffer status and writer stats for this poll
        buffered_count = self.event_buffer.size()
        logger.info(f"Poll complete: {total_found} found, {total_processed} processed, "
                   f"{buffered_count} events buffered")
        logger.info(f"Writer stats: {self.writer_thread.metrics_written} written, "
                   f"{self.writer_thread.metrics_failed} failed")
        
        # Memory diagnostics
        logger.info(f"Memory tracking: flush_count_per_minute={len(self.flush_count_per_minute)} entries, "
                   f"timestamp_cache={len(self.parser.timestamp_cache)} entries, "
                   f"queue_size={self.write_queue.qsize()}")
        
        # Reset counters for next poll
        self.writer_thread.metrics_written = 0
        self.writer_thread.metrics_failed = 0
    
    def run_daemon(self, poll_interval: int = 60, lookback_hours: int = 2):
        """Run as daemon that polls every N seconds"""
        logger.info(f"Starting S3 importer daemon (poll interval: {poll_interval}s)")
        
        while not self.shutdown_requested:
            try:
                self.poll_once(lookback_hours=lookback_hours)
            except Exception as e:
                logger.error(f"Error in poll cycle: {e}", exc_info=True)
            
            if self.shutdown_requested:
                break
            
            logger.info(f"Sleeping for {poll_interval}s...")
            for _ in range(poll_interval):
                if self.shutdown_requested:
                    break
                time.sleep(1)
    
    def shutdown(self):
        """Graceful shutdown - waits for writer thread to flush all pending metrics"""
        logger.info("Shutdown requested - stopping new file processing...")
        self.shutdown_requested = True
        
        # Final buffer flush (flush everything regardless of age)
        all_events = self.event_buffer.get_and_clear()
        if all_events:
            logger.info(f"Final flush: {len(all_events)} buffered events")
            self._flush_events(all_events, "Shutdown")
        
        # Wait for write queue to empty
        queue_size = self.write_queue.qsize()
        if queue_size > 0:
            logger.info(f"Waiting for {queue_size} metric batches to flush to Prometheus...")
            self.write_queue.join()
            logger.info("All pending metrics flushed successfully")
        
        # Stop writer thread
        logger.info("Stopping writer thread...")
        self.writer_thread.stop()
        self.writer_thread.join(timeout=10)
        
        logger.info(f"Shutdown complete (written: {self.writer_thread.metrics_written}, "
                   f"failed: {self.writer_thread.metrics_failed}, dropped: {self.dropped_events})")


def main():
    parser = argparse.ArgumentParser(description='CDN77 S3 Real-Time Logs to Prometheus Importer')
    
    # S3 configuration
    parser.add_argument('--s3-endpoint', default=os.getenv('S3_ENDPOINT'),
                       help='S3 base endpoint URL, e.g. https://eu-1.cdn77-storage.com (or S3_ENDPOINT env var)')
    parser.add_argument('--s3-bucket', default=os.getenv('S3_BUCKET'),
                       help='S3 bucket name, e.g. real-time-logs-synwudjt (or S3_BUCKET env var)')
    parser.add_argument('--s3-access-key', default=os.getenv('S3_ACCESS_KEY'),
                       help='S3 access key (or S3_ACCESS_KEY env var)')
    parser.add_argument('--s3-secret-key', default=os.getenv('S3_SECRET_KEY'),
                       help='S3 secret key (or S3_SECRET_KEY env var)')
    parser.add_argument('--s3-region', default=os.getenv('S3_REGION', 'eu-1'),
                       help='S3 region (default: eu-1)')
    parser.add_argument('--s3-prefix', default=os.getenv('S3_PREFIX', 'real-time-logs/logs'),
                       help='S3 folder prefix path (default: real-time-logs/logs, or S3_PREFIX env var)')
    
    # Prometheus configuration
    parser.add_argument('--prometheus-url', default=os.getenv('PROMETHEUS_URL'),
                       help='Prometheus remote write URL (or PROMETHEUS_URL env var)')
    parser.add_argument('--prometheus-user', default=os.getenv('PROMETHEUS_USER'),
                       help='Prometheus username (or PROMETHEUS_USER env var)')
    parser.add_argument('--prometheus-password', default=os.getenv('PROMETHEUS_PASSWORD'),
                       help='Prometheus password (or PROMETHEUS_PASSWORD env var)')
    
    # Operational parameters
    parser.add_argument('--poll-interval', type=int, default=60,
                       help='Polling interval in seconds (default: 60)')
    parser.add_argument('--lookback-hours', type=int, default=2,
                       help='How many hours back to look for files (default: 2)')
    parser.add_argument('--flush-delay', type=int, default=60,
                       help='Wait this many seconds before flushing events (default: 60)')
    parser.add_argument('--geoip-db-path', default=os.getenv('GEOIP_DB_PATH'),
                       help='Path to MaxMind GeoIP2 database file (optional, or GEOIP_DB_PATH env var)')
    
    args = parser.parse_args()
    
    # Validate required arguments
    required = {
        'S3 endpoint': args.s3_endpoint,
        'S3 bucket': args.s3_bucket,
        'S3 access key': args.s3_access_key,
        'S3 secret key': args.s3_secret_key,
        'Prometheus URL': args.prometheus_url
    }
    
    missing = [name for name, value in required.items() if not value]
    if missing:
        logger.error(f"Missing required parameters: {', '.join(missing)}")
        sys.exit(1)
    
    # Create S3 client
    s3_client = S3Client(
        endpoint_url=args.s3_endpoint,
        access_key=args.s3_access_key,
        secret_key=args.s3_secret_key,
        bucket=args.s3_bucket,
        region=args.s3_region
    )
    
    # Create importer
    importer = S3Importer(
        s3_client=s3_client,
        prometheus_url=args.prometheus_url,
        prometheus_user=args.prometheus_user,
        prometheus_password=args.prometheus_password,
        s3_prefix=args.s3_prefix,
        flush_delay_seconds=args.flush_delay,
        geoip_db_path=args.geoip_db_path
    )
    
    # Setup signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        logger.info(f"Received {signal_name} - initiating graceful shutdown...")
        importer.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run daemon
    try:
        importer.run_daemon(
            poll_interval=args.poll_interval,
            lookback_hours=args.lookback_hours
        )
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        importer.shutdown()
        sys.exit(1)
        importer.shutdown()
        sys.exit(1)


if __name__ == '__main__':
    main()
