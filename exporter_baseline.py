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
import ipaddress
import json
import logging
import os
import queue
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
from urllib.parse import unquote

import boto3
from botocore.exceptions import ClientError
import requests
import snappy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

try:
    import geoip2.database
    from geoip2.errors import AddressNotFoundError
    GEOIP_AVAILABLE = True
except ImportError:
    GEOIP_AVAILABLE = False
    logger.warning("GeoIP2 not available. Install with: pip install geoip2 maxminddb")


# Metric prefix configuration
METRIC_PREFIX = os.getenv('METRIC_PREFIX', 'cdn_')


def _env_int(name: str, default: int) -> int:
    """Read integer environment variable with fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning(f"Invalid integer for {name}={raw!r}; using default {default}")
        return default


def _env_bool(name: str, default: bool) -> bool:
    """Read boolean environment variable with fallback."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _duration_label_from_seconds(seconds: int) -> str:
    """Format duration in seconds as compact human-readable label."""
    if seconds <= 0:
        return "0s"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _watch_band_label(duration_seconds: float) -> str:
    """Map watch duration to a coarse product-facing band label."""
    if duration_seconds <= 60:
        return "0_1m"
    if duration_seconds <= 300:
        return "1_5m"
    if duration_seconds <= 600:
        return "5_10m"
    if duration_seconds <= 1200:
        return "10_20m"
    if duration_seconds <= 3600:
        return "20_60m"
    return "60_plus"


def _ip_version_label(ip_address_text: str) -> str:
    """Classify textual IP address as ipv4/ipv6, falling back to unknown."""
    try:
        return "ipv4" if ipaddress.ip_address(ip_address_text).version == 4 else "ipv6"
    except ValueError:
        return "unknown"


def floor_to_minute(dt: datetime) -> datetime:
    """Round datetime down to minute boundary in UTC."""
    return dt.astimezone(timezone.utc).replace(second=0, microsecond=0)


def minute_timestamp_ms(dt: datetime, batch_offset_ms: int = 0) -> int:
    """Convert datetime to minute-aligned Unix timestamp in milliseconds."""
    return int(floor_to_minute(dt).timestamp() * 1000) + batch_offset_ms


def minute_key_to_datetime(minute_key: str) -> datetime:
    """Parse exporter minute key into UTC datetime."""
    dt = datetime.strptime(minute_key, '%d/%b/%Y %H:%M')
    return dt.replace(tzinfo=timezone.utc)


UNMATCHED_DEVICE_LOGGING = _env_bool('UNMATCHED_DEVICE_LOGGING', True)
UNMATCHED_DEVICE_MAX_TRACKED = _env_int('UNMATCHED_DEVICE_MAX_TRACKED', 5000)
EMIT_LEGACY_STREAM_NAME_LABEL = _env_bool('EMIT_LEGACY_STREAM_NAME_LABEL', True)

_unmatched_device_lock = threading.Lock()
_unmatched_device_counts: Dict[str, int] = {}
_unmatched_device_overflow_total = 0


def metric_name(suffix: str) -> str:
    """Build metric name with configured prefix."""
    return f"{METRIC_PREFIX}{suffix}"


def add_legacy_stream_name_label(labels: Dict[str, str]) -> Dict[str, str]:
    """Add legacy stream_name label mirroring stream label when enabled."""
    if not EMIT_LEGACY_STREAM_NAME_LABEL:
        return labels

    stream = labels.get('stream')
    if not stream:
        return labels

    compat_labels = labels.copy()
    compat_labels.setdefault('stream_name', stream)
    return compat_labels


def stream_label_selector(stream: str) -> str:
    """Build stream label selector string with optional legacy stream_name mirror."""
    labels = [f'stream="{stream}"']
    if EMIT_LEGACY_STREAM_NAME_LABEL:
        labels.append(f'stream_name="{stream}"')
    return ','.join(labels)


def _log_unmatched_device(user_agent: str):
    """Accumulate unmatched user agents for end-of-poll summary logging."""
    if not UNMATCHED_DEVICE_LOGGING:
        return

    ua = user_agent.strip() if user_agent else ""
    if not ua:
        ua = "<empty>"

    normalized = ua[:300]

    global _unmatched_device_overflow_total
    with _unmatched_device_lock:
        if normalized in _unmatched_device_counts:
            _unmatched_device_counts[normalized] += 1
            return

        if len(_unmatched_device_counts) < UNMATCHED_DEVICE_MAX_TRACKED:
            _unmatched_device_counts[normalized] = 1
        else:
            _unmatched_device_overflow_total += 1


def flush_unmatched_device_summary() -> int:
    """Log unmatched user-agent counts, reset accumulator, and return total unmatched."""
    if not UNMATCHED_DEVICE_LOGGING:
        return 0

    global _unmatched_device_overflow_total
    with _unmatched_device_lock:
        if not _unmatched_device_counts and _unmatched_device_overflow_total == 0:
            return 0

        counts = _unmatched_device_counts.copy()
        overflow_total = _unmatched_device_overflow_total
        _unmatched_device_counts.clear()
        _unmatched_device_overflow_total = 0

    total_unmatched = sum(counts.values()) + overflow_total
    logger.info(
        f"Unmatched device summary: total={total_unmatched}, unique={len(counts)}, overflow={overflow_total}"
    )

    for user_agent, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
        logger.info(f"Unmatched user-agent count={count}: {user_agent}")

    return total_unmatched


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
    raw_data: Dict[str, Any]  # Full JSON for future extensibility
    
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
    first_seen: datetime
    last_seen: datetime
    device_type: str
    country: str
    region: str  # State/province
    watch_session_closed: bool = False


class SessionTracker:
    """
    Tracks unique IP sessions in a rolling time window for accurate concurrent viewer counts.
    
    Handles retroactive data by updating timestamps as new events arrive.
    Generates gauge metrics representing unique viewers in the window.
    """
    
    def __init__(
        self,
        window_seconds: int = 7200,
        geoip_db_path: Optional[str] = None,
        session_gap_seconds: int = 120,
    ):
        """
        Initialize session tracker
        
        Args:
            window_seconds: Rolling window size in seconds (default: 2 hours)
            geoip_db_path: Path to MaxMind GeoIP2 database file (optional)
        """
        self.window = timedelta(seconds=window_seconds)
        self.session_gap_seconds = max(1, session_gap_seconds)
        self.watch_time_buckets_seconds: Tuple[int, ...] = (60, 180, 300, 600, 1200, 1800, 3600, 7200)
        
        # Store: {stream_id: {ip_address: SessionInfo}}
        self.sessions: Dict[str, Dict[str, SessionInfo]] = defaultdict(dict)
        self._lock = threading.Lock()
        self._geo_cache: Dict[str, Tuple[str, str]] = {}
        self._geo_warning_count = 0
        self._geo_warning_limit = 25
        self._geo_reader_missing_logged = False
        self._watch_sessions_total: Dict[str, int] = defaultdict(int)
        self._watch_sessions_by_device_total: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._watch_time_seconds_total: Dict[str, float] = defaultdict(float)
        self._watch_time_seconds_by_device_total: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._watch_time_seconds_by_device_band_total: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(float))
        )
        self._watch_duration_sum: Dict[str, float] = defaultdict(float)
        self._watch_duration_count: Dict[str, int] = defaultdict(int)
        self._watch_duration_bucket_counts: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
        self._watch_duration_band_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        
        # GeoIP lookup
        self.geoip_reader = None
        if geoip_db_path and GEOIP_AVAILABLE:
            try:
                self.geoip_reader = geoip2.database.Reader(geoip_db_path)
                logger.info(f"GeoIP database loaded: {geoip_db_path}")
            except Exception as e:
                logger.warning(f"Failed to load GeoIP database: {e}")
        elif geoip_db_path and not GEOIP_AVAILABLE:
            logger.warning(
                f"GEOIP_DB_PATH is set to '{geoip_db_path}', but geoip2/maxminddb libraries are unavailable; "
                "region metrics will not be emitted"
            )
        else:
            logger.warning(
                "GEOIP_DB_PATH is not set; viewers_by_region metrics will not be emitted"
            )

    def _log_geo_warning(self, message: str):
        """Rate-limit GeoIP warnings to avoid log spam."""
        if self._geo_warning_count < self._geo_warning_limit:
            logger.warning(message)
            self._geo_warning_count += 1
            if self._geo_warning_count == self._geo_warning_limit:
                logger.warning("GeoIP warning log limit reached; suppressing further per-IP warnings")

    def _record_closed_session(self, stream_id: str, session: SessionInfo):
        """Aggregate watch-time metrics for a closed session."""
        duration_seconds = max(0.0, (session.last_seen - session.first_seen).total_seconds())
        duration_band = _watch_band_label(duration_seconds)
        device_type = session.device_type or 'other'

        self._watch_sessions_total[stream_id] += 1
        self._watch_sessions_by_device_total[stream_id][device_type] += 1
        self._watch_time_seconds_total[stream_id] += duration_seconds
        self._watch_time_seconds_by_device_total[stream_id][device_type] += duration_seconds
        self._watch_time_seconds_by_device_band_total[stream_id][device_type][duration_band] += duration_seconds
        self._watch_duration_sum[stream_id] += duration_seconds
        self._watch_duration_count[stream_id] += 1
        self._watch_duration_band_counts[stream_id][duration_band] += 1
        for bucket in self.watch_time_buckets_seconds:
            if duration_seconds <= bucket:
                self._watch_duration_bucket_counts[stream_id][bucket] += 1

    def _close_idle_watch_sessions_locked(self, reference_time: datetime):
        """Close sessions idle longer than session_gap_seconds for watch-time accounting."""
        idle_cutoff = reference_time - timedelta(seconds=self.session_gap_seconds)
        for stream_id, sessions in self.sessions.items():
            for session in sessions.values():
                if session.watch_session_closed:
                    continue
                if session.last_seen < idle_cutoff:
                    self._record_closed_session(stream_id, session)
                    session.watch_session_closed = True

    def lookup_geo(self, ip_address: str) -> Tuple[str, str]:
        """
        Lookup country and region for IP address
        
        Returns:
            Tuple of (country_code, region_name)
        """
        cached = self._geo_cache.get(ip_address)
        if cached is not None:
            return cached

        if not self.geoip_reader:
            if not self._geo_reader_missing_logged:
                logger.warning(
                    "GeoIP reader is not initialized; returning country='XX', region='Unknown' for lookups"
                )
                self._geo_reader_missing_logged = True
            result = ("XX", "Unknown")
            self._geo_cache[ip_address] = result
            return result
        
        try:
            response = self.geoip_reader.city(ip_address)
            country = response.country.iso_code or "XX"
            region = response.subdivisions.most_specific.name if response.subdivisions else "Unknown"
            result = (country, region)
            self._geo_cache[ip_address] = result
            return result
        except (AddressNotFoundError, AttributeError):
            self._log_geo_warning(
                f"GeoIP lookup unresolved for IP {ip_address}; using country='XX', region='Unknown'"
            )
            result = ("XX", "Unknown")
            self._geo_cache[ip_address] = result
            return result
        except Exception as e:
            self._log_geo_warning(
                f"GeoIP lookup failed for IP {ip_address}: {e}; using country='XX', region='Unknown'"
            )
            result = ("XX", "Unknown")
            self._geo_cache[ip_address] = result
            return result
    
    def update(self, events: List[Event]):
        """
        Update session tracker with new events.
        Events may be retroactive (from delayed files).
        """
        with self._lock:
            for event in events:
                # Try to get country from event first; region usually requires GeoIP
                country = event.client_country if hasattr(event, 'client_country') and event.client_country else None
                region = "Unknown"

                if self.geoip_reader:
                    geo_country, geo_region = self.lookup_geo(event.client_ip)
                    if not country:
                        country = geo_country
                    if geo_region:
                        region = geo_region

                if not country:
                    country = "XX"
                
                # Update or create session info
                current_session = self.sessions[event.stream_id].get(event.client_ip)
                
                if current_session is None:
                    self.sessions[event.stream_id][event.client_ip] = SessionInfo(
                        first_seen=event.timestamp,
                        last_seen=event.timestamp,
                        device_type=event.device_type,
                        country=country,
                        region=region,
                    )
                    continue

                # Ignore out-of-order older events for session timing updates
                if event.timestamp <= current_session.last_seen:
                    continue

                gap_seconds = (event.timestamp - current_session.last_seen).total_seconds()
                if gap_seconds > self.session_gap_seconds:
                    # Previous session closed, start a new one for this IP
                    if not current_session.watch_session_closed:
                        self._record_closed_session(event.stream_id, current_session)
                    self.sessions[event.stream_id][event.client_ip] = SessionInfo(
                        first_seen=event.timestamp,
                        last_seen=event.timestamp,
                        device_type=event.device_type,
                        country=country,
                        region=region,
                    )
                else:
                    if current_session.watch_session_closed:
                        # Session was previously closed due idle timeout; start fresh.
                        self.sessions[event.stream_id][event.client_ip] = SessionInfo(
                            first_seen=event.timestamp,
                            last_seen=event.timestamp,
                            device_type=event.device_type,
                            country=country,
                            region=region,
                        )
                    else:
                        # Continue current session
                        current_session.last_seen = event.timestamp
                        current_session.device_type = event.device_type
                        current_session.country = country
                        current_session.region = region
    
    def expire_old(self, reference_time: datetime):
        """
        Remove sessions older than the rolling window.
        
        Args:
            reference_time: Current time to calculate cutoff
        """
        cutoff = reference_time - self.window
        
        with self._lock:
            self._close_idle_watch_sessions_locked(reference_time)
            for stream_id in list(self.sessions.keys()):
                expired_ips = [
                    ip for ip, session in self.sessions[stream_id].items()
                    if session.last_seen < cutoff
                ]
                
                for ip in expired_ips:
                    session = self.sessions[stream_id][ip]
                    if not session.watch_session_closed:
                        self._record_closed_session(stream_id, session)
                    del self.sessions[stream_id][ip]
                
                # Remove empty streams
                if not self.sessions[stream_id]:
                    del self.sessions[stream_id]
    
    def get_gauge_metrics(self, reference_time: Optional[datetime] = None, batch_offset_ms: int = 0) -> List[Dict]:
        """
        Generate gauge metrics for current session state.
        
        Returns metrics representing unique viewers in the rolling window:
        - cdn77_viewers: Total unique IPs per stream
        - cdn77_viewers_by_device: Unique IPs per stream per device
        - cdn77_viewers_by_ip_version: Unique IPs per stream per IP version
        - cdn77_viewers_by_country: Unique IPs per stream per country
        - cdn77_viewers_by_region: Unique IPs per stream per region
        
        Args:
            reference_time: Reference time for timestamp/cutoff calculations (defaults to now)
            batch_offset_ms: Millisecond offset to prevent duplicate timestamps
        """
        with self._lock:
            metrics = []
            reference_dt = floor_to_minute(reference_time or datetime.now(timezone.utc))
            timestamp_ms = int(reference_dt.timestamp() * 1000) + batch_offset_ms
            skipped_unknown_region_series = 0
            
            for stream_id, sessions in self.sessions.items():
                if not sessions:
                    continue
                
                # Total unique viewers
                metrics.append({
                    'metric_name': metric_name('viewers'),
                    'value': float(len(sessions)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {
                        'stream': stream_id,
                    }
                })
                
                # Group by device type
                by_device = defaultdict(set)
                by_ip_version = defaultdict(set)
                by_country = defaultdict(set)
                by_region = defaultdict(set)
                
                for ip, session in sessions.items():
                    by_device[session.device_type].add(ip)
                    by_ip_version[_ip_version_label(ip)].add(ip)
                    by_country[session.country].add(ip)
                    by_region[f"{session.country}_{session.region}"].add(ip)
                
                # Device metrics
                for device_type, ips in by_device.items():
                    metrics.append({
                        'metric_name': metric_name('viewers_by_device'),
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'device_type': device_type,
                        }
                    })

                # IP version metrics
                for ip_version, ips in by_ip_version.items():
                    metrics.append({
                        'metric_name': metric_name('viewers_by_ip_version'),
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'ip_version': ip_version,
                        }
                    })
                
                # Country metrics
                for country, ips in by_country.items():
                    metrics.append({
                        'metric_name': metric_name('viewers_by_country'),
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'country': country,
                        }
                    })
                
                # Region metrics (country_region to avoid conflicts)
                for region_key, ips in by_region.items():
                    country, region = region_key.split('_', 1)
                    if region != "Unknown":  # Skip unknown regions
                        metrics.append({
                            'metric_name': metric_name('viewers_by_region'),
                            'value': float(len(ips)),
                            'timestamp_ms': timestamp_ms,
                            'labels': {
                                'stream': stream_id,
                                'country': country,
                                'region': region,
                            }
                        })
                    else:
                        skipped_unknown_region_series += len(ips)

            if skipped_unknown_region_series > 0:
                logger.info(
                    "Skipped viewers_by_region emission for %d unique sessions with unknown region",
                    skipped_unknown_region_series,
                )
            
            return metrics

    def get_watch_time_metrics(
        self,
        reference_time: Optional[datetime] = None,
        batch_offset_ms: int = 0,
    ) -> List[Dict]:
        """Return aggregate watch-time counters/histogram metrics by stream."""
        with self._lock:
            reference_dt = floor_to_minute(reference_time or datetime.now(timezone.utc))
            self._close_idle_watch_sessions_locked(reference_dt)
            timestamp_ms = int(reference_dt.timestamp() * 1000) + batch_offset_ms
            metrics: List[Dict] = []

            for stream_id, sessions_total in self._watch_sessions_total.items():
                for device_type in sorted(self._watch_sessions_by_device_total.get(stream_id, {}).keys()):
                    metrics.append({
                        'metric_name': metric_name('watch_sessions_total'),
                        'value': float(self._watch_sessions_by_device_total[stream_id].get(device_type, 0)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'device_type': device_type,
                        },
                    })
                metrics.append({
                    'metric_name': metric_name('watch_time_seconds_total'),
                    'value': float(self._watch_time_seconds_total.get(stream_id, 0.0)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {'stream': stream_id},
                })
                for device_type in sorted(self._watch_time_seconds_by_device_total.get(stream_id, {}).keys()):
                    metrics.append({
                        'metric_name': metric_name('watch_time_seconds_by_device_total'),
                        'value': float(self._watch_time_seconds_by_device_total[stream_id].get(device_type, 0.0)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'device_type': device_type,
                        },
                    })
                for device_type in sorted(self._watch_time_seconds_by_device_band_total.get(stream_id, {}).keys()):
                    for band in ('0_1m', '1_5m', '5_10m', '10_20m', '20_60m', '60_plus'):
                        metrics.append({
                            'metric_name': metric_name('watch_time_seconds_by_device_band_total'),
                            'value': float(
                                self._watch_time_seconds_by_device_band_total[stream_id][device_type].get(band, 0.0)
                            ),
                            'timestamp_ms': timestamp_ms,
                            'labels': {
                                'stream': stream_id,
                                'device_type': device_type,
                                'band': band,
                            },
                        })
                metrics.append({
                    'metric_name': metric_name('watch_session_duration_seconds_sum'),
                    'value': float(self._watch_duration_sum.get(stream_id, 0.0)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {'stream': stream_id},
                })
                metrics.append({
                    'metric_name': metric_name('watch_session_duration_seconds_count'),
                    'value': float(self._watch_duration_count.get(stream_id, 0)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {'stream': stream_id},
                })

                for band in ('0_1m', '1_5m', '5_10m', '10_20m', '20_60m', '60_plus'):
                    metrics.append({
                        'metric_name': metric_name('watch_session_duration_band_total'),
                        'value': float(self._watch_duration_band_counts[stream_id].get(band, 0)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {'stream': stream_id, 'band': band},
                    })

                for bucket in self.watch_time_buckets_seconds:
                    metrics.append({
                        'metric_name': metric_name('watch_session_duration_seconds_bucket'),
                        'value': float(self._watch_duration_bucket_counts[stream_id].get(bucket, 0)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {'stream': stream_id, 'le': str(bucket)},
                    })
                metrics.append({
                    'metric_name': metric_name('watch_session_duration_seconds_bucket'),
                    'value': float(self._watch_duration_count.get(stream_id, 0)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {'stream': stream_id, 'le': '+Inf'},
                })

            return metrics

    def get_unique_metrics(
        self,
        window_seconds: int,
        window_label: str,
        reference_time: Optional[datetime] = None,
        batch_offset_ms: int = 0,
    ) -> List[Dict]:
        """Generate unique-viewer gauges constrained to a specific lookback window."""
        with self._lock:
            metrics = []
            reference_dt = floor_to_minute(reference_time or datetime.now(timezone.utc))
            cutoff = reference_dt - timedelta(seconds=window_seconds)
            timestamp_ms = int(reference_dt.timestamp() * 1000) + batch_offset_ms
            skipped_unknown_region_series = 0

            for stream_id, sessions in self.sessions.items():
                if not sessions:
                    continue

                active_sessions = {
                    ip: session
                    for ip, session in sessions.items()
                    if session.last_seen >= cutoff
                }

                if not active_sessions:
                    continue

                metrics.append({
                    'metric_name': metric_name('viewers_unique'),
                    'value': float(len(active_sessions)),
                    'timestamp_ms': timestamp_ms,
                    'labels': {
                        'stream': stream_id,
                        'window': window_label,
                    }
                })

                by_device = defaultdict(set)
                by_ip_version = defaultdict(set)
                by_country = defaultdict(set)
                by_region = defaultdict(set)

                for ip, session in active_sessions.items():
                    by_device[session.device_type].add(ip)
                    by_ip_version[_ip_version_label(ip)].add(ip)
                    by_country[session.country].add(ip)
                    by_region[f"{session.country}_{session.region}"].add(ip)

                for device_type, ips in by_device.items():
                    metrics.append({
                        'metric_name': metric_name('viewers_unique_by_device'),
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'device_type': device_type,
                            'window': window_label,
                        }
                    })

                for ip_version, ips in by_ip_version.items():
                    metrics.append({
                        'metric_name': metric_name('viewers_unique_by_ip_version'),
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'ip_version': ip_version,
                            'window': window_label,
                        }
                    })

                for country, ips in by_country.items():
                    metrics.append({
                        'metric_name': metric_name('viewers_unique_by_country'),
                        'value': float(len(ips)),
                        'timestamp_ms': timestamp_ms,
                        'labels': {
                            'stream': stream_id,
                            'country': country,
                            'window': window_label,
                        }
                    })

                for region_key, ips in by_region.items():
                    country, region = region_key.split('_', 1)
                    if region != "Unknown":
                        metrics.append({
                            'metric_name': metric_name('viewers_unique_by_region'),
                            'value': float(len(ips)),
                            'timestamp_ms': timestamp_ms,
                            'labels': {
                                'stream': stream_id,
                                'country': country,
                                'region': region,
                                'window': window_label,
                            }
                        })
                    else:
                        skipped_unknown_region_series += len(ips)

            if skipped_unknown_region_series > 0:
                logger.info(
                    "Skipped viewers_unique_by_region emission for %d unique sessions with unknown region",
                    skipped_unknown_region_series,
                )

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

    def snapshot_rows(self) -> List[List[Any]]:
        """Return compact snapshot rows for persistence."""
        with self._lock:
            rows: List[List[Any]] = []
            for stream_id, sessions in self.sessions.items():
                for ip_address, session in sessions.items():
                    rows.append([
                        stream_id,
                        ip_address,
                        int(session.first_seen.timestamp()),
                        int(session.last_seen.timestamp()),
                        session.device_type,
                        session.country,
                        session.region,
                        bool(session.watch_session_closed),
                    ])
            return rows

    def persist_to_file(self, file_path: str, reference_time: Optional[datetime] = None) -> bool:
        """Persist session state to compressed JSON snapshot atomically."""
        try:
            reference_dt = reference_time or datetime.now(timezone.utc)
            self.expire_old(reference_dt)
            rows = self.snapshot_rows()

            target = Path(file_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            temp_path = target.with_suffix(f"{target.suffix}.tmp")

            with self._lock:
                watch_payload = {
                    'sessions_total': dict(self._watch_sessions_total),
                    'sessions_by_device_total': {
                        stream_id: dict(device_totals)
                        for stream_id, device_totals in self._watch_sessions_by_device_total.items()
                    },
                    'time_seconds_total': dict(self._watch_time_seconds_total),
                    'time_seconds_by_device_total': {
                        stream_id: dict(device_totals)
                        for stream_id, device_totals in self._watch_time_seconds_by_device_total.items()
                    },
                    'time_seconds_by_device_band_total': {
                        stream_id: {
                            device_type: dict(band_totals)
                            for device_type, band_totals in device_totals.items()
                        }
                        for stream_id, device_totals in self._watch_time_seconds_by_device_band_total.items()
                    },
                    'duration_sum': dict(self._watch_duration_sum),
                    'duration_count': dict(self._watch_duration_count),
                    'duration_band_counts': {
                        stream_id: dict(bands)
                        for stream_id, bands in self._watch_duration_band_counts.items()
                    },
                    'duration_bucket_counts': {
                        stream_id: {str(bucket): count for bucket, count in buckets.items()}
                        for stream_id, buckets in self._watch_duration_bucket_counts.items()
                    },
                }

            payload = {
                'version': 2,
                'saved_at': reference_dt.isoformat(),
                'window_seconds': int(self.window.total_seconds()),
                'session_gap_seconds': self.session_gap_seconds,
                'rows': rows,
                'watch': watch_payload,
            }

            with gzip.open(temp_path, 'wt', encoding='utf-8') as fp:
                json.dump(payload, fp, separators=(',', ':'))

            os.replace(temp_path, target)
            logger.info("Persisted session tracker state: %d sessions -> %s", len(rows), file_path)
            return True
        except Exception as exc:
            logger.error("Failed to persist session tracker state to %s: %s", file_path, exc)
            return False

    def restore_from_file(self, file_path: str, reference_time: Optional[datetime] = None) -> int:
        """Restore session state from compressed JSON snapshot, returning loaded session count."""
        target = Path(file_path)
        if not target.exists():
            logger.info("Session tracker state file not found, starting fresh: %s", file_path)
            return 0

        try:
            with gzip.open(target, 'rt', encoding='utf-8') as fp:
                payload = json.load(fp)

            rows = payload.get('rows', [])
            restored: Dict[str, Dict[str, SessionInfo]] = defaultdict(dict)
            for row in rows:
                if not isinstance(row, list) or len(row) not in (6, 7, 8):
                    continue
                if len(row) == 8:
                    stream_id, ip_address, first_seen_epoch, last_seen_epoch, device_type, country, region, watch_session_closed = row
                    first_seen = datetime.fromtimestamp(int(first_seen_epoch), tz=timezone.utc)
                elif len(row) == 7:
                    stream_id, ip_address, first_seen_epoch, last_seen_epoch, device_type, country, region = row
                    first_seen = datetime.fromtimestamp(int(first_seen_epoch), tz=timezone.utc)
                    watch_session_closed = False
                else:
                    stream_id, ip_address, last_seen_epoch, device_type, country, region = row
                    first_seen = datetime.fromtimestamp(int(last_seen_epoch), tz=timezone.utc)
                    watch_session_closed = False
                last_seen = datetime.fromtimestamp(int(last_seen_epoch), tz=timezone.utc)
                restored[str(stream_id)][str(ip_address)] = SessionInfo(
                    first_seen=first_seen,
                    last_seen=last_seen,
                    device_type=str(device_type),
                    country=str(country),
                    region=str(region),
                    watch_session_closed=bool(watch_session_closed),
                )

            watch_payload = payload.get('watch', {}) if isinstance(payload, dict) else {}
            restored_watch_sessions_total = defaultdict(int, {
                str(k): int(v) for k, v in watch_payload.get('sessions_total', {}).items()
            })
            restored_watch_sessions_by_device_total: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for stream_id, device_totals in watch_payload.get('sessions_by_device_total', {}).items():
                for device_type, total in device_totals.items():
                    restored_watch_sessions_by_device_total[str(stream_id)][str(device_type)] = int(total)
            restored_watch_time_total = defaultdict(float, {
                str(k): float(v) for k, v in watch_payload.get('time_seconds_total', {}).items()
            })
            restored_watch_time_by_device_total: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
            for stream_id, device_totals in watch_payload.get('time_seconds_by_device_total', {}).items():
                for device_type, total in device_totals.items():
                    restored_watch_time_by_device_total[str(stream_id)][str(device_type)] = float(total)
            restored_watch_time_by_device_band_total: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(
                lambda: defaultdict(lambda: defaultdict(float))
            )
            for stream_id, device_totals in watch_payload.get('time_seconds_by_device_band_total', {}).items():
                for device_type, band_totals in device_totals.items():
                    for band, total in band_totals.items():
                        restored_watch_time_by_device_band_total[str(stream_id)][str(device_type)][str(band)] = float(total)
            restored_watch_duration_sum = defaultdict(float, {
                str(k): float(v) for k, v in watch_payload.get('duration_sum', {}).items()
            })
            restored_watch_duration_count = defaultdict(int, {
                str(k): int(v) for k, v in watch_payload.get('duration_count', {}).items()
            })
            restored_watch_bands: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for stream_id, bands in watch_payload.get('duration_band_counts', {}).items():
                for band, count in bands.items():
                    restored_watch_bands[str(stream_id)][str(band)] = int(count)
            restored_watch_buckets: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
            for stream_id, buckets in watch_payload.get('duration_bucket_counts', {}).items():
                for bucket, count in buckets.items():
                    restored_watch_buckets[str(stream_id)][int(bucket)] = int(count)

            with self._lock:
                self.sessions = restored
                self._watch_sessions_total = restored_watch_sessions_total
                self._watch_sessions_by_device_total = restored_watch_sessions_by_device_total
                self._watch_time_seconds_total = restored_watch_time_total
                self._watch_time_seconds_by_device_total = restored_watch_time_by_device_total
                self._watch_time_seconds_by_device_band_total = restored_watch_time_by_device_band_total
                self._watch_duration_sum = restored_watch_duration_sum
                self._watch_duration_count = restored_watch_duration_count
                self._watch_duration_band_counts = restored_watch_bands
                self._watch_duration_bucket_counts = restored_watch_buckets

            self.expire_old(reference_time or datetime.now(timezone.utc))
            loaded_count = self.get_stats()['total_sessions']
            logger.info("Restored session tracker state: %d sessions from %s", loaded_count, file_path)
            return loaded_count
        except Exception as exc:
            logger.error("Failed to restore session tracker state from %s: %s", file_path, exc)
            return 0


def build_file_viewer_metrics(
    events: List[Event],
    timestamp_ms: int,
    geo_lookup: Callable[[str], Tuple[str, str]],
) -> List[Dict]:
    """Build viewer metrics from the current flushed event set (file-specific view)."""
    if not events:
        return []

    by_stream_ips: Dict[str, Set[str]] = defaultdict(set)
    by_stream_device: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    by_stream_ip_version: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    by_stream_country: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    by_stream_track: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    by_stream_region: Dict[Tuple[str, str, str], Set[str]] = defaultdict(set)

    for event in events:
        stream = event.stream_id
        ip = event.client_ip

        by_stream_ips[stream].add(ip)
        by_stream_device[(stream, event.device_type)].add(ip)
        by_stream_ip_version[(stream, _ip_version_label(ip))].add(ip)
        track = extract_resolution_track(event)
        if is_video_track(track):
            by_stream_track[(stream, track)].add(ip)

        country = event.client_country if event.client_country else "XX"
        by_stream_country[(stream, country)].add(ip)

        lookup_country, region = geo_lookup(ip)
        if not country and lookup_country:
            country = lookup_country
        if region and region != "Unknown":
            by_stream_region[(stream, country, region)].add(ip)

    metrics: List[Dict] = []

    for stream, ips in by_stream_ips.items():
        metrics.append({
            'metric': f'{metric_name("viewers")}{{{stream_label_selector(stream)}}}',
            'value': float(len(ips)),
            'timestamp': timestamp_ms,
        })

    for (stream, device_type), ips in by_stream_device.items():
        metrics.append({
            'metric': f'{metric_name("viewers_by_device")}{{{stream_label_selector(stream)},device_type="{device_type}"}}',
            'value': float(len(ips)),
            'timestamp': timestamp_ms,
        })

    for (stream, ip_version), ips in by_stream_ip_version.items():
        metrics.append({
            'metric': f'{metric_name("viewers_by_ip_version")}{{{stream_label_selector(stream)},ip_version="{ip_version}"}}',
            'value': float(len(ips)),
            'timestamp': timestamp_ms,
        })

    for (stream, country), ips in by_stream_country.items():
        metrics.append({
            'metric': f'{metric_name("viewers_by_country")}{{{stream_label_selector(stream)},country="{country}"}}',
            'value': float(len(ips)),
            'timestamp': timestamp_ms,
        })

    for (stream, track), ips in by_stream_track.items():
        metrics.append({
            'metric': f'{metric_name("viewers_by_resolution")}{{{stream_label_selector(stream)},track="{track}"}}',
            'value': float(len(ips)),
            'timestamp': timestamp_ms,
        })

    for (stream, country, region), ips in by_stream_region.items():
        metrics.append({
            'metric': f'{metric_name("viewers_by_region")}{{{stream_label_selector(stream)},country="{country}",region="{region}"}}',
            'value': float(len(ips)),
            'timestamp': timestamp_ms,
        })

    return metrics


# ============================================================================
# DEVICE DETECTION
# ============================================================================

def detect_device_type(user_agent: str) -> str:
    """
    Detect device type from User-Agent string
    
    Returns:
        - ios: iPhone/iPad/tvOS-adjacent iOS app playback
        - android: Android mobile/tablet app playback
        - apple_tv: Apple TV / tvOS
        - roku: Roku devices
        - firestick: Amazon Fire TV
        - web: Desktop/mobile browsers
        - bots: Automated clients/crawlers/bots
        - streamology: Streamology playback/ingest client signatures
        - other: Unknown devices
    """
    if not user_agent:
        _log_unmatched_device(user_agent)
        return "other"

    # Decode percent-encoded user-agents to improve matching (e.g. Virtual%20Railfan)
    normalized_ua = unquote(user_agent).strip()
    ua_lower = normalized_ua.lower()

    # OTT Platform detection
    if "appletv" in ua_lower or "apple tv" in ua_lower or "tvos" in ua_lower:
        return "apple_tv"
    
    if "roku" in ua_lower:
        return "roku"
    
    if "aft" in ua_lower or "amazon" in ua_lower and "fire" in ua_lower:
        return "firestick"

    # Bot / automation fingerprints
    if any(bot in ua_lower for bot in [
        "bot",
        "crawler",
        "spider",
        "scraper",
        "headless",
        "frame capture",
        "yallbot",
    ]):
        return "bots"

    # First-party app names mapped to platform
    if "baron" in ua_lower or "virtualrailfan" in ua_lower or "virtual railfan" in ua_lower:
        if "android" in ua_lower:
            return "android"
        if "ios" in ua_lower or "iphone" in ua_lower or "ipad" in ua_lower:
            return "ios"

    # iOS app and platform fingerprints
    if (
        "iphone" in ua_lower
        or "ipad" in ua_lower
        or "cpu os" in ua_lower
        or "applecoremedia" in ua_lower
    ):
        return "ios"

    # Android app and platform fingerprints
    if (
        "android" in ua_lower
        or "androidxmedia3" in ua_lower
        or "exoplayer" in ua_lower
        or "dalvik" in ua_lower
    ):
        return "android"

    # Streamology playback/ingest signatures
    if (
        ua_lower.startswith("lavf/")
        or "gstreamer" in ua_lower
        or "gst-launch" in ua_lower
        or "libgst" in ua_lower
    ):
        return "streamology"

    # Ambiguous network/client library signatures are treated as other
    if "cfnetwork/" in ua_lower:
        return "other"

    # Network/client library signatures often used by app-embedded web players
    if "darwin/" in ua_lower:
        return "web"
    
    # Web browsers
    if any(browser in ua_lower for browser in ["chrome", "firefox", "safari", "edge", "opera"]):
        return "web"
    
    _log_unmatched_device(normalized_ua)
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


TRACK_RESOLUTION_PATTERN = re.compile(r'^/[a-f0-9]+/tracks-([^/]+)/')


def extract_track_from_path(path: str) -> str:
    """Extract normalized track label from request path, e.g. tracks-v3 -> v3."""
    if not path:
        return "unknown"

    match = TRACK_RESOLUTION_PATTERN.match(path)
    if not match:
        return "unknown"

    return match.group(1).strip().lower() or "unknown"


def extract_resolution_track(event: Event) -> str:
    """Extract track/resolution label from raw request path."""
    path = event.raw_data.get('clientRequestPath', '') if event.raw_data else ''
    return extract_track_from_path(path)


def is_video_track(track: str) -> bool:
    """Return True when track label represents video (v*) and not audio (a*)."""
    if not track:
        return False
    return track.startswith('v')


# Metric definitions
METRIC_DEFINITIONS = [
    MetricDefinition(
        name=metric_name("transfer_bytes_total"),
        value_extractor=lambda e: float(e.response_bytes),
        aggregation=AggregationType.SUM,
        labels={
            "stream": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "pop": extract_location_id,
        },
        help_text="Total bytes transferred per stream"
    ),
    MetricDefinition(
        name=metric_name("time_to_first_byte_ms"),
        value_extractor=lambda e: float(e.time_to_first_byte_ms) if e.time_to_first_byte_ms > 0 else None,
        aggregation=AggregationType.AVG,
        labels={
            "stream": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "pop": extract_location_id,
        },
        help_text="Average time to first byte in milliseconds"
    ),
    MetricDefinition(
        name=metric_name("requests_total"),
        value_extractor=lambda e: 1.0,
        aggregation=AggregationType.SUM,
        labels={
            "stream": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "pop": extract_location_id,
        },
        help_text="Total number of requests"
    ),
    MetricDefinition(
        name=metric_name("responses_total"),
        value_extractor=lambda e: 1.0,
        aggregation=AggregationType.SUM,
        labels={
            "stream": extract_stream_id,
            "cdn_id": extract_cdn_id,
            "cache_status": extract_cache_status,
            "response_status": extract_response_status,
            "pop": extract_location_id,
        },
        help_text="Request count by HTTP response status code"
    ),
    MetricDefinition(
        name=metric_name("users_total"),
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream": extract_stream_id,
        },
        help_text="Count of unique IP addresses per stream"
    ),
    MetricDefinition(
        name=metric_name("users_by_device_total"),
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream": extract_stream_id,
            "device_type": extract_device_type,
        },
        help_text="Count of unique IP addresses per stream by device type"
    ),
    MetricDefinition(
        name=metric_name("users_by_country_total"),
        value_extractor=lambda e: e.client_ip,  # Value is the IP address itself
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream": extract_stream_id,
            "country": extract_client_country,
        },
        help_text="Count of unique IP addresses per stream by country"
    ),
    MetricDefinition(
        name=metric_name("users_by_resolution_total"),
        value_extractor=lambda e: e.client_ip if is_video_track(extract_resolution_track(e)) else None,
        aggregation=AggregationType.UNIQUE_COUNT,
        labels={
            "stream": extract_stream_id,
            "track": extract_resolution_track,
        },
        help_text="Count of unique IP addresses per stream by track/resolution"
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
    
    def __init__(self):
        self.debug_count = 0
        self.success_count = 0
        self.timestamp_cache = {}
    
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
                client_ip=data.get('clientIP', '0.0.0.0'),
                device_type=detect_device_type(user_agent),
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
    
    def __init__(self, metric_definitions: List[MetricDefinition]):
        self.metric_definitions = metric_definitions
    
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
                dt = minute_key_to_datetime(minute_key)
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
            output_labels = add_legacy_stream_name_label(labels)
            for key, val in sorted(output_labels.items()):
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
            output_labels = add_legacy_stream_name_label(labels)
            for key, val in sorted(output_labels.items()):
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
                if '{' in metric_str and metric_str.endswith('}'):
                    brace_idx = metric_str.index('{')
                    metric_name = metric_str[:brace_idx]
                    labels_str = metric_str[brace_idx+1:-1]
                else:
                    metric_name = metric_str
                    labels_str = ""
                
                # Add __name__ label
                label = ts.labels.add()
                label.name = '__name__'
                label.value = metric_name
                
                # Add other labels
                if labels_str:
                    for label_pair in labels_str.split(','):
                        if not label_pair:
                            continue
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
    
    def __init__(
        self,
        exporter: PrometheusExporter,
        write_queue: queue.Queue,
        max_retry_attempts: int = 5,
        retry_base_delay_seconds: float = 1.0,
        retry_max_delay_seconds: float = 30.0,
    ):
        super().__init__(daemon=True, name="PrometheusWriter")
        self.exporter = exporter
        self.write_queue = write_queue
        self.running = True
        self.metrics_written = 0
        self.metrics_failed = 0
        self.max_retry_attempts = max_retry_attempts
        self.retry_base_delay_seconds = retry_base_delay_seconds
        self.retry_max_delay_seconds = retry_max_delay_seconds

    def _push_with_retries(self, metrics: List[Dict], source: str) -> bool:
        """Push metrics with bounded exponential backoff retries for transient failures."""
        log_prefix = f"[{source}] "

        for attempt in range(self.max_retry_attempts + 1):
            if self.exporter.push_metrics(metrics, log_prefix):
                return True

            if attempt >= self.max_retry_attempts:
                break

            delay = min(
                self.retry_base_delay_seconds * (2 ** attempt),
                self.retry_max_delay_seconds,
            )
            logger.warning(
                f"{log_prefix}Push failed, retrying in {delay:.1f}s "
                f"(attempt {attempt + 1}/{self.max_retry_attempts})"
            )
            time.sleep(delay)

        logger.error(
            f"{log_prefix}Exhausted retries after {self.max_retry_attempts + 1} attempts"
        )
        return False
    
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

                if self._push_with_retries(metrics, source):
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


class SessionStatePersistenceThread(threading.Thread):
    """Background thread for periodic session tracker state persistence."""

    def __init__(
        self,
        tracker: SessionTracker,
        state_path: str,
        save_interval_seconds: int = 300,
    ):
        super().__init__(daemon=True, name="SessionStatePersistence")
        self.tracker = tracker
        self.state_path = state_path
        self.save_interval_seconds = max(5, save_interval_seconds)
        self._stop_event = threading.Event()

    def persist_once(self) -> bool:
        """Persist session state one time."""
        return self.tracker.persist_to_file(self.state_path)

    def run(self):
        logger.info(
            "Session state persistence thread started (interval=%ss, path=%s)",
            self.save_interval_seconds,
            self.state_path,
        )
        while not self._stop_event.wait(self.save_interval_seconds):
            self.persist_once()

        logger.info("Session state persistence thread stopped")

    def stop(self):
        """Request thread stop."""
        self._stop_event.set()


class S3Importer:
    """Main S3 log importer - processes events with buffering for overlaps"""
    
    def __init__(self, s3_client: S3Client, prometheus_url: str,
                 prometheus_user: Optional[str] = None, prometheus_password: Optional[str] = None,
                 s3_prefix: str = 'real-time-logs/logs',
                 metric_definitions: List[MetricDefinition] = None,
                 flush_delay_seconds: int = 180):
        self.s3_client = s3_client
        self.s3_prefix = s3_prefix.rstrip('/')  # Remove trailing slash
        self.parser = LogParser()
        self.shutdown_requested = False
        self.aggregator = MetricAggregator(metric_definitions or METRIC_DEFINITIONS)
        self.exporter = PrometheusExporter(prometheus_url, prometheus_user, prometheus_password)
        self.write_queue = queue.Queue(maxsize=100)
        
        # Event buffer for handling file overlaps - flush immediately with batch offsets
        self.event_buffer = EventBuffer()
        self.flush_delay_seconds = flush_delay_seconds  # Still used for determining when to flush
        
        # Track flush count per minute for timestamp offsets (prevents duplicates)
        # Key: minute_key, Value: (flush_count, last_flush_time)
        self.flush_count_per_minute: Dict[str, tuple[int, datetime]] = {}
        
        # Startup offset for first sample placement within the minute (default exact minute)
        self.startup_offset_ms = _env_int('STARTUP_OFFSET_MS', 0)
        if self.startup_offset_ms < 0:
            logger.warning(f"Invalid STARTUP_OFFSET_MS={self.startup_offset_ms}; using 0")
            self.startup_offset_ms = 0
        logger.info(f"Using startup offset: {self.startup_offset_ms}ms")
        
        # Track dropped events (for statistics)
        self.dropped_events = 0

        # Prometheus retry settings
        self.prometheus_retry_attempts = int(os.getenv('PROMETHEUS_RETRY_ATTEMPTS', '5'))
        self.prometheus_retry_base_delay_seconds = float(os.getenv('PROMETHEUS_RETRY_BASE_DELAY_SECONDS', '1.0'))
        self.prometheus_retry_max_delay_seconds = float(os.getenv('PROMETHEUS_RETRY_MAX_DELAY_SECONDS', '30.0'))

        # Rolling-session retention and unique-viewer window
        self.unique_viewers_window_seconds = _env_int('UNIQUE_VIEWERS_WINDOW_SECONDS', 3600)
        if self.unique_viewers_window_seconds <= 0:
            logger.warning(
                f"Invalid UNIQUE_VIEWERS_WINDOW_SECONDS={self.unique_viewers_window_seconds}; using default 3600"
            )
            self.unique_viewers_window_seconds = 3600
        self.unique_viewers_window_label = _duration_label_from_seconds(self.unique_viewers_window_seconds)

        configured_session_window_seconds = _env_int('SESSION_WINDOW_SECONDS', 7200)
        self.session_gap_seconds = _env_int('SESSION_GAP_SECONDS', 120)
        self.viewers_window_seconds = max(configured_session_window_seconds, self.unique_viewers_window_seconds)
        if configured_session_window_seconds < self.unique_viewers_window_seconds:
            logger.warning(
                "SESSION_WINDOW_SECONDS (%s) is less than UNIQUE_VIEWERS_WINDOW_SECONDS (%s); "
                "expanding session retention to %s",
                configured_session_window_seconds,
                self.unique_viewers_window_seconds,
                self.viewers_window_seconds,
            )

        self.geoip_db_path = os.getenv('GEOIP_DB_PATH')
        self.session_tracker = SessionTracker(
            window_seconds=self.viewers_window_seconds,
            geoip_db_path=self.geoip_db_path,
            session_gap_seconds=self.session_gap_seconds,
        )
        self.session_state_path = os.getenv('SESSION_STATE_PATH', '').strip()
        self.session_state_save_interval_seconds = _env_int('SESSION_STATE_SAVE_INTERVAL_SECONDS', 300)
        self.session_state_thread: Optional[SessionStatePersistenceThread] = None

        if self.session_state_path:
            self.session_tracker.restore_from_file(self.session_state_path)
            self.session_state_thread = SessionStatePersistenceThread(
                tracker=self.session_tracker,
                state_path=self.session_state_path,
                save_interval_seconds=self.session_state_save_interval_seconds,
            )
            self.session_state_thread.start()
        else:
            logger.info("SESSION_STATE_PATH not set; session tracker state persistence is disabled")
        
        # Start writer thread
        self.writer_thread = PrometheusWriterThread(
            self.exporter,
            self.write_queue,
            max_retry_attempts=self.prometheus_retry_attempts,
            retry_base_delay_seconds=self.prometheus_retry_base_delay_seconds,
            retry_max_delay_seconds=self.prometheus_retry_max_delay_seconds,
        )
        self.writer_thread.start()
        self.last_flush_time = datetime.now(timezone.utc)
        self.last_processed_event_time: Optional[datetime] = None
    
    def _flush_events(self, events: List[Event], source: str):
        """Aggregate events into metrics and queue for Prometheus with batch offsets"""
        if not events:
            return

        # Keep rolling session state current for unique-window gauges
        self.session_tracker.update(events)
        max_event_time = max(event.timestamp for event in events)
        if self.last_processed_event_time is None or max_event_time > self.last_processed_event_time:
            self.last_processed_event_time = max_event_time
        
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
        
        # Build consolidated flush payload with per-minute offsets
        consolidated_metrics: List[Dict] = []
        file_viewer_metrics: List[Dict] = []
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
                logger.info(f"Built {len(metrics)} metrics for minute {minute_key} ({len(minute_events)} events){offset_str}")
                consolidated_metrics.extend(metrics)

            minute_viewer_metrics = build_file_viewer_metrics(
                minute_events,
                timestamp_ms=minute_timestamp_ms(minute_key_to_datetime(minute_key), batch_offset_ms),
                geo_lookup=self.session_tracker.lookup_geo,
            )
            file_viewer_metrics.extend(minute_viewer_metrics)

        consolidated_metrics.extend(file_viewer_metrics)

        if consolidated_metrics:
            try:
                self.write_queue.put((consolidated_metrics, source), timeout=5.0)
                logger.info(f"Queued consolidated flush payload: {len(consolidated_metrics)} metrics from {source}")
            except queue.Full:
                logger.error(f"Queue full! Dropping {len(consolidated_metrics)} consolidated metrics from {source}")
    
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
        started_at = time.monotonic()
        lines_processed = 0

        def emit_file_processing_metrics(success: bool, processed_lines: int, reference_time: Optional[datetime] = None):
            duration_seconds = time.monotonic() - started_at
            timestamp_ms = minute_timestamp_ms(reference_time or datetime.now(timezone.utc))
            result_label = "success" if success else "failure"
            metrics = [
                {
                    'metric': f'{metric_name("file_process_time_seconds")}{{result="{result_label}"}}',
                    'value': duration_seconds,
                    'timestamp': timestamp_ms,
                },
                {
                    'metric': f'{metric_name("file_lines_processed_total")}{{result="{result_label}"}}',
                    'value': float(processed_lines),
                    'timestamp': timestamp_ms,
                },
            ]
            try:
                self.write_queue.put((metrics, "FileProcessMetrics"), timeout=1.0)
            except queue.Full:
                logger.error("Queue full! Dropping file processing metrics")

        try:
            logger.info(f"Processing: {s3_key}")
            
            # Download file
            compressed_data = self.s3_client.download_object(s3_key)
            if not compressed_data:
                emit_file_processing_metrics(False, lines_processed)
                return False
            
            # Decompress gzip
            try:
                ndjson_content = gzip.decompress(compressed_data).decode('utf-8')
            except Exception as e:
                logger.error(f"Failed to decompress {s3_key}: {e}")
                emit_file_processing_metrics(False, lines_processed)
                return False
            
            # Parse events from file
            events = []
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

            reference_time = max(e.timestamp for e in events) if events else datetime.now(timezone.utc)
            emit_file_processing_metrics(True, lines_processed, reference_time=reference_time)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing {s3_key}: {e}", exc_info=True)
            emit_file_processing_metrics(False, lines_processed)
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

        # Emit configurable unique-window session snapshot (default: 1h)
        unique_reference_time = self.last_processed_event_time or datetime.now(timezone.utc)
        self.session_tracker.expire_old(unique_reference_time)
        unique_gauge_data = self.session_tracker.get_unique_metrics(
            window_seconds=self.unique_viewers_window_seconds,
            window_label=self.unique_viewers_window_label,
            reference_time=unique_reference_time,
            batch_offset_ms=self.startup_offset_ms,
        )
        watch_time_data = self.session_tracker.get_watch_time_metrics(
            reference_time=unique_reference_time,
            batch_offset_ms=self.startup_offset_ms,
        )
        unique_gauge_metrics = self.exporter.build_gauge_metrics(unique_gauge_data + watch_time_data)
        if unique_gauge_metrics:
            try:
                self.write_queue.put((unique_gauge_metrics, "UniqueSessionGauges"), timeout=5.0)
            except queue.Full:
                logger.error(f"Queue full! Dropping {len(unique_gauge_metrics)} unique session gauge metrics")

        # Wait for queue to empty
        logger.info("Waiting for writer thread to finish...")
        self.write_queue.join()
        
        # Report buffer status and writer stats for this poll
        buffered_count = self.event_buffer.size()
        logger.info(f"Poll complete: {total_found} found, {total_processed} processed, "
                   f"{buffered_count} events buffered")
        logger.info(f"Writer stats: {self.writer_thread.metrics_written} written, "
                   f"{self.writer_thread.metrics_failed} failed")

        # Emit unmatched user-agent summary once per poll cycle
        total_undefined_devices = flush_unmatched_device_summary()

        if total_undefined_devices > 0:
            undefined_metric = {
                'metric': f'{metric_name("viewer_undefined_devices_total")}{{}}',
                'value': float(total_undefined_devices),
                'timestamp': minute_timestamp_ms(unique_reference_time),
            }
            try:
                self.write_queue.put(([undefined_metric], "UndefinedDevices"), timeout=1.0)
                self.write_queue.join()
            except queue.Full:
                logger.error("Queue full! Dropping viewer_undefined_devices_total metric")
        
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

        if self.session_state_thread:
            logger.info("Stopping session state persistence thread...")
            self.session_state_thread.stop()
            self.session_state_thread.join(timeout=10)

        if self.session_state_path:
            logger.info("Persisting final session tracker snapshot...")
            self.session_tracker.persist_to_file(self.session_state_path)
        
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
        flush_delay_seconds=args.flush_delay
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
