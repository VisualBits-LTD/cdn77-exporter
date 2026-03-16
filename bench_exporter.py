#!/usr/bin/env python3
"""
Performance benchmark suite for CDN77 Prometheus Exporter.

Benchmarks five isolated processing stages plus end-to-end:
  1. JSON parsing (LogParser.parse_json_to_event)
  2. Device detection (detect_device_type)
  3. Metric aggregation (MetricAggregator)
  4. Session tracking (SessionTracker)
  5. End-to-end pipeline (parse → aggregate → session → export)

Usage:
    python bench_exporter.py                    # default: medium scale
    python bench_exporter.py --scale small      # 10K events
    python bench_exporter.py --scale medium     # 100K events
    python bench_exporter.py --scale large      # 1M events
    python bench_exporter.py --scale xl         # 10M events
    python bench_exporter.py --output results/  # save JSON results
    python bench_exporter.py --only parse,session  # run specific benchmarks
"""

import argparse
import gc
import gzip
import json
import logging
import os
import random
import sys
import time
import tracemalloc

from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Import from the exporter module (triggers basicConfig at import time)
from exporter import (
    Event,
    LogParser,
    MetricAggregator,
    SessionTracker,
    SessionInfo,
    METRIC_DEFINITIONS,
    detect_device_type,
    build_file_viewer_metrics,
    metric_name,
)

# Suppress exporter logging during benchmarks (must be after import)
logging.getLogger().setLevel(logging.ERROR)


# ============================================================================
# SCALE TIERS
# ============================================================================

SCALES = {
    "small":  {"events": 10_000,     "streams": 5,   "unique_ips": 1_000,    "locations": 5},
    "medium": {"events": 100_000,    "streams": 10,  "unique_ips": 10_000,   "locations": 10},
    "large":  {"events": 1_000_000,  "streams": 20,  "unique_ips": 50_000,   "locations": 15},
    "xl":     {"events": 10_000_000, "streams": 50,  "unique_ips": 500_000,  "locations": 20},
}


# ============================================================================
# BENCHMARK RESULT
# ============================================================================

@dataclass
class BenchmarkResult:
    name: str
    scale: str
    event_count: int
    wall_time_s: float
    peak_memory_mb: float
    current_memory_mb: float
    throughput_events_per_s: float
    gc_collections: Dict[str, int] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)

    def summary_line(self) -> str:
        return (
            f"{self.name:<30s} | {self.wall_time_s:>8.3f}s | "
            f"{self.peak_memory_mb:>10.1f} MB peak | "
            f"{self.throughput_events_per_s:>12,.0f} evt/s"
        )


# ============================================================================
# MEASUREMENT CONTEXT MANAGER
# ============================================================================

class Measure:
    """Context manager that captures wall time, memory, and GC stats."""

    def __init__(self):
        self.wall_time_s = 0.0
        self.peak_memory_mb = 0.0
        self.current_memory_mb = 0.0
        self.gc_collections = {}

    def __enter__(self):
        gc.collect()
        gc.collect()
        self._gc_before = {f"gen{i}": gc.get_stats()[i]["collections"] for i in range(3)}
        tracemalloc.start()
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.wall_time_s = time.perf_counter() - self._t0
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.peak_memory_mb = peak / (1024 * 1024)
        self.current_memory_mb = current / (1024 * 1024)
        gc_after = {f"gen{i}": gc.get_stats()[i]["collections"] for i in range(3)}
        self.gc_collections = {k: gc_after[k] - self._gc_before[k] for k in gc_after}
        return False


# ============================================================================
# DATA GENERATORS
# ============================================================================

USER_AGENTS = [
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0", "web"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15", "web"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) Safari/605.1", "ios"),
    ("Roku/DVP-11.0 (11.0.0.4193)", "roku"),
    ("AppleTV11,1/11.1", "appletv"),
    ("Dalvik/2.1.0 (Linux; U; Android 9; AFTMM Build/PS7633)", "firetv"),
    ("Dalvik/2.1.0 (Linux; U; Android 13; SM-S911B)", "android"),
]

UA_WEIGHTS = [30, 10, 20, 15, 10, 10, 5]

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "MX"]
CACHE_STATUSES = ["HIT", "MISS", "EXPIRED"]
CACHE_WEIGHTS = [70, 20, 10]
STATUS_CODES = [200, 206, 304, 404, 500]
STATUS_WEIGHTS = [75, 15, 5, 3, 2]

TRACKS = [
    "tracks-v3/rewind-3600.fmp4.m3u8",
    "tracks-v3/720p.fmp4.m3u8",
    "tracks-v3/1080p.fmp4.m3u8",
    "tracks-v3/480p.fmp4.m3u8",
    "tracks-v3/audio-aac.fmp4.m3u8",
]


def generate_ip_pool(n: int, seed: int = 42) -> List[str]:
    """Pre-generate a pool of unique IP addresses."""
    rng = random.Random(seed)
    ips = set()
    while len(ips) < n:
        ips.add(f"{rng.randint(1,223)}.{rng.randint(0,255)}.{rng.randint(0,255)}.{rng.randint(1,254)}")
    return list(ips)


def generate_stream_ids(n: int, seed: int = 42) -> List[str]:
    """Pre-generate stream IDs (32-char hex)."""
    rng = random.Random(seed)
    return [f"{rng.getrandbits(128):032x}" for _ in range(n)]


def generate_location_ids(n: int) -> List[str]:
    """Generate realistic CDN PoP location IDs."""
    locations = [
        "losangelesUSCA", "newyorkUSNY", "chicagoUSIL", "dallasUSTX",
        "frankfurtDEHE", "londonGBEN", "parisIDF", "amsterdamNLNH",
        "tokyoJPTY", "sydneyAUNSW", "saopauloBRSP", "mumbaiINMH",
        "seoulKRSO", "singaporeSGSG", "torontoCAON", "miamiUSFL",
        "warsawPLMZ", "pragmaCZPR", "madridESMA", "milanoITMI",
    ]
    return locations[:n]


def generate_ndjson_lines(
    num_events: int,
    num_streams: int = 10,
    num_unique_ips: int = 10_000,
    num_locations: int = 10,
    seed: int = 42,
    time_spread_minutes: int = 2,
) -> List[str]:
    """
    Generate NDJSON lines with realistic distributions.
    Returns list of JSON strings (one per event).
    """
    rng = random.Random(seed)
    ip_pool = generate_ip_pool(num_unique_ips, seed=seed)
    stream_ids = generate_stream_ids(num_streams, seed=seed)
    locations = generate_location_ids(num_locations)

    base_time = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    spread_seconds = time_spread_minutes * 60

    # Pre-select random values in bulk for speed
    ips = rng.choices(ip_pool, k=num_events)
    streams = rng.choices(stream_ids, k=num_events)
    locs = rng.choices(locations, k=num_events)
    uas = rng.choices(USER_AGENTS, weights=UA_WEIGHTS, k=num_events)
    cache_stats = rng.choices(CACHE_STATUSES, weights=CACHE_WEIGHTS, k=num_events)
    statuses = rng.choices(STATUS_CODES, weights=STATUS_WEIGHTS, k=num_events)
    countries = rng.choices(COUNTRIES, k=num_events)
    timestamps_offset = [rng.randint(0, spread_seconds) for _ in range(num_events)]
    tracks = rng.choices(TRACKS, k=num_events)

    lines = []
    for i in range(num_events):
        ts = base_time + timedelta(seconds=timestamps_offset[i])
        entry = {
            "timestamp": ts.isoformat().replace('+00:00', 'Z'),
            "startTimeMs": int(ts.timestamp() * 1000),
            "tcpRTTus": rng.randint(10000, 100000),
            "requestTimeMs": rng.randint(100, 500),
            "timeToFirstByteMs": rng.randint(50, 300),
            "resourceID": 1693215245,
            "cacheAge": rng.randint(0, 10),
            "cacheStatus": cache_stats[i],
            "clientCountry": countries[i],
            "clientIP": ips[i],
            "clientPort": rng.randint(10000, 65535),
            "clientASN": rng.randint(100, 10000),
            "clientRequestAccept": "*/*",
            "clientRequestBytes": 65,
            "clientRequestCompleted": True,
            "clientRequestConnTimeout": False,
            "clientRequestContentType": "",
            "clientRequestHost": "1693215245.rsc.cdn77.org",
            "clientRequestMethod": "GET",
            "clientRequestPath": f"/{streams[i]}/{tracks[i]}",
            "clientRequestProtocol": "HTTP20",
            "clientRequestRange": "",
            "clientRequestReferer": "https://example.com/",
            "clientRequestScheme": "HTTPS",
            "clientRequestServerPush": False,
            "clientRequestUserAgent": uas[i][0],
            "clientRequestVia": "",
            "clientRequestXForwardedFor": ips[i],
            "clientSSLCipher": "TLS_AES_256_GCM_SHA384",
            "clientSSLFlags": 1,
            "clientSSLProtocol": "TLS13",
            "locationID": locs[i],
            "serverIP": "143.244.51.39",
            "serverPort": 443,
            "responseStatus": statuses[i],
            "responseBytes": rng.randint(1000, 500000),
            "responseContentType": "application/vnd.apple.mpegurl",
            "responseContentRange": "",
            "responseContentEncoding": "GZIP",
        }
        lines.append(json.dumps(entry, separators=(",", ":")))

    return lines


def generate_ndjson_string(lines: List[str]) -> str:
    """Join pre-generated lines into NDJSON string."""
    return "\n".join(lines)


def generate_gzip_bytes(lines: List[str]) -> bytes:
    """Compress pre-generated lines into gzip bytes."""
    return gzip.compress("\n".join(lines).encode("utf-8"))


def generate_events_direct(
    num_events: int,
    num_streams: int = 10,
    num_unique_ips: int = 10_000,
    num_locations: int = 10,
    seed: int = 42,
    time_spread_minutes: int = 2,
) -> List[Event]:
    """
    Generate Event objects directly (skipping JSON serialization/parsing).
    Used to benchmark aggregation and sessions in isolation.
    """
    rng = random.Random(seed)
    ip_pool = generate_ip_pool(num_unique_ips, seed=seed)
    stream_ids = generate_stream_ids(num_streams, seed=seed)
    locations = generate_location_ids(num_locations)

    base_time = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    spread_seconds = time_spread_minutes * 60

    ips = rng.choices(ip_pool, k=num_events)
    streams = rng.choices(stream_ids, k=num_events)
    locs = rng.choices(locations, k=num_events)
    uas = rng.choices(USER_AGENTS, weights=UA_WEIGHTS, k=num_events)
    cache_stats = rng.choices(CACHE_STATUSES, weights=CACHE_WEIGHTS, k=num_events)
    statuses = rng.choices(STATUS_CODES, weights=STATUS_WEIGHTS, k=num_events)
    countries = rng.choices(COUNTRIES, k=num_events)
    timestamps_offset = [rng.randint(0, spread_seconds) for _ in range(num_events)]

    events = []
    for i in range(num_events):
        ts = base_time + timedelta(seconds=timestamps_offset[i])
        events.append(Event(
            timestamp=ts,
            stream_id=streams[i],
            resource_id=1693215245,
            cache_status=cache_stats[i],
            response_bytes=rng.randint(1000, 500000),
            time_to_first_byte_ms=rng.randint(50, 300),
            tcp_rtt_us=rng.randint(10000, 100000),
            request_time_ms=rng.randint(100, 500),
            response_status=statuses[i],
            client_country=countries[i],
            location_id=locs[i],
            client_ip=ips[i],
            device_type=uas[i][1],
            raw_data={},  # empty to isolate non-parse benchmarks from raw_data cost
        ))

    return events


# ============================================================================
# MEMORY PROFILING HELPERS
# ============================================================================

def deep_sizeof(obj, seen=None) -> int:
    """Recursively measure memory of an object graph."""
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)

    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        for k, v in obj.items():
            size += deep_sizeof(k, seen)
            size += deep_sizeof(v, seen)
    elif isinstance(obj, (list, tuple, set, frozenset)):
        for item in obj:
            size += deep_sizeof(item, seen)
    elif hasattr(obj, '__dict__'):
        size += deep_sizeof(obj.__dict__, seen)
    elif hasattr(obj, '__slots__'):
        for slot in obj.__slots__:
            if hasattr(obj, slot):
                size += deep_sizeof(getattr(obj, slot), seen)

    return size


def measure_event_memory(events: List[Event], sample_size: int = 100) -> Dict[str, float]:
    """Profile memory breakdown of Event objects."""
    sample = events[:sample_size]

    shell_sizes = [sys.getsizeof(e) for e in sample]
    raw_data_sizes = [deep_sizeof(e.raw_data) for e in sample]
    total_sizes = [deep_sizeof(e) for e in sample]

    avg_shell = sum(shell_sizes) / len(shell_sizes)
    avg_raw_data = sum(raw_data_sizes) / len(raw_data_sizes)
    avg_total = sum(total_sizes) / len(total_sizes)

    return {
        "avg_shell_bytes": avg_shell,
        "avg_raw_data_bytes": avg_raw_data,
        "avg_total_bytes": avg_total,
        "projected_total_mb": (avg_total * len(events)) / (1024 * 1024),
        "raw_data_pct": (avg_raw_data / avg_total * 100) if avg_total > 0 else 0,
        "sample_size": sample_size,
    }


def measure_session_memory(tracker: SessionTracker) -> Dict[str, float]:
    """Profile memory of SessionTracker internals."""
    stats = tracker.get_stats()

    # Measure a sample of SessionInfo objects
    sample_sessions = []
    count = 0
    for stream_id, sessions in tracker.sessions.items():
        for ip, session in sessions.items():
            sample_sessions.append((ip, session))
            count += 1
            if count >= 100:
                break
        if count >= 100:
            break

    if sample_sessions:
        session_sizes = [deep_sizeof(s) for _, s in sample_sessions]
        ip_key_sizes = [sys.getsizeof(ip) for ip, _ in sample_sessions]
        avg_session = sum(session_sizes) / len(session_sizes)
        avg_ip_key = sum(ip_key_sizes) / len(ip_key_sizes)
    else:
        avg_session = 0
        avg_ip_key = 0

    total_sessions = stats["total_sessions"]
    geo_cache_size = deep_sizeof(tracker._geo_cache) if tracker._geo_cache else 0

    return {
        "total_streams": stats["streams"],
        "total_sessions": total_sessions,
        "avg_session_info_bytes": avg_session,
        "avg_ip_key_bytes": avg_ip_key,
        "avg_per_ip_bytes": avg_session + avg_ip_key,
        "projected_sessions_mb": ((avg_session + avg_ip_key) * total_sessions) / (1024 * 1024),
        "geo_cache_entries": len(tracker._geo_cache),
        "geo_cache_bytes": geo_cache_size,
    }


# ============================================================================
# BENCHMARKS
# ============================================================================

def bench_parse_ndjson(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """Benchmark LogParser.parse_json_to_event over raw NDJSON."""
    parser = LogParser()

    with Measure() as m:
        events = []
        for line in ndjson_str.splitlines():
            if not line.strip():
                continue
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)

    parsed_count = len(events)

    # Measure event memory breakdown (outside timed section)
    mem_breakdown = measure_event_memory(events, sample_size=min(200, parsed_count))

    return BenchmarkResult(
        name="parse_ndjson",
        scale=scale,
        event_count=parsed_count,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=parsed_count / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "input_lines": expected_count,
            "parsed_events": parsed_count,
            "parse_rate_pct": parsed_count / expected_count * 100 if expected_count else 0,
            "memory_breakdown": mem_breakdown,
        },
    )


def bench_detect_device(num_calls: int, scale: str) -> BenchmarkResult:
    """Benchmark detect_device_type in isolation."""
    rng = random.Random(42)
    agents = [ua for ua, _ in USER_AGENTS]
    test_agents = rng.choices(agents, weights=UA_WEIGHTS, k=num_calls)

    with Measure() as m:
        for ua in test_agents:
            detect_device_type(ua)

    return BenchmarkResult(
        name="detect_device_type",
        scale=scale,
        event_count=num_calls,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=num_calls / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
    )


def bench_aggregate(events: List[Event], scale: str) -> BenchmarkResult:
    """Benchmark MetricAggregator.aggregate_events + compute_final_values."""
    aggregator = MetricAggregator(METRIC_DEFINITIONS)

    with Measure() as m:
        aggregated = aggregator.aggregate_events(events)
        results = aggregator.compute_final_values(aggregated)

    return BenchmarkResult(
        name="aggregate_events",
        scale=scale,
        event_count=len(events),
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=len(events) / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "metric_definitions": len(METRIC_DEFINITIONS),
            "result_series": len(results),
            "effective_iterations": len(events) * len(METRIC_DEFINITIONS),
        },
    )


def bench_session_update(events: List[Event], scale: str) -> BenchmarkResult:
    """Benchmark SessionTracker.update + expire_old + get_gauge_metrics."""
    tracker = SessionTracker(window_seconds=7200, session_gap_seconds=120)

    with Measure() as m:
        tracker.update(events)
        ref_time = max(e.timestamp for e in events)
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

    session_mem = measure_session_memory(tracker)

    return BenchmarkResult(
        name="session_tracking",
        scale=scale,
        event_count=len(events),
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=len(events) / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "gauge_metrics_generated": len(gauge_metrics),
            "session_memory": session_mem,
        },
    )


def bench_viewer_metrics(events: List[Event], scale: str) -> BenchmarkResult:
    """Benchmark build_file_viewer_metrics (per-file viewer gauge generation)."""
    ref_time = max(e.timestamp for e in events)
    timestamp_ms = int(ref_time.timestamp() * 1000)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        metrics = build_file_viewer_metrics(events, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="viewer_metrics",
        scale=scale,
        event_count=len(events),
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=len(events) / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={"metrics_generated": len(metrics)},
    )


def bench_end_to_end(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """
    Benchmark the full pipeline: parse → aggregate → session update → viewer metrics.
    Mocks S3 download and Prometheus push.
    """
    parser = LogParser()
    aggregator = MetricAggregator(METRIC_DEFINITIONS)
    tracker = SessionTracker(window_seconds=7200, session_gap_seconds=120)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        # Parse
        events = []
        for line in ndjson_str.splitlines():
            if not line.strip():
                continue
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)

        # Aggregate
        aggregated = aggregator.aggregate_events(events)
        results = aggregator.compute_final_values(aggregated)

        # Session tracking
        tracker.update(events)
        ref_time = max(e.timestamp for e in events) if events else datetime.now(timezone.utc)
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

        # Viewer metrics
        timestamp_ms = int(ref_time.timestamp() * 1000)
        viewer_metrics = build_file_viewer_metrics(events, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="end_to_end",
        scale=scale,
        event_count=len(events),
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=len(events) / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "parsed_events": len(events),
            "aggregated_series": len(results),
            "gauge_metrics": len(gauge_metrics),
            "viewer_metrics": len(viewer_metrics),
        },
    )


def bench_event_memory(scale_name: str, scale_params: Dict) -> BenchmarkResult:
    """
    Benchmark memory footprint of Event objects with and without raw_data.
    Parses a small sample from real NDJSON to get realistic raw_data sizes.
    """
    sample_size = min(1000, scale_params["events"])
    lines = generate_ndjson_lines(
        num_events=sample_size,
        num_streams=scale_params["streams"],
        num_unique_ips=min(sample_size, scale_params["unique_ips"]),
        num_locations=scale_params["locations"],
    )

    parser = LogParser()
    events_with_raw = []
    for line in lines:
        event = parser.parse_json_to_event(line)
        if event:
            events_with_raw.append(event)

    # Measure with raw_data
    with_raw = measure_event_memory(events_with_raw, sample_size=len(events_with_raw))

    # Measure without raw_data (simulate dropping it)
    for e in events_with_raw:
        e.raw_data = {}
    without_raw = measure_event_memory(events_with_raw, sample_size=len(events_with_raw))

    total_events = scale_params["events"]

    return BenchmarkResult(
        name="event_memory_profile",
        scale=scale_name,
        event_count=len(events_with_raw),
        wall_time_s=0,
        peak_memory_mb=0,
        current_memory_mb=0,
        throughput_events_per_s=0,
        extra={
            "with_raw_data": with_raw,
            "without_raw_data": without_raw,
            "savings_per_event_bytes": with_raw["avg_total_bytes"] - without_raw["avg_total_bytes"],
            "projected_savings_at_scale_mb": (
                (with_raw["avg_total_bytes"] - without_raw["avg_total_bytes"])
                * total_events / (1024 * 1024)
            ),
            "projected_with_raw_mb": with_raw["avg_total_bytes"] * total_events / (1024 * 1024),
            "projected_without_raw_mb": without_raw["avg_total_bytes"] * total_events / (1024 * 1024),
        },
    )


def bench_gzip_decompress(lines: List[str], scale: str) -> BenchmarkResult:
    """Benchmark gzip decompression: full load vs streaming."""
    compressed = generate_gzip_bytes(lines)
    num_events = len(lines)
    compressed_mb = len(compressed) / (1024 * 1024)

    # Method 1: current approach — decompress all at once
    with Measure() as m1:
        content = gzip.decompress(compressed).decode("utf-8")
        line_count = sum(1 for line in content.splitlines() if line.strip())

    result_full = {
        "wall_time_s": m1.wall_time_s,
        "peak_memory_mb": m1.peak_memory_mb,
    }

    # Method 2: streaming decompression
    import io
    with Measure() as m2:
        stream = io.BytesIO(compressed)
        line_count_stream = 0
        with gzip.open(stream, "rt", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    line_count_stream += 1

    result_streaming = {
        "wall_time_s": m2.wall_time_s,
        "peak_memory_mb": m2.peak_memory_mb,
    }

    return BenchmarkResult(
        name="gzip_decompress",
        scale=scale,
        event_count=num_events,
        wall_time_s=m1.wall_time_s,
        peak_memory_mb=m1.peak_memory_mb,
        current_memory_mb=m1.current_memory_mb,
        throughput_events_per_s=num_events / m1.wall_time_s if m1.wall_time_s > 0 else 0,
        gc_collections=m1.gc_collections,
        extra={
            "compressed_mb": compressed_mb,
            "full_load": result_full,
            "streaming": result_streaming,
            "memory_savings_pct": (
                (1 - result_streaming["peak_memory_mb"] / result_full["peak_memory_mb"]) * 100
                if result_full["peak_memory_mb"] > 0 else 0
            ),
            "time_diff_pct": (
                (result_streaming["wall_time_s"] / result_full["wall_time_s"] - 1) * 100
                if result_full["wall_time_s"] > 0 else 0
            ),
        },
    )


# ============================================================================
# RUNNER
# ============================================================================

ALL_BENCHMARKS = [
    "parse", "device", "aggregate", "session", "viewer", "end_to_end",
    "event_memory", "gzip",
]


def run_benchmarks(
    scale_name: str,
    only: Optional[List[str]] = None,
) -> List[BenchmarkResult]:
    """Run all (or selected) benchmarks at the given scale."""
    params = SCALES[scale_name]
    benchmarks_to_run = only or ALL_BENCHMARKS
    results = []

    num_events = params["events"]
    num_streams = params["streams"]
    num_unique_ips = params["unique_ips"]
    num_locations = params["locations"]

    print(f"\n{'='*72}")
    print(f"Scale: {scale_name.upper()} — {num_events:,} events, "
          f"{num_streams} streams, {num_unique_ips:,} unique IPs, "
          f"{num_locations} locations")
    print(f"{'='*72}")

    # Generate data (outside timed sections)
    ndjson_lines = None
    ndjson_str = None
    events_direct = None

    needs_ndjson = any(b in benchmarks_to_run for b in ["parse", "end_to_end", "gzip"])
    needs_events = any(b in benchmarks_to_run for b in ["aggregate", "session", "viewer"])

    if needs_ndjson:
        print(f"\nGenerating {num_events:,} NDJSON lines...")
        t0 = time.perf_counter()
        ndjson_lines = generate_ndjson_lines(
            num_events=num_events,
            num_streams=num_streams,
            num_unique_ips=num_unique_ips,
            num_locations=num_locations,
        )
        gen_time = time.perf_counter() - t0
        print(f"  Generated in {gen_time:.1f}s")

        print("  Joining into NDJSON string...")
        t0 = time.perf_counter()
        ndjson_str = generate_ndjson_string(ndjson_lines)
        join_time = time.perf_counter() - t0
        ndjson_mb = len(ndjson_str) / (1024 * 1024)
        print(f"  {ndjson_mb:.1f} MB NDJSON in {join_time:.1f}s")

    if needs_events:
        print(f"\nGenerating {num_events:,} Event objects directly...")
        t0 = time.perf_counter()
        events_direct = generate_events_direct(
            num_events=num_events,
            num_streams=num_streams,
            num_unique_ips=num_unique_ips,
            num_locations=num_locations,
        )
        gen_time = time.perf_counter() - t0
        print(f"  Generated in {gen_time:.1f}s")

    # Run benchmarks
    print(f"\n{'─'*72}")
    print(f"{'Benchmark':<30s} | {'Time':>8s} | {'Peak Memory':>14s} | {'Throughput':>14s}")
    print(f"{'─'*72}")

    if "parse" in benchmarks_to_run and ndjson_str is not None:
        r = bench_parse_ndjson(ndjson_str, num_events, scale_name)
        print(r.summary_line())
        results.append(r)

    if "device" in benchmarks_to_run:
        r = bench_detect_device(num_events, scale_name)
        print(r.summary_line())
        results.append(r)

    if "aggregate" in benchmarks_to_run and events_direct is not None:
        r = bench_aggregate(events_direct, scale_name)
        print(r.summary_line())
        results.append(r)

    if "session" in benchmarks_to_run and events_direct is not None:
        r = bench_session_update(events_direct, scale_name)
        print(r.summary_line())
        results.append(r)

    if "viewer" in benchmarks_to_run and events_direct is not None:
        r = bench_viewer_metrics(events_direct, scale_name)
        print(r.summary_line())
        results.append(r)

    if "end_to_end" in benchmarks_to_run and ndjson_str is not None:
        r = bench_end_to_end(ndjson_str, num_events, scale_name)
        print(r.summary_line())
        results.append(r)

    if "event_memory" in benchmarks_to_run:
        r = bench_event_memory(scale_name, params)
        results.append(r)

    if "gzip" in benchmarks_to_run and ndjson_lines is not None:
        r = bench_gzip_decompress(ndjson_lines, scale_name)
        print(r.summary_line())
        results.append(r)

    print(f"{'─'*72}")

    # Print memory details
    for r in results:
        if r.name == "event_memory_profile":
            extra = r.extra
            print(f"\nEvent Memory Profile (sampled {r.event_count} events, projected to {num_events:,}):")
            print(f"  With raw_data:    {extra['with_raw_data']['avg_total_bytes']:.0f} bytes/event "
                  f"→ {extra['projected_with_raw_mb']:.0f} MB total")
            print(f"  Without raw_data: {extra['without_raw_data']['avg_total_bytes']:.0f} bytes/event "
                  f"→ {extra['projected_without_raw_mb']:.0f} MB total")
            print(f"  raw_data share:   {extra['with_raw_data']['raw_data_pct']:.1f}% of per-event memory")
            print(f"  Savings:          {extra['savings_per_event_bytes']:.0f} bytes/event "
                  f"→ {extra['projected_savings_at_scale_mb']:.0f} MB at scale")

        if r.name == "session_tracking" and "session_memory" in r.extra:
            sm = r.extra["session_memory"]
            print(f"\nSession Memory Profile:")
            print(f"  Streams:          {sm['total_streams']}")
            print(f"  Total sessions:   {sm['total_sessions']:,}")
            print(f"  Per-IP cost:      {sm['avg_per_ip_bytes']:.0f} bytes "
                  f"(SessionInfo: {sm['avg_session_info_bytes']:.0f} + key: {sm['avg_ip_key_bytes']:.0f})")
            print(f"  Projected:        {sm['projected_sessions_mb']:.1f} MB")
            print(f"  GeoIP cache:      {sm['geo_cache_entries']} entries, "
                  f"{sm['geo_cache_bytes'] / 1024:.1f} KB")

        if r.name == "gzip_decompress":
            extra = r.extra
            print(f"\nGzip Decompression Comparison:")
            print(f"  Compressed size:  {extra['compressed_mb']:.1f} MB")
            print(f"  Full load:        {extra['full_load']['wall_time_s']:.3f}s, "
                  f"{extra['full_load']['peak_memory_mb']:.1f} MB peak")
            print(f"  Streaming:        {extra['streaming']['wall_time_s']:.3f}s, "
                  f"{extra['streaming']['peak_memory_mb']:.1f} MB peak")
            print(f"  Memory savings:   {extra['memory_savings_pct']:.1f}%")

    return results


def save_results(results: List[BenchmarkResult], output_dir: str):
    """Save benchmark results as JSON."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Build output document
    import subprocess
    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], text=True
        ).strip()
    except Exception:
        git_sha = "unknown"

    doc = {
        "run_id": datetime.now(timezone.utc).isoformat(),
        "git_sha": git_sha,
        "python_version": sys.version,
        "scale": results[0].scale if results else "unknown",
        "benchmarks": {r.name: asdict(r) for r in results},
    }

    filename = f"bench_{results[0].scale}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = output_path / filename

    with open(filepath, "w") as f:
        json.dump(doc, f, indent=2, default=str)

    print(f"\nResults saved to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="CDN77 Exporter Performance Benchmarks")
    parser.add_argument(
        "--scale", choices=list(SCALES.keys()), default="medium",
        help="Scale tier (default: medium)",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Directory to save JSON results (e.g., bench_results/)",
    )
    parser.add_argument(
        "--only", type=str, default=None,
        help=f"Comma-separated list of benchmarks to run: {','.join(ALL_BENCHMARKS)}",
    )

    args = parser.parse_args()

    only = args.only.split(",") if args.only else None
    if only:
        invalid = [b for b in only if b not in ALL_BENCHMARKS]
        if invalid:
            print(f"Unknown benchmarks: {invalid}")
            print(f"Available: {ALL_BENCHMARKS}")
            sys.exit(1)

    print("CDN77 Exporter Performance Benchmarks")
    print(f"Python {sys.version}")
    print(f"Scale: {args.scale}")

    results = run_benchmarks(args.scale, only=only)

    if args.output:
        save_results(results, args.output)


if __name__ == "__main__":
    main()
