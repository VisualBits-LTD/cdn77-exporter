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
import polars as pl
from exporter_polars import (
    parse_ndjson_to_dataframe,
    parse_ndjson_native,
    dataframe_from_events,
    dataframe_from_events_direct,
    PolarsMetricAggregator,
    PolarsSessionTracker,
    build_file_viewer_metrics_polars,
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
        if self.peak_memory_mb > 0:
            mem_str = f"{self.peak_memory_mb:>10.1f} MB peak"
        else:
            mem_str = f"{'—':>14s}"
        return (
            f"{self.name:<35s} | {self.wall_time_s:>8.3f}s | "
            f"{mem_str} | "
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
            request_path=f"/{streams[i]}/tracks-v3/720p.fmp4.m3u8",
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
    total_sizes = [deep_sizeof(e) for e in sample]

    avg_shell = sum(shell_sizes) / len(shell_sizes)
    avg_total = sum(total_sizes) / len(total_sizes)

    return {
        "avg_shell_bytes": avg_shell,
        "avg_total_bytes": avg_total,
        "projected_total_mb": (avg_total * len(events)) / (1024 * 1024),
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
        name="parse [python]",
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
        name="aggregate [python]",
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
        name="session [python]",
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
        name="viewer_metrics [python]",
        scale=scale,
        event_count=len(events),
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=len(events) / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={"metrics_generated": len(metrics)},
    )


def bench_end_to_end_baseline(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """
    Benchmark the original unmodified pipeline (pre-optimization baseline).
    Uses exporter_baseline.py extracted from the main branch.
    """
    import exporter_baseline as baseline_mod

    parser = baseline_mod.LogParser()
    aggregator = baseline_mod.MetricAggregator(baseline_mod.METRIC_DEFINITIONS)
    tracker = baseline_mod.SessionTracker(window_seconds=7200, session_gap_seconds=120)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        events = []
        for line in ndjson_str.splitlines():
            if not line.strip():
                continue
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)

        aggregated = aggregator.aggregate_events(events)
        results = aggregator.compute_final_values(aggregated)

        tracker.update(events)
        ref_time = max(e.timestamp for e in events) if events else datetime.now(timezone.utc)
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

        timestamp_ms = int(ref_time.timestamp() * 1000)
        viewer_metrics = baseline_mod.build_file_viewer_metrics(events, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="end_to_end [baseline]",
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
        name="end_to_end [improved]",
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
    """Benchmark memory footprint of Event objects."""
    sample_size = min(1000, scale_params["events"])
    lines = generate_ndjson_lines(
        num_events=sample_size,
        num_streams=scale_params["streams"],
        num_unique_ips=min(sample_size, scale_params["unique_ips"]),
        num_locations=scale_params["locations"],
    )

    parser = LogParser()
    events = []
    for line in lines:
        event = parser.parse_json_to_event(line)
        if event:
            events.append(event)

    mem = measure_event_memory(events, sample_size=len(events))
    total_events = scale_params["events"]

    return BenchmarkResult(
        name="event_memory_profile",
        scale=scale_name,
        event_count=len(events),
        wall_time_s=0,
        peak_memory_mb=0,
        current_memory_mb=0,
        throughput_events_per_s=0,
        extra={
            "per_event": mem,
            "projected_total_mb": mem["avg_total_bytes"] * total_events / (1024 * 1024),
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
# VALIDATION
# ============================================================================


def validate_aggregation_results(
    events: List[Event],
    df: pl.DataFrame,
) -> Dict[str, Any]:
    """
    Run both aggregation implementations on the same data and compare outputs.
    Returns a dict with match status and any discrepancies.
    """
    # Original path
    aggregator = MetricAggregator(METRIC_DEFINITIONS)
    aggregated = aggregator.aggregate_events(events)
    original_results = aggregator.compute_final_values(aggregated)

    # Polars path
    polars_agg = PolarsMetricAggregator()
    polars_results = polars_agg.aggregate(df)

    # Normalize both into comparable form: {(metric_name, frozenset(labels)): value}
    def normalize(results_list):
        normalized = {}
        for r in results_list:
            key = (r['metric_name'], frozenset(r['labels'].items()))
            normalized[key] = r['value']
        return normalized

    orig_norm = normalize(original_results)
    polars_norm = normalize(polars_results)

    orig_keys = set(orig_norm.keys())
    polars_keys = set(polars_norm.keys())

    missing_in_polars = orig_keys - polars_keys
    extra_in_polars = polars_keys - orig_keys
    common_keys = orig_keys & polars_keys

    value_mismatches = []
    for key in common_keys:
        orig_val = orig_norm[key]
        polars_val = polars_norm[key]
        if abs(orig_val - polars_val) > 0.01:
            value_mismatches.append({
                'metric': key[0],
                'labels': dict(key[1]),
                'original': orig_val,
                'polars': polars_val,
                'diff': abs(orig_val - polars_val),
            })

    match = len(missing_in_polars) == 0 and len(extra_in_polars) == 0 and len(value_mismatches) == 0

    return {
        'match': match,
        'original_series': len(original_results),
        'polars_series': len(polars_results),
        'common': len(common_keys),
        'missing_in_polars': len(missing_in_polars),
        'extra_in_polars': len(extra_in_polars),
        'value_mismatches': len(value_mismatches),
        'mismatch_details': value_mismatches[:10],  # first 10
        'missing_details': [
            {'metric': k[0], 'labels': dict(k[1])} for k in list(missing_in_polars)[:5]
        ],
        'extra_details': [
            {'metric': k[0], 'labels': dict(k[1])} for k in list(extra_in_polars)[:5]
        ],
    }


# ============================================================================
# POLARS BENCHMARKS
# ============================================================================


def bench_parse_ndjson_polars(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """Benchmark parse_ndjson_to_dataframe (Polars path)."""
    with Measure() as m:
        df = parse_ndjson_to_dataframe(ndjson_str)

    return BenchmarkResult(
        name="parse [polars]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={"dataframe_columns": df.width, "input_lines": expected_count},
    )


def bench_aggregate_polars(df: pl.DataFrame, scale: str) -> BenchmarkResult:
    """Benchmark PolarsMetricAggregator.aggregate."""
    agg = PolarsMetricAggregator()

    with Measure() as m:
        results = agg.aggregate(df)

    return BenchmarkResult(
        name="aggregate [polars]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={"result_series": len(results)},
    )


def bench_session_polars(df: pl.DataFrame, scale: str) -> BenchmarkResult:
    """Benchmark PolarsSessionTracker.update_from_df."""
    tracker = PolarsSessionTracker(window_seconds=7200, session_gap_seconds=120)

    with Measure() as m:
        tracker.update_from_df(df)
        ref_time = df['timestamp'].max()
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

    session_mem = measure_session_memory(tracker)

    return BenchmarkResult(
        name="session [polars]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "gauge_metrics_generated": len(gauge_metrics),
            "session_memory": session_mem,
        },
    )


def bench_viewer_metrics_polars(df: pl.DataFrame, scale: str) -> BenchmarkResult:
    """Benchmark build_file_viewer_metrics_polars."""
    ref_time = df['timestamp'].max()
    timestamp_ms = int(ref_time.timestamp() * 1000)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        metrics = build_file_viewer_metrics_polars(df, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="viewer_metrics [polars]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={"metrics_generated": len(metrics)},
    )


def bench_end_to_end_polars(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """Benchmark full Polars pipeline: parse → aggregate → session → viewer."""
    agg = PolarsMetricAggregator()
    tracker = PolarsSessionTracker(window_seconds=7200, session_gap_seconds=120)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        df = parse_ndjson_to_dataframe(ndjson_str)

        results = agg.aggregate(df)

        tracker.update_from_df(df)
        ref_time = df['timestamp'].max()
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

        timestamp_ms = int(ref_time.timestamp() * 1000)
        viewer_metrics = build_file_viewer_metrics_polars(df, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="end_to_end [polars]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "parsed_events": df.height,
            "aggregated_series": len(results),
            "gauge_metrics": len(gauge_metrics),
            "viewer_metrics": len(viewer_metrics),
        },
    )


def bench_end_to_end_hybrid(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """
    Hybrid pipeline using exporter_hybrid module.
    Original parse + sessions, Polars aggregate + viewer metrics.
    """
    from exporter_hybrid import HybridAggregator as _HybridAggregator

    parser = LogParser()
    hybrid_agg = _HybridAggregator()
    tracker = SessionTracker(window_seconds=7200, session_gap_seconds=120)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        # 1. Original parse
        events = []
        for line in ndjson_str.splitlines():
            if not line.strip():
                continue
            event = parser.parse_json_to_event(line)
            if event:
                events.append(event)

        # 2. Hybrid aggregate (Events → DataFrame → Polars group_by)
        agg_results, df = hybrid_agg.aggregate_and_compute(events)  # Events → DataFrame → Polars group_by

        # 3. Original session tracking
        tracker.update(events)
        ref_time = max(e.timestamp for e in events) if events else datetime.now(timezone.utc)
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

        # 4. Polars viewer metrics
        timestamp_ms = int(ref_time.timestamp() * 1000)
        viewer_metrics = build_file_viewer_metrics_polars(df, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="end_to_end [hybrid]",
        scale=scale,
        event_count=len(events),
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=len(events) / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "parsed_events": len(events),
            "aggregated_series": len(agg_results),
            "gauge_metrics": len(gauge_metrics),
            "viewer_metrics": len(viewer_metrics),
        },
    )


def bench_parse_ndjson_native(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """Benchmark parse_ndjson_native (Polars native NDJSON reader, no Python loop)."""
    ndjson_bytes = ndjson_str.encode('utf-8')

    with Measure() as m:
        df = parse_ndjson_native(ndjson_bytes)

    return BenchmarkResult(
        name="parse [native]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={"dataframe_columns": df.width, "input_lines": expected_count},
    )


def bench_end_to_end_native(ndjson_str: str, expected_count: int, scale: str) -> BenchmarkResult:
    """
    Full native Polars pipeline: native NDJSON parse → Polars aggregate → Polars viewer.
    Session tracking uses PolarsSessionTracker (iter_rows from DataFrame).
    """
    ndjson_bytes = ndjson_str.encode('utf-8')
    polars_agg = PolarsMetricAggregator()
    tracker = PolarsSessionTracker(window_seconds=7200, session_gap_seconds=120)

    def noop_geo(ip: str):
        return ("XX", "Unknown")

    with Measure() as m:
        df = parse_ndjson_native(ndjson_bytes)

        agg_results = polars_agg.aggregate(df)

        tracker.update_from_df(df)
        ref_time = df['timestamp'].max()
        tracker.expire_old(ref_time)
        gauge_metrics = tracker.get_gauge_metrics(reference_time=ref_time)

        timestamp_ms = int(ref_time.timestamp() * 1000)
        viewer_metrics = build_file_viewer_metrics_polars(df, timestamp_ms, noop_geo)

    return BenchmarkResult(
        name="end_to_end [native]",
        scale=scale,
        event_count=df.height,
        wall_time_s=m.wall_time_s,
        peak_memory_mb=m.peak_memory_mb,
        current_memory_mb=m.current_memory_mb,
        throughput_events_per_s=df.height / m.wall_time_s if m.wall_time_s > 0 else 0,
        gc_collections=m.gc_collections,
        extra={
            "parsed_events": df.height,
            "aggregated_series": len(agg_results),
            "gauge_metrics": len(gauge_metrics),
            "viewer_metrics": len(viewer_metrics),
        },
    )


def _bench_rust_binary(ndjson_str: str, scale: str, label: str, bin_path: str) -> List[BenchmarkResult]:
    """Run a Rust binary benchmark via subprocess.

    Returns a list of BenchmarkResults: one end_to_end plus one per stage.
    """
    import subprocess
    import tempfile

    name = f"end_to_end [{label}]"

    if not os.path.exists(bin_path):
        return [BenchmarkResult(
            name=name, scale=scale, event_count=0,
            wall_time_s=0, peak_memory_mb=0, current_memory_mb=0,
            throughput_events_per_s=0,
            extra={"error": f"Binary not found: {bin_path}"},
        )]

    with tempfile.NamedTemporaryFile(suffix=".ndjson", mode="w", delete=False) as f:
        f.write(ndjson_str)
        tmp_path = f.name

    try:
        with Measure() as m:
            result = subprocess.run(
                [bin_path, tmp_path],
                capture_output=True, text=True, timeout=600,
            )

        if result.returncode != 0:
            return [BenchmarkResult(
                name=name, scale=scale, event_count=0,
                wall_time_s=m.wall_time_s, peak_memory_mb=m.peak_memory_mb,
                current_memory_mb=0, throughput_events_per_s=0,
                extra={"error": result.stderr[:500]},
            )]

        output = json.loads(result.stdout)
        stages = output["stages"]
        event_count = stages.get("parse", {}).get("event_count", 0)
        rust_total = output["total_wall_time_s"]

        all_results = []

        # Per-stage results
        stage_name_map = {
            "parse": "parse",
            "aggregate": "aggregate",
            "session": "session",
            "viewer": "viewer_metrics",
        }
        for stage_key, display_name in stage_name_map.items():
            if stage_key in stages:
                stage = stages[stage_key]
                stage_time = stage["wall_time_s"]
                all_results.append(BenchmarkResult(
                    name=f"{display_name} [{label}]",
                    scale=scale,
                    event_count=event_count,
                    wall_time_s=stage_time,
                    peak_memory_mb=0,
                    current_memory_mb=0,
                    throughput_events_per_s=event_count / stage_time if stage_time > 0 else 0,
                    extra={k: v for k, v in stage.items() if k != "wall_time_s"},
                ))

        # End-to-end result
        all_results.append(BenchmarkResult(
            name=name,
            scale=scale,
            event_count=event_count,
            wall_time_s=rust_total,
            peak_memory_mb=m.peak_memory_mb,
            current_memory_mb=0,
            throughput_events_per_s=event_count / rust_total if rust_total > 0 else 0,
            extra={
                "rust_stages": {k: v["wall_time_s"] for k, v in stages.items()},
                "aggregated_series": stages.get("aggregate", {}).get("result_series", 0),
                "viewer_metrics": stages.get("viewer", {}).get("viewer_metrics", 0),
                "gauge_metrics": stages.get("session", {}).get("gauge_metrics", 0),
                "rust_aggregation_results": output.get("aggregation_results", []),
            },
        ))

        return all_results
    finally:
        os.unlink(tmp_path)


def bench_end_to_end_cdn_exporter(ndjson_str: str, expected_count: int, scale: str) -> List[BenchmarkResult]:
    """Benchmark Rust (simd-json + ahash) pipeline."""
    bin_path = os.path.join(os.path.dirname(__file__), "cdn-exporter", "target", "release", "cdn-exporter")
    return _bench_rust_binary(ndjson_str, scale, "cdn-exporter", bin_path)


def bench_end_to_end_cdn_exporter_polars(ndjson_str: str, expected_count: int, scale: str) -> List[BenchmarkResult]:
    """Benchmark Rust (Polars) pipeline."""
    bin_path = os.path.join(os.path.dirname(__file__), "cdn-exporter-polars", "target", "release", "cdn-exporter-polars")
    return _bench_rust_binary(ndjson_str, scale, "cdn-exporter-polars", bin_path)


def validate_rust_results(
    python_results: List[Dict],
    rust_output: Dict,
) -> Dict[str, Any]:
    """Compare Rust aggregation results against Python original."""
    rust_results = rust_output.get("rust_aggregation_results", [])
    if not rust_results:
        return {"match": False, "error": "No Rust aggregation results found"}

    def normalize(results_list):
        normalized = {}
        for r in results_list:
            labels = r.get("labels", {})
            key = (r["metric_name"], frozenset(labels.items()))
            normalized[key] = r["value"]
        return normalized

    orig_norm = normalize(python_results)
    rust_norm = normalize(rust_results)

    orig_keys = set(orig_norm.keys())
    rust_keys = set(rust_norm.keys())

    missing = orig_keys - rust_keys
    extra = rust_keys - orig_keys
    common = orig_keys & rust_keys

    mismatches = []
    for key in common:
        if abs(orig_norm[key] - rust_norm[key]) > 0.01:
            mismatches.append({
                "metric": key[0],
                "labels": dict(key[1]),
                "original": orig_norm[key],
                "rust": rust_norm[key],
            })

    return {
        "match": len(missing) == 0 and len(extra) == 0 and len(mismatches) == 0,
        "original_series": len(orig_norm),
        "rust_series": len(rust_norm),
        "common": len(common),
        "missing_in_rust": len(missing),
        "extra_in_rust": len(extra),
        "value_mismatches": len(mismatches),
        "mismatch_details": mismatches[:5],
        "missing_details": [{"metric": k[0], "labels": dict(k[1])} for k in list(missing)[:5]],
        "extra_details": [{"metric": k[0], "labels": dict(k[1])} for k in list(extra)[:5]],
    }


# ============================================================================
# RUNNER
# ============================================================================

ALL_BENCHMARKS = [
    "parse", "device", "aggregate", "session", "viewer",
    "end_to_end_baseline", "end_to_end",
    "event_memory", "gzip",
    "parse_polars", "aggregate_polars", "session_polars",
    "viewer_polars", "end_to_end_polars",
    "end_to_end_hybrid",
    "parse_native", "end_to_end_native",
    "end_to_end_cdn_exporter", "end_to_end_cdn_exporter_polars",
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
    df_direct = None

    # Generate NDJSON as the single source of truth for all benchmarks
    needs_ndjson = any(b in benchmarks_to_run for b in [
        "parse", "end_to_end", "end_to_end_baseline", "gzip",
        "parse_polars", "end_to_end_polars",
        "end_to_end_hybrid",
        "parse_native", "end_to_end_native",
        "end_to_end_cdn_exporter", "end_to_end_cdn_exporter_polars",
    ])
    needs_parsed = any(b in benchmarks_to_run for b in [
        "aggregate", "session", "viewer",
        "aggregate_polars", "session_polars", "viewer_polars",
    ])

    if needs_ndjson or needs_parsed:
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

    # Parse NDJSON into both Event list and DataFrame from the SAME source
    if needs_parsed and ndjson_str is not None:
        print(f"\n  Parsing NDJSON → Event objects...")
        t0 = time.perf_counter()
        parser = LogParser()
        events_direct = []
        for line in ndjson_str.splitlines():
            if not line.strip():
                continue
            event = parser.parse_json_to_event(line)
            if event:
                events_direct.append(event)
        gen_time = time.perf_counter() - t0
        print(f"  {len(events_direct):,} events in {gen_time:.1f}s")

        print(f"  Parsing NDJSON → Polars DataFrame...")
        t0 = time.perf_counter()
        df_direct = parse_ndjson_to_dataframe(ndjson_str)
        gen_time = time.perf_counter() - t0
        print(f"  {df_direct.height:,} rows in {gen_time:.1f}s ({df_direct.estimated_size('mb'):.1f} MB)")

    # Run benchmarks — collect all results, then print grouped by category
    def _run(r):
        results.append(r)
        return r

    # ── Stage benchmarks ──────────────────────────────────────────────
    stage_results = []

    if "parse" in benchmarks_to_run and ndjson_str is not None:
        stage_results.append(_run(bench_parse_ndjson(ndjson_str, num_events, scale_name)))

    if "parse_polars" in benchmarks_to_run and ndjson_str is not None:
        stage_results.append(_run(bench_parse_ndjson_polars(ndjson_str, num_events, scale_name)))

    if "parse_native" in benchmarks_to_run and ndjson_str is not None:
        stage_results.append(_run(bench_parse_ndjson_native(ndjson_str, num_events, scale_name)))

    if "device" in benchmarks_to_run:
        stage_results.append(_run(bench_detect_device(num_events, scale_name)))

    if "aggregate" in benchmarks_to_run and events_direct is not None:
        stage_results.append(_run(bench_aggregate(events_direct, scale_name)))

    if "aggregate_polars" in benchmarks_to_run and df_direct is not None:
        stage_results.append(_run(bench_aggregate_polars(df_direct, scale_name)))

    if "session" in benchmarks_to_run and events_direct is not None:
        stage_results.append(_run(bench_session_update(events_direct, scale_name)))

    if "session_polars" in benchmarks_to_run and df_direct is not None:
        stage_results.append(_run(bench_session_polars(df_direct, scale_name)))

    if "viewer" in benchmarks_to_run and events_direct is not None:
        stage_results.append(_run(bench_viewer_metrics(events_direct, scale_name)))

    if "viewer_polars" in benchmarks_to_run and df_direct is not None:
        stage_results.append(_run(bench_viewer_metrics_polars(df_direct, scale_name)))

    if "gzip" in benchmarks_to_run and ndjson_lines is not None:
        stage_results.append(_run(bench_gzip_decompress(ndjson_lines, scale_name)))

    if "event_memory" in benchmarks_to_run:
        _run(bench_event_memory(scale_name, params))

    # ── End-to-end benchmarks ─────────────────────────────────────────
    e2e_results = []

    if "end_to_end_baseline" in benchmarks_to_run and ndjson_str is not None:
        e2e_results.append(_run(bench_end_to_end_baseline(ndjson_str, num_events, scale_name)))

    if "end_to_end" in benchmarks_to_run and ndjson_str is not None:
        e2e_results.append(_run(bench_end_to_end(ndjson_str, num_events, scale_name)))

    if "end_to_end_polars" in benchmarks_to_run and ndjson_str is not None:
        e2e_results.append(_run(bench_end_to_end_polars(ndjson_str, num_events, scale_name)))

    if "end_to_end_hybrid" in benchmarks_to_run and ndjson_str is not None:
        e2e_results.append(_run(bench_end_to_end_hybrid(ndjson_str, num_events, scale_name)))

    if "end_to_end_native" in benchmarks_to_run and ndjson_str is not None:
        e2e_results.append(_run(bench_end_to_end_native(ndjson_str, num_events, scale_name)))

    # Rust binaries produce both stage and end-to-end results
    if "end_to_end_cdn_exporter" in benchmarks_to_run and ndjson_str is not None:
        rs = bench_end_to_end_cdn_exporter(ndjson_str, num_events, scale_name)
        for r in rs:
            results.append(r)
            if r.name.startswith("end_to_end"):
                e2e_results.append(r)
            else:
                stage_results.append(r)

    if "end_to_end_cdn_exporter_polars" in benchmarks_to_run and ndjson_str is not None:
        rs = bench_end_to_end_cdn_exporter_polars(ndjson_str, num_events, scale_name)
        for r in rs:
            results.append(r)
            if r.name.startswith("end_to_end"):
                e2e_results.append(r)
            else:
                stage_results.append(r)

    # ── Print results ─────────────────────────────────────────────────
    header = f"{'Benchmark':<35s} | {'Time':>8s} | {'Peak Memory':>14s} | {'Throughput':>14s}"
    sep = '─' * 80

    if stage_results:
        print(f"\n── Stage benchmarks {'─'*60}")
        print(header)
        print(sep)
        for r in stage_results:
            print(r.summary_line())

    if e2e_results:
        print(f"\n── End-to-end benchmarks {'─'*55}")
        print(header)
        print(sep)
        for r in e2e_results:
            print(r.summary_line())

    # Print comparison table for matched benchmarks
    comparisons = [
        ("parse [python]", "parse [polars]"),
        ("parse [python]", "parse [native]"),
        ("aggregate [python]", "aggregate [polars]"),
        ("session [python]", "session [polars]"),
        ("viewer_metrics [python]", "viewer_metrics [polars]"),
        ("end_to_end [improved]", "end_to_end [polars]"),
    ]
    results_by_name = {r.name: r for r in results}
    pairs = [(a, b) for a, b in comparisons if a in results_by_name and b in results_by_name]
    if pairs:
        print(f"\n{'Comparison':<24s} | {'Original':>10s} | {'Polars':>10s} | {'Speedup':>8s} | {'Mem Δ':>10s}")
        print(f"{'─'*72}")
        for orig_name, polars_name in pairs:
            o = results_by_name[orig_name]
            p = results_by_name[polars_name]
            speedup = o.wall_time_s / p.wall_time_s if p.wall_time_s > 0 else float('inf')
            mem_delta = p.peak_memory_mb - o.peak_memory_mb
            label = orig_name.replace(" [python]", "")
            print(f"{label:<24s} | {o.wall_time_s:>9.3f}s | {p.wall_time_s:>9.3f}s | {speedup:>7.1f}x | {mem_delta:>+9.1f} MB")

    # ── End-to-end implementation comparison ──────────────────────────────
    e2e_impls = {
        "end_to_end [baseline]": {
            "label": "baseline",
            "desc": "Original Python (json.loads, raw_data, no slots)",
        },
        "end_to_end [improved]": {
            "label": "improved",
            "desc": "Python + orjson, no raw_data, __slots__, pre-computed keys",
        },
        "end_to_end [polars]": {
            "label": "polars",
            "desc": "Python parse + Polars aggregate/viewer + Polars sessions",
        },
        "end_to_end [hybrid]": {
            "label": "hybrid",
            "desc": "Python parse/sessions + Polars aggregate/viewer",
        },
        "end_to_end [native]": {
            "label": "native",
            "desc": "Polars native NDJSON reader + Polars aggregate/viewer",
        },
        "end_to_end [cdn-exporter]": {
            "label": "cdn-exporter",
            "desc": "Rust (simd-json, ahash) — full pipeline",
        },
        "end_to_end [cdn-exporter-polars]": {
            "label": "cdn-exporter-polars",
            "desc": "Rust (Polars native NDJSON + group_by) — full pipeline",
        },
    }

    e2e_results = [(n, results_by_name[n]) for n in e2e_impls if n in results_by_name]

    if e2e_results:
        baseline_r = results_by_name.get("end_to_end [baseline]") or results_by_name.get("end_to_end [improved]")
        baseline_time = baseline_r.wall_time_s if baseline_r else 1.0

        print(f"\n{'='*80}")
        print(f"  IMPLEMENTATION COMPARISON  ({num_events:,} events)")
        print(f"{'='*80}")

        for i, (name, r) in enumerate(e2e_results):
            info = e2e_impls[name]
            speedup = baseline_time / r.wall_time_s if r.wall_time_s > 0 else 0

            if i > 0:
                print()

            print(f"\n  {info['label'].upper()}")
            print(f"  {info['desc']}")
            print(f"  {'─'*74}")
            print(f"  Time:        {r.wall_time_s:>9.3f}s")
            print(f"  Speedup:     {speedup:>9.1f}x")
            print(f"  Throughput:  {r.throughput_events_per_s:>9,.0f} evt/s")
            print(f"  Peak memory: {r.peak_memory_mb:>9.0f} MB")
            if "aggregated_series" in r.extra:
                print(f"  Metrics:     {r.extra['aggregated_series']:>9,} series")

            # Show per-stage breakdown for Rust
            if "rust_stages" in r.extra:
                stages = r.extra["rust_stages"]
                print(f"  Stages:      ", end="")
                stage_parts = [f"{k} {v:.3f}s" for k, v in sorted(stages.items())]
                print(" | ".join(stage_parts))

        # Summary table
        print(f"\n{'='*80}")
        print(f"  SUMMARY")
        print(f"{'='*80}")
        print(f"\n  {'Implementation':<16s} {'Time':>10s} {'Speedup':>9s} {'Memory':>10s} {'Throughput':>14s}")
        print(f"  {'─'*62}")
        for name, r in e2e_results:
            info = e2e_impls[name]
            speedup = baseline_time / r.wall_time_s if r.wall_time_s > 0 else 0
            print(
                f"  {info['label']:<16s} "
                f"{r.wall_time_s:>9.3f}s "
                f"{speedup:>8.1f}x "
                f"{r.peak_memory_mb:>8.0f} MB "
                f"{r.throughput_events_per_s:>13,.0f} evt/s"
            )

    # ── Output validation ───────────────────────────────────────────────
    validations = []

    # Get original results once for validation
    orig_computed = None
    if events_direct is not None:
        for rust_name in ["end_to_end [cdn-exporter]", "end_to_end [cdn-exporter-polars]"]:
            rust_result = results_by_name.get(rust_name)
            if rust_result and "rust_aggregation_results" in rust_result.extra:
                if orig_computed is None:
                    orig_aggregator = MetricAggregator(METRIC_DEFINITIONS)
                    orig_agg = orig_aggregator.aggregate_events(events_direct)
                    orig_computed = orig_aggregator.compute_final_values(orig_agg)
                label = rust_name.replace("end_to_end ", "").strip("[]")
                rv = validate_rust_results(orig_computed, rust_result.extra)
                validations.append((f"{label} vs python", rv))

    if events_direct is not None and df_direct is not None:
        pv = validate_aggregation_results(events_direct, df_direct)
        validations.append(("polars vs python", pv))

    if validations:
        print(f"\n  {'─'*62}")
        print(f"  OUTPUT VALIDATION")
        print(f"  {'─'*62}")
        for label, v in validations:
            status = "PASS" if v["match"] else "FAIL"
            series_info = f"{v.get('common', 0)} series"
            print(f"  {status}  {label:<30s} {series_info}")
            if not v["match"]:
                if v.get("missing_in_polars", 0) > 0 or v.get("missing_in_rust", 0) > 0:
                    missing = v.get("missing_in_polars", v.get("missing_in_rust", 0))
                    print(f"       Missing: {missing}")
                    for d in v.get("missing_details", []):
                        print(f"         {d['metric']}: {d['labels']}")
                if v.get("extra_in_polars", 0) > 0 or v.get("extra_in_rust", 0) > 0:
                    extra = v.get("extra_in_polars", v.get("extra_in_rust", 0))
                    print(f"       Extra: {extra}")
                if v.get("value_mismatches", 0) > 0:
                    print(f"       Value mismatches: {v['value_mismatches']}")
                    for d in v.get("mismatch_details", []):
                        print(f"         {d['metric']}: {d}")

    # Print memory details
    for r in results:
        if r.name == "event_memory_profile":
            extra = r.extra
            mem = extra["per_event"]
            print(f"\nEvent Memory Profile (sampled {r.event_count} events, projected to {num_events:,}):")
            print(f"  Per event:        {mem['avg_total_bytes']:.0f} bytes "
                  f"(shell: {mem['avg_shell_bytes']:.0f})")
            print(f"  Projected total:  {extra['projected_total_mb']:.0f} MB")

        if r.name == "session [python]" and "session_memory" in r.extra:
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
