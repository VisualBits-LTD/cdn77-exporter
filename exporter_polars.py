#!/usr/bin/env python3
"""
Polars-based alternative aggregation pipeline for CDN77 Prometheus Exporter.

Replaces the per-event Python iteration in MetricAggregator with
vectorized Polars group_by operations. Session tracking remains
Python dict-based (inherently stateful per-IP).

Output format is identical to the original pipeline so
PrometheusExporter can consume it without changes.
"""

import re
from datetime import datetime, timezone
from typing import Callable, Dict, List, Tuple

import polars as pl

try:
    import orjson
    _json_loads = orjson.loads
except ImportError:
    import json
    _json_loads = json.loads

from exporter import (
    SessionTracker,
    _escape_prometheus_label_value,
    detect_device_type,
    extract_track_from_path,
    metric_name,
    stream_label_selector,
)


# ============================================================================
# NDJSON → DataFrame
# ============================================================================

_STREAM_PATTERN = re.compile(r'^/([a-f0-9]+)/')


def _polars_device_type_expr() -> pl.Expr:
    """
    Build a Polars expression that replicates detect_device_type logic.
    Operates on a column named 'ua_lower' (lowercased, URL-decoded user-agent).
    Order of when() clauses mirrors the Python function's priority.
    """
    ua = pl.col('ua_lower')
    return (
        pl.when(ua.str.contains('appletv') | ua.str.contains('apple tv') | ua.str.contains('tvos'))
        .then(pl.lit('apple_tv'))
        .when(ua.str.contains('roku'))
        .then(pl.lit('roku'))
        .when(ua.str.contains('aft') | (ua.str.contains('amazon') & ua.str.contains('fire')))
        .then(pl.lit('firestick'))
        .when(
            ua.str.contains('bot') | ua.str.contains('crawler') | ua.str.contains('spider')
            | ua.str.contains('scraper') | ua.str.contains('headless')
            | ua.str.contains('frame capture') | ua.str.contains('yallbot')
        )
        .then(pl.lit('bots'))
        # First-party apps with android/ios disambiguation
        .when(
            (ua.str.contains('baron') | ua.str.contains('virtualrailfan') | ua.str.contains('virtual railfan'))
            & ua.str.contains('android')
        )
        .then(pl.lit('android'))
        .when(
            (ua.str.contains('baron') | ua.str.contains('virtualrailfan') | ua.str.contains('virtual railfan'))
            & (ua.str.contains('ios') | ua.str.contains('iphone') | ua.str.contains('ipad'))
        )
        .then(pl.lit('ios'))
        # iOS
        .when(
            ua.str.contains('iphone') | ua.str.contains('ipad')
            | ua.str.contains('cpu os') | ua.str.contains('applecoremedia')
        )
        .then(pl.lit('ios'))
        # Android
        .when(
            ua.str.contains('android') | ua.str.contains('androidxmedia3')
            | ua.str.contains('exoplayer') | ua.str.contains('dalvik')
        )
        .then(pl.lit('android'))
        # Streamology
        .when(
            ua.str.starts_with('lavf/') | ua.str.contains('gstreamer')
            | ua.str.contains('gst-launch') | ua.str.contains('libgst')
        )
        .then(pl.lit('streamology'))
        # CFNetwork → other
        .when(ua.str.contains('cfnetwork/'))
        .then(pl.lit('other'))
        # Darwin → web
        .when(ua.str.contains('darwin/'))
        .then(pl.lit('web'))
        # Browsers → web
        .when(
            ua.str.contains('chrome') | ua.str.contains('firefox')
            | ua.str.contains('safari') | ua.str.contains('edge')
            | ua.str.contains('opera')
        )
        .then(pl.lit('web'))
        .otherwise(pl.lit('other'))
    )


def parse_ndjson_native(ndjson_bytes: bytes) -> pl.DataFrame:
    """
    Parse NDJSON using Polars native reader — no Python-level iteration.
    All field extraction, device detection, and derived columns happen in Rust/Polars.

    Args:
        ndjson_bytes: raw NDJSON as bytes (UTF-8)
    """
    import io

    # Polars native NDJSON reader (Rust-based, no Python per-row loop)
    df = pl.read_ndjson(
        io.BytesIO(ndjson_bytes),
        schema_overrides={
            'resourceID': pl.Int64,
            'responseBytes': pl.Int64,
            'timeToFirstByteMs': pl.Int64,
            'tcpRTTus': pl.Int64,
            'requestTimeMs': pl.Int64,
            'responseStatus': pl.Int32,
        },
        ignore_errors=True,
    )

    # Rename JSON fields to our internal column names and add derived columns
    df = df.rename({
        'clientRequestPath': 'request_path',
        'resourceID': 'resource_id',
        'cacheStatus': 'cache_status',
        'responseBytes': 'response_bytes',
        'timeToFirstByteMs': 'time_to_first_byte_ms',
        'tcpRTTus': 'tcp_rtt_us',
        'requestTimeMs': 'request_time_ms',
        'responseStatus': 'response_status',
        'clientCountry': 'client_country',
        'locationID': 'location_id',
        'clientIP': 'client_ip',
        'clientRequestUserAgent': 'user_agent',
    })

    # Filter rows missing required fields
    df = df.filter(
        pl.col('timestamp').is_not_null()
        & pl.col('request_path').is_not_null()
    )

    # Parse timestamp string → datetime
    df = df.with_columns(
        pl.col('timestamp').str.replace('Z', '+00:00', literal=True)
            .str.to_datetime(time_zone='UTC')
            .alias('timestamp'),
    )

    # Extract stream_id from request_path via regex
    df = df.with_columns(
        pl.col('request_path').str.extract(r'^/([a-f0-9]+)/', 1).alias('stream_id'),
    )

    # Drop rows without a valid stream_id
    df = df.filter(pl.col('stream_id').is_not_null())

    # URL-decode and lowercase user-agent, then apply device detection
    df = df.with_columns(
        pl.col('user_agent').fill_null('').str.to_lowercase().alias('ua_lower'),
    )
    df = df.with_columns(
        _polars_device_type_expr().alias('device_type'),
    )

    # Derived columns
    df = df.with_columns([
        pl.col('timestamp').dt.strftime('%d/%b/%Y %H:%M').alias('minute_key'),
        pl.col('request_path').str.extract(r'/(tracks-[^/]+)/', 1)
            .str.replace(r'^tracks-', '')
            .fill_null('unknown')
            .alias('track'),
    ])

    # Fill nulls for optional fields
    df = df.with_columns([
        pl.col('client_country').fill_null('XX'),
        pl.col('location_id').fill_null('unknown'),
        pl.col('client_ip').fill_null('0.0.0.0'),
        pl.col('resource_id').fill_null(0),
        pl.col('response_bytes').fill_null(0),
        pl.col('time_to_first_byte_ms').fill_null(0),
        pl.col('tcp_rtt_us').fill_null(0),
        pl.col('request_time_ms').fill_null(0),
        pl.col('response_status').fill_null(0),
    ])

    # Select only the columns we need (drop raw JSON extras)
    return df.select([
        'timestamp', 'stream_id', 'resource_id', 'cache_status',
        'response_bytes', 'time_to_first_byte_ms', 'tcp_rtt_us',
        'request_time_ms', 'response_status', 'client_country',
        'location_id', 'client_ip', 'device_type', 'request_path',
        'minute_key', 'track',
    ])


def parse_ndjson_to_dataframe(ndjson_str: str) -> pl.DataFrame:
    """
    Parse NDJSON string into a Polars DataFrame.

    Uses orjson for JSON parsing and applies stream_id extraction
    and device_type detection per-row before constructing the DataFrame.
    """
    # Column-oriented storage for bulk DataFrame construction
    timestamps = []
    stream_ids = []
    resource_ids = []
    cache_statuses = []
    response_bytes_list = []
    ttfb_list = []
    tcp_rtt_list = []
    request_time_list = []
    response_statuses = []
    client_countries = []
    location_ids = []
    client_ips = []
    device_types = []
    request_paths = []

    for line in ndjson_str.splitlines():
        if not line.strip():
            continue

        data = _json_loads(line)

        timestamp_str = data.get('timestamp')
        path = data.get('clientRequestPath', '')

        if not timestamp_str or not path:
            continue

        match = _STREAM_PATTERN.match(path)
        if not match:
            continue

        # Parse timestamp
        try:
            dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            continue

        user_agent = data.get('clientRequestUserAgent', '')

        timestamps.append(dt)
        stream_ids.append(match.group(1))
        resource_ids.append(data.get('resourceID', 0))
        cache_statuses.append(data.get('cacheStatus', 'UNKNOWN'))
        response_bytes_list.append(data.get('responseBytes', 0))
        ttfb_list.append(data.get('timeToFirstByteMs', 0))
        tcp_rtt_list.append(data.get('tcpRTTus', 0))
        request_time_list.append(data.get('requestTimeMs', 0))
        response_statuses.append(data.get('responseStatus', 0))
        client_countries.append(data.get('clientCountry', 'XX'))
        location_ids.append(data.get('locationID', 'unknown'))
        client_ips.append(data.get('clientIP', '0.0.0.0'))
        device_types.append(detect_device_type(user_agent))
        request_paths.append(path)

    df = pl.DataFrame({
        'timestamp': timestamps,
        'stream_id': stream_ids,
        'resource_id': resource_ids,
        'cache_status': cache_statuses,
        'response_bytes': response_bytes_list,
        'time_to_first_byte_ms': ttfb_list,
        'tcp_rtt_us': tcp_rtt_list,
        'request_time_ms': request_time_list,
        'response_status': response_statuses,
        'client_country': client_countries,
        'location_id': location_ids,
        'client_ip': client_ips,
        'device_type': device_types,
        'request_path': request_paths,
    })

    # Add derived columns using Polars expressions
    df = df.with_columns([
        pl.col('timestamp').dt.strftime('%d/%b/%Y %H:%M').alias('minute_key'),
        pl.col('request_path').str.extract(r'/(tracks-[^/]+)/', 1)
            .str.replace(r'^tracks-', '')
            .fill_null('unknown')
            .alias('track'),
    ])

    return df


def dataframe_from_events(events) -> pl.DataFrame:
    """
    Convert a list of Event objects into a Polars DataFrame.
    Used by the hybrid path: parse with original parser, aggregate with Polars.
    """
    if not events:
        return pl.DataFrame()

    df = pl.DataFrame({
        'timestamp': [e.timestamp for e in events],
        'stream_id': [e.stream_id for e in events],
        'resource_id': [e.resource_id for e in events],
        'cache_status': [e.cache_status for e in events],
        'response_bytes': [e.response_bytes for e in events],
        'time_to_first_byte_ms': [e.time_to_first_byte_ms for e in events],
        'tcp_rtt_us': [e.tcp_rtt_us for e in events],
        'request_time_ms': [e.request_time_ms for e in events],
        'response_status': [e.response_status for e in events],
        'client_country': [e.client_country for e in events],
        'location_id': [e.location_id for e in events],
        'client_ip': [e.client_ip for e in events],
        'device_type': [e.device_type for e in events],
        'request_path': [e.request_path for e in events],
        'minute_key': [e.minute_key for e in events],
    })

    df = df.with_columns(
        pl.col('request_path').str.extract(r'/(tracks-[^/]+)/', 1)
            .str.replace(r'^tracks-', '')
            .fill_null('unknown')
            .alias('track'),
    )

    return df


def dataframe_from_events_direct(
    num_events: int,
    num_streams: int = 10,
    num_unique_ips: int = 10_000,
    num_locations: int = 10,
    seed: int = 42,
    time_spread_minutes: int = 2,
) -> pl.DataFrame:
    """
    Generate a Polars DataFrame directly (bypassing JSON).
    Equivalent to bench_exporter.generate_events_direct but columnar.
    """
    import random
    from datetime import timedelta

    # Reuse generators from bench_exporter
    from bench_exporter import (
        generate_ip_pool,
        generate_stream_ids,
        generate_location_ids,
        USER_AGENTS, UA_WEIGHTS,
        COUNTRIES, CACHE_STATUSES, CACHE_WEIGHTS,
        STATUS_CODES, STATUS_WEIGHTS,
        TRACKS,
    )

    rng = random.Random(seed)
    ip_pool = generate_ip_pool(num_unique_ips, seed=seed)
    stream_id_pool = generate_stream_ids(num_streams, seed=seed)
    locations = generate_location_ids(num_locations)

    base_time = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    spread_seconds = time_spread_minutes * 60

    ips = rng.choices(ip_pool, k=num_events)
    streams = rng.choices(stream_id_pool, k=num_events)
    locs = rng.choices(locations, k=num_events)
    uas = rng.choices(USER_AGENTS, weights=UA_WEIGHTS, k=num_events)
    caches = rng.choices(CACHE_STATUSES, weights=CACHE_WEIGHTS, k=num_events)
    statuses = rng.choices(STATUS_CODES, weights=STATUS_WEIGHTS, k=num_events)
    countries = rng.choices(COUNTRIES, k=num_events)
    ts_offsets = [rng.randint(0, spread_seconds) for _ in range(num_events)]
    tracks = rng.choices(TRACKS, k=num_events)

    timestamps = [base_time + timedelta(seconds=o) for o in ts_offsets]
    device_types = [ua[1] for ua in uas]
    request_paths = [f"/{streams[i]}/{tracks[i]}" for i in range(num_events)]
    resp_bytes = [rng.randint(1000, 500000) for _ in range(num_events)]
    ttfbs = [rng.randint(50, 300) for _ in range(num_events)]

    df = pl.DataFrame({
        'timestamp': timestamps,
        'stream_id': streams,
        'resource_id': [1693215245] * num_events,
        'cache_status': caches,
        'response_bytes': resp_bytes,
        'time_to_first_byte_ms': ttfbs,
        'tcp_rtt_us': [50000] * num_events,
        'request_time_ms': [150] * num_events,
        'response_status': statuses,
        'client_country': countries,
        'location_id': locs,
        'client_ip': ips,
        'device_type': device_types,
        'request_path': request_paths,
    })

    df = df.with_columns([
        pl.col('timestamp').dt.strftime('%d/%b/%Y %H:%M').alias('minute_key'),
        pl.col('request_path').str.extract(r'/(tracks-[^/]+)/', 1)
            .str.replace(r'^tracks-', '')
            .fill_null('unknown')
            .alias('track'),
    ])

    return df


# ============================================================================
# POLARS METRIC AGGREGATOR
# ============================================================================

class PolarsMetricAggregator:
    """Replaces MetricAggregator with vectorized Polars group_by operations."""

    def aggregate(self, df: pl.DataFrame) -> List[Dict]:
        """
        Run all 8 metric aggregations on the DataFrame.

        Returns list of dicts matching compute_final_values output:
            {'metric_name': str, 'labels': dict, 'value': float, 'minute_key': str}
        """
        results: List[Dict] = []

        # 1. cdn_transfer_bytes_total (SUM response_bytes)
        results.extend(self._group_sum(
            df, 'response_bytes',
            metric_name('transfer_bytes_total'),
            group_cols=['cache_status', 'resource_id', 'location_id', 'minute_key', 'stream_id'],
            label_map={'cache_status': 'cache_status', 'cdn_id': 'resource_id',
                       'pop': 'location_id', 'stream': 'stream_id'},
        ))

        # 2. cdn_time_to_first_byte_ms (AVG where > 0)
        ttfb_df = df.filter(pl.col('time_to_first_byte_ms') > 0)
        if ttfb_df.height > 0:
            agg = ttfb_df.group_by(
                ['cache_status', 'resource_id', 'location_id', 'minute_key', 'stream_id']
            ).agg(
                pl.col('time_to_first_byte_ms').cast(pl.Float64).mean().alias('value')
            )
            results.extend(self._df_to_dicts(
                agg, metric_name('time_to_first_byte_ms'),
                {'cache_status': 'cache_status', 'cdn_id': 'resource_id',
                 'pop': 'location_id', 'stream': 'stream_id'},
            ))

        # 3. cdn_requests_total (COUNT)
        results.extend(self._group_count(
            df, metric_name('requests_total'),
            group_cols=['cache_status', 'resource_id', 'location_id', 'minute_key', 'stream_id'],
            label_map={'cache_status': 'cache_status', 'cdn_id': 'resource_id',
                       'pop': 'location_id', 'stream': 'stream_id'},
        ))

        # 4. cdn_responses_total (COUNT, adds response_status label)
        results.extend(self._group_count(
            df, metric_name('responses_total'),
            group_cols=['cache_status', 'resource_id', 'location_id', 'minute_key',
                        'response_status', 'stream_id'],
            label_map={'cache_status': 'cache_status', 'cdn_id': 'resource_id',
                       'pop': 'location_id', 'response_status': 'response_status',
                       'stream': 'stream_id'},
        ))

        # 5. cdn_users_total (UNIQUE_COUNT of client_ip)
        results.extend(self._group_nunique(
            df, 'client_ip', metric_name('users_total'),
            group_cols=['minute_key', 'stream_id'],
            label_map={'stream': 'stream_id'},
        ))

        # 6. cdn_users_by_device_total
        results.extend(self._group_nunique(
            df, 'client_ip', metric_name('users_by_device_total'),
            group_cols=['device_type', 'minute_key', 'stream_id'],
            label_map={'device_type': 'device_type', 'stream': 'stream_id'},
        ))

        # 7. cdn_users_by_country_total
        results.extend(self._group_nunique(
            df, 'client_ip', metric_name('users_by_country_total'),
            group_cols=['client_country', 'minute_key', 'stream_id'],
            label_map={'country': 'client_country', 'stream': 'stream_id'},
        ))

        # 8. cdn_users_by_resolution_total (only video tracks)
        video_df = df.filter(pl.col('track').str.starts_with('v'))
        if video_df.height > 0:
            results.extend(self._group_nunique(
                video_df, 'client_ip', metric_name('users_by_resolution_total'),
                group_cols=['minute_key', 'stream_id', 'track'],
                label_map={'stream': 'stream_id', 'track': 'track'},
            ))

        return results

    # -- Helpers ---------------------------------------------------------------

    def _group_sum(self, df, value_col, name, group_cols, label_map):
        agg = df.group_by(group_cols).agg(
            pl.col(value_col).cast(pl.Float64).sum().alias('value')
        )
        return self._df_to_dicts(agg, name, label_map)

    def _group_count(self, df, name, group_cols, label_map):
        agg = df.group_by(group_cols).agg(
            pl.len().cast(pl.Float64).alias('value')
        )
        return self._df_to_dicts(agg, name, label_map)

    def _group_nunique(self, df, col, name, group_cols, label_map):
        agg = df.group_by(group_cols).agg(
            pl.col(col).n_unique().cast(pl.Float64).alias('value')
        )
        return self._df_to_dicts(agg, name, label_map)

    def _df_to_dicts(self, agg_df, name, label_map):
        """Convert aggregated DataFrame to list of output dicts."""
        results = []
        for row in agg_df.iter_rows(named=True):
            labels = {
                label_name: str(row[col_name])
                for label_name, col_name in label_map.items()
            }
            labels['minute'] = row['minute_key']
            results.append({
                'metric_name': name,
                'labels': labels,
                'value': row['value'],
                'minute_key': row['minute_key'],
            })
        return results


# ============================================================================
# POLARS FILE VIEWER METRICS
# ============================================================================

def build_file_viewer_metrics_polars(
    df: pl.DataFrame,
    timestamp_ms: int,
    geo_lookup: Callable[[str], Tuple[str, str]],
) -> List[Dict]:
    """
    Build viewer metrics from a DataFrame.
    Equivalent to build_file_viewer_metrics but using Polars group_by.
    """
    if df.height == 0:
        return []

    metrics: List[Dict] = []

    # viewers per stream
    agg = df.group_by('stream_id').agg(pl.col('client_ip').n_unique())
    for row in agg.iter_rows(named=True):
        stream = row['stream_id']
        metrics.append({
            'metric': f'{metric_name("viewers")}{{{stream_label_selector(stream)}}}',
            'value': float(row['client_ip']),
            'timestamp': timestamp_ms,
        })

    # viewers_by_device
    agg = df.group_by(['stream_id', 'device_type']).agg(pl.col('client_ip').n_unique())
    for row in agg.iter_rows(named=True):
        stream = row['stream_id']
        device_type = _escape_prometheus_label_value(row['device_type'])
        metrics.append({
            'metric': f'{metric_name("viewers_by_device")}{{{stream_label_selector(stream)},device_type="{device_type}"}}',
            'value': float(row['client_ip']),
            'timestamp': timestamp_ms,
        })

    # viewers_by_country
    agg = df.group_by(['stream_id', 'client_country']).agg(pl.col('client_ip').n_unique())
    for row in agg.iter_rows(named=True):
        stream = row['stream_id']
        country = _escape_prometheus_label_value(row['client_country'])
        metrics.append({
            'metric': f'{metric_name("viewers_by_country")}{{{stream_label_selector(stream)},country="{country}"}}',
            'value': float(row['client_ip']),
            'timestamp': timestamp_ms,
        })

    # viewers_by_resolution (video tracks only)
    video_df = df.filter(pl.col('track').str.starts_with('v'))
    if video_df.height > 0:
        agg = video_df.group_by(['stream_id', 'track']).agg(pl.col('client_ip').n_unique())
        for row in agg.iter_rows(named=True):
            stream = row['stream_id']
            track = _escape_prometheus_label_value(row['track'])
            metrics.append({
                'metric': f'{metric_name("viewers_by_resolution")}{{{stream_label_selector(stream)},track="{track}"}}',
                'value': float(row['client_ip']),
                'timestamp': timestamp_ms,
            })

    # viewers_by_region (requires geo_lookup)
    unique_ips = df.select(['client_ip', 'client_country']).unique()
    ip_list = unique_ips['client_ip'].to_list()
    country_list = unique_ips['client_country'].to_list()

    geo_results = {}
    for ip, country in zip(ip_list, country_list):
        lookup_country, region = geo_lookup(ip)
        if not country:
            country = lookup_country
        geo_results[ip] = (country, region)

    # Build region lookup DataFrame and join
    if geo_results:
        region_data = {
            'client_ip': list(geo_results.keys()),
            'geo_country': [v[0] for v in geo_results.values()],
            'geo_region': [v[1] for v in geo_results.values()],
        }
        region_df = pl.DataFrame(region_data)

        df_with_region = df.join(region_df, on='client_ip', how='left')
        region_valid = df_with_region.filter(
            (pl.col('geo_region') != 'Unknown') & pl.col('geo_region').is_not_null()
        )

        if region_valid.height > 0:
            agg = region_valid.group_by(
                ['stream_id', 'geo_country', 'geo_region']
            ).agg(pl.col('client_ip').n_unique())
            for row in agg.iter_rows(named=True):
                stream = row['stream_id']
                country = _escape_prometheus_label_value(row['geo_country'])
                region = _escape_prometheus_label_value(row['geo_region'])
                metrics.append({
                    'metric': (
                        f'{metric_name("viewers_by_region")}'
                        f'{{{stream_label_selector(stream)},'
                        f'country="{country}",'
                        f'region="{region}"}}'
                    ),
                    'value': float(row['client_ip']),
                    'timestamp': timestamp_ms,
                })

    return metrics


# ============================================================================
# SESSION TRACKER ADAPTER
# ============================================================================

class PolarsSessionTracker(SessionTracker):
    """SessionTracker that accepts Polars DataFrames."""

    def update_from_df(self, df: pl.DataFrame):
        """
        Update sessions from a DataFrame.
        Extracts minimal columns and delegates to parent update logic.
        """
        # Sort by timestamp for correct session gap detection
        sorted_df = df.select(
            ['stream_id', 'client_ip', 'timestamp', 'device_type', 'client_country']
        ).sort('timestamp')

        with self._lock:
            for row in sorted_df.iter_rows(named=True):
                stream_id = row['stream_id']
                client_ip = row['client_ip']
                ts = row['timestamp']
                device_type = row['device_type']
                country = row['client_country'] or 'XX'
                region = 'Unknown'

                if self.geoip_reader:
                    geo_country, geo_region = self.lookup_geo(client_ip)
                    if not country:
                        country = geo_country
                    if geo_region:
                        region = geo_region

                current_session = self.sessions[stream_id].get(client_ip)

                if current_session is None:
                    from exporter import SessionInfo
                    self.sessions[stream_id][client_ip] = SessionInfo(
                        first_seen=ts,
                        last_seen=ts,
                        device_type=device_type,
                        country=country,
                        region=region,
                    )
                    continue

                if ts <= current_session.last_seen:
                    continue

                gap_seconds = (ts - current_session.last_seen).total_seconds()
                if gap_seconds > self.session_gap_seconds:
                    if not current_session.watch_session_closed:
                        self._record_closed_session(stream_id, current_session)
                    self.sessions[stream_id][client_ip] = SessionInfo(
                        first_seen=ts,
                        last_seen=ts,
                        device_type=device_type,
                        country=country,
                        region=region,
                    )
                else:
                    if current_session.watch_session_closed:
                        self.sessions[stream_id][client_ip] = SessionInfo(
                            first_seen=ts,
                            last_seen=ts,
                            device_type=device_type,
                            country=country,
                            region=region,
                        )
                    else:
                        current_session.last_seen = ts
                        current_session.device_type = device_type
                        current_session.country = country
                        current_session.region = region
