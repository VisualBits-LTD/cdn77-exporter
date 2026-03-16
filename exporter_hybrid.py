#!/usr/bin/env python3
"""
Hybrid S3Importer that uses Polars for aggregation and viewer metrics.

Usage:
    Replace S3Importer with HybridS3Importer — same constructor, same API.
    Everything else (S3 polling, event buffering, session tracking,
    Prometheus writer) is inherited unchanged.

    from exporter_hybrid import HybridS3Importer
    importer = HybridS3Importer(s3_client, prometheus_url, ...)
    importer.run_daemon()

Performance (1M events):
    - Aggregation:    110x faster (Polars group_by vs Python loop)
    - Viewer metrics: 3.7x faster (Polars group_by vs Python sets)
    - Sessions:       unchanged (original Python dict tracker)
    - Parse:          unchanged (original orjson parser)
"""

import logging
import queue
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import polars as pl

from exporter import (
    Event,
    S3Importer,
    build_file_viewer_metrics,
    minute_key_to_datetime,
    minute_timestamp_ms,
)
from exporter_polars import (
    dataframe_from_events,
    PolarsMetricAggregator,
    build_file_viewer_metrics_polars,
)

logger = logging.getLogger(__name__)


class HybridAggregator:
    """
    Replaces MetricAggregator with Polars vectorized group_by.

    Converts Event objects to a Polars DataFrame once, then runs
    all 8 metric aggregations in Rust.
    """

    def __init__(self):
        self._polars_agg = PolarsMetricAggregator()

    def aggregate_and_compute(self, events: List[Event]) -> Tuple[List[Dict], pl.DataFrame]:
        """
        Returns:
            (computed_metrics, dataframe) — DataFrame is reused for viewer
            metrics to avoid a second conversion.
        """
        df = dataframe_from_events(events)
        computed = self._polars_agg.aggregate(df)
        return computed, df


class HybridS3Importer(S3Importer):
    """
    S3Importer with Polars-accelerated aggregation and viewer metrics.

    Overrides only _flush_events. Everything else — S3 polling, gzip
    streaming, event parsing, buffering, session tracking, Prometheus
    writer — is inherited from S3Importer.
    """

    def _flush_events(self, events: List[Event], source: str):
        """Aggregate events using Polars and queue for Prometheus."""
        if not events:
            return

        # Session tracking — original Python dict path (fastest)
        self.session_tracker.update(events)
        max_event_time = max(event.timestamp for event in events)
        if self.last_processed_event_time is None or max_event_time > self.last_processed_event_time:
            self.last_processed_event_time = max_event_time

        now = datetime.now(timezone.utc)

        # Group events by minute_key
        events_by_minute: Dict[str, List[Event]] = {}
        for event in events:
            minute_key = event.minute_key
            if minute_key not in events_by_minute:
                events_by_minute[minute_key] = []
            events_by_minute[minute_key].append(event)

        # Cleanup old flush counts (>1 hour old)
        cutoff_time = now - timedelta(hours=1)
        keys_to_remove = [k for k, (_, last_time) in self.flush_count_per_minute.items()
                          if last_time < cutoff_time]
        for key in keys_to_remove:
            del self.flush_count_per_minute[key]
        if keys_to_remove:
            logger.info(f"Cleaned up {len(keys_to_remove)} old flush count entries")

        # Aggregate + build metrics per minute
        aggregator = HybridAggregator()
        consolidated_metrics: List[Dict] = []
        file_viewer_metrics: List[Dict] = []

        for minute_key in sorted(events_by_minute.keys()):
            minute_events = events_by_minute[minute_key]

            # Flush count / batch offset (same logic as original)
            if minute_key in self.flush_count_per_minute:
                flush_count, first_flush_time = self.flush_count_per_minute[minute_key]
                flush_count += 1

                time_since_first = (now - first_flush_time).total_seconds() / 60
                if time_since_first > 10:
                    logger.warning(
                        f"Late batch for {minute_key}: {time_since_first:.1f} minutes "
                        f"after first flush (batch #{flush_count})"
                    )
            else:
                flush_count = 0
                first_flush_time = now

            self.flush_count_per_minute[minute_key] = (flush_count, first_flush_time)
            batch_offset_ms = (flush_count * 100) + self.startup_offset_ms

            # Polars aggregation (110x faster)
            computed_metrics, df = aggregator.aggregate_and_compute(minute_events)

            # Build Prometheus metrics (reuses original PrometheusExporter)
            metrics = self.exporter.build_metrics_from_computed(computed_metrics, batch_offset_ms)

            if metrics:
                offset_str = f" [+{batch_offset_ms}ms offset]" if batch_offset_ms > 0 else ""
                logger.info(
                    f"Built {len(metrics)} metrics for minute {minute_key} "
                    f"({len(minute_events)} events){offset_str}"
                )
                consolidated_metrics.extend(metrics)

            # Polars viewer metrics (3.7x faster)
            timestamp_ms = minute_timestamp_ms(
                minute_key_to_datetime(minute_key), batch_offset_ms
            )
            minute_viewer_metrics = build_file_viewer_metrics_polars(
                df,
                timestamp_ms=timestamp_ms,
                geo_lookup=self.session_tracker.lookup_geo,
            )
            file_viewer_metrics.extend(minute_viewer_metrics)

        consolidated_metrics.extend(file_viewer_metrics)

        if consolidated_metrics:
            try:
                self.write_queue.put((consolidated_metrics, source), timeout=5.0)
                logger.info(
                    f"Queued consolidated flush payload: "
                    f"{len(consolidated_metrics)} metrics from {source}"
                )
            except queue.Full:
                logger.error(
                    f"Queue full! Dropping {len(consolidated_metrics)} "
                    f"consolidated metrics from {source}"
                )
