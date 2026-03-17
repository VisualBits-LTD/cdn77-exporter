use ahash::{AHashMap, AHashSet};

use crate::types::{Event, ViewerMetric};

const METRIC_PREFIX: &str = "cdn_";

/// Build per-file viewer metrics — mirrors Python build_file_viewer_metrics.
pub fn build_file_viewer_metrics(events: &[Event], timestamp_ms: i64) -> Vec<ViewerMetric> {
    if events.is_empty() {
        return Vec::new();
    }

    let mut by_stream: AHashMap<&str, AHashSet<&str>> = AHashMap::new();
    let mut by_stream_device: AHashMap<(&str, &str), AHashSet<&str>> = AHashMap::new();
    let mut by_stream_country: AHashMap<(&str, &str), AHashSet<&str>> = AHashMap::new();
    let mut by_stream_track: AHashMap<(&str, &str), AHashSet<&str>> = AHashMap::new();

    for event in events {
        let stream = event.stream_id.as_str();
        let ip = event.client_ip.as_str();

        by_stream.entry(stream).or_default().insert(ip);
        by_stream_device
            .entry((stream, event.device_type.as_str()))
            .or_default()
            .insert(ip);

        let country = if event.client_country.is_empty() {
            "XX"
        } else {
            event.client_country.as_str()
        };
        by_stream_country
            .entry((stream, country))
            .or_default()
            .insert(ip);

        if event.track.starts_with('v') {
            by_stream_track
                .entry((stream, event.track.as_str()))
                .or_default()
                .insert(ip);
        }
    }

    let mut metrics = Vec::new();

    for (stream, ips) in &by_stream {
        metrics.push(ViewerMetric {
            metric: format!(
                "{}viewers{{stream=\"{}\",stream_name=\"{}\"}}",
                METRIC_PREFIX, stream, stream
            ),
            value: ips.len() as f64,
            timestamp: timestamp_ms,
        });
    }

    for ((stream, device), ips) in &by_stream_device {
        metrics.push(ViewerMetric {
            metric: format!(
                "{}viewers_by_device{{stream=\"{}\",stream_name=\"{}\",device_type=\"{}\"}}",
                METRIC_PREFIX, stream, stream, device
            ),
            value: ips.len() as f64,
            timestamp: timestamp_ms,
        });
    }

    for ((stream, country), ips) in &by_stream_country {
        metrics.push(ViewerMetric {
            metric: format!(
                "{}viewers_by_country{{stream=\"{}\",stream_name=\"{}\",country=\"{}\"}}",
                METRIC_PREFIX, stream, stream, country
            ),
            value: ips.len() as f64,
            timestamp: timestamp_ms,
        });
    }

    for ((stream, track), ips) in &by_stream_track {
        metrics.push(ViewerMetric {
            metric: format!(
                "{}viewers_by_resolution{{stream=\"{}\",stream_name=\"{}\",track=\"{}\"}}",
                METRIC_PREFIX, stream, stream, track
            ),
            value: ips.len() as f64,
            timestamp: timestamp_ms,
        });
    }

    metrics
}
