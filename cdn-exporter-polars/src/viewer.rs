use polars::prelude::*;

use crate::types::ViewerMetric;

const METRIC_PREFIX: &str = "cdn_";

/// Build per-file viewer metrics using Polars group_by.
pub fn build_file_viewer_metrics(df: &DataFrame, timestamp_ms: i64) -> Vec<ViewerMetric> {
    if df.height() == 0 {
        return Vec::new();
    }

    let mut metrics = Vec::new();
    let lf = df.clone().lazy();

    // viewers per stream
    if let Ok(agg) = lf.clone()
        .group_by([col("stream_id")])
        .agg([col("client_ip").n_unique().alias("count")])
        .collect()
    {
        let streams = agg.column("stream_id").unwrap().str().unwrap();
        let counts = agg.column("count").unwrap().u32().unwrap();
        for i in 0..agg.height() {
            let stream = streams.get(i).unwrap_or("");
            metrics.push(ViewerMetric {
                metric: format!(
                    "{METRIC_PREFIX}viewers{{stream=\"{stream}\",stream_name=\"{stream}\"}}"
                ),
                value: counts.get(i).unwrap_or(0) as f64,
                timestamp: timestamp_ms,
            });
        }
    }

    // viewers_by_device
    if let Ok(agg) = lf.clone()
        .group_by([col("stream_id"), col("device_type")])
        .agg([col("client_ip").n_unique().alias("count")])
        .collect()
    {
        let streams = agg.column("stream_id").unwrap().str().unwrap();
        let devices = agg.column("device_type").unwrap().str().unwrap();
        let counts = agg.column("count").unwrap().u32().unwrap();
        for i in 0..agg.height() {
            let stream = streams.get(i).unwrap_or("");
            let device = devices.get(i).unwrap_or("");
            metrics.push(ViewerMetric {
                metric: format!(
                    "{METRIC_PREFIX}viewers_by_device{{stream=\"{stream}\",stream_name=\"{stream}\",device_type=\"{device}\"}}"
                ),
                value: counts.get(i).unwrap_or(0) as f64,
                timestamp: timestamp_ms,
            });
        }
    }

    // viewers_by_country
    if let Ok(agg) = lf.clone()
        .group_by([col("stream_id"), col("client_country")])
        .agg([col("client_ip").n_unique().alias("count")])
        .collect()
    {
        let streams = agg.column("stream_id").unwrap().str().unwrap();
        let countries = agg.column("client_country").unwrap().str().unwrap();
        let counts = agg.column("count").unwrap().u32().unwrap();
        for i in 0..agg.height() {
            let stream = streams.get(i).unwrap_or("");
            let country = countries.get(i).unwrap_or("");
            metrics.push(ViewerMetric {
                metric: format!(
                    "{METRIC_PREFIX}viewers_by_country{{stream=\"{stream}\",stream_name=\"{stream}\",country=\"{country}\"}}"
                ),
                value: counts.get(i).unwrap_or(0) as f64,
                timestamp: timestamp_ms,
            });
        }
    }

    // viewers_by_resolution (video tracks only)
    if let Ok(agg) = lf
        .filter(col("track").str().starts_with(lit("v")))
        .group_by([col("stream_id"), col("track")])
        .agg([col("client_ip").n_unique().alias("count")])
        .collect()
    {
        let streams = agg.column("stream_id").unwrap().str().unwrap();
        let tracks = agg.column("track").unwrap().str().unwrap();
        let counts = agg.column("count").unwrap().u32().unwrap();
        for i in 0..agg.height() {
            let stream = streams.get(i).unwrap_or("");
            let track = tracks.get(i).unwrap_or("");
            metrics.push(ViewerMetric {
                metric: format!(
                    "{METRIC_PREFIX}viewers_by_resolution{{stream=\"{stream}\",stream_name=\"{stream}\",track=\"{track}\"}}"
                ),
                value: counts.get(i).unwrap_or(0) as f64,
                timestamp: timestamp_ms,
            });
        }
    }

    metrics
}
