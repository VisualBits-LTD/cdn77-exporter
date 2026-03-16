use ahash::AHashMap;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Parsed CDN log event — mirrors Python Event dataclass.
pub struct Event {
    pub timestamp: DateTime<Utc>,
    pub stream_id: String,
    pub resource_id: i64,
    pub cache_status: String,
    pub response_bytes: i64,
    pub time_to_first_byte_ms: i64,
    pub request_time_ms: i64,
    pub response_status: i32,
    pub client_country: String,
    pub location_id: String,
    pub client_ip: String,
    pub device_type: String,
    pub request_path: String,
    pub minute_key: String,
    pub track: String,
}

/// Aggregated metric result — matches Python compute_final_values output.
#[derive(Debug, Clone, Serialize)]
pub struct MetricResult {
    pub metric_name: String,
    pub labels: AHashMap<String, String>,
    pub value: f64,
    pub minute_key: String,
}

/// Session info for per-IP tracking.
pub struct SessionInfo {
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub device_type: String,
    pub country: String,
    pub region: String,
    pub watch_session_closed: bool,
}

/// Stage timing result.
#[derive(Debug, Serialize)]
pub struct StageTiming {
    pub wall_time_s: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_series: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gauge_metrics: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_sessions: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub viewer_metrics: Option<usize>,
}

/// Full benchmark output — serialized to JSON for Python consumption.
#[derive(Debug, Serialize)]
pub struct BenchmarkOutput {
    pub stages: AHashMap<String, StageTiming>,
    pub total_wall_time_s: f64,
    pub aggregation_results: Vec<MetricResult>,
    pub viewer_metrics: Vec<ViewerMetric>,
}

/// Viewer metric output — matches Python build_file_viewer_metrics format.
#[derive(Debug, Clone, Serialize)]
pub struct ViewerMetric {
    pub metric: String,
    pub value: f64,
    pub timestamp: i64,
}
