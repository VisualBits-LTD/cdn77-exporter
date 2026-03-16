use ahash::AHashMap;
use chrono::{DateTime, Duration, Utc};
use polars::prelude::*;

use crate::types::{SessionInfo, ViewerMetric};

const METRIC_PREFIX: &str = "cdn_";

fn watch_band_label(duration_seconds: f64) -> &'static str {
    if duration_seconds < 60.0 { "0-1m" }
    else if duration_seconds < 300.0 { "1-5m" }
    else if duration_seconds < 1200.0 { "5-20m" }
    else if duration_seconds < 3600.0 { "20-60m" }
    else { "60m+" }
}

pub struct SessionTracker {
    window: Duration,
    session_gap_seconds: i64,
    pub sessions: AHashMap<String, AHashMap<String, SessionInfo>>,
    watch_sessions_total: AHashMap<String, u64>,
    watch_time_seconds_total: AHashMap<String, f64>,
    watch_duration_band_counts: AHashMap<String, AHashMap<String, u64>>,
}

impl SessionTracker {
    pub fn new(window_seconds: i64, session_gap_seconds: i64) -> Self {
        Self {
            window: Duration::seconds(window_seconds),
            session_gap_seconds,
            sessions: AHashMap::new(),
            watch_sessions_total: AHashMap::new(),
            watch_time_seconds_total: AHashMap::new(),
            watch_duration_band_counts: AHashMap::new(),
        }
    }

    /// Update sessions from a Polars DataFrame.
    /// Sorts by timestamp, then iterates rows (session tracking is inherently stateful).
    pub fn update_from_df(&mut self, df: &DataFrame) {
        let sorted = df.clone().lazy()
            .sort(["timestamp"], Default::default())
            .collect()
            .expect("Failed to sort DataFrame");

        let ts_col = sorted.column("timestamp").unwrap().datetime().unwrap();
        let stream_col = sorted.column("stream_id").unwrap().str().unwrap();
        let ip_col = sorted.column("client_ip").unwrap().str().unwrap();
        let device_col = sorted.column("device_type").unwrap().str().unwrap();
        let country_col = sorted.column("client_country").unwrap().str().unwrap();

        for i in 0..sorted.height() {
            let ts_ms = ts_col.get(i).unwrap();
            let timestamp = DateTime::from_timestamp_millis(ts_ms).unwrap().with_timezone(&Utc);
            let stream_id = stream_col.get(i).unwrap_or("");
            let client_ip = ip_col.get(i).unwrap_or("");
            let device_type = device_col.get(i).unwrap_or("other");
            let country = country_col.get(i).unwrap_or("XX");
            let country = if country.is_empty() { "XX" } else { country };

            let stream_sessions = self.sessions
                .entry(stream_id.to_string())
                .or_default();

            match stream_sessions.get(client_ip) {
                None => {
                    stream_sessions.insert(client_ip.to_string(), SessionInfo {
                        first_seen: timestamp,
                        last_seen: timestamp,
                        device_type: device_type.to_string(),
                        country: country.to_string(),
                        region: "Unknown".to_string(),
                        watch_session_closed: false,
                    });
                }
                Some(session) => {
                    if timestamp <= session.last_seen {
                        continue;
                    }
                    let gap = (timestamp - session.last_seen).num_seconds();
                    if gap > self.session_gap_seconds {
                        if !session.watch_session_closed {
                            let dur = (session.last_seen - session.first_seen).num_milliseconds().max(0) as f64 / 1000.0;
                            let band = watch_band_label(dur);
                            *self.watch_sessions_total.entry(stream_id.to_string()).or_default() += 1;
                            *self.watch_time_seconds_total.entry(stream_id.to_string()).or_default() += dur;
                            *self.watch_duration_band_counts.entry(stream_id.to_string()).or_default().entry(band.to_string()).or_default() += 1;
                        }
                        stream_sessions.insert(client_ip.to_string(), SessionInfo {
                            first_seen: timestamp, last_seen: timestamp,
                            device_type: device_type.to_string(), country: country.to_string(),
                            region: "Unknown".to_string(), watch_session_closed: false,
                        });
                    } else if session.watch_session_closed {
                        stream_sessions.insert(client_ip.to_string(), SessionInfo {
                            first_seen: timestamp, last_seen: timestamp,
                            device_type: device_type.to_string(), country: country.to_string(),
                            region: "Unknown".to_string(), watch_session_closed: false,
                        });
                    } else {
                        let session = stream_sessions.get_mut(client_ip).unwrap();
                        session.last_seen = timestamp;
                        session.device_type = device_type.to_string();
                        session.country = country.to_string();
                    }
                }
            }
        }
    }

    pub fn expire_old(&mut self, reference_time: DateTime<Utc>) {
        let cutoff = reference_time - self.window;
        let idle_cutoff = reference_time - Duration::seconds(self.session_gap_seconds);

        // Close idle sessions
        for (stream_id, sessions) in &mut self.sessions {
            for session in sessions.values_mut() {
                if !session.watch_session_closed && session.last_seen < idle_cutoff {
                    let dur = (session.last_seen - session.first_seen).num_milliseconds().max(0) as f64 / 1000.0;
                    let band = watch_band_label(dur);
                    *self.watch_sessions_total.entry(stream_id.clone()).or_default() += 1;
                    *self.watch_time_seconds_total.entry(stream_id.clone()).or_default() += dur;
                    *self.watch_duration_band_counts.entry(stream_id.clone()).or_default().entry(band.to_string()).or_default() += 1;
                    session.watch_session_closed = true;
                }
            }
        }

        // Remove expired
        let mut empty_streams = Vec::new();
        for (stream_id, sessions) in &mut self.sessions {
            sessions.retain(|_, s| s.last_seen >= cutoff);
            if sessions.is_empty() {
                empty_streams.push(stream_id.clone());
            }
        }
        for s in empty_streams {
            self.sessions.remove(&s);
        }
    }

    pub fn get_gauge_metrics(&self, reference_time: DateTime<Utc>) -> Vec<ViewerMetric> {
        let timestamp_ms = reference_time.timestamp_millis();
        let mut metrics = Vec::new();

        for (stream_id, sessions) in &self.sessions {
            if sessions.is_empty() { continue; }

            metrics.push(ViewerMetric {
                metric: format!("{METRIC_PREFIX}viewers{{stream=\"{stream_id}\",stream_name=\"{stream_id}\"}}"),
                value: sessions.len() as f64,
                timestamp: timestamp_ms,
            });

            let mut by_device: AHashMap<&str, usize> = AHashMap::new();
            let mut by_country: AHashMap<&str, usize> = AHashMap::new();
            for session in sessions.values() {
                *by_device.entry(&session.device_type).or_default() += 1;
                *by_country.entry(&session.country).or_default() += 1;
            }
            for (device, count) in &by_device {
                metrics.push(ViewerMetric {
                    metric: format!("{METRIC_PREFIX}viewers_by_device{{stream=\"{stream_id}\",stream_name=\"{stream_id}\",device_type=\"{device}\"}}"),
                    value: *count as f64, timestamp: timestamp_ms,
                });
            }
            for (country, count) in &by_country {
                metrics.push(ViewerMetric {
                    metric: format!("{METRIC_PREFIX}viewers_by_country{{stream=\"{stream_id}\",stream_name=\"{stream_id}\",country=\"{country}\"}}"),
                    value: *count as f64, timestamp: timestamp_ms,
                });
            }
        }
        metrics
    }

    pub fn total_sessions(&self) -> usize {
        self.sessions.values().map(|s| s.len()).sum()
    }
}
