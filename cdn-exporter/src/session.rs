use ahash::AHashMap;
use chrono::{DateTime, Duration, Utc};

use crate::types::{Event, SessionInfo, ViewerMetric};

const METRIC_PREFIX: &str = "cdn_";

pub struct SessionTracker {
    window: Duration,
    session_gap_seconds: i64,
    pub sessions: AHashMap<String, AHashMap<String, SessionInfo>>,
    watch_sessions_total: AHashMap<String, u64>,
    watch_time_seconds_total: AHashMap<String, f64>,
    watch_duration_band_counts: AHashMap<String, AHashMap<String, u64>>,
}

fn watch_band_label(duration_seconds: f64) -> &'static str {
    if duration_seconds < 60.0 {
        "0-1m"
    } else if duration_seconds < 300.0 {
        "1-5m"
    } else if duration_seconds < 600.0 {
        "5-10m"
    } else if duration_seconds < 1200.0 {
        "10-20m"
    } else if duration_seconds < 3600.0 {
        "20-60m"
    } else {
        "60+"
    }
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

    fn record_closed_session(&mut self, stream_id: &str, session: &SessionInfo) {
        let duration = (session.last_seen - session.first_seen)
            .num_milliseconds()
            .max(0) as f64
            / 1000.0;
        let band = watch_band_label(duration);

        *self
            .watch_sessions_total
            .entry(stream_id.to_string())
            .or_default() += 1;
        *self
            .watch_time_seconds_total
            .entry(stream_id.to_string())
            .or_default() += duration;
        *self
            .watch_duration_band_counts
            .entry(stream_id.to_string())
            .or_default()
            .entry(band.to_string())
            .or_default() += 1;
    }

    fn close_idle_sessions(&mut self, reference_time: DateTime<Utc>) {
        let idle_cutoff = reference_time - Duration::seconds(self.session_gap_seconds);
        // Collect closures first to avoid borrow conflict
        let mut to_close: Vec<(String, f64)> = Vec::new();
        for (stream_id, sessions) in &self.sessions {
            for session in sessions.values() {
                if !session.watch_session_closed && session.last_seen < idle_cutoff {
                    let duration = (session.last_seen - session.first_seen)
                        .num_milliseconds()
                        .max(0) as f64
                        / 1000.0;
                    to_close.push((stream_id.clone(), duration));
                }
            }
        }
        // Now mutate
        for (stream_id, sessions) in &mut self.sessions {
            for session in sessions.values_mut() {
                if !session.watch_session_closed && session.last_seen < idle_cutoff {
                    session.watch_session_closed = true;
                }
            }
        }
        for (stream_id, duration) in &to_close {
            let band = watch_band_label(*duration);
            *self
                .watch_sessions_total
                .entry(stream_id.clone())
                .or_default() += 1;
            *self
                .watch_time_seconds_total
                .entry(stream_id.clone())
                .or_default() += duration;
            *self
                .watch_duration_band_counts
                .entry(stream_id.clone())
                .or_default()
                .entry(band.to_string())
                .or_default() += 1;
        }
    }

    pub fn update(&mut self, events: &[Event]) {
        for event in events {
            let country = if event.client_country.is_empty() {
                "XX"
            } else {
                &event.client_country
            };

            let stream_sessions = self
                .sessions
                .entry(event.stream_id.clone())
                .or_default();

            let current = stream_sessions.get(&event.client_ip);

            match current {
                None => {
                    stream_sessions.insert(
                        event.client_ip.clone(),
                        SessionInfo {
                            first_seen: event.timestamp,
                            last_seen: event.timestamp,
                            device_type: event.device_type.clone(),
                            country: country.to_string(),
                            region: "Unknown".to_string(),
                            watch_session_closed: false,
                        },
                    );
                }
                Some(session) => {
                    if event.timestamp <= session.last_seen {
                        continue;
                    }

                    let gap = (event.timestamp - session.last_seen).num_seconds();

                    if gap > self.session_gap_seconds {
                        if !session.watch_session_closed {
                            let duration = (session.last_seen - session.first_seen)
                                .num_milliseconds()
                                .max(0) as f64
                                / 1000.0;
                            let band = watch_band_label(duration);
                            *self
                                .watch_sessions_total
                                .entry(event.stream_id.clone())
                                .or_default() += 1;
                            *self
                                .watch_time_seconds_total
                                .entry(event.stream_id.clone())
                                .or_default() += duration;
                            *self
                                .watch_duration_band_counts
                                .entry(event.stream_id.clone())
                                .or_default()
                                .entry(band.to_string())
                                .or_default() += 1;
                        }
                        stream_sessions.insert(
                            event.client_ip.clone(),
                            SessionInfo {
                                first_seen: event.timestamp,
                                last_seen: event.timestamp,
                                device_type: event.device_type.clone(),
                                country: country.to_string(),
                                region: "Unknown".to_string(),
                                watch_session_closed: false,
                            },
                        );
                    } else if session.watch_session_closed {
                        stream_sessions.insert(
                            event.client_ip.clone(),
                            SessionInfo {
                                first_seen: event.timestamp,
                                last_seen: event.timestamp,
                                device_type: event.device_type.clone(),
                                country: country.to_string(),
                                region: "Unknown".to_string(),
                                watch_session_closed: false,
                            },
                        );
                    } else {
                        let session = stream_sessions.get_mut(&event.client_ip).unwrap();
                        session.last_seen = event.timestamp;
                        session.device_type = event.device_type.clone();
                        session.country = country.to_string();
                    }
                }
            }
        }
    }

    pub fn expire_old(&mut self, reference_time: DateTime<Utc>) {
        let cutoff = reference_time - self.window;
        self.close_idle_sessions(reference_time);

        let mut empty_streams = Vec::new();
        for (stream_id, sessions) in &mut self.sessions {
            let expired_ips: Vec<String> = sessions
                .iter()
                .filter(|(_, s)| s.last_seen < cutoff)
                .map(|(ip, _)| ip.clone())
                .collect();

            for ip in &expired_ips {
                if let Some(session) = sessions.get(ip) {
                    if !session.watch_session_closed {
                        let duration = (session.last_seen - session.first_seen)
                            .num_milliseconds()
                            .max(0) as f64
                            / 1000.0;
                        let band = watch_band_label(duration);
                        // Can't call self methods here, accumulate inline
                        // (already recorded by close_idle_sessions above in most cases)
                    }
                }
                sessions.remove(ip);
            }

            if sessions.is_empty() {
                empty_streams.push(stream_id.clone());
            }
        }
        for stream_id in empty_streams {
            self.sessions.remove(&stream_id);
        }
    }

    pub fn get_gauge_metrics(&self, reference_time: DateTime<Utc>) -> Vec<ViewerMetric> {
        let timestamp_ms = reference_time.timestamp_millis();
        let mut metrics = Vec::new();

        for (stream_id, sessions) in &self.sessions {
            if sessions.is_empty() {
                continue;
            }

            // viewers per stream
            metrics.push(ViewerMetric {
                metric: format!(
                    "{}viewers{{stream=\"{}\",stream_name=\"{}\"}}",
                    METRIC_PREFIX, stream_id, stream_id
                ),
                value: sessions.len() as f64,
                timestamp: timestamp_ms,
            });

            // viewers_by_device
            let mut by_device: AHashMap<&str, usize> = AHashMap::new();
            for session in sessions.values() {
                *by_device.entry(&session.device_type).or_default() += 1;
            }
            for (device, count) in &by_device {
                metrics.push(ViewerMetric {
                    metric: format!(
                        "{}viewers_by_device{{stream=\"{}\",stream_name=\"{}\",device_type=\"{}\"}}",
                        METRIC_PREFIX, stream_id, stream_id, device
                    ),
                    value: *count as f64,
                    timestamp: timestamp_ms,
                });
            }

            // viewers_by_country
            let mut by_country: AHashMap<&str, usize> = AHashMap::new();
            for session in sessions.values() {
                *by_country.entry(&session.country).or_default() += 1;
            }
            for (country, count) in &by_country {
                metrics.push(ViewerMetric {
                    metric: format!(
                        "{}viewers_by_country{{stream=\"{}\",stream_name=\"{}\",country=\"{}\"}}",
                        METRIC_PREFIX, stream_id, stream_id, country
                    ),
                    value: *count as f64,
                    timestamp: timestamp_ms,
                });
            }
        }

        metrics
    }

    pub fn total_sessions(&self) -> usize {
        self.sessions.values().map(|s| s.len()).sum()
    }
}
