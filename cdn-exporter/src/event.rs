use ahash::AHashMap;
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::Deserialize;
use std::sync::LazyLock;

use crate::device::detect_device_type;
use crate::types::Event;

static STREAM_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^/([a-f0-9]+)/").unwrap());

static TRACK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/(tracks-[^/]+)/").unwrap());

/// Raw JSON fields — borrows from the input buffer to minimize allocations.
#[derive(Deserialize)]
pub struct RawLogEntry<'a> {
    pub timestamp: Option<&'a str>,
    #[serde(rename = "clientRequestPath")]
    pub client_request_path: Option<&'a str>,
    #[serde(rename = "resourceID")]
    pub resource_id: Option<i64>,
    #[serde(rename = "cacheStatus")]
    pub cache_status: Option<&'a str>,
    #[serde(rename = "responseBytes")]
    pub response_bytes: Option<i64>,
    #[serde(rename = "timeToFirstByteMs")]
    pub time_to_first_byte_ms: Option<i64>,
    #[serde(rename = "requestTimeMs")]
    pub request_time_ms: Option<i64>,
    #[serde(rename = "responseStatus")]
    pub response_status: Option<i32>,
    #[serde(rename = "clientCountry")]
    pub client_country: Option<&'a str>,
    #[serde(rename = "locationID")]
    pub location_id: Option<&'a str>,
    #[serde(rename = "clientIP")]
    pub client_ip: Option<&'a str>,
    #[serde(rename = "clientRequestUserAgent")]
    pub client_request_user_agent: Option<&'a str>,
}

/// Parse a timestamp string like "2026-03-15T12:01:34.664Z" into DateTime<Utc>.
fn parse_timestamp(
    s: &str,
    cache: &mut AHashMap<String, DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    if let Some(dt) = cache.get(s) {
        return Some(*dt);
    }

    // Replace trailing Z with +00:00 for RFC 3339 compat
    let normalized = if s.ends_with('Z') {
        format!("{}+00:00", &s[..s.len() - 1])
    } else {
        s.to_string()
    };

    let dt = DateTime::parse_from_rfc3339(&normalized).ok()?.with_timezone(&Utc);

    if cache.len() < 1024 {
        cache.insert(s.to_string(), dt);
    }
    Some(dt)
}

/// Extract track label from request_path, e.g. "/abc/tracks-v3/file" -> "v3".
fn extract_track(path: &str) -> String {
    TRACK_RE
        .captures(path)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().strip_prefix("tracks-").unwrap_or(m.as_str()))
        .unwrap_or("unknown")
        .to_string()
}

/// Parse NDJSON bytes into a Vec<Event>.
pub fn parse_ndjson(input: &[u8]) -> Vec<Event> {
    let mut events = Vec::new();
    let mut ts_cache: AHashMap<String, DateTime<Utc>> = AHashMap::new();

    for line in input.split(|&b| b == b'\n') {
        if line.is_empty() || line.iter().all(|&b| b == b' ' || b == b'\t' || b == b'\r') {
            continue;
        }

        // simd-json needs a mutable copy
        let mut line_buf = line.to_vec();
        let entry: RawLogEntry = match simd_json::from_slice(&mut line_buf) {
            Ok(e) => e,
            Err(_) => continue,
        };

        let ts_str = match entry.timestamp {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };
        let path = match entry.client_request_path {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };

        // Need to re-borrow path from the original line since RawLogEntry borrows from line_buf
        // which we modified. Use serde_json fallback for owned strings.
        let path_owned = path.to_string();

        let stream_id = match STREAM_RE.captures(&path_owned) {
            Some(caps) => caps.get(1).unwrap().as_str().to_string(),
            None => continue,
        };

        let timestamp = match parse_timestamp(ts_str, &mut ts_cache) {
            Some(dt) => dt,
            None => continue,
        };

        let minute_key = timestamp.format("%d/%b/%Y %H:%M").to_string();
        let track = extract_track(&path_owned);
        let ua = entry.client_request_user_agent.unwrap_or("");
        let device_type = detect_device_type(ua);

        events.push(Event {
            timestamp,
            stream_id,
            resource_id: entry.resource_id.unwrap_or(0),
            cache_status: entry.cache_status.unwrap_or("UNKNOWN").to_string(),
            response_bytes: entry.response_bytes.unwrap_or(0),
            time_to_first_byte_ms: entry.time_to_first_byte_ms.unwrap_or(0),
            request_time_ms: entry.request_time_ms.unwrap_or(0),
            response_status: entry.response_status.unwrap_or(0),
            client_country: entry.client_country.unwrap_or("XX").to_string(),
            location_id: entry.location_id.unwrap_or("unknown").to_string(),
            client_ip: entry.client_ip.unwrap_or("0.0.0.0").to_string(),
            device_type: device_type.to_string(),
            request_path: path_owned,
            minute_key,
            track,
        });
    }

    events
}
