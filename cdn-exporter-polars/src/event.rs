use polars::prelude::*;
use std::io::Cursor;
use std::num::NonZero;

use crate::device::device_type_expr;

/// Parse NDJSON bytes into a Polars DataFrame using native reader.
/// All parsing, field extraction, and device detection happen in Polars — no per-row loop.
pub fn parse_ndjson(input: &[u8]) -> DataFrame {
    let cursor = Cursor::new(input);
    let mut df = JsonReader::new(cursor)
        .with_json_format(JsonFormat::JsonLines)
        .infer_schema_len(Some(NonZero::new(100).unwrap()))
        .finish()
        .expect("Failed to parse NDJSON");

    let lf = df.lazy()
        // Rename JSON fields to internal column names
        .rename(
            [
                "clientRequestPath", "resourceID", "cacheStatus", "responseBytes",
                "timeToFirstByteMs", "tcpRTTus", "requestTimeMs", "responseStatus",
                "clientCountry", "locationID", "clientIP", "clientRequestUserAgent",
            ],
            [
                "request_path", "resource_id", "cache_status", "response_bytes",
                "time_to_first_byte_ms", "tcp_rtt_us", "request_time_ms", "response_status",
                "client_country", "location_id", "client_ip", "user_agent",
            ],
            true,
        )
        // Cast numeric columns
        .with_columns([
            col("resource_id").cast(DataType::Int64),
            col("response_bytes").cast(DataType::Int64),
            col("time_to_first_byte_ms").cast(DataType::Int64),
            col("response_status").cast(DataType::Int32),
        ]);

    let lf = lf
        .filter(
            col("timestamp").is_not_null()
                .and(col("request_path").is_not_null())
        )
        .with_column(
            col("timestamp")
                .str()
                .replace(lit("Z"), lit("+00:00"), true)
                .str()
                .to_datetime(
                    Some(TimeUnit::Milliseconds),
                    None,
                    StrptimeOptions::default(),
                    lit("raise"),
                )
                .alias("timestamp"),
        )
        .with_column(
            col("request_path")
                .str()
                .extract(lit(r"^/([a-f0-9]+)/"), 1)
                .alias("stream_id"),
        )
        .filter(col("stream_id").is_not_null())
        .with_column(
            col("user_agent")
                .fill_null(lit(""))
                .str()
                .to_lowercase()
                .alias("ua_lower"),
        )
        .with_column(device_type_expr().alias("device_type"))
        .with_columns([
            col("timestamp")
                .dt()
                .strftime("%d/%b/%Y %H:%M")
                .alias("minute_key"),
            col("request_path")
                .str()
                .extract(lit(r"/(tracks-[^/]+)/"), 1)
                .str()
                .replace(lit(r"^tracks-"), lit(""), false)
                .fill_null(lit("unknown"))
                .alias("track"),
        ])
        .with_columns([
            col("client_country").fill_null(lit("XX")),
            col("location_id").fill_null(lit("unknown")),
            col("client_ip").fill_null(lit("0.0.0.0")),
            col("resource_id").fill_null(lit(0i64)),
            col("response_bytes").fill_null(lit(0i64)),
            col("time_to_first_byte_ms").fill_null(lit(0i64)),
            col("response_status").fill_null(lit(0i32)),
        ])
        .select([
            col("timestamp"),
            col("stream_id"),
            col("resource_id"),
            col("cache_status"),
            col("response_bytes"),
            col("time_to_first_byte_ms"),
            col("response_status"),
            col("client_country"),
            col("location_id"),
            col("client_ip"),
            col("device_type"),
            col("request_path"),
            col("minute_key"),
            col("track"),
        ]);

    lf.collect().expect("Failed to collect DataFrame")
}
