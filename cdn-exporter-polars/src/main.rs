mod aggregator;
mod device;
mod event;
mod session;
mod types;
mod viewer;

use ahash::AHashMap;
use chrono::{DateTime, Utc};
use std::io::Read;
use std::time::Instant;

use types::{BenchmarkOutput, StageTiming};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let input = if args.len() > 1 && args[1] != "-" {
        std::fs::read(&args[1]).expect("Failed to read input file")
    } else {
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf).expect("Failed to read stdin");
        buf
    };

    let total_start = Instant::now();
    let mut stages: AHashMap<String, StageTiming> = AHashMap::new();

    // 1. Parse NDJSON via Polars native reader
    let parse_start = Instant::now();
    let df = event::parse_ndjson(&input);
    let parse_time = parse_start.elapsed().as_secs_f64();
    let event_count = df.height();

    stages.insert("parse".to_string(), StageTiming {
        wall_time_s: parse_time,
        event_count: Some(event_count),
        result_series: None,
        gauge_metrics: None,
        total_sessions: None,
        viewer_metrics: None,
    });

    // 2. Aggregate via Polars group_by
    let agg_start = Instant::now();
    let agg_results = aggregator::aggregate_events(&df);
    let agg_time = agg_start.elapsed().as_secs_f64();

    stages.insert("aggregate".to_string(), StageTiming {
        wall_time_s: agg_time,
        event_count: None,
        result_series: Some(agg_results.len()),
        gauge_metrics: None,
        total_sessions: None,
        viewer_metrics: None,
    });

    // 3. Session tracking (DataFrame → row iteration, inherently stateful)
    let session_start = Instant::now();
    let mut tracker = session::SessionTracker::new(7200, 120);
    tracker.update_from_df(&df);

    // Get max timestamp from DataFrame
    let ts_col = df.column("timestamp").unwrap().datetime().unwrap();
    let max_ts_ms = ts_col.into_iter().filter_map(|v| v).max().unwrap_or(0);
    let ref_time: DateTime<Utc> = DateTime::from_timestamp_millis(max_ts_ms)
        .unwrap_or_else(Utc::now)
        .with_timezone(&Utc);

    tracker.expire_old(ref_time);
    let gauge_metrics = tracker.get_gauge_metrics(ref_time);
    let session_time = session_start.elapsed().as_secs_f64();

    stages.insert("session".to_string(), StageTiming {
        wall_time_s: session_time,
        event_count: None,
        result_series: None,
        gauge_metrics: Some(gauge_metrics.len()),
        total_sessions: Some(tracker.total_sessions()),
        viewer_metrics: None,
    });

    // 4. Viewer metrics via Polars group_by
    let viewer_start = Instant::now();
    let timestamp_ms = ref_time.timestamp_millis();
    let viewer_results = viewer::build_file_viewer_metrics(&df, timestamp_ms);
    let viewer_time = viewer_start.elapsed().as_secs_f64();

    stages.insert("viewer".to_string(), StageTiming {
        wall_time_s: viewer_time,
        event_count: None,
        result_series: None,
        gauge_metrics: None,
        total_sessions: None,
        viewer_metrics: Some(viewer_results.len()),
    });

    let total_time = total_start.elapsed().as_secs_f64();

    let output = BenchmarkOutput {
        stages,
        total_wall_time_s: total_time,
        aggregation_results: agg_results,
        viewer_metrics: viewer_results,
    };

    let json = serde_json::to_string(&output).expect("Failed to serialize output");
    println!("{}", json);

    eprintln!("cdn-exporter-polars: {event_count} events in {total_time:.3}s");
    eprintln!(
        "  parse: {parse_time:.3}s | aggregate: {agg_time:.3}s | session: {session_time:.3}s | viewer: {viewer_time:.3}s"
    );
}
