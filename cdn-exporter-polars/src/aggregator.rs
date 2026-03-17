use ahash::AHashMap;
use polars::prelude::*;

use crate::types::MetricResult;

const METRIC_PREFIX: &str = "cdn_";

/// Aggregate a DataFrame into metric results using Polars group_by operations.
pub fn aggregate_events(df: &DataFrame) -> Vec<MetricResult> {
    let mut results = Vec::new();
    let lf = df.clone().lazy();

    // 1. transfer_bytes_total (SUM response_bytes)
    results.extend(group_sum(
        &lf,
        "response_bytes",
        &format!("{METRIC_PREFIX}transfer_bytes_total"),
        &["cache_status", "resource_id", "location_id", "minute_key", "stream_id"],
        &[("cache_status", "cache_status"), ("cdn_id", "resource_id"),
          ("pop", "location_id"), ("stream", "stream_id")],
    ));

    // 2. time_to_first_byte_ms (AVG where > 0)
    let ttfb_lf = lf.clone().filter(col("time_to_first_byte_ms").gt(lit(0)));
    results.extend(group_avg(
        &ttfb_lf,
        "time_to_first_byte_ms",
        &format!("{METRIC_PREFIX}time_to_first_byte_ms"),
        &["cache_status", "resource_id", "location_id", "minute_key", "stream_id"],
        &[("cache_status", "cache_status"), ("cdn_id", "resource_id"),
          ("pop", "location_id"), ("stream", "stream_id")],
    ));

    // 3. requests_total (COUNT)
    results.extend(group_count(
        &lf,
        &format!("{METRIC_PREFIX}requests_total"),
        &["cache_status", "resource_id", "location_id", "minute_key", "stream_id"],
        &[("cache_status", "cache_status"), ("cdn_id", "resource_id"),
          ("pop", "location_id"), ("stream", "stream_id")],
    ));

    // 4. responses_total (COUNT, adds response_status)
    results.extend(group_count(
        &lf,
        &format!("{METRIC_PREFIX}responses_total"),
        &["cache_status", "resource_id", "location_id", "minute_key", "response_status", "stream_id"],
        &[("cache_status", "cache_status"), ("cdn_id", "resource_id"),
          ("pop", "location_id"), ("response_status", "response_status"), ("stream", "stream_id")],
    ));

    // 5. users_total (UNIQUE_COUNT of client_ip)
    results.extend(group_nunique(
        &lf,
        "client_ip",
        &format!("{METRIC_PREFIX}users_total"),
        &["minute_key", "stream_id"],
        &[("stream", "stream_id")],
    ));

    // 6. users_by_device_total
    results.extend(group_nunique(
        &lf,
        "client_ip",
        &format!("{METRIC_PREFIX}users_by_device_total"),
        &["device_type", "minute_key", "stream_id"],
        &[("device_type", "device_type"), ("stream", "stream_id")],
    ));

    // 7. users_by_country_total
    results.extend(group_nunique(
        &lf,
        "client_ip",
        &format!("{METRIC_PREFIX}users_by_country_total"),
        &["client_country", "minute_key", "stream_id"],
        &[("country", "client_country"), ("stream", "stream_id")],
    ));

    // 8. users_by_resolution_total (video tracks only)
    let video_lf = lf.filter(col("track").str().starts_with(lit("v")));
    results.extend(group_nunique(
        &video_lf,
        "client_ip",
        &format!("{METRIC_PREFIX}users_by_resolution_total"),
        &["minute_key", "stream_id", "track"],
        &[("stream", "stream_id"), ("track", "track")],
    ));

    results
}

fn df_to_results(
    agg_df: DataFrame,
    metric_name: &str,
    label_map: &[(&str, &str)],
) -> Vec<MetricResult> {
    let mut results = Vec::with_capacity(agg_df.height());

    let minute_col = agg_df.column("minute_key").unwrap().str().unwrap();
    let value_col = agg_df.column("value").unwrap().f64().unwrap();

    // Pre-fetch label columns
    let label_cols: Vec<(&str, &ChunkedArray<StringType>)> = label_map
        .iter()
        .map(|(label_name, col_name)| {
            (*label_name, agg_df.column(col_name).unwrap().str().unwrap())
        })
        .collect();

    for i in 0..agg_df.height() {
        let minute_key = minute_col.get(i).unwrap_or("").to_string();
        let value = value_col.get(i).unwrap_or(0.0);

        let mut labels = AHashMap::new();
        for (label_name, col_data) in &label_cols {
            labels.insert(
                label_name.to_string(),
                col_data.get(i).unwrap_or("").to_string(),
            );
        }
        labels.insert("minute".to_string(), minute_key.clone());

        results.push(MetricResult {
            metric_name: metric_name.to_string(),
            labels,
            value,
            minute_key,
        });
    }

    results
}

fn group_sum(
    lf: &LazyFrame,
    value_col: &str,
    metric_name: &str,
    group_cols: &[&str],
    label_map: &[(&str, &str)],
) -> Vec<MetricResult> {
    let group_exprs: Vec<Expr> = group_cols.iter().map(|&c| col(c).cast(DataType::String)).collect();
    let agg_df = lf.clone()
        .group_by(group_exprs)
        .agg([col(value_col).cast(DataType::Float64).sum().alias("value")])
        .collect()
        .unwrap_or_default();
    df_to_results(agg_df, metric_name, label_map)
}

fn group_avg(
    lf: &LazyFrame,
    value_col: &str,
    metric_name: &str,
    group_cols: &[&str],
    label_map: &[(&str, &str)],
) -> Vec<MetricResult> {
    let group_exprs: Vec<Expr> = group_cols.iter().map(|&c| col(c).cast(DataType::String)).collect();
    let agg_df = lf.clone()
        .group_by(group_exprs)
        .agg([col(value_col).cast(DataType::Float64).mean().alias("value")])
        .collect()
        .unwrap_or_default();
    df_to_results(agg_df, metric_name, label_map)
}

fn group_count(
    lf: &LazyFrame,
    metric_name: &str,
    group_cols: &[&str],
    label_map: &[(&str, &str)],
) -> Vec<MetricResult> {
    let group_exprs: Vec<Expr> = group_cols.iter().map(|&c| col(c).cast(DataType::String)).collect();
    let agg_df = lf.clone()
        .group_by(group_exprs)
        .agg([len().cast(DataType::Float64).alias("value")])
        .collect()
        .unwrap_or_default();
    df_to_results(agg_df, metric_name, label_map)
}

fn group_nunique(
    lf: &LazyFrame,
    count_col: &str,
    metric_name: &str,
    group_cols: &[&str],
    label_map: &[(&str, &str)],
) -> Vec<MetricResult> {
    let group_exprs: Vec<Expr> = group_cols.iter().map(|&c| col(c).cast(DataType::String)).collect();
    let agg_df = lf.clone()
        .group_by(group_exprs)
        .agg([col(count_col).n_unique().cast(DataType::Float64).alias("value")])
        .collect()
        .unwrap_or_default();
    df_to_results(agg_df, metric_name, label_map)
}
