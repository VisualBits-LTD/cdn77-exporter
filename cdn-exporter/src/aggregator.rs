use ahash::{AHashMap, AHashSet};

use crate::types::{Event, MetricResult};

const METRIC_PREFIX: &str = "cdn_";

/// Accumulator for different aggregation types.
enum Accumulator {
    Sum(f64),
    Avg { sum: f64, count: u64 },
    UniqueCount(AHashSet<String>),
}

/// Label key for grouping — a fixed-size tuple serialized as a Vec for hashing.
type LabelKey = Vec<String>;

struct MetricDef {
    name: &'static str,
    /// Returns (should_include, value, unique_key) for this event.
    extractor: fn(&Event) -> Option<AccValue>,
    label_extractors: Vec<(&'static str, fn(&Event) -> String)>,
}

enum AccValue {
    Numeric(f64),
    Unique(String),
}

fn metric_defs() -> Vec<MetricDef> {
    vec![
        // 1. transfer_bytes_total (SUM)
        MetricDef {
            name: "transfer_bytes_total",
            extractor: |e| Some(AccValue::Numeric(e.response_bytes as f64)),
            label_extractors: vec![
                ("cache_status", |e| e.cache_status.clone()),
                ("cdn_id", |e| e.resource_id.to_string()),
                ("pop", |e| e.location_id.clone()),
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 2. time_to_first_byte_ms (AVG, skip <= 0)
        MetricDef {
            name: "time_to_first_byte_ms",
            extractor: |e| {
                if e.time_to_first_byte_ms > 0 {
                    Some(AccValue::Numeric(e.time_to_first_byte_ms as f64))
                } else {
                    None
                }
            },
            label_extractors: vec![
                ("cache_status", |e| e.cache_status.clone()),
                ("cdn_id", |e| e.resource_id.to_string()),
                ("pop", |e| e.location_id.clone()),
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 3. requests_total (SUM of 1.0)
        MetricDef {
            name: "requests_total",
            extractor: |_| Some(AccValue::Numeric(1.0)),
            label_extractors: vec![
                ("cache_status", |e| e.cache_status.clone()),
                ("cdn_id", |e| e.resource_id.to_string()),
                ("pop", |e| e.location_id.clone()),
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 4. responses_total (SUM of 1.0, adds response_status)
        MetricDef {
            name: "responses_total",
            extractor: |_| Some(AccValue::Numeric(1.0)),
            label_extractors: vec![
                ("cache_status", |e| e.cache_status.clone()),
                ("cdn_id", |e| e.resource_id.to_string()),
                ("pop", |e| e.location_id.clone()),
                ("response_status", |e| e.response_status.to_string()),
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 5. users_total (UNIQUE_COUNT of client_ip)
        MetricDef {
            name: "users_total",
            extractor: |e| Some(AccValue::Unique(e.client_ip.clone())),
            label_extractors: vec![
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 6. users_by_device_total
        MetricDef {
            name: "users_by_device_total",
            extractor: |e| Some(AccValue::Unique(e.client_ip.clone())),
            label_extractors: vec![
                ("device_type", |e| e.device_type.clone()),
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 7. users_by_country_total
        MetricDef {
            name: "users_by_country_total",
            extractor: |e| Some(AccValue::Unique(e.client_ip.clone())),
            label_extractors: vec![
                ("country", |e| e.client_country.clone()),
                ("stream", |e| e.stream_id.clone()),
            ],
        },
        // 8. users_by_resolution_total (video tracks only)
        MetricDef {
            name: "users_by_resolution_total",
            extractor: |e| {
                if e.track.starts_with('v') {
                    Some(AccValue::Unique(e.client_ip.clone()))
                } else {
                    None
                }
            },
            label_extractors: vec![
                ("stream", |e| e.stream_id.clone()),
                ("track", |e| e.track.clone()),
            ],
        },
    ]
}

/// Aggregate events into metric results — replaces Python MetricAggregator.
pub fn aggregate_events(events: &[Event]) -> Vec<MetricResult> {
    let defs = metric_defs();
    let mut all_results = Vec::new();

    for def in &defs {
        let metric_name = format!("{}{}", METRIC_PREFIX, def.name);
        // Label key order: sorted label names + "minute" (matching Python)
        let mut label_names: Vec<&str> = def.label_extractors.iter().map(|(n, _)| *n).collect();
        label_names.push("minute");
        label_names.sort();

        let is_unique = matches!(def.name,
            "users_total" | "users_by_device_total" | "users_by_country_total" | "users_by_resolution_total"
        );
        let is_avg = def.name == "time_to_first_byte_ms";

        let mut buckets: AHashMap<LabelKey, Accumulator> = AHashMap::new();

        for event in events {
            let acc_value = match (def.extractor)(event) {
                Some(v) => v,
                None => continue,
            };

            // Build label values in sorted key order
            let mut label_map: AHashMap<&str, String> = AHashMap::new();
            for (name, extractor) in &def.label_extractors {
                label_map.insert(name, extractor(event));
            }
            label_map.insert("minute", event.minute_key.clone());

            let key: LabelKey = label_names.iter().map(|n| label_map[n].clone()).collect();

            let acc = buckets.entry(key).or_insert_with(|| {
                if is_unique {
                    Accumulator::UniqueCount(AHashSet::new())
                } else if is_avg {
                    Accumulator::Avg { sum: 0.0, count: 0 }
                } else {
                    Accumulator::Sum(0.0)
                }
            });

            match (&mut *acc, acc_value) {
                (Accumulator::Sum(ref mut total), AccValue::Numeric(v)) => *total += v,
                (Accumulator::Avg { sum, count }, AccValue::Numeric(v)) => {
                    *sum += v;
                    *count += 1;
                }
                (Accumulator::UniqueCount(ref mut set), AccValue::Unique(v)) => {
                    set.insert(v);
                }
                _ => {}
            }
        }

        // Compute final values
        for (key, acc) in buckets {
            let value = match acc {
                Accumulator::Sum(v) => v,
                Accumulator::Avg { sum, count } => {
                    if count > 0 { sum / count as f64 } else { continue }
                }
                Accumulator::UniqueCount(set) => set.len() as f64,
            };

            let mut labels = AHashMap::new();
            for (i, name) in label_names.iter().enumerate() {
                labels.insert(name.to_string(), key[i].clone());
            }

            all_results.push(MetricResult {
                metric_name: metric_name.clone(),
                labels,
                value,
                minute_key: key[label_names.iter().position(|&n| n == "minute").unwrap()].clone(),
            });
        }
    }

    all_results
}
