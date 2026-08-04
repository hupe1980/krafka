---
applyTo: "src/metrics.rs"
description: "Use when editing metrics: gauge vs counter semantics, pluggable exporter traits, snapshot structs, and reset completeness."
---

# Metrics Module Rules

## Adding a New Metric

Every new metric must appear in **all six** locations:

1. Field on the metrics struct (`ConsumerMetrics`, `ProducerMetrics`, or `ConnectionMetrics`)
2. `export_metrics()` impl for `MetricsVisitable` — call the matching `export_counter()`, `export_gauge()`, or `export_latency()` on the exporter
3. Prometheus text export via `PrometheusExporter` (verified by existing `export_metrics` path)
4. Snapshot struct field + `snapshot()` method
5. `KrafkaMetrics::reset()` — counters via `.reset()`, gauges via `.set(0)`
6. `site/content/docs/metrics.md` — in the correct table with type and description

## Pluggable Export Traits

- **`MetricsExporter`** (visitor trait): backends implement `export_counter()`, `export_gauge()`, `export_latency()`. Built-in: `PrometheusExporter`, `JsonExporter`, and `OtlpExporter` (feature `telemetry`).
- **`MetricsVisitable`**: implemented on each metrics struct (`ProducerMetrics`, `ConsumerMetrics`, `ConnectionMetrics`). The `export_metrics(&self, prefix, &mut dyn MetricsExporter)` method drives the visitor.
- Convenience methods: `to_prometheus_text()` and `to_json()` are provided as default impls on `MetricsVisitable`.

When adding a new exporter, implement `MetricsExporter`. When adding a new metric, update the `export_metrics()` impl on the corresponding struct.

## Gauge vs Counter

- **Counter**: monotonically increasing totals (records_sent, errors). Reset via `.reset()`
- **Gauge**: current state that fluctuates (lag, assigned_partitions, connections). Reset via `.set(0)`

`Gauge` stores `u64` internally. When setting from `i64` arithmetic (e.g., lag = hw - pos), always clamp: `.max(0) as u64`.

## Snapshot Structs

All `*MetricsSnapshot` structs are `#[non_exhaustive]`. New fields are non-breaking for downstream consumers but still require updating `snapshot()`.

## Prometheus Format

```
# HELP {prefix}_{name} {description}
# TYPE {prefix}_{name} {counter|gauge|summary}
{prefix}_{name} {value}
```

Latency trackers emit `_seconds_count`, `_seconds_sum`, `_seconds_min`, `_seconds_max`.
