---
applyTo: "src/metrics.rs"
description: "Use when editing metrics: gauge vs counter semantics, Prometheus export format, snapshot structs, and reset completeness."
---

# Metrics Module Rules

## Adding a New Metric

Every new metric must appear in **all five** locations:

1. Field on the metrics struct (`ConsumerMetrics`, `ProducerMetrics`, or `ConnectionMetrics`)
2. Prometheus text export in `to_prometheus_text()` with correct `# HELP` and `# TYPE`
3. Snapshot struct field + `snapshot()` method
4. `KrafkaMetrics::reset()` — counters via `.reset()`, gauges via `.set(0)`
5. `docs/metrics.md` — in the correct table with type and description

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
