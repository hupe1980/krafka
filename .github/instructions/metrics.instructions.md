---
applyTo: "src/metrics.rs"
description: "Use when editing metrics: the Metrics snapshot, its recorders, Prometheus and KIP-714 names."
---

# Metrics Module Rules

## Adding a New Metric

Every new metric appears in:

1. A `pub(crate)` field on the recorder (`ProducerRecorder`, `ConsumerRecorder` or `ConnectionRecorder`) that the client writes.
2. The public section of the snapshot (`ProducerMetrics`, `ConsumerMetrics` or `ConnectionMetrics`), filled in the recorder's `snapshot()`.
3. The section's `add()`, which `Kafka::metrics()` sums with: counters add, gauges add or take the max.
4. `Metrics::scalars()` (or `latencies()`), with its Prometheus name (`krafka_*`), its KIP-714 name (dotted, Java's where Java has the same metric) and its help text. `prometheus_text()` and the telemetry reporter both read that list.
5. `site/content/docs/metrics.md`, in the right table.

## Rules

- There is one snapshot type, `Metrics`; there is no registry and no exporter trait. Recorders are crate-private.
- Counters are monotonic for a client's lifetime; there is no reset. The KIP-714 reporter computes deltas from successive snapshots.
- `Gauge::dec` saturates at zero and logs a `warn!`: reaching it is an unmatched inc/dec pair.
- Gauges store `u64`; clamp `i64` arithmetic (`.max(0) as u64`).
- Snapshot structs are `#[non_exhaustive]`.
