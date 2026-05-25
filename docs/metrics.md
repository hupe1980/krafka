---
layout: default
title: Metrics & Observability
nav_order: 8
description: "Metrics collection and Prometheus export"
---

# Metrics & Observability Guide

This guide covers metrics collection and export in Krafka.

## Overview

Krafka provides built-in metrics collection that is automatically wired into all hot paths:

- **Producer metrics**: Records sent, bytes, batches, errors, retries, send latency — recorded in `send()`, `send_to_partition()`, and batch accumulator flush
- **Consumer metrics**: Records received, polls, fetches, commits, rebalances, seeks, assigned partitions, lag, poll latency — recorded in `poll()`, `commit()`, `seek()`, `seek_many()`, and `close()`
- **Connection metrics**: Connections created/closed, errors, establishment latency, priority scheduling counters, and broker throttle delays

All metrics are lock-free using atomic operations for minimal performance impact. Access metrics via `producer.metrics_handle()`, `consumer.metrics()`, or the connection metric handles exposed by producer, consumer, share-consumer, admin, and connection-pool APIs.

## Pluggable Export

Krafka uses a trait-based export system. Implement `MetricsExporter` to add any
backend. Built-in exporters:

| Exporter | Format | Dependency |
|----------|--------|------------|
| `PrometheusExporter` | Prometheus text exposition | None |
| `JsonExporter` | JSON array of metric objects | None |
| `OtlpExporter` | OTLP MetricsData v1 protobuf | `telemetry` feature |

### Custom Exporter

```rust
use krafka::metrics::{MetricsExporter, LatencySnapshot};

struct StatsDExporter { /* ... */ }

impl MetricsExporter for StatsDExporter {
    fn export_counter(&mut self, name: &str, _help: &str, value: u64) {
        // send_udp(format!("{name}:{value}|c"));
    }
    fn export_gauge(&mut self, name: &str, _help: &str, value: u64) {
        // send_udp(format!("{name}:{value}|g"));
    }
    fn export_latency(&mut self, name: &str, _help: &str, snapshot: &LatencySnapshot) {
        // send_udp(format!("{name}.count:{0}|g", snapshot.count));
    }
}
```

## Basic Usage

Each client type has its own metrics:

```rust
use krafka::metrics::{ProducerMetrics, ConsumerMetrics, ConnectionMetrics, MetricsVisitable};

let producer_metrics = ProducerMetrics::new();
let consumer_metrics = ConsumerMetrics::new();
let connection_metrics = ConnectionMetrics::new();

// Record some metrics
producer_metrics.record_send(100);
producer_metrics.send_latency.record(std::time::Duration::from_millis(5));

// Get a snapshot
let snapshot = producer_metrics.snapshot();
println!("Records sent: {}", snapshot.records_sent);
println!("Bytes sent: {}", snapshot.bytes_sent);
```

## Prometheus Export

```rust
use krafka::metrics::{ProducerMetrics, MetricsVisitable};

let metrics = ProducerMetrics::new();
metrics.record_send(100);
metrics.record_batch(5);

// Export in Prometheus text format (convenience method)
let prometheus_output = metrics.to_prometheus_text("krafka_producer");
println!("{}", prometheus_output);
```

Or use the exporter directly:

```rust
use krafka::metrics::{ProducerMetrics, PrometheusExporter, MetricsVisitable};

let metrics = ProducerMetrics::new();
let mut exporter = PrometheusExporter::new();
metrics.export_metrics("krafka_producer", &mut exporter);
let output = exporter.finish();
```

## JSON Export

```rust
use krafka::metrics::{ProducerMetrics, JsonExporter, MetricsVisitable};

let metrics = ProducerMetrics::new();
metrics.record_send(100);

let mut exporter = JsonExporter::new();
metrics.export_metrics("krafka_producer", &mut exporter);
let json = exporter.finish();
// [{"name":"krafka_producer_records_sent","type":"counter","help":"Total records sent","value":1}, ...]
```

## Aggregated Metrics

Use `KrafkaMetrics` to collect and export all metrics from multiple components:

```rust
use std::sync::Arc;
use krafka::metrics::KrafkaMetrics;

let metrics = KrafkaMetrics::new();

// Get shared metrics handles for your clients
let producer_metrics = metrics.producer_metrics();
let consumer_metrics = metrics.consumer_metrics();
let connection_metrics = metrics.connection_metrics();

// Record metrics during operations
producer_metrics.record_send(100);
consumer_metrics.record_poll(5);

// Export all metrics in a single call
let all_metrics = metrics.to_prometheus_text();
println!("{}", all_metrics);

// Export as JSON
let json = metrics.to_json();

// Use a custom exporter
use krafka::metrics::PrometheusExporter;
let mut exporter = PrometheusExporter::new();
metrics.export_all(&mut exporter);
let output = exporter.finish();

// Reset all metrics (e.g., after scrape)
metrics.reset();
```

## HTTP Metrics Endpoint

For production use, expose metrics via HTTP:

```rust
use std::sync::Arc;
use krafka::metrics::KrafkaMetrics;

// Create shared metrics registry
let metrics = Arc::new(KrafkaMetrics::new());

// In your HTTP server handler (pseudo-code):
async fn metrics_handler(metrics: Arc<KrafkaMetrics>) -> String {
    metrics.to_prometheus_text()
}
```

Example with Axum:

```rust
use axum::{routing::get, Router, Extension};
use std::sync::Arc;
use krafka::metrics::KrafkaMetrics;

async fn metrics_handler(Extension(metrics): Extension<Arc<KrafkaMetrics>>) -> String {
    metrics.to_prometheus_text()
}

#[tokio::main]
async fn main() {
    let metrics = Arc::new(KrafkaMetrics::new());
    
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .layer(Extension(metrics.clone()));
    
    // Use metrics.producer_metrics() etc. with your Kafka clients
}
```

## Available Metrics

### Producer Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `records_sent_total` | Counter | Total records sent successfully |
| `bytes_sent_total` | Counter | Total bytes sent (record values) |
| `batches_sent_total` | Counter | Total batches sent |
| `errors_total` | Counter | Total send errors |
| `retries_total` | Counter | Total retry attempts |
| `compressed_bytes_total` | Counter | Total compressed bytes written for compressed batches |
| `uncompressed_bytes_total` | Counter | Total uncompressed bytes for the same compressed batches |
| `connections` | Gauge | Current active connections |
| `buffered_records` | Gauge | Producer records currently admitted under the memory budget |
| `send_latency_seconds` | Summary | Send latency statistics |

`compression_ratio_avg` is available as a derived field in `ProducerMetricsSnapshot` (computed as `compressed_bytes / uncompressed_bytes`). A value of 0.3 means the codec reduced data to 30% of original size. The field is `None` when no compressed batches have been sent.

### Consumer Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `records_received_total` | Counter | Total records received |
| `bytes_received_total` | Counter | Total bytes received |
| `fetches_total` | Counter | Total fetch requests |
| `polls_total` | Counter | Total poll operations |
| `empty_polls_total` | Counter | Polls that returned no records |
| `commits_total` | Counter | Total offset commits |
| `errors_total` | Counter | Total errors |
| `rebalances_total` | Counter | Total rebalance operations |
| `seeks_total` | Counter | Total seek operations (seek + seek_many partition count) |
| `lag` | Gauge | Total consumer lag across all assigned partitions |
| `lag_max` | Gauge | Maximum per-partition consumer lag |
| `assigned_partitions` | Gauge | Currently assigned partitions |
| `paused_partitions` | Gauge | Currently paused partitions |
| `buffered_records` | Gauge | Currently buffered records in recv() buffer |
| `poll_latency_seconds` | Summary | Poll latency statistics |
| `fetch_latency_seconds` | Summary | Fetch latency statistics |

### Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `connections_created_total` | Counter | Total connections created |
| `connections_closed_total` | Counter | Total connections closed |
| `connection_errors_total` | Counter | Connection errors |
| `high_priority_requests_total` | Counter | High-priority requests sent |
| `normal_priority_requests_total` | Counter | Normal-priority requests sent |
| `high_priority_bypasses_total` | Counter | High-priority requests processed ahead of normal-priority work |
| `high_priority_bypass_yields_total` | Counter | Forced normal-priority drain steps after exhausting the high-priority bypass budget |
| `throttle_delays_total` | Counter | Normal-priority requests delayed due to broker throttling |
| `throttle_delay_ms_total` | Counter | Total broker-throttle delay applied to normal-priority requests, in milliseconds |
| `active_connections` | Gauge | Current active connections |
| `connect_latency_seconds` | Summary | Connection establishment latency |
| `tls_handshake_latency_seconds` | Summary | TLS handshake latency (populated for TLS connections only) |

## Latency Tracking

The `LatencyTracker` provides detailed latency statistics:

> **Accuracy note — percentile estimates:** `LatencyTracker` uses a 256-sub-bucket
> histogram (4 equal sub-buckets per power-of-2 band). The percentile estimate is
> the midpoint of the matching sub-bucket, giving a **maximum relative error of
> ≤ 12.5 %** per sub-bucket. In practice:
>
> | True p99        | Sub-bucket width | Max error |
> |-----------------|-----------------|-----------|
> | 1 ms – 2 ms     | 250 µs          | 12.5 %    |
> | 8 ms – 16 ms    | 2 ms            | 12.5 %    |
> | 64 ms – 128 ms  | 16 ms           | 12.5 %    |
>
> This is suitable for p99 SLO alerting with a threshold tolerance of ≥ 15 %.
> For sub-millisecond or tighter requirements, use the OTLP exporter
> and aggregate into an HDR histogram or T-Digest in your observability backend.

```rust
use krafka::metrics::LatencyTracker;
use std::time::Duration;

let tracker = LatencyTracker::new();

// Manual recording
tracker.record(Duration::from_millis(50));
tracker.record(Duration::from_millis(100));

// Or use guard for automatic timing
{
    let _guard = tracker.start();
    // ... operation being timed ...
} // Guard records latency when dropped

// Get statistics
println!("Count: {}", tracker.count());
println!("Min: {:?}", tracker.min());
println!("Max: {:?}", tracker.max());
println!("Avg: {:?}", tracker.avg());
println!("Sum: {:?}", tracker.sum());

// Get immutable snapshot
let snapshot = tracker.snapshot();
```

## Integration with OpenTelemetry

### Built-in OTLP Export (feature `telemetry`)

Enable the `telemetry` feature for native OTLP protobuf export and KIP-714 broker telemetry:

```toml
[dependencies]
krafka = { version = "...", features = ["telemetry"] }
```

Export metrics as OTLP protobuf bytes for ingestion by any OTLP-compatible backend:

```rust
use krafka::telemetry::otlp::OtlpExporter;
use krafka::metrics::{KrafkaMetrics, MetricsVisitable};

let metrics = KrafkaMetrics::new();
// ... record metrics ...

let mut exporter = OtlpExporter::new(true, 0); // delta temporality
exporter.add_resource_attribute("service.name", "my-service");
metrics.export_all(&mut exporter);
let otlp_bytes: Vec<u8> = exporter.finish();
// Send otlp_bytes to your OTLP receiver via gRPC or HTTP
```

### KIP-714 Automatic Telemetry

The `TelemetryReporter` implements KIP-714 client telemetry — it subscribes to the
broker's telemetry endpoint and pushes metric snapshots on the broker-specified interval:

```rust
use krafka::telemetry::reporter::{TelemetryReporter, TelemetryConfig};

let config = TelemetryConfig {
    enabled: true,
    metrics_prefix: "krafka".into(),
    resource_attributes: vec![
        ("service.name".into(), "my-app".into()),
    ],
};

let reporter = TelemetryReporter::new(connection, krafka_metrics, config, shutdown_rx);
tokio::spawn(reporter.run());
```

The reporter handles subscription polling, push interval jitter, local OTLP payload
chunking under the broker's `TelemetryMaxBytes` limit, re-subscription on
`UNKNOWN_SUBSCRIPTION_ID` or unsplittable oversized metrics, and a graceful terminating
push on shutdown. When the broker advertises accepted compression codecs, the reporter
tries them in broker preference order, skips locally unavailable codecs after the first
failure, and only uses uncompressed payloads when the broker explicitly advertises
`Compression::None`; otherwise the reporter stops if none of the advertised codecs is
locally usable. If a multi-chunk push is only partially accepted, the reporter commits
delta baselines for the accepted chunks and retries the exact remaining chunk slice on the
next interval.

### Manual Bridge to External OTel SDKs

You can also bridge metrics to an external OpenTelemetry SDK using snapshots or a custom exporter:

```rust
use krafka::metrics::{KrafkaMetrics, ProducerMetricsSnapshot};

fn export_to_otel(snapshot: &ProducerMetricsSnapshot) {
    // Use your OpenTelemetry SDK to record metrics
    // meter.create_counter("krafka.records_sent").add(snapshot.records_sent);
}
```

## Performance Considerations

- All metrics use atomic operations (lock-free)
- Counter increments use `Ordering::Relaxed` for minimal overhead
- Latency tracking uses compare-and-swap for min/max updates
- Gauge updates are immediate (no aggregation)
- Gauge `dec()` saturates at zero (will not underflow below 0), ensuring correctness for connection and partition counting
- Prometheus and JSON export only happen on request (pull-based)
- OTLP protobuf encoding is zero-copy where possible; no external protobuf dependency
- KIP-714 telemetry push runs on a background task with broker-controlled intervals

## Next Steps

- [Producer Guide](producer.md) - Configure producer metrics
- [Consumer Guide](consumer.md) - Configure consumer metrics
- [Configuration Reference](configuration.md) - All configuration options
