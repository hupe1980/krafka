---
layout: default
title: Metrics & Observability
nav_order: 8
description: "Metrics collection and Prometheus export"
---

# Metrics & Observability Guide

This guide covers metrics collection and export in Krafka.

## Overview

Krafka provides built-in metrics collection for:

- **Producer metrics**: Records sent, bytes, batches, errors, retries, latency
- **Consumer metrics**: Records received, polls, fetches, commits, lag, latency
- **Connection metrics**: Connections created/closed, errors, establishment latency

All metrics are lock-free using atomic operations for minimal performance impact on hot paths.

## Basic Usage

Each client type has its own metrics:

```rust
use krafka::metrics::{ProducerMetrics, ConsumerMetrics, ConnectionMetrics};

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

Krafka provides built-in Prometheus text format export without external dependencies:

```rust
use krafka::metrics::{ProducerMetrics, MetricsExport};

let metrics = ProducerMetrics::new();
metrics.record_send(100);
metrics.record_batch(5);

// Export in Prometheus text format
let prometheus_output = metrics.to_prometheus_text("krafka_producer");
println!("{}", prometheus_output);
```

Example output:

```text
# HELP krafka_producer_records_sent_total Total number of records sent
# TYPE krafka_producer_records_sent_total counter
krafka_producer_records_sent_total 6
# HELP krafka_producer_bytes_sent_total Total bytes sent
# TYPE krafka_producer_bytes_sent_total counter
krafka_producer_bytes_sent_total 100
# HELP krafka_producer_batches_sent_total Total batches sent
# TYPE krafka_producer_batches_sent_total counter
krafka_producer_batches_sent_total 1
# HELP krafka_producer_send_latency_seconds Send latency
# TYPE krafka_producer_send_latency_seconds summary
krafka_producer_send_latency_seconds_count 0
krafka_producer_send_latency_seconds_sum 0.000000000
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
| `connections` | Gauge | Current active connections |
| `buffered_records` | Gauge | Currently buffered records |
| `send_latency_seconds` | Summary | Send latency statistics |

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
| `lag` | Gauge | Current consumer lag |
| `assigned_partitions` | Gauge | Currently assigned partitions |
| `paused_partitions` | Gauge | Currently paused partitions |
| `poll_latency_seconds` | Summary | Poll latency statistics |
| `fetch_latency_seconds` | Summary | Fetch latency statistics |

### Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `connections_created_total` | Counter | Total connections created |
| `connections_closed_total` | Counter | Total connections closed |
| `connection_errors_total` | Counter | Connection errors |
| `active_connections` | Gauge | Current active connections |
| `connect_latency_seconds` | Summary | Connection establishment latency |

## Latency Tracking

The `LatencyTracker` provides detailed latency statistics:

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

While Krafka doesn't have a built-in OpenTelemetry integration, you can use the Prometheus
exporter with OpenTelemetry's Prometheus receiver, or export snapshots directly:

```rust
use krafka::metrics::{KrafkaMetrics, ProducerMetricsSnapshot};

fn export_to_otel(snapshot: &ProducerMetricsSnapshot) {
    // Use your OpenTelemetry SDK to record metrics
    // meter.create_counter("krafka.records_sent").add(snapshot.records_sent);
    // etc.
}
```

## Performance Considerations

- All metrics use atomic operations (lock-free)
- Counter increments use `Ordering::Relaxed` for minimal overhead
- Latency tracking uses compare-and-swap for min/max updates
- Gauge updates are immediate (no aggregation)
- Prometheus export only happens on request (pull-based)

## Next Steps

- [Producer Guide](producer.md) - Configure producer metrics
- [Consumer Guide](consumer.md) - Configure consumer metrics
- [Configuration Reference](configuration.md) - All configuration options
