---
layout: default
title: Interceptors
nav_order: 6
description: "Producer and consumer interceptor hooks for observability and record modification"
---

# Interceptors Guide

Interceptors allow you to hook into the producer and consumer pipelines at key points.
They are modeled after the Kafka Java client's `ProducerInterceptor` and `ConsumerInterceptor`
interfaces.

## Overview

| Hook | Pipeline | When |
|------|----------|------|
| `on_send` | Producer | Before a record is partitioned and sent |
| `on_acknowledgement` | Producer | After a record is acknowledged (or fails) |
| `close` | Producer | When the producer is shutting down |
| `on_consume` | Consumer | After records are fetched, before returned to the application |
| `on_commit` | Consumer | After offsets are committed |
| `close` | Consumer | When the consumer is shutting down |

Use cases:
- **Observability**: Count records, measure latency, log errors
- **Record enrichment**: Add tracing headers, inject metadata
- **Auditing**: Track what was produced and consumed
- **Metrics collection**: Feed data into Prometheus, StatsD, etc.

## Producer Interceptor

### Trait Definition

```rust
pub trait ProducerInterceptor: Send + Sync + fmt::Debug {
    /// Called before a record is sent (before partitioning).
    /// The record can be mutated (e.g., adding headers).
    fn on_send(&self, _record: &mut ProducerRecord) {}

    /// Called after a record is acknowledged or fails.
    /// `error` is `None` on success.
    fn on_acknowledgement(&self, _metadata: &RecordMetadata, _error: Option<&KrafkaError>) {}

    /// Called when the producer is being closed.
    /// Use this to release any resources held by the interceptor.
    fn close(&self) {}
}
```

All methods have default no-op implementations, so you only need to override the hooks you care about.

**Note:** `on_acknowledgement` is called for *all* send paths — both the direct send path
(linger = 0) and the batched accumulator path (linger > 0).

### Example: Tracing Headers

```rust
use krafka::interceptor::ProducerInterceptor;
use krafka::producer::{Producer, ProducerRecord, RecordMetadata};
use krafka::error::KrafkaError;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
struct TracingInterceptor;

impl ProducerInterceptor for TracingInterceptor {
    fn on_send(&self, record: &mut ProducerRecord) {
        let trace_id = Uuid::new_v4().to_string();
        record.headers.push(("x-trace-id".to_string(), trace_id.into_bytes()));
    }

    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&KrafkaError>) {
        match error {
            None => tracing::info!(
                topic = %metadata.topic,
                partition = metadata.partition,
                offset = metadata.offset,
                "record acknowledged"
            ),
            Some(e) => tracing::error!("send failed: {}", e),
        }
    }
}

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .interceptor(Arc::new(TracingInterceptor))
    .build()
    .await?;
```

### Example: Metrics Counter

```rust
use krafka::interceptor::ProducerInterceptor;
use krafka::producer::{ProducerRecord, RecordMetadata};
use krafka::error::KrafkaError;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct MetricsInterceptor {
    sent: AtomicU64,
    errors: AtomicU64,
}

impl MetricsInterceptor {
    fn new() -> Self {
        Self {
            sent: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}

impl ProducerInterceptor for MetricsInterceptor {
    fn on_acknowledgement(&self, _metadata: &RecordMetadata, error: Option<&KrafkaError>) {
        if error.is_some() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

## Consumer Interceptor

### Trait Definition

```rust
pub trait ConsumerInterceptor: Send + Sync + fmt::Debug {
    /// Called after records are fetched, before returned to the application.
    fn on_consume(&self, _records: &[ConsumerRecord]) {}

    /// Called after offsets are committed.
    /// The map keys are `(topic, partition)` and values are the committed offsets.
    fn on_commit(
        &self,
        _offsets: &HashMap<(String, PartitionId), Offset>,
        _error: Option<&KrafkaError>,
    ) {}

    /// Called when the consumer is being closed.
    /// Use this to release any resources held by the interceptor.
    fn close(&self) {}
}
```

### Example: Consumption Logging

```rust
use krafka::interceptor::ConsumerInterceptor;
use krafka::consumer::{Consumer, ConsumerRecord};
use std::sync::Arc;

#[derive(Debug)]
struct LoggingInterceptor;

impl ConsumerInterceptor for LoggingInterceptor {
    fn on_consume(&self, records: &[ConsumerRecord]) {
        for record in records {
            println!(
                "Consumed: topic={}, partition={}, offset={}",
                record.topic, record.partition, record.offset
            );
        }
    }
}

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .interceptor(Arc::new(LoggingInterceptor))
    .build()
    .await?;
```

### Example: Commit Monitoring

```rust
use krafka::interceptor::ConsumerInterceptor;
use krafka::error::KrafkaError;
use krafka::{Offset, PartitionId};
use std::collections::HashMap;

#[derive(Debug)]
struct CommitMonitor;

impl ConsumerInterceptor for CommitMonitor {
    fn on_commit(
        &self,
        offsets: &HashMap<(String, PartitionId), Offset>,
        error: Option<&KrafkaError>,
    ) {
        match error {
            None => {
                for ((topic, partition), offset) in offsets {
                    println!("Committed {}:{} at offset {}", topic, partition, offset);
                }
            }
            Some(e) => eprintln!("Commit failed: {}", e),
        }
    }
}
```

## Wiring Interceptors

### Producer

```rust
use std::sync::Arc;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .interceptor(Arc::new(MyProducerInterceptor))
    .build()
    .await?;
```

### Consumer

```rust
use std::sync::Arc;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .interceptor(Arc::new(MyConsumerInterceptor))
    .build()
    .await?;
```

### No Interceptor (Default)

When no interceptor is configured, a no-op implementation is used internally.
There is zero overhead — the no-op methods are inlined away by the compiler.

## Pipeline Integration Points

### Producer Pipeline

```
  send_record(record)
       │
       ▼
  interceptor.on_send(&mut record)     ← Modify record here
       │
       ▼
  partitioner.partition()
       │
       ▼
  encode + send to broker
       │
       ├─ success ─► interceptor.on_acknowledgement(metadata, None)
       │
       └─ failure ─► interceptor.on_acknowledgement(metadata, Some(error))
```

### Consumer Pipeline

```
  poll()
    │
    ▼
  fetch from brokers
    │
    ▼
  interceptor.on_consume(&records)     ← Observe records here
    │
    ▼
  return records to application
    │
    ▼
  commit()
    │
    ▼
  interceptor.on_commit(&offsets, error)  ← Only committed offsets (filtered to assigned partitions)
```

## Thread Safety

Interceptors must implement `Send + Sync + Debug`. Use atomic types or `Mutex`/`RwLock`
for any mutable state:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct SafeInterceptor {
    counter: AtomicU64,
}

// AtomicU64 is Send + Sync, so SafeInterceptor is too
```

## Panic Safety

All interceptor method calls are wrapped in `catch_unwind`. If your interceptor panics,
the panic is caught and logged as an error — it will **not** crash the producer or consumer.
This means interceptors can never bring down the application, though a panicking interceptor
will silently stop executing for that invocation.

## Next Steps

- [Producer Guide](producer.md) - Producer configuration and usage
- [Consumer Guide](consumer.md) - Consumer groups and offset management
- [Admin Client](admin.md) - Cluster administration
