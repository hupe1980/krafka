+++
title = "Interceptors"
description = "Producer and consumer hooks for tracing, redaction, auditing and record rewriting."
weight = 90

[extra]
slug_id = "interceptors"
+++

Interceptors hook into the producer and consumer pipelines at defined points. They are modelled on the Kafka Java client's `ProducerInterceptor` and `ConsumerInterceptor`, including **ordered chains**.

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
pub type InterceptorResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub trait ProducerInterceptor: Send + Sync + fmt::Debug {
    /// Called before a record is sent (before partitioning).
    /// The record can be mutated (e.g., adding headers).
    fn on_send(&self, _record: &mut ProducerRecord) -> InterceptorResult { Ok(()) }

    /// Called after a record is acknowledged or fails.
    /// `error` is `None` on success.
    fn on_acknowledgement(&self, _metadata: &RecordMetadata, _error: Option<&KrafkaError>) -> InterceptorResult { Ok(()) }

    /// Called when the producer is being closed.
    /// Use this to release any resources held by the interceptor.
    fn close(&self) -> InterceptorResult { Ok(()) }
}
```

All methods have default no-op implementations, so you only need to override the hooks you care about.

**Note:** `on_acknowledgement` fires exactly once per record, on success and on
permanent failure alike, at every `linger` setting and on the
`TransactionalProducer` too — there is only one send path to instrument.

### Example: Tracing Headers

```rust
use krafka::interceptor::{InterceptorResult, ProducerInterceptor};
use krafka::producer::{Producer, ProducerRecord, RecordMetadata};
use krafka::error::KrafkaError;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
struct TracingInterceptor;

impl ProducerInterceptor for TracingInterceptor {
    fn on_send(&self, record: &mut ProducerRecord) -> InterceptorResult {
        let trace_id = Uuid::new_v4().to_string();
        record.headers.push(("x-trace-id".to_string(), trace_id.into_bytes()));
        Ok(())
    }

    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&KrafkaError>) -> InterceptorResult {
        match error {
            None => tracing::info!(
                topic = %metadata.topic,
                partition = metadata.partition,
                offset = metadata.offset,
                "record acknowledged"
            ),
            Some(e) => tracing::error!("send failed: {}", e),
        }
        Ok(())
    }
}

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .add_interceptor(Arc::new(TracingInterceptor))
    .build()
    .await?;
```

### Example: Metrics Counter

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor};
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
    fn on_acknowledgement(&self, _metadata: &RecordMetadata, error: Option<&KrafkaError>) -> InterceptorResult {
        if error.is_some() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}
```

## Consumer Interceptor

### Trait Definition

```rust
pub trait ConsumerInterceptor: Send + Sync + fmt::Debug {
    /// Called after records are fetched, before returned to the application.
    fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult { Ok(()) }

    /// Called after offsets are committed.
    /// The map keys are `(topic, partition)` and values are the committed offsets.
    fn on_commit(
        &self,
        _offsets: &HashMap<(String, PartitionId), Offset>,
        _error: Option<&KrafkaError>,
    ) -> InterceptorResult { Ok(()) }

    /// Called when the consumer is being closed.
    /// Use this to release any resources held by the interceptor.
    fn close(&self) -> InterceptorResult { Ok(()) }
}
```

### Example: Consumption Logging

```rust
use krafka::interceptor::{ConsumerInterceptor, InterceptorResult};
use krafka::consumer::{Consumer, ConsumerRecord};
use std::sync::Arc;

#[derive(Debug)]
struct LoggingInterceptor;

impl ConsumerInterceptor for LoggingInterceptor {
    fn on_consume(&self, records: &[ConsumerRecord]) -> InterceptorResult {
        for record in records {
            println!(
                "Consumed: topic={}, partition={}, offset={}",
                record.topic, record.partition, record.offset
            );
        }
        Ok(())
    }
}

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .add_interceptor(Arc::new(LoggingInterceptor))
    .build()
    .await?;
```

### Example: Commit Monitoring

```rust,compile
use krafka::interceptor::{CommitOffsets, ConsumerInterceptor, InterceptorResult};
use krafka::error::KrafkaError;

#[derive(Debug)]
struct CommitMonitor;

impl ConsumerInterceptor for CommitMonitor {
    // `CommitOffsets` is the map type the trait uses. Naming the underlying
    // `std::collections::HashMap` here would not compile — the trait's map is
    // an `ahash::AHashMap`, which is a different type.
    fn on_commit(
        &self,
        offsets: &CommitOffsets,
        error: Option<&KrafkaError>,
    ) -> InterceptorResult {
        match error {
            None => {
                for ((topic, partition), offset) in offsets {
                    println!("Committed {}:{} at offset {}", topic, partition, offset);
                }
            }
            Some(e) => eprintln!("Commit failed: {}", e),
        }
        Ok(())
    }
}
```

## Wiring Interceptors

### Single Interceptor

```rust
use std::sync::Arc;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .interceptor(Arc::new(MyProducerInterceptor))
    .build()
    .await?;
```

### Interceptor Chain

Multiple interceptors execute in the order they are added. Each interceptor is
individually error- and panic-isolated — a failure in one interceptor will
not prevent the remaining interceptors from running.

For `on_send`, each interceptor sees the record as modified by all preceding
interceptors in the chain.

> **Error semantics:** In Java, `onSend` returns a new record — if an
> interceptor throws, the next one receives the record from the last
> *successful* interceptor. In Rust, `on_send` mutates in-place (`&mut`);
> if an interceptor returns an error or panics mid-mutation, the next
> interceptor sees a partially-mutated record. Avoid building chains
> where later interceptors depend on invariants set by earlier ones.

```rust
use std::sync::Arc;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .add_interceptor(Arc::new(TracingInterceptor))
    .add_interceptor(Arc::new(MetricsInterceptor))
    .add_interceptor(Arc::new(AuditInterceptor))
    .build()
    .await?;
```

```rust
use std::sync::Arc;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .add_interceptor(Arc::new(LoggingInterceptor))
    .add_interceptor(Arc::new(MetricsInterceptor))
    .build()
    .await?;
```

> **Note:** `interceptor()` replaces any previously added interceptors with a
> single one. `add_interceptor()` appends to the chain. Don't mix both in the
> same builder.

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

```rust,compile
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct SafeInterceptor {
    counter: AtomicU64,
}

// AtomicU64 is Send + Sync, so SafeInterceptor is too
```

## Security Considerations

- **Headers may contain credentials:** `on_send()` receives all record headers, which
  may include auth tokens or API keys. Do not log full record contents without sanitization.
- **Error messages may leak secrets:** `on_acknowledgement()` error messages from auth
  failures may contain broker-echoed details.
- **Debug impls may expose secrets:** Never log the interceptor instance itself
  (e.g. `{:?}`) — user-provided `Debug` implementations may expose credentials.

## Error Handling & Panic Safety

All interceptor methods return `InterceptorResult` (`Result<(), Box<dyn Error + Send + Sync>>`).
Errors are **non-fatal** — the chain continues and the error is logged at `warn!`.
This gives interceptor authors a clean, idiomatic way to signal failures
(e.g. a metrics backend is down) without resorting to panics.

As a safety net, all calls are additionally wrapped in `catch_unwind`.
Panics are caught and logged at `error!` with the panic payload **redacted**
(user-provided `Debug` impls may leak secrets). The chain continues even after a panic.

| Outcome | Log level | Chain continues? |
|---------|-----------|------------------|
| `Ok(())` | — | Yes |
| `Err(e)` | `warn!` | Yes |
| panic | `error!` (payload redacted) | Yes |

## Next Steps

- [Producer Guide](@/docs/producer.md) - Producer configuration and usage
- [Consumer Guide](@/docs/consumer.md) - Consumer groups and offset management
- [Admin Client](@/docs/admin.md) - Cluster administration
