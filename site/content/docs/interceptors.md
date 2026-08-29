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
| `on_acknowledgement` | Producer | After a record reaches its terminal outcome |
| `close` | Producer | When the producer is shutting down |
| `on_consume` | Consumer | After records are fetched, before returned to the application |
| `on_commit` | Consumer | After offsets are committed |
| `close` | Consumer | When the consumer is shutting down |

Use cases:
- **Observability**: Count records, measure latency, log errors
- **Record enrichment**: Add tracing headers, inject metadata
- **Distributed tracing**: Open a span in `on_send`, close it in `on_acknowledgement` — see [Per-Record State](#per-record-state)
- **Auditing**: Track what was produced and consumed
- **Metrics collection**: Feed data into Prometheus, StatsD, etc.

## Producer Interceptor

### Trait Definition

```rust
pub type InterceptorResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub trait ProducerInterceptor: Send + Sync + fmt::Debug {
    /// Called before a record is sent (before partitioning).
    /// The record can be mutated (e.g., adding headers), and `ctx` is this
    /// record's scratch space — see "Per-Record State" below.
    fn on_send(&self, _record: &mut ProducerRecord, _ctx: &mut RecordContext) -> InterceptorResult { Ok(()) }

    /// Called after a record reaches its terminal outcome.
    /// `error` is `None` on success. `headers` is the record's final,
    /// read-only header set. `ctx` is the same context `on_send` saw.
    fn on_acknowledgement(
        &self,
        _metadata: &RecordMetadata,
        _error: Option<&KrafkaError>,
        _headers: &RecordHeaders,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult { Ok(()) }

    /// Called when the producer is being closed.
    /// Use this to release any resources held by the interceptor.
    fn close(&self) -> InterceptorResult { Ok(()) }
}
```

All methods have default no-op implementations, so you only need to override the hooks you care about.

### The pairing guarantee

`on_acknowledgement` fires **exactly once for every record `on_send` observed** —
on success, on permanent failure, and for records rejected before they ever
reach the accumulator (a failing serializer, failed validation, an unrouteable
topic, `max.block.ms` exhausted). It holds at every `linger` setting and on the
`TransactionalProducer`, and dropping the `DeliveryHandle` does not suppress it:
the handle discards the *caller's* view of the acknowledgement, not the
interceptor's. The one exception is a panic inside krafka's own send task.

A record that failed before it was routed reports `DeliveryConfirmation::Failed`,
offset `-1` and `krafka::producer::UNKNOWN_PARTITION`.

That is what makes it safe to hold a span, a timer or a permit across the two
callbacks.

### Example: Tracing Headers

```rust
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::producer::{Producer, ProducerRecord, RecordHeaders, RecordMetadata};
use krafka::error::KrafkaError;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
struct TracingInterceptor;

impl ProducerInterceptor for TracingInterceptor {
    fn on_send(&self, record: &mut ProducerRecord, _ctx: &mut RecordContext) -> InterceptorResult {
        let trace_id = Uuid::new_v4().to_string();
        record.headers.push(("x-trace-id".to_string(), Some(trace_id.into_bytes().into())));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        metadata: &RecordMetadata,
        error: Option<&KrafkaError>,
        _headers: &RecordHeaders,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
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
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::producer::{ProducerRecord, RecordHeaders, RecordMetadata};
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
    fn on_acknowledgement(
        &self,
        _metadata: &RecordMetadata,
        error: Option<&KrafkaError>,
        _headers: &RecordHeaders,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
        if error.is_some() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}
```

## Per-Record State

`on_send` gets the record. `on_acknowledgement` gets a `RecordMetadata` — topic,
partition, offset, timestamp — plus the final headers, but nothing identifying
*which* record this was to the interceptor that sent it: no key, no identifier,
and the partition is not chosen until after `on_send` returns.

`RecordContext` closes that gap. The library creates one per record before
`on_send`, carries it through the accumulator, batching, retries and
`MESSAGE_TOO_LARGE` batch splits, and hands the *same* context back to
`on_acknowledgement`.

```rust
pub struct RecordContext { /* ... */ }

impl RecordContext {
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T>;
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T>;
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T>;
    pub fn take<T: Send + Sync + 'static>(&mut self) -> Option<T>;
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool;
}
```

Values are keyed by type. One interceptor may store any number of *distinct*
types; storing the same type twice replaces the previous value and returns it.

### Example: End-to-end delivery latency

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::producer::{ProducerRecord, RecordHeaders, RecordMetadata};
use krafka::error::KrafkaError;
use std::time::Instant;

/// Newtype so this interceptor's slot cannot collide with anything else it
/// might store later.
struct SendStart(Instant);

#[derive(Debug)]
struct LatencyInterceptor;

impl ProducerInterceptor for LatencyInterceptor {
    fn on_send(&self, _record: &mut ProducerRecord, ctx: &mut RecordContext) -> InterceptorResult {
        ctx.insert(SendStart(Instant::now()));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        metadata: &RecordMetadata,
        error: Option<&KrafkaError>,
        _headers: &RecordHeaders,
        ctx: &mut RecordContext,
    ) -> InterceptorResult {
        // `take` rather than `get`: this is the end of the value's life.
        if let Some(SendStart(started)) = ctx.take::<SendStart>() {
            let elapsed = started.elapsed();
            tracing::info!(
                topic = %metadata.topic,
                millis = elapsed.as_millis(),
                ok = error.is_none(),
                "end-to-end delivery latency"
            );
        }
        Ok(())
    }
}
```

The clock covers serialization, the `linger` window, backpressure on
`buffer_memory`, every retry and the broker round trip. Timing the `send()`
future from the application side measures the same span only if the caller
awaits every handle immediately, which defeats pipelining.

### Example: Retaining and completing a span

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::producer::{ProducerRecord, RecordHeaders, RecordMetadata};
use krafka::error::KrafkaError;
use tracing::Span;

/// Whatever your propagator produces for the current context.
fn current_traceparent() -> Vec<u8> {
    b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_vec()
}

struct ProduceSpan(Span);

#[derive(Debug)]
struct SpanInterceptor;

impl ProducerInterceptor for SpanInterceptor {
    fn on_send(&self, record: &mut ProducerRecord, ctx: &mut RecordContext) -> InterceptorResult {
        let span = tracing::info_span!(
            "kafka.produce",
            topic = %record.topic,
            offset = tracing::field::Empty,
        );
        // Inject propagation headers while the span is current.
        let entered = span.enter();
        record.headers.push((
            "traceparent".to_string(),
            Some(current_traceparent().into()),
        ));
        drop(entered);
        ctx.insert(ProduceSpan(span));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        metadata: &RecordMetadata,
        error: Option<&KrafkaError>,
        _headers: &RecordHeaders,
        ctx: &mut RecordContext,
    ) -> InterceptorResult {
        if let Some(ProduceSpan(span)) = ctx.take::<ProduceSpan>() {
            span.record("offset", metadata.offset);
            if let Some(e) = error {
                tracing::error!(parent: &span, error = %e, "produce failed");
            }
            // Dropping the span here closes it — at the acknowledgement, which
            // is what makes the span's duration mean something.
            drop(span);
        }
        Ok(())
    }
}
```

Because of [the pairing guarantee](#the-pairing-guarantee), this span is always
closed, including on the paths that reject a record before it is ever queued.

### Isolation between chained interceptors

Values are keyed by `(interceptor, type)`, not by type alone. Two interceptors
in the same chain that both store a `Span` each see their own, and neither can
read, overwrite or `take` the other's. An interceptor's behaviour therefore
cannot be changed by what its neighbours in the chain happen to store — the same
isolation the chain already gives you for errors and panics.

### Cost

An unused context allocates nothing, so records flowing past an interceptor that
stores nothing — or through a producer with no interceptor — pay no heap
traffic. The first `insert` allocates once, and one allocation serves the whole
chain.

What you store is held for the record's entire buffered lifetime, up to
`delivery.timeout.ms`, and is **not** counted against `buffer_memory`. Store
handles — a span, an `Instant`, an ID — not payloads, and keep their `Drop`
cheap: they are dropped on the producer's send task.

`T: Send + Sync` because the context travels into the accumulator's send tasks,
where the batch holding it is borrowed across `await` points. Spans, `Instant`s,
IDs and OpenTelemetry contexts are all `Sync`; wrap anything that is not in a
`Mutex`.

### Why not just await the `DeliveryHandle`?

An application can hold state around `producer.enqueue(..).await?` and finish it
when the handle resolves — but only in code it controls at every call site. A
reusable interceptor plugged in with `add_interceptor` never sees the handle.

### Headers at acknowledgement

`on_acknowledgement` also receives the record's **final** header set, read-only:
everything this interceptor, the ones after it in the chain, and the configured
serializers wrote. `on_send` cannot show you that, because it runs before the
rest of the chain. Mirrors the Java client's
[KIP-512](https://cwiki.apache.org/confluence/display/KAFKA/KIP-512%3A+make+Record+Headers+available+in+onAcknowledgement)
(Kafka 4.1).

```rust
fn on_acknowledgement(
    &self,
    metadata: &RecordMetadata,
    _error: Option<&KrafkaError>,
    headers: &RecordHeaders,
    _ctx: &mut RecordContext,
) -> InterceptorResult {
    // Audit exactly what was produced, alongside where it landed.
    for (key, value) in headers {
        tracing::debug!(offset = metadata.offset, %key, present = value.is_some());
    }
    Ok(())
}
```

Headers tell you what was *sent*; the context carries what you need *back*.
Prefer the context for correlation: a header key means a side table to maintain,
and only state that survives being reduced to bytes can go in one — a live
`Span` cannot.

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

### Per-record state on the consumer side

`RecordContext` is producer-only. The consumer hooks have no per-record terminal
event to carry state *to*: `on_commit` reports per-partition offsets that may
cover records the interceptor never saw, and with auto-commit disabled may never
arrive at all. For consumer-side tracing, extract the parent context from the
record's headers in `on_consume` and start the span in your own processing code,
where the unit of work begins and ends.

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

The *record* is shared down the chain; the [`RecordContext`](#per-record-state)
is not. Each interceptor addresses its own slot, so a panic in one leaves its
neighbours' per-record state intact.

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
There is zero overhead — the no-op methods are inlined away by the compiler, and
the per-record `RecordContext` nobody writes to never allocates.

## Pipeline Integration Points

### Producer Pipeline

```
  send_record(record) / enqueue(record)
       │
       ▼
  on_send(&mut record, &mut ctx)        ← modify record, park per-record state
       │
       ├─ rejected here (serializer, validation, unknown topic, max.block)
       │      └─► on_acknowledgement(Failed / UNKNOWN_PARTITION, err, headers, ctx)
       ▼
  partitioner.partition()
       │
       ▼
  accumulator: linger, batching, retries, splits   ← ctx rides with the record
       │
       ▼
  encode + send to broker
       │
       ├─ success ─► on_acknowledgement(metadata, None,        headers, ctx)
       └─ failure ─► on_acknowledgement(metadata, Some(error), headers, ctx)
```

Every arrow out of `on_send` ends at an `on_acknowledgement`.

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
- **Contexts hold what you put in them:** a `RecordContext` value lives until the
  record's terminal callback, which under backpressure can be as long as
  `delivery.timeout.ms`. Storing a decrypted payload there keeps it in memory far
  longer than the send itself.

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
