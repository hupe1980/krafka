+++
title = "Interceptors"
description = "Producer and consumer hooks for tracing, redaction, auditing and record rewriting."
weight = 90

[extra]
slug_id = "interceptors"
+++

Interceptors hook into the producer and consumer pipelines, as the Kafka Java
client's `ProducerInterceptor` and `ConsumerInterceptor` do, in ordered chains.

## Overview

| Hook | Pipeline | When |
|------|----------|------|
| `on_send` | Producer | Before a record is partitioned and sent; may mutate it |
| `on_acknowledgement` | Producer | After a record reaches its terminal outcome |
| `close` | Producer | When the producer is shutting down |
| `on_consume` | Consumer | After records are fetched, before they are returned to the application |
| `on_commit` | Consumer | After offsets are committed |
| `close` | Consumer | When the consumer is shutting down |

krafka already emits a `send` span per record (see
[Metrics](@/docs/metrics.md#spans)); use an interceptor for what it does not
do, such as trace-context propagation headers, auditing, or a span of your own
held from `on_send` to `on_acknowledgement` — see
[Per-Record State](#per-record-state).

## Producer Interceptor

### Trait Definition

```rust
pub type InterceptorResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

pub trait ProducerInterceptor: Send + Sync + fmt::Debug {
    /// Before partitioning. The record may be mutated; `ctx` is this
    /// record's scratch space.
    fn on_send(&self, _record: &mut Record, _ctx: &mut RecordContext) -> InterceptorResult { Ok(()) }

    /// After the terminal outcome. `headers` is the record's final,
    /// read-only header set; `ctx` is the context `on_send` saw.
    fn on_acknowledgement(
        &self,
        _topic: &str,
        _partition: PartitionId,
        _result: Result<&RecordMetadata, &KrafkaError>,
        _headers: &Headers,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult { Ok(()) }

    /// When the producer is closed.
    fn close(&self) -> InterceptorResult { Ok(()) }
}
```

Every method has a no-op default. `on_acknowledgement` runs on the producer's
send task and must not block. A `None` value is a tombstone; an interceptor
that rewrites values should leave it alone.

### The pairing guarantee

`on_acknowledgement` fires **exactly once for every record `on_send`
observed**: on success, on permanent failure, and for records rejected before
they are queued (failed validation, an unresolved topic, `max_block`
exhausted, a panic in `on_send`). This holds on both producers and is not
suppressed by dropping the `DeliveryHandle` or the `send()` future. A record
rejected before it was routed reports `Err` with partition
`krafka::producer::UNKNOWN_PARTITION`.

That makes it safe to hold a span, a timer or a permit across the two
callbacks.

### Example: Tracing Headers

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::Headers;
use krafka::producer::{Record, RecordMetadata};
use krafka::error::KrafkaError;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
struct TracingInterceptor {
    next_id: AtomicU64,
}

impl ProducerInterceptor for TracingInterceptor {
    fn on_send(&self, record: &mut Record, _ctx: &mut RecordContext) -> InterceptorResult {
        // Use your tracer's id here; a counter keeps the sample dependency-free.
        let trace_id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        record.headers.push(("x-trace-id".to_string(), Some(trace_id.into_bytes().into())));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        topic: &str,
        partition: i32,
        result: Result<&RecordMetadata, &KrafkaError>,
        _headers: &Headers,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
        match result {
            Ok(metadata) => tracing::info!(
                topic,
                partition,
                offset = metadata.offset,
                "record acknowledged"
            ),
            Err(e) => tracing::error!("send failed: {}", e),
        }
        Ok(())
    }
}

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .interceptor(TracingInterceptor::default())
    .build()
    .await?;
```

## Per-Record State

`on_acknowledgement` gets the topic, partition, outcome and headers, but
nothing that identifies the record to the interceptor that saw it in
`on_send`. `RecordContext` carries that: krafka creates one per record before
`on_send` and hands the same context to `on_acknowledgement`, through
batching, retries and batch splits.

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

Values are keyed by type within one interceptor; storing the same type again
replaces and returns the previous value.

### Example: End-to-end delivery latency

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::Headers;
use krafka::producer::{Record, RecordMetadata};
use krafka::error::KrafkaError;
use std::time::Instant;

/// Newtype so this interceptor's slot cannot collide with anything else it
/// might store later.
struct SendStart(Instant);

#[derive(Debug)]
struct LatencyInterceptor;

impl ProducerInterceptor for LatencyInterceptor {
    fn on_send(&self, _record: &mut Record, ctx: &mut RecordContext) -> InterceptorResult {
        ctx.insert(SendStart(Instant::now()));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        topic: &str,
        _partition: i32,
        result: Result<&RecordMetadata, &KrafkaError>,
        _headers: &Headers,
        ctx: &mut RecordContext,
    ) -> InterceptorResult {
        // `take` rather than `get`: this is the end of the value's life.
        if let Some(SendStart(started)) = ctx.take::<SendStart>() {
            let elapsed = started.elapsed();
            tracing::info!(
                topic,
                millis = elapsed.as_millis(),
                ok = result.is_ok(),
                "end-to-end delivery latency"
            );
        }
        Ok(())
    }
}
```

The measured time covers topic resolution, the wait for `buffer_memory`, the
`linger` window, every retry and the broker round trip — not serialization,
which a `TypedProducer` does before `on_send`.

### Example: Retaining and completing a span

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::Headers;
use krafka::producer::{Record, RecordMetadata};
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
    fn on_send(&self, record: &mut Record, ctx: &mut RecordContext) -> InterceptorResult {
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
        _topic: &str,
        _partition: i32,
        result: Result<&RecordMetadata, &KrafkaError>,
        _headers: &Headers,
        ctx: &mut RecordContext,
    ) -> InterceptorResult {
        if let Some(ProduceSpan(span)) = ctx.take::<ProduceSpan>() {
            match result {
                Ok(metadata) => {
                    span.record("offset", metadata.offset);
                }
                Err(e) => tracing::error!(parent: &span, error = %e, "produce failed"),
            }
            // Dropping the span closes it at the acknowledgement.
            drop(span);
        }
        Ok(())
    }
}
```

By [the pairing guarantee](#the-pairing-guarantee), the span is always closed,
including for records rejected before they are queued.

### Isolation between chained interceptors

Values are keyed by `(interceptor, type)`: two interceptors in one chain that
both store a `Span` each see only their own.

### Cost and lifetime

An unused context does not allocate; the first `insert` allocates once for the
whole chain. Stored values live until the record's outcome — up to
`delivery_timeout` — are **not** counted against `buffer_memory`, and are
dropped on the producer's send task. Store handles (a span, an `Instant`, an
ID), not payloads, and keep their `Drop` cheap. Wrap non-`Sync` values in a
`Mutex`.

### Headers at acknowledgement

`on_acknowledgement` receives the record's **final** header set: everything
this interceptor, later interceptors and a `TypedProducer`'s serializers
wrote — the Java client's `onAcknowledgement(RecordMetadata, Exception, Headers)`
([KIP-512](https://cwiki.apache.org/confluence/display/KAFKA/KIP-512%3A+make+Record+Headers+available+in+onAcknowledgement)).

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::Headers;
use krafka::producer::RecordMetadata;
use krafka::error::KrafkaError;

#[derive(Debug)]
struct HeaderAudit;

impl ProducerInterceptor for HeaderAudit {
    fn on_acknowledgement(
        &self,
        _topic: &str,
        partition: i32,
        _result: Result<&RecordMetadata, &KrafkaError>,
        headers: &Headers,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
        // Audit exactly what was produced, alongside where it landed.
        for (key, value) in headers {
            tracing::debug!(partition, %key, present = value.is_some());
        }
        Ok(())
    }
}
```

Use the context, not a header, to correlate `on_send` with
`on_acknowledgement`: a live `Span` cannot be encoded in a header.

## Consumer Interceptor

### Trait Definition

```rust
pub type CommitOffsets = HashMap<(String, PartitionId), Offset>;

pub trait ConsumerInterceptor: Send + Sync + fmt::Debug {
    /// After records are fetched, before they are returned to the application.
    fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult { Ok(()) }

    /// After offsets are committed (or the commit failed).
    fn on_commit(
        &self,
        _offsets: &CommitOffsets,
        _error: Option<&KrafkaError>,
    ) -> InterceptorResult { Ok(()) }

    /// When the consumer is closed.
    fn close(&self) -> InterceptorResult { Ok(()) }
}
```

### Example: Consumption Logging

```rust,compile
use krafka::interceptor::{ConsumerInterceptor, InterceptorResult};
use krafka::consumer::ConsumerRecord;

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

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .interceptor(LoggingInterceptor)
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

`on_commit` receives only offsets of partitions still assigned.
`RecordContext` is producer-only: for consumer-side tracing, extract the parent
context from the record's headers and start the span in your processing code.

## Wiring Interceptors

Builders take an interceptor by value; pass an `Arc` to keep a handle to it.
Each `interceptor()` call appends to the chain, and interceptors run in the
order they were added. In `on_send`, each interceptor sees the record as the
previous ones left it.

```rust,compile
use krafka::interceptor::{ConsumerInterceptor, ProducerInterceptor};
use std::sync::Arc;

#[derive(Debug)]
struct Tracing;
impl ProducerInterceptor for Tracing {}

#[derive(Debug)]
struct Audit;
impl ProducerInterceptor for Audit {}

#[derive(Debug)]
struct Logging;
impl ConsumerInterceptor for Logging {}

// Keep a handle to read the interceptor's state later.
let audit = Arc::new(Audit);

let producer = kafka
    .producer()
    .interceptor(Tracing)
    .interceptor(Arc::clone(&audit))
    .build()
    .await?;

let consumer = kafka.consumer("my-group").interceptor(Logging).build().await?;
```

Interceptors must be `Send + Sync + Debug`; keep mutable state in atomics or
locks.

## Errors and Panics

| Outcome | Log level | Chain continues? |
|---------|-----------|------------------|
| `Ok(())` | — | Yes |
| `Err(e)` | `warn!` | Yes |
| panic in `on_send` | `error!` (payload redacted) | No — the send fails |
| panic elsewhere | `error!` (payload redacted) | Yes |

An `Err` from `on_send` keeps whatever changes the interceptor made to the
record. A panic in `on_send` may leave the record half-modified, so it is not
produced; the error names the interceptor's position in the chain, and
`on_acknowledgement` still fires. Other interceptors' per-record state is
unaffected.

Both producers call every interceptor's `close()` exactly once: on the first
`close()`, or when the producer is dropped without one.

## Security Considerations

- `on_send` sees every header, which may carry credentials; do not log records
  unredacted.
- Error messages in `on_acknowledgement` may echo broker details from
  authentication failures.
- Do not log an interceptor with `{:?}`: its `Debug` may expose secrets.
- A `RecordContext` value lives until the record's outcome, up to
  `delivery_timeout`; do not store decrypted payloads in it.

## Next Steps

- [Producer Guide](@/docs/producer.md) - Producer configuration and usage
- [Consumer Guide](@/docs/consumer.md) - Consumer groups and offset management
- [Admin Client](@/docs/admin.md) - Cluster administration
