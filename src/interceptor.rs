//! Interceptor hooks for producers and consumers.
//!
//! Interceptors allow you to observe and modify records at key points in the
//! producer and consumer pipelines. They are modeled after the Kafka Java
//! client's `ProducerInterceptor` and `ConsumerInterceptor` interfaces.
//!
//! # Error Handling
//!
//! All interceptor methods return [`InterceptorResult`]. Errors are non-fatal:
//! the chain continues and the error is logged at `warn!`. This gives
//! interceptor authors a clean way to signal failures (e.g. a metrics backend
//! is down) without resorting to panics. Panics are still caught by
//! `catch_unwind` as a safety net and logged at `error!`.
//!
//! # Interceptor Chaining
//!
//! Multiple interceptors can be registered and execute as an ordered chain,
//! matching the Java client's behavior. Each interceptor is individually
//! error- and panic-isolated — an error or panic in one interceptor is caught
//! and logged, and the remaining interceptors still execute.
//!
//! ```rust,ignore
//! let producer = Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .add_interceptor(Arc::new(TracingInterceptor))
//!     .add_interceptor(Arc::new(MetricsInterceptor))
//!     .build()
//!     .await?;
//! ```
//!
//! # Producer Interceptors
//!
//! Producer interceptors can inspect or modify records before they are sent,
//! and observe the acknowledgement (or error) after a send completes.
//!
//! ```rust,ignore
//! use krafka::interceptor::{ProducerInterceptor, InterceptorResult, RecordContext};
//! use krafka::producer::{ProducerRecord, RecordMetadata};
//! use krafka::error::KrafkaError;
//!
//! struct LoggingInterceptor;
//!
//! impl ProducerInterceptor for LoggingInterceptor {
//!     fn on_send(&self, record: &mut ProducerRecord, ctx: &mut RecordContext) -> InterceptorResult {
//!         println!("Sending to topic: {}", record.topic);
//!         Ok(())
//!     }
//!
//!     fn on_acknowledgement(
//!         &self,
//!         metadata: &RecordMetadata,
//!         error: Option<&KrafkaError>,
//!         ctx: &mut RecordContext,
//!     ) -> InterceptorResult {
//!         if let Some(err) = error {
//!             eprintln!("Send failed: {}", err);
//!         } else {
//!             println!("Sent to {}:{} offset {}", metadata.topic, metadata.partition, metadata.offset);
//!         }
//!         Ok(())
//!     }
//! }
//!
//! let producer = Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .add_interceptor(Arc::new(LoggingInterceptor))
//!     .build()
//!     .await?;
//! ```
//!
//! # Per-record State
//!
//! `on_send` mutates the record; `on_acknowledgement` sees [`RecordMetadata`]
//! and the final headers, but nothing identifying *which* record this was to
//! the interceptor that sent it. [`RecordContext`] closes that gap: the library
//! creates one per record before `on_send`, carries it through the accumulator,
//! batching, retries and batch splits, and hands the same context back to
//! `on_acknowledgement` — so a tracing interceptor can open a span in one
//! callback and close it in the other.
//!
//! Values are keyed by `(interceptor, type)`, so chained interceptors cannot
//! see or clobber each other's state. `on_acknowledgement` fires **exactly
//! once** for every record `on_send` observed, including records rejected
//! before they reach the accumulator and records whose
//! [`DeliveryHandle`](crate::producer::DeliveryHandle) the caller dropped, so
//! an interceptor never leaks a span. See [`RecordContext`] for the full
//! contract, its cost, and an example.
//!
//! # Consumer Interceptors
//!
//! Consumer interceptors can inspect records after they are fetched and
//! observe offset commits.
//!
//! ```rust,ignore
//! use krafka::interceptor::{ConsumerInterceptor, InterceptorResult};
//! use krafka::consumer::ConsumerRecord;
//!
//! struct MetricsInterceptor;
//!
//! impl ConsumerInterceptor for MetricsInterceptor {
//!     fn on_consume(&self, records: &[ConsumerRecord]) -> InterceptorResult {
//!         println!("Consumed {} records", records.len());
//!         Ok(())
//!     }
//! }
//!
//! let consumer = Consumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-group")
//!     .add_interceptor(Arc::new(MetricsInterceptor))
//!     .build()
//!     .await?;
//! ```

use ahash::AHashMap as HashMap;
use bytes::Bytes;
use std::any::{Any, TypeId};
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use crate::consumer::ConsumerRecord;
use crate::error::KrafkaError;
use crate::producer::{ProducerRecord, RecordHeaders, RecordMetadata};
use crate::{Offset, PartitionId, Timestamp};

/// The offset map handed to [`ConsumerInterceptor::on_commit`].
///
/// Named rather than spelled out, because the trait signature would otherwise
/// leak `ahash::AHashMap` — an implementor would have to add `ahash` to their
/// own `Cargo.toml` just to name the parameter. The interceptor guide reached
/// for `std::collections::HashMap` instead, which is a different type, so the
/// documented implementation never compiled.
pub type CommitOffsets = HashMap<(String, PartitionId), Offset>;

/// Result type for interceptor callbacks.
///
/// Interceptor errors are **non-fatal**: the chain continues and the error is
/// logged at `warn!`. Return `Err` to signal that something went wrong
/// (e.g. a metrics backend is unreachable) without panicking.
///
/// The error type is intentionally `Box<dyn Error>` rather than a concrete type:
/// interceptor errors are only logged, never programmatically matched, so callers
/// don't need to downcast. Use the `Display` impl to provide diagnostic context.
pub type InterceptorResult = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// One value parked in a [`RecordContext`], tagged with the interceptor that
/// owns it.
///
/// `owner` is the interceptor's index in the chain. Tagging every value —
/// rather than giving each interceptor its own map — is what keeps the whole
/// context to a *single* lazily-allocated `Vec` per record instead of one
/// allocation per interceptor per record.
struct ContextEntry {
    owner: u16,
    type_id: TypeId,
    value: Box<dyn Any + Send + Sync>,
}

/// Per-record state that travels from [`ProducerInterceptor::on_send`] to
/// [`ProducerInterceptor::on_acknowledgement`].
///
/// A context is created by the library *before* `on_send` and carried with the
/// record through the accumulator, batching, retries and batch splits, until
/// the record reaches its terminal outcome. Whatever an interceptor parks in it
/// during `on_send` is handed back — same record, same context — in
/// `on_acknowledgement`.
///
/// This is what makes stateful interceptors possible. [`RecordMetadata`]
/// carries no key and no identifier, and the partition is not chosen until
/// after `on_send` returns; the headers can serve as a key into a side table
/// you maintain yourself, but only for state that survives being reduced to
/// bytes. A context hands back the value itself — a live span, a timer, a
/// permit.
///
/// # Isolation
///
/// Values are keyed by `(interceptor, type)`, not by type alone. Two
/// interceptors in the same chain that both store a `SpanGuard` each see their
/// own, and neither can read, overwrite or [`take`](Self::take) the other's.
/// This preserves the chain's isolation guarantee: an interceptor's behaviour
/// cannot be changed by what its neighbours do.
///
/// A single interceptor may store any number of *distinct* types; storing the
/// same type twice replaces the previous value (and returns it).
///
/// # Cost
///
/// An unused context allocates nothing — it is an empty `Vec` — so records
/// flowing through a producer with no interceptor, or past an interceptor that
/// stores nothing, pay no heap traffic at all. The first
/// [`insert`](Self::insert) allocates once, and that one allocation serves
/// every interceptor and every type in the chain.
///
/// Inline it is 32 bytes on 64-bit targets, carried on each buffered record and
/// pinned by `a_context_stays_within_its_per_record_size_budget`.
///
/// Whatever you store is held for the record's entire buffered lifetime, up to
/// `delivery.timeout.ms`, and is **not** counted against `buffer_memory`.
/// Store handles (a span, an `Instant`, an ID), not payloads.
///
/// # Delivery guarantee
///
/// Every record that `on_send` observes reaches `on_acknowledgement` exactly
/// once, with its context — including records rejected before they ever reach
/// the accumulator (serialization failure, validation failure, unknown topic,
/// `max.block.ms` exhaustion). Those arrive with
/// [`DeliveryConfirmation::Failed`](crate::producer::DeliveryConfirmation::Failed),
/// offset `-1` and, when routing never happened,
/// [`UNKNOWN_PARTITION`](crate::producer::UNKNOWN_PARTITION) — the same values
/// the Java client synthesizes in `ProducerInterceptors.onSendError`. The one
/// exception is a panic inside krafka's own batch-send task, which abandons the
/// batch it was sending and costs the caller its acknowledgement too.
///
/// A value you leave in the context is dropped on the producer's send task,
/// immediately after the terminal callback returns. Keep its `Drop` cheap and
/// non-blocking, for the same reason the callbacks themselves must be.
///
/// # Example
///
/// ```rust
/// use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
/// use krafka::producer::{ProducerRecord, RecordHeaders, RecordMetadata};
/// use krafka::error::KrafkaError;
/// use std::time::Instant;
///
/// #[derive(Debug)]
/// struct LatencyInterceptor;
///
/// struct SendStart(Instant);
///
/// impl ProducerInterceptor for LatencyInterceptor {
///     fn on_send(&self, _record: &mut ProducerRecord, ctx: &mut RecordContext) -> InterceptorResult {
///         ctx.insert(SendStart(Instant::now()));
///         Ok(())
///     }
///
///     fn on_acknowledgement(
///         &self,
///         _metadata: &RecordMetadata,
///         _error: Option<&KrafkaError>,
///         _headers: &RecordHeaders,
///         ctx: &mut RecordContext,
///     ) -> InterceptorResult {
///         if let Some(SendStart(started)) = ctx.take::<SendStart>() {
///             let _e2e = started.elapsed();
///         }
///         Ok(())
///     }
/// }
/// ```
#[derive(Default)]
pub struct RecordContext {
    /// Lazily allocated: an untouched context never reaches the heap.
    entries: Vec<ContextEntry>,
    /// Index of the interceptor currently executing. Set by
    /// [`ProducerInterceptorChain`] around each call; `0` for a producer
    /// configured with a single interceptor.
    owner: u16,
}

impl fmt::Debug for RecordContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The values are `dyn Any` and cannot be formatted; showing how many
        // are parked (and for whom) is the useful part and leaks no payload.
        f.debug_struct("RecordContext")
            .field("entries", &self.entries.len())
            .field("owner", &self.owner)
            .finish()
    }
}

impl RecordContext {
    /// Create an empty context.
    ///
    /// The producer creates one per record; you only need this to unit-test an
    /// interceptor without a live producer.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            owner: 0,
        }
    }

    /// Point the accessors at interceptor `owner`, returning the previous one.
    ///
    /// Called by the chain around each interceptor invocation. Keeping this
    /// crate-private is what makes the isolation guarantee enforceable: an
    /// interceptor cannot address another's slot because it cannot name it.
    pub(crate) fn set_owner(&mut self, owner: u16) -> u16 {
        std::mem::replace(&mut self.owner, owner)
    }

    /// Index of the calling interceptor's `T`, if present.
    fn position<T: 'static>(&self) -> Option<usize> {
        let type_id = TypeId::of::<T>();
        self.entries
            .iter()
            .position(|entry| entry.owner == self.owner && entry.type_id == type_id)
    }

    /// Store `value`, returning the previous value of the same type if the
    /// calling interceptor had already stored one.
    ///
    /// `T: Send + Sync` because the context travels with the record into the
    /// accumulator's send tasks, and the batch it lands in is borrowed across
    /// `await` points there — a shared borrow of a non-`Sync` value is not
    /// `Send`, so the whole batch future would stop being spawnable. In
    /// practice this costs nothing: spans, `Instant`s, IDs and OpenTelemetry
    /// contexts are all `Sync`. Wrap genuinely non-`Sync` state in a `Mutex`.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        match self.position::<T>() {
            Some(index) => {
                let previous = self
                    .entries
                    .get_mut(index)
                    .map(|entry| std::mem::replace(&mut entry.value, Box::new(value)))?;
                previous.downcast::<T>().ok().map(|boxed| *boxed)
            }
            None => {
                self.entries.push(ContextEntry {
                    owner: self.owner,
                    type_id: TypeId::of::<T>(),
                    value: Box::new(value),
                });
                None
            }
        }
    }

    /// Borrow the value of type `T` stored by the calling interceptor.
    #[must_use]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        let index = self.position::<T>()?;
        self.entries.get(index)?.value.downcast_ref::<T>()
    }

    /// Mutably borrow the value of type `T` stored by the calling interceptor.
    #[must_use]
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        let index = self.position::<T>()?;
        self.entries.get_mut(index)?.value.downcast_mut::<T>()
    }

    /// Remove and return the value of type `T` stored by the calling
    /// interceptor.
    ///
    /// This is the natural end of a span or timer in `on_acknowledgement`:
    /// taking ownership means the value is dropped when you are done with it,
    /// rather than living until the record's context is dropped.
    #[must_use]
    pub fn take<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        let index = self.position::<T>()?;
        let entry = self.entries.remove(index);
        entry.value.downcast::<T>().ok().map(|boxed| *boxed)
    }

    /// Whether the calling interceptor has stored a value of type `T`.
    #[must_use]
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.position::<T>().is_some()
    }
}

/// Interceptor for the Kafka producer pipeline.
///
/// Implement this trait to hook into the producer's send and acknowledgement
/// flow. All methods have default no-op implementations so you can override
/// only the hooks you need.
///
/// # Error contract
///
/// Return `Err(...)` to signal a non-fatal failure. The error is logged at
/// `warn!` and the chain continues. Reserve panics for genuine bugs —
/// they are caught by `catch_unwind` and logged at `error!`.
pub trait ProducerInterceptor: Send + Sync + fmt::Debug {
    /// Called before a record is sent.
    ///
    /// The record can be mutated (e.g. adding headers, modifying the key).
    /// This is invoked on the calling thread before partitioning.
    ///
    /// A `None` value is a **tombstone**, which on a compacted topic deletes
    /// the record's key; `Some(Bytes::new())` looks similar and does the
    /// opposite. An interceptor that rewrites values should leave `None` alone
    /// unless it means to cancel the deletion. Header values likewise.
    ///
    /// `ctx` is this record's [`RecordContext`]. Anything stored in it here is
    /// handed back to [`on_acknowledgement`](Self::on_acknowledgement) for the
    /// same record — that is how a span, a timer or a correlation ID survives
    /// the trip through the accumulator.
    fn on_send(&self, _record: &mut ProducerRecord, _ctx: &mut RecordContext) -> InterceptorResult {
        Ok(())
    }

    /// Called after a record has reached its terminal outcome.
    ///
    /// `error` is `None` on success. This is invoked asynchronously and
    /// should not block.
    ///
    /// Fires exactly once for every record [`on_send`](Self::on_send)
    /// observed — see [`RecordContext`] for the precise guarantee and its one
    /// exception. Records rejected before reaching the accumulator arrive with
    /// [`DeliveryConfirmation::Failed`](crate::producer::DeliveryConfirmation::Failed),
    /// offset `-1`, and
    /// [`UNKNOWN_PARTITION`](crate::producer::UNKNOWN_PARTITION) when the
    /// record never got as far as being routed. Dropping the
    /// [`DeliveryHandle`](crate::producer::DeliveryHandle) does not suppress
    /// it: the handle discards the *caller's* view of the acknowledgement, not
    /// the interceptor's.
    ///
    /// `headers` is the record's **final** header set, read-only: everything
    /// this interceptor, the ones after it in the chain, and the configured
    /// serializers wrote. `on_send` cannot show you that — it runs before the
    /// rest of the chain — so this is the only place the complete set is
    /// visible. Mirrors the Java client's
    /// `onAcknowledgement(RecordMetadata, Exception, Headers)` (KIP-512).
    ///
    /// For correlating with `on_send`, reach for `ctx` rather than a header:
    /// see [`RecordContext`] for why.
    ///
    /// `ctx` is the same [`RecordContext`] `on_send` saw for this record.
    fn on_acknowledgement(
        &self,
        _metadata: &RecordMetadata,
        _error: Option<&KrafkaError>,
        _headers: &RecordHeaders,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
        Ok(())
    }

    /// Called when the producer is being closed.
    ///
    /// Use this to release any resources held by the interceptor.
    fn close(&self) -> InterceptorResult {
        Ok(())
    }
}

/// Interceptor for the Kafka consumer pipeline.
///
/// (See [`CommitOffsets`] for the map type `on_commit` receives.)
///
/// Implement this trait to hook into the consumer's poll and commit flow.
/// All methods have default no-op implementations so you can override
/// only the hooks you need.
///
/// # Error contract
///
/// Return `Err(...)` to signal a non-fatal failure. The error is logged at
/// `warn!` and the chain continues. Reserve panics for genuine bugs —
/// they are caught by `catch_unwind` and logged at `error!`.
pub trait ConsumerInterceptor: Send + Sync + fmt::Debug {
    /// Called after records have been fetched but before they are returned
    /// to the application.
    ///
    /// This is useful for metrics, logging, or record-level filtering.
    fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult {
        Ok(())
    }

    /// Called after offsets have been committed.
    ///
    /// The map keys are `(topic, partition)` and values are the committed offsets.
    fn on_commit(
        &self,
        _offsets: &CommitOffsets,
        _error: Option<&KrafkaError>,
    ) -> InterceptorResult {
        Ok(())
    }

    /// Called when the consumer is being closed.
    ///
    /// Use this to release any resources held by the interceptor.
    fn close(&self) -> InterceptorResult {
        Ok(())
    }
}

/// A no-op producer interceptor used as the default.
#[derive(Debug)]
pub(crate) struct NoOpProducerInterceptor;

impl ProducerInterceptor for NoOpProducerInterceptor {}

/// A no-op consumer interceptor used as the default.
#[derive(Debug)]
pub(crate) struct NoOpConsumerInterceptor;

impl ConsumerInterceptor for NoOpConsumerInterceptor {}

/// An O(1), allocation-free snapshot of the parts of a [`ProducerRecord`] that
/// can be cheaply rolled back after a panicking interceptor.
///
/// Capturing this costs two `Bytes` refcount bumps and a few `Copy`s — no
/// `String` or `Vec` allocation — which is what makes it viable on the
/// producer's per-record hot path. See [`ProducerInterceptorChain`] for the
/// resulting panic semantics.
struct CheapRecordSnapshot {
    partition: Option<PartitionId>,
    key: Option<Bytes>,
    value: Option<Bytes>,
    timestamp: Option<Timestamp>,
    /// Number of headers present before the interceptor ran.
    header_len: usize,
}

impl CheapRecordSnapshot {
    /// Capture the cheaply-restorable fields of `record`.
    #[inline]
    fn capture(record: &ProducerRecord) -> Self {
        Self {
            partition: record.partition,
            key: record.key.clone(),
            value: record.value.clone(),
            timestamp: record.timestamp,
            header_len: record.headers.len(),
        }
    }

    /// Restore the captured fields onto `record`.
    ///
    /// `topic`, `record_name`, in-place edits to pre-existing header values,
    /// and removed headers are **not** restored — they were never captured.
    #[inline]
    fn restore(self, record: &mut ProducerRecord) {
        record.partition = self.partition;
        record.key = self.key;
        record.value = self.value;
        record.timestamp = self.timestamp;
        // Drop any headers the interceptor appended before panicking.
        record.headers.truncate(self.header_len);
    }
}

/// An ordered chain of producer interceptors.
///
/// Executes each interceptor in registration order. Each interceptor is
/// individually panic-isolated — a panic in one interceptor is caught and
/// logged, and the remaining interceptors still execute. This matches the
/// Java Kafka client's `ProducerInterceptors` behavior.
///
/// For `on_send`, each interceptor sees the record as modified by the
/// previous interceptors in the chain.
///
/// # Panic semantics
///
/// In Java, `onSend` returns a new record; if interceptor N throws,
/// interceptor N+1 receives the record from the last *successful* interceptor.
/// In Rust, `on_send` mutates in-place (`&mut`), so a full rollback would
/// require cloning the record before *every* interceptor call. That clone is
/// deep (`topic: String`, `headers: Vec<(String, Option<Bytes>)>`) and sits on the
/// producer's per-record hot path, so it is deliberately **not** taken.
///
/// Instead, a panic triggers a cheap, allocation-free rollback of exactly the
/// fields that can be restored in O(1) (see [`CheapRecordSnapshot`]):
///
/// - `partition`, `timestamp` — `Copy`, restored exactly.
/// - `key`, `value` — `Option<Bytes>`, restored via refcount bump (no data
///   copy), nullness included.
/// - `headers` — truncated back to its pre-call length, undoing any headers
///   the panicking interceptor appended. Values it mutated *in place*, and any
///   headers it removed, are **not** restored.
/// - `topic`, `record_name` — `String`; **not** restored. A panicking
///   interceptor that had already reassigned the topic leaves the new value in
///   place.
///
/// The panic itself is caught, logged at `error!` with the chain index, and the
/// remaining interceptors still execute — unchanged from previous behaviour.
/// Avoid building chains where later interceptors depend on invariants set by
/// earlier ones.
pub(crate) struct ProducerInterceptorChain {
    interceptors: Vec<Arc<dyn ProducerInterceptor>>,
}

/// [`RecordContext`] owner tag for chain position `index`.
///
/// Saturates rather than wrapping: a chain long enough to overflow `u16` would
/// otherwise alias interceptor 65 536 onto interceptor 0 and silently break
/// isolation between them. Saturating collapses only the tail of an absurd
/// chain, and does so deterministically.
#[inline]
fn chain_owner(index: usize) -> u16 {
    u16::try_from(index).unwrap_or(u16::MAX)
}

impl fmt::Debug for ProducerInterceptorChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerInterceptorChain")
            .field("len", &self.interceptors.len())
            .finish()
    }
}

impl ProducerInterceptorChain {
    /// Create a chain from a list of interceptors.
    pub fn new(interceptors: Vec<Arc<dyn ProducerInterceptor>>) -> Self {
        Self { interceptors }
    }
}

impl ProducerInterceptor for ProducerInterceptorChain {
    fn on_send(&self, record: &mut ProducerRecord, ctx: &mut RecordContext) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            // Scope the shared context to this interceptor's slot, so what it
            // stores is invisible — and untakeable — to the rest of the chain.
            let previous_owner = ctx.set_owner(chain_owner(i));
            // O(1) snapshot of the cheaply-restorable fields. Deliberately not
            // a full `record.clone()`: that deep-copies `topic` and every
            // header key on the producer's per-record hot path. See the type
            // docs for exactly what a panic does and does not roll back.
            let snapshot = CheapRecordSnapshot::capture(record);
            match catch_unwind(AssertUnwindSafe(|| interceptor.on_send(record, ctx))) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        topic = record.topic.as_str(),
                        error = %e,
                        "ProducerInterceptor.on_send failed",
                    );
                }
                Err(_) => {
                    // Partial, allocation-free rollback so the next interceptor
                    // sees a mostly-consistent record.
                    snapshot.restore(record);
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        topic = record.topic.as_str(),
                        "ProducerInterceptor.on_send panicked — record partially restored (payload redacted)",
                    );
                }
            }
            ctx.set_owner(previous_owner);
        }
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        metadata: &RecordMetadata,
        error: Option<&KrafkaError>,
        headers: &RecordHeaders,
        ctx: &mut RecordContext,
    ) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            // Same slot this interceptor wrote to in `on_send` — see
            // `chain_owner`.
            let previous_owner = ctx.set_owner(chain_owner(i));
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                interceptor.on_acknowledgement(metadata, error, headers, ctx)
            }));
            ctx.set_owner(previous_owner);
            match outcome {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        topic = metadata.topic.as_str(),
                        partition = metadata.partition,
                        error = %e,
                        "ProducerInterceptor.on_acknowledgement failed",
                    );
                }
                Err(_) => {
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        topic = metadata.topic.as_str(),
                        partition = metadata.partition,
                        "ProducerInterceptor.on_acknowledgement panicked (payload redacted)",
                    );
                }
            }
        }
        Ok(())
    }

    fn close(&self) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            match catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        error = %e,
                        "ProducerInterceptor.close failed",
                    );
                }
                Err(_) => {
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        "ProducerInterceptor.close panicked (payload redacted)",
                    );
                }
            }
        }
        Ok(())
    }
}

/// An ordered chain of consumer interceptors.
///
/// Executes each interceptor in registration order. Each interceptor is
/// individually panic-isolated — a panic in one interceptor is caught and
/// logged, and the remaining interceptors still execute. This matches the
/// Java Kafka client's `ConsumerInterceptors` behavior.
pub(crate) struct ConsumerInterceptorChain {
    interceptors: Vec<Arc<dyn ConsumerInterceptor>>,
}

impl fmt::Debug for ConsumerInterceptorChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConsumerInterceptorChain")
            .field("len", &self.interceptors.len())
            .finish()
    }
}

impl ConsumerInterceptorChain {
    /// Create a chain from a list of interceptors.
    pub fn new(interceptors: Vec<Arc<dyn ConsumerInterceptor>>) -> Self {
        Self { interceptors }
    }
}

impl ConsumerInterceptor for ConsumerInterceptorChain {
    fn on_consume(&self, records: &[ConsumerRecord]) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            match catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(records))) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        record_count = records.len(),
                        error = %e,
                        "ConsumerInterceptor.on_consume failed",
                    );
                }
                Err(_) => {
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        record_count = records.len(),
                        "ConsumerInterceptor.on_consume panicked (payload redacted)",
                    );
                }
            }
        }
        Ok(())
    }

    fn on_commit(
        &self,
        offsets: &HashMap<(String, PartitionId), Offset>,
        error: Option<&KrafkaError>,
    ) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            match catch_unwind(AssertUnwindSafe(|| interceptor.on_commit(offsets, error))) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        offset_count = offsets.len(),
                        error = %e,
                        "ConsumerInterceptor.on_commit failed",
                    );
                }
                Err(_) => {
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        offset_count = offsets.len(),
                        "ConsumerInterceptor.on_commit panicked (payload redacted)",
                    );
                }
            }
        }
        Ok(())
    }

    fn close(&self) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            match catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        error = %e,
                        "ConsumerInterceptor.close failed",
                    );
                }
                Err(_) => {
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        "ConsumerInterceptor.close panicked (payload redacted)",
                    );
                }
            }
        }
        Ok(())
    }
}

/// Panic-safe wrapper for producer interceptor `on_send`.
///
/// Catches errors and panics from user-provided interceptor code so that a
/// misbehaving interceptor cannot crash the producer.
pub(crate) fn safe_on_send(
    interceptor: &dyn ProducerInterceptor,
    record: &mut ProducerRecord,
    ctx: &mut RecordContext,
) {
    match catch_unwind(AssertUnwindSafe(|| interceptor.on_send(record, ctx))) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                topic = record.topic.as_str(),
                error = %e,
                "ProducerInterceptor.on_send failed",
            );
        }
        Err(_) => {
            tracing::error!(
                topic = record.topic.as_str(),
                "ProducerInterceptor.on_send panicked (payload redacted)",
            );
        }
    }
}

/// Panic-safe wrapper for producer interceptor `on_acknowledgement`.
pub(crate) fn safe_on_acknowledgement(
    interceptor: &dyn ProducerInterceptor,
    metadata: &RecordMetadata,
    error: Option<&KrafkaError>,
    headers: &RecordHeaders,
    ctx: &mut RecordContext,
) {
    match catch_unwind(AssertUnwindSafe(|| {
        interceptor.on_acknowledgement(metadata, error, headers, ctx)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                topic = metadata.topic.as_str(),
                partition = metadata.partition,
                error = %e,
                "ProducerInterceptor.on_acknowledgement failed",
            );
        }
        Err(_) => {
            tracing::error!(
                topic = metadata.topic.as_str(),
                partition = metadata.partition,
                "ProducerInterceptor.on_acknowledgement panicked (payload redacted)",
            );
        }
    }
}

/// Panic-safe wrapper for producer interceptor `close`.
pub(crate) fn safe_producer_close(interceptor: &dyn ProducerInterceptor) {
    match catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e,
                "ProducerInterceptor.close failed",
            );
        }
        Err(_) => {
            tracing::error!("ProducerInterceptor.close panicked (payload redacted)");
        }
    }
}

/// Panic-safe wrapper for consumer interceptor `on_consume`.
pub(crate) fn safe_on_consume(interceptor: &dyn ConsumerInterceptor, records: &[ConsumerRecord]) {
    match catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(records))) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                record_count = records.len(),
                error = %e,
                "ConsumerInterceptor.on_consume failed",
            );
        }
        Err(_) => {
            tracing::error!(
                record_count = records.len(),
                "ConsumerInterceptor.on_consume panicked (payload redacted)",
            );
        }
    }
}

/// Panic-safe wrapper for consumer interceptor `on_commit`.
pub(crate) fn safe_on_commit(
    interceptor: &dyn ConsumerInterceptor,
    offsets: &HashMap<(String, PartitionId), Offset>,
    error: Option<&KrafkaError>,
) {
    match catch_unwind(AssertUnwindSafe(|| interceptor.on_commit(offsets, error))) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                offset_count = offsets.len(),
                error = %e,
                "ConsumerInterceptor.on_commit failed",
            );
        }
        Err(_) => {
            tracing::error!(
                offset_count = offsets.len(),
                "ConsumerInterceptor.on_commit panicked (payload redacted)",
            );
        }
    }
}

/// Panic-safe wrapper for consumer interceptor `close`.
pub(crate) fn safe_consumer_close(interceptor: &dyn ConsumerInterceptor) {
    match catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e,
                "ConsumerInterceptor.close failed",
            );
        }
        Err(_) => {
            tracing::error!("ConsumerInterceptor.close panicked (payload redacted)");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestProducerInterceptor {
        send_count: std::sync::atomic::AtomicUsize,
        ack_count: std::sync::atomic::AtomicUsize,
    }

    impl TestProducerInterceptor {
        fn new() -> Self {
            Self {
                send_count: std::sync::atomic::AtomicUsize::new(0),
                ack_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn send_count(&self) -> usize {
            self.send_count.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn ack_count(&self) -> usize {
            self.ack_count.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl ProducerInterceptor for TestProducerInterceptor {
        fn on_send(
            &self,
            record: &mut ProducerRecord,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            self.send_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Add a tracing header
            record.headers.push((
                "x-intercepted".to_string(),
                Some(bytes::Bytes::from_static(b"true")),
            ));
            Ok(())
        }

        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
            _headers: &RecordHeaders,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            self.ack_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct TestConsumerInterceptor {
        consume_count: std::sync::atomic::AtomicUsize,
        commit_count: std::sync::atomic::AtomicUsize,
    }

    impl TestConsumerInterceptor {
        fn new() -> Self {
            Self {
                consume_count: std::sync::atomic::AtomicUsize::new(0),
                commit_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn consume_count(&self) -> usize {
            self.consume_count
                .load(std::sync::atomic::Ordering::Relaxed)
        }

        fn commit_count(&self) -> usize {
            self.commit_count.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl ConsumerInterceptor for TestConsumerInterceptor {
        fn on_consume(&self, records: &[ConsumerRecord]) -> InterceptorResult {
            self.consume_count
                .fetch_add(records.len(), std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) -> InterceptorResult {
            self.commit_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn test_producer_interceptor_on_send() {
        let interceptor = TestProducerInterceptor::new();
        let mut record = ProducerRecord::new("test-topic", b"value".to_vec());
        assert_eq!(interceptor.send_count(), 0);
        assert!(record.headers.is_empty());

        interceptor
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        assert_eq!(interceptor.send_count(), 1);
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.headers[0].0, "x-intercepted");
        assert_eq!(
            record.headers[0].1,
            Some(bytes::Bytes::from_static(b"true"))
        );
    }

    #[test]
    fn test_producer_interceptor_on_acknowledgement() {
        let interceptor = TestProducerInterceptor::new();
        let metadata = RecordMetadata {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            timestamp: 1000,
            delivery: crate::producer::DeliveryConfirmation::Offset,
        };

        interceptor
            .on_acknowledgement(&metadata, None, &[], &mut RecordContext::new())
            .unwrap();
        assert_eq!(interceptor.ack_count(), 1);

        let err = KrafkaError::config("test error");
        interceptor
            .on_acknowledgement(&metadata, Some(&err), &[], &mut RecordContext::new())
            .unwrap();
        assert_eq!(interceptor.ack_count(), 2);
    }

    #[test]
    fn test_consumer_interceptor_on_consume() {
        let interceptor = TestConsumerInterceptor::new();
        let records = vec![
            ConsumerRecord::new("test-topic", 0, 0, None, Some(bytes::Bytes::from("v1"))),
            ConsumerRecord::new("test-topic", 0, 1, None, Some(bytes::Bytes::from("v2"))),
        ];

        interceptor.on_consume(&records).unwrap();
        assert_eq!(interceptor.consume_count(), 2);
    }

    #[test]
    fn test_consumer_interceptor_on_commit() {
        let interceptor = TestConsumerInterceptor::new();
        let mut offsets = HashMap::new();
        offsets.insert(("test-topic".to_string(), 0), 10i64);

        interceptor.on_commit(&offsets, None).unwrap();
        assert_eq!(interceptor.commit_count(), 1);
    }

    #[test]
    fn test_noop_interceptors() {
        let producer_interceptor = NoOpProducerInterceptor;
        let mut record = ProducerRecord::new("test", b"value".to_vec());
        producer_interceptor
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();
        assert!(record.headers.is_empty());

        let consumer_interceptor = NoOpConsumerInterceptor;
        consumer_interceptor.on_consume(&[]).unwrap();
        consumer_interceptor
            .on_commit(&HashMap::new(), None)
            .unwrap();
    }

    // --- Panic-safety tests ---

    #[derive(Debug)]
    struct PanickingProducerInterceptor;

    impl ProducerInterceptor for PanickingProducerInterceptor {
        fn on_send(
            &self,
            _record: &mut ProducerRecord,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            panic!("on_send panic");
        }
        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
            _headers: &RecordHeaders,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            panic!("on_acknowledgement panic");
        }
        fn close(&self) -> InterceptorResult {
            panic!("producer close panic");
        }
    }

    #[derive(Debug)]
    struct PanickingConsumerInterceptor;

    impl ConsumerInterceptor for PanickingConsumerInterceptor {
        fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult {
            panic!("on_consume panic");
        }
        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) -> InterceptorResult {
            panic!("on_commit panic");
        }
        fn close(&self) -> InterceptorResult {
            panic!("consumer close panic");
        }
    }

    #[test]
    fn test_safe_on_send_catches_panic() {
        let interceptor = PanickingProducerInterceptor;
        let mut record = ProducerRecord::new("test", b"value".to_vec());
        // Should not propagate the panic
        safe_on_send(&interceptor, &mut record, &mut RecordContext::new());
    }

    #[test]
    fn test_safe_on_acknowledgement_catches_panic() {
        let interceptor = PanickingProducerInterceptor;
        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            delivery: crate::producer::DeliveryConfirmation::Offset,
        };
        safe_on_acknowledgement(
            &interceptor,
            &metadata,
            None,
            &[],
            &mut RecordContext::new(),
        );
    }

    #[test]
    fn test_safe_producer_close_catches_panic() {
        let interceptor = PanickingProducerInterceptor;
        safe_producer_close(&interceptor);
    }

    #[test]
    fn test_safe_on_consume_catches_panic() {
        let interceptor = PanickingConsumerInterceptor;
        safe_on_consume(&interceptor, &[]);
    }

    #[test]
    fn test_safe_on_commit_catches_panic() {
        let interceptor = PanickingConsumerInterceptor;
        safe_on_commit(&interceptor, &HashMap::new(), None);
    }

    #[test]
    fn test_safe_consumer_close_catches_panic() {
        let interceptor = PanickingConsumerInterceptor;
        safe_consumer_close(&interceptor);
    }

    #[test]
    fn test_close_default_noop() {
        // Default close() is a no-op — should not panic
        let p = NoOpProducerInterceptor;
        p.close().unwrap();

        let c = NoOpConsumerInterceptor;
        c.close().unwrap();
    }

    // --- Interceptor chain tests ---

    /// Records the order of interceptor invocations into a shared log.
    #[derive(Debug)]
    struct OrderedProducerInterceptor {
        name: &'static str,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ProducerInterceptor for OrderedProducerInterceptor {
        fn on_send(
            &self,
            _record: &mut ProducerRecord,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_send", self.name));
            Ok(())
        }

        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
            _headers: &RecordHeaders,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_ack", self.name));
            Ok(())
        }

        fn close(&self) -> InterceptorResult {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.close", self.name));
            Ok(())
        }
    }

    #[test]
    fn test_producer_chain_executes_in_order() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(OrderedProducerInterceptor {
                name: "first",
                log: Arc::clone(&log),
            }),
            Arc::new(OrderedProducerInterceptor {
                name: "second",
                log: Arc::clone(&log),
            }),
            Arc::new(OrderedProducerInterceptor {
                name: "third",
                log: Arc::clone(&log),
            }),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            delivery: crate::producer::DeliveryConfirmation::Offset,
        };
        chain
            .on_acknowledgement(&metadata, None, &[], &mut RecordContext::new())
            .unwrap();
        chain.close().unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "first.on_send",
                "second.on_send",
                "third.on_send",
                "first.on_ack",
                "second.on_ack",
                "third.on_ack",
                "first.close",
                "second.close",
                "third.close",
            ]
        );
    }

    #[test]
    fn test_producer_chain_on_send_mutations_visible_to_next() {
        /// Appends a header with its name.
        #[derive(Debug)]
        struct HeaderAdder(&'static str);

        impl ProducerInterceptor for HeaderAdder {
            fn on_send(
                &self,
                record: &mut ProducerRecord,
                _ctx: &mut RecordContext,
            ) -> InterceptorResult {
                record.headers.push((
                    self.0.to_string(),
                    Some(bytes::Bytes::copy_from_slice(self.0.as_bytes())),
                ));
                Ok(())
            }
        }

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(HeaderAdder("first")),
            Arc::new(HeaderAdder("second")),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].0, "first");
        assert_eq!(record.headers[1].0, "second");
    }

    #[test]
    fn test_producer_chain_panic_isolation() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(OrderedProducerInterceptor {
                name: "before",
                log: Arc::clone(&log),
            }),
            Arc::new(PanickingProducerInterceptor),
            Arc::new(OrderedProducerInterceptor {
                name: "after",
                log: Arc::clone(&log),
            }),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
            delivery: crate::producer::DeliveryConfirmation::Offset,
        };
        chain
            .on_acknowledgement(&metadata, None, &[], &mut RecordContext::new())
            .unwrap();
        chain.close().unwrap();

        let log = log.lock().unwrap();
        // Both "before" and "after" run; the panicking interceptor is skipped
        assert_eq!(
            *log,
            vec![
                "before.on_send",
                "after.on_send",
                "before.on_ack",
                "after.on_ack",
                "before.close",
                "after.close",
            ]
        );
    }

    // --- Cheap-snapshot rollback semantics (no per-record deep clone) ---

    /// Mutates every field of the record, then panics.
    #[derive(Debug)]
    struct MutateThenPanicInterceptor;

    impl ProducerInterceptor for MutateThenPanicInterceptor {
        fn on_send(
            &self,
            record: &mut ProducerRecord,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            record.partition = Some(99);
            record.timestamp = Some(1234);
            record.key = Some(bytes::Bytes::from_static(b"clobbered-key"));
            record.value = Some(bytes::Bytes::from_static(b"clobbered-value"));
            record
                .headers
                .push(("added-before-panic".to_string(), Some(bytes::Bytes::new())));
            record.topic = "clobbered-topic".to_string();
            panic!("mutate then panic");
        }
    }

    #[test]
    fn test_producer_chain_panic_restores_cheap_fields() {
        let chain = ProducerInterceptorChain::new(vec![Arc::new(MutateThenPanicInterceptor)]);

        let mut record = ProducerRecord::new("original-topic", b"original-value".to_vec());
        record.key = Some(bytes::Bytes::from_static(b"original-key"));
        record.partition = Some(1);
        record.timestamp = Some(7);
        record.headers.push((
            "pre-existing".to_string(),
            Some(bytes::Bytes::from_static(b"h")),
        ));

        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        // Cheaply-restorable fields are rolled back exactly.
        assert_eq!(record.partition, Some(1));
        assert_eq!(record.timestamp, Some(7));
        assert_eq!(record.key, Some(bytes::Bytes::from_static(b"original-key")));
        assert_eq!(record.value, Some(bytes::Bytes::from("original-value")));
        // Headers appended by the panicking interceptor are dropped; the
        // pre-existing header survives.
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.headers[0].0, "pre-existing");
    }

    #[test]
    fn test_producer_chain_panic_does_not_deep_clone_topic() {
        // Documents the new semantics: because no deep clone is taken, a topic
        // reassigned by an interceptor that then panics is NOT rolled back.
        // If a deep snapshot were reintroduced this assertion would fail.
        let chain = ProducerInterceptorChain::new(vec![Arc::new(MutateThenPanicInterceptor)]);

        let mut record = ProducerRecord::new("original-topic", b"v".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        assert_eq!(record.topic, "clobbered-topic");
    }

    #[test]
    fn test_producer_chain_panic_still_surfaced_and_chain_continues() {
        // The panic is caught (not propagated), the chain still returns Ok,
        // and later interceptors observe the partially-restored record.
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        /// Records what the record looked like when it was invoked.
        #[derive(Debug)]
        struct Observer(Arc<std::sync::Mutex<Vec<String>>>);

        impl ProducerInterceptor for Observer {
            fn on_send(
                &self,
                record: &mut ProducerRecord,
                _ctx: &mut RecordContext,
            ) -> InterceptorResult {
                self.0.lock().unwrap().push(format!(
                    "value={} headers={}",
                    record.value_str().unwrap_or("<null>"),
                    record.headers.len()
                ));
                Ok(())
            }
        }

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(MutateThenPanicInterceptor),
            Arc::new(Observer(Arc::clone(&log))),
        ]);

        let mut record = ProducerRecord::new("t", b"v".to_vec());
        // Returns Ok — the panic is caught and logged, exactly as before.
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["value=v headers=0"]);
    }

    #[test]
    fn test_producer_chain_no_panic_keeps_mutations() {
        // The snapshot must never be applied on the success path.
        #[derive(Debug)]
        struct Mutator;

        impl ProducerInterceptor for Mutator {
            fn on_send(
                &self,
                record: &mut ProducerRecord,
                _ctx: &mut RecordContext,
            ) -> InterceptorResult {
                record.partition = Some(5);
                record.value = Some(bytes::Bytes::from_static(b"new"));
                record
                    .headers
                    .push(("added".to_string(), Some(bytes::Bytes::new())));
                Ok(())
            }
        }

        let chain = ProducerInterceptorChain::new(vec![Arc::new(Mutator)]);
        let mut record = ProducerRecord::new("t", b"old".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        assert_eq!(record.partition, Some(5));
        assert_eq!(record.value, Some(bytes::Bytes::from_static(b"new")));
        assert_eq!(record.headers.len(), 1);
    }

    #[test]
    fn test_producer_chain_error_return_does_not_roll_back() {
        // An `Err` return is not a panic: mutations made before returning Err
        // are kept (unchanged behaviour).
        #[derive(Debug)]
        struct MutateThenErr;

        impl ProducerInterceptor for MutateThenErr {
            fn on_send(
                &self,
                record: &mut ProducerRecord,
                _ctx: &mut RecordContext,
            ) -> InterceptorResult {
                record.partition = Some(3);
                Err("boom".into())
            }
        }

        let chain = ProducerInterceptorChain::new(vec![Arc::new(MutateThenErr)]);
        let mut record = ProducerRecord::new("t", b"v".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        assert_eq!(record.partition, Some(3));
    }

    #[test]
    fn test_producer_chain_empty() {
        let chain = ProducerInterceptorChain::new(vec![]);
        let mut record = ProducerRecord::new("test", b"value".to_vec());
        // Empty chain is a no-op — should not panic
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();
        chain.close().unwrap();
    }

    #[derive(Debug)]
    struct OrderedConsumerInterceptor {
        name: &'static str,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ConsumerInterceptor for OrderedConsumerInterceptor {
        fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_consume", self.name));
            Ok(())
        }

        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) -> InterceptorResult {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_commit", self.name));
            Ok(())
        }

        fn close(&self) -> InterceptorResult {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.close", self.name));
            Ok(())
        }
    }

    #[test]
    fn test_consumer_chain_executes_in_order() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ConsumerInterceptorChain::new(vec![
            Arc::new(OrderedConsumerInterceptor {
                name: "first",
                log: Arc::clone(&log),
            }),
            Arc::new(OrderedConsumerInterceptor {
                name: "second",
                log: Arc::clone(&log),
            }),
        ]);

        chain.on_consume(&[]).unwrap();
        chain.on_commit(&HashMap::new(), None).unwrap();
        chain.close().unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "first.on_consume",
                "second.on_consume",
                "first.on_commit",
                "second.on_commit",
                "first.close",
                "second.close",
            ]
        );
    }

    #[test]
    fn test_consumer_chain_panic_isolation() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ConsumerInterceptorChain::new(vec![
            Arc::new(OrderedConsumerInterceptor {
                name: "before",
                log: Arc::clone(&log),
            }),
            Arc::new(PanickingConsumerInterceptor),
            Arc::new(OrderedConsumerInterceptor {
                name: "after",
                log: Arc::clone(&log),
            }),
        ]);

        chain.on_consume(&[]).unwrap();
        chain.on_commit(&HashMap::new(), None).unwrap();
        chain.close().unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "before.on_consume",
                "after.on_consume",
                "before.on_commit",
                "after.on_commit",
                "before.close",
                "after.close",
            ]
        );
    }

    #[test]
    fn test_consumer_chain_empty() {
        let chain = ConsumerInterceptorChain::new(vec![]);
        chain.on_consume(&[]).unwrap();
        chain.on_commit(&HashMap::new(), None).unwrap();
        chain.close().unwrap();
    }

    #[test]
    fn test_chain_via_safe_wrappers() {
        // Verify chains work through the safe_* wrappers (belt-and-suspenders)
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(OrderedProducerInterceptor {
                name: "a",
                log: Arc::clone(&log),
            }),
            Arc::new(OrderedProducerInterceptor {
                name: "b",
                log: Arc::clone(&log),
            }),
        ]);

        let mut record = ProducerRecord::new("test", b"v".to_vec());
        safe_on_send(&chain, &mut record, &mut RecordContext::new());

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["a.on_send", "b.on_send"]);
    }

    // --- Error-returning interceptor tests ---

    /// An interceptor that returns an error from on_send.
    #[derive(Debug)]
    struct FailingProducerInterceptor;

    impl ProducerInterceptor for FailingProducerInterceptor {
        fn on_send(
            &self,
            _record: &mut ProducerRecord,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            Err("metrics backend unavailable".into())
        }
        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
            _headers: &RecordHeaders,
            _ctx: &mut RecordContext,
        ) -> InterceptorResult {
            Err("ack handler failed".into())
        }
        fn close(&self) -> InterceptorResult {
            Err("cleanup failed".into())
        }
    }

    #[derive(Debug)]
    struct FailingConsumerInterceptor;

    impl ConsumerInterceptor for FailingConsumerInterceptor {
        fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult {
            Err("consume handler failed".into())
        }
        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) -> InterceptorResult {
            Err("commit handler failed".into())
        }
        fn close(&self) -> InterceptorResult {
            Err("cleanup failed".into())
        }
    }

    #[test]
    fn test_producer_chain_error_isolation() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(OrderedProducerInterceptor {
                name: "before",
                log: Arc::clone(&log),
            }),
            Arc::new(FailingProducerInterceptor),
            Arc::new(OrderedProducerInterceptor {
                name: "after",
                log: Arc::clone(&log),
            }),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        // Chain returns Ok — individual errors are logged, not propagated.
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();
        chain.close().unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "before.on_send",
                "after.on_send",
                "before.close",
                "after.close"
            ]
        );
    }

    #[test]
    fn test_consumer_chain_error_isolation() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ConsumerInterceptorChain::new(vec![
            Arc::new(OrderedConsumerInterceptor {
                name: "before",
                log: Arc::clone(&log),
            }),
            Arc::new(FailingConsumerInterceptor),
            Arc::new(OrderedConsumerInterceptor {
                name: "after",
                log: Arc::clone(&log),
            }),
        ]);

        chain.on_consume(&[]).unwrap();
        chain.close().unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            *log,
            vec![
                "before.on_consume",
                "after.on_consume",
                "before.close",
                "after.close"
            ]
        );
    }

    #[test]
    fn test_safe_wrappers_catch_errors() {
        let interceptor = FailingProducerInterceptor;
        let mut record = ProducerRecord::new("test", b"v".to_vec());
        // Should not panic — error is caught and logged
        safe_on_send(&interceptor, &mut record, &mut RecordContext::new());
        safe_producer_close(&interceptor);

        let interceptor = FailingConsumerInterceptor;
        safe_on_consume(&interceptor, &[]);
        safe_consumer_close(&interceptor);
    }

    #[test]
    fn test_producer_chain_error_at_first_position() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(FailingProducerInterceptor),
            Arc::new(OrderedProducerInterceptor {
                name: "second",
                log: Arc::clone(&log),
            }),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["second.on_send"]);
    }

    #[test]
    fn test_producer_chain_error_at_last_position() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(OrderedProducerInterceptor {
                name: "first",
                log: Arc::clone(&log),
            }),
            Arc::new(FailingProducerInterceptor),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["first.on_send"]);
    }

    #[test]
    fn test_producer_chain_mixed_error_and_panic() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(OrderedProducerInterceptor {
                name: "first",
                log: Arc::clone(&log),
            }),
            Arc::new(FailingProducerInterceptor),
            Arc::new(PanickingProducerInterceptor),
            Arc::new(OrderedProducerInterceptor {
                name: "last",
                log: Arc::clone(&log),
            }),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        // Chain survives both an error and a panic — all healthy interceptors run
        chain
            .on_send(&mut record, &mut RecordContext::new())
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["first.on_send", "last.on_send"]);
    }

    #[test]
    fn test_consumer_chain_mixed_error_and_panic() {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));

        let chain = ConsumerInterceptorChain::new(vec![
            Arc::new(OrderedConsumerInterceptor {
                name: "first",
                log: Arc::clone(&log),
            }),
            Arc::new(FailingConsumerInterceptor),
            Arc::new(PanickingConsumerInterceptor),
            Arc::new(OrderedConsumerInterceptor {
                name: "last",
                log: Arc::clone(&log),
            }),
        ]);

        chain.on_consume(&[]).unwrap();

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["first.on_consume", "last.on_consume"]);
    }

    // -----------------------------------------------------------------
    // RecordContext
    // -----------------------------------------------------------------

    #[derive(Debug, PartialEq)]
    struct Span(&'static str);

    #[derive(Debug, PartialEq)]
    struct Started(u64);

    #[test]
    fn record_context_round_trips_distinct_types() {
        let mut ctx = RecordContext::new();

        assert!(ctx.insert(Span("send")).is_none());
        assert!(ctx.insert(Started(7)).is_none());

        assert_eq!(ctx.get::<Span>(), Some(&Span("send")));
        assert_eq!(ctx.get::<Started>(), Some(&Started(7)));
        assert!(ctx.contains::<Span>());

        if let Some(started) = ctx.get_mut::<Started>() {
            started.0 = 9;
        }
        assert_eq!(ctx.take::<Started>(), Some(Started(9)));
        assert!(
            !ctx.contains::<Started>(),
            "take removes the value, so a second take sees nothing"
        );
        assert_eq!(ctx.take::<Started>(), None);
        // Unrelated types are untouched by a take.
        assert_eq!(ctx.get::<Span>(), Some(&Span("send")));
    }

    #[test]
    fn record_context_insert_of_the_same_type_returns_the_previous_value() {
        let mut ctx = RecordContext::new();
        assert!(ctx.insert(Span("first")).is_none());
        assert_eq!(ctx.insert(Span("second")), Some(Span("first")));
        assert_eq!(ctx.get::<Span>(), Some(&Span("second")));
    }

    #[test]
    fn record_context_of_a_type_never_stored_is_empty() {
        let ctx = RecordContext::new();
        assert_eq!(ctx.get::<Span>(), None);
        assert!(!ctx.contains::<Span>());
    }

    /// Stores `Span(name)` in `on_send` and records what it reads back in
    /// `on_acknowledgement`, so a test can assert on cross-interceptor
    /// visibility rather than on internal state.
    #[derive(Debug)]
    struct ContextInterceptor {
        name: &'static str,
        /// What this interceptor saw in its own slot at acknowledgement time.
        seen: Arc<std::sync::Mutex<Option<Span>>>,
        /// Panic at the end of `on_send`, after storing.
        panic_after_store: bool,
    }

    impl ContextInterceptor {
        fn new(name: &'static str) -> (Arc<Self>, Arc<std::sync::Mutex<Option<Span>>>) {
            let seen = Arc::new(std::sync::Mutex::new(None));
            (
                Arc::new(Self {
                    name,
                    seen: Arc::clone(&seen),
                    panic_after_store: false,
                }),
                seen,
            )
        }
    }

    impl ProducerInterceptor for ContextInterceptor {
        fn on_send(
            &self,
            _record: &mut ProducerRecord,
            ctx: &mut RecordContext,
        ) -> InterceptorResult {
            ctx.insert(Span(self.name));
            if self.panic_after_store {
                panic!("interceptor blew up after storing its state");
            }
            Ok(())
        }

        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
            _headers: &RecordHeaders,
            ctx: &mut RecordContext,
        ) -> InterceptorResult {
            *self.seen.lock().unwrap() = ctx.take::<Span>();
            Ok(())
        }
    }

    #[test]
    fn chained_interceptors_cannot_see_each_others_context() {
        let (first, first_seen) = ContextInterceptor::new("first");
        let (second, second_seen) = ContextInterceptor::new("second");
        let chain = ProducerInterceptorChain::new(vec![first, second]);

        let mut ctx = RecordContext::new();
        let mut record = ProducerRecord::new("test", b"v".to_vec());
        chain.on_send(&mut record, &mut ctx).unwrap();

        // Both stored a `Span`; keyed by type alone the second would have
        // clobbered the first.
        let metadata = RecordMetadata::failed("test".to_string(), 0);
        chain
            .on_acknowledgement(&metadata, None, &[], &mut ctx)
            .unwrap();

        assert_eq!(*first_seen.lock().unwrap(), Some(Span("first")));
        assert_eq!(*second_seen.lock().unwrap(), Some(Span("second")));
    }

    #[test]
    fn a_chained_interceptor_cannot_take_a_neighbours_state() {
        /// Tries to steal whatever `Span` is in the context.
        #[derive(Debug)]
        struct Thief {
            stole: Arc<std::sync::Mutex<bool>>,
        }

        impl ProducerInterceptor for Thief {
            fn on_send(
                &self,
                _record: &mut ProducerRecord,
                ctx: &mut RecordContext,
            ) -> InterceptorResult {
                *self.stole.lock().unwrap() = ctx.take::<Span>().is_some();
                Ok(())
            }
        }

        let (victim, victim_seen) = ContextInterceptor::new("victim");
        let stole = Arc::new(std::sync::Mutex::new(false));
        let thief = Arc::new(Thief {
            stole: Arc::clone(&stole),
        });
        let chain = ProducerInterceptorChain::new(vec![victim, thief]);

        let mut ctx = RecordContext::new();
        let mut record = ProducerRecord::new("test", b"v".to_vec());
        chain.on_send(&mut record, &mut ctx).unwrap();
        let metadata = RecordMetadata::failed("test".to_string(), 0);
        chain
            .on_acknowledgement(&metadata, None, &[], &mut ctx)
            .unwrap();

        assert!(!*stole.lock().unwrap(), "the thief must see an empty slot");
        assert_eq!(
            *victim_seen.lock().unwrap(),
            Some(Span("victim")),
            "the victim's state must survive the attempt"
        );
    }

    #[test]
    fn a_panicking_interceptor_does_not_disturb_its_neighbours_context() {
        let (healthy, healthy_seen) = ContextInterceptor::new("healthy");
        let exploder = Arc::new(ContextInterceptor {
            name: "exploder",
            seen: Arc::new(std::sync::Mutex::new(None)),
            panic_after_store: true,
        });
        let chain = ProducerInterceptorChain::new(vec![exploder, healthy]);

        let mut ctx = RecordContext::new();
        let mut record = ProducerRecord::new("test", b"v".to_vec());
        chain.on_send(&mut record, &mut ctx).unwrap();

        let metadata = RecordMetadata::failed("test".to_string(), 0);
        chain
            .on_acknowledgement(&metadata, None, &[], &mut ctx)
            .unwrap();

        assert_eq!(
            *healthy_seen.lock().unwrap(),
            Some(Span("healthy")),
            "a panic in interceptor 0 must not cost interceptor 1 its state"
        );
    }

    /// The context rides on every buffered record, so its inline size is a
    /// budget, not an accident.
    ///
    /// 32 bytes = a `Vec` (24) plus the owner tag, padded. If a field is added
    /// here, weigh it against `PendingRecord`, which is 184 bytes — the context
    /// is already about a sixth of it, and a producer under backpressure holds
    /// one per buffered record for up to `delivery.timeout.ms`.
    #[test]
    #[cfg(target_pointer_width = "64")]
    fn a_context_stays_within_its_per_record_size_budget() {
        assert_eq!(std::mem::size_of::<RecordContext>(), 32);
    }

    /// The empty case is the common case — a producer with no interceptor, or
    /// an interceptor that stores nothing, must not touch the heap.
    #[test]
    fn an_untouched_context_never_allocates() {
        let ctx = RecordContext::new();
        assert_eq!(ctx.entries.capacity(), 0, "an empty Vec owns no allocation");
    }

    #[test]
    fn chain_owner_saturates_rather_than_wrapping() {
        assert_eq!(chain_owner(0), 0);
        assert_eq!(chain_owner(usize::from(u16::MAX)), u16::MAX);
        assert_eq!(
            chain_owner(usize::from(u16::MAX) + 1),
            u16::MAX,
            "an absurd chain collapses its tail rather than aliasing onto index 0"
        );
    }
}
