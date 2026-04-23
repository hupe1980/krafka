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
//! use krafka::interceptor::{ProducerInterceptor, InterceptorResult};
//! use krafka::producer::{ProducerRecord, RecordMetadata};
//! use krafka::error::KrafkaError;
//!
//! struct LoggingInterceptor;
//!
//! impl ProducerInterceptor for LoggingInterceptor {
//!     fn on_send(&self, record: &mut ProducerRecord) -> InterceptorResult {
//!         println!("Sending to topic: {}", record.topic);
//!         Ok(())
//!     }
//!
//!     fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&KrafkaError>) -> InterceptorResult {
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

use std::collections::HashMap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use crate::consumer::ConsumerRecord;
use crate::error::KrafkaError;
use crate::producer::{ProducerRecord, RecordMetadata};
use crate::{Offset, PartitionId};

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
    fn on_send(&self, _record: &mut ProducerRecord) -> InterceptorResult {
        Ok(())
    }

    /// Called after a record has been acknowledged (or failed).
    ///
    /// `error` is `None` on success. This is invoked asynchronously and
    /// should not block.
    fn on_acknowledgement(
        &self,
        _metadata: &RecordMetadata,
        _error: Option<&KrafkaError>,
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
        _offsets: &HashMap<(String, PartitionId), Offset>,
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
/// **Panic semantics vs Java:** In Java, `onSend` returns a new record;
/// if interceptor N throws, interceptor N+1 receives the record from the
/// last *successful* interceptor. In Rust, `on_send` mutates in-place
/// (`&mut`); if interceptor N panics mid-mutation, interceptor N+1 sees
/// a partially-mutated record. Avoid building chains where later
/// interceptors depend on invariants set by earlier ones.
pub(crate) struct ProducerInterceptorChain {
    interceptors: Vec<Arc<dyn ProducerInterceptor>>,
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
    fn on_send(&self, record: &mut ProducerRecord) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            // Snapshot before mutation so a panic mid-`on_send` doesn't leave
            // the record in a partially-mutated state for subsequent interceptors.
            let snapshot = record.clone();
            match catch_unwind(AssertUnwindSafe(|| interceptor.on_send(record))) {
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
                    // Restore the pre-panic snapshot so the next interceptor
                    // sees a consistent record (matches Java's behavior).
                    *record = snapshot;
                    tracing::error!(
                        chain_index = i,
                        chain_len = self.interceptors.len(),
                        topic = record.topic.as_str(),
                        "ProducerInterceptor.on_send panicked — record restored (payload redacted)",
                    );
                }
            }
        }
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        metadata: &RecordMetadata,
        error: Option<&KrafkaError>,
    ) -> InterceptorResult {
        for (i, interceptor) in self.interceptors.iter().enumerate() {
            match catch_unwind(AssertUnwindSafe(|| {
                interceptor.on_acknowledgement(metadata, error)
            })) {
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
pub(crate) fn safe_on_send(interceptor: &dyn ProducerInterceptor, record: &mut ProducerRecord) {
    match catch_unwind(AssertUnwindSafe(|| interceptor.on_send(record))) {
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
) {
    match catch_unwind(AssertUnwindSafe(|| {
        interceptor.on_acknowledgement(metadata, error)
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
        fn on_send(&self, record: &mut ProducerRecord) -> InterceptorResult {
            self.send_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Add a tracing header
            record
                .headers
                .push(("x-intercepted".to_string(), b"true".to_vec()));
            Ok(())
        }

        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
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

        interceptor.on_send(&mut record).unwrap();

        assert_eq!(interceptor.send_count(), 1);
        assert_eq!(record.headers.len(), 1);
        assert_eq!(record.headers[0].0, "x-intercepted");
        assert_eq!(record.headers[0].1, b"true");
    }

    #[test]
    fn test_producer_interceptor_on_acknowledgement() {
        let interceptor = TestProducerInterceptor::new();
        let metadata = RecordMetadata {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            timestamp: 1000,
        };

        interceptor.on_acknowledgement(&metadata, None).unwrap();
        assert_eq!(interceptor.ack_count(), 1);

        let err = KrafkaError::config("test error");
        interceptor
            .on_acknowledgement(&metadata, Some(&err))
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
        producer_interceptor.on_send(&mut record).unwrap();
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
        fn on_send(&self, _record: &mut ProducerRecord) -> InterceptorResult {
            panic!("on_send panic");
        }
        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
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
        safe_on_send(&interceptor, &mut record);
    }

    #[test]
    fn test_safe_on_acknowledgement_catches_panic() {
        let interceptor = PanickingProducerInterceptor;
        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        safe_on_acknowledgement(&interceptor, &metadata, None);
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
        fn on_send(&self, _record: &mut ProducerRecord) -> InterceptorResult {
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
        chain.on_send(&mut record).unwrap();

        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        chain.on_acknowledgement(&metadata, None).unwrap();
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
            fn on_send(&self, record: &mut ProducerRecord) -> InterceptorResult {
                record
                    .headers
                    .push((self.0.to_string(), self.0.as_bytes().to_vec()));
                Ok(())
            }
        }

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(HeaderAdder("first")),
            Arc::new(HeaderAdder("second")),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain.on_send(&mut record).unwrap();

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
        chain.on_send(&mut record).unwrap();

        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        chain.on_acknowledgement(&metadata, None).unwrap();
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

    #[test]
    fn test_producer_chain_empty() {
        let chain = ProducerInterceptorChain::new(vec![]);
        let mut record = ProducerRecord::new("test", b"value".to_vec());
        // Empty chain is a no-op — should not panic
        chain.on_send(&mut record).unwrap();
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
        safe_on_send(&chain, &mut record);

        let log = log.lock().unwrap();
        assert_eq!(*log, vec!["a.on_send", "b.on_send"]);
    }

    // --- Error-returning interceptor tests ---

    /// An interceptor that returns an error from on_send.
    #[derive(Debug)]
    struct FailingProducerInterceptor;

    impl ProducerInterceptor for FailingProducerInterceptor {
        fn on_send(&self, _record: &mut ProducerRecord) -> InterceptorResult {
            Err("metrics backend unavailable".into())
        }
        fn on_acknowledgement(
            &self,
            _metadata: &RecordMetadata,
            _error: Option<&KrafkaError>,
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
        chain.on_send(&mut record).unwrap();
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
        safe_on_send(&interceptor, &mut record);
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
        chain.on_send(&mut record).unwrap();

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
        chain.on_send(&mut record).unwrap();

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
        chain.on_send(&mut record).unwrap();

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
}
