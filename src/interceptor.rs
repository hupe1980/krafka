//! Interceptor hooks for producers and consumers.
//!
//! Interceptors allow you to observe and modify records at key points in the
//! producer and consumer pipelines. They are modeled after the Kafka Java
//! client's `ProducerInterceptor` and `ConsumerInterceptor` interfaces.
//!
//! # Interceptor Chaining
//!
//! Multiple interceptors can be registered and execute as an ordered chain,
//! matching the Java client's behavior. Each interceptor is individually
//! panic-isolated — a panic in one interceptor is caught and logged, and
//! the remaining interceptors still execute.
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
//! use krafka::interceptor::ProducerInterceptor;
//! use krafka::producer::{ProducerRecord, RecordMetadata};
//! use krafka::error::KrafkaError;
//!
//! struct LoggingInterceptor;
//!
//! impl ProducerInterceptor for LoggingInterceptor {
//!     fn on_send(&self, record: &mut ProducerRecord) {
//!         println!("Sending to topic: {}", record.topic);
//!     }
//!
//!     fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&KrafkaError>) {
//!         if let Some(err) = error {
//!             eprintln!("Send failed: {}", err);
//!         } else {
//!             println!("Sent to {}:{} offset {}", metadata.topic, metadata.partition, metadata.offset);
//!         }
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
//! use krafka::interceptor::ConsumerInterceptor;
//! use krafka::consumer::ConsumerRecord;
//!
//! struct MetricsInterceptor;
//!
//! impl ConsumerInterceptor for MetricsInterceptor {
//!     fn on_consume(&self, records: &[ConsumerRecord]) {
//!         println!("Consumed {} records", records.len());
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

/// Interceptor for the Kafka producer pipeline.
///
/// Implement this trait to hook into the producer's send and acknowledgement
/// flow. All methods have default no-op implementations so you can override
/// only the hooks you need.
pub trait ProducerInterceptor: Send + Sync + fmt::Debug {
    /// Called before a record is sent.
    ///
    /// The record can be mutated (e.g. adding headers, modifying the key).
    /// This is invoked on the calling thread before partitioning.
    fn on_send(&self, _record: &mut ProducerRecord) {}

    /// Called after a record has been acknowledged (or failed).
    ///
    /// `error` is `None` on success. This is invoked asynchronously and
    /// should not block.
    fn on_acknowledgement(&self, _metadata: &RecordMetadata, _error: Option<&KrafkaError>) {}

    /// Called when the producer is being closed.
    ///
    /// Use this to release any resources held by the interceptor.
    fn close(&self) {}
}

/// Interceptor for the Kafka consumer pipeline.
///
/// Implement this trait to hook into the consumer's poll and commit flow.
/// All methods have default no-op implementations so you can override
/// only the hooks you need.
pub trait ConsumerInterceptor: Send + Sync + fmt::Debug {
    /// Called after records have been fetched but before they are returned
    /// to the application.
    ///
    /// This is useful for metrics, logging, or record-level filtering.
    fn on_consume(&self, _records: &[ConsumerRecord]) {}

    /// Called after offsets have been committed.
    ///
    /// The map keys are `(topic, partition)` and values are the committed offsets.
    fn on_commit(
        &self,
        _offsets: &HashMap<(String, PartitionId), Offset>,
        _error: Option<&KrafkaError>,
    ) {
    }

    /// Called when the consumer is being closed.
    ///
    /// Use this to release any resources held by the interceptor.
    fn close(&self) {}
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
    fn on_send(&self, record: &mut ProducerRecord) {
        for interceptor in &self.interceptors {
            if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.on_send(record))) {
                tracing::error!(
                    interceptor = ?interceptor,
                    "ProducerInterceptor.on_send panicked: {:?}", e,
                );
            }
        }
    }

    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&KrafkaError>) {
        for interceptor in &self.interceptors {
            if let Err(e) = catch_unwind(AssertUnwindSafe(|| {
                interceptor.on_acknowledgement(metadata, error);
            })) {
                tracing::error!(
                    interceptor = ?interceptor,
                    "ProducerInterceptor.on_acknowledgement panicked: {:?}", e,
                );
            }
        }
    }

    fn close(&self) {
        for interceptor in &self.interceptors {
            if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
                tracing::error!(
                    interceptor = ?interceptor,
                    "ProducerInterceptor.close panicked: {:?}", e,
                );
            }
        }
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
    fn on_consume(&self, records: &[ConsumerRecord]) {
        for interceptor in &self.interceptors {
            if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(records))) {
                tracing::error!(
                    interceptor = ?interceptor,
                    "ConsumerInterceptor.on_consume panicked: {:?}", e,
                );
            }
        }
    }

    fn on_commit(
        &self,
        offsets: &HashMap<(String, PartitionId), Offset>,
        error: Option<&KrafkaError>,
    ) {
        for interceptor in &self.interceptors {
            if let Err(e) = catch_unwind(AssertUnwindSafe(|| {
                interceptor.on_commit(offsets, error);
            })) {
                tracing::error!(
                    interceptor = ?interceptor,
                    "ConsumerInterceptor.on_commit panicked: {:?}", e,
                );
            }
        }
    }

    fn close(&self) {
        for interceptor in &self.interceptors {
            if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
                tracing::error!(
                    interceptor = ?interceptor,
                    "ConsumerInterceptor.close panicked: {:?}", e,
                );
            }
        }
    }
}

/// Panic-safe wrapper for producer interceptor `on_send`.
///
/// Catches panics from user-provided interceptor code so that a misbehaving
/// interceptor cannot crash the producer.
pub(crate) fn safe_on_send(interceptor: &dyn ProducerInterceptor, record: &mut ProducerRecord) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.on_send(record))) {
        tracing::error!("ProducerInterceptor.on_send panicked: {:?}", e);
    }
}

/// Panic-safe wrapper for producer interceptor `on_acknowledgement`.
pub(crate) fn safe_on_acknowledgement(
    interceptor: &dyn ProducerInterceptor,
    metadata: &RecordMetadata,
    error: Option<&KrafkaError>,
) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| {
        interceptor.on_acknowledgement(metadata, error);
    })) {
        tracing::error!("ProducerInterceptor.on_acknowledgement panicked: {:?}", e);
    }
}

/// Panic-safe wrapper for producer interceptor `close`.
pub(crate) fn safe_producer_close(interceptor: &dyn ProducerInterceptor) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
        tracing::error!("ProducerInterceptor.close panicked: {:?}", e);
    }
}

/// Panic-safe wrapper for consumer interceptor `on_consume`.
pub(crate) fn safe_on_consume(interceptor: &dyn ConsumerInterceptor, records: &[ConsumerRecord]) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(records))) {
        tracing::error!("ConsumerInterceptor.on_consume panicked: {:?}", e);
    }
}

/// Panic-safe wrapper for consumer interceptor `on_commit`.
pub(crate) fn safe_on_commit(
    interceptor: &dyn ConsumerInterceptor,
    offsets: &HashMap<(String, PartitionId), Offset>,
    error: Option<&KrafkaError>,
) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.on_commit(offsets, error))) {
        tracing::error!("ConsumerInterceptor.on_commit panicked: {:?}", e);
    }
}

/// Panic-safe wrapper for consumer interceptor `close`.
pub(crate) fn safe_consumer_close(interceptor: &dyn ConsumerInterceptor) {
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| interceptor.close())) {
        tracing::error!("ConsumerInterceptor.close panicked: {:?}", e);
    }
}

#[cfg(test)]
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
        fn on_send(&self, record: &mut ProducerRecord) {
            self.send_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Add a tracing header
            record
                .headers
                .push(("x-intercepted".to_string(), b"true".to_vec()));
        }

        fn on_acknowledgement(&self, _metadata: &RecordMetadata, _error: Option<&KrafkaError>) {
            self.ack_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        fn on_consume(&self, records: &[ConsumerRecord]) {
            self.consume_count
                .fetch_add(records.len(), std::sync::atomic::Ordering::Relaxed);
        }

        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) {
            self.commit_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn test_producer_interceptor_on_send() {
        let interceptor = TestProducerInterceptor::new();
        let mut record = ProducerRecord::new("test-topic", b"value".to_vec());
        assert_eq!(interceptor.send_count(), 0);
        assert!(record.headers.is_empty());

        interceptor.on_send(&mut record);

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

        interceptor.on_acknowledgement(&metadata, None);
        assert_eq!(interceptor.ack_count(), 1);

        let err = KrafkaError::config("test error");
        interceptor.on_acknowledgement(&metadata, Some(&err));
        assert_eq!(interceptor.ack_count(), 2);
    }

    #[test]
    fn test_consumer_interceptor_on_consume() {
        let interceptor = TestConsumerInterceptor::new();
        let records = vec![
            ConsumerRecord::new("test-topic", 0, 0, None, Some(bytes::Bytes::from("v1"))),
            ConsumerRecord::new("test-topic", 0, 1, None, Some(bytes::Bytes::from("v2"))),
        ];

        interceptor.on_consume(&records);
        assert_eq!(interceptor.consume_count(), 2);
    }

    #[test]
    fn test_consumer_interceptor_on_commit() {
        let interceptor = TestConsumerInterceptor::new();
        let mut offsets = HashMap::new();
        offsets.insert(("test-topic".to_string(), 0), 10i64);

        interceptor.on_commit(&offsets, None);
        assert_eq!(interceptor.commit_count(), 1);
    }

    #[test]
    fn test_noop_interceptors() {
        let producer_interceptor = NoOpProducerInterceptor;
        let mut record = ProducerRecord::new("test", b"value".to_vec());
        producer_interceptor.on_send(&mut record);
        assert!(record.headers.is_empty());

        let consumer_interceptor = NoOpConsumerInterceptor;
        consumer_interceptor.on_consume(&[]);
        consumer_interceptor.on_commit(&HashMap::new(), None);
    }

    // --- Panic-safety tests ---

    #[derive(Debug)]
    struct PanickingProducerInterceptor;

    impl ProducerInterceptor for PanickingProducerInterceptor {
        fn on_send(&self, _record: &mut ProducerRecord) {
            panic!("on_send panic");
        }
        fn on_acknowledgement(&self, _metadata: &RecordMetadata, _error: Option<&KrafkaError>) {
            panic!("on_acknowledgement panic");
        }
        fn close(&self) {
            panic!("producer close panic");
        }
    }

    #[derive(Debug)]
    struct PanickingConsumerInterceptor;

    impl ConsumerInterceptor for PanickingConsumerInterceptor {
        fn on_consume(&self, _records: &[ConsumerRecord]) {
            panic!("on_consume panic");
        }
        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) {
            panic!("on_commit panic");
        }
        fn close(&self) {
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
        p.close();

        let c = NoOpConsumerInterceptor;
        c.close();
    }

    // --- Interceptor chain tests ---

    /// Records the order of interceptor invocations into a shared log.
    #[derive(Debug)]
    struct OrderedProducerInterceptor {
        name: &'static str,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ProducerInterceptor for OrderedProducerInterceptor {
        fn on_send(&self, _record: &mut ProducerRecord) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_send", self.name));
        }

        fn on_acknowledgement(&self, _metadata: &RecordMetadata, _error: Option<&KrafkaError>) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_ack", self.name));
        }

        fn close(&self) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.close", self.name));
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
        chain.on_send(&mut record);

        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        chain.on_acknowledgement(&metadata, None);
        chain.close();

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
            fn on_send(&self, record: &mut ProducerRecord) {
                record
                    .headers
                    .push((self.0.to_string(), self.0.as_bytes().to_vec()));
            }
        }

        let chain = ProducerInterceptorChain::new(vec![
            Arc::new(HeaderAdder("first")),
            Arc::new(HeaderAdder("second")),
        ]);

        let mut record = ProducerRecord::new("test", b"value".to_vec());
        chain.on_send(&mut record);

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
        chain.on_send(&mut record);

        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        chain.on_acknowledgement(&metadata, None);
        chain.close();

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
        chain.on_send(&mut record);
        chain.close();
    }

    #[derive(Debug)]
    struct OrderedConsumerInterceptor {
        name: &'static str,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ConsumerInterceptor for OrderedConsumerInterceptor {
        fn on_consume(&self, _records: &[ConsumerRecord]) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_consume", self.name));
        }

        fn on_commit(
            &self,
            _offsets: &HashMap<(String, PartitionId), Offset>,
            _error: Option<&KrafkaError>,
        ) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.on_commit", self.name));
        }

        fn close(&self) {
            self.log
                .lock()
                .unwrap()
                .push(format!("{}.close", self.name));
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

        chain.on_consume(&[]);
        chain.on_commit(&HashMap::new(), None);
        chain.close();

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

        chain.on_consume(&[]);
        chain.on_commit(&HashMap::new(), None);
        chain.close();

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
        chain.on_consume(&[]);
        chain.on_commit(&HashMap::new(), None);
        chain.close();
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
}
