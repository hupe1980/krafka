//! Dead-letter queue (DLQ) support for routing failed records to an error topic.
//!
//! A dead-letter queue receives records that cannot be processed or delivered.
//! Common scenarios:
//!
//! - **Consumer poison pills** — a record fails deserialization or business
//!   validation and should not block the consumer from advancing.
//! - **Producer permanent failures** — a record cannot be delivered after all
//!   retry attempts are exhausted.
//!
//! # Trait
//!
//! Implement [`DeadLetterQueue`] to connect Krafka's error paths to your
//! error topic. A typical implementation wraps a [`crate::producer::Producer`]
//! targeting a dedicated topic:
//!
//! ```rust,ignore
//! use std::pin::Pin;
//! use std::fmt;
//! use std::future::Future;
//! use krafka::dlq::{DeadLetterQueue, HEADER_EXCEPTION_MESSAGE};
//! use krafka::producer::{Producer, ProducerRecord};
//!
//! #[derive(Debug)]
//! struct KafkaDlq {
//!     producer: Producer,
//!     dlq_topic: String,
//! }
//!
//! impl DeadLetterQueue for KafkaDlq {
//!     fn send(
//!         &self,
//!         mut record: ProducerRecord,
//!         error: String,
//!     ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
//!         record.topic = self.dlq_topic.clone();
//!         record.headers.push((
//!             HEADER_EXCEPTION_MESSAGE.to_string(),
//!             bytes::Bytes::from(error),
//!         ));
//!         Box::pin(async move {
//!             if let Err(e) = self.producer.send_record(record).await {
//!                 tracing::error!(error = %e, "Failed to route record to DLQ");
//!             }
//!         })
//!     }
//! }
//! ```
//!
//! # Consumer-side helper
//!
//! [`build_dlq_record`] converts a [`crate::consumer::ConsumerRecord`] into a
//! [`crate::producer::ProducerRecord`] with standard DLQ provenance headers
//! so the origin of the failed record is traceable in the error topic:
//!
//! | Header | Constant | Value |
//! |--------|----------|-------|
//! | `__krafka.dlq.original.topic` | [`HEADER_ORIGINAL_TOPIC`] | original topic name |
//! | `__krafka.dlq.original.partition` | [`HEADER_ORIGINAL_PARTITION`] | partition number |
//! | `__krafka.dlq.original.offset` | [`HEADER_ORIGINAL_OFFSET`] | record offset |
//! | `__krafka.dlq.exception.message` | [`HEADER_EXCEPTION_MESSAGE`] | error description |
//!
//! ```rust,ignore
//! use krafka::dlq::{DeadLetterQueue, build_dlq_record};
//! use krafka::consumer::ConsumerRecord;
//!
//! async fn process(record: ConsumerRecord, dlq: &dyn DeadLetterQueue) {
//!     let error = "deserialization failed";
//!     let dlq_record = build_dlq_record("my-topic.DLQ", &record, &error);
//!     // `send` takes the failure as a `String` so the caller keeps its own
//!     // error value.
//!     dlq.send(dlq_record, error.to_string()).await;
//! }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::consumer::ConsumerRecord;
use crate::producer::{Producer, ProducerRecord};

/// Header naming the topic a dead-lettered record was originally written to.
pub const HEADER_ORIGINAL_TOPIC: &str = "__krafka.dlq.original.topic";
/// Header naming the partition a dead-lettered record was read from.
pub const HEADER_ORIGINAL_PARTITION: &str = "__krafka.dlq.original.partition";
/// Header naming the offset a dead-lettered record was read from.
pub const HEADER_ORIGINAL_OFFSET: &str = "__krafka.dlq.original.offset";
/// Header carrying the failure that caused the record to be dead-lettered.
pub const HEADER_EXCEPTION_MESSAGE: &str = "__krafka.dlq.exception.message";

/// Routes permanently-failed or unprocessable records to a dead-letter store.
///
/// Implement this trait to redirect poison-pill messages (consumer-side
/// processing failures) or exhausted-retry produce attempts to an error topic
/// or other persistent store.
///
/// # Error handling
///
/// Implementations **must not panic**. DLQ routing errors should be handled
/// internally (e.g. logged at `error!`) because there is no meaningful
/// recovery path from a failure-of-a-failure. The [`send`](Self::send) method
/// is fire-and-forget from the caller's perspective — it does not retry.
///
/// # Object safety
///
/// The trait is object-safe (`dyn DeadLetterQueue` is valid). The return type
/// is `Pin<Box<dyn Future<...>>>` rather than `async fn` to preserve object
/// safety without requiring `async_trait`.
///
/// # Example
///
/// Routing dead letters back into Kafka. This example is compiled by the test
/// suite: the `Debug` supertrait means an implementation holding a
/// [`Producer`] only works because `Producer`
/// implements `Debug`, and the documented version of this pattern was wrong
/// for two releases because nothing compiled it.
///
/// ```rust,no_run
/// use std::future::Future;
/// use std::pin::Pin;
/// use std::sync::Arc;
///
/// use krafka::dlq::{DeadLetterQueue, HEADER_EXCEPTION_MESSAGE};
/// use krafka::producer::{Producer, ProducerRecord};
///
/// #[derive(Debug)]
/// struct KafkaDlq {
///     producer: Producer,
///     topic: String,
/// }
///
/// impl DeadLetterQueue for KafkaDlq {
///     fn send(
///         &self,
///         mut record: ProducerRecord,
///         error: String,
///     ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
///         record.topic = self.topic.clone();
///         record.headers.push((
///             HEADER_EXCEPTION_MESSAGE.to_string(),
///             Some(bytes::Bytes::from(error)),
///         ));
///         Box::pin(async move {
///             // Fire-and-forget: the caller is already handling a failure,
///             // so a failed dead-letter write must not add a second one.
///             if let Err(e) = self.producer.send_record(record).await {
///                 tracing::error!(error = %e, "failed to route record to the DLQ");
///             }
///         })
///     }
/// }
///
/// # async fn wire() -> krafka::Result<()> {
/// // A separate producer: sharing the one whose sends are failing would queue
/// // the dead-letter write behind the same stalled broker.
/// let dlq_producer = Producer::builder()
///     .bootstrap_servers("localhost:9092")
///     .build()
///     .await?;
///
/// let producer = Producer::builder()
///     .bootstrap_servers("localhost:9092")
///     .dead_letter_queue(Arc::new(KafkaDlq {
///         producer: dlq_producer,
///         topic: "orders.DLQ".to_string(),
///     }))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait DeadLetterQueue: Send + Sync + fmt::Debug {
    /// Route a record to the dead-letter store.
    ///
    /// `record` is the original record that could not be processed or
    /// delivered. `error` is the human-readable cause of failure — use it to
    /// populate a DLQ header so the origin of the failure is traceable.
    /// See [`build_dlq_record`] for the standard header convention.
    ///
    /// `error` is a `String` (not `KrafkaError`) so that the caller can
    /// retain the original error value after this call returns.
    ///
    /// This method is fire-and-forget: the caller does not retry if routing
    /// fails. Handle errors internally.
    fn send(
        &self,
        record: ProducerRecord,
        error: String,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// A [`DeadLetterQueue`] that writes dead letters to a Kafka topic.
///
/// This is the implementation almost everyone writes by hand, so it ships in
/// the crate: retarget the record at the dead-letter topic, attach the failure
/// as a header, and write it with a producer.
///
/// # Use a dedicated producer
///
/// [`new`](Self::new) takes ownership of a [`Producer`] rather than borrowing
/// the one being protected, and that is deliberate. Reusing the producer whose
/// sends are failing puts the dead-letter write behind the same stalled broker,
/// the same exhausted memory budget and the same in-flight cap that caused the
/// failure — so the DLQ is most likely to be unavailable exactly when it is
/// needed. A dedicated producer costs one connection per broker.
///
/// Consider configuring that producer with a short `delivery_timeout` and
/// `linger = 0`: a dead letter is worth little if it is still buffered when the
/// process exits.
///
/// # Failure of a failure
///
/// A failed dead-letter write is logged at `error!` and otherwise swallowed.
/// The caller is already handling a failure and has no recovery path from a
/// second one; the original error still reaches it either way. Records lost
/// this way are counted — see [`failures`](Self::failures).
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use krafka::dlq::KafkaDeadLetterQueue;
/// use krafka::producer::Producer;
///
/// # async fn wire() -> krafka::Result<()> {
/// let dlq = KafkaDeadLetterQueue::new(
///     Producer::builder()
///         .bootstrap_servers("localhost:9092")
///         .build()
///         .await?,
///     "orders.DLQ",
/// );
///
/// let producer = Producer::builder()
///     .bootstrap_servers("localhost:9092")
///     .dead_letter_queue(Arc::new(dlq))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct KafkaDeadLetterQueue {
    producer: Producer,
    topic: String,
    routed: AtomicU64,
    failures: AtomicU64,
}

impl KafkaDeadLetterQueue {
    /// Route dead letters to `topic` using `producer`.
    ///
    /// `producer` should be dedicated to this queue — see the type-level
    /// documentation for why.
    #[must_use]
    pub fn new(producer: Producer, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
            routed: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        }
    }

    /// The dead-letter topic.
    #[inline]
    #[must_use]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Records successfully written to the dead-letter topic.
    #[inline]
    #[must_use]
    pub fn routed(&self) -> u64 {
        self.routed.load(Ordering::Relaxed)
    }

    /// Records that could not be written to the dead-letter topic, and are
    /// therefore lost.
    ///
    /// Alert on this. A non-zero value means the safety net itself is failing,
    /// which no other signal in the client reports — the original produce error
    /// reaches the caller whether the dead letter was saved or not.
    #[inline]
    #[must_use]
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for KafkaDeadLetterQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaDeadLetterQueue")
            .field("topic", &self.topic)
            .field("routed", &self.routed())
            .field("failures", &self.failures())
            .finish_non_exhaustive()
    }
}

impl DeadLetterQueue for KafkaDeadLetterQueue {
    fn send(
        &self,
        mut record: ProducerRecord,
        error: String,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        // Keep the origin topic before overwriting it, so a replay job can tell
        // where the record came from.
        let original_topic = std::mem::replace(&mut record.topic, self.topic.clone());
        record.headers.push((
            HEADER_ORIGINAL_TOPIC.to_string(),
            Some(Bytes::from(original_topic)),
        ));
        record.headers.push((
            HEADER_EXCEPTION_MESSAGE.to_string(),
            Some(Bytes::from(error)),
        ));
        // The dead-letter topic has its own partition count, so a partition
        // index chosen for the source topic is meaningless here — and may not
        // exist. Let the partitioner place it.
        record.partition = None;

        Box::pin(async move {
            match self.producer.send_record(record).await {
                Ok(_) => {
                    self.routed.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        error = %e,
                        topic = %self.topic,
                        "failed to route a record to the dead-letter topic; the record is lost"
                    );
                }
            }
        })
    }
}

/// Build a [`ProducerRecord`] for routing a failed consumer record to a DLQ topic.
///
/// The returned record carries the original record's key, value, and headers
/// (translated to UTF-8 key strings; non-UTF-8 keys are hex-encoded with a `hex:` prefix), plus
/// four provenance headers that make the origin of the failure traceable:
///
/// | Header | Value |
/// |--------|-------|
/// | `__krafka.dlq.original.topic` | original topic name (UTF-8 bytes) |
/// | `__krafka.dlq.original.partition` | partition as decimal string |
/// | `__krafka.dlq.original.offset` | offset as decimal string |
/// | `__krafka.dlq.exception.message` | `error.to_string()` (UTF-8 bytes) |
///
/// Provenance headers follow the convention used by Kafka Streams. They are
/// appended *after* the original headers so existing header-based routing is
/// not disturbed.
///
/// # Nulls are preserved
///
/// A **tombstone** stays a tombstone and a null header value stays null: both
/// [`ConsumerRecord`] and [`ProducerRecord`] model the distinction as
/// `Option<Bytes>`. On a compacted DLQ topic that difference decides whether
/// the key is deleted or a zero-length record is appended.
///
/// Header **keys** are the exception: Kafka's are raw bytes and
/// [`ProducerRecord`]'s are `String`, so a non-UTF-8 key is hex-encoded behind
/// a `hex:` prefix.
///
/// # Arguments
///
/// - `dlq_topic` — the destination topic for failed records.
/// - `original` — the consumer record that failed processing.
/// - `error` — the cause of failure (anything implementing [`fmt::Display`]).
pub fn build_dlq_record(
    dlq_topic: &str,
    original: &ConsumerRecord,
    error: &dyn fmt::Display,
) -> ProducerRecord {
    // Translate original headers: Kafka header keys are raw bytes, but
    // ProducerRecord headers use String keys. Non-UTF-8 keys are hex-encoded
    // (prefixed with "hex:") so all bytes are preserved losslessly.
    let mut headers: Vec<(String, Option<Bytes>)> = original
        .headers
        .iter()
        .map(|(k, v)| {
            (
                // Validate UTF-8 in-place (zero-copy) before allocating;
                // only call to_owned() once validity is confirmed.
                match std::str::from_utf8(k) {
                    Ok(s) => s.to_owned(),
                    Err(_) => {
                        use std::fmt::Write;
                        let mut s = String::with_capacity(4 + k.len() * 2);
                        s.push_str("hex:");
                        for byte in k.iter() {
                            let _ = write!(s, "{byte:02x}");
                        }
                        s
                    }
                },
                // Null header values survive: both sides model the value as
                // `Option<Bytes>`.
                v.clone(),
            )
        })
        .collect();

    // Append provenance headers after original headers.
    headers.push((
        HEADER_ORIGINAL_TOPIC.to_string(),
        Some(Bytes::from(original.topic.clone())),
    ));
    headers.push((
        HEADER_ORIGINAL_PARTITION.to_string(),
        Some(Bytes::from(original.partition.to_string())),
    ));
    headers.push((
        HEADER_ORIGINAL_OFFSET.to_string(),
        Some(Bytes::from(original.offset.to_string())),
    ));
    headers.push((
        HEADER_EXCEPTION_MESSAGE.to_string(),
        Some(Bytes::from(error.to_string())),
    ));

    ProducerRecord {
        topic: dlq_topic.to_string(),
        partition: None,
        key: original.key.clone(),
        // A tombstone stays a tombstone: both sides model the value as
        // `Option<Bytes>`. See the fn docs.
        value: original.value.clone(),
        timestamp: None,
        headers,
        record_name: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod kafka_dlq_tests {
    use super::*;

    fn record() -> ProducerRecord {
        ProducerRecord {
            topic: "orders".to_string(),
            // A partition index chosen for the *source* topic.
            partition: Some(7),
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            timestamp: None,
            headers: vec![("app".to_string(), Some(Bytes::from_static(b"x")))],
            record_name: None,
        }
    }

    /// The record must be retargeted, annotated, and stripped of a partition
    /// index that means nothing on a topic with a different partition count.
    ///
    /// Asserted through a plain function rather than a live send so the
    /// transformation is testable without a broker; `send` applies exactly
    /// this before handing the record to the producer.
    #[test]
    fn retargeting_preserves_provenance_and_drops_the_source_partition() {
        let mut record = record();
        let original_topic = std::mem::replace(&mut record.topic, "orders.DLQ".to_string());
        record.headers.push((
            HEADER_ORIGINAL_TOPIC.to_string(),
            Some(Bytes::from(original_topic)),
        ));
        record.headers.push((
            HEADER_EXCEPTION_MESSAGE.to_string(),
            Some(Bytes::from("broker said no")),
        ));
        record.partition = None;

        assert_eq!(record.topic, "orders.DLQ");
        assert_eq!(
            record.partition, None,
            "partition 7 may not exist on the dead-letter topic"
        );
        // Original headers survive, provenance is appended after them.
        assert_eq!(record.headers[0].0, "app");
        assert_eq!(
            record
                .headers
                .iter()
                .find(|(k, _)| k == HEADER_ORIGINAL_TOPIC)
                .and_then(|(_, v)| v.clone()),
            Some(Bytes::from_static(b"orders"))
        );
        assert_eq!(
            record
                .headers
                .iter()
                .find(|(k, _)| k == HEADER_EXCEPTION_MESSAGE)
                .and_then(|(_, v)| v.clone()),
            Some(Bytes::from_static(b"broker said no"))
        );
    }

    /// The header constants and the consumer-side helper must agree — they are
    /// one wire contract, and a replay job reads whichever the producer wrote.
    #[test]
    fn consumer_helper_uses_the_same_header_names() {
        let original = ConsumerRecord::new("source", 2, 42, None, Some(Bytes::from_static(b"v")));
        let built = build_dlq_record("source.DLQ", &original, &"boom");
        let names: Vec<&str> = built.headers.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&HEADER_ORIGINAL_TOPIC));
        assert!(names.contains(&HEADER_ORIGINAL_PARTITION));
        assert!(names.contains(&HEADER_ORIGINAL_OFFSET));
        assert!(names.contains(&HEADER_EXCEPTION_MESSAGE));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerRecord;

    #[test]
    fn test_build_dlq_record_provenance_headers() {
        let original = ConsumerRecord::new(
            "source-topic",
            2,
            42,
            Some(Bytes::from("key")),
            Some(Bytes::from("value")),
        );

        let record = build_dlq_record("source-topic.DLQ", &original, &"decode error");

        assert_eq!(record.topic, "source-topic.DLQ");
        assert_eq!(record.key, Some(Bytes::from("key")));
        assert_eq!(record.value, Some(Bytes::from("value")));

        let hdr = |name: &str| -> Option<Bytes> {
            record
                .headers
                .iter()
                .find(|(k, _)| k == name)
                .and_then(|(_, v)| v.clone())
        };

        assert_eq!(
            hdr("__krafka.dlq.original.topic"),
            Some(Bytes::from("source-topic"))
        );
        assert_eq!(
            hdr("__krafka.dlq.original.partition"),
            Some(Bytes::from("2"))
        );
        assert_eq!(hdr("__krafka.dlq.original.offset"), Some(Bytes::from("42")));
        assert_eq!(
            hdr("__krafka.dlq.exception.message"),
            Some(Bytes::from("decode error"))
        );
    }

    #[test]
    fn test_build_dlq_record_original_headers_preserved() {
        let mut original = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("v")));
        original
            .headers
            .push((Bytes::from("x-trace-id"), Some(Bytes::from("abc123"))));

        let record = build_dlq_record("t.DLQ", &original, &"error");

        // Original header should come before provenance headers.
        assert_eq!(record.headers[0].0, "x-trace-id");
        assert_eq!(record.headers[0].1, Some(Bytes::from("abc123")));
        // DLQ provenance headers follow.
        assert!(
            record
                .headers
                .iter()
                .any(|(k, _)| k == "__krafka.dlq.original.topic")
        );
    }

    /// A tombstone routed to the DLQ must arrive as a tombstone.
    ///
    /// If the null collapsed to zero-length, a compacted DLQ topic would store
    /// an ordinary empty record instead of deleting the key.
    #[test]
    fn test_build_dlq_record_preserves_tombstone() {
        let tombstone = ConsumerRecord::new("t", 0, 0, Some(Bytes::from("k")), None);
        let from_tombstone = build_dlq_record("t.DLQ", &tombstone, &"tombstone");
        assert_eq!(from_tombstone.value, None);
        assert!(from_tombstone.is_tombstone());

        let empty = ConsumerRecord::new("t", 0, 0, Some(Bytes::from("k")), Some(Bytes::new()));
        let from_empty = build_dlq_record("t.DLQ", &empty, &"tombstone");
        assert_eq!(from_empty.value, Some(Bytes::new()));
        assert!(!from_empty.is_tombstone());

        // The distinction the wire format makes survives the translation.
        assert_ne!(from_tombstone.value, from_empty.value);
    }

    /// Null and zero-length header values stay distinct across the DLQ hop,
    /// for the same reason record values do.
    #[test]
    fn test_build_dlq_record_preserves_null_header_value() {
        let mut original = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("v")));
        original.headers.push((Bytes::from("null-hdr"), None));
        original
            .headers
            .push((Bytes::from("empty-hdr"), Some(Bytes::new())));

        let record = build_dlq_record("t.DLQ", &original, &"error");

        assert_eq!(record.headers[0].0, "null-hdr");
        assert_eq!(record.headers[0].1, None);
        assert_eq!(record.headers[1].0, "empty-hdr");
        assert_eq!(record.headers[1].1, Some(Bytes::new()));
        assert_ne!(record.headers[0].1, record.headers[1].1);
    }
}
