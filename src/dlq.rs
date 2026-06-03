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
//! use krafka::dlq::DeadLetterQueue;
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
//!             "__krafka.dlq.exception.message".to_string(),
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
//! | Header | Value |
//! |--------|-------|
//! | `__krafka.dlq.original.topic` | original topic name |
//! | `__krafka.dlq.original.partition` | partition number |
//! | `__krafka.dlq.original.offset` | record offset |
//! | `__krafka.dlq.exception.message` | error description |
//!
//! ```rust,ignore
//! use krafka::dlq::{DeadLetterQueue, build_dlq_record};
//! use krafka::consumer::ConsumerRecord;
//!
//! async fn process(record: ConsumerRecord, dlq: &dyn DeadLetterQueue) {
//!     let dlq_record = build_dlq_record("my-topic.DLQ", &record, &"deserialization failed");
//!     dlq.send(dlq_record, krafka::error::KrafkaError::invalid_state("bad payload")).await;
//! }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::consumer::ConsumerRecord;
use crate::producer::ProducerRecord;

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
    let mut headers: Vec<(String, Bytes)> = original
        .headers
        .iter()
        .map(|(k, v)| {
            (
                String::from_utf8(k.to_vec()).unwrap_or_else(|_| {
                    use std::fmt::Write;
                    let mut s = String::with_capacity(4 + k.len() * 2);
                    s.push_str("hex:");
                    for byte in k.iter() {
                        let _ = write!(s, "{byte:02x}");
                    }
                    s
                }),
                v.clone().unwrap_or_default(),
            )
        })
        .collect();

    // Append provenance headers after original headers.
    headers.push((
        "__krafka.dlq.original.topic".to_string(),
        Bytes::from(original.topic.clone()),
    ));
    headers.push((
        "__krafka.dlq.original.partition".to_string(),
        Bytes::from(original.partition.to_string()),
    ));
    headers.push((
        "__krafka.dlq.original.offset".to_string(),
        Bytes::from(original.offset.to_string()),
    ));
    headers.push((
        "__krafka.dlq.exception.message".to_string(),
        Bytes::from(error.to_string()),
    ));

    ProducerRecord {
        topic: dlq_topic.to_string(),
        partition: None,
        key: original.key.clone(),
        value: original.value.clone().unwrap_or_default(),
        timestamp: None,
        headers,
        record_name: None,
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
        assert_eq!(record.value, Bytes::from("value"));

        let hdr = |name: &str| -> Option<Bytes> {
            record
                .headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
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
        assert_eq!(record.headers[0].1, Bytes::from("abc123"));
        // DLQ provenance headers follow.
        assert!(
            record
                .headers
                .iter()
                .any(|(k, _)| k == "__krafka.dlq.original.topic")
        );
    }

    #[test]
    fn test_build_dlq_record_no_value() {
        let original = ConsumerRecord::new("t", 0, 0, None, None);
        let record = build_dlq_record("t.DLQ", &original, &"tombstone");
        assert_eq!(record.value, Bytes::new());
    }
}
