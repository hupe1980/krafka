//! Dead-letter helper for consumer-side poison pills.
//!
//! When a consumed record cannot be processed, [`record_for`] turns it into a
//! [`Record`] for a dead-letter topic, carrying the original key,
//! value and headers plus provenance headers that make the origin traceable:
//!
//! | Header | Constant | Value |
//! |--------|----------|-------|
//! | `__krafka.dlq.original.topic` | [`HEADER_ORIGINAL_TOPIC`] | original topic name |
//! | `__krafka.dlq.original.partition` | [`HEADER_ORIGINAL_PARTITION`] | partition number |
//! | `__krafka.dlq.original.offset` | [`HEADER_ORIGINAL_OFFSET`] | record offset |
//! | `__krafka.dlq.exception.message` | [`HEADER_EXCEPTION_MESSAGE`] | error description |
//!
//! ```rust,no_run
//! use krafka::consumer::ConsumerRecord;
//! use krafka::producer::Producer;
//!
//! async fn dead_letter(dlq: &Producer, record: &ConsumerRecord, error: &str) -> krafka::Result<()> {
//!     dlq.send(krafka::dlq::record_for("orders.DLQ", record, &error)).await?;
//!     Ok(())
//! }
//! ```

use std::fmt;

use bytes::Bytes;

use crate::consumer::ConsumerRecord;
use crate::producer::Record;

/// Header naming the topic a dead-lettered record was originally written to.
pub const HEADER_ORIGINAL_TOPIC: &str = "__krafka.dlq.original.topic";
/// Header naming the partition a dead-lettered record was read from.
pub const HEADER_ORIGINAL_PARTITION: &str = "__krafka.dlq.original.partition";
/// Header naming the offset a dead-lettered record was read from.
pub const HEADER_ORIGINAL_OFFSET: &str = "__krafka.dlq.original.offset";
/// Header carrying the failure that caused the record to be dead-lettered.
pub const HEADER_EXCEPTION_MESSAGE: &str = "__krafka.dlq.exception.message";

/// Build a [`Record`] for routing a failed consumer record to a DLQ topic.
///
/// The returned record carries the original record's key, value, and headers
/// (the [`Headers`](crate::Headers) the consumer decoded), plus
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
/// [`ConsumerRecord`] and [`Record`] model the distinction as
/// `Option<Bytes>`. On a compacted DLQ topic that difference decides whether
/// the key is deleted or a zero-length record is appended.
///
/// # Arguments
///
/// - `dlq_topic` — the destination topic for failed records.
/// - `original` — the consumer record that failed processing.
/// - `error` — the cause of failure (anything implementing [`fmt::Display`]).
pub fn record_for(dlq_topic: &str, original: &ConsumerRecord, error: &dyn fmt::Display) -> Record {
    let mut headers = original.headers.clone();

    // Append provenance headers after original headers.
    headers.push((
        HEADER_ORIGINAL_TOPIC.to_string(),
        Some(Bytes::copy_from_slice(original.topic.as_bytes())),
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

    Record {
        topic: dlq_topic.to_string(),
        partition: None,
        key: original.key.clone(),
        // A tombstone stays a tombstone: both sides model the value as
        // `Option<Bytes>`. See the fn docs.
        value: original.value.clone(),
        timestamp: None,
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_for_provenance_headers() {
        let original = ConsumerRecord::new(
            "source-topic",
            2,
            42,
            Some(Bytes::from("key")),
            Some(Bytes::from("value")),
        );

        let record = record_for("source-topic.DLQ", &original, &"decode error");

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
    fn test_record_for_original_headers_preserved() {
        let mut original = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("v")));
        original
            .headers
            .push(("x-trace-id".to_string(), Some(Bytes::from("abc123"))));

        let record = record_for("t.DLQ", &original, &"error");

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
    fn test_record_for_preserves_tombstone() {
        let tombstone = ConsumerRecord::new("t", 0, 0, Some(Bytes::from("k")), None);
        let from_tombstone = record_for("t.DLQ", &tombstone, &"tombstone");
        assert_eq!(from_tombstone.value, None);
        assert!(from_tombstone.is_tombstone());

        let empty = ConsumerRecord::new("t", 0, 0, Some(Bytes::from("k")), Some(Bytes::new()));
        let from_empty = record_for("t.DLQ", &empty, &"tombstone");
        assert_eq!(from_empty.value, Some(Bytes::new()));
        assert!(!from_empty.is_tombstone());

        // The distinction the wire format makes survives the translation.
        assert_ne!(from_tombstone.value, from_empty.value);
    }

    /// Null and zero-length header values stay distinct across the DLQ hop,
    /// for the same reason record values do.
    #[test]
    fn test_record_for_preserves_null_header_value() {
        let mut original = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("v")));
        original.headers.push(("null-hdr".to_string(), None));
        original
            .headers
            .push(("empty-hdr".to_string(), Some(Bytes::new())));

        let record = record_for("t.DLQ", &original, &"error");

        assert_eq!(record.headers[0].0, "null-hdr");
        assert_eq!(record.headers[0].1, None);
        assert_eq!(record.headers[1].0, "empty-hdr");
        assert_eq!(record.headers[1].1, Some(Bytes::new()));
        assert_ne!(record.headers[0].1, record.headers[1].1);
    }
}
