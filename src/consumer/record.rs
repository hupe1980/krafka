//! Consumer record types.

use std::sync::Arc;

use bytes::Bytes;

pub use crate::protocol::TimestampType;
use crate::{Headers, Offset, PartitionId, Timestamp};

/// A record consumed from Kafka.
#[non_exhaustive]
#[must_use = "contains data consumed from Kafka"]
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Topic name, shared by every record of the topic in one fetch.
    pub topic: Arc<str>,
    /// Partition.
    pub partition: PartitionId,
    /// Offset within the partition.
    pub offset: Offset,
    /// Timestamp (milliseconds since the epoch).
    pub timestamp: Timestamp,
    /// Whether `timestamp` is the producer's create time or the broker's
    /// log-append time.
    pub timestamp_type: TimestampType,
    /// Record key.
    pub key: Option<Bytes>,
    /// Record value.
    pub value: Option<Bytes>,
    /// Headers in wire order, duplicates and null values kept — the same type
    /// a produced [`Record`](crate::Record) carries. A key that is not valid
    /// UTF-8 is decoded lossily (U+FFFD), as Java's `Header.key()` does.
    pub headers: Headers,
    /// Leader epoch.
    pub leader_epoch: Option<i32>,
    /// How many times a share group delivered this record (KIP-932); `None`
    /// for records from a [`Consumer`](super::Consumer).
    pub delivery_count: Option<i16>,
}

impl ConsumerRecord {
    /// Create a new consumer record.
    pub fn new(
        topic: impl Into<Arc<str>>,
        partition: PartitionId,
        offset: Offset,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp: 0,
            timestamp_type: TimestampType::CreateTime,
            key,
            value,
            headers: Headers::new(),
            leader_epoch: None,
            delivery_count: None,
        }
    }

    /// Returns `true` if this record is a tombstone (delete marker): a key
    /// and no value. Log compaction removes older records for that key.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.key.is_some() && self.value.is_none()
    }

    /// Serialized key size in bytes, or `None` if the key is absent.
    #[inline]
    pub fn serialized_key_size(&self) -> Option<usize> {
        self.key.as_ref().map(|k| k.len())
    }

    /// Serialized value size in bytes, or `None` if the value is absent.
    #[inline]
    pub fn serialized_value_size(&self) -> Option<usize> {
        self.value.as_ref().map(|v| v.len())
    }

    /// Get the key as a string if present.
    #[inline]
    pub fn key_str(&self) -> Option<&str> {
        self.key.as_ref().and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Get the value as a string if present.
    #[inline]
    pub fn value_str(&self) -> Option<&str> {
        self.value
            .as_ref()
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// The first header named `key`: `Some(Some(value))`, `Some(None)` for a
    /// null value, `None` when there is no such header.
    #[inline]
    pub fn header(&self, key: &str) -> Option<Option<&Bytes>> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
    }

    /// The first header named `key` as a string; `None` when it is missing,
    /// null or not UTF-8.
    #[inline]
    pub fn header_str(&self, key: &str) -> Option<&str> {
        self.header(key)
            .flatten()
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Every value of the headers named `key`, nulls included, in order.
    #[inline]
    pub fn headers_by_key(&self, key: &str) -> Vec<Option<&Bytes>> {
        self.headers
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
            .collect()
    }
}

/// Decode a wire header key: lossily when it is not UTF-8.
pub(crate) fn header_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

/// Convert decoded wire headers to [`Headers`].
pub(crate) fn headers_from_wire(headers: Vec<crate::protocol::RecordHeader>) -> Headers {
    headers
        .into_iter()
        .map(|h| (header_key(&h.key), h.value))
        .collect()
}

/// Represents a topic-partition pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct TopicPartition {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
}

impl TopicPartition {
    /// Create a new topic-partition reference.
    pub fn new(topic: impl Into<String>, partition: PartitionId) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    /// Get the topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the partition.
    #[inline]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_record_new() {
        let record = ConsumerRecord::new(
            "test-topic",
            0,
            42,
            Some(Bytes::from("key")),
            Some(Bytes::from("value")),
        );

        assert_eq!(&*record.topic, "test-topic");
        assert_eq!(record.partition, 0);
        assert_eq!(record.offset, 42);
        assert_eq!(record.key_str(), Some("key"));
        assert_eq!(record.value_str(), Some("value"));
        assert_eq!(record.serialized_key_size(), Some(3));
        assert_eq!(record.serialized_value_size(), Some(5));
    }

    #[test]
    fn test_consumer_record_serialized_sizes_absent() {
        let record = ConsumerRecord::new("topic", 0, 0, None, None);
        assert_eq!(record.serialized_key_size(), None);
        assert_eq!(record.serialized_value_size(), None);
    }

    #[test]
    fn test_consumer_record_is_tombstone() {
        // Key + no value → tombstone
        let tombstone = ConsumerRecord::new("t", 0, 0, Some(Bytes::from("key")), None);
        assert!(tombstone.is_tombstone());

        // Key + value → not a tombstone
        let normal = ConsumerRecord::new(
            "t",
            0,
            0,
            Some(Bytes::from("key")),
            Some(Bytes::from("val")),
        );
        assert!(!normal.is_tombstone());

        // No key + no value → not a tombstone (keyless record)
        let keyless = ConsumerRecord::new("t", 0, 0, None, None);
        assert!(!keyless.is_tombstone());

        // No key + value → not a tombstone
        let no_key = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("val")));
        assert!(!no_key.is_tombstone());
    }

    #[test]
    fn test_consumer_record_duplicate_headers_preserved() {
        let mut record = ConsumerRecord::new("test-topic", 0, 0, None, Some(Bytes::from("value")));

        record
            .headers
            .push(("trace-id".to_string(), Some(Bytes::from("abc"))));
        record
            .headers
            .push(("trace-id".to_string(), Some(Bytes::from("def"))));
        record
            .headers
            .push(("other".to_string(), Some(Bytes::from("xyz"))));

        // Both duplicates should be preserved
        assert_eq!(
            record.headers.len(),
            3,
            "all headers including duplicates should be preserved"
        );

        // header() returns the first match
        assert_eq!(
            record.header("trace-id"),
            Some(Some(&Bytes::from("abc"))),
            "header() should return the first matching header value"
        );
    }

    #[test]
    fn test_consumer_record_headers_by_key() {
        let mut record = ConsumerRecord::new("test-topic", 0, 0, None, Some(Bytes::from("value")));

        record
            .headers
            .push(("trace-id".to_string(), Some(Bytes::from("first"))));
        record
            .headers
            .push(("trace-id".to_string(), Some(Bytes::from("second"))));
        record
            .headers
            .push(("trace-id".to_string(), Some(Bytes::from("third"))));
        record
            .headers
            .push(("other-key".to_string(), Some(Bytes::from("other"))));

        let trace_values = record.headers_by_key("trace-id");
        assert_eq!(
            trace_values.len(),
            3,
            "headers_by_key should return all values for a duplicate key"
        );
        assert_eq!(trace_values[0], Some(&Bytes::from("first")));
        assert_eq!(trace_values[1], Some(&Bytes::from("second")));
        assert_eq!(trace_values[2], Some(&Bytes::from("third")));

        let other_values = record.headers_by_key("other-key");
        assert_eq!(other_values.len(), 1);

        let missing_values = record.headers_by_key("nonexistent");
        assert!(
            missing_values.is_empty(),
            "headers_by_key for missing key should return empty vec"
        );
    }

    // ── null header values ──

    #[test]
    fn test_consumer_record_header_with_null_value() {
        let mut record = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("v")));
        record.headers.push(("x-null".to_string(), None));
        record
            .headers
            .push(("x-present".to_string(), Some(Bytes::from("data"))));

        // header() returns Some(None) for a null-valued header
        assert_eq!(record.header("x-null"), Some(None));
        // header() returns Some(Some(&bytes)) for a present-valued header
        assert_eq!(record.header("x-present"), Some(Some(&Bytes::from("data"))));
        // header() returns None for a missing key
        assert_eq!(record.header("missing"), None);
    }

    #[test]
    fn test_consumer_record_header_str_returns_none_for_null() {
        let mut record = ConsumerRecord::new("t", 0, 0, None, None);
        record.headers.push(("h".to_string(), None));
        record
            .headers
            .push(("h2".to_string(), Some(Bytes::from("text"))));

        // null header → None
        assert_eq!(record.header_str("h"), None);
        // present header with valid UTF-8 → Some(str)
        assert_eq!(record.header_str("h2"), Some("text"));
    }

    #[test]
    fn test_consumer_record_headers_by_key_with_nulls() {
        let mut record = ConsumerRecord::new("t", 0, 0, None, None);
        record
            .headers
            .push(("k".to_string(), Some(Bytes::from("a"))));
        record.headers.push(("k".to_string(), None));
        record
            .headers
            .push(("k".to_string(), Some(Bytes::from("b"))));

        let vals = record.headers_by_key("k");
        assert_eq!(vals.len(), 3);
        assert_eq!(vals[0], Some(&Bytes::from("a")));
        assert_eq!(vals[1], None);
        assert_eq!(vals[2], Some(&Bytes::from("b")));
    }

    /// Header keys are `String`s; a key that is not UTF-8 is decoded lossily
    /// and the record is still delivered. Negative control: a strict
    /// `String::from_utf8(..).unwrap()` in `header_key` panics here.
    #[test]
    fn a_non_utf8_header_key_is_decoded_lossily() {
        let headers = headers_from_wire(vec![crate::protocol::RecordHeader::new(
            Bytes::from_static(b"tr\xffce"),
            Bytes::from_static(b"v"),
        )]);
        assert_eq!(headers[0].0, "tr\u{fffd}ce");
        assert_eq!(headers[0].1.as_deref(), Some(&b"v"[..]));
    }
}
