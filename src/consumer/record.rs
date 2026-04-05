//! Consumer record types.

use std::collections::HashSet;

use bytes::Bytes;

use crate::{Offset, PartitionId, Timestamp};

/// A record consumed from Kafka.
#[non_exhaustive]
#[must_use = "contains data consumed from Kafka"]
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    /// Topic name.
    pub topic: String,
    /// Partition.
    pub partition: PartitionId,
    /// Offset within the partition.
    pub offset: Offset,
    /// Timestamp.
    pub timestamp: Timestamp,
    /// Timestamp type (0 = CreateTime, 1 = LogAppendTime).
    pub timestamp_type: i8,
    /// Record key.
    pub key: Option<Bytes>,
    /// Record value.
    pub value: Option<Bytes>,
    /// Headers (preserves duplicate keys and null values, matching the Kafka protocol).
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Leader epoch.
    pub leader_epoch: Option<i32>,
}

impl ConsumerRecord {
    /// Create a new consumer record.
    pub fn new(
        topic: impl Into<String>,
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
            timestamp_type: 0,
            key,
            value,
            headers: Vec::new(),
            leader_epoch: None,
        }
    }

    /// Returns `true` if this record is a tombstone (delete marker).
    ///
    /// In log-compacted topics, a record with a key but no value marks the
    /// key for deletion. After compaction, the key and all its prior values
    /// are removed from the log.
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

    /// Get the first header value matching the given key.
    /// Returns `Some(Some(bytes))` if a header with a value is found,
    /// `Some(None)` if a header with a null value is found,
    /// or `None` if no header with that key exists.
    #[inline]
    pub fn header(&self, key: &str) -> Option<Option<&Bytes>> {
        self.headers
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
    }

    /// Get the first header value as a string.
    /// Returns `None` for missing headers and headers with null values.
    #[inline]
    pub fn header_str(&self, key: &str) -> Option<&str> {
        self.header(key)
            .flatten()
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Get the first non-null header value matching the given key.
    #[inline]
    pub fn header_value(&self, key: &str) -> Option<&Bytes> {
        self.headers
            .iter()
            .find(|(k, v)| k == key && v.is_some())
            .and_then(|(_, v)| v.as_ref())
    }

    /// Get all header values matching the given key (including nulls).
    #[inline]
    pub fn headers_by_key(&self, key: &str) -> Vec<Option<&Bytes>> {
        self.headers
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_ref())
            .collect()
    }
}

/// Represents a topic-partition pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

/// A collection of consumer records from a poll.
#[derive(Debug, Default)]
pub struct ConsumerRecords {
    records: Vec<ConsumerRecord>,
    partitions: Vec<(String, PartitionId)>,
}

impl ConsumerRecords {
    /// Create an empty record collection.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create from a vector of records.
    pub fn from_records(records: Vec<ConsumerRecord>) -> Self {
        let mut seen = HashSet::new();
        let mut partitions = Vec::new();
        for record in &records {
            let tp = (record.topic.clone(), record.partition);
            if seen.insert(tp.clone()) {
                partitions.push(tp);
            }
        }
        Self {
            records,
            partitions,
        }
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get the number of records.
    #[inline]
    pub fn count(&self) -> usize {
        self.records.len()
    }

    /// Get records for a specific topic.
    pub fn records_for_topic(&self, topic: &str) -> impl Iterator<Item = &ConsumerRecord> {
        self.records.iter().filter(move |r| r.topic == topic)
    }

    /// Get records for a specific partition.
    pub fn records_for_partition(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> impl Iterator<Item = &ConsumerRecord> {
        self.records
            .iter()
            .filter(move |r| r.topic == topic && r.partition == partition)
    }

    /// Get all partitions in this record set.
    #[inline]
    pub fn partitions(&self) -> &[(String, PartitionId)] {
        &self.partitions
    }

    /// Iterate over all records.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &ConsumerRecord> {
        self.records.iter()
    }

    /// Convert to a vector.
    pub fn into_vec(self) -> Vec<ConsumerRecord> {
        self.records
    }
}

impl IntoIterator for ConsumerRecords {
    type Item = ConsumerRecord;
    type IntoIter = std::vec::IntoIter<ConsumerRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a> IntoIterator for &'a ConsumerRecords {
    type Item = &'a ConsumerRecord;
    type IntoIter = std::slice::Iter<'a, ConsumerRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

#[cfg(test)]
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

        assert_eq!(record.topic, "test-topic");
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
    fn test_consumer_records_iteration() {
        let records = vec![
            ConsumerRecord::new("topic1", 0, 0, None, Some(Bytes::from("a"))),
            ConsumerRecord::new("topic1", 0, 1, None, Some(Bytes::from("b"))),
            ConsumerRecord::new("topic1", 1, 0, None, Some(Bytes::from("c"))),
        ];

        let consumer_records = ConsumerRecords::from_records(records);
        assert_eq!(consumer_records.count(), 3);
        assert!(!consumer_records.is_empty());

        let p0_records: Vec<_> = consumer_records
            .records_for_partition("topic1", 0)
            .collect();
        assert_eq!(p0_records.len(), 2);
    }

    #[test]
    fn test_consumer_records_partitions() {
        let records = vec![
            ConsumerRecord::new("topic1", 0, 0, None, None),
            ConsumerRecord::new("topic1", 1, 0, None, None),
            ConsumerRecord::new("topic2", 0, 0, None, None),
        ];

        let consumer_records = ConsumerRecords::from_records(records);
        assert_eq!(consumer_records.partitions().len(), 3);
    }

    #[test]
    fn test_consumer_record_duplicate_headers_preserved() {
        let mut record = ConsumerRecord::new("test-topic", 0, 0, None, Some(Bytes::from("value")));

        // Add duplicate header keys
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

    // ── R9.7: null header values ──

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
    fn test_consumer_record_header_value_skips_null() {
        let mut record = ConsumerRecord::new("t", 0, 0, None, Some(Bytes::from("v")));
        // First entry is null, second is non-null
        record.headers.push(("key".to_string(), None));
        record
            .headers
            .push(("key".to_string(), Some(Bytes::from("real"))));

        // header_value() should skip the null and return the first non-null
        assert_eq!(record.header_value("key"), Some(&Bytes::from("real")));

        // If all values for a key are null, header_value() returns None
        let mut record2 = ConsumerRecord::new("t", 0, 0, None, None);
        record2.headers.push(("all-null".to_string(), None));
        assert_eq!(record2.header_value("all-null"), None);
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
}
