//! Consumer record types.

use std::collections::HashMap;

use bytes::Bytes;

use crate::{Offset, PartitionId, Timestamp};

/// A record consumed from Kafka.
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
    /// Headers.
    pub headers: HashMap<String, Bytes>,
    /// Leader epoch.
    pub leader_epoch: Option<i32>,
    /// Serialized key size.
    pub serialized_key_size: i32,
    /// Serialized value size.
    pub serialized_value_size: i32,
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
        let serialized_key_size = key.as_ref().map(|k| k.len() as i32).unwrap_or(-1);
        let serialized_value_size = value.as_ref().map(|v| v.len() as i32).unwrap_or(-1);

        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp: 0,
            timestamp_type: 0,
            key,
            value,
            headers: HashMap::new(),
            leader_epoch: None,
            serialized_key_size,
            serialized_value_size,
        }
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

    /// Get a header value.
    #[inline]
    pub fn header(&self, key: &str) -> Option<&Bytes> {
        self.headers.get(key)
    }

    /// Get a header value as a string.
    #[inline]
    pub fn header_str(&self, key: &str) -> Option<&str> {
        self.headers
            .get(key)
            .and_then(|v| std::str::from_utf8(v).ok())
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
        let mut partitions = Vec::new();
        for record in &records {
            let tp = (record.topic.clone(), record.partition);
            if !partitions.contains(&tp) {
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
}
