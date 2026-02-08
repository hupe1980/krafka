//! Producer record types.

use crate::{PartitionId, Timestamp};

/// A record to be sent to Kafka.
#[must_use]
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// Target topic.
    pub topic: String,
    /// Target partition (optional, will be computed if not set).
    pub partition: Option<PartitionId>,
    /// Record key (optional).
    pub key: Option<Vec<u8>>,
    /// Record value.
    pub value: Vec<u8>,
    /// Record timestamp (optional, will use current time if not set).
    pub timestamp: Option<Timestamp>,
    /// Record headers.
    pub headers: Vec<(String, Vec<u8>)>,
}

impl ProducerRecord {
    /// Create a new producer record.
    pub fn new(topic: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: value.into(),
            timestamp: None,
            headers: Vec::new(),
        }
    }

    /// Set the partition.
    pub fn with_partition(mut self, partition: PartitionId) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the key.
    pub fn with_key(mut self, key: Option<Vec<u8>>) -> Self {
        self.key = key;
        self
    }

    /// Set the timestamp.
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Add a header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Get the key as a string (if valid UTF-8).
    #[inline]
    pub fn key_str(&self) -> Option<&str> {
        self.key.as_ref().and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Get the value as a string (if valid UTF-8).
    #[inline]
    pub fn value_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
    }

    /// Get the estimated size in bytes.
    #[inline]
    pub fn estimated_size(&self) -> usize {
        let key_size = self.key.as_ref().map(|k| k.len()).unwrap_or(0);
        let value_size = self.value.len();
        let headers_size: usize = self.headers.iter().map(|(k, v)| k.len() + v.len()).sum();

        // Overhead for record metadata
        key_size + value_size + headers_size + 50
    }
}

/// Metadata returned after successfully sending a record.
#[must_use = "contains the result of a send operation"]
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic the record was sent to.
    pub topic: String,
    /// Partition the record was sent to.
    pub partition: PartitionId,
    /// Offset of the record.
    pub offset: i64,
    /// Timestamp of the record.
    pub timestamp: Timestamp,
}

impl RecordMetadata {
    /// Check if the record was successfully sent.
    #[inline]
    pub fn is_success(&self) -> bool {
        self.offset >= 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_new() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec());
        assert_eq!(record.topic, "test-topic");
        assert_eq!(record.value, b"hello");
        assert!(record.key.is_none());
        assert!(record.partition.is_none());
    }

    #[test]
    fn test_producer_record_with_key() {
        let record =
            ProducerRecord::new("test-topic", b"hello".to_vec()).with_key(Some(b"my-key".to_vec()));

        assert_eq!(record.key, Some(b"my-key".to_vec()));
        assert_eq!(record.key_str(), Some("my-key"));
    }

    #[test]
    fn test_producer_record_with_partition() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec()).with_partition(5);

        assert_eq!(record.partition, Some(5));
    }

    #[test]
    fn test_producer_record_with_headers() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec())
            .with_header("h1", b"v1".to_vec())
            .with_header("h2", b"v2".to_vec());

        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].0, "h1");
        assert_eq!(record.headers[1].0, "h2");
    }

    #[test]
    fn test_producer_record_estimated_size() {
        let record = ProducerRecord::new("test-topic", b"hello world".to_vec())
            .with_key(Some(b"key".to_vec()));

        let size = record.estimated_size();
        assert!(size > 3 + 11); // key + value at minimum
    }

    #[test]
    fn test_record_metadata() {
        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 42,
            timestamp: 1234567890000,
        };

        assert!(metadata.is_success());
        assert_eq!(metadata.offset, 42);
    }
}
