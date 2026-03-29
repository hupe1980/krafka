//! Producer record types.

use bytes::Bytes;

use crate::error::{KrafkaError, Result};
use crate::{PartitionId, Timestamp};

/// A record to be sent to Kafka.
#[must_use]
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// Target topic.
    pub topic: String,
    /// Target partition (optional, will be computed if not set).
    pub partition: Option<PartitionId>,
    /// Record key (optional, zero-copy via `Bytes`).
    pub key: Option<Bytes>,
    /// Record value (zero-copy via `Bytes`).
    pub value: Bytes,
    /// Record timestamp (optional, will use current time if not set).
    pub timestamp: Option<Timestamp>,
    /// Record headers.
    pub headers: Vec<(String, Vec<u8>)>,
}

impl ProducerRecord {
    /// Create a new producer record.
    pub fn new(topic: impl Into<String>, value: impl Into<Bytes>) -> Self {
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
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
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

    /// Validate that this record's fields do not exceed Kafka wire-format limits.
    ///
    /// Checks:
    /// - Key length fits in `i32` (Kafka bytes encoding limit of 2 GiB)
    /// - Value length fits in `i32`
    /// - Each header key fits in `i32` (record batch v2 uses varint/i32 length prefix)
    /// - Each header value fits in `i32`
    /// - Topic name fits in `i16` (Kafka string encoding limit of 32 KiB)
    pub fn validate(&self) -> Result<()> {
        // Topic names are encoded as KafkaString (i16 length prefix)
        if self.topic.len() > i16::MAX as usize {
            return Err(KrafkaError::protocol(format!(
                "topic name length {} exceeds protocol limit of {}",
                self.topic.len(),
                i16::MAX
            )));
        }

        // Key is encoded as KafkaBytes (i32 length prefix)
        if let Some(ref key) = self.key
            && key.len() > i32::MAX as usize
        {
            return Err(KrafkaError::protocol(format!(
                "record key length {} exceeds protocol limit of {}",
                key.len(),
                i32::MAX
            )));
        }

        // Value is encoded as KafkaBytes (i32 length prefix)
        if self.value.len() > i32::MAX as usize {
            return Err(KrafkaError::protocol(format!(
                "record value length {} exceeds protocol limit of {}",
                self.value.len(),
                i32::MAX
            )));
        }

        // Header keys and values are encoded with varint i32 length prefixes
        // in the record batch v2 format.
        for (i, (key, value)) in self.headers.iter().enumerate() {
            if key.len() > i32::MAX as usize {
                return Err(KrafkaError::protocol(format!(
                    "header[{}] key length {} exceeds protocol limit of {}",
                    i,
                    key.len(),
                    i32::MAX
                )));
            }
            if value.len() > i32::MAX as usize {
                return Err(KrafkaError::protocol(format!(
                    "header[{}] value length {} exceeds protocol limit of {}",
                    i,
                    value.len(),
                    i32::MAX
                )));
            }
        }

        Ok(())
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
        assert_eq!(record.value.as_ref(), b"hello");
        assert!(record.key.is_none());
        assert!(record.partition.is_none());
    }

    #[test]
    fn test_producer_record_with_key() {
        let record =
            ProducerRecord::new("test-topic", b"hello".to_vec()).with_key(b"my-key".to_vec());

        assert_eq!(record.key, Some(Bytes::from_static(b"my-key")));
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
        let record =
            ProducerRecord::new("test-topic", b"hello world".to_vec()).with_key(b"key".to_vec());

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

    #[test]
    fn test_validate_valid_record() {
        let record = ProducerRecord::new("topic", b"value".to_vec())
            .with_key(b"key".to_vec())
            .with_header("h1", b"v1".to_vec());
        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_oversized_topic() {
        let record = ProducerRecord::new("x".repeat(i16::MAX as usize + 1), b"v".to_vec());
        let err = record.validate().unwrap_err().to_string();
        assert!(err.contains("topic name length"), "unexpected: {err}");
    }

    #[test]
    fn test_validate_accepts_header_key_within_i32_limit() {
        // Header keys use varint i32 length prefix in record batch v2,
        // so i16::MAX + 1 must be accepted (previously rejected).
        let record = ProducerRecord::new("topic", b"v".to_vec())
            .with_header("x".repeat(i16::MAX as usize + 1), b"v".to_vec());
        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_validate_accepts_max_valid_sizes() {
        // Topic at max i16 length
        let record = ProducerRecord::new("a".repeat(i16::MAX as usize), b"v".to_vec());
        assert!(record.validate().is_ok());
    }
}
