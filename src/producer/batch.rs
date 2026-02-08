//! Producer batch for batching records.

use std::time::Instant;

use tokio::sync::oneshot;

use super::record::{ProducerRecord, RecordMetadata};
use crate::PartitionId;
use crate::error::Result;
use crate::protocol::{Compression, RecordBatch, RecordBatchBuilder};

/// A batch of records to be sent together.
#[derive(Debug)]
pub struct ProducerBatch {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// Records in the batch.
    records: Vec<BatchRecord>,
    /// Current size in bytes.
    size: usize,
    /// Maximum batch size.
    max_size: usize,
    /// Compression type.
    compression: Compression,
    /// When the batch was created.
    created_at: Instant,
}

/// A record in a batch with its completion channel.
#[derive(Debug)]
struct BatchRecord {
    record: ProducerRecord,
    #[allow(dead_code)]
    callback: Option<oneshot::Sender<Result<RecordMetadata>>>,
}

impl ProducerBatch {
    /// Create a new producer batch.
    pub fn new(
        topic: String,
        partition: PartitionId,
        max_size: usize,
        compression: Compression,
    ) -> Self {
        Self {
            topic,
            partition,
            records: Vec::new(),
            size: 0,
            max_size,
            compression,
            created_at: Instant::now(),
        }
    }

    /// Try to add a record to the batch.
    ///
    /// Returns `false` if the batch is full.
    #[inline]
    pub fn try_add(&mut self, record: ProducerRecord) -> bool {
        let record_size = record.estimated_size();

        if !self.records.is_empty() && self.size + record_size > self.max_size {
            return false;
        }

        self.size += record_size;
        self.records.push(BatchRecord {
            record,
            callback: None,
        });
        true
    }

    /// Try to add a record with a completion callback.
    #[inline]
    pub fn try_add_with_callback(
        &mut self,
        record: ProducerRecord,
        callback: oneshot::Sender<Result<RecordMetadata>>,
    ) -> bool {
        let record_size = record.estimated_size();

        if !self.records.is_empty() && self.size + record_size > self.max_size {
            return false;
        }

        self.size += record_size;
        self.records.push(BatchRecord {
            record,
            callback: Some(callback),
        });
        true
    }

    /// Check if the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get the number of records in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Get the current size in bytes.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Check if the batch is full.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    /// Get the age of the batch.
    #[inline]
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Build the record batch for sending.
    pub fn build(&self) -> RecordBatch {
        let mut builder = RecordBatchBuilder::new().compression(self.compression);

        for batch_record in &self.records {
            let key = batch_record.record.key.clone();
            let value = batch_record.record.value.clone();
            builder = builder.add_record(key, Some(value));
        }

        builder.build()
    }

    /// Drain all records from the batch.
    pub fn drain(&mut self) -> Vec<ProducerRecord> {
        self.size = 0;
        self.records.drain(..).map(|br| br.record).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_new() {
        let batch = ProducerBatch::new("test".to_string(), 0, 1024, Compression::None);
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
        assert_eq!(batch.size(), 0);
    }

    #[test]
    fn test_batch_try_add() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 1024, Compression::None);

        let record = ProducerRecord::new("test", b"hello".to_vec());
        assert!(batch.try_add(record));

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
        assert!(batch.size() > 0);
    }

    #[test]
    fn test_batch_full() {
        // With 50 bytes estimated overhead per record, a 20 byte value = ~70 bytes estimated
        let mut batch = ProducerBatch::new("test".to_string(), 0, 200, Compression::None);

        // First record should fit (~70 bytes estimated)
        let record1 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record1));

        // Second record should fit (~140 bytes total)
        let record2 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record2));

        // Third record should not fit (~210 bytes total > max_size of 200)
        let record3 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(!batch.try_add(record3));
    }

    #[test]
    fn test_batch_drain() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 1024, Compression::None);

        batch.try_add(ProducerRecord::new("test", b"hello".to_vec()));
        batch.try_add(ProducerRecord::new("test", b"world".to_vec()));

        let records = batch.drain();
        assert_eq!(records.len(), 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_build() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 1024, Compression::None);

        batch.try_add(
            ProducerRecord::new("test", b"value".to_vec()).with_key(Some(b"key".to_vec())),
        );

        let record_batch = batch.build();
        assert_eq!(record_batch.records.len(), 1);
    }
}
