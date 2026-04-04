//! Producer batch for batching records.

use std::time::Instant;

use super::record::ProducerRecord;
use crate::PartitionId;
use crate::protocol::{Compression, RecordBatch, RecordBatchBuilder};

/// A batch of records to be sent together.
#[derive(Debug)]
pub struct ProducerBatch {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// Records in the batch (used by `build()` / `drain()`).
    records: Vec<ProducerRecord>,
    /// Number of tracked records (includes those added via `track()`).
    tracked_count: usize,
    /// Current size in bytes.
    size: usize,
    /// Maximum batch size.
    max_size: usize,
    /// Compression type.
    compression: Compression,
    /// When the batch was created.
    created_at: Instant,
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
            tracked_count: 0,
            size: 0,
            max_size,
            compression,
            created_at: Instant::now(),
        }
    }

    /// Try to add a record to the batch.
    ///
    /// Returns `Ok(())` on success. Returns `Err(record)` if the batch is full,
    /// giving back ownership of the record so callers avoid a clone.
    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_add(&mut self, record: ProducerRecord) -> Result<(), ProducerRecord> {
        let record_size = record.estimated_size();

        if !self.is_empty() && self.size + record_size > self.max_size {
            return Err(record);
        }

        self.size += record_size;
        self.tracked_count += 1;
        self.records.push(record);
        Ok(())
    }

    /// Check if a record of the given size would fit in the batch.
    ///
    /// The caller provides the pre-computed `record_size` to ensure
    /// the same value is used for both the fit check and subsequent
    /// [`Self::track`] call.
    #[inline]
    pub fn would_fit(&self, record_size: usize) -> bool {
        self.is_empty() || self.size + record_size <= self.max_size
    }

    /// Track a record's size without storing its data.
    ///
    /// Use with [`Self::would_fit`] when the caller manages record storage
    /// separately (e.g., in `PendingRecord`). Increments `len()`, `size()`,
    /// and `is_full()` as if the record were added.
    #[inline]
    pub fn track(&mut self, record_size: usize) {
        self.size += record_size;
        self.tracked_count += 1;
    }

    /// Check if the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tracked_count == 0
    }

    /// Get the number of records in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.tracked_count
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

        for record in &self.records {
            if record.headers.is_empty() {
                builder = builder.add_record(record.key.clone(), Some(record.value.clone()));
            } else {
                let hdrs: Vec<(String, Vec<u8>)> = record
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                builder = builder.add_record_with_headers(
                    record.key.clone(),
                    Some(record.value.clone()),
                    hdrs,
                );
            }
        }

        builder.build()
    }

    /// Drain all records from the batch.
    pub fn drain(&mut self) -> Vec<ProducerRecord> {
        self.size = 0;
        self.tracked_count = 0;
        self.records.drain(..).collect()
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
        assert!(batch.try_add(record).is_ok());

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
        assert!(batch.size() > 0);
    }

    #[test]
    fn test_batch_full() {
        // Overhead per record: topic("test").len() + 64 = 68, plus 20 byte value = 88 bytes each
        let mut batch = ProducerBatch::new("test".to_string(), 0, 200, Compression::None);

        // First record should fit (~88 bytes)
        let record1 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record1).is_ok());

        // Second record should fit (~176 bytes total)
        let record2 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record2).is_ok());

        // Third record should not fit (~264 bytes total > max_size of 200)
        let record3 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record3).is_err());
    }

    #[test]
    fn test_batch_drain() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 1024, Compression::None);

        let _ = batch.try_add(ProducerRecord::new("test", b"hello".to_vec()));
        let _ = batch.try_add(ProducerRecord::new("test", b"world".to_vec()));

        let records = batch.drain();
        assert_eq!(records.len(), 2);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_build() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 1024, Compression::None);

        let _ =
            batch.try_add(ProducerRecord::new("test", b"value".to_vec()).with_key(b"key".to_vec()));

        let record_batch = batch.build();
        assert_eq!(record_batch.records.len(), 1);
    }

    #[test]
    fn test_batch_build_preserves_headers() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 4096, Compression::None);

        let record = ProducerRecord::new("test", b"value".to_vec())
            .with_key(b"key".to_vec())
            .with_header("trace-id", b"abc123")
            .with_header("content-type", b"application/json");
        let _ = batch.try_add(record);

        let record_batch = batch.build();
        assert_eq!(record_batch.records.len(), 1);
        assert_eq!(
            record_batch.records[0].headers.len(),
            2,
            "Headers should be preserved in built batch"
        );
        assert_eq!(record_batch.records[0].headers[0].key, "trace-id");
        assert_eq!(record_batch.records[0].headers[1].key, "content-type");
    }

    #[test]
    fn test_would_fit_and_track() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 200, Compression::None);

        let record = ProducerRecord::new("test", vec![0u8; 20]);
        let size = record.estimated_size();

        // First record always fits (empty batch)
        assert!(batch.would_fit(size));
        batch.track(size);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.size(), size);
        assert!(!batch.is_empty());

        // Second record fits
        assert!(batch.would_fit(size));
        batch.track(size);
        assert_eq!(batch.len(), 2);

        // Third would exceed max_size
        assert!(!batch.would_fit(size));
    }

    #[test]
    fn test_would_fit_first_record_always_fits() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 10, Compression::None);

        // Even a record larger than max_size fits as the first record
        let large_size = 100;
        assert!(batch.would_fit(large_size));
        batch.track(large_size);
        assert!(batch.is_full());
        // Second record won't fit
        assert!(!batch.would_fit(1));
    }
}
