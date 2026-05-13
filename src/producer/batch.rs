//! Producer batch for batching records.

use std::time::Instant;

use super::record::ProducerRecord;
use crate::PartitionId;
use crate::error::{KrafkaError, Result};
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
    pub fn try_add(&mut self, record: ProducerRecord) -> std::result::Result<(), ProducerRecord> {
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
    pub(crate) fn would_fit(&self, record_size: usize) -> bool {
        self.is_empty() || self.size + record_size <= self.max_size
    }

    /// Track a record's size without storing its data.
    ///
    /// Use with [`Self::would_fit`] when the caller manages record storage
    /// separately (e.g., in `PendingRecord`). Increments `len()`, `size()`,
    /// and `is_full()` as if the record were added.
    ///
    /// # Warning
    ///
    /// When `track()` is used, `self.records` is **not** populated. Calling
    /// [`Self::build()`] on a track-only batch will produce an **empty**
    /// `RecordBatch` regardless of `len()`. Only call `build()` on batches
    /// where all records were added via [`Self::try_add()`].
    #[inline]
    pub(crate) fn track(&mut self, record_size: usize) {
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
    ///
    /// Returns an error if `track()` was used instead of `try_add()` for any
    /// records — calling `build()` on a track-only batch would silently return
    /// an empty `RecordBatch`. This is detectable in both debug and release
    /// builds, unlike the previous `debug_assert!`-only guard.
    pub fn try_build(&self) -> Result<RecordBatch> {
        if self.tracked_count > 0 && self.records.is_empty() {
            return Err(KrafkaError::invalid_state(format!(
                "ProducerBatch::try_build() called on a track-only batch \
                 (tracked_count={} but records is empty); use try_add() for \
                 records that should appear in the built RecordBatch",
                self.tracked_count
            )));
        }
        if self.records.len() != self.tracked_count {
            return Err(KrafkaError::invalid_state(format!(
                "ProducerBatch::try_build() called on a mixed track()/try_add() batch: \
                 records.len()={} but tracked_count={}",
                self.records.len(),
                self.tracked_count,
            )));
        }

        let mut builder = RecordBatchBuilder::new().compression(self.compression);

        for record in &self.records {
            if record.headers.is_empty() {
                builder = builder.add_record(record.key.clone(), Some(record.value.clone()));
            } else {
                builder = builder.add_record_with_headers(
                    record.key.clone(),
                    Some(record.value.clone()),
                    record.headers.clone(),
                );
            }
        }

        Ok(builder.build())
    }

    /// Drain all records from the batch.
    pub fn drain(&mut self) -> Vec<ProducerRecord> {
        self.size = 0;
        self.tracked_count = 0;
        self.records.drain(..).collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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

        let record_batch = batch.try_build().unwrap();
        assert_eq!(record_batch.records.len(), 1);
    }

    #[test]
    fn test_batch_build_preserves_headers() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 4096, Compression::None);

        let record = ProducerRecord::new("test", b"value".to_vec())
            .with_key(b"key".to_vec())
            .with_header("trace-id", bytes::Bytes::from_static(b"abc123"))
            .with_header(
                "content-type",
                bytes::Bytes::from_static(b"application/json"),
            );
        let _ = batch.try_add(record);

        let record_batch = batch.try_build().unwrap();
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
    fn test_batch_try_build_rejects_track_only() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 4096, Compression::None);
        batch.track(100);
        let err = batch.try_build().unwrap_err();
        assert!(
            err.to_string().contains("track-only"),
            "expected track-only error, got: {err}"
        );
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
