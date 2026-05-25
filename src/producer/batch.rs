//! Producer batch for collecting records before transmission.
//!
//! [`ProducerBatch`] is a **recording** batch: records are stored in full and
//! serialised into a wire-format [`RecordBatch`] via [`ProducerBatch::build`].
//! The build step is infallible — callers cannot construct a batch in an
//! un-buildable state.
//!
//! Size-tracking-only usage (accumulator path) is handled directly in
//! [`super::accumulator::AccumulatorBatch`], which stores records separately
//! and never calls [`ProducerBatch::build`].

use std::time::Instant;

use super::record::ProducerRecord;
use crate::PartitionId;
use crate::protocol::{Compression, RecordBatch, RecordBatchBuilder};

/// A batch of producer records to be sent together.
///
/// Records are added via [`Self::try_add`]; the batch is serialised with
/// [`Self::build`]. Both operations are cheap, and `build` is infallible.
#[derive(Debug)]
pub struct ProducerBatch {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// Stored records.
    records: Vec<ProducerRecord>,
    /// Running byte-size estimate.
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
            size: 0,
            max_size,
            compression,
            created_at: Instant::now(),
        }
    }

    /// Try to add a record to the batch.
    ///
    /// Returns `None` on success. Returns `Some(record)` if the batch is full,
    /// giving back ownership so the caller avoids a clone.
    #[inline]
    pub fn try_add(&mut self, record: ProducerRecord) -> Option<ProducerRecord> {
        let record_size = record.estimated_size();

        if !self.is_empty() && self.size + record_size > self.max_size {
            return Some(record);
        }

        self.size += record_size;
        self.records.push(record);
        None
    }

    /// Return `true` if a record of `record_size` bytes would fit.
    ///
    /// An empty batch always accepts the first record, even when
    /// `record_size > max_size`, to prevent a single oversized record from
    /// blocking the send path indefinitely.
    #[inline]
    pub fn would_fit(&self, record_size: usize) -> bool {
        self.is_empty() || self.size + record_size <= self.max_size
    }

    /// Return `true` if the batch contains no records.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Number of records currently in the batch.
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

    /// Build the wire-format [`RecordBatch`] from all stored records.
    ///
    /// This is infallible: a `ProducerBatch` can only contain records added
    /// via [`Self::try_add`], so the internal state is always consistent.
    pub fn build(&self) -> RecordBatch {
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

        builder.build()
    }

    /// Drain all records from the batch, resetting it to empty.
    pub fn drain(&mut self) -> Vec<ProducerRecord> {
        self.size = 0;
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
        assert!(batch.try_add(record).is_none());

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
        assert!(batch.try_add(record1).is_none());

        // Second record should fit (~176 bytes total)
        let record2 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record2).is_none());

        // Third record should not fit (~264 bytes total > max_size of 200)
        let record3 = ProducerRecord::new("test", vec![0u8; 20]);
        assert!(batch.try_add(record3).is_some());
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
            .with_header("trace-id", bytes::Bytes::from_static(b"abc123"))
            .with_header(
                "content-type",
                bytes::Bytes::from_static(b"application/json"),
            );
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
    fn test_would_fit() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 200, Compression::None);

        let record1 = ProducerRecord::new("test", vec![0u8; 20]);
        let size = record1.estimated_size();

        // First record always fits (empty batch)
        assert!(batch.would_fit(size));
        let _ = batch.try_add(record1);
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());

        // Second record
        let record2 = ProducerRecord::new("test", vec![0u8; 20]);
        let _ = batch.try_add(record2);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_would_fit_first_record_always_fits() {
        let mut batch = ProducerBatch::new("test".to_string(), 0, 10, Compression::None);

        // Even a record larger than max_size fits as the first record (empty batch)
        assert!(batch.would_fit(100));
        let large_record = ProducerRecord::new("test", vec![0u8; 100]);
        let _ = batch.try_add(large_record);
        assert!(batch.is_full());
        // Second record won't fit
        assert!(!batch.would_fit(1));
    }
}
