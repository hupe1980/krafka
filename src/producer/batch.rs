//! A batch of records for one partition, and what each record is owed.

use std::sync::Arc;
use tokio::time::Instant;

use bytes::Bytes;
use tokio::sync::{Semaphore, oneshot};

use super::gate::TxnTicket;
use super::identity::BatchIdentity;
use super::record::{RecordMetadata, RoutedRecord};
use crate::barrier::InFlightOpGuard;
use crate::error::{KrafkaError, Result};
use crate::interceptor::{ProducerInterceptor, RecordContext};
use crate::metrics::ProducerRecorder;
use crate::protocol::{Compression, Record, RecordBatch, RecordHeader};
use crate::{PartitionId, Timestamp};

/// The wire content of one record, shared with the request that sends it.
#[derive(Debug, Clone)]
pub(crate) struct RecordData {
    pub(crate) key: Option<Bytes>,
    pub(crate) value: Option<Bytes>,
    pub(crate) headers: Vec<(String, Option<Bytes>)>,
    /// Create time: the record's own timestamp, or the time it was sent.
    pub(crate) timestamp: Timestamp,
}

impl RecordData {
    pub(crate) fn new(record: RoutedRecord, now: Timestamp) -> Self {
        Self {
            key: record.key,
            value: record.value,
            headers: record.headers,
            timestamp: record.timestamp.unwrap_or(now),
        }
    }
}

/// Bytes reserved from the producer's `buffer_memory`, returned on drop.
pub(crate) struct Reservation {
    pub(crate) bytes: usize,
    pub(crate) memory: Arc<Semaphore>,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.memory.add_permits(self.bytes);
    }
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

/// Counts one record in the producer's buffered-records gauge while it lives.
#[derive(Debug)]
pub(crate) struct BufferedRecord {
    metrics: Arc<ProducerRecorder>,
}

impl BufferedRecord {
    pub(crate) fn new(metrics: Arc<ProducerRecorder>) -> Self {
        metrics.buffered_records.inc();
        Self { metrics }
    }
}

impl Drop for BufferedRecord {
    fn drop(&mut self) {
        self.metrics.buffered_records.dec();
    }
}

/// What a record is owed: its caller's answer, the interceptor's terminal
/// callback, and the release of everything it holds.
#[derive(Debug)]
pub(crate) struct Waiter {
    pub(crate) response: oneshot::Sender<Result<RecordMetadata>>,
    /// The record's `send` span; it ends when the waiter is answered.
    pub(crate) span: tracing::Span,
    pub(crate) context: RecordContext,
    pub(crate) txn: Option<TxnTicket>,
    pub(crate) _operation: InFlightOpGuard,
    pub(crate) _reservation: Reservation,
    pub(crate) _buffered: BufferedRecord,
}

impl Waiter {
    /// Deliver the record's outcome: interceptor first, then the transaction
    /// gate, then the caller.
    pub(crate) fn answer(
        mut self,
        interceptor: &dyn ProducerInterceptor,
        topic: &str,
        partition: PartitionId,
        headers: &crate::Headers,
        result: Result<RecordMetadata>,
    ) {
        match &result {
            Ok(metadata) if metadata.offset >= 0 => {
                self.span.record("messaging.kafka.offset", metadata.offset);
            }
            Ok(_) => {}
            Err(error) => crate::tracing_ext::record_error(&self.span, error),
        }
        crate::interceptor::safe_on_acknowledgement(
            interceptor,
            topic,
            partition,
            result.as_ref(),
            headers,
            &mut self.context,
        );
        if let Some(ticket) = self.txn.take() {
            ticket.complete(result.as_ref().map(|_| ()));
        }
        let _ = self.response.send(result);
    }
}

/// Records for one partition, sent together under one stamp.
#[derive(Debug)]
pub(crate) struct Batch {
    /// Shared with the request future that encodes them.
    pub(crate) records: Arc<Vec<RecordData>>,
    /// `None` once the callers were answered while the batch was in flight.
    pub(crate) waiters: Option<Vec<Waiter>>,
    /// Estimated bytes, as charged against `buffer_memory`.
    pub(crate) bytes: usize,
    /// When the first record was appended: the linger clock.
    pub(crate) created: Instant,
    /// `created + delivery_timeout`: the batch resolves by then.
    pub(crate) deadline: Instant,
    /// Highest barrier generation among its records.
    pub(crate) generation: u64,
    pub(crate) stamp: Option<BatchIdentity>,
    /// Encoded bytes for the stamp; reused by every retry.
    pub(crate) encoded: Option<Bytes>,
    /// Some attempt was handed to a connection.
    pub(crate) written: bool,
    /// Some written attempt got no answer that proves it was not appended.
    pub(crate) maybe_appended: bool,
    pub(crate) attempts: u32,
    pub(crate) split_depth: u8,
    /// Send this batch in a request of its own.
    pub(crate) alone: bool,
    pub(crate) last_error: Option<KrafkaError>,
}

impl Batch {
    pub(crate) fn new(now: Instant, deadline: Instant) -> Self {
        Self {
            records: Arc::new(Vec::new()),
            waiters: Some(Vec::new()),
            bytes: 0,
            created: now,
            deadline,
            generation: 0,
            stamp: None,
            encoded: None,
            written: false,
            maybe_appended: false,
            attempts: 0,
            split_depth: 0,
            alone: false,
            last_error: None,
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether a record of `size` bytes still fits under `batch_size`. An
    /// empty batch takes any record.
    #[inline]
    pub(crate) fn fits(&self, size: usize, batch_size: usize) -> bool {
        self.records.is_empty() || self.bytes + size <= batch_size
    }

    pub(crate) fn push(&mut self, data: RecordData, size: usize, generation: u64, waiter: Waiter) {
        Arc::make_mut(&mut self.records).push(data);
        if let Some(waiters) = self.waiters.as_mut() {
            waiters.push(waiter);
        }
        self.bytes += size;
        self.generation = self.generation.max(generation);
    }

    /// Split into two unstamped batches, keeping the deadline. `None` for a
    /// batch of one record.
    pub(crate) fn split(mut self) -> Option<(Self, Self)> {
        let count = self.records.len();
        if count < 2 {
            return None;
        }
        let mid = count / 2;
        let mut records: Vec<RecordData> = self.records.as_ref().clone();
        let tail_records = records.split_off(mid);
        let mut waiters = self.waiters.take().unwrap_or_default();
        let tail_waiters = waiters.split_off(mid.min(waiters.len()));
        let ratio = |n: usize| self.bytes * n / count;
        let half = |records: Vec<RecordData>, waiters: Vec<Waiter>, bytes: usize| Self {
            records: Arc::new(records),
            waiters: Some(waiters),
            bytes,
            created: self.created,
            deadline: self.deadline,
            generation: self.generation,
            stamp: None,
            encoded: None,
            written: false,
            maybe_appended: false,
            attempts: 0,
            split_depth: self.split_depth + 1,
            alone: false,
            last_error: None,
        };
        let head = half(records, waiters, ratio(mid));
        let tail = half(tail_records, tail_waiters, ratio(count - mid));
        Some((head, tail))
    }
}

/// Encode `records` as one v2 record batch.
///
/// Each record keeps its own timestamp: the batch's base timestamp is the
/// first record's, every record carries its delta, and `max_timestamp` is the
/// largest.
pub(crate) fn encode(
    records: &[RecordData],
    stamp: Option<BatchIdentity>,
    transactional: bool,
    compression: Compression,
    compression_level: Option<i32>,
) -> Result<Bytes> {
    let base_timestamp = records.first().map_or(0, |r| r.timestamp);
    let max_timestamp = records
        .iter()
        .map(|r| r.timestamp)
        .max()
        .unwrap_or(base_timestamp);
    let wire: Vec<Record> = records
        .iter()
        .enumerate()
        .map(|(index, r)| Record {
            attributes: 0,
            timestamp_delta: r.timestamp.saturating_sub(base_timestamp),
            offset_delta: i32::try_from(index).unwrap_or(i32::MAX),
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|(k, v)| RecordHeader {
                    key: Bytes::copy_from_slice(k.as_bytes()),
                    value: v.clone(),
                })
                .collect(),
        })
        .collect();
    let mut batch = RecordBatch::new().with_compression(compression);
    batch.attributes.is_transactional = transactional;
    batch.last_offset_delta = i32::try_from(wire.len().saturating_sub(1)).unwrap_or(i32::MAX);
    batch.base_timestamp = base_timestamp;
    batch.max_timestamp = max_timestamp;
    if let Some(stamp) = stamp {
        batch.producer_id = stamp.producer_id;
        batch.producer_epoch = stamp.epoch;
        batch.base_sequence = stamp.base_sequence;
    }
    batch.records = wire;
    batch.compression_level = compression_level;
    batch.encode()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn data(timestamp: Timestamp, value: &'static [u8]) -> RecordData {
        RecordData {
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: vec![("h".into(), Some(Bytes::from_static(b"v")))],
            timestamp,
        }
    }

    /// Every record keeps the timestamp it was given; the batch does not
    /// stamp its own wall-clock time.
    #[test]
    fn per_record_timestamps_reach_the_wire() {
        let records = [data(1_000, b"a"), data(5_000, b"b"), data(3_000, b"c")];
        let mut bytes = encode(&records, None, false, Compression::None, None).unwrap();
        let decoded = RecordBatch::decode(&mut bytes).unwrap();
        assert_eq!(decoded.base_timestamp, 1_000);
        assert_eq!(decoded.max_timestamp, 5_000);
        let timestamps: Vec<i64> = decoded
            .records
            .iter()
            .map(|r| decoded.base_timestamp + r.timestamp_delta)
            .collect();
        assert_eq!(timestamps, vec![1_000, 5_000, 3_000]);
        assert_eq!(decoded.records[1].headers[0].key, Bytes::from_static(b"h"));
    }

    #[test]
    fn the_stamp_and_transactional_flag_reach_the_header() {
        let stamp = BatchIdentity {
            producer_id: 42,
            epoch: 3,
            base_sequence: 17,
            generation: 0,
        };
        let mut bytes =
            encode(&[data(1, b"a")], Some(stamp), true, Compression::None, None).unwrap();
        let decoded = RecordBatch::decode(&mut bytes).unwrap();
        assert_eq!(
            (
                decoded.producer_id,
                decoded.producer_epoch,
                decoded.base_sequence
            ),
            (42, 3, 17)
        );
        assert!(decoded.attributes.is_transactional);
    }
}
