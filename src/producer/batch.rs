//! A batch of records for one partition, and what each record is owed.

use std::sync::Arc;
use tokio::time::Instant;

use bytes::Bytes;
use tokio::sync::{Semaphore, oneshot};

use super::gate::TxnTicket;
use super::identity::BatchIdentity;
use super::record::{RecordMetadata, RoutedRecord};
use crate::barrier::InFlightBarrier;
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

/// What a record holds while it has no outcome, and gives back once it has
/// one: its bytes of `buffer_memory`, its count in the flush/close barrier,
/// and its count in the buffered-records gauge. One per producer.
#[derive(Debug)]
pub(crate) struct Release {
    memory: Arc<Semaphore>,
    barrier: Arc<InFlightBarrier>,
    metrics: Arc<ProducerRecorder>,
}

impl Release {
    pub(crate) fn new(
        memory: Arc<Semaphore>,
        barrier: Arc<InFlightBarrier>,
        metrics: Arc<ProducerRecorder>,
    ) -> Self {
        Self {
            memory,
            barrier,
            metrics,
        }
    }

    /// Give back `bytes`, `records` and the barrier counts of `generations`,
    /// each in one operation.
    fn settle(&self, bytes: usize, records: usize, generations: &[(u64, u32)]) {
        if bytes > 0 {
            self.memory.add_permits(bytes);
        }
        self.barrier.complete_many(generations);
        self.metrics.buffered_records.sub(records as u64);
    }
}

/// One record's share of a [`Release`], from the moment it is reserved.
///
/// *Armed* when built at enqueue: dropped so, it gives back its own share —
/// the path of a record refused after its reservation, or still in the
/// engine's channel when the engine stops. *Disarmed* when a batch takes it
/// into its [`Waiters`], which give back every record's share at once.
#[derive(Debug)]
pub(crate) struct RecordHold {
    bytes: usize,
    generation: u64,
    release: Option<Arc<Release>>,
}

impl RecordHold {
    /// Count the record in the buffered gauge and hold `bytes` of memory and
    /// one barrier count of `generation`, both already taken by the caller.
    pub(crate) fn new(bytes: usize, generation: u64, release: Arc<Release>) -> Self {
        release.metrics.buffered_records.inc();
        Self {
            bytes,
            generation,
            release: Some(release),
        }
    }

    /// Hand this record's share to whoever settles it next.
    fn disarm(&mut self) -> Option<Arc<Release>> {
        self.release.take()
    }
}

impl Drop for RecordHold {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release.settle(self.bytes, 1, &[(self.generation, 1)]);
        }
    }
}

/// The callers of one batch, and everything their records hold, given back
/// once — when this is dropped, which every resolution path does after it
/// answered the last caller.
#[derive(Debug, Default)]
pub(crate) struct Waiters {
    list: Vec<Waiter>,
    bytes: usize,
    /// Barrier counts per generation, in append order; almost always one run.
    first: Option<(u64, u32)>,
    more: Vec<(u64, u32)>,
    release: Option<Arc<Release>>,
}

impl Waiters {
    /// Take `waiter` in, with its record's share.
    pub(crate) fn push(&mut self, mut waiter: Waiter) {
        if let Some(release) = waiter.hold.disarm() {
            self.count(waiter.hold.bytes, waiter.hold.generation);
            if self.release.is_none() {
                self.release = Some(release);
            }
        }
        self.list.push(waiter);
    }

    fn count(&mut self, bytes: usize, generation: u64) {
        self.bytes += bytes;
        if let Some((last, n)) = self.more.last_mut().or(self.first.as_mut())
            && *last == generation
        {
            *n += 1;
        } else if self.first.is_none() {
            self.first = Some((generation, 1));
        } else {
            self.more.push((generation, 1));
        }
    }

    /// Split into the first `mid` callers and the rest, each with its own
    /// records' shares.
    fn split_off(mut self, mid: usize) -> (Self, Self) {
        let tail = self.list.split_off(mid.min(self.list.len()));
        let release = self.release.take();
        let mut halves = (Self::default(), Self::default());
        for (half, list) in [
            (&mut halves.0, std::mem::take(&mut self.list)),
            (&mut halves.1, tail),
        ] {
            half.release.clone_from(&release);
            for waiter in &list {
                half.count(waiter.hold.bytes, waiter.hold.generation);
            }
            half.list = list;
        }
        // The shares moved to the halves; `self` gives back nothing.
        self.bytes = 0;
        self.first = None;
        self.more.clear();
        halves
    }

    /// The callers, in record order, to be answered. What they hold is given
    /// back when `self` drops, after the last of them.
    pub(crate) fn drain(&mut self) -> std::vec::Drain<'_, Waiter> {
        self.list.drain(..)
    }
}

impl Drop for Waiters {
    fn drop(&mut self) {
        let Some(release) = self.release.take() else {
            return;
        };
        let records = self
            .first
            .iter()
            .chain(&self.more)
            .map(|&(_, n)| n as usize)
            .sum();
        match self.first {
            Some(first) if self.more.is_empty() => {
                release.settle(self.bytes, records, &[first]);
            }
            Some(first) => {
                let mut all = Vec::with_capacity(self.more.len() + 1);
                all.push(first);
                all.extend_from_slice(&self.more);
                release.settle(self.bytes, records, &all);
            }
            None => {}
        }
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
    /// The record's memory and counts; settled with its batch's [`Waiters`].
    pub(crate) hold: RecordHold,
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
    pub(crate) waiters: Option<Waiters>,
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
            waiters: Some(Waiters::default()),
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
        match self.waiters.as_mut() {
            Some(waiters) => waiters.push(waiter),
            // Not reached: records join only an open batch, whose callers
            // are unanswered. Dropped, the armed hold settles and the caller
            // sees `Closed`.
            None => drop(waiter),
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
        let (waiters, tail_waiters) = self.waiters.take().unwrap_or_default().split_off(mid);
        let ratio = |n: usize| self.bytes * n / count;
        let half = |records: Vec<RecordData>, waiters: Waiters, bytes: usize| Self {
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

    // ── Settling ────────────────────────────────────────────────────────

    const CAPACITY: usize = 1000;

    struct Rig {
        memory: Arc<Semaphore>,
        barrier: Arc<InFlightBarrier>,
        metrics: Arc<ProducerRecorder>,
        release: Arc<Release>,
    }

    impl Rig {
        fn new() -> Self {
            let memory = Arc::new(Semaphore::new(CAPACITY));
            let barrier = Arc::new(InFlightBarrier::new());
            let metrics = Arc::new(ProducerRecorder::default());
            let release = Arc::new(Release::new(
                Arc::clone(&memory),
                Arc::clone(&barrier),
                Arc::clone(&metrics),
            ));
            Self {
                memory,
                barrier,
                metrics,
                release,
            }
        }

        /// A waiter for a record of `bytes`, taken from the budget and
        /// registered in the barrier's current generation, as enqueue does.
        fn waiter(&self, bytes: usize) -> Waiter {
            self.memory
                .try_acquire_many(u32::try_from(bytes).unwrap())
                .unwrap()
                .forget();
            let generation = self.barrier.register("test").unwrap();
            Waiter {
                response: oneshot::channel().0,
                span: tracing::Span::none(),
                context: RecordContext::new(),
                txn: None,
                hold: RecordHold::new(bytes, generation, Arc::clone(&self.release)),
            }
        }

        fn assert_whole(&self) {
            assert_eq!(self.memory.available_permits(), CAPACITY, "memory");
            assert!(self.barrier.is_idle(), "barrier");
            assert_eq!(self.metrics.buffered_records.get(), 0, "gauge");
        }
    }

    #[test]
    fn waiters_count_bytes_and_generation_runs() {
        let rig = Rig::new();
        let mut waiters = Waiters::default();
        waiters.push(rig.waiter(10));
        waiters.push(rig.waiter(20));
        let _ = rig.barrier.snapshot();
        waiters.push(rig.waiter(30));
        assert_eq!(waiters.bytes, 60);
        assert_eq!(waiters.first, Some((0, 2)));
        assert_eq!(waiters.more, vec![(1, 1)]);
        assert_eq!(rig.memory.available_permits(), CAPACITY - 60);

        let (head, tail) = waiters.split_off(1);
        assert_eq!(
            (head.bytes, head.first, head.more.len()),
            (10, Some((0, 1)), 0)
        );
        assert_eq!(
            (tail.bytes, tail.first, tail.more.clone()),
            (50, Some((0, 1)), vec![(1, 1)])
        );

        drop(head);
        assert_eq!(
            rig.memory.available_permits(),
            CAPACITY - 50,
            "head gave back 10"
        );
        assert_eq!(rig.metrics.buffered_records.get(), 2);
        drop(tail);
        rig.assert_whole();
    }

    /// Answering does not settle; dropping the drained collection does, once.
    #[test]
    fn waiters_settle_once_after_the_last_caller() {
        let rig = Rig::new();
        let mut waiters = Waiters::default();
        for _ in 0..5 {
            waiters.push(rig.waiter(100));
        }
        for waiter in waiters.drain() {
            assert_eq!(rig.memory.available_permits(), CAPACITY - 500, "not before");
            drop(waiter);
        }
        assert_eq!(
            rig.memory.available_permits(),
            CAPACITY - 500,
            "not on drain"
        );
        drop(waiters);
        rig.assert_whole();
    }

    /// An armed hold gives back its own share; a disarmed one gives nothing.
    #[test]
    fn an_armed_hold_settles_alone_and_a_disarmed_one_not_at_all() {
        let rig = Rig::new();
        let alone = rig.waiter(100);
        assert_eq!(rig.metrics.buffered_records.get(), 1);
        drop(alone);
        rig.assert_whole();

        let mut taken = rig.waiter(100);
        let release = taken.hold.disarm();
        drop(taken);
        assert_eq!(
            rig.memory.available_permits(),
            CAPACITY - 100,
            "disarmed: nothing"
        );
        assert!(release.is_some());
    }

    /// A batch dropped without answering (the engine is gone) still settles.
    #[test]
    fn unanswered_waiters_settle_on_drop() {
        let rig = Rig::new();
        let mut waiters = Waiters::default();
        for _ in 0..3 {
            waiters.push(rig.waiter(50));
        }
        drop(waiters);
        rig.assert_whole();
    }
}
