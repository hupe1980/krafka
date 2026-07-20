//! Idempotent producer support.
//!
//! This module provides exactly-once semantics for message production by using
//! producer IDs (PID) and sequence numbers per partition.
//!
//! # How Idempotency Works
//!
//! 1. Producer obtains a unique Producer ID (PID) and epoch from the broker
//! 2. For each partition, the producer maintains a sequence number
//! 3. Each record batch includes the PID, epoch, and sequence number
//! 4. Broker uses these to detect and filter duplicates
//!
//! # State Persistence
//!
//! **Important**: Producer ID and sequence numbers are stored in-memory only.
//! On producer restart:
//!
//! - A new Producer ID is obtained from the broker via `InitProducerId`
//! - Sequence numbers start from 0 for each partition
//! - The broker handles this correctly because each new PID is unique
//!
//! This is the **expected behavior** and matches the Kafka Java client behavior.
//! The idempotency guarantee is:
//!
//! > Within a single producer session (single PID), messages will not be duplicated.
//!
//! # Zombie Producer Fencing — Limitation
//!
//! Plain idempotent producers (without a `transactional.id`) do **not** provide
//! zombie fencing. If a producer crashes and restarts, both the old instance (the
//! "zombie") and the new instance may produce to the same partition simultaneously.
//! The broker cannot distinguish between them because each obtains a new PID.
//!
//! This is a known limitation documented in KIP-360. The zombie scenario:
//!
//! 1. Producer A (epoch N) stalls due to a long GC pause.
//! 2. Producer A restarts, obtains a new PID (epoch 0 on the new PID).
//! 3. Original A wakes up and retries an old batch — broker accepts it
//!    because the old PID+epoch+sequence are valid.
//! 4. Both producers may now write to the same partition.
//!
//! For exactly-once semantics across producer restarts, use
//! [`TransactionalProducer`](super::transaction::TransactionalProducer) with a
//! stable `transactional.id`. The broker fences the old producer epoch when a new
//! producer initialises with the same transactional ID (KIP-360).
//!
//! For exactly-once guarantees across producer restarts, use **transactions** with
//! a stable `transactional.id`, which the broker uses to fence zombie producers.
//!
//! # Example
//!
//! Idempotent production is enabled by default (KIP-679 / Kafka 3.0+):
//!
//! ```ignore
//! use krafka::producer::{Producer, ProducerConfig};
//!
//! // Idempotent by default — no extra configuration needed.
//! let producer = Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .build()
//!     .await?;
//!
//! // To explicitly disable idempotency:
//! let producer = Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .idempotent(false)
//!     .build()
//!     .await?;
//! ```

use ahash::AHashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;

use crate::PartitionId;
use crate::error::{KrafkaError, ProtocolErrorKind, Result};

/// Producer identity for idempotent production.
///
/// This struct holds the producer ID and epoch assigned by the broker,
/// along with sequence numbers for each partition.
///
/// All mutable identity state (`producer_id`, `producer_epoch`, and
/// `sequences`) is protected by a single [`RwLock`].  This eliminates the
/// TOCTOU window that would otherwise exist between a lock-free
/// `is_initialized()` read and a concurrent `initialize()` that clears
/// sequences: callers always observe a fully consistent snapshot.
///
/// The `needs_reinit` flag is a separate [`AtomicBool`] because it is set and
/// checked on an independent code path that does not need to be coordinated
/// with sequence state.
#[derive(Debug)]
pub struct ProducerIdentity {
    /// Set when the local sequence space for this PID can no longer be trusted
    /// (for example after a non-tail rollback) and a fresh `InitProducerId`
    /// must be performed before the next batch is dispatched.
    ///
    /// This replaces the previous `poisoned` flag, which permanently bricked
    /// the producer.  Requesting a re-init is recoverable: the next send
    /// obtains a new PID and restarts every partition at sequence 0.
    needs_reinit: AtomicBool,
    /// All mutable identity state behind one lock for consistency.
    inner: RwLock<IdentityInner>,
}

/// Outcome of a sequence-range rollback attempt.
///
/// See [`ProducerIdentity::rollback_sequence_range`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackOutcome {
    /// The batch owned the tail of the allocated range and the partition
    /// counter was rewound; the sequence space is intact.
    RolledBack,
    /// The batch did **not** own the tail — a newer range has already been
    /// allocated for this partition, so rewinding would hand the same
    /// sequence numbers to two different batches.  Nothing was changed and
    /// the caller must recover by re-initialising the producer ID (or, for
    /// a transactional producer, bumping the epoch) and resetting the
    /// partition sequences.
    NotTail,
}

/// Mutable state held inside [`ProducerIdentity`].
#[derive(Debug)]
struct IdentityInner {
    /// Producer ID assigned by the broker (-1 if not initialized).
    producer_id: i64,
    /// Producer epoch assigned by the broker (-1 if not initialized).
    ///
    /// Stored as `i16` to match the Kafka wire type, eliminating the need for
    /// a truncating `as i16` cast on read paths.
    producer_epoch: i16,
    /// Sequence numbers per topic-partition.
    sequences: AHashMap<String, AHashMap<PartitionId, SequenceState>>,
}

impl IdentityInner {
    fn uninitialized() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1_i16,
            sequences: AHashMap::new(),
        }
    }

    fn is_initialized(&self) -> bool {
        self.producer_id >= 0
    }
}

/// Sequence number state for a partition.
#[derive(Debug, Clone)]
struct SequenceState {
    /// The next sequence number to use.
    next_sequence: i32,
    /// The last successfully acknowledged sequence number.
    last_acked_sequence: i32,
}

const SEQUENCE_SPACE: u32 = i32::MAX as u32 + 1;
const HALF_SEQUENCE_SPACE: u32 = SEQUENCE_SPACE / 2;

/// Compute the last sequence number in a multi-record batch.
///
/// Given a `base_sequence` and `count` records, returns
/// `base_sequence + count - 1`, wrapping at the sequence space boundary.
/// Matches the Kafka Java client's `ProducerBatch.lastSequence()`.
///
/// # Errors
///
/// Returns an error if `count <= 0`.
pub(crate) fn last_sequence_of_batch(base_sequence: i32, count: i32) -> Result<i32> {
    if count <= 0 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidValue,
            "count must be positive",
        ));
    }
    Ok(((base_sequence as u32).wrapping_add((count - 1) as u32) % SEQUENCE_SPACE) as i32)
}

fn next_sequence_after(sequence: i32) -> i32 {
    if !(0..i32::MAX).contains(&sequence) {
        0
    } else {
        sequence + 1
    }
}

fn is_newer_sequence(last_acked_sequence: i32, candidate_sequence: i32) -> bool {
    if last_acked_sequence < 0 {
        return true;
    }
    if candidate_sequence == last_acked_sequence {
        return false;
    }

    // Negative candidate sequences are invalid; reject before casting to u32.
    if candidate_sequence < 0 {
        return false;
    }

    let last = last_acked_sequence as u32;
    let candidate = candidate_sequence as u32;
    let forward_distance = if candidate >= last {
        candidate - last
    } else {
        (SEQUENCE_SPACE - last) + candidate
    };

    forward_distance < HALF_SEQUENCE_SPACE
}

impl Default for SequenceState {
    fn default() -> Self {
        Self {
            next_sequence: 0,
            last_acked_sequence: -1,
        }
    }
}

impl ProducerIdentity {
    /// Create a new uninitialized producer identity.
    pub fn new() -> Self {
        Self {
            needs_reinit: AtomicBool::new(false),
            inner: RwLock::new(IdentityInner::uninitialized()),
        }
    }

    /// Check if the producer identity has been initialized.
    ///
    /// The check is performed while holding the inner read lock, so the result
    /// is always consistent with the current `producer_id` and `sequences`
    /// state — there is no TOCTOU window with a concurrent
    /// [`initialize`](Self::initialize) or [`reset`](Self::reset).
    pub fn is_initialized(&self) -> bool {
        self.inner.read().is_initialized()
    }

    /// Get the producer ID.
    pub fn producer_id(&self) -> i64 {
        self.inner.read().producer_id
    }

    /// Get the producer epoch.
    pub fn producer_epoch(&self) -> i16 {
        self.inner.read().producer_epoch
    }

    /// Initialize with the producer ID and epoch from the broker.
    ///
    /// All fields — `producer_id`, `producer_epoch`, and `sequences` — are
    /// updated atomically under the write lock.  Concurrent callers of
    /// [`is_initialized`](Self::is_initialized), [`producer_id`](Self::producer_id),
    /// or any sequence method will either see the fully-old state or the
    /// fully-new state; there is no intermediate window.
    pub fn initialize(&self, producer_id: i64, producer_epoch: i16) {
        self.set_identity(producer_id, producer_epoch);
    }

    /// Adopt a `(producer_id, producer_epoch)` pair that the transaction
    /// coordinator bumped on the client's behalf.
    ///
    /// # Transaction version
    ///
    /// This is the KIP-890 **transaction version 2** path. Under TV2 the
    /// coordinator increments the producer epoch as part of writing every
    /// commit/abort marker and returns the new pair in the `EndTxn` v4+
    /// response. The producer must adopt it: the old epoch is fenced the
    /// moment the marker is written, so the next transaction started with the
    /// stale epoch fails with `InvalidProducerEpoch`.
    ///
    /// When the epoch would overflow `i16::MAX` the coordinator allocates a
    /// fresh producer ID and returns epoch 0 instead, which is why the
    /// producer ID is adopted alongside the epoch rather than assumed stable.
    ///
    /// Under **transaction version 1** the coordinator does not bump on
    /// completion and this is never called.
    ///
    /// # Sequence reset
    ///
    /// A new epoch (or a new producer ID) starts a fresh sequence space: the
    /// broker resets its expected sequence for every partition to 0, so every
    /// local per-partition counter must be dropped in the same critical
    /// section. Keeping a stale counter would make the first batch of the next
    /// transaction arrive with a non-zero base sequence and be rejected as
    /// `OutOfOrderSequenceNumber`.
    ///
    /// Any pending re-init request is also cleared — the bump already gave the
    /// producer the clean sequence space that a re-init would have obtained.
    pub fn bump_epoch(&self, producer_id: i64, producer_epoch: i16) {
        self.set_identity(producer_id, producer_epoch);
    }

    /// Install a producer identity and drop all derived sequence state.
    ///
    /// Every field is written under a single write lock so concurrent readers
    /// observe either the fully-old or the fully-new identity, never a PID
    /// paired with the previous epoch's sequence counters.
    fn set_identity(&self, producer_id: i64, producer_epoch: i16) {
        let mut inner = self.inner.write();
        inner.producer_id = producer_id;
        inner.producer_epoch = producer_epoch;
        self.needs_reinit.store(false, Ordering::Release);
        inner.sequences.clear();
    }

    /// Reset the identity (e.g., after a fatal error).
    ///
    /// All fields are updated atomically under the write lock.
    pub fn reset(&self) {
        let mut inner = self.inner.write();
        inner.producer_id = -1;
        inner.producer_epoch = -1_i16;
        self.needs_reinit.store(false, Ordering::Release);
        inner.sequences.clear();
    }

    /// Request that a fresh `InitProducerId` be performed before the next
    /// batch is dispatched.
    ///
    /// Used when the local sequence space can no longer be trusted — for
    /// example when a failed batch did not own the tail of the allocated
    /// range, so rewinding would duplicate sequence numbers.  Unlike the
    /// previous "poison" behaviour this is fully recoverable: the next send
    /// obtains a new PID and every partition restarts at sequence 0.
    pub(crate) fn request_reinit(&self) {
        self.needs_reinit.store(true, Ordering::Release);
    }

    /// Whether a re-initialisation has been requested via
    /// [`request_reinit`](Self::request_reinit).
    pub(crate) fn needs_reinit(&self) -> bool {
        self.needs_reinit.load(Ordering::Acquire)
    }

    /// Consume a pending re-init request, clearing the PID/epoch and all
    /// sequences so the next `InitProducerId` starts from a clean slate.
    ///
    /// Returns `true` when a request was pending (and state was cleared).
    pub(crate) fn take_reinit_request(&self) -> bool {
        if self.needs_reinit.swap(false, Ordering::AcqRel) {
            let mut inner = self.inner.write();
            inner.producer_id = -1;
            inner.producer_epoch = -1_i16;
            inner.sequences.clear();
            true
        } else {
            false
        }
    }

    /// Drop all per-partition sequence state while keeping the current
    /// PID/epoch.
    ///
    /// Used after a transactional epoch bump, where the broker resets its
    /// expected sequence for every partition to 0 but the producer ID stays
    /// the same.
    pub fn reset_all_sequences(&self) {
        self.inner.write().sequences.clear();
    }

    /// Drop the sequence state for a single partition.
    ///
    /// The next allocation for that partition starts again at 0 with no
    /// acknowledged sequence.  Only safe once the broker-side expectation has
    /// also been reset (new PID or bumped epoch).
    pub fn reset_partition_sequences(&self, topic: &str, partition: PartitionId) {
        let mut inner = self.inner.write();
        if let Some(parts) = inner.sequences.get_mut(topic) {
            parts.remove(&partition);
        }
    }

    /// Get the next sequence number for a topic-partition (single-record batch).
    ///
    /// This allocates a new sequence number for the next batch.
    /// Sequence numbers wrap to 0 at `i32::MAX`, matching the Kafka Java client
    /// behavior (`DefaultRecordBatch.incrementSequence()`).
    ///
    /// For multi-record batches, use [`allocate_sequence`](Self::allocate_sequence).
    pub fn next_sequence(&self, topic: &str, partition: PartitionId) -> Result<i32> {
        self.allocate_sequence(topic, partition, 1)
    }

    /// Allocate a contiguous range of `count` sequence numbers for a batch.
    ///
    /// Returns the base sequence. The internal counter advances by `count`,
    /// wrapping at the sequence space boundary (`i32::MAX` → 0).
    ///
    /// # Errors
    ///
    /// Returns an error if `count <= 0`.
    pub fn allocate_sequence(
        &self,
        topic: &str,
        partition: PartitionId,
        count: i32,
    ) -> Result<i32> {
        if count <= 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                "count must be positive",
            ));
        }

        let mut inner = self.inner.write();
        let state = inner
            .sequences
            .entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_default();
        let base = state.next_sequence;
        state.next_sequence = ((base as u32).wrapping_add(count as u32) % SEQUENCE_SPACE) as i32;
        Ok(base)
    }

    /// Peek at the next sequence number without incrementing.
    pub fn peek_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let inner = self.inner.read();
        inner
            .sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
            .map(|s| s.next_sequence)
            .unwrap_or(0)
    }

    /// Acknowledge a sequence number for a partition.
    ///
    /// Call this when a batch is successfully acknowledged by the broker.
    pub fn acknowledge(&self, topic: &str, partition: PartitionId, sequence: i32) {
        let mut inner = self.inner.write();
        if let Some(state) = inner
            .sequences
            .get_mut(topic)
            .and_then(|parts| parts.get_mut(&partition))
            && is_newer_sequence(state.last_acked_sequence, sequence)
        {
            state.last_acked_sequence = sequence;
        }
    }

    /// Roll back a single-record allocation that starts at `base_sequence`.
    ///
    /// Convenience wrapper around
    /// [`rollback_sequence_range`](Self::rollback_sequence_range) with
    /// `count = 1`; the same tail-ownership rules apply.
    pub fn rollback_sequence(
        &self,
        topic: &str,
        partition: PartitionId,
        base_sequence: i32,
    ) -> Result<RollbackOutcome> {
        self.rollback_sequence_range(topic, partition, base_sequence, 1)
    }

    /// Roll back the range `[base_sequence, base_sequence + count)` **only if
    /// the caller owns the tail** of the partition's allocated sequence space.
    ///
    /// Used when a batch was allocated a sequence range via
    /// [`allocate_sequence`](Self::allocate_sequence) but will never reach the
    /// broker (encode failure, exhausted retries), so that the next batch does
    /// not open a permanent gap.
    ///
    /// # Why the tail check matters
    ///
    /// An unconditional modular subtraction corrupts the sequence space: if a
    /// newer batch has already been allocated `[base + count, …)`, rewinding
    /// hands the *same* sequence numbers to two different batches. The broker
    /// then either silently deduplicates real data or wedges the partition
    /// with `OUT_OF_ORDER_SEQUENCE_NUMBER` forever.
    ///
    /// This method therefore verifies that `next_sequence == base_sequence +
    /// count` before rewinding. When that does not hold it changes nothing and
    /// returns [`RollbackOutcome::NotTail`]; the caller must recover by
    /// requesting a producer-ID re-init or, for a transactional producer, an
    /// epoch bump, and then resetting the partition sequences.
    ///
    /// # Errors
    ///
    /// Returns an error if `count <= 0`.
    pub fn rollback_sequence_range(
        &self,
        topic: &str,
        partition: PartitionId,
        base_sequence: i32,
        count: i32,
    ) -> Result<RollbackOutcome> {
        if count <= 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                "count must be positive",
            ));
        }

        let expected_tail =
            ((base_sequence as u32).wrapping_add(count as u32) % SEQUENCE_SPACE) as i32;

        let mut inner = self.inner.write();
        let Some(state) = inner
            .sequences
            .get_mut(topic)
            .and_then(|parts| parts.get_mut(&partition))
        else {
            // Nothing allocated for this partition at all — there is no tail
            // to rewind and no newer allocation to corrupt.
            return Ok(RollbackOutcome::RolledBack);
        };

        if state.next_sequence != expected_tail {
            return Ok(RollbackOutcome::NotTail);
        }

        state.next_sequence = base_sequence;
        Ok(RollbackOutcome::RolledBack)
    }

    /// Whether it is safe to locally rewind after
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` for the batch
    /// `[base_sequence, base_sequence + count)`.
    ///
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` usually means an **earlier** batch never
    /// made it into the log (log truncation, unclean leader election). Blindly
    /// resetting `next_sequence` to `last_acked + 1` and resending writes the
    /// current batch into that hole and reports success for a stream that is
    /// silently missing records.
    ///
    /// A local reset is only defensible when this batch is head-of-line:
    ///
    /// * its base sequence is exactly `last_acked + 1`, so no earlier batch is
    ///   unaccounted for, **and**
    /// * no newer range has been allocated for the partition, so nothing else
    ///   depends on the current counter.
    ///
    /// When this returns `false` the caller must surface a fatal, non-retriable
    /// error rather than rewinding (matching librdkafka, which raises a fatal
    /// error for a head-of-line failure).
    ///
    /// # Errors
    ///
    /// Returns an error if `count <= 0`.
    pub fn can_reset_after_out_of_order(
        &self,
        topic: &str,
        partition: PartitionId,
        base_sequence: i32,
        count: i32,
    ) -> Result<bool> {
        let last_sequence = last_sequence_of_batch(base_sequence, count)?;
        let inner = self.inner.read();
        let Some(state) = inner
            .sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
        else {
            return Ok(false);
        };

        Ok(
            base_sequence == next_sequence_after(state.last_acked_sequence)
                && state.next_sequence == next_sequence_after(last_sequence),
        )
    }

    /// Reset sequence number for a partition (e.g., after an out-of-order error).
    pub fn reset_sequence(&self, topic: &str, partition: PartitionId) {
        let mut inner = self.inner.write();
        if let Some(state) = inner
            .sequences
            .get_mut(topic)
            .and_then(|parts| parts.get_mut(&partition))
        {
            // Reset to the last acknowledged + 1
            state.next_sequence = next_sequence_after(state.last_acked_sequence);
        }
    }

    /// Atomically reset the partition sequence and allocate a fresh range.
    ///
    /// Equivalent to [`reset_sequence`](Self::reset_sequence) +
    /// [`allocate_sequence`](Self::allocate_sequence) under a single lock,
    /// preventing TOCTOU races when multiple concurrent sends hit
    /// `OutOfOrderSequenceNumber` for the same partition.
    ///
    /// # Errors
    ///
    /// Returns an error if `count <= 0`.
    pub fn reset_and_allocate(
        &self,
        topic: &str,
        partition: PartitionId,
        count: i32,
    ) -> Result<i32> {
        if count <= 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                "count must be positive",
            ));
        }

        let mut inner = self.inner.write();
        let state = inner
            .sequences
            .entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_default();
        state.next_sequence = next_sequence_after(state.last_acked_sequence);
        let base = state.next_sequence;
        state.next_sequence = ((base as u32).wrapping_add(count as u32) % SEQUENCE_SPACE) as i32;
        Ok(base)
    }

    /// Get the last acknowledged sequence for a partition.
    pub fn last_acked_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let inner = self.inner.read();
        inner
            .sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
            .map(|s| s.last_acked_sequence)
            .unwrap_or(-1)
    }

    /// Return whether an `UnknownProducerId` for this batch can be retried
    /// safely after resetting producer state.
    ///
    /// Recovery is only safe when the failing batch is still the oldest
    /// unresolved sequence range for the partition and no newer sequence range
    /// has been allocated locally. That matches the local condition we can
    /// verify before reinitializing the producer identity and rebuilding the
    /// batch under a fresh PID/epoch.
    ///
    /// Only used in unit tests — production code uses the atomic
    /// `check_and_reset_if_retryable()` to avoid the TOCTOU window.
    #[cfg(test)]
    pub(crate) fn can_retry_unknown_producer_id(
        &self,
        topic: &str,
        partition: PartitionId,
        base_sequence: i32,
        count: i32,
    ) -> Result<bool> {
        let last_sequence = last_sequence_of_batch(base_sequence, count)?;
        let inner = self.inner.read();
        let Some(state) = inner
            .sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
        else {
            return Ok(false);
        };

        Ok(
            base_sequence == next_sequence_after(state.last_acked_sequence)
                && state.next_sequence == next_sequence_after(last_sequence),
        )
    }

    /// Atomically check whether this `UnknownProducerId` error is retryable
    /// and, if so, reset all identity state in the same write-lock acquisition.
    ///
    /// This eliminates the TOCTOU window that would exist between a separate
    /// `can_retry_unknown_producer_id()` (read lock) and `reset()` (write lock)
    /// call: a concurrent thread cannot allocate new sequence numbers between
    /// the check and the reset.
    ///
    /// Returns `true` if the identity was reset and recovery can proceed.
    /// Returns `false` (and leaves state unchanged) if recovery is unsafe
    /// (newer in-flight batches already used the current PID+epoch).
    pub(crate) fn check_and_reset_if_retryable(
        &self,
        topic: &str,
        partition: PartitionId,
        base_sequence: i32,
        count: i32,
    ) -> Result<bool> {
        let last_sequence = last_sequence_of_batch(base_sequence, count)?;
        let mut inner = self.inner.write();
        let Some(state) = inner
            .sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
        else {
            return Ok(false);
        };

        let retryable = base_sequence == next_sequence_after(state.last_acked_sequence)
            && state.next_sequence == next_sequence_after(last_sequence);

        if retryable {
            inner.producer_id = -1;
            inner.producer_epoch = -1_i16;
            inner.sequences.clear();
            self.needs_reinit.store(false, Ordering::Release);
        }
        Ok(retryable)
    }

    /// Allocate a sequence range only if the identity is currently initialized.
    ///
    /// Combines the `is_initialized()` check and `allocate_sequence()` into a
    /// single write-lock acquisition, eliminating the TOCTOU window where a
    /// concurrent `reset()` could clear the PID between the check and the
    /// allocation.
    ///
    /// Returns `None` if the identity is not yet initialized (caller should
    /// invoke `init_idempotent_producer_id` and retry). Returns `Some(base)`
    /// with the allocated base sequence on success.
    ///
    /// # Errors
    ///
    /// Returns an error if `count <= 0`.
    pub(crate) fn checked_allocate_sequence(
        &self,
        topic: &str,
        partition: PartitionId,
        count: i32,
    ) -> Result<Option<i32>> {
        if count <= 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                "count must be positive",
            ));
        }
        let mut inner = self.inner.write();
        if !inner.is_initialized() {
            return Ok(None);
        }
        let state = inner
            .sequences
            .entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_default();
        let base = state.next_sequence;
        state.next_sequence = ((base as u32).wrapping_add(count as u32) % SEQUENCE_SPACE) as i32;
        Ok(Some(base))
    }

    /// Create a consistent snapshot of the current idempotent state.
    ///
    /// `producer_id`, `producer_epoch`, and all partition sequences are read
    /// under a single read lock, so the snapshot is always self-consistent.
    pub fn snapshot(&self) -> ProducerIdentitySnapshot {
        let inner = self.inner.read();
        let partition_sequences = inner
            .sequences
            .iter()
            .flat_map(|(topic, parts)| {
                parts
                    .iter()
                    .map(move |(part, state)| PartitionSequenceSnapshot {
                        topic: topic.clone(),
                        partition: *part,
                        next_sequence: state.next_sequence,
                        last_acked_sequence: state.last_acked_sequence,
                    })
            })
            .collect();

        ProducerIdentitySnapshot {
            producer_id: inner.producer_id,
            producer_epoch: inner.producer_epoch,
            partition_sequences,
        }
    }

    /// Remove sequence tracking for a single partition.
    ///
    /// Call when a partition is decommissioned or its topic is deleted, to
    /// prevent unbounded growth of the in-memory sequence map.
    pub fn remove_partition(&self, topic: &str, partition: PartitionId) {
        let mut inner = self.inner.write();
        if let Some(parts) = inner.sequences.get_mut(topic) {
            parts.remove(&partition);
            if parts.is_empty() {
                inner.sequences.remove(topic);
            }
        }
    }

    /// Remove all sequence tracking for a topic.
    ///
    /// Removes every partition entry for the given topic. Useful when a topic
    /// is deleted or the producer stops writing to it entirely.
    pub fn remove_topic(&self, topic: &str) {
        self.inner.write().sequences.remove(topic);
    }

    /// Retain only the partitions listed in `active`; drop all others.
    ///
    /// `active` maps topic name to the set of active partition IDs. Any
    /// topic not present in the map, or any partition not listed under its
    /// topic, is removed from the sequence state. Use this after a metadata
    /// refresh to prune stale entries.
    pub fn retain_partitions(&self, active: &ahash::AHashMap<String, Vec<PartitionId>>) {
        let mut inner = self.inner.write();
        inner.sequences.retain(|topic, parts| {
            if let Some(active_parts) = active.get(topic.as_str()) {
                parts.retain(|pid, _| active_parts.contains(pid));
                !parts.is_empty()
            } else {
                false
            }
        });
    }

    /// Initialize identity from a previously persisted snapshot.
    ///
    /// Atomically replaces the current `producer_id`, `producer_epoch`, and
    /// all per-partition sequence numbers with the values in `snapshot`.
    ///
    /// # Safety
    ///
    /// Only call this when the snapshot's `producer_id` and `producer_epoch`
    /// match what the broker returned from `InitProducerId`. This typically
    /// only happens with a transactional producer that uses a stable
    /// `transactional.id` — the broker uses the transactional ID to fence
    /// zombie producers and may return the same PID with a bumped epoch.
    ///
    /// For plain idempotent producers, the broker returns a fresh PID with
    /// epoch 0 on every call; do **not** call this method in that case as the
    /// sequences would be invalid for the new PID.
    ///
    /// The [`ProducerBuilder::state_store`](super::ProducerBuilder::state_store)
    /// integration only restores the snapshot when `producer_id` and
    /// `producer_epoch` match the broker response; manual callers are
    /// responsible for the same check.
    pub fn restore_from_snapshot(&self, snapshot: &ProducerIdentitySnapshot) {
        let mut inner = self.inner.write();
        inner.producer_id = snapshot.producer_id;
        inner.producer_epoch = snapshot.producer_epoch;
        inner.sequences.clear();
        for ps in &snapshot.partition_sequences {
            inner.sequences.entry(ps.topic.clone()).or_default().insert(
                ps.partition,
                SequenceState {
                    next_sequence: ps.next_sequence,
                    last_acked_sequence: ps.last_acked_sequence,
                },
            );
        }
        self.needs_reinit.store(false, Ordering::Release);
    }

    /// Directly set sequence state for a partition.
    ///
    /// Used in tests to seed specific sequence states without going through
    /// the public API (which would require fabricating a full produce cycle).
    #[cfg(test)]
    fn set_sequence_state(
        &self,
        topic: &str,
        partition: PartitionId,
        next_sequence: i32,
        last_acked_sequence: i32,
    ) {
        self.inner
            .write()
            .sequences
            .entry(topic.to_string())
            .or_default()
            .insert(
                partition,
                SequenceState {
                    next_sequence,
                    last_acked_sequence,
                },
            );
    }
}

impl Default for ProducerIdentity {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of producer identity state for metrics/debugging.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ProducerIdentitySnapshot {
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Sequence states per partition.
    pub partition_sequences: Vec<PartitionSequenceSnapshot>,
}

/// Snapshot of sequence state for a single partition.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PartitionSequenceSnapshot {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// Next sequence number to use.
    pub next_sequence: i32,
    /// Last acknowledged sequence number.
    pub last_acked_sequence: i32,
}

/// Pluggable persistence hook for producer identity state.
///
/// Implement this trait to save and restore [`ProducerIdentitySnapshot`]s
/// across producer restarts, enabling recovery of sequence state for
/// transactional producers.
///
/// # Safety — sequence restoration
///
/// Restoring sequence numbers into a producer is only safe when **all** of
/// the following hold:
///
/// 1. The stored `producer_id` **and** `producer_epoch` exactly match what
///    the broker returned from `InitProducerId`.
/// 2. The producer uses a stable `transactional.id` so the broker can fence
///    zombie producers from prior sessions.
///
/// For plain idempotent producers (no `transactional.id`), `InitProducerId`
/// always returns a fresh PID with epoch 0, so restored sequences can never
/// match and will be ignored. The store is still useful for observability in
/// this case.
///
/// # Example
///
/// ```ignore
/// use krafka::producer::{ProducerStateStore, ProducerIdentitySnapshot};
///
/// struct FileStateStore { path: std::path::PathBuf }
///
/// impl ProducerStateStore for FileStateStore {
///     async fn load(&self) -> krafka::Result<Option<ProducerIdentitySnapshot>> {
///         // read from disk …
///         Ok(None)
///     }
///     async fn store(&self, snapshot: &ProducerIdentitySnapshot) -> krafka::Result<()> {
///         // write to disk …
///         Ok(())
///     }
/// }
/// ```
pub trait ProducerStateStore: Send + Sync {
    /// Load a previously persisted snapshot, if any.
    ///
    /// Called once during [`Producer::build()`](super::Producer). Return
    /// `Ok(None)` if no snapshot exists (first run).
    fn load(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<ProducerIdentitySnapshot>>> + Send;

    /// Persist the current snapshot.
    ///
    /// Called asynchronously after each successful batch acknowledgement.
    /// Errors are logged at `warn!` level and do not fail the produce
    /// operation.
    fn store(
        &self,
        snapshot: &ProducerIdentitySnapshot,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

// ── Object-safe erased trait for `Arc<dyn …>` storage ─────────────────────
//
// `ProducerStateStore` uses `async fn` / RPITIT returns which are not
// dyn-compatible.  `ErasedProducerStateStore` mirrors it with
// `Pin<Box<dyn Future>>` returns so `Producer` can store
// `Arc<dyn ErasedProducerStateStore>` without generic parameters.
// A blanket impl converts any `ProducerStateStore` transparently.
pub(crate) trait ErasedProducerStateStore: Send + Sync {
    fn load_erased(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<Option<ProducerIdentitySnapshot>>> + Send + '_>,
    >;

    fn store_erased<'a>(
        &'a self,
        snapshot: &'a ProducerIdentitySnapshot,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
}

impl<T: ProducerStateStore> ErasedProducerStateStore for T {
    fn load_erased(
        &self,
    ) -> Pin<
        Box<dyn std::future::Future<Output = Result<Option<ProducerIdentitySnapshot>>> + Send + '_>,
    > {
        Box::pin(self.load())
    }

    fn store_erased<'a>(
        &'a self,
        snapshot: &'a ProducerIdentitySnapshot,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(self.store(snapshot))
    }
}

/// Allow sharing a state store instance behind an `Arc`.
///
/// Enables `Arc<MyStore>` to be passed to
/// [`ProducerBuilder::state_store()`](super::ProducerBuilder::state_store)
/// when the same store is shared across multiple producers.
impl<T: ProducerStateStore> ProducerStateStore for std::sync::Arc<T> {
    fn load(
        &self,
    ) -> impl std::future::Future<Output = Result<Option<ProducerIdentitySnapshot>>> + Send {
        T::load(self)
    }

    fn store(
        &self,
        snapshot: &ProducerIdentitySnapshot,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        T::store(self, snapshot)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_identity_new() {
        let identity = ProducerIdentity::new();
        assert!(!identity.is_initialized());
        assert_eq!(identity.producer_id(), -1);
        assert_eq!(identity.producer_epoch(), -1);
    }

    #[test]
    fn test_producer_identity_initialize() {
        let identity = ProducerIdentity::new();
        identity.initialize(12345, 0);

        assert!(identity.is_initialized());
        assert_eq!(identity.producer_id(), 12345);
        assert_eq!(identity.producer_epoch(), 0);
    }

    #[test]
    fn test_sequence_numbers() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // First sequence should be 0
        assert_eq!(identity.next_sequence("topic", 0).unwrap(), 0);
        assert_eq!(identity.next_sequence("topic", 0).unwrap(), 1);
        assert_eq!(identity.next_sequence("topic", 0).unwrap(), 2);

        // Different partition starts at 0
        assert_eq!(identity.next_sequence("topic", 1).unwrap(), 0);

        // Different topic starts at 0
        assert_eq!(identity.next_sequence("other-topic", 0).unwrap(), 0);
    }

    #[test]
    fn test_peek_sequence() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Peek should not increment
        assert_eq!(identity.peek_sequence("topic", 0), 0);
        assert_eq!(identity.peek_sequence("topic", 0), 0);

        // After getting next, peek should show new value
        identity.next_sequence("topic", 0).unwrap();
        assert_eq!(identity.peek_sequence("topic", 0), 1);
    }

    #[test]
    fn test_acknowledge() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Get some sequences
        identity.next_sequence("topic", 0).unwrap();
        identity.next_sequence("topic", 0).unwrap();
        identity.next_sequence("topic", 0).unwrap();

        // Acknowledge sequence 1
        identity.acknowledge("topic", 0, 1);
        assert_eq!(identity.last_acked_sequence("topic", 0), 1);

        // Acknowledging lower sequence should not change
        identity.acknowledge("topic", 0, 0);
        assert_eq!(identity.last_acked_sequence("topic", 0), 1);

        // Acknowledging higher sequence should update
        identity.acknowledge("topic", 0, 2);
        assert_eq!(identity.last_acked_sequence("topic", 0), 2);
    }

    #[test]
    fn test_reset_sequence() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Advance sequence
        identity.next_sequence("topic", 0).unwrap();
        identity.next_sequence("topic", 0).unwrap();
        identity.next_sequence("topic", 0).unwrap();

        // Acknowledge up to 1
        identity.acknowledge("topic", 0, 1);

        // Reset should go back to last_acked + 1
        identity.reset_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), 2);
    }

    #[test]
    fn test_acknowledge_wraps_from_max_to_zero() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        identity.set_sequence_state("topic", 0, 1, i32::MAX);

        identity.acknowledge("topic", 0, 0);
        assert_eq!(identity.last_acked_sequence("topic", 0), 0);

        identity.acknowledge("topic", 0, i32::MAX);
        assert_eq!(identity.last_acked_sequence("topic", 0), 0);
    }

    #[test]
    fn test_reset_sequence_wraps_to_zero_after_max_ack() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        identity.set_sequence_state("topic", 0, 1, i32::MAX);

        identity.reset_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    #[test]
    fn test_reset_identity() {
        let identity = ProducerIdentity::new();
        identity.initialize(12345, 5);
        identity.next_sequence("topic", 0).unwrap();

        identity.reset();

        assert!(!identity.is_initialized());
        assert_eq!(identity.producer_id(), -1);
        assert_eq!(identity.producer_epoch(), -1);
        // Sequences are cleared, so next should start at 0
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    /// The KIP-890 TV2 epoch bump keeps the producer initialized (unlike
    /// `reset`) while restarting every partition's sequence space, because the
    /// broker resets its expected sequence to 0 for the new epoch.
    #[test]
    fn test_bump_epoch_keeps_identity_and_resets_all_sequences() {
        let identity = ProducerIdentity::new();
        identity.initialize(12345, 5);

        for _ in 0..3 {
            identity.next_sequence("topic-a", 0).unwrap();
        }
        identity.next_sequence("topic-b", 7).unwrap();
        identity.acknowledge("topic-a", 0, 2);
        assert_eq!(identity.peek_sequence("topic-a", 0), 3);
        assert_eq!(identity.last_acked_sequence("topic-a", 0), 2);

        identity.bump_epoch(12345, 6);

        assert!(identity.is_initialized());
        assert_eq!(identity.producer_id(), 12345);
        assert_eq!(identity.producer_epoch(), 6);
        assert_eq!(identity.peek_sequence("topic-a", 0), 0);
        assert_eq!(identity.peek_sequence("topic-b", 7), 0);
        assert_eq!(
            identity.last_acked_sequence("topic-a", 0),
            -1,
            "acknowledgements from the previous epoch must not carry over"
        );
    }

    /// A bump hands the producer the clean sequence space that a pending
    /// re-init would have obtained, so the request is satisfied and cleared.
    #[test]
    fn test_bump_epoch_clears_pending_reinit_request() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        identity.request_reinit();
        assert!(identity.needs_reinit());

        identity.bump_epoch(1, 1);

        assert!(!identity.needs_reinit());
        assert_eq!(identity.producer_epoch(), 1);
    }

    #[test]
    fn test_snapshot() {
        let identity = ProducerIdentity::new();
        identity.initialize(100, 1);
        identity.next_sequence("topic1", 0).unwrap();
        identity.next_sequence("topic1", 0).unwrap();
        identity.acknowledge("topic1", 0, 0);
        identity.next_sequence("topic2", 0).unwrap();

        let snapshot = identity.snapshot();
        assert_eq!(snapshot.producer_id, 100);
        assert_eq!(snapshot.producer_epoch, 1);
        assert_eq!(snapshot.partition_sequences.len(), 2);
    }

    #[test]
    fn test_can_retry_unknown_producer_id_for_oldest_unacked_batch() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        assert_eq!(identity.allocate_sequence("topic", 0, 2).unwrap(), 0);

        assert!(
            identity
                .can_retry_unknown_producer_id("topic", 0, 0, 2)
                .unwrap()
        );
    }

    #[test]
    fn test_cannot_retry_unknown_producer_id_when_newer_batch_exists() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        assert_eq!(identity.allocate_sequence("topic", 0, 2).unwrap(), 0);
        assert_eq!(identity.allocate_sequence("topic", 0, 1).unwrap(), 2);

        assert!(
            !identity
                .can_retry_unknown_producer_id("topic", 0, 0, 2)
                .unwrap()
        );
    }

    #[test]
    fn test_reinit_request_clears_on_reset_and_reinitialize() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        identity.request_reinit();

        assert!(identity.needs_reinit());

        identity.reset();
        assert!(!identity.needs_reinit());

        identity.request_reinit();
        identity.initialize(2, 1);
        assert!(!identity.needs_reinit());
    }

    /// `take_reinit_request` consumes the flag exactly once and wipes the
    /// PID/epoch so the next `InitProducerId` starts from a clean slate.
    #[test]
    fn test_take_reinit_request_clears_identity_once() {
        let identity = ProducerIdentity::new();
        identity.initialize(7, 3);
        identity.allocate_sequence("topic", 0, 4).unwrap();
        identity.request_reinit();

        assert!(identity.take_reinit_request());
        assert!(!identity.is_initialized());
        assert_eq!(identity.peek_sequence("topic", 0), 0);

        // Second call is a no-op — the request was already consumed.
        assert!(!identity.take_reinit_request());
    }

    /// Requesting a re-init must never brick the producer — after the
    /// follow-up `initialize()` the identity is usable again.
    #[test]
    fn test_reinit_is_recoverable_not_permanent() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        identity.request_reinit();
        assert!(identity.take_reinit_request());

        identity.initialize(99, 0);
        assert!(identity.is_initialized());
        assert!(!identity.needs_reinit());
        assert_eq!(identity.allocate_sequence("topic", 0, 1).unwrap(), 0);
    }

    #[test]
    fn test_sequence_wrapping() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Set up state near max
        identity.set_sequence_state("topic", 0, i32::MAX, i32::MAX - 1);

        // Should wrap to 0 (matching Kafka Java client behavior)
        assert_eq!(identity.next_sequence("topic", 0).unwrap(), i32::MAX);
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    #[test]
    fn test_rollback_sequence_tail() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate sequence 0, then roll back — the batch owns the tail.
        assert_eq!(identity.next_sequence("topic", 0).unwrap(), 0);
        assert_eq!(identity.peek_sequence("topic", 0), 1);
        assert_eq!(
            identity.rollback_sequence("topic", 0, 0).unwrap(),
            RollbackOutcome::RolledBack
        );
        assert_eq!(identity.peek_sequence("topic", 0), 0);

        // Re-allocate gives the same sequence
        assert_eq!(identity.next_sequence("topic", 0).unwrap(), 0);
    }

    /// Rolling back a range that no longer owns the tail must be refused,
    /// otherwise two batches would be handed the same sequence numbers.
    #[test]
    fn test_rollback_sequence_range_refuses_non_tail() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        let first = identity.allocate_sequence("topic", 0, 3).unwrap();
        let second = identity.allocate_sequence("topic", 0, 2).unwrap();
        assert_eq!(first, 0);
        assert_eq!(second, 3);
        assert_eq!(identity.peek_sequence("topic", 0), 5);

        // The older batch does not own the tail — refuse and change nothing.
        assert_eq!(
            identity
                .rollback_sequence_range("topic", 0, first, 3)
                .unwrap(),
            RollbackOutcome::NotTail
        );
        assert_eq!(
            identity.peek_sequence("topic", 0),
            5,
            "a refused rollback must not modify the counter"
        );

        // The newest batch does own the tail.
        assert_eq!(
            identity
                .rollback_sequence_range("topic", 0, second, 2)
                .unwrap(),
            RollbackOutcome::RolledBack
        );
        assert_eq!(identity.peek_sequence("topic", 0), 3);
    }

    /// A partition with no recorded state has nothing to corrupt.
    #[test]
    fn test_rollback_sequence_range_unknown_partition_is_noop() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        assert_eq!(
            identity.rollback_sequence_range("topic", 7, 0, 4).unwrap(),
            RollbackOutcome::RolledBack
        );
    }

    #[test]
    fn test_rollback_sequence_range_rejects_non_positive_count() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        assert!(identity.rollback_sequence_range("topic", 0, 0, 0).is_err());
    }

    #[test]
    fn test_rollback_sequence_wraps_from_zero() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Set up state at 0 (just wrapped): the batch based at i32::MAX owns
        // the tail because i32::MAX + 1 wraps to 0.
        identity.set_sequence_state("topic", 0, 0, i32::MAX - 1);

        assert_eq!(
            identity.rollback_sequence("topic", 0, i32::MAX).unwrap(),
            RollbackOutcome::RolledBack
        );
        assert_eq!(identity.peek_sequence("topic", 0), i32::MAX);
    }

    // ── OUT_OF_ORDER_SEQUENCE_NUMBER reset safety ─────────────────

    /// A head-of-line batch (base == last_acked + 1, nothing newer allocated)
    /// may be locally rewound and resent.
    #[test]
    fn test_can_reset_after_out_of_order_head_of_line() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        let base = identity.allocate_sequence("topic", 0, 3).unwrap();
        assert_eq!(base, 0);

        assert!(
            identity
                .can_reset_after_out_of_order("topic", 0, base, 3)
                .unwrap()
        );
    }

    /// When an earlier batch is still unaccounted for, the failing batch is
    /// *not* head-of-line: rewinding would write it into the hole left by the
    /// lost batch and silently drop data.
    #[test]
    fn test_cannot_reset_after_out_of_order_when_earlier_batch_missing() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Batch A = [0,3), batch B = [3,5). Nothing acked yet, so B is not
        // head-of-line — A may have been lost by the broker.
        identity.allocate_sequence("topic", 0, 3).unwrap();
        let b = identity.allocate_sequence("topic", 0, 2).unwrap();

        assert!(
            !identity
                .can_reset_after_out_of_order("topic", 0, b, 2)
                .unwrap()
        );
    }

    /// Even a head-of-line batch must not rewind while a newer range has
    /// already been allocated for the partition.
    #[test]
    fn test_cannot_reset_after_out_of_order_when_newer_allocation_exists() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        let a = identity.allocate_sequence("topic", 0, 3).unwrap();
        identity.allocate_sequence("topic", 0, 2).unwrap();

        assert!(
            !identity
                .can_reset_after_out_of_order("topic", 0, a, 3)
                .unwrap()
        );
    }

    #[test]
    fn test_cannot_reset_after_out_of_order_unknown_partition() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        assert!(
            !identity
                .can_reset_after_out_of_order("topic", 3, 0, 1)
                .unwrap()
        );
    }

    #[test]
    fn test_reset_partition_sequences_only_affects_that_partition() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);
        identity.allocate_sequence("topic", 0, 4).unwrap();
        identity.allocate_sequence("topic", 1, 2).unwrap();

        identity.reset_partition_sequences("topic", 0);

        assert_eq!(identity.peek_sequence("topic", 0), 0);
        assert_eq!(identity.last_acked_sequence("topic", 0), -1);
        assert_eq!(identity.peek_sequence("topic", 1), 2);
    }

    #[test]
    fn test_reset_all_sequences_keeps_pid_and_epoch() {
        let identity = ProducerIdentity::new();
        identity.initialize(42, 9);
        identity.allocate_sequence("topic", 0, 4).unwrap();

        identity.reset_all_sequences();

        assert_eq!(identity.producer_id(), 42);
        assert_eq!(identity.producer_epoch(), 9);
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    #[test]
    fn test_last_sequence_of_batch_single_record() {
        assert_eq!(last_sequence_of_batch(0, 1).unwrap(), 0);
        assert_eq!(last_sequence_of_batch(5, 1).unwrap(), 5);
        assert_eq!(last_sequence_of_batch(i32::MAX, 1).unwrap(), i32::MAX);
    }

    #[test]
    fn test_last_sequence_of_batch_multi_record() {
        assert_eq!(last_sequence_of_batch(0, 5).unwrap(), 4);
        assert_eq!(last_sequence_of_batch(10, 3).unwrap(), 12);
        assert_eq!(last_sequence_of_batch(100, 100).unwrap(), 199);
    }

    #[test]
    fn test_last_sequence_of_batch_wrapping() {
        // Near max: base = i32::MAX - 2, count = 5
        // Last = i32::MAX - 2 + 4 = i32::MAX + 2 → wraps to 1
        assert_eq!(last_sequence_of_batch(i32::MAX - 2, 5).unwrap(), 1);

        // At boundary: base = i32::MAX, count = 2
        // Last = i32::MAX + 1 → wraps to 0
        assert_eq!(last_sequence_of_batch(i32::MAX, 2).unwrap(), 0);
    }

    #[test]
    fn test_multi_record_batch_ack_then_reset() {
        // Verify that acknowledging the last sequence of a multi-record batch
        // makes reset_sequence compute the correct next value.
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate a 5-record batch: base=0, sequences 0..4
        let base = identity.allocate_sequence("topic", 0, 5).unwrap();
        assert_eq!(base, 0);
        assert_eq!(identity.peek_sequence("topic", 0), 5);

        // Acknowledge the last sequence (4), not the base (0)
        let last_seq = last_sequence_of_batch(base, 5).unwrap();
        assert_eq!(last_seq, 4);
        identity.acknowledge("topic", 0, last_seq);
        assert_eq!(identity.last_acked_sequence("topic", 0), 4);

        // Reset should compute next = last_acked + 1 = 5
        identity.reset_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), 5);
    }

    #[test]
    fn test_reset_and_allocate_atomic() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate sequences 0..2, acknowledge up to 1
        identity.allocate_sequence("topic", 0, 3).unwrap();
        identity.acknowledge("topic", 0, 1);

        // reset_and_allocate should atomically:
        // 1. Reset next_sequence = last_acked + 1 = 2
        // 2. Allocate 5 sequences: base=2, next=7
        let base = identity.reset_and_allocate("topic", 0, 5).unwrap();
        assert_eq!(base, 2);
        assert_eq!(identity.peek_sequence("topic", 0), 7);
    }

    #[test]
    fn test_reset_and_allocate_no_prior_ack() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate without any ack — last_acked = -1
        identity.allocate_sequence("topic", 0, 3).unwrap();

        // reset_and_allocate: next_sequence_after(-1) = 0, allocate 2 → base=0, next=2
        let base = identity.reset_and_allocate("topic", 0, 2).unwrap();
        assert_eq!(base, 0);
        assert_eq!(identity.peek_sequence("topic", 0), 2);
    }

    #[test]
    fn test_reset_and_allocate_fresh_partition() {
        // Called on a partition that has never been seen
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        let base = identity.reset_and_allocate("topic", 99, 3).unwrap();
        assert_eq!(base, 0);
        assert_eq!(identity.peek_sequence("topic", 99), 3);
    }

    #[test]
    fn test_restore_from_snapshot_replaces_state() {
        let identity = ProducerIdentity::new();
        identity.initialize(100, 2);
        identity.next_sequence("topic1", 0).unwrap();
        identity.next_sequence("topic1", 0).unwrap();

        // Build a snapshot with different sequences
        let snapshot = ProducerIdentitySnapshot {
            producer_id: 100,
            producer_epoch: 2,
            partition_sequences: vec![
                PartitionSequenceSnapshot {
                    topic: "topic1".to_string(),
                    partition: 0,
                    next_sequence: 6,
                    last_acked_sequence: 5,
                },
                PartitionSequenceSnapshot {
                    topic: "topic2".to_string(),
                    partition: 1,
                    next_sequence: 10,
                    last_acked_sequence: 9,
                },
            ],
        };

        identity.restore_from_snapshot(&snapshot);

        assert_eq!(identity.peek_sequence("topic1", 0), 6);
        assert_eq!(identity.peek_sequence("topic2", 1), 10);
    }

    #[test]
    fn test_restore_from_snapshot_clears_reinit_request() {
        let identity = ProducerIdentity::new();
        identity.initialize(50, 0);
        identity.request_reinit();
        assert!(identity.needs_reinit());

        let snapshot = ProducerIdentitySnapshot {
            producer_id: 50,
            producer_epoch: 0,
            partition_sequences: vec![],
        };
        identity.restore_from_snapshot(&snapshot);
        assert!(!identity.needs_reinit());
    }
}
