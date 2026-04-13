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

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};

use parking_lot::RwLock;

use crate::PartitionId;

/// Producer identity for idempotent production.
///
/// This struct holds the producer ID and epoch assigned by the broker,
/// along with sequence numbers for each partition.
#[derive(Debug)]
pub struct ProducerIdentity {
    /// Producer ID assigned by the broker (-1 if not initialized).
    producer_id: AtomicI64,
    /// Producer epoch assigned by the broker (-1 if not initialized).
    producer_epoch: AtomicI32,
    /// Sequence numbers per topic-partition (two-level map avoids
    /// per-call `String` allocations on lookups).
    sequences: RwLock<HashMap<String, HashMap<PartitionId, SequenceState>>>,
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
/// # Panics
///
/// Panics if `count <= 0`.
pub(crate) fn last_sequence_of_batch(base_sequence: i32, count: i32) -> i32 {
    assert!(count > 0, "count must be positive");
    ((base_sequence as u32).wrapping_add((count - 1) as u32) % SEQUENCE_SPACE) as i32
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
            producer_id: AtomicI64::new(-1),
            producer_epoch: AtomicI32::new(-1),
            sequences: RwLock::new(HashMap::new()),
        }
    }

    /// Check if the producer identity has been initialized.
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.producer_id.load(Ordering::SeqCst) >= 0
    }

    /// Get the producer ID.
    #[inline]
    pub fn producer_id(&self) -> i64 {
        self.producer_id.load(Ordering::SeqCst)
    }

    /// Get the producer epoch.
    #[inline]
    pub fn producer_epoch(&self) -> i16 {
        self.producer_epoch.load(Ordering::SeqCst) as i16
    }

    /// Initialize with the producer ID and epoch from the broker.
    ///
    /// This should be called once with the response from InitProducerId.
    /// The sequences lock is acquired first so concurrent readers
    /// cannot observe a half-updated identity.
    pub fn initialize(&self, producer_id: i64, producer_epoch: i16) {
        let mut sequences = self.sequences.write();
        self.producer_id.store(producer_id, Ordering::SeqCst);
        self.producer_epoch
            .store(producer_epoch as i32, Ordering::SeqCst);
        sequences.clear();
    }

    /// Reset the identity (e.g., after a fatal error).
    ///
    /// The sequences lock is acquired first so concurrent readers
    /// cannot observe a half-updated identity.
    pub fn reset(&self) {
        let mut sequences = self.sequences.write();
        self.producer_id.store(-1, Ordering::SeqCst);
        self.producer_epoch.store(-1, Ordering::SeqCst);
        sequences.clear();
    }

    /// Get the next sequence number for a topic-partition (single-record batch).
    ///
    /// This allocates a new sequence number for the next batch.
    /// Sequence numbers wrap to 0 at `i32::MAX`, matching the Kafka Java client
    /// behavior (`DefaultRecordBatch.incrementSequence()`).
    ///
    /// For multi-record batches, use [`allocate_sequence`](Self::allocate_sequence).
    pub fn next_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        self.allocate_sequence(topic, partition, 1)
    }

    /// Allocate a contiguous range of `count` sequence numbers for a batch.
    ///
    /// Returns the base sequence. The internal counter advances by `count`,
    /// wrapping at the sequence space boundary (`i32::MAX` → 0).
    ///
    /// # Panics
    ///
    /// Panics if `count <= 0`.
    pub fn allocate_sequence(&self, topic: &str, partition: PartitionId, count: i32) -> i32 {
        assert!(count > 0, "count must be positive");

        let mut sequences = self.sequences.write();
        let state = sequences
            .entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_default();
        let base = state.next_sequence;
        state.next_sequence = ((base as u32).wrapping_add(count as u32) % SEQUENCE_SPACE) as i32;
        base
    }

    /// Peek at the next sequence number without incrementing.
    #[inline]
    pub fn peek_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let sequences = self.sequences.read();
        sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
            .map(|s| s.next_sequence)
            .unwrap_or(0)
    }

    /// Acknowledge a sequence number for a partition.
    ///
    /// Call this when a batch is successfully acknowledged by the broker.
    pub fn acknowledge(&self, topic: &str, partition: PartitionId, sequence: i32) {
        let mut sequences = self.sequences.write();
        if let Some(state) = sequences
            .get_mut(topic)
            .and_then(|parts| parts.get_mut(&partition))
            && is_newer_sequence(state.last_acked_sequence, sequence)
        {
            state.last_acked_sequence = sequence;
        }
    }

    /// Roll back the most recent sequence allocation for a partition.
    ///
    /// Call this when a sequence was allocated via [`Self::next_sequence`] but the
    /// request was never sent (e.g., encode failure). Decrements `next_sequence`
    /// by one, wrapping from 0 back to `i32::MAX`.
    pub fn rollback_sequence(&self, topic: &str, partition: PartitionId) {
        self.rollback_sequence_range(topic, partition, 1);
    }

    /// Roll back a range of `count` sequence numbers.
    ///
    /// Used when a multi-record batch was allocated a sequence range via
    /// [`allocate_sequence`](Self::allocate_sequence) but failed before being
    /// sent (e.g., encode failure), preventing sequence gaps.
    ///
    /// # Panics
    ///
    /// Panics if `count <= 0`.
    pub fn rollback_sequence_range(&self, topic: &str, partition: PartitionId, count: i32) {
        assert!(count > 0, "count must be positive");

        let mut sequences = self.sequences.write();
        if let Some(state) = sequences
            .get_mut(topic)
            .and_then(|parts| parts.get_mut(&partition))
        {
            let current = state.next_sequence as u32;
            state.next_sequence =
                ((current + SEQUENCE_SPACE - count as u32) % SEQUENCE_SPACE) as i32;
        }
    }

    /// Reset sequence number for a partition (e.g., after an out-of-order error).
    pub fn reset_sequence(&self, topic: &str, partition: PartitionId) {
        let mut sequences = self.sequences.write();
        if let Some(state) = sequences
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
    /// # Panics
    ///
    /// Panics if `count <= 0`.
    pub fn reset_and_allocate(&self, topic: &str, partition: PartitionId, count: i32) -> i32 {
        assert!(count > 0, "count must be positive");

        let mut sequences = self.sequences.write();
        let state = sequences
            .entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_default();
        state.next_sequence = next_sequence_after(state.last_acked_sequence);
        let base = state.next_sequence;
        state.next_sequence = ((base as u32).wrapping_add(count as u32) % SEQUENCE_SPACE) as i32;
        base
    }

    /// Get the last acknowledged sequence for a partition.
    #[inline]
    pub fn last_acked_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let sequences = self.sequences.read();
        sequences
            .get(topic)
            .and_then(|parts| parts.get(&partition))
            .map(|s| s.last_acked_sequence)
            .unwrap_or(-1)
    }

    /// Create a snapshot of the current idempotent state.
    pub fn snapshot(&self) -> ProducerIdentitySnapshot {
        let partition_sequences = {
            let sequences = self.sequences.read();
            sequences
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
                .collect()
        };

        ProducerIdentitySnapshot {
            producer_id: self.producer_id(),
            producer_epoch: self.producer_epoch(),
            partition_sequences,
        }
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

#[cfg(test)]
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
        assert_eq!(identity.next_sequence("topic", 0), 0);
        assert_eq!(identity.next_sequence("topic", 0), 1);
        assert_eq!(identity.next_sequence("topic", 0), 2);

        // Different partition starts at 0
        assert_eq!(identity.next_sequence("topic", 1), 0);

        // Different topic starts at 0
        assert_eq!(identity.next_sequence("other-topic", 0), 0);
    }

    #[test]
    fn test_peek_sequence() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Peek should not increment
        assert_eq!(identity.peek_sequence("topic", 0), 0);
        assert_eq!(identity.peek_sequence("topic", 0), 0);

        // After getting next, peek should show new value
        identity.next_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), 1);
    }

    #[test]
    fn test_acknowledge() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Get some sequences
        identity.next_sequence("topic", 0);
        identity.next_sequence("topic", 0);
        identity.next_sequence("topic", 0);

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
        identity.next_sequence("topic", 0);
        identity.next_sequence("topic", 0);
        identity.next_sequence("topic", 0);

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

        {
            let mut sequences = identity.sequences.write();
            sequences.entry("topic".to_string()).or_default().insert(
                0,
                SequenceState {
                    next_sequence: 1,
                    last_acked_sequence: i32::MAX,
                },
            );
        }

        identity.acknowledge("topic", 0, 0);
        assert_eq!(identity.last_acked_sequence("topic", 0), 0);

        identity.acknowledge("topic", 0, i32::MAX);
        assert_eq!(identity.last_acked_sequence("topic", 0), 0);
    }

    #[test]
    fn test_reset_sequence_wraps_to_zero_after_max_ack() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        {
            let mut sequences = identity.sequences.write();
            sequences.entry("topic".to_string()).or_default().insert(
                0,
                SequenceState {
                    next_sequence: 1,
                    last_acked_sequence: i32::MAX,
                },
            );
        }

        identity.reset_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    #[test]
    fn test_reset_identity() {
        let identity = ProducerIdentity::new();
        identity.initialize(12345, 5);
        identity.next_sequence("topic", 0);

        identity.reset();

        assert!(!identity.is_initialized());
        assert_eq!(identity.producer_id(), -1);
        assert_eq!(identity.producer_epoch(), -1);
        // Sequences are cleared, so next should start at 0
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    #[test]
    fn test_snapshot() {
        let identity = ProducerIdentity::new();
        identity.initialize(100, 1);
        identity.next_sequence("topic1", 0);
        identity.next_sequence("topic1", 0);
        identity.acknowledge("topic1", 0, 0);
        identity.next_sequence("topic2", 0);

        let snapshot = identity.snapshot();
        assert_eq!(snapshot.producer_id, 100);
        assert_eq!(snapshot.producer_epoch, 1);
        assert_eq!(snapshot.partition_sequences.len(), 2);
    }

    #[test]
    fn test_sequence_wrapping() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Set up state near max
        {
            let mut sequences = identity.sequences.write();
            sequences.entry("topic".to_string()).or_default().insert(
                0,
                SequenceState {
                    next_sequence: i32::MAX,
                    last_acked_sequence: i32::MAX - 1,
                },
            );
        }

        // Should wrap to 0 (matching Kafka Java client behavior)
        assert_eq!(identity.next_sequence("topic", 0), i32::MAX);
        assert_eq!(identity.peek_sequence("topic", 0), 0);
    }

    #[test]
    fn test_rollback_sequence() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate sequence 0, then roll back
        assert_eq!(identity.next_sequence("topic", 0), 0);
        assert_eq!(identity.peek_sequence("topic", 0), 1);
        identity.rollback_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), 0);

        // Re-allocate gives the same sequence
        assert_eq!(identity.next_sequence("topic", 0), 0);
    }

    #[test]
    fn test_rollback_sequence_wraps_from_zero() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Set up state at 0 (just wrapped)
        {
            let mut sequences = identity.sequences.write();
            sequences.entry("topic".to_string()).or_default().insert(
                0,
                SequenceState {
                    next_sequence: 0,
                    last_acked_sequence: i32::MAX - 1,
                },
            );
        }

        // Rollback from 0 should wrap to i32::MAX
        identity.rollback_sequence("topic", 0);
        assert_eq!(identity.peek_sequence("topic", 0), i32::MAX);
    }

    #[test]
    fn test_last_sequence_of_batch_single_record() {
        assert_eq!(last_sequence_of_batch(0, 1), 0);
        assert_eq!(last_sequence_of_batch(5, 1), 5);
        assert_eq!(last_sequence_of_batch(i32::MAX, 1), i32::MAX);
    }

    #[test]
    fn test_last_sequence_of_batch_multi_record() {
        assert_eq!(last_sequence_of_batch(0, 5), 4);
        assert_eq!(last_sequence_of_batch(10, 3), 12);
        assert_eq!(last_sequence_of_batch(100, 100), 199);
    }

    #[test]
    fn test_last_sequence_of_batch_wrapping() {
        // Near max: base = i32::MAX - 2, count = 5
        // Last = i32::MAX - 2 + 4 = i32::MAX + 2 → wraps to 1
        assert_eq!(last_sequence_of_batch(i32::MAX - 2, 5), 1);

        // At boundary: base = i32::MAX, count = 2
        // Last = i32::MAX + 1 → wraps to 0
        assert_eq!(last_sequence_of_batch(i32::MAX, 2), 0);
    }

    #[test]
    fn test_multi_record_batch_ack_then_reset() {
        // Verify that acknowledging the last sequence of a multi-record batch
        // makes reset_sequence compute the correct next value.
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate a 5-record batch: base=0, sequences 0..4
        let base = identity.allocate_sequence("topic", 0, 5);
        assert_eq!(base, 0);
        assert_eq!(identity.peek_sequence("topic", 0), 5);

        // Acknowledge the last sequence (4), not the base (0)
        let last_seq = last_sequence_of_batch(base, 5);
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
        identity.allocate_sequence("topic", 0, 3);
        identity.acknowledge("topic", 0, 1);

        // reset_and_allocate should atomically:
        // 1. Reset next_sequence = last_acked + 1 = 2
        // 2. Allocate 5 sequences: base=2, next=7
        let base = identity.reset_and_allocate("topic", 0, 5);
        assert_eq!(base, 2);
        assert_eq!(identity.peek_sequence("topic", 0), 7);
    }

    #[test]
    fn test_reset_and_allocate_no_prior_ack() {
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        // Allocate without any ack — last_acked = -1
        identity.allocate_sequence("topic", 0, 3);

        // reset_and_allocate: next_sequence_after(-1) = 0, allocate 2 → base=0, next=2
        let base = identity.reset_and_allocate("topic", 0, 2);
        assert_eq!(base, 0);
        assert_eq!(identity.peek_sequence("topic", 0), 2);
    }

    #[test]
    fn test_reset_and_allocate_fresh_partition() {
        // Called on a partition that has never been seen
        let identity = ProducerIdentity::new();
        identity.initialize(1, 0);

        let base = identity.reset_and_allocate("topic", 99, 3);
        assert_eq!(base, 0);
        assert_eq!(identity.peek_sequence("topic", 99), 3);
    }
}
