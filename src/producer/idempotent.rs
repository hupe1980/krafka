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
//! ```ignore
//! use krafka::producer::{Producer, ProducerConfig};
//!
//! let producer = Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .idempotence(true)  // Enable idempotent producer
//!     .build()
//!     .await?;
//! ```

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};

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
    /// Sequence numbers per topic-partition.
    sequences: RwLock<HashMap<(String, PartitionId), SequenceState>>,
}

/// Sequence number state for a partition.
#[derive(Debug, Clone)]
struct SequenceState {
    /// The next sequence number to use.
    next_sequence: i32,
    /// The last successfully acknowledged sequence number.
    last_acked_sequence: i32,
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
    pub fn initialize(&self, producer_id: i64, producer_epoch: i16) {
        self.producer_id.store(producer_id, Ordering::SeqCst);
        self.producer_epoch
            .store(producer_epoch as i32, Ordering::SeqCst);

        // Clear all sequence numbers on initialization
        if let Ok(mut sequences) = self.sequences.write() {
            sequences.clear();
        }
    }

    /// Reset the identity (e.g., after a fatal error).
    pub fn reset(&self) {
        self.producer_id.store(-1, Ordering::SeqCst);
        self.producer_epoch.store(-1, Ordering::SeqCst);

        if let Ok(mut sequences) = self.sequences.write() {
            sequences.clear();
        }
    }

    /// Get the next sequence number for a topic-partition.
    ///
    /// This allocates a new sequence number for the next batch.
    /// Sequence numbers wrap to 0 at `i32::MAX`, matching the Kafka Java client
    /// behavior (`DefaultRecordBatch.incrementSequence()`).
    pub fn next_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let key = (topic.to_string(), partition);

        if let Ok(mut sequences) = self.sequences.write() {
            let state = sequences.entry(key).or_default();
            let seq = state.next_sequence;
            state.next_sequence = if seq == i32::MAX { 0 } else { seq + 1 };
            seq
        } else {
            0
        }
    }

    /// Peek at the next sequence number without incrementing.
    #[inline]
    pub fn peek_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let key = (topic.to_string(), partition);

        if let Ok(sequences) = self.sequences.read() {
            sequences.get(&key).map(|s| s.next_sequence).unwrap_or(0)
        } else {
            0
        }
    }

    /// Acknowledge a sequence number for a partition.
    ///
    /// Call this when a batch is successfully acknowledged by the broker.
    pub fn acknowledge(&self, topic: &str, partition: PartitionId, sequence: i32) {
        let key = (topic.to_string(), partition);

        if let Ok(mut sequences) = self.sequences.write()
            && let Some(state) = sequences.get_mut(&key)
            && sequence > state.last_acked_sequence
        {
            state.last_acked_sequence = sequence;
        }
    }

    /// Reset sequence number for a partition (e.g., after an out-of-order error).
    pub fn reset_sequence(&self, topic: &str, partition: PartitionId) {
        let key = (topic.to_string(), partition);

        if let Ok(mut sequences) = self.sequences.write()
            && let Some(state) = sequences.get_mut(&key)
        {
            // Reset to the last acknowledged + 1
            state.next_sequence = state.last_acked_sequence.wrapping_add(1);
        }
    }

    /// Get the last acknowledged sequence for a partition.
    #[inline]
    pub fn last_acked_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        let key = (topic.to_string(), partition);

        if let Ok(sequences) = self.sequences.read() {
            sequences
                .get(&key)
                .map(|s| s.last_acked_sequence)
                .unwrap_or(-1)
        } else {
            -1
        }
    }

    /// Create a snapshot of the current idempotent state.
    pub fn snapshot(&self) -> ProducerIdentitySnapshot {
        let partition_sequences = if let Ok(sequences) = self.sequences.read() {
            sequences
                .iter()
                .map(|((topic, part), state)| PartitionSequenceSnapshot {
                    topic: topic.clone(),
                    partition: *part,
                    next_sequence: state.next_sequence,
                    last_acked_sequence: state.last_acked_sequence,
                })
                .collect()
        } else {
            Vec::new()
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
            let mut sequences = identity.sequences.write().unwrap();
            sequences.insert(
                ("topic".to_string(), 0),
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
}
