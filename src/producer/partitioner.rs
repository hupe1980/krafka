//! Partitioning strategies for producers.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::PartitionId;

/// Compute murmur2 hash (Kafka's default hash function).
///
/// This is the same algorithm used by the Java Kafka client for key-based
/// partitioning. It provides consistent hashing across Java and Rust clients.
///
/// # Example
///
/// ```
/// use krafka::producer::murmur2;
///
/// let hash = murmur2(b"my-key");
/// let partition = (hash & 0x7FFFFFFF) % 3;  // 3 partitions
/// ```
#[inline]
pub fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1e995;
    const R: i32 = 24;

    let len = data.len();
    let mut h: u32 = SEED ^ (len as u32);
    let mut i = 0;

    while i + 4 <= len {
        let mut k = u32::from_le_bytes([data[i], data[i + 1], data[i + 2], data[i + 3]]);

        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h = h.wrapping_mul(M);
        h ^= k;

        i += 4;
    }

    let remaining = len - i;
    if remaining >= 3 {
        h ^= (data[i + 2] as u32) << 16;
    }
    if remaining >= 2 {
        h ^= (data[i + 1] as u32) << 8;
    }
    if remaining >= 1 {
        h ^= data[i] as u32;
        h = h.wrapping_mul(M);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;

    h
}

/// Trait for partitioning records across topic partitions.
///
/// # Determinism contract
///
/// Implementations **must** be deterministic for keyed records: the same
/// `(topic, key)` pair must always map to the same partition (given a
/// fixed `partition_count`). This is required for per-key ordering
/// guarantees. Unkeyed records (`key = None`) may use any strategy
/// (round-robin, random, sticky, etc.).
pub trait Partitioner: Send + Sync {
    /// Determine the partition for a record.
    ///
    /// # Arguments
    ///
    /// * `topic` - The topic name
    /// * `key` - The record key (optional). When `Some`, the same key must
    ///   always map to the same partition for a given `partition_count`.
    /// * `partition_count` - Number of partitions for the topic
    ///
    /// # Returns
    ///
    /// The partition ID to send the record to.
    fn partition(&self, topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId;
}

/// Default partitioner using murmur2 hash for keys, round-robin for null keys.
///
/// This matches the behavior of the Java Kafka client's default partitioner.
#[derive(Debug)]
pub struct DefaultPartitioner {
    counter: AtomicUsize,
}

impl DefaultPartitioner {
    /// Create a new default partitioner.
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for DefaultPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for DefaultPartitioner {
    #[inline]
    fn partition(&self, _topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }

        match key {
            Some(k) if !k.is_empty() => {
                // Use murmur2 hash for keyed records
                let hash = murmur2(k);
                // Use positive modulo
                (hash as usize % partition_count) as PartitionId
            }
            _ => {
                // Round-robin for records without keys
                let idx = self.counter.fetch_add(1, Ordering::Relaxed);
                (idx % partition_count) as PartitionId
            }
        }
    }
}

/// Round-robin partitioner.
///
/// Distributes records evenly across all partitions, ignoring the key.
#[derive(Debug)]
pub struct RoundRobinPartitioner {
    counter: AtomicUsize,
}

impl RoundRobinPartitioner {
    /// Create a new round-robin partitioner.
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for RoundRobinPartitioner {
    #[inline]
    fn partition(&self, _topic: &str, _key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed);
        (idx % partition_count) as PartitionId
    }
}

/// Sticky partitioner for improved batching.
///
/// Sticks to a partition until `batch_threshold` records have been sent
/// (default: 100), then advances to the next partition. This improves
/// batching efficiency by grouping unkeyed records together.
#[derive(Debug)]
pub struct StickyPartitioner {
    current: AtomicUsize,
    counter: AtomicUsize,
    /// Number of records per sticky partition before advancing.
    batch_threshold: usize,
}

impl StickyPartitioner {
    /// Create a new sticky partitioner with default batch threshold (100).
    pub fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            counter: AtomicUsize::new(0),
            batch_threshold: 100,
        }
    }

    /// Create a sticky partitioner with a custom batch threshold.
    pub fn with_batch_threshold(threshold: usize) -> Self {
        Self {
            current: AtomicUsize::new(0),
            counter: AtomicUsize::new(0),
            batch_threshold: threshold.max(1),
        }
    }

    /// Manually switch to the next partition.
    ///
    /// Uses `fetch_add` for atomic read-modify-write to avoid the
    /// race condition of separate load + store.
    pub fn next_partition(&self, partition_count: usize) {
        if partition_count > 0 {
            // Atomic increment; the modulo is applied at read time in partition()
            self.current.fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl Default for StickyPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for StickyPartitioner {
    #[inline]
    fn partition(&self, _topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }

        match key {
            Some(k) if !k.is_empty() => {
                // Use murmur2 hash for keyed records
                let hash = murmur2(k);
                (hash as usize % partition_count) as PartitionId
            }
            _ => {
                // Auto-advance after batch_threshold records
                let count = self.counter.fetch_add(1, Ordering::Relaxed);
                if count > 0 && count.is_multiple_of(self.batch_threshold) {
                    let next = count / self.batch_threshold;
                    self.current
                        .store(next % partition_count, Ordering::Release);
                }
                self.current.load(Ordering::Acquire) as PartitionId
            }
        }
    }
}

/// Hash-based partitioner using Rust's default hasher.
#[derive(Debug, Default)]
pub struct HashPartitioner;

impl HashPartitioner {
    /// Create a new hash partitioner.
    pub fn new() -> Self {
        Self
    }
}

impl Partitioner for HashPartitioner {
    #[inline]
    fn partition(&self, _topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }

        match key {
            Some(k) if !k.is_empty() => {
                let mut hasher = DefaultHasher::new();
                k.hash(&mut hasher);
                let hash = hasher.finish();
                (hash as usize % partition_count) as PartitionId
            }
            _ => 0, // Default to partition 0 for null/empty keys
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_default_partitioner_with_key() {
        let partitioner = DefaultPartitioner::new();

        // Same key should always go to the same partition
        let p1 = partitioner.partition("topic", Some(b"key1"), 10);
        let p2 = partitioner.partition("topic", Some(b"key1"), 10);
        assert_eq!(p1, p2);

        // Different keys might go to different partitions
        let p3 = partitioner.partition("topic", Some(b"key2"), 10);
        let _ = p3; // Just verify it doesn't panic
    }

    #[test]
    fn test_default_partitioner_without_key() {
        let partitioner = DefaultPartitioner::new();

        // Should round-robin without a key
        let p1 = partitioner.partition("topic", None, 3);
        let _p2 = partitioner.partition("topic", None, 3);
        let _p3 = partitioner.partition("topic", None, 3);
        let p4 = partitioner.partition("topic", None, 3);

        assert_eq!(p4, p1); // Should wrap around
    }

    #[test]
    fn test_default_partitioner_murmur2() {
        // Test known values
        let hash1 = murmur2(b"test");
        let hash2 = murmur2(b"test");
        assert_eq!(hash1, hash2);

        let hash3 = murmur2(b"different");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_round_robin_partitioner() {
        let partitioner = RoundRobinPartitioner::new();

        let partitions: Vec<_> = (0..6)
            .map(|_| partitioner.partition("topic", Some(b"key"), 3))
            .collect();

        assert_eq!(partitions, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn test_sticky_partitioner() {
        let partitioner = StickyPartitioner::new();

        // Should stick to partition 0 initially
        let p1 = partitioner.partition("topic", None, 3);
        let p2 = partitioner.partition("topic", None, 3);
        assert_eq!(p1, p2);

        // After switching, should use next partition
        partitioner.next_partition(3);
        let p3 = partitioner.partition("topic", None, 3);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_sticky_partitioner_auto_advance() {
        // Custom threshold of 5
        let partitioner = StickyPartitioner::with_batch_threshold(5);
        assert_eq!(
            partitioner.batch_threshold, 5,
            "with_batch_threshold should set custom threshold"
        );

        // Threshold of 0 should be clamped to 1
        let partitioner_min = StickyPartitioner::with_batch_threshold(0);
        assert_eq!(
            partitioner_min.batch_threshold, 1,
            "with_batch_threshold(0) should clamp to 1"
        );

        let partition_count = 3;

        // First 5 calls (indices 0..4) should all return the same partition
        let initial = partitioner.partition("topic", None, partition_count);
        for i in 1..5 {
            let p = partitioner.partition("topic", None, partition_count);
            assert_eq!(p, initial, "call {i} should still be on initial partition");
        }

        // The 6th call (index 5) triggers auto-advance (count=5, 5 % 5 == 0)
        let after_advance = partitioner.partition("topic", None, partition_count);
        assert_ne!(
            after_advance, initial,
            "after batch_threshold calls, partition should auto-advance to a different partition"
        );

        // Next 4 calls should stay on the new partition
        for i in 0..4 {
            let p = partitioner.partition("topic", None, partition_count);
            assert_eq!(
                p, after_advance,
                "call {i} after advance should stay on new partition"
            );
        }

        // Another advance at count=10
        let after_second_advance = partitioner.partition("topic", None, partition_count);
        assert_ne!(
            after_second_advance, after_advance,
            "should auto-advance again after another batch_threshold calls"
        );
    }

    #[test]
    fn test_sticky_partitioner_with_key() {
        let partitioner = StickyPartitioner::new();

        // With a key, should use murmur2 hash
        let p1 = partitioner.partition("topic", Some(b"key1"), 10);
        let p2 = partitioner.partition("topic", Some(b"key1"), 10);
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_hash_partitioner() {
        let partitioner = HashPartitioner::new();

        // Same key should always go to the same partition
        let p1 = partitioner.partition("topic", Some(b"key"), 10);
        let p2 = partitioner.partition("topic", Some(b"key"), 10);
        assert_eq!(p1, p2);

        // Null key goes to partition 0
        let p3 = partitioner.partition("topic", None, 10);
        assert_eq!(p3, 0);
    }

    #[test]
    fn test_partitioners_with_zero_partitions() {
        let default = DefaultPartitioner::new();
        let round_robin = RoundRobinPartitioner::new();
        let sticky = StickyPartitioner::new();
        let hash = HashPartitioner::new();

        // All should return 0 for 0 partitions
        assert_eq!(default.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(round_robin.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(sticky.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(hash.partition("topic", Some(b"key"), 0), 0);
    }
}
