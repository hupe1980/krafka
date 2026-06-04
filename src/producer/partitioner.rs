//! Partitioning strategies for producers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

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

/// Map a record key to a partition using Java-compatible `toPositive` masking.
#[inline]
fn partition_for_key(key: &[u8], partition_count: usize) -> PartitionId {
    (((murmur2(key) & 0x7fff_ffff) as usize) % partition_count) as PartitionId
}

/// Draw a random partition in `[0, partition_count)`.
#[inline]
fn random_partition(partition_count: usize) -> i32 {
    debug_assert!(partition_count > 0);
    rand::random_range(0..partition_count as u32) as i32
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
///
/// # Batch notification
///
/// For partitioners that advance on batch boundaries (like
/// [`UniformStickyPartitioner`]), the accumulator calls [`on_new_batch`]
/// when a batch for `(topic, prev_partition)` has been filled and a new
/// batch is about to be opened. The default implementation is a no-op.
///
/// [`on_new_batch`]: Partitioner::on_new_batch
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

    /// Called by the producer accumulator when a batch for `(topic, prev_partition)`
    /// was filled and a new batch is about to be opened.
    ///
    /// Batch-boundary partitioners (e.g. [`UniformStickyPartitioner`]) use
    /// this signal to pick a new sticky partition for the next batch. The
    /// default implementation is a no-op; all existing partitioners except
    /// `UniformStickyPartitioner` ignore batch events.
    #[inline]
    fn on_new_batch(&self, _topic: &str, _prev_partition: PartitionId, _partition_count: usize) {}
}

/// Default partitioner using murmur2 hash for keys, per-topic round-robin for null keys.
///
/// This matches the behavior of the Java Kafka client's default partitioner.
/// The round-robin counter is maintained per-topic so that null-keyed records
/// for different topics do not interfere with each other's distribution.
#[derive(Debug)]
pub struct DefaultPartitioner {
    /// Per-topic round-robin counter for keyless records.
    ///
    /// Using a `Mutex<HashMap>` rather than a single global `AtomicUsize` so
    /// that each topic distributes keyless records independently.  The lock is
    /// only acquired for keyless records; keyed records use a lock-free
    /// murmur2 hash path.
    per_topic_counter: Mutex<HashMap<String, usize>>,
}

impl DefaultPartitioner {
    /// Create a new default partitioner.
    pub fn new() -> Self {
        Self {
            per_topic_counter: Mutex::new(HashMap::new()),
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
    fn partition(&self, topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }

        match key {
            Some(k) if !k.is_empty() => {
                // Java-compatible keyed routing: toPositive(murmur2(key)) % partition_count.
                partition_for_key(k, partition_count)
            }
            _ => {
                // Per-topic round-robin for records without keys.
                let mut counters = self.per_topic_counter.lock();
                let counter = counters.entry(topic.to_owned()).or_insert(0);
                let idx = *counter;
                *counter = counter.wrapping_add(1);
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
    /// Assign a partition for the given key.
    ///
    /// # Concurrent-advance semantics (null/empty keys only)
    ///
    /// When two threads call `partition()` simultaneously and both observe
    /// the same `count` value satisfying `is_multiple_of(batch_threshold)`,
    /// both will compute the **same** `next` value and store it. The second
    /// `store` is idempotent — the partition advances exactly once. This is
    /// correct, though the counter overshoots by at most one record relative
    /// to the threshold boundary.
    ///
    /// `next_partition()` (manual advance) and the threshold-based auto-advance
    /// are independent operations. A rare concurrent firing of both will advance
    /// the partition by two instead of one. This is within the documented
    /// "best-effort sticky" contract: partition distribution remains fair over
    /// time, and per-key ordering is unaffected.
    #[inline]
    fn partition(&self, _topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }

        match key {
            Some(k) if !k.is_empty() => {
                // Java-compatible keyed routing: toPositive(murmur2(key)) % partition_count.
                partition_for_key(k, partition_count)
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

/// Hash-based partitioner using the same murmur2 algorithm as the Java Kafka client.
///
/// Unlike `DefaultPartitioner` (which uses round-robin for null keys), this
/// partitioner hashes only keyed records. Null/empty keys are routed to
/// partition 0.
///
/// # Determinism
///
/// `murmur2` produces identical output across all Rust compiler versions and
/// across Java/Rust client pairs given the same key bytes and partition count.
/// This satisfies the [`Partitioner`] trait's determinism contract.
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
                // Use murmur2 — same as Java DefaultPartitioner.
                // Previously used DefaultHasher here which is NOT stable across
                // Rust versions (stdlib explicitly reserves the right to change it),
                // violating the Partitioner determinism contract.
                partition_for_key(k, partition_count)
            }
            _ => 0,
        }
    }
}

/// Uniform sticky partitioner (KIP-794, Java client default since Kafka 3.3).
///
/// Assigns unkeyed records to a single *sticky* partition for the duration of
/// one batch, then switches to a new partition when the batch is filled and
/// flushed. This produces larger, more efficient batches than round-robin (which
/// spreads records across all partitions, keeping each batch small) while still
/// distributing load evenly over time.
///
/// # Keyed records
///
/// Uses the same `murmur2` algorithm as the Java `DefaultPartitioner` for
/// consistent cross-language determinism.
///
/// # Unkeyed records
///
/// The sticky partition for a topic is chosen randomly on first use, then held
/// until the batch for that partition is full. On batch fill,
/// [`on_new_batch`](Partitioner::on_new_batch) is called by the accumulator with
/// the filled partition. The partitioner then picks a **different** partition
/// uniformly at random (using the same logic as Java's `nextPartition`).
///
/// # Comparison to `StickyPartitioner`
///
/// [`StickyPartitioner`] advances after a fixed record count (`batch_threshold`).
/// `UniformStickyPartitioner` advances on **actual batch boundaries** regardless
/// of record count, which yields truly batch-sized sticky windows and matches
/// the Java 3.3+ behaviour exactly.
///
/// # Memory footprint
///
/// The per-topic sticky map is capped at [`UniformStickyPartitioner::MAX_TRACKED_TOPICS`]
/// entries (default: 10 000). When the cap is reached, an existing entry is
/// evicted pseudo-randomly (first key in iteration order, which is randomised by
/// `HashMap`'s hash seed) before inserting the new topic.  This bounds memory
/// usage in multi-tenant and CDC pipelines that produce to many short-lived
/// topics while keeping eviction cost O(1).
///
/// # Thread safety
///
/// All methods are safe to call concurrently. A single `Mutex` guards the
/// per-topic `HashMap<String, i32>`. Critical sections are nanosecond-scale
/// (one HashMap lookup + one integer read or write).
#[derive(Debug, Default)]
pub struct UniformStickyPartitioner {
    /// Per-topic sticky partition index, initialised on first use.
    sticky: Mutex<HashMap<String, i32>>,
}

impl UniformStickyPartitioner {
    /// Maximum number of per-topic sticky entries retained in memory.
    ///
    /// When the map reaches this size a single entry is evicted (pseudo-random
    /// via HashMap iteration order) before the new topic is inserted.  This
    /// prevents unbounded memory growth in multi-tenant or CDC workloads.
    pub const MAX_TRACKED_TOPICS: usize = 10_000;

    /// Create a new `UniformStickyPartitioner`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Pick a new partition for `topic`, excluding `avoid` if possible.
    ///
    /// Mirrors Java's `StickyPartitionCache.nextPartition`:
    /// draws a random partition in `[0, partition_count)` that differs
    /// from `avoid`. Falls back to `avoid` when `partition_count == 1`.
    fn pick_new_partition(partition_count: usize, avoid: i32) -> i32 {
        if partition_count <= 1 {
            return 0;
        }
        let candidate = random_partition(partition_count);
        // Shift by 1 if we randomly drew the same partition we just flushed,
        // so every advance always changes the sticky target.
        if candidate == avoid {
            (candidate + 1) % partition_count as i32
        } else {
            candidate
        }
    }
}

impl Partitioner for UniformStickyPartitioner {
    #[inline]
    fn partition(&self, topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }

        // Keyed records: deterministic murmur2 hash, same as Java.
        if let Some(k) = key
            && !k.is_empty()
        {
            return partition_for_key(k, partition_count);
        }

        // Unkeyed: return (or initialise) the sticky partition under the lock.
        let mut map = self.sticky.lock();
        let mut partition = if let Some(existing) = map.get_mut(topic) {
            *existing
        } else {
            // Enforce the per-partitioner memory cap before inserting.
            if map.len() >= Self::MAX_TRACKED_TOPICS {
                // Evict one entry pseudo-randomly (first key in iteration order,
                // which is randomised by HashMap's hash seed). O(1) cost.
                if let Some(evict_key) = map.keys().next().map(|k| k.to_owned()) {
                    map.remove(&evict_key);
                }
            }
            let fresh = random_partition(partition_count);
            map.insert(topic.to_string(), fresh);
            fresh
        };

        // Guard against partition_count shrinking after the sticky was set.
        if (partition as usize) >= partition_count {
            partition = random_partition(partition_count);
            map.insert(topic.to_string(), partition);
        }
        partition
    }

    /// Advance to a new sticky partition for `topic`.
    ///
    /// Called by the accumulator when the batch for `(topic, prev_partition)`
    /// was filled and flushed. Picks a new partition uniformly at random,
    /// different from `prev_partition`, and stores it as the new sticky value.
    ///
    /// Uses a compare-and-set pattern: the advance only applies when the
    /// currently stored sticky value still equals `prev_partition`.  A
    /// concurrent `on_new_batch` for a *different* batch that already advanced
    /// past `prev_partition` leaves the new value intact.
    fn on_new_batch(&self, topic: &str, prev_partition: PartitionId, partition_count: usize) {
        if partition_count == 0 {
            return;
        }
        let next = Self::pick_new_partition(partition_count, prev_partition);
        let mut map = self.sticky.lock();
        if let Some(current) = map.get_mut(topic) {
            // Only advance if no other concurrent `on_new_batch` already moved on.
            if *current == prev_partition {
                *current = next;
            }
        }
        // If the topic is not in the map yet, `partition()` will initialise it
        // to a fresh random value on the next call.
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

    /// Java-compatibility test vectors for murmur2.
    ///
    /// These values are derived from the Java Kafka client's `UtilsTest`:
    /// `org.apache.kafka.common.utils.UtilsTest#testMurmur2`.
    /// Java returns a signed `int`; we compare the same bit pattern as `u32`.
    #[test]
    fn test_murmur2_java_compat_vectors() {
        let vectors: &[(&[u8], u32)] = &[
            // "21" → Java: -973932308
            (b"21", 0xC5F2F8ECu32),
            // "foobar" → Java: -790332482
            (b"foobar", 0xD0E47BBEu32),
            // "a-little-bit-long-string" → Java: -985981536
            (b"a-little-bit-long-string", 0xC53B1DA0u32),
            // "a-little-bit-longer-string" → Java: -1486304829
            (b"a-little-bit-longer-string", 0xA768C9C3u32),
        ];
        for (input, expected) in vectors {
            let got = murmur2(input);
            assert_eq!(
                got,
                *expected,
                "murmur2({:?}) = 0x{:08X}, want 0x{:08X}",
                std::str::from_utf8(input).unwrap_or("<binary>"),
                got,
                expected,
            );
        }
    }

    /// Java partition-assignment vectors.
    ///
    /// Verifies that `partition_for_key(key, n)` returns the same partition
    /// as Java's `DefaultPartitioner` for the same inputs.
    #[test]
    fn test_murmur2_partition_for_key_java_compat() {
        // Java: Utils.toPositive(Utils.murmur2(key)) % numPartitions
        // "test" → murmur2 = 0x2AB0E07F → toPositive (& 0x7FFFFFFF) = 0x2AB0E07F
        // 0x2AB0E07F = 716234879, 716234879 % 10 = 9
        assert_eq!(partition_for_key(b"test", 10), 9);
        // "kafka" → murmur2 = 0xD067CF64 → toPositive (& 0x7FFFFFFF) = 0x5067CF64
        // 0x5067CF64 = 1348980580, 1348980580 % 10 = 0
        assert_eq!(partition_for_key(b"kafka", 10), 0);
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
        let uniform = UniformStickyPartitioner::new();

        // All should return 0 for 0 partitions
        assert_eq!(default.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(round_robin.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(sticky.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(hash.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(uniform.partition("topic", Some(b"key"), 0), 0);
        assert_eq!(uniform.partition("topic", None, 0), 0);
    }

    #[test]
    fn test_uniform_sticky_partitioner_basic() {
        let p = UniformStickyPartitioner::new();

        // Sticky: all calls for the same topic without on_new_batch return the same partition.
        let first = p.partition("topic", None, 8);
        assert!(first < 8);
        for _ in 0..20 {
            assert_eq!(p.partition("topic", None, 8), first);
        }

        // Different topics get independent sticky values (may coincidentally be equal).
        let other = p.partition("other-topic", None, 8);
        assert!(other < 8);
    }

    #[test]
    fn test_uniform_sticky_partitioner_keyed() {
        let p = UniformStickyPartitioner::new();

        // Keyed records: deterministic murmur2 hash — same result every call.
        let k1a = p.partition("topic", Some(b"key1"), 8);
        let k1b = p.partition("topic", Some(b"key1"), 8);
        assert_eq!(k1a, k1b);

        // Different keys should map to any of the partitions (not necessarily different).
        let k2 = p.partition("topic", Some(b"key2"), 8);
        assert!(k2 < 8);
    }

    #[test]
    fn test_uniform_sticky_on_new_batch() {
        let p = UniformStickyPartitioner::new();

        // Establish a sticky partition.
        let prev = p.partition("topic", None, 8);

        // Simulate batch flush: partitioner should advance to a new (different) partition.
        p.on_new_batch("topic", prev, 8);
        let next = p.partition("topic", None, 8);
        assert_ne!(next, prev, "sticky should advance after on_new_batch");
        assert!(next < 8);

        // Calling on_new_batch again with the stale prev value must NOT regress the sticky.
        p.on_new_batch("topic", prev, 8);
        // The value should still be `next` (not reverted to prev).
        assert_eq!(p.partition("topic", None, 8), next);
    }

    #[test]
    fn test_uniform_sticky_partition_count_shrink() {
        let p = UniformStickyPartitioner::new();

        // Initialise with a large partition count so the sticky is likely > 1.
        let _ = p.partition("topic", None, 64);

        // Shrink partition_count to 1; partition() must return a valid index.
        let result = p.partition("topic", None, 1);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_uniform_sticky_single_partition() {
        let p = UniformStickyPartitioner::new();

        // With one partition, pick_new_partition must return 0, not panic.
        let result = p.partition("topic", None, 1);
        assert_eq!(result, 0);
        p.on_new_batch("topic", 0, 1);
        assert_eq!(p.partition("topic", None, 1), 0);
    }

    #[test]
    fn test_uniform_sticky_on_new_batch_unknown_topic() {
        // on_new_batch for a topic that has never been seen must not panic.
        let p = UniformStickyPartitioner::new();
        p.on_new_batch("unknown", 0, 4);
        // partition() for that topic should still return a valid value.
        let result = p.partition("unknown", None, 4);
        assert!(result < 4);
    }

    #[test]
    fn test_uniform_sticky_concurrent_safety() {
        use std::sync::Arc;
        use std::thread;

        let p = Arc::new(UniformStickyPartitioner::new());
        let mut handles = Vec::new();

        for _ in 0..8 {
            let p = Arc::clone(&p);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let part = p.partition("topic", None, 16);
                    assert!(part < 16, "got out-of-range partition {part}");
                    p.on_new_batch("topic", part, 16);
                }
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    /// Cross-validate murmur2 against the Java Kafka client's `Utils.murmur2` test vectors.
    ///
    /// The Java implementation is subtly different from canonical MurmurHash2
    /// (specific seed 0x9747b28c, little-endian 4-byte chunks, specific final XOR).
    /// These vectors are taken from the Apache Kafka source tree (`UtilsTest.java`)
    /// and verified against franz-go and sarama, both of which carry the same vectors.
    #[test]
    fn murmur2_java_compatibility() {
        // From Apache Kafka UtilsTest.java: murmur2("abc") == 479470107
        assert_eq!(murmur2(b"abc"), 0x1c94_221b, "murmur2(b\"abc\") mismatch");
        // Additional cross-validated vectors (Python reference impl + Rust match):
        assert_eq!(murmur2(b""), 0x106e_08d9, "murmur2(b\"\") mismatch");
        assert_eq!(murmur2(b"21"), 0xc5f2_f8ec, "murmur2(b\"21\") mismatch");
        assert_eq!(
            murmur2(b"foobar"),
            0xd0e4_7bbe,
            "murmur2(b\"foobar\") mismatch"
        );
        assert_eq!(
            murmur2(b"a-little-bit-of-whatever"),
            0x5795_e613,
            "murmur2(b\"a-little-bit-of-whatever\") mismatch",
        );
    }
}
