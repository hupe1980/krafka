//! How a record chooses its partition.
//!
//! Without a custom [`Partitioner`] the producer partitions as Java's
//! built-in partitioner does (KIP-794): a keyed record goes to
//! `murmur2(key) mod partitions`; keyless records stick to one partition
//! until at least `batch_size` bytes were routed to it, then switch to a
//! different partition chosen at random. With `partitioner_rack_aware` and a
//! `client_rack`, a switch only chooses partitions whose leader is in that
//! rack (KIP-1123), falling back to all partitions when none is.
//!
//! A custom partitioner gets neither the byte accounting nor the racks, as in
//! Java; its answer is range-checked before the record is queued.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ahash::AHashMap;
use parking_lot::Mutex;

use crate::PartitionId;
use crate::metadata::ClusterMetadata;

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

/// A custom partitioning strategy.
///
/// Set with `partitioner` on either producer builder; without one the
/// producer uses the built-in partitioner described in the module docs.
///
/// # Determinism contract
///
/// Implementations **must** be deterministic for keyed records: the same
/// `(topic, key)` pair must always map to the same partition (given a
/// fixed `partition_count`). This is required for per-key ordering
/// guarantees. Unkeyed records (`key = None`) may use any strategy.
///
/// The answer must lie in `[0, partition_count)`; anything else fails the
/// send with a configuration error.
pub trait Partitioner: Send + Sync {
    /// Determine the partition for a record.
    ///
    /// * `topic` - The topic name
    /// * `key` - The record key, if any
    /// * `partition_count` - Number of partitions for the topic
    fn partition(&self, topic: &str, key: Option<&[u8]>, partition_count: usize) -> PartitionId;
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

/// How one producer partitions: the built-in partitioner or a custom one.
pub(crate) enum Partitioning {
    BuiltIn(BuiltInPartitioner),
    Custom(Arc<dyn Partitioner>),
}

impl Partitioning {
    pub(crate) fn new(
        custom: Option<Arc<dyn Partitioner>>,
        batch_size: usize,
        rack: Option<String>,
    ) -> Self {
        match custom {
            Some(custom) => Self::Custom(custom),
            None => Self::BuiltIn(BuiltInPartitioner::new(batch_size, rack)),
        }
    }

    /// The partition for a record of `record_size` bytes. Unchecked: the
    /// caller range-checks the answer.
    pub(crate) fn partition(
        &self,
        metadata: &ClusterMetadata,
        topic: &Arc<str>,
        key: Option<&[u8]>,
        record_size: usize,
        partition_count: usize,
    ) -> PartitionId {
        match self {
            Self::BuiltIn(built_in) => {
                built_in.partition(metadata, topic, key, record_size, partition_count)
            }
            Self::Custom(custom) => custom.partition(topic, key, partition_count),
        }
    }
}

/// One topic's sticky choice for keyless records.
#[derive(Debug, Clone, Copy)]
struct Sticky {
    partition: PartitionId,
    /// Bytes routed to `partition` since it was chosen.
    bytes: usize,
}

/// The default partitioner: murmur2 for keys, byte-accounted stickiness for
/// keyless records (KIP-794), optionally rack-aware (KIP-1123).
#[derive(Debug)]
pub(crate) struct BuiltInPartitioner {
    batch_size: usize,
    /// `Some` when rack-aware partitioning is on.
    rack: Option<String>,
    sticky: Mutex<AHashMap<Arc<str>, Sticky>>,
}

impl BuiltInPartitioner {
    /// Topics whose sticky state is kept; beyond it one entry is evicted.
    pub(crate) const MAX_TRACKED_TOPICS: usize = 10_000;

    pub(crate) fn new(batch_size: usize, rack: Option<String>) -> Self {
        Self {
            batch_size: batch_size.max(1),
            rack,
            sticky: Mutex::new(AHashMap::new()),
        }
    }

    fn partition(
        &self,
        metadata: &ClusterMetadata,
        topic: &Arc<str>,
        key: Option<&[u8]>,
        record_size: usize,
        partition_count: usize,
    ) -> PartitionId {
        if partition_count == 0 {
            return 0;
        }
        if let Some(key) = key {
            return partition_for_key(key, partition_count);
        }

        let mut sticky = self.sticky.lock();
        if !sticky.contains_key(topic) && sticky.len() >= Self::MAX_TRACKED_TOPICS {
            let evict = sticky.keys().next().cloned();
            if let Some(evict) = evict {
                sticky.remove(&evict);
            }
        }
        let entry = sticky.entry(Arc::clone(topic)).or_insert_with(|| Sticky {
            partition: self.choose(metadata, topic, partition_count, None),
            bytes: 0,
        });
        // The topic shrank (or the state is stale): choose again.
        if entry.partition as usize >= partition_count {
            *entry = Sticky {
                partition: self.choose(metadata, topic, partition_count, None),
                bytes: 0,
            };
        }
        let chosen = entry.partition;
        entry.bytes += record_size;
        if entry.bytes >= self.batch_size {
            *entry = Sticky {
                partition: self.choose(metadata, topic, partition_count, Some(chosen)),
                bytes: 0,
            };
        }
        chosen
    }

    /// A partition chosen uniformly at random, other than `avoid` when there
    /// is another, among the partitions led in the client's rack if any are.
    fn choose(
        &self,
        metadata: &ClusterMetadata,
        topic: &str,
        partition_count: usize,
        avoid: Option<PartitionId>,
    ) -> PartitionId {
        let in_rack = self
            .rack
            .as_deref()
            .map(|rack| partitions_led_in_rack(metadata, topic, rack, partition_count))
            .filter(|candidates| !candidates.is_empty());
        match in_rack {
            Some(candidates) => pick_other(&candidates, avoid),
            None => {
                let all: Vec<PartitionId> = (0..partition_count)
                    .map(|p| PartitionId::try_from(p).unwrap_or(PartitionId::MAX))
                    .collect();
                pick_other(&all, avoid)
            }
        }
    }
}

/// Partitions of `topic` whose current leader advertises `rack`. A leader
/// that advertises no rack, or no known leader, does not count.
fn partitions_led_in_rack(
    metadata: &ClusterMetadata,
    topic: &str,
    rack: &str,
    partition_count: usize,
) -> Vec<PartitionId> {
    let Some(info) = metadata.topic_arc(topic) else {
        return Vec::new();
    };
    let mut candidates: Vec<PartitionId> = info
        .partitions_iter()
        .filter(|p| p.leader >= 0 && (p.partition as usize) < partition_count)
        .filter(|p| {
            metadata
                .broker(p.leader)
                .is_some_and(|broker| broker.rack() == Some(rack))
        })
        .map(|p| p.partition)
        .collect();
    candidates.sort_unstable();
    candidates
}

/// A uniformly random element of `candidates`, other than `avoid` when
/// `candidates` has another.
fn pick_other(candidates: &[PartitionId], avoid: Option<PartitionId>) -> PartitionId {
    let others: Vec<PartitionId> = candidates
        .iter()
        .copied()
        .filter(|p| Some(*p) != avoid)
        .collect();
    let pool = if others.is_empty() {
        candidates
    } else {
        &others
    };
    match pool.len() {
        0 => 0,
        1 => pool[0],
        n => pool[crate::util::with_rng(|rng| rand::Rng::random_range(rng, 0..n))],
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_murmur2_is_deterministic() {
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

    fn metadata() -> ClusterMetadata {
        let pool = Arc::new(crate::network::ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            std::time::Duration::from_secs(300),
        )
    }

    /// Keyless records stay on one partition until `batch_size` bytes were
    /// routed to it, then move to a different one — whatever happens to the
    /// batches.
    #[test]
    fn keyless_records_switch_after_batch_size_bytes() {
        let metadata = metadata();
        let topic: Arc<str> = Arc::from("t");
        let p = BuiltInPartitioner::new(1000, None);
        let mut runs = vec![(p.partition(&metadata, &topic, None, 100, 4), 1usize)];
        for _ in 1..100 {
            let partition = p.partition(&metadata, &topic, None, 100, 4);
            match runs.last_mut() {
                Some((current, n)) if *current == partition => *n += 1,
                _ => runs.push((partition, 1)),
            }
        }
        assert_eq!(runs.len(), 10, "{runs:?}");
        assert!(runs.iter().all(|(_, n)| *n == 10), "{runs:?}");
        assert!(
            runs.windows(2).all(|w| w[0].0 != w[1].0),
            "every switch moves to a different partition: {runs:?}"
        );
    }

    /// Keyed records hash, and do not count toward the keyless budget.
    #[test]
    fn keyed_records_hash_and_are_not_charged() {
        let metadata = metadata();
        let topic: Arc<str> = Arc::from("t");
        let p = BuiltInPartitioner::new(1000, None);
        let first = p.partition(&metadata, &topic, None, 100, 4);
        for _ in 0..50 {
            assert_eq!(
                p.partition(&metadata, &topic, Some(b"kafka"), 500, 10),
                partition_for_key(b"kafka", 10)
            );
        }
        assert_eq!(p.partition(&metadata, &topic, None, 100, 4), first);
    }

    #[test]
    fn a_shrunken_topic_gets_a_valid_partition() {
        let metadata = metadata();
        let topic: Arc<str> = Arc::from("t");
        let p = BuiltInPartitioner::new(1_000_000, None);
        for _ in 0..20 {
            let _ = p.partition(&metadata, &topic, None, 1, 64);
        }
        assert_eq!(p.partition(&metadata, &topic, None, 1, 1), 0);
        assert_eq!(p.partition(&metadata, &topic, None, 1, 0), 0);
    }

    #[test]
    fn a_single_partition_takes_every_switch() {
        let metadata = metadata();
        let topic: Arc<str> = Arc::from("t");
        let p = BuiltInPartitioner::new(10, None);
        for _ in 0..100 {
            assert_eq!(p.partition(&metadata, &topic, None, 7, 1), 0);
        }
    }

    #[test]
    fn concurrent_keyless_routing_stays_in_range() {
        use std::thread;
        let metadata = Arc::new(metadata());
        let p = Arc::new(BuiltInPartitioner::new(100, None));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let p = Arc::clone(&p);
            let metadata = Arc::clone(&metadata);
            handles.push(thread::spawn(move || {
                let topic: Arc<str> = Arc::from("t");
                for _ in 0..1000 {
                    let part = p.partition(&metadata, &topic, None, 10, 16);
                    assert!((0..16).contains(&part), "got out-of-range partition {part}");
                }
            }));
        }
        for h in handles {
            h.join().expect("thread panicked");
        }
    }

    #[test]
    fn pick_other_avoids_the_partition_just_left() {
        for _ in 0..100 {
            assert_eq!(pick_other(&[3, 5], Some(3)), 5);
        }
        assert_eq!(pick_other(&[3], Some(3)), 3);
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
