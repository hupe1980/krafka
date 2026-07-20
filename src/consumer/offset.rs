//! Offset management for consumers.

use ahash::AHashMap as HashMap;

use crate::{Offset, PartitionId};

/// Offset commit metadata.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct OffsetAndMetadata {
    /// The offset to commit.
    pub offset: Offset,
    /// Leader epoch.
    pub leader_epoch: Option<i32>,
    /// Optional metadata.
    pub metadata: Option<String>,
}

impl OffsetAndMetadata {
    /// Create a new offset with no metadata.
    pub fn new(offset: Offset) -> Self {
        Self {
            offset,
            leader_epoch: None,
            metadata: None,
        }
    }

    /// Create with leader epoch.
    pub fn with_epoch(offset: Offset, epoch: i32) -> Self {
        Self {
            offset,
            leader_epoch: Some(epoch),
            metadata: None,
        }
    }

    /// Create with metadata.
    pub fn with_metadata(offset: Offset, metadata: impl Into<String>) -> Self {
        Self {
            offset,
            leader_epoch: None,
            metadata: Some(metadata.into()),
        }
    }
}

/// Tracks committed and fetched offsets.
///
/// # Offset convention — read this before using [`commit`]
///
/// Kafka offsets are **next-to-read** positions, never last-processed ones.
/// Both [`commit`] and [`set_position`] take a value with that meaning:
///
/// > the offset of the next record the consumer should read
///
/// So after successfully processing the record at offset `N`, the value to
/// store is `N + 1`. Storing `N` itself makes the broker hand record `N` back
/// on the next restart, silently re-delivering the last record of every
/// partition forever. Because this is off-by-one rather than obviously broken,
/// it usually survives testing and shows up as mysterious duplicates in
/// production.
///
/// Use [`commit_processed`] to avoid doing the arithmetic by hand:
///
/// ```
/// use krafka::consumer::OffsetStore;
///
/// let mut store = OffsetStore::new();
///
/// // Just finished processing the record at offset 99.
/// store.commit_processed("orders", 0, 99);
///
/// // The stored value is the *next* offset to read, not 99.
/// assert_eq!(store.committed("orders", 0).unwrap().offset, 100);
/// ```
///
/// The same convention applies to [`position`]: a partition whose records up
/// to and including offset 99 have been fetched has position `100`. It is
/// therefore normal and correct for `position` to exceed `committed` — the
/// difference is exactly the records that have been fetched but not yet
/// committed.
///
/// # Layout
///
/// Keyed as `topic → partition → value` using two-level `HashMap` nesting.
/// This gives zero-allocation reads (the inner `HashMap::get` takes `&PartitionId`
/// which is `Copy`, and the outer takes `&str` via `String: Borrow<str>`).
///
/// A flat `(String, PartitionId)` key would require calling `.to_owned()`
/// on every read path because Rust's `Borrow` trait does not extend to tuples.
///
/// [`commit`]: OffsetStore::commit
/// [`commit_processed`]: OffsetStore::commit_processed
/// [`set_position`]: OffsetStore::set_position
/// [`position`]: OffsetStore::position
#[derive(Debug, Default)]
pub struct OffsetStore {
    /// Committed offsets: topic → partition → metadata.
    ///
    /// The stored offset is the next offset to read, i.e.
    /// `last_processed_offset + 1`.
    committed: HashMap<String, HashMap<PartitionId, OffsetAndMetadata>>,
    /// Current fetch position: topic → partition → offset.
    ///
    /// The stored offset is the next offset to fetch, i.e.
    /// `last_fetched_offset + 1`.
    position: HashMap<String, HashMap<PartitionId, Offset>>,
}

impl OffsetStore {
    /// Create a new offset store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the committed offset for a topic-partition.
    ///
    /// `offset.offset` must be the **next offset to read**, not the offset of
    /// the last record processed. See the [type-level convention][OffsetStore]
    /// for why. If you have the last processed offset in hand, prefer
    /// [`commit_processed`](OffsetStore::commit_processed).
    #[inline]
    pub fn commit(&mut self, topic: &str, partition: PartitionId, offset: OffsetAndMetadata) {
        match self.committed.get_mut(topic) {
            Some(inner) => {
                inner.insert(partition, offset);
            }
            None => {
                self.committed
                    .insert(topic.to_owned(), HashMap::from([(partition, offset)]));
            }
        }
    }

    /// Get the committed offset for a topic-partition.
    #[inline]
    pub fn committed(&self, topic: &str, partition: PartitionId) -> Option<&OffsetAndMetadata> {
        self.committed.get(topic)?.get(&partition)
    }

    /// Record that the record at `processed_offset` has been fully processed,
    /// committing `processed_offset + 1` as the next offset to read.
    ///
    /// This is the arithmetic-free counterpart to [`commit`](OffsetStore::commit)
    /// and the method to reach for in a normal consume loop, where what you
    /// naturally have is the offset of the record you just handled:
    ///
    /// ```
    /// # use krafka::consumer::OffsetStore;
    /// # let mut store = OffsetStore::new();
    /// # let record_topic = "t";
    /// # let record_partition = 0;
    /// # let record_offset = 41;
    /// // ... process(record) ...
    /// store.commit_processed(record_topic, record_partition, record_offset);
    /// assert_eq!(store.committed("t", 0).unwrap().offset, 42);
    /// ```
    ///
    /// Saturates rather than overflowing at `Offset::MAX`.
    #[inline]
    pub fn commit_processed(&mut self, topic: &str, partition: PartitionId, processed: Offset) {
        self.commit(
            topic,
            partition,
            OffsetAndMetadata::new(processed.saturating_add(1)),
        );
    }

    /// Set the current position for a topic-partition.
    ///
    /// `offset` must be the **next offset to fetch**, not the offset of the
    /// last record fetched. See the [type-level convention][OffsetStore].
    #[inline]
    pub fn set_position(&mut self, topic: &str, partition: PartitionId, offset: Offset) {
        match self.position.get_mut(topic) {
            Some(inner) => {
                inner.insert(partition, offset);
            }
            None => {
                self.position
                    .insert(topic.to_owned(), HashMap::from([(partition, offset)]));
            }
        }
    }

    /// Get the current position for a topic-partition.
    #[inline]
    pub fn position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.position.get(topic)?.get(&partition).copied()
    }

    /// Iterate over all committed offsets.
    #[inline]
    pub fn all_committed(&self) -> impl Iterator<Item = ((&str, PartitionId), &OffsetAndMetadata)> {
        self.committed
            .iter()
            .flat_map(|(t, parts)| parts.iter().map(move |(p, v)| ((t.as_str(), *p), v)))
    }

    /// Iterate over all positions.
    #[inline]
    pub fn all_positions(&self) -> impl Iterator<Item = ((&str, PartitionId), Offset)> {
        self.position
            .iter()
            .flat_map(|(t, parts)| parts.iter().map(move |(p, v)| ((t.as_str(), *p), *v)))
    }

    /// Clear all offsets for a topic.
    pub fn clear_topic(&mut self, topic: &str) {
        self.committed.remove(topic);
        self.position.remove(topic);
    }

    /// Clear all offsets.
    pub fn clear(&mut self) {
        self.committed.clear();
        self.position.clear();
    }
}

/// Offset reset strategy result.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetOffset {
    /// Use the earliest available offset.
    Earliest,
    /// Use the latest available offset.
    Latest,
    /// Use a specific offset.
    Specific(Offset),
}

impl ResetOffset {
    /// Convert to the protocol offset value.
    pub fn to_offset(&self) -> Offset {
        match self {
            ResetOffset::Earliest => -2,
            ResetOffset::Latest => -1,
            ResetOffset::Specific(o) => *o,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_and_metadata() {
        let om = OffsetAndMetadata::new(100);
        assert_eq!(om.offset, 100);
        assert!(om.leader_epoch.is_none());
        assert!(om.metadata.is_none());

        let om = OffsetAndMetadata::with_epoch(200, 5);
        assert_eq!(om.offset, 200);
        assert_eq!(om.leader_epoch, Some(5));

        let om = OffsetAndMetadata::with_metadata(300, "test");
        assert_eq!(om.metadata, Some("test".to_string()));
    }

    #[test]
    fn test_offset_store() {
        let mut store = OffsetStore::new();

        // Records up to offset 99 fetched -> position is the next offset, 100.
        store.set_position("topic1", 0, 100);
        store.set_position("topic1", 1, 200);
        // Records up to offset 49 processed and committed -> committed is 50.
        // Position ahead of committed is the normal steady state: the gap is
        // exactly the records fetched but not yet committed.
        store.commit("topic1", 0, OffsetAndMetadata::new(50));

        assert_eq!(store.position("topic1", 0), Some(100));
        assert_eq!(store.position("topic1", 1), Some(200));
        assert_eq!(store.position("topic1", 2), None);

        assert_eq!(store.committed("topic1", 0).unwrap().offset, 50);
        assert!(store.committed("topic1", 1).is_none());
    }

    #[test]
    fn test_commit_processed_stores_next_offset_to_read() {
        let mut store = OffsetStore::new();

        // Having processed the record at offset 99, the next read starts at 100.
        // Storing 99 here would re-deliver record 99 on every restart.
        store.commit_processed("topic1", 0, 99);
        assert_eq!(store.committed("topic1", 0).unwrap().offset, 100);

        // Processing the very first record of a partition commits 1, not 0.
        store.commit_processed("topic1", 1, 0);
        assert_eq!(store.committed("topic1", 1).unwrap().offset, 1);
    }

    #[test]
    fn test_commit_processed_saturates_at_max() {
        let mut store = OffsetStore::new();
        store.commit_processed("topic1", 0, Offset::MAX);
        assert_eq!(store.committed("topic1", 0).unwrap().offset, Offset::MAX);
    }

    #[test]
    fn test_commit_processed_matches_manual_commit() {
        let mut a = OffsetStore::new();
        let mut b = OffsetStore::new();

        a.commit_processed("t", 0, 41);
        b.commit("t", 0, OffsetAndMetadata::new(42));

        assert_eq!(
            a.committed("t", 0).unwrap().offset,
            b.committed("t", 0).unwrap().offset
        );
    }

    #[test]
    fn test_offset_store_clear() {
        let mut store = OffsetStore::new();
        store.set_position("topic1", 0, 100);
        store.set_position("topic2", 0, 200);

        store.clear_topic("topic1");
        assert!(store.position("topic1", 0).is_none());
        assert_eq!(store.position("topic2", 0), Some(200));

        store.clear();
        assert!(store.position("topic2", 0).is_none());
    }

    #[test]
    fn test_reset_offset() {
        assert_eq!(ResetOffset::Earliest.to_offset(), -2);
        assert_eq!(ResetOffset::Latest.to_offset(), -1);
        assert_eq!(ResetOffset::Specific(42).to_offset(), 42);
    }
}
