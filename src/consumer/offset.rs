//! Offset management for consumers.

use std::collections::HashMap;

use crate::{Offset, PartitionId};

/// Offset commit metadata.
#[derive(Debug, Clone, Default)]
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
/// Internally uses nested maps (`topic → partition → value`) so that
/// read-only lookups (`committed`, `position`) never allocate. Only
/// mutating operations that introduce a new topic key allocate a `String`.
#[derive(Debug, Default)]
pub struct OffsetStore {
    /// Committed offsets: topic → partition → metadata.
    committed: HashMap<String, HashMap<PartitionId, OffsetAndMetadata>>,
    /// Current fetch position: topic → partition → offset.
    position: HashMap<String, HashMap<PartitionId, Offset>>,
}

impl OffsetStore {
    /// Create a new offset store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the committed offset for a topic-partition.
    #[inline]
    pub fn commit(&mut self, topic: &str, partition: PartitionId, offset: OffsetAndMetadata) {
        self.committed
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }

    /// Get the committed offset for a topic-partition.
    #[inline]
    pub fn committed(&self, topic: &str, partition: PartitionId) -> Option<&OffsetAndMetadata> {
        self.committed.get(topic)?.get(&partition)
    }

    /// Set the current position for a topic-partition.
    #[inline]
    pub fn set_position(&mut self, topic: &str, partition: PartitionId, offset: Offset) {
        self.position
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }

    /// Get the current position for a topic-partition.
    #[inline]
    pub fn position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.position.get(topic)?.get(&partition).copied()
    }

    /// Get all committed offsets.
    #[inline]
    pub fn all_committed(&self) -> &HashMap<String, HashMap<PartitionId, OffsetAndMetadata>> {
        &self.committed
    }

    /// Get all positions.
    #[inline]
    pub fn all_positions(&self) -> &HashMap<String, HashMap<PartitionId, Offset>> {
        &self.position
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

        store.set_position("topic1", 0, 100);
        store.set_position("topic1", 1, 200);
        store.commit("topic1", 0, OffsetAndMetadata::new(50));

        assert_eq!(store.position("topic1", 0), Some(100));
        assert_eq!(store.position("topic1", 1), Some(200));
        assert_eq!(store.position("topic1", 2), None);

        assert_eq!(store.committed("topic1", 0).unwrap().offset, 50);
        assert!(store.committed("topic1", 1).is_none());
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
