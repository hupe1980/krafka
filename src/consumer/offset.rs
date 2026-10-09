//! Offset management for consumers.

use crate::Offset;

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
}
