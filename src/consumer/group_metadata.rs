//! Consumer group identity used to fence transactional offset commits.

/// A snapshot of a consumer's identity within its group.
///
/// # Why this exists
///
/// When a transactional producer commits consumer offsets as part of a
/// transaction (`sendOffsetsToTransaction`), the group coordinator needs to
/// know *which* member is committing. A commit that carries no group identity
/// is accepted unconditionally: the coordinator has no way to tell a live
/// member from a zombie that was partitioned away, lost its partitions to a
/// rebalance, and then came back. Such a zombie will happily commit the
/// position it had before the rebalance, overwriting the position of the
/// member that now owns the partition. The new owner then either re-reads
/// records it already processed or, more often, skips records it never
/// processed — either way, exactly-once is broken.
///
/// Passing this metadata along with the commit lets the coordinator reject the
/// zombie: if the generation is older than the current one, or the member id
/// is no longer part of the group, the commit is fenced with
/// `FENCED_INSTANCE_ID` / `ILLEGAL_GENERATION` instead of silently applied.
/// This is the mechanism specified by KIP-447.
///
/// # Re-read it every transaction
///
/// The generation changes on **every** rebalance. A `ConsumerGroupMetadata`
/// captured once and reused across transactions will start failing (or, worse,
/// stop fencing correctly) as soon as the group rebalances. Always call
/// [`Consumer::group_metadata`] again for each transaction rather than caching
/// the value.
///
/// [`Consumer::group_metadata`]: crate::consumer::Consumer::group_metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    /// The consumer group this member belongs to.
    group_id: String,
    /// The group generation (classic protocol) or member epoch (KIP-848).
    ///
    /// Negative when the member holds no valid generation.
    generation_id: i32,
    /// The coordinator-assigned member id. Empty before the first successful
    /// join.
    member_id: String,
    /// The static membership instance id (`group.instance.id`, KIP-345), if
    /// this member is a static member.
    group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Create a new group metadata snapshot.
    pub fn new(
        group_id: impl Into<String>,
        generation_id: i32,
        member_id: impl Into<String>,
        group_instance_id: Option<String>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id,
            member_id: member_id.into(),
            group_instance_id,
        }
    }

    /// The consumer group id.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// The group generation (classic) or member epoch (KIP-848).
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// The coordinator-assigned member id.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The static membership instance id, if configured.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Whether this snapshot carries enough identity for the coordinator to
    /// fence a stale commit.
    ///
    /// A negative generation means the member has not completed a join, and an
    /// empty member id means the coordinator has not assigned one yet. In
    /// either case the coordinator cannot distinguish this member from any
    /// other, so a transactional commit built from this snapshot would not be
    /// fenced.
    pub fn is_fenceable(&self) -> bool {
        self.generation_id >= 0 && !self.member_id.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_accessors_round_trip() {
        let m = ConsumerGroupMetadata::new("g1", 7, "member-a", Some("inst-1".to_string()));
        assert_eq!(m.group_id(), "g1");
        assert_eq!(m.generation_id(), 7);
        assert_eq!(m.member_id(), "member-a");
        assert_eq!(m.group_instance_id(), Some("inst-1"));
    }

    #[test]
    fn test_dynamic_member_has_no_instance_id() {
        let m = ConsumerGroupMetadata::new("g1", 1, "member-a", None);
        assert_eq!(m.group_instance_id(), None);
    }

    #[test]
    fn test_is_fenceable_requires_generation_and_member_id() {
        // Fully joined member: the coordinator can fence a zombie.
        assert!(ConsumerGroupMetadata::new("g", 0, "m", None).is_fenceable());
        assert!(ConsumerGroupMetadata::new("g", 42, "m", None).is_fenceable());

        // No generation yet — join has not completed.
        assert!(!ConsumerGroupMetadata::new("g", -1, "m", None).is_fenceable());

        // No member id yet — coordinator has not assigned one.
        assert!(!ConsumerGroupMetadata::new("g", 5, "", None).is_fenceable());

        // Neither.
        assert!(!ConsumerGroupMetadata::new("g", -1, "", None).is_fenceable());
    }

    #[test]
    fn test_equality_and_clone() {
        let a = ConsumerGroupMetadata::new("g", 3, "m", Some("i".to_string()));
        let b = a.clone();
        assert_eq!(a, b);

        // Generation is part of identity: a rebalance produces a distinct value.
        let c = ConsumerGroupMetadata::new("g", 4, "m", Some("i".to_string()));
        assert_ne!(a, c);
    }
}
