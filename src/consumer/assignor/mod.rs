//! Client-side partition assignors for the classic group protocol.
//!
//! Each assignor is a pure function of every member's decoded subscription —
//! its topics, the partitions it owns and the generation it owned them in —
//! and the partition lists of the topics any member subscribed to. Nothing is
//! remembered between calls, so any member computes the same assignment when
//! it is the leader.

mod range;
mod roundrobin;
mod sticky;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use tracing::debug;

use crate::PartitionId;
use crate::consumer::config::PartitionAssignmentStrategy;
use crate::consumer::group::MemberAssignment;
use crate::protocol::ConsumerProtocolSubscription;

/// One member's subscription as the leader decoded it from JoinGroup.
#[derive(Debug, Clone)]
pub(crate) struct MemberSubscription {
    pub(crate) member_id: String,
    /// Topics the member subscribed to.
    pub(crate) topics: HashSet<String>,
    /// Partitions the member reported owning (cooperative protocol only).
    pub(crate) owned: Vec<(String, PartitionId)>,
    /// Generation in which it owned them; `-1` when unknown.
    pub(crate) generation: i32,
}

impl MemberSubscription {
    pub(crate) fn from_protocol(
        member_id: &str,
        subscription: ConsumerProtocolSubscription,
    ) -> Self {
        Self {
            member_id: member_id.to_string(),
            topics: subscription.topics.into_iter().collect(),
            owned: subscription
                .owned_partitions
                .into_iter()
                .flat_map(|tp| {
                    let topic = tp.topic;
                    tp.partitions.into_iter().map(move |p| (topic.clone(), p))
                })
                .collect(),
            generation: subscription.generation_id,
        }
    }

    fn subscribes(&self, topic: &str) -> bool {
        self.topics.contains(topic)
    }
}

/// Every topic at least one member subscribed to, sorted.
pub(crate) fn subscribed_topics(members: &[MemberSubscription]) -> Vec<String> {
    let mut topics: Vec<String> = members
        .iter()
        .flat_map(|m| m.topics.iter().cloned())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    topics.sort();
    topics
}

/// Assign `partitions` (keyed by topic) to `members` with `strategy`.
///
/// Every member appears in the result, with an empty assignment if it gets
/// nothing. A member only ever receives partitions of topics it subscribed
/// to, and every partition of a subscribed topic is assigned.
pub(crate) fn assign(
    strategy: PartitionAssignmentStrategy,
    members: &[MemberSubscription],
    partitions: &HashMap<String, Vec<PartitionId>>,
) -> HashMap<String, MemberAssignment> {
    let topics = subscribed_topics(members);
    let mut sorted: Vec<(String, Vec<PartitionId>)> = topics
        .into_iter()
        .filter_map(|topic| {
            let mut ps = partitions.get(&topic)?.clone();
            ps.sort_unstable();
            ps.dedup();
            Some((topic, ps))
        })
        .collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    match strategy {
        PartitionAssignmentStrategy::Range => range::assign(members, &sorted),
        PartitionAssignmentStrategy::RoundRobin => roundrobin::assign(members, &sorted),
        PartitionAssignmentStrategy::CooperativeSticky => sticky::assign(members, &sorted),
    }
}

/// Collect `(topic, partition) -> member` pairs into per-member assignments.
fn collect(
    members: &[MemberSubscription],
    owners: impl IntoIterator<Item = ((String, PartitionId), usize)>,
) -> HashMap<String, MemberAssignment> {
    let mut out: HashMap<String, MemberAssignment> = members
        .iter()
        .map(|m| (m.member_id.clone(), MemberAssignment::empty()))
        .collect();
    for ((topic, partition), index) in owners {
        if let Some(assignment) = out.get_mut(&members[index].member_id) {
            assignment
                .partitions
                .entry(topic)
                .or_default()
                .push(partition);
        }
    }
    for assignment in out.values_mut() {
        for ps in assignment.partitions.values_mut() {
            ps.sort_unstable();
        }
    }
    out
}

/// Remove every partition that is moving between two live members from its
/// new owner's assignment (cooperative protocol, KIP-429).
///
/// The previous owner is still consuming it, so handing it over in the same
/// generation would let two members consume it at once, and the new owner
/// could read the committed offset before the previous owner's final commit.
/// The partition is assigned to nobody this generation; the previous owner
/// sees it missing, revokes it and rejoins, and the follow-up rebalance
/// assigns it. A partition whose previous owner left passes through.
pub(crate) fn withhold_transferring_partitions(
    assignments: &mut HashMap<String, MemberAssignment>,
    members: &[MemberSubscription],
) {
    let previous_owner = sticky::resolve_claims(members);
    for (member_id, assignment) in assignments.iter_mut() {
        assignment.partitions.retain(|topic, partitions| {
            partitions.retain(|&p| match previous_owner.get(&(topic.clone(), p)) {
                Some(&owner) if members[owner].member_id != *member_id => {
                    debug!(
                        topic,
                        partition = p,
                        from = %members[owner].member_id,
                        to = %member_id,
                        "Withholding transferring partition for one generation"
                    );
                    false
                }
                _ => true,
            });
            !partitions.is_empty()
        });
    }
}

#[cfg(test)]
mod tests;
