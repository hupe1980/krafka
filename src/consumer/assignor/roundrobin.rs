//! The round-robin assignor: every partition of every subscribed topic, in
//! `(topic, partition)` order, to the next member in turn that subscribed to
//! its topic.

use ahash::AHashMap as HashMap;

use super::{MemberSubscription, collect};
use crate::PartitionId;
use crate::consumer::group::MemberAssignment;

pub(super) fn assign(
    members: &[MemberSubscription],
    topics: &[(String, Vec<PartitionId>)],
) -> HashMap<String, MemberAssignment> {
    let mut owners = Vec::new();
    if members.is_empty() {
        return collect(members, owners);
    }
    let mut cursor = 0usize;
    for (topic, partitions) in topics {
        for &partition in partitions {
            let mut chosen = None;
            for step in 0..members.len() {
                let candidate = (cursor + step) % members.len();
                if members[candidate].subscribes(topic) {
                    chosen = Some(candidate);
                    break;
                }
            }
            if let Some(member) = chosen {
                owners.push(((topic.clone(), partition), member));
                cursor = member + 1;
            }
        }
    }
    collect(members, owners)
}
