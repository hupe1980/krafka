//! The range assignor: per topic, contiguous partition ranges over the
//! members subscribed to it, in member order.

use ahash::AHashMap as HashMap;

use super::{MemberSubscription, collect};
use crate::PartitionId;
use crate::consumer::group::MemberAssignment;

pub(super) fn assign(
    members: &[MemberSubscription],
    topics: &[(String, Vec<PartitionId>)],
) -> HashMap<String, MemberAssignment> {
    let mut owners = Vec::new();
    for (topic, partitions) in topics {
        let subscribers: Vec<usize> = (0..members.len())
            .filter(|&i| members[i].subscribes(topic))
            .collect();
        if subscribers.is_empty() {
            continue;
        }
        let per_member = partitions.len() / subscribers.len();
        let extra = partitions.len() % subscribers.len();
        let mut next = 0;
        for (rank, &member) in subscribers.iter().enumerate() {
            let count = per_member + usize::from(rank < extra);
            for &partition in &partitions[next..next + count] {
                owners.push(((topic.clone(), partition), member));
            }
            next += count;
        }
    }
    collect(members, owners)
}
