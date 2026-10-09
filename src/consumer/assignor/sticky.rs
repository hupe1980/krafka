//! The cooperative-sticky assignor: keep every member's partitions where
//! balance allows, then even out the counts by moving as few partitions as
//! possible.
//!
//! 1. Each member keeps the partitions it reported owning, if their topic
//!    still exists and it still subscribes to it. A partition claimed by two
//!    members goes to the claim with the higher generation.
//! 2. Every remaining partition goes to the subscribed member with the fewest
//!    partitions.
//! 3. While some member has at least two more partitions than another member
//!    that subscribes to one of its topics, one such partition moves across —
//!    preferring a partition it did not own before, so stickiness is only
//!    given up where the counts demand it.
//!
//! Step 3 is what makes a new member get work: three members holding two of
//! six partitions each and a fourth holding none differ by two, so one
//! partition moves to the new member.

use std::collections::BTreeSet;

use ahash::AHashMap as HashMap;

use super::{MemberSubscription, collect};
use crate::PartitionId;
use crate::consumer::group::MemberAssignment;

type TopicPartition = (String, PartitionId);

/// The live owner of each claimed partition: the claim with the highest
/// generation, the first one on a tie.
pub(super) fn resolve_claims(members: &[MemberSubscription]) -> HashMap<TopicPartition, usize> {
    let mut claims: HashMap<TopicPartition, (usize, i32)> = HashMap::new();
    for (index, member) in members.iter().enumerate() {
        for tp in &member.owned {
            match claims.get(tp) {
                Some(&(_, generation)) if generation >= member.generation => {}
                _ => {
                    claims.insert(tp.clone(), (index, member.generation));
                }
            }
        }
    }
    claims
        .into_iter()
        .map(|(tp, (index, _))| (tp, index))
        .collect()
}

pub(super) fn assign(
    members: &[MemberSubscription],
    topics: &[(String, Vec<PartitionId>)],
) -> HashMap<String, MemberAssignment> {
    if members.is_empty() {
        return collect(members, Vec::new());
    }

    let all: Vec<TopicPartition> = topics
        .iter()
        .flat_map(|(topic, ps)| ps.iter().map(move |&p| (topic.clone(), p)))
        .collect();
    let exists: std::collections::HashSet<&TopicPartition> = all.iter().collect();

    let mut assigned: Vec<BTreeSet<TopicPartition>> = vec![BTreeSet::new(); members.len()];
    let mut owner: HashMap<TopicPartition, usize> = HashMap::new();

    // 1. Sticky claims.
    for (tp, index) in resolve_claims(members) {
        if exists.contains(&tp) && members[index].subscribes(&tp.0) {
            assigned[index].insert(tp.clone());
            owner.insert(tp, index);
        }
    }
    let kept: Vec<BTreeSet<TopicPartition>> = assigned.clone();

    // 2. Unowned partitions to the least-loaded subscriber.
    for tp in &all {
        if owner.contains_key(tp) {
            continue;
        }
        let target = (0..members.len())
            .filter(|&i| members[i].subscribes(&tp.0))
            .min_by_key(|&i| (assigned[i].len(), i));
        if let Some(index) = target {
            assigned[index].insert(tp.clone());
            owner.insert(tp.clone(), index);
        }
    }

    // 3. Balance. Every move lowers the sum of squared counts, so this ends.
    loop {
        let mut by_count: Vec<usize> = (0..members.len()).collect();
        by_count.sort_by_key(|&i| (assigned[i].len(), i));
        let mut moved = false;
        'search: for &to in &by_count {
            for &from in by_count.iter().rev() {
                if assigned[from].len() < assigned[to].len() + 2 {
                    break;
                }
                let candidate = assigned[from]
                    .iter()
                    .rev()
                    .filter(|tp| members[to].subscribes(&tp.0))
                    .max_by_key(|tp| !kept[from].contains(*tp))
                    .cloned();
                if let Some(tp) = candidate {
                    assigned[from].remove(&tp);
                    assigned[to].insert(tp.clone());
                    owner.insert(tp, to);
                    moved = true;
                    break 'search;
                }
            }
        }
        if !moved {
            break;
        }
    }

    collect(members, owner)
}
