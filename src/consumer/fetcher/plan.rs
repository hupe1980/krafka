//! Which broker each fetchable partition is fetched from.

use ahash::AHashMap as HashMap;

use crate::BrokerId;
use crate::consumer::state::{FetchTarget, PartitionKey};

/// Fetch targets grouped by the broker they are sent to.
#[derive(Debug, Default)]
pub(super) struct FetchRoutingPlan {
    /// Partitions per broker, in the order they were given.
    pub(super) by_broker: Vec<(BrokerId, Vec<FetchTarget>)>,
    /// Partitions with neither a live preferred replica nor a known leader.
    pub(super) skipped: Vec<PartitionKey>,
}

/// Route each target to its KIP-392 preferred replica when it has a live one,
/// else to its leader. `leaders` comes from the metadata cache, which is also
/// where a leader a broker reported in a fetch response (KIP-951) lands.
pub(super) fn build_fetch_routing_plan(
    targets: Vec<FetchTarget>,
    leaders: &HashMap<PartitionKey, BrokerId>,
) -> FetchRoutingPlan {
    let mut plan = FetchRoutingPlan::default();
    let mut index: HashMap<BrokerId, usize> = HashMap::new();
    for target in targets {
        let broker = match target.preferred_replica {
            Some(replica) => replica,
            None => match leaders.get(&target.key) {
                Some(&leader) => leader,
                None => {
                    plan.skipped.push(target.key);
                    continue;
                }
            },
        };
        let slot = *index.entry(broker).or_insert_with(|| {
            plan.by_broker.push((broker, Vec::new()));
            plan.by_broker.len() - 1
        });
        plan.by_broker[slot].1.push(target);
    }
    plan
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn target(topic: &str, partition: i32, preferred: Option<BrokerId>) -> FetchTarget {
        FetchTarget {
            key: (topic.to_string(), partition),
            version: 1,
            offset: 0,
            last_fetched_epoch: -1,
            preferred_replica: preferred,
        }
    }

    fn leaders(entries: &[(&str, i32, BrokerId)]) -> HashMap<PartitionKey, BrokerId> {
        entries
            .iter()
            .map(|(t, p, b)| ((t.to_string(), *p), *b))
            .collect()
    }

    fn brokers_of(plan: &FetchRoutingPlan, topic: &str, partition: i32) -> Vec<BrokerId> {
        plan.by_broker
            .iter()
            .filter(|(_, targets)| {
                targets
                    .iter()
                    .any(|t| t.key == (topic.to_string(), partition))
            })
            .map(|(b, _)| *b)
            .collect()
    }

    #[test]
    fn the_leader_is_used_without_a_preferred_replica() {
        let plan = build_fetch_routing_plan(vec![target("t", 0, None)], &leaders(&[("t", 0, 1)]));
        assert_eq!(brokers_of(&plan, "t", 0), vec![1]);
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn a_preferred_replica_wins_over_the_leader() {
        let plan =
            build_fetch_routing_plan(vec![target("t", 0, Some(3))], &leaders(&[("t", 0, 1)]));
        assert_eq!(brokers_of(&plan, "t", 0), vec![3]);
    }

    #[test]
    fn a_partition_without_a_leader_is_skipped() {
        let plan = build_fetch_routing_plan(
            vec![target("t", 0, None), target("t", 1, None)],
            &leaders(&[("t", 1, 2)]),
        );
        assert_eq!(plan.skipped, vec![("t".to_string(), 0)]);
        assert_eq!(brokers_of(&plan, "t", 1), vec![2]);
    }

    #[test]
    fn partitions_of_one_broker_share_a_request_in_order() {
        let plan = build_fetch_routing_plan(
            vec![
                target("t", 2, None),
                target("t", 0, None),
                target("u", 1, Some(2)),
            ],
            &leaders(&[("t", 0, 1), ("t", 2, 1), ("u", 1, 1)]),
        );
        assert_eq!(plan.by_broker.len(), 2);
        let (broker, targets) = &plan.by_broker[0];
        assert_eq!(*broker, 1);
        let keys: Vec<i32> = targets.iter().map(|t| t.key.1).collect();
        assert_eq!(keys, vec![2, 0]);
    }
}
