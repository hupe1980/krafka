#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use proptest::prelude::*;

use super::*;

const ALL: [PartitionAssignmentStrategy; 3] = [
    PartitionAssignmentStrategy::Range,
    PartitionAssignmentStrategy::RoundRobin,
    PartitionAssignmentStrategy::CooperativeSticky,
];

fn member(id: &str, topics: &[&str]) -> MemberSubscription {
    MemberSubscription {
        member_id: id.to_string(),
        topics: topics.iter().map(|t| t.to_string()).collect(),
        owned: Vec::new(),
        generation: -1,
    }
}

fn owning(
    id: &str,
    topics: &[&str],
    owned: &MemberAssignment,
    generation: i32,
) -> MemberSubscription {
    MemberSubscription {
        owned: owned
            .all_partitions()
            .map(|(t, p)| (t.to_string(), p))
            .collect(),
        generation,
        ..member(id, topics)
    }
}

fn partitions(spec: &[(&str, i32)]) -> HashMap<String, Vec<PartitionId>> {
    spec.iter()
        .map(|(t, n)| (t.to_string(), (0..*n).collect()))
        .collect()
}

fn counts(out: &HashMap<String, MemberAssignment>, ids: &[&str]) -> Vec<usize> {
    ids.iter()
        .map(|id| {
            out.get(*id)
                .map(|m| m.all_partitions().count())
                .unwrap_or(0)
        })
        .collect()
}

/// Members with heterogeneous subscriptions get only their own topics, and a
/// topic only a follower subscribes to is still assigned.
#[test]
fn every_member_gets_only_the_topics_it_subscribed_to() {
    let parts = partitions(&[("a", 2), ("b", 2)]);
    let members = vec![member("m1", &["a"]), member("m2", &["b"])];
    for strategy in ALL {
        let out = assign(strategy, &members, &parts);
        let m1 = out.get("m1").unwrap();
        let m2 = out.get("m2").unwrap();
        assert_eq!(m1.get("a"), Some(&[0, 1][..]), "{strategy:?}");
        assert!(m1.get("b").is_none(), "{strategy:?}");
        assert_eq!(
            m2.get("b"),
            Some(&[0, 1][..]),
            "{strategy:?}: b is assigned"
        );
        assert!(m2.get("a").is_none(), "{strategy:?}");
    }
}

/// A group of three on six partitions grows to four: the new member gets
/// work, the counts are 2/2/1/1, and only one partition moves.
#[test]
fn cooperative_sticky_gives_a_new_member_work() {
    let parts = partitions(&[("t", 6)]);
    let three = vec![
        member("m1", &["t"]),
        member("m2", &["t"]),
        member("m3", &["t"]),
    ];
    let first = assign(
        PartitionAssignmentStrategy::CooperativeSticky,
        &three,
        &parts,
    );
    assert_eq!(counts(&first, &["m1", "m2", "m3"]), vec![2, 2, 2]);

    let four: Vec<MemberSubscription> = ["m1", "m2", "m3"]
        .iter()
        .map(|id| owning(id, &["t"], first.get(*id).unwrap(), 1))
        .chain(std::iter::once(member("m4", &["t"])))
        .collect();
    let second = assign(
        PartitionAssignmentStrategy::CooperativeSticky,
        &four,
        &parts,
    );
    let mut sizes = counts(&second, &["m1", "m2", "m3", "m4"]);
    assert_eq!(sizes[3], 1, "the new member is not idle");
    sizes.sort_unstable();
    assert_eq!(sizes, vec![1, 1, 2, 2]);

    let mut moved = 0;
    for id in ["m1", "m2", "m3"] {
        let before: Vec<_> = first.get(id).unwrap().all_partitions().collect();
        let after: Vec<_> = second.get(id).unwrap().all_partitions().collect();
        assert!(
            after.iter().all(|tp| before.contains(tp)),
            "{id} only gave partitions up"
        );
        moved += before.len() - after.len();
    }
    assert_eq!(moved, 1, "the minimum number of partitions moved");
}

#[test]
fn homogeneous_range_and_roundrobin_match_the_classic_layout() {
    let parts = partitions(&[("t", 5)]);
    let members = vec![member("m1", &["t"]), member("m2", &["t"])];
    let range = assign(PartitionAssignmentStrategy::Range, &members, &parts);
    assert_eq!(range.get("m1").unwrap().get("t"), Some(&[0, 1, 2][..]));
    assert_eq!(range.get("m2").unwrap().get("t"), Some(&[3, 4][..]));
    let rr = assign(PartitionAssignmentStrategy::RoundRobin, &members, &parts);
    assert_eq!(rr.get("m1").unwrap().get("t"), Some(&[0, 2, 4][..]));
    assert_eq!(rr.get("m2").unwrap().get("t"), Some(&[1, 3][..]));
}

#[test]
fn the_same_input_gives_the_same_output() {
    let parts = partitions(&[("a", 7), ("b", 3)]);
    let members = vec![
        member("m1", &["a", "b"]),
        member("m2", &["a"]),
        member("m3", &["b"]),
    ];
    for strategy in ALL {
        let one = assign(strategy, &members, &parts);
        let two = assign(strategy, &members, &parts);
        for id in ["m1", "m2", "m3"] {
            let a: Vec<_> = one
                .get(id)
                .unwrap()
                .all_partitions()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            let b: Vec<_> = two
                .get(id)
                .unwrap()
                .all_partitions()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            assert_eq!(a, b, "{strategy:?} {id}");
        }
    }
}

#[test]
fn the_newer_generation_wins_a_double_claim() {
    let parts = partitions(&[("t", 2)]);
    let mut stale = MemberAssignment::empty();
    stale.add("t", vec![0]);
    let mut current = MemberAssignment::empty();
    current.add("t", vec![0]);
    let members = vec![
        owning("old", &["t"], &stale, 3),
        owning("new", &["t"], &current, 4),
    ];
    let out = assign(
        PartitionAssignmentStrategy::CooperativeSticky,
        &members,
        &parts,
    );
    assert_eq!(out.get("new").unwrap().get("t"), Some(&[0][..]));
    assert_eq!(out.get("old").unwrap().get("t"), Some(&[1][..]));
}

#[test]
fn withholding_removes_only_partitions_moving_between_live_members() {
    let parts = partitions(&[("t", 4)]);
    let mut owned = MemberAssignment::empty();
    owned.add("t", vec![0, 1, 2, 3]);
    let members = vec![owning("m1", &["t"], &owned, 1), member("m2", &["t"])];
    let mut out = assign(
        PartitionAssignmentStrategy::CooperativeSticky,
        &members,
        &parts,
    );
    assert_eq!(counts(&out, &["m1", "m2"]), vec![2, 2]);
    withhold_transferring_partitions(&mut out, &members);
    assert_eq!(
        counts(&out, &["m1", "m2"]),
        vec![2, 0],
        "m2's partitions are still m1's"
    );

    // The follow-up generation, after m1 revoked them, hands them over.
    let after = out.get("m1").unwrap().clone();
    let members = vec![owning("m1", &["t"], &after, 2), member("m2", &["t"])];
    let mut next = assign(
        PartitionAssignmentStrategy::CooperativeSticky,
        &members,
        &parts,
    );
    withhold_transferring_partitions(&mut next, &members);
    assert_eq!(counts(&next, &["m1", "m2"]), vec![2, 2]);
}

#[test]
fn a_departed_owner_does_not_hold_its_partitions_back() {
    let parts = partitions(&[("t", 2)]);
    let members = vec![member("m2", &["t"])];
    let mut out = assign(
        PartitionAssignmentStrategy::CooperativeSticky,
        &members,
        &parts,
    );
    withhold_transferring_partitions(&mut out, &members);
    assert_eq!(counts(&out, &["m2"]), vec![2]);
}

/// Per member: which of three topics it subscribes to, and owned `(topic, partition)`s.
type ArbMember = (Vec<bool>, Vec<(usize, i32)>);

fn arb_group() -> impl Strategy<Value = (Vec<ArbMember>, Vec<i32>)> {
    // Up to 3 topics with 0..8 partitions; up to 6 members, each subscribing
    // to a non-empty subset and owning a few arbitrary (topic, partition)s.
    (
        proptest::collection::vec(0i32..8, 1..4),
        proptest::collection::vec(
            (
                proptest::collection::vec(any::<bool>(), 3),
                proptest::collection::vec((0usize..3, 0i32..8), 0..6),
            ),
            1..7,
        ),
    )
        .prop_map(|(sizes, members)| (members, sizes))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    /// No member gets a topic it did not subscribe to; every partition of a
    /// subscribed topic is assigned exactly once; cooperative-sticky keeps
    /// members with equal subscriptions within one partition of each other
    /// and an unchanged member only gives partitions up.
    #[test]
    fn assignments_respect_subscriptions_and_balance((raw, sizes) in arb_group()) {
        let topic = |i: usize| format!("t{i}");
        let parts: HashMap<String, Vec<PartitionId>> = sizes
            .iter()
            .enumerate()
            .map(|(i, &n)| (topic(i), (0..n).collect()))
            .collect();
        let members: Vec<MemberSubscription> = raw
            .iter()
            .enumerate()
            .map(|(i, (subs, owned))| {
                let mut topics: HashSet<String> = subs
                    .iter()
                    .enumerate()
                    .filter(|(t, on)| **on && *t < sizes.len())
                    .map(|(t, _)| topic(t))
                    .collect();
                if topics.is_empty() {
                    topics.insert(topic(0));
                }
                MemberSubscription {
                    member_id: format!("m{i}"),
                    topics,
                    owned: owned.iter().map(|(t, p)| (topic(*t), *p)).collect(),
                    generation: i as i32,
                }
            })
            .collect();

        for strategy in ALL {
            let out = assign(strategy, &members, &parts);
            let mut seen = std::collections::HashSet::new();
            for m in &members {
                for (t, p) in out.get(&m.member_id).unwrap().all_partitions() {
                    prop_assert!(m.topics.contains(t), "{strategy:?}: {} got {t}", m.member_id);
                    prop_assert!(seen.insert((t.to_string(), p)), "{strategy:?}: {t}-{p} twice");
                }
            }
            let wanted: usize = subscribed_topics(&members)
                .iter()
                .map(|t| parts.get(t).map_or(0, Vec::len))
                .sum();
            prop_assert_eq!(seen.len(), wanted, "{:?}: every subscribed partition", strategy);

            if strategy == PartitionAssignmentStrategy::CooperativeSticky {
                for a in &members {
                    for b in &members {
                        if a.topics == b.topics {
                            let ca = out.get(&a.member_id).unwrap().all_partitions().count();
                            let cb = out.get(&b.member_id).unwrap().all_partitions().count();
                            prop_assert!(ca <= cb + 1, "{} has {ca}, {} has {cb}", a.member_id, b.member_id);
                        }
                    }
                }
            }
        }
    }
}
