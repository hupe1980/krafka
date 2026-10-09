#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::{BufMut, Bytes, BytesMut};

use super::heartbeat::PollTracker;
use super::*;
use crate::consumer::config::PartitionAssignmentStrategy as S;
use crate::protocol::{
    CONSUMER_PROTOCOL_V2, CONSUMER_PROTOCOL_V3, ConsumerProtocolSubscription,
    ConsumerProtocolTopicPartitions, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
    decode_consumer_protocol_subscription, encode_consumer_protocol_subscription,
};

fn coordinator(strategy: S) -> GroupCoordinator {
    let pool = Arc::new(ConnectionPool::new(
        crate::network::ConnectionConfig::default(),
    ));
    GroupCoordinator::new(
        "test-group",
        pool.clone(),
        Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )),
        Duration::from_secs(10),
        Duration::from_secs(3),
        Duration::from_secs(30),
    )
    .with_assignor_strategies(vec![strategy])
}

// ── KIP-447: stable committed offsets ───────────────────────────────────

#[test]
fn require_stable_is_asked_for_only_under_read_committed() {
    use crate::consumer::IsolationLevel;
    assert!(require_stable_for(IsolationLevel::ReadCommitted.to_i8()));
    assert!(!require_stable_for(IsolationLevel::ReadUncommitted.to_i8()));
}

fn offset_fetch_response(codes: &[ErrorCode]) -> OffsetFetchResponse {
    OffsetFetchResponse {
        throttle_time_ms: 0,
        topics: vec![OffsetFetchResponseTopic {
            name: "orders".to_string(),
            topic_id: None,
            partitions: codes
                .iter()
                .enumerate()
                .map(|(i, &error_code)| OffsetFetchResponsePartition {
                    partition_index: i as PartitionId,
                    committed_offset: 42,
                    committed_leader_epoch: 7,
                    metadata: None,
                    error_code,
                })
                .collect(),
        }],
        error_code: ErrorCode::None,
    }
}

/// An `UNSTABLE_OFFSET_COMMIT` partition must be noticed: a missing entry
/// would read as "never committed" and send the partition through
/// `auto_offset_reset`.
#[test]
fn an_unstable_offset_is_reported_rather_than_dropped() {
    let response = offset_fetch_response(&[ErrorCode::None, ErrorCode::UnstableOffsetCommit]);
    assert_eq!(first_unstable_offset(&response), Some(("orders", 1)));
    let clean = offset_fetch_response(&[ErrorCode::None, ErrorCode::UnknownTopicOrPartition]);
    assert_eq!(first_unstable_offset(&clean), None);
}

#[test]
fn the_unstable_offset_retry_is_bounded_and_the_error_is_retriable() {
    assert!((2..=10).contains(&UNSTABLE_OFFSET_MAX_ATTEMPTS));
    assert!(ErrorCode::UnstableOffsetCommit.is_retriable());
}

/// A `JoinGroup` is given the group's rebalance window, not the ordinary
/// request budget: the coordinator holds it until the rebalance converges.
#[test]
fn the_join_timeout_covers_the_rebalance_window() {
    let c = coordinator(S::Range);
    assert!(c.join_group_timeout() > Duration::from_secs(30));
    let defaults = crate::consumer::config::ConsumerConfig::default();
    assert!(defaults.max_poll_interval > Duration::from_secs(30));
}

// ── subscription encoding ───────────────────────────────────────────────

fn round_trip(subscription: &ConsumerProtocolSubscription) -> ConsumerProtocolSubscription {
    let mut buf = BytesMut::new();
    encode_consumer_protocol_subscription(subscription, &mut buf).unwrap();
    decode_consumer_protocol_subscription(&buf.freeze()).unwrap()
}

#[test]
fn an_eager_member_subscribes_at_v0() {
    let c = coordinator(S::Range);
    let topics = vec!["topic1".to_string(), "topic2".to_string()];
    let subscription = c.build_subscription(&topics, &HashMap::new(), -1);
    assert_eq!(subscription.version, 0);
    let decoded = round_trip(&subscription);
    assert_eq!(decoded.topics, topics);
    assert!(decoded.owned_partitions.is_empty());
}

/// Cooperative members report owned partitions and the generation they
/// owned them in, so the leader can settle a double claim.
#[test]
fn a_cooperative_member_reports_owned_partitions_and_its_generation() {
    let c = coordinator(S::CooperativeSticky);
    let topics = vec!["topic1".to_string(), "topic2".to_string()];
    let mut owned = HashMap::new();
    owned.insert("topic1".to_string(), vec![2, 0, 1]);
    owned.insert("topic2".to_string(), vec![0]);
    let subscription = c.build_subscription(&topics, &owned, 7);
    assert_eq!(subscription.version, CONSUMER_PROTOCOL_V2);
    let decoded = round_trip(&subscription);
    assert_eq!(decoded.generation_id, 7);
    assert_eq!(
        decoded.owned_partitions,
        vec![
            ConsumerProtocolTopicPartitions {
                topic: "topic1".to_string(),
                partitions: vec![0, 1, 2],
            },
            ConsumerProtocolTopicPartitions {
                topic: "topic2".to_string(),
                partitions: vec![0],
            },
        ]
    );
}

#[test]
fn a_configured_rack_reaches_the_group_leader() {
    let c = coordinator(S::Range).with_client_rack(Some("us-east-1a".to_string()));
    let subscription = c.build_subscription(&["topic1".to_string()], &HashMap::new(), -1);
    assert_eq!(subscription.version, CONSUMER_PROTOCOL_V3);
    assert_eq!(
        round_trip(&subscription).rack_id.as_deref(),
        Some("us-east-1a")
    );
}

#[test]
fn a_truncated_subscription_blob_is_an_error() {
    assert!(decode_consumer_protocol_subscription(&Bytes::from_static(&[0, 0])).is_err());
    let empty = decode_consumer_protocol_subscription(&Bytes::new()).unwrap();
    assert!(empty.topics.is_empty());
}

/// A partition count far larger than the bytes behind it fails on the first
/// missing byte instead of allocating for the claim.
#[test]
fn an_overcounted_partition_array_is_rejected_without_allocating_for_it() {
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    buf.put_i32(1);
    buf.put_i16(3);
    buf.put_slice(b"sub");
    buf.put_i32(-1);
    buf.put_i32(1);
    buf.put_i16(4);
    buf.put_slice(b"test");
    buf.put_i32(5_000);
    buf.put_i32(0);
    buf.put_i32(1);
    buf.put_i32(2);
    let err = decode_consumer_protocol_subscription(&buf.freeze()).unwrap_err();
    assert_eq!(
        err.protocol_error_kind(),
        Some(crate::error::ProtocolErrorKind::TruncatedFrame)
    );
}

// ── protocol negotiation ────────────────────────────────────────────────

#[test]
fn strategy_names_round_trip_and_only_cooperative_sticky_is_cooperative() {
    for s in [S::Range, S::RoundRobin, S::CooperativeSticky] {
        assert_eq!(S::from_protocol_name(s.protocol_name()), Some(s));
    }
    assert_eq!(
        S::from_protocol_name("sticky"),
        None,
        "the eager sticky assignor is gone"
    );
    assert!(!S::Range.is_cooperative());
    assert!(!S::RoundRobin.is_cooperative());
    assert!(S::CooperativeSticky.is_cooperative());
}

/// The coordinator's choice wins over this member's preference.
#[test]
fn the_negotiated_strategy_follows_the_coordinator() {
    let c = coordinator(S::Range).with_assignor_strategies(vec![S::CooperativeSticky, S::Range]);
    assert!(c.is_cooperative());
    c.latch_negotiated_strategy("range");
    assert_eq!(c.negotiated_strategy(), S::Range);
    assert!(!c.is_cooperative());
    c.latch_negotiated_strategy("something-we-never-advertised");
    assert_eq!(c.negotiated_strategy(), S::Range);
    let unchanged = coordinator(S::RoundRobin).with_assignor_strategies(vec![]);
    assert_eq!(unchanged.negotiated_strategy(), S::RoundRobin);
}

// ── poll interval ───────────────────────────────────────────────────────

#[test]
fn the_poll_tracker_latches_expiry_once_and_resets() {
    let t = PollTracker::new(Duration::from_millis(20));
    assert!(!t.is_expired());
    std::thread::sleep(Duration::from_millis(40));
    assert!(t.is_expired());
    assert!(t.mark_exceeded());
    assert!(!t.mark_exceeded());
    assert!(t.exceeded());
    t.reset();
    assert!(!t.exceeded());
    assert!(!t.is_expired());
}

#[test]
fn rejoining_after_expiry_clears_the_latch_and_starts_over() {
    let c = coordinator(S::Range).with_group_protocol(GroupProtocol::Consumer);
    c.member_epoch.store(9, Ordering::Release);
    c.inner.write().state = GroupState::Stable;
    c.poll_tracker.mark_exceeded();
    assert!(c.poll_interval_exceeded());
    c.rejoin_after_expiry();
    assert!(!c.poll_interval_exceeded());
    assert_eq!(c.member_epoch.load(Ordering::Acquire), 0);
    assert!(c.needs_rejoin());
}

// ── losses ──────────────────────────────────────────────────────────────

#[test]
fn a_lost_member_is_reported_once() {
    let c = coordinator(S::Range);
    c.inner.write().member_id = "m".to_string();
    c.inner.write().state = GroupState::Stable;
    c.heartbeat.signal_member_invalidated();
    assert!(c.take_lost());
    assert!(!c.take_lost());
    assert!(c.member_id().is_empty(), "a classic member re-registers");
    assert!(c.needs_rejoin());
}

#[test]
fn a_fenced_kip848_member_keeps_its_id_and_rejoins_at_epoch_zero() {
    let c = coordinator(S::Range).with_group_protocol(GroupProtocol::Consumer);
    c.inner.write().member_id = "m".to_string();
    c.member_epoch.store(7, Ordering::Release);
    c.heartbeat.signal_member_invalidated();
    assert!(c.take_lost());
    assert_eq!(c.member_id(), "m");
    assert_eq!(c.member_epoch.load(Ordering::Acquire), 0);
    assert!(c.owned_assignment.read().is_empty());
}

// ── group metadata ──────────────────────────────────────────────────────

#[test]
fn group_metadata_reports_the_generation_or_the_member_epoch() {
    let c = coordinator(S::Range);
    assert!(
        c.group_metadata().is_none(),
        "no identity before the first join"
    );
    {
        let mut inner = c.inner.write();
        inner.member_id = "member-7".to_string();
        inner.generation_id = 12;
    }
    let m = c.group_metadata().unwrap();
    assert_eq!(m.generation_id(), 12);
    assert_eq!(m.member_id(), "member-7");

    let k = coordinator(S::Range).with_group_protocol(GroupProtocol::Consumer);
    k.inner.write().member_id = "member-7".to_string();
    k.member_epoch.store(99, Ordering::Release);
    assert_eq!(k.group_metadata().unwrap().generation_id(), 99);
}

// ── static membership (KIP-345) ─────────────────────────────────────────

#[tokio::test]
async fn a_static_member_leaves_locally_and_keeps_its_member_id() {
    let c = coordinator(S::Range).with_group_instance_id(Some("inst-1".to_string()));
    {
        let mut inner = c.inner.write();
        inner.member_id = "member-1".to_string();
        inner.generation_id = 5;
        inner.state = GroupState::Stable;
    }
    c.topic_names_cache
        .write()
        .insert([1u8; 16], "t".to_string());
    // No broker is reachable: completing proves no LeaveGroup was attempted.
    c.leave_group().await.unwrap();
    let inner = c.inner.read();
    assert_eq!(inner.member_id, "member-1");
    assert_eq!(inner.state, GroupState::Unjoined);
    assert_eq!(inner.generation_id, -1);
    assert!(c.topic_names_cache.read().is_empty());
}

// ── KIP-848 owned vs target assignment ──────────────────────────────────

#[test]
fn the_owned_assignment_advances_only_when_acknowledged() {
    let c = coordinator(S::Range).with_group_protocol(GroupProtocol::Consumer);
    let tp = ConsumerGroupTopicPartitions {
        topic_id: [7u8; 16],
        partitions: vec![0, 1],
    };
    *c.target_assignment.write() = vec![tp.clone()];
    assert!(c.owned_assignment.read().is_empty());
    c.acknowledge_assignment();
    assert_eq!(*c.owned_assignment.read(), vec![tp]);
}

#[test]
fn a_fatal_error_is_reported_once() {
    let c = coordinator(S::Range);
    assert!(c.take_fatal_error().is_none());
    *c.fatal_error.lock() = Some(KrafkaError::illegal_state("boom"));
    assert!(c.take_fatal_error().is_some());
    assert!(c.take_fatal_error().is_none());
}

// ── join rounds ─────────────────────────────────────────────────────────

/// A join that finishes after the member left is discarded, not applied.
#[tokio::test]
async fn a_join_outcome_from_before_a_leave_is_discarded() {
    let c = Arc::new(coordinator(S::Range));
    c.spawn_rejoin(true);
    assert!(c.rejoin_in_flight());
    c.leave_group().await.unwrap();
    c.await_rejoin(Duration::from_secs(30)).await;
    assert!(!c.rejoin_in_flight());
    assert!(c.take_pending_rebalance().is_none());
}

/// A finished join parks its outcome for the next poll, even when nobody
/// waited for it.
#[tokio::test]
async fn a_join_outcome_is_parked_for_the_next_poll() {
    let c = Arc::new(coordinator(S::Range));
    c.spawn_rejoin(true);
    c.await_rejoin(Duration::from_secs(30)).await;
    assert!(
        c.take_pending_rebalance().is_some_and(|r| r.is_err()),
        "no broker: the outcome is an error, but it is delivered"
    );
    assert!(c.take_pending_rebalance().is_none(), "exactly once");
}
