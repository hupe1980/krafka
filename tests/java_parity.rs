//! Java client parity against the fake broker: leave or remain on close
//! (KIP-1092), offset reset by duration (KIP-1106), the KIP-848 server
//! assignor, the earliest-pending-upload offset spec (KIP-1023) and
//! node-targeted feature description (KIP-1160).
//!
//! Run: `cargo test --features test-broker --test java_parity`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use krafka::admin::{DescribeFeaturesOptions, ListOffsetsOptions, OffsetSpec, TopicPartition};
use krafka::consumer::{AutoOffsetReset, Consumer, GroupProtocol, OffsetAndMetadata};
use krafka::testing::{ApiKey, FakeBroker};
use krafka::{CloseOptions, GroupMembershipOperation, Kafka, KrafkaError};

const T: Duration = Duration::from_secs(2);
const HOUR_MS: i64 = 3_600_000;

async fn kafka(broker: &FakeBroker) -> Kafka {
    Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Produce one record per age, oldest first, timestamped `now - age`.
async fn produce_aged(broker: &FakeBroker, topic: &str, ages_hours: &[i64]) {
    let producer = kafka(broker).await.producer().build().await.unwrap();
    let now = now_ms();
    for age in ages_hours {
        let _ = producer
            .send(
                krafka::Record::new(topic, bytes::Bytes::from(format!("{age}h")))
                    .timestamp(now - age * HOUR_MS),
            )
            .await
            .unwrap();
    }
    let _ = producer.close().await;
}

/// Poll until the consumer has an assignment; returns the offsets received
/// meanwhile.
async fn wait_assigned(consumer: &Consumer) -> Vec<i64> {
    let mut seen = Vec::new();
    for _ in 0..50 {
        if let Ok(records) = consumer.poll(Duration::from_millis(200)).await {
            seen.extend(records.into_iter().map(|r| r.offset));
        }
        if !consumer.assignment().await.is_empty() {
            return seen;
        }
    }
    panic!("never assigned");
}

async fn drain(consumer: &Consumer, polls: usize) -> Vec<i64> {
    let mut seen = Vec::new();
    for _ in 0..polls {
        seen.extend(
            consumer
                .poll(Duration::from_millis(200))
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.offset),
        );
    }
    seen
}

// ── KIP-1092: leave or remain on close ──────────────────────────────────

/// What a close sent, as the coordinator saw it.
#[derive(Debug, PartialEq, Eq)]
enum Sent {
    /// Nothing.
    Nothing,
    /// A classic `LeaveGroup` naming this instance id.
    LeaveGroup(Option<String>),
    /// A KIP-848 heartbeat at this negative epoch.
    LeaveHeartbeat(i32),
}

async fn close_and_record(
    protocol: GroupProtocol,
    instance_id: Option<&str>,
    operation: Option<GroupMembershipOperation>,
    group: &str,
) -> Sent {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let mut builder = kafka(&broker)
        .await
        .consumer(group)
        .group_protocol(protocol)
        .enable_auto_commit(false);
    if let Some(id) = instance_id {
        builder = builder.group_instance_id(id);
    }
    let consumer = builder.build().await.unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    let _ = wait_assigned(&consumer).await;

    match operation {
        None => consumer.close().await.unwrap(),
        Some(op) => consumer
            .close_with(CloseOptions::new().group_membership_operation(op))
            .await
            .unwrap(),
    }

    let leaves = broker.leave_group_members();
    let leave_heartbeats: Vec<i32> = broker
        .consumer_group_heartbeats()
        .into_iter()
        .filter(|h| h.member_epoch < 0)
        .map(|h| h.member_epoch)
        .collect();
    match (leaves.as_slice(), leave_heartbeats.as_slice()) {
        ([], []) => Sent::Nothing,
        ([leave], []) => {
            assert_eq!(leave.group_id, group);
            Sent::LeaveGroup(leave.group_instance_id.clone())
        }
        ([], [epoch]) => Sent::LeaveHeartbeat(*epoch),
        other => panic!("{group}: more than one leave: {other:?}"),
    }
}

/// FR-006's table, one cell per (membership, option); `None` is plain
/// `close()`, which must equal `Default`.
fn expected(protocol: GroupProtocol, is_static: bool, op: GroupMembershipOperation) -> Sent {
    use GroupMembershipOperation as Op;
    let instance = is_static.then(|| "instance-1".to_string());
    match (protocol, op, is_static) {
        (_, Op::RemainInGroup, _) => Sent::Nothing,
        (GroupProtocol::Classic, Op::Default, true) => Sent::Nothing,
        (GroupProtocol::Classic, _, _) => Sent::LeaveGroup(instance),
        (_, Op::Default, true) => Sent::LeaveHeartbeat(-2),
        _ => Sent::LeaveHeartbeat(-1),
    }
}

async fn close_matrix(protocol: GroupProtocol, tag: &str) {
    use GroupMembershipOperation as Op;
    for is_static in [false, true] {
        let instance = is_static.then_some("instance-1");
        for op in [
            None,
            Some(Op::Default),
            Some(Op::LeaveGroup),
            Some(Op::RemainInGroup),
        ] {
            let group = format!("g-close-{tag}-{is_static}-{op:?}");
            let sent = close_and_record(protocol, instance, op, &group).await;
            assert_eq!(
                sent,
                expected(protocol, is_static, op.unwrap_or_default()),
                "{protocol:?}, static={is_static}, {op:?}"
            );
        }
    }
}

#[tokio::test]
async fn classic_close_sends_the_leave_the_option_asks_for() {
    close_matrix(GroupProtocol::Classic, "classic").await;
}

#[tokio::test]
async fn kip848_close_sends_the_leave_the_option_asks_for() {
    close_matrix(GroupProtocol::Consumer, "kip848").await;
}

/// A consumer without a group accepts any option and sends nothing.
#[tokio::test]
async fn close_options_without_a_group_are_a_no_op() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = kafka(&broker)
        .await
        .consumer_without_group()
        .build()
        .await
        .unwrap();
    consumer.assign("events", vec![0]).await.unwrap();
    consumer
        .close_with(
            CloseOptions::new().group_membership_operation(GroupMembershipOperation::LeaveGroup),
        )
        .await
        .unwrap();
    assert!(broker.leave_group_members().is_empty());
    assert_eq!(broker.request_count(ApiKey::ConsumerGroupHeartbeat), 0);
}

// ── KIP-1106: reset by duration ─────────────────────────────────────────

const AGES_48H: [i64; 8] = [47, 41, 35, 29, 23, 17, 11, 5];

/// The timestamps the consumer looked up, i.e. every non-sentinel lookup.
fn timestamp_lookups(broker: &FakeBroker) -> Vec<i64> {
    broker
        .list_offsets_lookups()
        .into_iter()
        .map(|l| l.timestamp)
        .filter(|ts| *ts >= 0)
        .collect()
}

/// A fresh group with a 24 h reset receives exactly the records of the last
/// 24 h of a 48 h log, and looked up `now - 24 h`.
///
/// Negative control: mapping `ByDuration` to `Latest` receives nothing.
#[tokio::test]
async fn a_fresh_group_starts_24_hours_back() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce_aged(&broker, "events", &AGES_48H).await;

    let consumer = kafka(&broker)
        .await
        .consumer("g-by-duration")
        .auto_offset_reset(AutoOffsetReset::ByDuration(Duration::from_secs(24 * 3600)))
        .build()
        .await
        .unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    let before = now_ms();
    let mut seen = wait_assigned(&consumer).await;
    seen.extend(drain(&consumer, 5).await);
    seen.sort_unstable();
    assert_eq!(
        seen,
        vec![4, 5, 6, 7],
        "exactly the records younger than 24 h"
    );

    let lookups = timestamp_lookups(&broker);
    assert_eq!(lookups.len(), 1, "one timestamp lookup: {lookups:?}");
    let expected = before - 24 * HOUR_MS;
    assert!(
        (lookups[0] - expected).abs() < 60_000,
        "looked up {} instead of now - 24 h ({expected})",
        lookups[0]
    );
    let _ = consumer.close().await;
}

/// No record newer than the duration: the position is the log end, and a
/// record written afterwards is the first one received.
#[tokio::test]
async fn by_duration_with_nothing_newer_starts_at_the_log_end() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce_aged(&broker, "events", &[47, 30]).await;

    let consumer = kafka(&broker)
        .await
        .consumer("g-by-duration-end")
        .auto_offset_reset(AutoOffsetReset::ByDuration(Duration::from_secs(24 * 3600)))
        .build()
        .await
        .unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    assert!(wait_assigned(&consumer).await.is_empty());
    assert!(drain(&consumer, 2).await.is_empty());
    assert_eq!(consumer.position("events", 0).await, Some(2));

    produce_aged(&broker, "events", &[0]).await;
    assert_eq!(drain(&consumer, 5).await, vec![2]);
    let _ = consumer.close().await;
}

/// A committed offset wins: the reset is not consulted.
#[tokio::test]
async fn a_committed_offset_is_not_reset_by_duration() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce_aged(&broker, "events", &AGES_48H).await;

    let k = kafka(&broker).await;
    let committer = k
        .consumer("g-by-duration-committed")
        .enable_auto_commit(false)
        .build()
        .await
        .unwrap();
    committer.subscribe(["events"]).await.unwrap();
    let _ = wait_assigned(&committer).await;
    let mut offsets = ahash::AHashMap::new();
    offsets.insert(
        krafka::consumer::TopicPartition::new("events", 0),
        OffsetAndMetadata::new(1),
    );
    committer.commit_offsets(&offsets).await.unwrap();
    let _ = committer.close().await;

    let consumer = k
        .consumer("g-by-duration-committed")
        .auto_offset_reset(AutoOffsetReset::ByDuration(Duration::from_secs(24 * 3600)))
        .build()
        .await
        .unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    let mut seen = wait_assigned(&consumer).await;
    seen.extend(drain(&consumer, 5).await);
    seen.sort_unstable();
    assert_eq!(seen, (1..8).collect::<Vec<_>>());
    assert!(timestamp_lookups(&broker).is_empty());
    let _ = consumer.close().await;
}

/// An out-of-range position resets by duration too.
#[tokio::test]
async fn an_out_of_range_position_resets_by_duration() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce_aged(&broker, "events", &AGES_48H).await;

    let consumer = kafka(&broker)
        .await
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::ByDuration(Duration::from_secs(24 * 3600)))
        .build()
        .await
        .unwrap();
    consumer.assign("events", vec![0]).await.unwrap();
    consumer.seek("events", 0, 1_000).await.unwrap();
    let mut seen = drain(&consumer, 10).await;
    seen.sort_unstable();
    assert_eq!(seen, vec![4, 5, 6, 7]);
    assert_eq!(timestamp_lookups(&broker).len(), 1);
    let _ = consumer.close().await;
}

// ── KIP-848 server assignor ─────────────────────────────────────────────

/// The joining heartbeat, and every heartbeat carrying the subscription,
/// carries the configured assignor; the others do not.
///
/// Negative control: sending `None` fails the first assertion.
#[tokio::test]
async fn the_joining_heartbeat_names_the_server_assignor() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = kafka(&broker)
        .await
        .consumer("g-assignor")
        .group_protocol(GroupProtocol::Consumer)
        .group_remote_assignor("range")
        .build()
        .await
        .unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    let _ = wait_assigned(&consumer).await;

    let heartbeats = broker.consumer_group_heartbeats();
    let join = heartbeats.first().expect("a joining heartbeat");
    assert_eq!(join.member_epoch, 0);
    assert_eq!(join.server_assignor.as_deref(), Some("range"));
    for h in &heartbeats {
        assert_eq!(
            h.server_assignor.is_some(),
            h.full,
            "the assignor travels with the subscription: {h:?}"
        );
    }
    let _ = consumer.close().await;
}

#[tokio::test]
async fn a_server_assignor_needs_the_consumer_protocol() {
    let broker = FakeBroker::start().await.unwrap();
    let k = kafka(&broker).await;
    let err = k
        .consumer("g")
        .group_remote_assignor("range")
        .build()
        .await
        .err()
        .expect("build fails");
    assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
    assert!(err.to_string().contains("GroupProtocol::Consumer"), "{err}");
    let err = k
        .consumer("g")
        .group_protocol(GroupProtocol::Consumer)
        .group_remote_assignor("")
        .build()
        .await
        .err()
        .expect("build fails");
    assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
}

/// A name the coordinator does not know surfaces as a non-retriable error
/// naming it.
#[tokio::test]
async fn an_unsupported_server_assignor_is_named_and_not_retried() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = kafka(&broker)
        .await
        .consumer("g-assignor-bad")
        .group_protocol(GroupProtocol::Consumer)
        .group_remote_assignor("no-such-assignor")
        .build()
        .await
        .unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    let mut error = None;
    for _ in 0..20 {
        if let Err(e) = consumer.poll(Duration::from_millis(200)).await {
            error = Some(e);
            break;
        }
    }
    let error = error.expect("the join fails");
    assert!(
        matches!(
            error,
            KrafkaError::Broker {
                code: krafka::error::ErrorCode::UnsupportedAssignor,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(error.to_string().contains("no-such-assignor"), "{error}");
    assert!(!error.is_retriable());
    let _ = consumer.close().await;
}

// ── KIP-1023: earliest pending upload ───────────────────────────────────

/// Negative control: mapping the spec to -5 fails the timestamp assertion.
#[tokio::test]
async fn earliest_pending_upload_sends_minus_six_on_v11() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("tiered", 1);
    let admin = kafka(&broker).await.admin();
    let tp = TopicPartition::new("tiered", 0);
    let result = admin
        .list_offsets(
            [(tp.clone(), OffsetSpec::EarliestPendingUpload)],
            ListOffsetsOptions::default(),
        )
        .await
        .unwrap();
    assert!(result[&tp].is_ok(), "{:?}", result[&tp]);
    let lookups = broker.list_offsets_lookups();
    assert_eq!(lookups.len(), 1);
    assert_eq!(lookups[0].timestamp, -6);
    assert_eq!(lookups[0].api_version, 11);
}

#[tokio::test]
async fn earliest_pending_upload_fails_below_v11_without_sending() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("tiered", 1);
    broker.set_api_versions(ApiKey::ListOffsets, 5, 10);
    let admin = kafka(&broker).await.admin();
    let tp = TopicPartition::new("tiered", 0);
    let result = admin
        .list_offsets(
            [(tp.clone(), OffsetSpec::EarliestPendingUpload)],
            ListOffsetsOptions::default(),
        )
        .await
        .unwrap();
    let err = result[&tp].as_ref().unwrap_err();
    assert!(err.to_string().contains("v11"), "{err}");
    assert!(broker.list_offsets_lookups().is_empty(), "nothing is sent");
}

// ── KIP-1160: node-targeted feature description ─────────────────────────

/// Negative control: routing to any broker sends `ApiVersions` to an
/// already-connected node instead of node 2.
#[tokio::test]
async fn describe_features_for_a_node_asks_only_that_node() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    let admin = kafka(&broker).await.admin();
    broker.clear_requests();
    admin
        .describe_features(DescribeFeaturesOptions::default().node_id(2))
        .await
        .unwrap();
    let nodes = broker.request_nodes(ApiKey::ApiVersions);
    assert!(!nodes.is_empty());
    assert!(
        nodes.iter().all(|n| *n == 2),
        "ApiVersions reached {nodes:?}"
    );
}

#[tokio::test]
async fn describe_features_for_an_unknown_node_fails() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    let admin = kafka(&broker).await.admin();
    let err = admin
        .describe_features(
            DescribeFeaturesOptions::default()
                .node_id(7)
                .timeout(Duration::from_secs(3)),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains('7'), "{err}");
}

/// `UNSUPPORTED_ASSIGNOR` on a later heartbeat stops the member with the
/// same non-retriable error instead of being retried.
///
/// Negative control: treating it as retriable in the heartbeat task leaves
/// every poll succeeding.
#[tokio::test]
async fn an_unsupported_assignor_on_a_later_heartbeat_is_fatal() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = kafka(&broker)
        .await
        .consumer("g-assignor-later")
        .group_protocol(GroupProtocol::Consumer)
        .group_remote_assignor("range")
        .build()
        .await
        .unwrap();
    consumer.subscribe(["events"]).await.unwrap();
    let _ = wait_assigned(&consumer).await;
    broker.on(ApiKey::ConsumerGroupHeartbeat, |_| {
        krafka::testing::Control::Error(krafka::error::ErrorCode::UnsupportedAssignor)
    });
    let mut error = None;
    for _ in 0..40 {
        if let Err(e) = consumer.poll(Duration::from_millis(200)).await {
            error = Some(e);
            break;
        }
    }
    let error = error.expect("the member stops");
    assert!(
        matches!(
            error,
            KrafkaError::Broker {
                code: krafka::error::ErrorCode::UnsupportedAssignor,
                ..
            }
        ),
        "{error:?}"
    );
    assert!(error.to_string().contains("range"), "{error}");
    broker.clear_hooks();
    let _ = consumer.close().await;
}
