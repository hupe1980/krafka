//! Subscribing by topic pattern, against the fake broker: client-side
//! patterns group-less and under both group protocols, broker-side
//! (KIP-848) patterns, topic churn, switching subscription kinds, and the
//! failure paths of each.
//!
//! Run: `cargo test --features test-broker --test topic_patterns`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use krafka::consumer::{
    AutoOffsetReset, Consumer, ConsumerRebalanceListener, GroupProtocol,
    PartitionAssignmentStrategy, SubscriptionPattern, TopicPartition, TopicPattern,
};
use krafka::error::ErrorCode;
use krafka::testing::{ApiKey, Control, FakeBroker};
use krafka::{Kafka, KrafkaError};

const T: Duration = Duration::from_secs(2);
/// Short enough that a test sees several pattern evaluations.
const MAX_AGE: Duration = Duration::from_millis(300);
const SETTLE: Duration = Duration::from_secs(20);

async fn kafka(broker: &FakeBroker) -> Kafka {
    Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .metadata_max_age(MAX_AGE)
        .connect()
        .await
        .unwrap()
}

async fn produce(broker: &FakeBroker, topic: &str, values: &[&str]) {
    let producer = kafka(broker).await.producer().build().await.unwrap();
    for value in values {
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(value.as_bytes()),
            ))
            .await
            .unwrap();
    }
    let _ = producer.close().await;
}

/// One listener callback and the partitions it named.
type Callback = (&'static str, Vec<TopicPartition>);

/// Records each callback with the topics it named.
#[derive(Default, Clone)]
struct Events(Arc<Mutex<Vec<Callback>>>);

impl Events {
    fn push(&self, kind: &'static str, p: &[TopicPartition]) {
        self.0.lock().unwrap().push((kind, p.to_vec()));
    }
    fn topics(&self, kind: &str) -> HashSet<String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == kind)
            .flat_map(|(_, p)| p.iter().map(|tp| tp.topic.clone()))
            .collect()
    }
    fn count(&self, kind: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == kind)
            .count()
    }
}

impl ConsumerRebalanceListener for Events {
    async fn on_partitions_assigned(&self, p: &[TopicPartition]) {
        self.push("assigned", p);
    }
    async fn on_partitions_revoked(&self, p: &[TopicPartition]) {
        self.push("revoked", p);
    }
    async fn on_partitions_lost(&self, p: &[TopicPartition]) {
        self.push("lost", p);
    }
}

async fn group_member(
    broker: &FakeBroker,
    group: &str,
    protocol: GroupProtocol,
    strategy: PartitionAssignmentStrategy,
    events: &Events,
) -> Consumer {
    kafka(broker)
        .await
        .consumer(group)
        .group_protocol(protocol)
        .partition_assignment_strategy(strategy)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .session_timeout(Duration::from_secs(10))
        .heartbeat_interval(Duration::from_millis(500))
        .rebalance_listener(events.clone())
        .build()
        .await
        .unwrap()
}

async fn group_less(broker: &FakeBroker) -> Consumer {
    kafka(broker)
        .await
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap()
}

fn assigned_topics(assignment: &HashMap<String, Vec<i32>>) -> HashSet<String> {
    assignment.keys().cloned().collect()
}

/// Poll every consumer until `done` holds, failing the test on any `poll`
/// error or when `SETTLE` runs out. Returns the values each consumer saw.
async fn poll_until(
    consumers: &[&Consumer],
    mut done: impl AsyncFnMut(&[Vec<String>]) -> bool,
    what: &str,
) -> Vec<Vec<String>> {
    let mut seen = vec![Vec::new(); consumers.len()];
    let deadline = Instant::now() + SETTLE;
    loop {
        for (i, consumer) in consumers.iter().enumerate() {
            let records = consumer
                .poll(Duration::from_millis(100))
                .await
                .unwrap_or_else(|e| panic!("poll failed while waiting for {what}: {e}"));
            seen[i].extend(
                records.iter().map(|r| {
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into()
                }),
            );
        }
        if done(&seen).await {
            return seen;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
    }
}

fn all(seen: &[Vec<String>]) -> HashSet<String> {
    seen.iter().flatten().cloned().collect()
}

// ── US1: client-side patterns ────────────────────────────────────────────

/// A group-less consumer follows the pattern: only matching topics are
/// assigned, a topic created later is consumed, a deleted one leaves the
/// assignment, and no `poll()` fails on the way.
#[tokio::test]
async fn a_group_less_pattern_follows_topic_creation_and_deletion() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 2);
    broker.create_topic("other", 1);
    produce(&broker, "pat-a", &["a0"]).await;
    produce(&broker, "other", &["o0"]).await;

    let consumer = group_less(&broker).await;
    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    assert_eq!(
        assigned_topics(&consumer.assignment().await),
        HashSet::from(["pat-a".to_string()])
    );

    broker.create_topic("pat-b", 1);
    produce(&broker, "pat-b", &["b0"]).await;
    let seen = poll_until(
        &[&consumer],
        async |seen| all(seen).contains("a0") && all(seen).contains("b0"),
        "pat-a and pat-b records",
    )
    .await;
    assert!(
        !all(&seen).contains("o0"),
        "a non-matching topic was consumed"
    );

    broker.delete_topic("pat-a");
    poll_until(
        &[&consumer],
        async |_| !consumer.assignment().await.contains_key("pat-a"),
        "pat-a to leave the assignment",
    )
    .await;
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["pat-b".to_string()])
    );
}

/// A classic member on a pattern: a topic created later is assigned and
/// consumed; a deleted one is revoked through the listener with no `poll()`
/// error. (The fake coordinator holds one member at a time; splitting a new
/// topic across members is checked against a real broker in
/// `integration_tests`.)
#[tokio::test]
async fn a_classic_member_follows_topic_creation_and_deletion() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 2);
    broker.create_topic("other", 2);
    let events = Events::default();
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Classic,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();

    broker.create_topic("pat-b", 4);
    let values: Vec<String> = (0..20).map(|i| format!("b{i}")).collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    produce(&broker, "pat-b", &refs).await;
    produce(&broker, "other", &["o0", "o1"]).await;

    let seen = poll_until(
        &[&consumer],
        async |seen| values.iter().all(|v| all(seen).contains(v)),
        "every pat-b record",
    )
    .await;
    assert!(
        !all(&seen).contains("o0"),
        "a non-matching topic was consumed"
    );
    assert_eq!(
        consumer.assignment().await.get("pat-b").map(Vec::len),
        Some(4)
    );
    assert!(events.topics("assigned").contains("pat-b"));

    broker.delete_topic("pat-a");
    poll_until(
        &[&consumer],
        async |_| !consumer.assignment().await.contains_key("pat-a"),
        "pat-a to be revoked",
    )
    .await;
    assert!(
        events.topics("revoked").contains("pat-a"),
        "the deleted topic was not revoked through the listener"
    );
    assert_eq!(events.count("lost"), 0);
}

/// Internal topics never match, and matching is against the whole name.
#[tokio::test]
async fn internal_topics_and_partial_names_do_not_match() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("__consumer_offsets", 1);
    broker.create_topic("pat-a", 1);
    broker.create_topic("xpat-a", 1);

    let consumer = group_less(&broker).await;
    consumer
        .subscribe_pattern(TopicPattern::new(".*").unwrap())
        .await
        .unwrap();
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["pat-a".to_string(), "xpat-a".to_string()])
    );

    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["pat-a".to_string()])
    );
    assert_eq!(
        assigned_topics(&consumer.assignment().await),
        HashSet::from(["pat-a".to_string()])
    );
}

/// A classic cooperative member switching from names to a pattern rejoins
/// without leaving the group and keeps the partitions both cover.
#[tokio::test]
async fn a_classic_switch_from_names_to_a_pattern_keeps_shared_partitions() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 2);
    broker.create_topic("pat-b", 2);
    let events = Events::default();
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Classic,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    consumer.subscribe(["pat-a"]).await.unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.get("pat-a").map(Vec::len) == Some(2),
        "pat-a assigned",
    )
    .await;
    let joins = broker.request_count(ApiKey::JoinGroup);

    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.get("pat-b").map(Vec::len) == Some(2),
        "pat-b assigned",
    )
    .await;

    assert!(
        broker.leave_group_members().is_empty(),
        "the switch left the group"
    );
    assert!(broker.request_count(ApiKey::JoinGroup) > joins, "no rejoin");
    assert!(
        !events.topics("revoked").contains("pat-a"),
        "pat-a was revoked although both subscriptions cover it"
    );
    assert_eq!(
        consumer.assignment().await.get("pat-a").map(Vec::len),
        Some(2)
    );
}

/// An invalid pattern never reaches the consumer, so the subscription in
/// force stays.
#[tokio::test]
async fn an_invalid_pattern_leaves_the_subscription_unchanged() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("a", 1);
    let consumer = group_less(&broker).await;
    consumer.subscribe(["a"]).await.unwrap();

    let err = TopicPattern::new("(").unwrap_err();
    assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["a".to_string()])
    );
}

/// A failing metadata refresh keeps the matched set and the assignment: no
/// rejoin, no `poll()` error.
#[tokio::test]
async fn a_failed_pattern_evaluation_changes_nothing() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 2);
    let events = Events::default();
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Classic,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.get("pat-a").map(Vec::len) == Some(2),
        "pat-a assigned",
    )
    .await;
    let joins = broker.request_count(ApiKey::JoinGroup);
    let metadata = broker.request_count(ApiKey::Metadata);

    broker.on(ApiKey::Metadata, |_| Control::Disconnect);
    let until = Instant::now() + MAX_AGE * 6;
    while Instant::now() < until {
        consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("a failed pattern evaluation must not fail poll()");
    }
    broker.clear_hooks();

    assert!(
        broker.request_count(ApiKey::Metadata) > metadata,
        "no evaluation was attempted while metadata failed"
    );
    assert_eq!(
        broker.request_count(ApiKey::JoinGroup),
        joins,
        "a failed evaluation rejoined"
    );
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["pat-a".to_string()])
    );
    assert_eq!(
        consumer.assignment().await.get("pat-a").map(Vec::len),
        Some(2)
    );
    assert_eq!(events.count("revoked") + events.count("lost"), 0);
}

/// A group-less manual assignment wins over a pattern that also matches the
/// topic.
#[tokio::test]
async fn a_pattern_does_not_widen_a_manual_assignment() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 3);
    broker.create_topic("pat-b", 1);
    let consumer = group_less(&broker).await;
    consumer.assign("pat-a", vec![0]).await.unwrap();
    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    for _ in 0..5 {
        consumer
            .poll(Duration::from_millis(MAX_AGE.as_millis() as u64))
            .await
            .unwrap();
    }
    let assignment = consumer.assignment().await;
    assert_eq!(assignment.get("pat-a"), Some(&vec![0]));
    assert_eq!(assignment.get("pat-b").map(Vec::len), Some(1));
}

/// A pattern matching nothing is a valid subscription that picks up a later
/// topic; `unsubscribe()` ends the evaluations.
#[tokio::test]
async fn an_empty_match_waits_and_unsubscribe_stops_evaluating() {
    let broker = FakeBroker::start().await.unwrap();
    let consumer = group_less(&broker).await;
    consumer
        .subscribe_pattern(TopicPattern::new("later-.*").unwrap())
        .await
        .unwrap();
    assert!(consumer.subscription().await.is_empty());
    assert!(
        consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap()
            .is_empty()
    );

    broker.create_topic("later-1", 1);
    produce(&broker, "later-1", &["l0"]).await;
    poll_until(
        &[&consumer],
        async |seen| all(seen).contains("l0"),
        "later-1's record",
    )
    .await;

    consumer.unsubscribe().await.unwrap();
    assert!(consumer.subscription().await.is_empty());
    let metadata = broker.request_count(ApiKey::Metadata);
    for _ in 0..4 {
        consumer
            .poll(Duration::from_millis(MAX_AGE.as_millis() as u64))
            .await
            .unwrap();
    }
    assert_eq!(
        broker.request_count(ApiKey::Metadata),
        metadata,
        "an unsubscribed consumer kept evaluating the pattern"
    );

    broker.create_topic("x", 1);
    consumer.subscribe(["x"]).await.unwrap();
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["x".to_string()])
    );
}

/// A member fenced while it switches to a pattern reports the loss once,
/// rejoins with the pattern's topics, and is assigned the new topic.
async fn a_member_fenced_mid_switch_recovers(protocol: GroupProtocol) {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 2);
    broker.create_topic("pat-b", 2);
    let events = Events::default();
    let consumer = group_member(
        &broker,
        "g",
        protocol,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    consumer.subscribe(["pat-a"]).await.unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.get("pat-a").map(Vec::len) == Some(2),
        "pat-a assigned",
    )
    .await;

    let (api, code) = match protocol {
        GroupProtocol::Consumer => (ApiKey::ConsumerGroupHeartbeat, ErrorCode::FencedMemberEpoch),
        _ => (ApiKey::Heartbeat, ErrorCode::UnknownMemberId),
    };
    let before = broker.request_count(api);
    broker.on_once(api, move |_| Control::Error(code));
    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    poll_until(
        &[&consumer],
        async |_| {
            let assignment = consumer.assignment().await;
            broker.request_count(api) > before
                && assignment.get("pat-a").map(Vec::len) == Some(2)
                && assignment.get("pat-b").map(Vec::len) == Some(2)
        },
        "both topics assigned after the fencing",
    )
    .await;
    assert!(
        events.count("lost") <= 1,
        "the loss was reported more than once"
    );
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["pat-a".to_string(), "pat-b".to_string()])
    );
}

#[tokio::test]
async fn a_classic_member_fenced_mid_switch_recovers() {
    a_member_fenced_mid_switch_recovers(GroupProtocol::Classic).await;
}

#[tokio::test]
async fn a_kip848_member_fenced_mid_switch_recovers() {
    a_member_fenced_mid_switch_recovers(GroupProtocol::Consumer).await;
}

// ── US3: a client-side pattern in a KIP-848 group ────────────────────────

/// A KIP-848 member switching from names to a client-side pattern keeps the
/// partitions both cover, is assigned a topic created later, and has a
/// deleted one revoked, with no `poll()` error.
#[tokio::test]
async fn a_kip848_member_follows_a_client_side_pattern() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 2);
    let events = Events::default();
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    consumer.subscribe(["pat-a"]).await.unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.get("pat-a").map(Vec::len) == Some(2),
        "pat-a assigned",
    )
    .await;

    consumer
        .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
        .await
        .unwrap();
    broker.create_topic("pat-b", 2);
    produce(&broker, "pat-b", &["b0"]).await;
    poll_until(
        &[&consumer],
        async |seen| all(seen).contains("b0"),
        "pat-b's record",
    )
    .await;
    assert!(
        broker
            .consumer_group_heartbeats()
            .iter()
            .all(|h| h.member_epoch != -1),
        "the switch left the group"
    );
    assert!(
        !events.topics("revoked").contains("pat-a"),
        "pat-a was revoked by the switch"
    );

    broker.delete_topic("pat-a");
    poll_until(
        &[&consumer],
        async |_| !consumer.assignment().await.contains_key("pat-a"),
        "pat-a to be revoked",
    )
    .await;
    assert!(events.topics("revoked").contains("pat-a"));
}

// ── US2: broker-side patterns (KIP-848) ──────────────────────────────────

fn broker_pattern(pattern: &str) -> SubscriptionPattern {
    SubscriptionPattern::new(pattern).unwrap()
}

/// A KIP-848 member on a broker-side pattern: its joining heartbeat carries
/// the pattern and an empty name list, and a topic created later is assigned
/// and consumed.
#[tokio::test]
async fn a_broker_pattern_is_sent_and_follows_new_topics() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 1);
    broker.create_topic("other", 1);
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &Events::default(),
    )
    .await;
    consumer
        .subscribe_broker_pattern(broker_pattern("pat-.*"))
        .await
        .unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.contains_key("pat-a"),
        "pat-a assigned by the coordinator",
    )
    .await;
    let join = broker
        .consumer_group_heartbeats()
        .into_iter()
        .find(|h| h.member_epoch == 0)
        .expect("a joining heartbeat");
    assert_eq!(join.subscribed_topic_regex.as_deref(), Some("pat-.*"));
    assert_eq!(join.subscribed_topic_names, Some(Vec::new()));

    broker.create_topic("pat-b", 1);
    produce(&broker, "pat-b", &["b0"]).await;
    let seen = poll_until(
        &[&consumer],
        async |seen| all(seen).contains("b0"),
        "pat-b's record",
    )
    .await;
    assert!(!consumer.assignment().await.contains_key("other"));
    assert!(!all(&seen).contains("o0"));
    assert_eq!(
        consumer.subscription().await,
        HashSet::from(["pat-a".to_string(), "pat-b".to_string()])
    );
}

/// Switching from a broker-side pattern to names clears the pattern with
/// `""` on the next full heartbeat, without leaving the group.
#[tokio::test]
async fn switching_from_a_broker_pattern_to_names_clears_it() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 1);
    broker.create_topic("pat-b", 1);
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &Events::default(),
    )
    .await;
    consumer
        .subscribe_broker_pattern(broker_pattern("pat-.*"))
        .await
        .unwrap();
    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.len() == 2,
        "both pattern topics assigned",
    )
    .await;

    consumer.subscribe(["pat-a"]).await.unwrap();
    poll_until(
        &[&consumer],
        async |_| {
            let assignment = consumer.assignment().await;
            assignment.contains_key("pat-a") && !assignment.contains_key("pat-b")
        },
        "only pat-a assigned after the switch",
    )
    .await;
    let heartbeats = broker.consumer_group_heartbeats();
    let clear = heartbeats
        .iter()
        .find(|h| h.subscribed_topic_regex.as_deref() == Some(""))
        .expect("a heartbeat clearing the pattern");
    assert_eq!(
        clear.subscribed_topic_names,
        Some(vec!["pat-a".to_string()])
    );
    assert!(
        heartbeats.iter().all(|h| h.member_epoch != -1),
        "the switch left the group"
    );
}

/// A pattern the coordinator rejects fails `poll()` with a non-retriable
/// error naming it.
#[tokio::test]
async fn an_invalid_broker_pattern_fails_poll_naming_it() {
    let broker = FakeBroker::start().await.unwrap();
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &Events::default(),
    )
    .await;
    consumer
        .subscribe_broker_pattern(broker_pattern("pat-("))
        .await
        .unwrap();
    let err = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect_err("the coordinator rejects the pattern");
    assert!(
        matches!(
            err,
            KrafkaError::Broker {
                code: ErrorCode::InvalidRegularExpression,
                ..
            }
        ),
        "{err:?}"
    );
    assert!(!err.is_retriable(), "{err:?}");
    assert!(err.to_string().contains("pat-("), "{err}");
}

/// A broker-side pattern needs a KIP-848 group; elsewhere the call fails and
/// changes nothing.
#[tokio::test]
async fn a_broker_pattern_is_refused_outside_a_kip848_group() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("a", 1);
    let classic = group_member(
        &broker,
        "g",
        GroupProtocol::Classic,
        PartitionAssignmentStrategy::CooperativeSticky,
        &Events::default(),
    )
    .await;
    let group_less = group_less(&broker).await;
    for consumer in [&classic, &group_less] {
        consumer.subscribe(["a"]).await.unwrap();
        let err = consumer
            .subscribe_broker_pattern(broker_pattern("a.*"))
            .await
            .expect_err("refused");
        assert!(matches!(err, KrafkaError::IllegalState { .. }), "{err:?}");
        assert_eq!(
            consumer.subscription().await,
            HashSet::from(["a".to_string()])
        );
    }
}

/// A coordinator that supports only `ConsumerGroupHeartbeat` v0 cannot take a
/// pattern: `poll()` fails saying so, and no pattern-less heartbeat goes out
/// in its place.
#[tokio::test]
async fn a_broker_pattern_needs_heartbeat_v1() {
    let broker = FakeBroker::start().await.unwrap();
    broker.set_api_versions(ApiKey::ConsumerGroupHeartbeat, 0, 0);
    let consumer = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &Events::default(),
    )
    .await;
    consumer
        .subscribe_broker_pattern(broker_pattern("pat-.*"))
        .await
        .unwrap();
    let err = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect_err("v0 cannot carry a pattern");
    assert!(err.to_string().contains("Kafka 4.0"), "{err}");
    assert_eq!(
        broker.request_count(ApiKey::ConsumerGroupHeartbeat),
        0,
        "a heartbeat went out without the pattern"
    );
}

// ── A subscription changed during a join ─────────────────────────────────

/// A subscription that changes while a join is in flight reaches the
/// coordinator: the join that carried the old topics is followed by
/// another. (Before, the member stayed on the old topics until something
/// else forced a rebalance.)
async fn a_subscription_changed_during_a_join_is_not_lost(protocol: GroupProtocol) {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("a", 1);
    broker.create_topic("b", 1);
    let join = match protocol {
        GroupProtocol::Consumer => ApiKey::ConsumerGroupHeartbeat,
        _ => ApiKey::JoinGroup,
    };
    broker.on_once(join, |_| Control::Delay(Duration::from_millis(600)));
    let consumer = group_member(
        &broker,
        "g",
        protocol,
        PartitionAssignmentStrategy::CooperativeSticky,
        &Events::default(),
    )
    .await;

    consumer.subscribe(["a"]).await.unwrap();
    // The join with ["a"] is in flight (classic: on its own task; KIP-848:
    // inside this poll) when the subscription changes.
    let (first, ()) = tokio::join!(consumer.poll(Duration::from_millis(100)), async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        consumer.subscribe(["a", "b"]).await.unwrap();
    });
    first.unwrap();

    poll_until(
        &[&consumer],
        async |_| consumer.assignment().await.contains_key("b"),
        "b to be assigned after the in-flight join",
    )
    .await;
}

#[tokio::test]
async fn a_classic_subscription_changed_during_a_join_is_not_lost() {
    a_subscription_changed_during_a_join_is_not_lost(GroupProtocol::Classic).await;
}

#[tokio::test]
async fn a_kip848_subscription_changed_during_a_join_is_not_lost() {
    a_subscription_changed_during_a_join_is_not_lost(GroupProtocol::Consumer).await;
}

/// A group-less `subscribe()` that replaces the topic list drops the
/// partitions of the topic it no longer names.
#[tokio::test]
async fn a_group_less_resubscribe_drops_the_replaced_topic() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("a", 2);
    broker.create_topic("b", 1);
    let consumer = group_less(&broker).await;
    consumer.subscribe(["a"]).await.unwrap();
    assert_eq!(consumer.assignment().await.get("a").map(Vec::len), Some(2));

    consumer.subscribe(["b"]).await.unwrap();
    consumer.poll(Duration::from_millis(100)).await.unwrap();
    assert_eq!(
        assigned_topics(&consumer.assignment().await),
        HashSet::from(["b".to_string()])
    );
}

// ── Two KIP-848 members share a new matching topic ───────────────────────

/// Two KIP-848 members on one pattern: a topic created later is split
/// between them, no partition is held by both, and every record is
/// processed. (The fake's classic coordinator holds one member at a time;
/// the classic split is checked against a real broker in
/// `integration_tests`.)
async fn kip848_members_share_a_new_topic(broker_side: bool) {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("pat-a", 1);
    let events = Events::default();
    let a = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    let b = group_member(
        &broker,
        "g",
        GroupProtocol::Consumer,
        PartitionAssignmentStrategy::CooperativeSticky,
        &events,
    )
    .await;
    for consumer in [&a, &b] {
        if broker_side {
            consumer
                .subscribe_broker_pattern(broker_pattern("pat-.*"))
                .await
                .unwrap();
        } else {
            consumer
                .subscribe_pattern(TopicPattern::new("pat-.*").unwrap())
                .await
                .unwrap();
        }
    }

    broker.create_topic("pat-b", 4);
    let values: Vec<String> = (0..20).map(|i| format!("b{i}")).collect();
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    produce(&broker, "pat-b", &refs).await;
    poll_until(
        &[&a, &b],
        async |seen| values.iter().all(|v| all(seen).contains(v)),
        "every pat-b record",
    )
    .await;

    // Each member re-evaluates on its own schedule, so the split settles
    // after the slower one has joined with the topic.
    poll_until(
        &[&a, &b],
        async |_| {
            let (pa, pb) = (a.assignment().await, b.assignment().await);
            let (na, nb) = (
                pa.get("pat-b").map_or(0, Vec::len),
                pb.get("pat-b").map_or(0, Vec::len),
            );
            na > 0 && nb > 0 && na + nb == 4
        },
        "pat-b split across both members",
    )
    .await;
    let (pa, pb) = (a.assignment().await, b.assignment().await);
    let shared: Vec<&i32> = pa["pat-b"]
        .iter()
        .filter(|p| pb["pat-b"].contains(p))
        .collect();
    assert!(
        shared.is_empty(),
        "partitions {shared:?} held by both members"
    );
    assert_eq!(events.count("lost"), 0);
}

#[tokio::test]
async fn kip848_members_share_a_new_topic_on_a_client_side_pattern() {
    kip848_members_share_a_new_topic(false).await;
}

#[tokio::test]
async fn kip848_members_share_a_new_topic_on_a_broker_side_pattern() {
    kip848_members_share_a_new_topic(true).await;
}
