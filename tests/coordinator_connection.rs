//! Group coordination traffic has its own connection: heartbeats of the
//! classic, KIP-848 and share protocols keep reaching a coordinator that also
//! holds the member's long-polling fetch, because the broker reads one
//! request per connection at a time.
//!
//! Run: `cargo test --features test-broker --test coordinator_connection`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
use std::time::Duration;

use krafka::consumer::{AutoOffsetReset, Consumer, GroupProtocol};
use krafka::share_consumer::ShareConsumer;
use krafka::testing::{ApiKey, Control, FakeBroker, RecordedRequest};

const T: Duration = Duration::from_secs(10);
/// How long the broker holds an empty fetch: four KIP-848 heartbeat
/// intervals of the fake coordinator (1 s) and more.
const HOLD: Duration = Duration::from_secs(5);
/// The part of the hold the heartbeats are counted in.
const WINDOW: Duration = Duration::from_millis(3500);

/// Requests a group member sends to its coordinator.
const COORDINATION: [ApiKey; 8] = [
    ApiKey::JoinGroup,
    ApiKey::SyncGroup,
    ApiKey::Heartbeat,
    ApiKey::LeaveGroup,
    ApiKey::ConsumerGroupHeartbeat,
    ApiKey::ShareGroupHeartbeat,
    ApiKey::OffsetCommit,
    ApiKey::OffsetFetch,
];

async fn consumer(broker: &FakeBroker, group: &str, protocol: GroupProtocol) -> Consumer {
    consumer_via(broker.bootstrap_servers(), group, protocol).await
}

async fn consumer_via(bootstrap: String, group: &str, protocol: GroupProtocol) -> Consumer {
    krafka::Kafka::builder(bootstrap)
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer(group)
        .group_protocol(protocol)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .heartbeat_interval(Duration::from_millis(200))
        .fetch_max_wait(HOLD)
        .build()
        .await
        .unwrap()
}

async fn share_consumer(broker: &FakeBroker, group: &str) -> ShareConsumer {
    krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .share_consumer(group)
        .fetch_max_wait(HOLD)
        .build()
        .await
        .unwrap()
}

/// Close the consumer, drop it, and wait for every connection it used to
/// close, the coordination connection included.
async fn close_all(broker: &FakeBroker, consumer: Consumer) {
    consumer.close().await.unwrap();
    drop(consumer);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while broker.open_connections() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(broker.open_connections(), 0, "every connection closed");
}

async fn wait_assigned(consumer: &Consumer) {
    for _ in 0..50 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        if !consumer.assignment().await.is_empty() {
            return;
        }
    }
    panic!("never assigned");
}

/// Wait for a fetch the broker will hold (the topic is empty), then count
/// the heartbeats that arrive while it is held. Returns the held fetch and
/// those heartbeats.
async fn heartbeats_during_held_fetch(
    broker: &FakeBroker,
    fetch: ApiKey,
    heartbeat: ApiKey,
) -> (RecordedRequest, Vec<RecordedRequest>) {
    let seen = broker.request_count(fetch);
    assert!(
        broker.wait_for_requests(fetch, seen + 1, 2 * HOLD).await,
        "setup: the member fetches"
    );
    let held = broker
        .requests()
        .into_iter()
        .filter(|r| r.api_key == fetch)
        .nth(seen)
        .unwrap();
    tokio::time::sleep(WINDOW).await;
    let heartbeats = broker
        .requests()
        .into_iter()
        .filter(|r| r.api_key == heartbeat && r.sequence > held.sequence)
        .collect();
    (held, heartbeats)
}

/// Requests every connection sends before its first real one.
const HANDSHAKE: [ApiKey; 3] = [
    ApiKey::ApiVersions,
    ApiKey::SaslHandshake,
    ApiKey::SaslAuthenticate,
];

/// No connection carries both coordination and data requests.
fn assert_purposes_disjoint(broker: &FakeBroker) {
    let requests: Vec<RecordedRequest> = broker
        .requests()
        .into_iter()
        .filter(|r| !HANDSHAKE.contains(&r.api_key))
        .collect();
    let coordination: HashSet<u64> = requests
        .iter()
        .filter(|r| COORDINATION.contains(&r.api_key))
        .map(|r| r.connection)
        .collect();
    let mixed: Vec<&RecordedRequest> = requests
        .iter()
        .filter(|r| !COORDINATION.contains(&r.api_key) && coordination.contains(&r.connection))
        .collect();
    assert!(
        mixed.is_empty(),
        "data requests on a coordination connection: {mixed:?}"
    );
}

async fn classic_or_kip848(protocol: GroupProtocol, heartbeat: ApiKey, group: &str) {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = consumer(&broker, group, protocol).await;
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let (measure, _) = tokio::join!(
        heartbeats_during_held_fetch(&broker, ApiKey::Fetch, heartbeat),
        consumer.poll(HOLD + HOLD),
    );
    let (held, heartbeats) = measure;
    assert!(
        heartbeats.len() >= 2,
        "{protocol:?}: {} heartbeat(s) reached the broker while a fetch was held",
        heartbeats.len()
    );
    assert!(
        heartbeats.iter().all(|h| h.connection != held.connection),
        "{protocol:?}: heartbeats travel on their own connection"
    );
    assert_purposes_disjoint(&broker);
    close_all(&broker, consumer).await;
}

#[tokio::test]
async fn classic_heartbeats_are_not_held_behind_a_fetch() {
    classic_or_kip848(GroupProtocol::Classic, ApiKey::Heartbeat, "g-classic").await;
}

#[tokio::test]
async fn kip848_heartbeats_are_not_held_behind_a_fetch() {
    classic_or_kip848(
        GroupProtocol::Consumer,
        ApiKey::ConsumerGroupHeartbeat,
        "g-kip848",
    )
    .await;
}

#[tokio::test]
async fn share_heartbeats_are_not_held_behind_a_share_fetch() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = share_consumer(&broker, "g-share").await;
    consumer.subscribe(&["events"]).await.unwrap();

    let (measure, _) = tokio::join!(
        heartbeats_during_held_fetch(&broker, ApiKey::ShareFetch, ApiKey::ShareGroupHeartbeat),
        consumer.poll(HOLD + HOLD + HOLD),
    );
    let (held, heartbeats) = measure;
    assert!(
        heartbeats.len() >= 2,
        "{} share heartbeat(s) reached the broker while a ShareFetch was held",
        heartbeats.len()
    );
    assert!(heartbeats.iter().all(|h| h.connection != held.connection));
    assert_purposes_disjoint(&broker);
    consumer.close().await.unwrap();
}

/// Negative control for the harness: with the coordinator on another node
/// the heartbeats were never behind the fetch, so the count above measures
/// the shared socket and nothing else.
#[tokio::test]
async fn heartbeats_to_a_coordinator_on_another_node_flow_during_a_held_fetch() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 1);
    broker.set_leader("events", 0, 0);
    broker.set_group_coordinator("g-split", 1);
    let consumer = consumer(&broker, "g-split", GroupProtocol::Classic).await;
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let (measure, _) = tokio::join!(
        heartbeats_during_held_fetch(&broker, ApiKey::Fetch, ApiKey::Heartbeat),
        consumer.poll(HOLD + HOLD),
    );
    let (held, heartbeats) = measure;
    assert_eq!(held.node_id, 0);
    assert!(heartbeats.len() >= 2, "{} heartbeat(s)", heartbeats.len());
    assert!(heartbeats.iter().all(|h| h.node_id == 1));
    consumer.close().await.unwrap();
}

/// A coordinator the member sends no data request to costs no extra
/// connection. Node 0 is the bootstrap, metadata and fetch broker and node 1
/// the coordinator; node 1 carries data only when the client happened to ask
/// it `FindCoordinator`, and then has one connection per purpose.
#[tokio::test]
async fn a_coordinator_that_is_not_a_data_broker_costs_no_extra_connection() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 1);
    broker.set_leader("events", 0, 0);
    broker.set_group_coordinator("g-apart", 1);
    let node0 = broker.broker_addr(0).unwrap().to_string();
    let consumer = consumer_via(node0, "g-apart", GroupProtocol::Classic).await;
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let on_coordinator: Vec<RecordedRequest> = broker
        .requests()
        .into_iter()
        .filter(|r| r.node_id == 1 && !HANDSHAKE.contains(&r.api_key))
        .collect();
    let connections: HashSet<u64> = on_coordinator.iter().map(|r| r.connection).collect();
    let carries_data = on_coordinator
        .iter()
        .any(|r| !COORDINATION.contains(&r.api_key));
    let expected = if carries_data { 2 } else { 1 };
    assert_eq!(
        connections.len(),
        expected,
        "connections to the coordinator: {on_coordinator:?}"
    );
    assert_purposes_disjoint(&broker);
    close_all(&broker, consumer).await;
}

/// A coordinator that is also the data broker costs exactly one more
/// connection, and `close()` closes both.
#[tokio::test]
async fn a_coordinator_that_is_a_data_broker_costs_one_connection() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = consumer(&broker, "g-one", GroupProtocol::Classic).await;
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let connections: HashSet<u64> = broker.requests().iter().map(|r| r.connection).collect();
    assert_eq!(connections.len(), 2, "data plus coordination");
    assert_purposes_disjoint(&broker);
    close_all(&broker, consumer).await;
}

/// At the connection cap the group still works over the shared connection,
/// and the fallback is counted.
#[tokio::test]
async fn at_the_connection_cap_the_group_shares_the_data_connection() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .max_connections(Some(1))
        .connect()
        .await
        .unwrap()
        .consumer("g-cap")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .heartbeat_interval(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let connections: HashSet<u64> = broker.requests().iter().map(|r| r.connection).collect();
    assert_eq!(connections.len(), 1);
    assert!(consumer.metrics().connections.coordination_fallbacks > 0);
    consumer.close().await.unwrap();
}

/// A transport error on the coordination connection rediscovers the
/// coordinator and keeps heartbeating; the member does not rejoin.
#[tokio::test]
async fn a_dropped_coordination_connection_does_not_rejoin() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = consumer(&broker, "g-drop", GroupProtocol::Classic).await;
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let joins = broker.request_count(ApiKey::JoinGroup);
    let heartbeats = broker.request_count(ApiKey::Heartbeat);
    broker.on_once(ApiKey::Heartbeat, |_| Control::Disconnect);
    assert!(
        broker
            .wait_for_requests(ApiKey::Heartbeat, heartbeats + 4, T)
            .await,
        "heartbeats resume after the dropped connection"
    );
    let _ = consumer.poll(Duration::from_millis(200)).await;
    assert_eq!(broker.request_count(ApiKey::JoinGroup), joins, "no rejoin");
    assert!(!consumer.assignment().await.is_empty());
    consumer.close().await.unwrap();
}

/// After the coordinator moves, its old coordination connection carries no
/// data, and the new coordinator gets the heartbeats.
#[tokio::test]
async fn a_moved_coordinator_takes_the_coordination_connection_along() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 1);
    broker.set_leader("events", 0, 0);
    broker.set_group_coordinator("g-move", 0);
    let consumer = consumer(&broker, "g-move", GroupProtocol::Classic).await;
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    broker.set_group_coordinator("g-move", 1);
    assert!(
        broker
            .wait_for_request_on_node(ApiKey::Heartbeat, 1, T)
            .await,
        "heartbeats follow the coordinator"
    );
    for _ in 0..5 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
    }
    assert_purposes_disjoint(&broker);
    close_all(&broker, consumer).await;
}
