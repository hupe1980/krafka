//! Client-driven tests: real `krafka` clients against the fake broker.
//!
//! Each test here exercises a client behaviour that previously needed Docker
//! and a well-timed cluster failure to reach at all. The assertions are on what
//! the *client* did — how many attempts it made, which broker it went to, and
//! whether it recovered — not on the broker's internals.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use crate::admin::{AdminClient, NewTopic};
use crate::error::ErrorCode;
use crate::producer::Producer;
use crate::protocol::ApiKey;

use super::{Control, FakeBroker};

/// Long enough for a client to notice and act, short enough that a genuine
/// hang fails the test rather than stalling CI.
const SETTLE: Duration = Duration::from_secs(15);

/// Request timeout for tests that need a request to actually time out.
///
/// Config validation rejects `request_timeout < connect_timeout`, so any test
/// wanting a short request timeout must lower `connect_timeout` to match — see
/// [`SHORT_CONNECT_TIMEOUT`]. The fake broker is on loopback, so a two-second
/// connect budget is generous.
const SHORT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Connect timeout paired with [`SHORT_REQUEST_TIMEOUT`].
const SHORT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

async fn admin_for(broker: &FakeBroker) -> AdminClient {
    AdminClient::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("admin client should connect to the fake broker")
}

// ---------------------------------------------------------------------------
// Baseline: the handshake and the default handlers actually work
// ---------------------------------------------------------------------------

/// If this fails, nothing else in this file means anything: it checks that a
/// real client completes ApiVersions negotiation and a Metadata refresh against
/// the fake broker, and that CreateTopics round-trips.
#[tokio::test]
async fn a_real_admin_client_completes_a_handshake_and_creates_a_topic() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    let results = admin
        .create_topics(vec![NewTopic::new("orders", 3, 1).unwrap()], SETTLE, false)
        .await
        .expect("CreateTopics should succeed");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].error, None, "topic creation reported an error");

    broker.with_state(|s| {
        let topic = s
            .topics
            .get("orders")
            .expect("broker should hold the topic");
        assert_eq!(topic.partitions.len(), 3);
    });

    assert!(broker.request_count(ApiKey::ApiVersions) >= 1);
    assert_eq!(broker.request_count(ApiKey::CreateTopics), 1);
}

/// A produce cycle through the in-memory log, proving the record-batch stamping
/// keeps batches decodable and offsets monotonic.
#[tokio::test]
async fn a_real_producer_appends_records_at_broker_assigned_offsets() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer should connect");

    for i in 0..3u8 {
        let _ = producer
            .send("events", None, &[b'v', i])
            .await
            .expect("send should be acknowledged");
    }

    assert_eq!(
        broker.next_offset("events", 0),
        Some(3),
        "three records should have been appended"
    );
}

// ---------------------------------------------------------------------------
// NOT_CONTROLLER on CreateTopics
// ---------------------------------------------------------------------------

/// `NOT_CONTROLLER` must make the admin client refresh metadata, re-resolve the
/// controller and retry — not surface the error to the caller.
///
/// This is the behaviour the controller-routing retry was added for, and it had
/// no test outside Docker.
#[tokio::test]
async fn not_controller_on_create_topics_makes_the_client_refresh_and_retry() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    // Everything after the first attempt is served normally.
    broker.on_once(ApiKey::CreateTopics, |_| {
        Control::Error(ErrorCode::NotController)
    });

    let metadata_before = broker.request_count(ApiKey::Metadata);

    let results = admin
        .create_topics(vec![NewTopic::new("orders", 1, 1).unwrap()], SETTLE, false)
        .await
        .expect("the client should retry past NOT_CONTROLLER, not fail");

    assert_eq!(results[0].error, None, "the retry should have succeeded");
    assert_eq!(
        broker.request_count(ApiKey::CreateTopics),
        2,
        "expected exactly one retry after NOT_CONTROLLER"
    );
    assert!(
        broker.request_count(ApiKey::Metadata) > metadata_before,
        "the client must refresh metadata to re-resolve the controller"
    );
}

/// A controller that never comes back must eventually surface as an error
/// rather than retrying forever.
#[tokio::test]
async fn a_permanently_missing_controller_gives_up_instead_of_looping() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    broker.on(ApiKey::CreateTopics, |_| {
        Control::Error(ErrorCode::NotController)
    });

    let outcome = admin
        .create_topics(vec![NewTopic::new("orders", 1, 1).unwrap()], SETTLE, false)
        .await;

    assert!(
        outcome.is_err(),
        "a permanent NOT_CONTROLLER must terminate, got {outcome:?}"
    );
    let attempts = broker.request_count(ApiKey::CreateTopics);
    assert!(
        (2..=10).contains(&attempts),
        "retries should be bounded, saw {attempts} attempts"
    );
}

// ---------------------------------------------------------------------------
// Leader moves
// ---------------------------------------------------------------------------

/// A partition leader change must be followed: the producer sees
/// `NOT_LEADER_FOR_PARTITION`, takes the leader the broker named alongside it
/// (KIP-951) and re-sends to that broker — with no metadata request in between.
#[tokio::test]
async fn a_producer_follows_a_partition_leader_to_another_broker() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 1);
    // create_topic spreads leadership round-robin; pin partition 0 to node 0 so
    // the move below is unambiguous.
    broker.with_state(|s| {
        if let Some(p) = s.partition_mut("events", 0) {
            p.leader = 0;
        }
    });

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .metadata_max_age(Duration::from_millis(500))
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer should connect");

    let _ = producer
        .send("events", None, b"before-the-move")
        .await
        .expect("the first send should land on the original leader");

    broker.clear_requests();
    assert!(broker.set_leader("events", 0, 1));

    let _ = producer
        .send("events", None, b"after-the-move")
        .await
        .expect("the producer should follow the leader rather than fail");

    assert_eq!(
        broker.next_offset("events", 0),
        Some(2),
        "both records should be in the log"
    );
    assert!(
        broker.request_nodes(ApiKey::Produce).contains(&1),
        "the producer never reached the new leader; hits were {:?}",
        broker.request_nodes(ApiKey::Produce)
    );
    assert_eq!(
        broker.request_count(ApiKey::Metadata),
        0,
        "the leader was named in the produce response, so no refresh was needed; \
         requests were {:?}",
        broker.requests()
    );
}

/// The same leader move, with the default `metadata_max_age`.
///
/// A leader move makes the cached entry *wrong*, not *old*, and a refresh would
/// not have helped anyway: `refresh_for_topics` short-circuits to `AlreadyFresh`
/// while the topic is younger than `metadata_max_age` (300 s by default). The
/// leader hint carried in the produce response is what makes this recover
/// immediately rather than re-sending to the stale leader until
/// `delivery_timeout` expires.
#[tokio::test]
async fn a_leader_move_is_followed_without_waiting_for_the_cache_to_age() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 1);
    broker.with_state(|s| {
        if let Some(p) = s.partition_mut("events", 0) {
            p.leader = 0;
        }
    });

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .delivery_timeout(Duration::from_secs(20))
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer should connect");

    let _ = producer.send("events", None, b"before").await.unwrap();

    broker.clear_requests();
    assert!(broker.set_leader("events", 0, 1));

    let _ = producer
        .send("events", None, b"after")
        .await
        .expect("a leader move must be followed without waiting out metadata_max_age");

    assert!(
        broker.request_nodes(ApiKey::Produce).contains(&1),
        "the producer never reached the new leader; hits were {:?}",
        broker.request_nodes(ApiKey::Produce)
    );
    assert_eq!(
        broker.request_count(ApiKey::Metadata),
        0,
        "the broker-supplied leader must be enough on its own"
    );
    assert_eq!(broker.next_offset("events", 0), Some(2));
}

/// The fallback the leader hint replaces must still work.
///
/// An injected `NOT_LEADER_FOR_PARTITION` carries no `CurrentLeader`, which is
/// what a broker on a pre-KIP-951 version sends. With nothing to apply, the
/// producer has to go and ask — so a metadata request is exactly what should
/// appear here, and its absence would mean the client had simply stopped
/// reacting to the error.
#[tokio::test]
async fn an_error_without_a_leader_hint_still_forces_a_metadata_refresh() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .metadata_max_age(Duration::from_millis(500))
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer should connect");

    let _ = producer.send("events", None, b"before").await.unwrap();

    broker.clear_requests();
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::NotLeaderForPartition)
    });

    let _ = producer
        .send("events", None, b"after")
        .await
        .expect("the retry should succeed once the injected error is spent");

    assert_eq!(broker.request_count(ApiKey::Produce), 2);
    assert!(
        broker.request_count(ApiKey::Metadata) >= 1,
        "with no leader named, the client must refresh metadata to find one"
    );
}

// ---------------------------------------------------------------------------
// Coordinator moves
// ---------------------------------------------------------------------------

/// When a group's coordinator moves to another broker mid-session, the client
/// must re-run FindCoordinator and reach the *new* broker rather than looping
/// against the stale one.
///
/// The move is real: a two-broker cluster with two listeners, and the decisive
/// assertion is that group traffic actually arrived at node 1.
#[tokio::test]
async fn a_group_coordinator_move_is_rediscovered_on_the_new_broker() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 1);
    broker.set_group_coordinator("analytics", 0);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("analytics")
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .metadata_max_age(Duration::from_millis(500))
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    assert!(
        broker.wait_for_requests(ApiKey::JoinGroup, 1, SETTLE).await,
        "the consumer should join the group against the original coordinator"
    );
    assert_eq!(
        broker.request_nodes(ApiKey::JoinGroup),
        vec![0],
        "the first join must go to the original coordinator"
    );

    // Move the coordinator. Node 0 now answers NOT_COORDINATOR for this group,
    // which is what should push the client back through FindCoordinator.
    broker.clear_requests();
    broker.set_group_coordinator("analytics", 1);

    // The client only notices when it next talks to the coordinator, so keep it
    // polling rather than sleeping and hoping.
    let poller = tokio::spawn(async move {
        loop {
            let _ = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
        }
    });

    let rediscovered = broker
        .wait_for_requests(ApiKey::FindCoordinator, 1, SETTLE)
        .await;
    // Wait for a join *on node 1* specifically. A join that was already in
    // flight against node 0 when the coordinator moved can land after
    // `clear_requests`, and counting joins on any node would let that stale
    // one end the wait before the real one arrives.
    let reached_new_node = broker
        .wait_for_request_on_node(ApiKey::JoinGroup, 1, SETTLE)
        .await;
    poller.abort();

    assert!(
        rediscovered,
        "the client must re-run FindCoordinator after NOT_COORDINATOR"
    );
    assert!(
        reached_new_node,
        "the client never re-joined after the coordinator moved"
    );
    assert!(
        broker.request_nodes(ApiKey::JoinGroup).contains(&1),
        "the client kept talking to the old coordinator; join hits were {:?}",
        broker.request_nodes(ApiKey::JoinGroup)
    );
}

// ---------------------------------------------------------------------------
// Late responses
// ---------------------------------------------------------------------------

/// A response that arrives after the client has already timed out the request
/// must not poison the connection: the correlation ID is simply unknown by
/// then, and subsequent requests have to keep working.
///
/// This is the shape of bug that is essentially unreachable against a real
/// broker, because you cannot ask one to answer late on demand.
#[tokio::test]
async fn a_response_arriving_after_the_client_timeout_leaves_the_connection_usable() {
    let broker = FakeBroker::start().await.unwrap();

    let admin = AdminClient::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("admin client should connect");

    // Answer a second past the request timeout, so the response is orphaned by
    // the time it reaches the client.
    broker.on_once(ApiKey::CreateTopics, |_| {
        Control::Delay(SHORT_REQUEST_TIMEOUT + Duration::from_secs(1))
    });

    let timed_out = admin
        .create_topics(vec![NewTopic::new("slow", 1, 1).unwrap()], SETTLE, false)
        .await;
    assert!(
        timed_out.is_err(),
        "the request should have timed out client-side, got {timed_out:?}"
    );

    // Let the late response actually land on the wire before continuing, so the
    // next request genuinely follows an orphaned response rather than racing it.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let after = admin
        .create_topics(vec![NewTopic::new("after", 1, 1).unwrap()], SETTLE, false)
        .await
        .expect("the client must still be usable after an orphaned response");
    assert_eq!(after[0].error, None);

    broker.with_state(|s| {
        assert!(
            s.topics.contains_key("after"),
            "the follow-up request should have been served normally"
        );
    });
}

/// The same situation with two clients: a delayed response on one connection
/// must not disturb another.
#[tokio::test]
async fn a_delayed_response_does_not_fail_requests_on_other_connections() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let slow = AdminClient::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("admin client should connect");

    let healthy = admin_for(&broker).await;

    broker.on_once(ApiKey::CreateTopics, |_| {
        Control::Delay(SHORT_REQUEST_TIMEOUT + Duration::from_secs(1))
    });

    let (slow_result, healthy_result) = tokio::join!(
        slow.create_topics(vec![NewTopic::new("slow", 1, 1).unwrap()], SETTLE, false),
        async {
            // Give the delayed request a head start so it is genuinely in flight.
            tokio::time::sleep(Duration::from_millis(50)).await;
            healthy.list_topics().await
        }
    );

    assert!(slow_result.is_err(), "the delayed request should time out");
    assert!(
        healthy_result.is_ok(),
        "an unrelated connection must be unaffected, got {healthy_result:?}"
    );
}

// ---------------------------------------------------------------------------
// Control-hook mechanics
// ---------------------------------------------------------------------------

/// A dropped connection must be re-established transparently rather than
/// surfacing as a permanent failure.
#[tokio::test]
async fn a_dropped_connection_is_re_established() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    admin
        .create_topics(vec![NewTopic::new("first", 1, 1).unwrap()], SETTLE, false)
        .await
        .expect("the first request should succeed");

    broker.on_once(ApiKey::Metadata, |_| Control::Disconnect);

    let outcome = admin
        .create_topics(vec![NewTopic::new("second", 1, 1).unwrap()], SETTLE, false)
        .await;
    assert!(
        outcome.is_ok(),
        "the client should recover from a dropped connection, got {outcome:?}"
    );
}

/// `on_times` must fire exactly the requested number of times.
#[tokio::test]
async fn on_times_applies_to_exactly_that_many_requests() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    broker.on_times(ApiKey::CreateTopics, 2, |_| {
        Control::Error(ErrorCode::NotController)
    });

    admin
        .create_topics(vec![NewTopic::new("orders", 1, 1).unwrap()], SETTLE, false)
        .await
        .expect("two rejections should still be within the retry budget");

    assert_eq!(
        broker.request_count(ApiKey::CreateTopics),
        3,
        "two injected failures then one success"
    );
}
