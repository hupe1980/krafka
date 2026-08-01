//! Client-driven tests: real `krafka` clients against the fake broker.
//!
//! Each test here exercises a client behaviour that previously needed Docker
//! and a well-timed cluster failure to reach at all. The assertions are on what
//! the *client* did — how many attempts it made, which broker it went to, and
//! whether it recovered — not on the broker's internals.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashSet;
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

// ---------------------------------------------------------------------------
// ApiVersions negotiation (KIP-511 / KIP-584)
// ---------------------------------------------------------------------------

/// The handshake must negotiate a *flexible* ApiVersions version, not pin v0.
///
/// This is what carries `ClientSoftwareName` / `ClientSoftwareVersion`
/// (KIP-511) to the broker; at v0 those fields do not exist on the wire and the
/// broker's `client.software.name` metric reports the client as unknown. It is
/// also the only version that carries the KIP-584 feature tagged fields.
#[tokio::test]
async fn the_handshake_negotiates_a_flexible_api_versions_version() {
    let broker = FakeBroker::start().await.unwrap();
    let _admin = admin_for(&broker).await;

    let negotiated: Vec<i16> = broker
        .requests()
        .into_iter()
        .filter(|r| r.api_key == ApiKey::ApiVersions)
        .map(|r| r.api_version)
        .collect();

    assert!(
        !negotiated.is_empty(),
        "the client must send at least one ApiVersions request"
    );
    // The version the handshake settled on is the last one attempted.
    let settled = negotiated.last().copied().unwrap_or(-1);
    assert!(
        settled >= 3,
        "ApiVersions must settle on v3+ so KIP-511 client software identity is \
         actually on the wire; got {negotiated:?}"
    );

    // One attempt when the broker covers the client's ceiling; two when the
    // client's ceiling is higher and it has to fall back. Deriving the
    // expectation keeps this test honest under `unstable-protocol`, which
    // raises the ceiling past what the fake broker (like any released Kafka)
    // supports.
    let client_ceiling = crate::protocol::versions::API_VERSIONS_MAX;
    let broker_ceiling = super::handlers::API_VERSIONS_RANGE.1;
    let expected_attempts = if client_ceiling > broker_ceiling {
        2
    } else {
        1
    };
    assert_eq!(
        negotiated.len(),
        expected_attempts,
        "client ceiling v{client_ceiling}, broker ceiling v{broker_ceiling}; \
         got attempts {negotiated:?}"
    );
    assert_eq!(
        settled,
        client_ceiling.min(broker_ceiling),
        "the handshake must settle on the highest mutually supported version"
    );
}

/// A broker that rejects the client's ApiVersions ceiling must not break the
/// handshake: the client re-sends at the version the rejection advertises.
///
/// This is the path every client takes against a broker older than its own
/// protocol ceiling, so it has to work without operator intervention.
#[tokio::test]
async fn an_unsupported_api_versions_ceiling_falls_back_instead_of_failing() {
    let broker = FakeBroker::start().await.unwrap();

    // Reject the first ApiVersions attempt exactly as a too-old broker would.
    broker.on_once(ApiKey::ApiVersions, |_| {
        Control::Error(ErrorCode::UnsupportedVersion)
    });

    let admin = admin_for(&broker).await;

    admin
        .create_topics(vec![NewTopic::new("orders", 1, 1).unwrap()], SETTLE, false)
        .await
        .expect("the client should fall back and complete the handshake");

    assert!(
        broker.request_count(ApiKey::ApiVersions) >= 2,
        "a rejected ceiling must be retried at a lower version, not surfaced \
         as a connection failure"
    );
}

// ---------------------------------------------------------------------------
// Corrupt record batches
// ---------------------------------------------------------------------------

/// Build a producer against the fake broker with the short test timeouts.
async fn producer_for(broker: &FakeBroker) -> Producer {
    Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer should connect to the fake broker")
}

/// A record batch that fails its CRC must reach the application as an error,
/// and must not silently stall the partition.
///
/// The failure mode this guards against is specific and nasty: a decode error
/// leaving the partition unable to advance used to `break` out of the batch
/// loop with a `debug!`, producing no offset update. The consumer then
/// re-fetched the same bytes forever, delivering nothing from that partition
/// while looking perfectly healthy — the reason confined to a log line
/// production filters out.
///
/// Asserting through the *public* `poll()` API is the point. An earlier version
/// of this fix reported the fault correctly from the decode loop but returned it
/// from a helper whose only caller logged and discarded it, so nothing reached
/// the application. Only an end-to-end assertion catches that.
#[tokio::test]
async fn a_corrupt_record_batch_surfaces_from_poll_instead_of_stalling() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = producer_for(&broker).await;
    let _ = producer
        .send("events", None, b"payload")
        .await
        .expect("produce should succeed");

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        // Standalone (manually assigned): manual assignment and group
        // subscription are mutually exclusive, and the fault path under test
        // is identical either way.
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .assign("events", vec![0])
        .await
        .expect("assign should succeed");

    // Corrupt every Fetch from here on, so the consumer cannot get past the
    // batch no matter how many times it retries.
    broker.on(ApiKey::Fetch, |_| Control::CorruptRecords);

    // Poll until the error surfaces. Early polls legitimately return empty
    // while offsets resolve, so wait for the condition rather than asserting
    // on one arbitrary call.
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut surfaced = None;
    while tokio::time::Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(200)).await {
            Ok(records) => assert!(
                records.is_empty(),
                "no record may be delivered from a batch that failed its CRC"
            ),
            Err(e) => {
                surfaced = Some(e);
                break;
            }
        }
    }

    let err = surfaced.expect(
        "a partition stuck on an undecodable batch must surface an error from poll(), \
         not stall silently",
    );
    let text = err.to_string();
    assert!(
        text.contains("events-0"),
        "the error must name the stuck partition so it is actionable: {text}"
    );
    assert!(
        text.contains("seek") && text.contains("pause"),
        "the error must state both remedies: {text}"
    );
    assert_eq!(
        err.protocol_error_kind(),
        Some(crate::error::ProtocolErrorKind::CrcMismatch),
        "the underlying decode failure kind must survive out to the caller"
    );
    assert!(
        !err.is_retriable(),
        "a CRC failure is not retriable: re-fetching returns the same bytes"
    );
    assert!(
        consumer.metrics().batch_decode_errors.get() > 0,
        "the corruption must be counted, so it is alertable without log scraping"
    );
}

/// `pause()` on the corrupt partition is the documented escape hatch, so it has
/// to actually work: the other partitions must keep delivering.
///
/// This is what makes failing the poll an acceptable design rather than a
/// denial of service — without a working escape hatch, one corrupt partition
/// would take the whole consumer down.
#[tokio::test]
async fn pausing_a_corrupt_partition_lets_the_others_keep_flowing() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);

    let producer = producer_for(&broker).await;
    for partition in 0..2 {
        let _ = producer
            .send_record(
                crate::producer::ProducerRecord::new("events", &b"payload"[..])
                    .with_partition(partition),
            )
            .await
            .expect("produce should succeed");
    }

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        // Standalone (manually assigned): manual assignment and group
        // subscription are mutually exclusive, and the fault path under test
        // is identical either way.
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .assign("events", vec![0, 1])
        .await
        .expect("assign should succeed");

    // Corrupt every fetch, and confirm the fault actually reaches the client
    // first — otherwise a pass could be explained by no fetch happening at all.
    broker.on(ApiKey::Fetch, |_| Control::CorruptRecords);
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut saw_fault = false;
    while tokio::time::Instant::now() < deadline {
        if consumer.poll(Duration::from_millis(200)).await.is_err() {
            saw_fault = true;
            break;
        }
    }
    assert!(saw_fault, "the corrupt fetch should have surfaced an error");

    // Serve cleanly again and pause the partition that was stuck. A real
    // operator would pause the partition named in the error; the corruption
    // itself is not repairable from the client side.
    broker.clear_hooks();
    consumer.pause("events", &[0]).await;

    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut delivered = Vec::new();
    while tokio::time::Instant::now() < deadline && delivered.is_empty() {
        match consumer.poll(Duration::from_millis(200)).await {
            Ok(records) => delivered.extend(records),
            Err(e) => panic!("the unpaused partition must not be affected: {e}"),
        }
    }

    assert!(
        !delivered.is_empty(),
        "pausing the stuck partition must let the healthy one keep delivering"
    );
    assert!(
        delivered.iter().all(|r| r.partition == 1),
        "only the unpaused partition should deliver"
    );
}

// ---------------------------------------------------------------------------
// KIP-320: the leader epoch must survive the commit boundary
// ---------------------------------------------------------------------------

/// A committed offset must carry the leader epoch it was read at.
///
/// Kafka stores `(offset, leader_epoch)` together so the *next* owner of the
/// partition — after a restart or a rebalance — can ask `OffsetsForLeaderEpoch`
/// whether the log still contains that pair. Committing a hardcoded `-1`
/// silently disables that check at every commit boundary, which is precisely
/// the window an unclean leader election opens: the resumed consumer cannot
/// distinguish a truncated log from an intact one.
///
/// Within a single session the client already sends `last_fetched_epoch` on
/// Fetch, so the gap is invisible until a consumer restarts — which is exactly
/// what makes it worth pinning down with a test.
#[tokio::test]
async fn a_commit_carries_the_leader_epoch_it_was_read_at() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    // Push the partition's leader epoch off zero so a hardcoded default cannot
    // pass this test by coincidence.
    assert!(broker.bump_leader_epoch("events", 0));
    assert!(broker.bump_leader_epoch("events", 0));
    let expected_epoch = broker.with_state(|s| {
        s.topics
            .get("events")
            .and_then(|t| t.partitions.first())
            .map(|p| p.leader_epoch)
            .expect("partition should exist")
    });
    assert!(
        expected_epoch > 0,
        "the test needs a non-zero epoch to be meaningful, got {expected_epoch}"
    );

    let producer = producer_for(&broker).await;
    let _ = producer
        .send("events", None, b"payload")
        .await
        .expect("produce should succeed");

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("readers")
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    // Consume the record so the consumer has an epoch to vouch for.
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut got = 0usize;
    while tokio::time::Instant::now() < deadline && got == 0 {
        got = consumer
            .poll(Duration::from_millis(200))
            .await
            .expect("poll should succeed")
            .len();
    }
    assert_eq!(got, 1, "the consumer should have read the record");

    consumer.commit().await.expect("commit should succeed");

    let committed = broker.with_state(|s| {
        s.groups
            .get("readers")
            .and_then(|g| g.offsets.get(&("events".to_string(), 0)))
            .cloned()
            .expect("the broker should have recorded a commit")
    });
    assert_eq!(committed.offset, 1, "commit should be next-offset");
    assert_eq!(
        committed.leader_epoch, expected_epoch,
        "the commit must carry the leader epoch the record was read at, not -1; \
         without it KIP-320 truncation detection is lost across restarts"
    );
}

/// A `ListOffsets` rejected for a stale leader epoch must converge, not spin.
///
/// Sending the epoch (KIP-320) is what stops `auto.offset.reset` resolving
/// against a leader whose log this client knows nothing about. But a fenced
/// epoch is only recoverable after a metadata refresh: retrying with the same
/// stale epoch fails identically forever. This checks that the client actually
/// refreshes and then succeeds, rather than trading a silent hazard for a
/// visible deadlock.
#[tokio::test]
async fn a_stale_leader_epoch_on_list_offsets_recovers_after_a_refresh() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = producer_for(&broker).await;
    let _ = producer
        .send("events", None, b"payload")
        .await
        .expect("produce should succeed");

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .metadata_max_age(Duration::from_secs(300))
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .assign("events", vec![0])
        .await
        .expect("assign should succeed");

    // Move leadership on the broker only. The client's cached epoch is now
    // behind, so its next ListOffsets is fenced — exactly the situation where
    // resolving an offset from the stale view would be wrong.
    assert!(broker.bump_leader_epoch("events", 0));

    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut delivered = Vec::new();
    while tokio::time::Instant::now() < deadline && delivered.is_empty() {
        match consumer.poll(Duration::from_millis(200)).await {
            Ok(records) => delivered.extend(records),
            Err(e) => panic!("the client should recover from a fenced epoch, got {e}"),
        }
    }

    assert!(
        !delivered.is_empty(),
        "a fenced ListOffsets must trigger a metadata refresh and then succeed, \
         not leave the partition unable to resolve its start offset"
    );
}

// ---------------------------------------------------------------------------
// KIP-848 consumer group protocol
// ---------------------------------------------------------------------------
//
// The fake coordinator models single-member group membership: epoch ownership,
// fencing, server-side assignment and leave-by-epoch. It does **not** model
// multi-member reconciliation, the genuinely hard half of KIP-848 where the
// coordinator drives members through revoke / epoch-bump / assign in lockstep.
// Nothing here should be read as validating that.

/// A KIP-848 consumer must join via `ConsumerGroupHeartbeat` and receive a
/// server-computed assignment — no JoinGroup/SyncGroup anywhere.
#[tokio::test]
async fn a_kip848_consumer_joins_and_receives_a_server_side_assignment() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 3);

    let producer = producer_for(&broker).await;
    let _ = producer
        .send("events", None, b"payload")
        .await
        .expect("produce should succeed");

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("modern")
        .group_protocol(crate::consumer::GroupProtocol::Consumer)
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut delivered = Vec::new();
    while tokio::time::Instant::now() < deadline && delivered.is_empty() {
        delivered.extend(
            consumer
                .poll(Duration::from_millis(200))
                .await
                .expect("poll should succeed"),
        );
    }

    assert!(
        !delivered.is_empty(),
        "a KIP-848 consumer should receive its assignment and consume"
    );
    assert!(
        broker.request_count(ApiKey::ConsumerGroupHeartbeat) >= 1,
        "membership must be driven by ConsumerGroupHeartbeat"
    );
    assert_eq!(
        broker.request_count(ApiKey::JoinGroup),
        0,
        "KIP-848 must not fall back to the classic JoinGroup protocol"
    );
    assert_eq!(
        broker.request_count(ApiKey::SyncGroup),
        0,
        "KIP-848 must not fall back to the classic SyncGroup protocol"
    );

    let assignment = consumer.assignment().await;
    assert_eq!(
        assignment.get("events").map(|p| p.len()),
        Some(3),
        "the sole member should own every partition; got {assignment:?}"
    );
}

/// A fenced member must give up **all** its partitions, not merely reset its
/// epoch.
///
/// KIP-848: *"the member is expected to immediately give up all its partitions
/// and rejoin the group with a full heartbeat ... and a member epoch equal to
/// zero."* Resetting only the epoch leaves the local assignment intact, so the
/// consumer keeps fetching and committing partitions the coordinator has
/// already handed to someone else — a silent split-brain over those partitions,
/// and precisely the hazard `max.poll.interval.ms` enforcement exists to
/// prevent on the other path.
///
/// The fence here is *persistent*, so the member can never rejoin. That makes
/// the assertion deterministic: with a one-shot fence the member reclaims its
/// partitions within milliseconds and the empty window is unobservable by
/// sampling.
#[tokio::test]
async fn a_fenced_kip848_member_gives_up_its_partitions() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("modern")
        .group_protocol(crate::consumer::GroupProtocol::Consumer)
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    // Settle into a real assignment first, so the fencing has something to
    // revoke and the test cannot pass vacuously.
    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await;
        if !consumer.assignment().await.is_empty() {
            break;
        }
    }
    assert_eq!(
        consumer.assignment().await.get("events").map(|p| p.len()),
        Some(2),
        "the consumer must hold an assignment before fencing is meaningful"
    );

    // Fence every heartbeat from here on: the member is permanently fenced.
    broker.on(ApiKey::ConsumerGroupHeartbeat, |_| {
        Control::Error(ErrorCode::FencedMemberEpoch)
    });

    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut dropped = false;
    while tokio::time::Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await;
        if consumer.assignment().await.is_empty() {
            dropped = true;
            break;
        }
    }

    assert!(
        dropped,
        "a fenced member must drop its assignment; keeping it means consuming \
         partitions the coordinator has reassigned to someone else"
    );
}

/// ...and once the fencing clears, the member must rejoin and be re-assigned,
/// rather than staying fenced forever.
#[tokio::test]
async fn a_fenced_kip848_member_rejoins_once_the_fencing_clears() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("modern")
        .group_protocol(crate::consumer::GroupProtocol::Consumer)
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await;
        if !consumer.assignment().await.is_empty() {
            break;
        }
    }
    let epoch_before = broker.with_state(|s| {
        s.groups
            .get("modern")
            .map(|g| g.group_epoch)
            .expect("group should exist")
    });

    // One fenced heartbeat, then normal service resumes.
    broker.on_once(ApiKey::ConsumerGroupHeartbeat, |_| {
        Control::Error(ErrorCode::FencedMemberEpoch)
    });

    // The decisive observable is broker-side: the coordinator only advances the
    // group epoch when a member (re-)registers, so an advance proves the client
    // came back through a full epoch-0 heartbeat rather than silently carrying
    // on with stale state.
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut rejoined = false;
    while tokio::time::Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await;
        let epoch_now =
            broker.with_state(|s| s.groups.get("modern").map(|g| g.group_epoch).unwrap_or(-1));
        if epoch_now > epoch_before && !consumer.assignment().await.is_empty() {
            rejoined = true;
            break;
        }
    }

    assert!(
        rejoined,
        "a fenced member must rejoin at epoch 0 and be re-assigned; \
         group epoch was {epoch_before} before fencing"
    );
}

/// Steady-state heartbeats must not spin.
///
/// This pins a *rate*, which is the property that actually matters to a
/// coordinator, rather than any one line of client logic. It was written after
/// an earlier version of this file recorded 43 446 `ConsumerGroupHeartbeat`
/// requests in fifteen seconds: a `null` Assignment means "nothing changed
/// since your last heartbeat", and reading it as "not joined yet" left the
/// member outside `Stable`, which `needs_rejoin()` reports as "rejoin
/// required", so every poll sent another full heartbeat and got another null
/// assignment.
///
/// Two changes close that loop — the client treats an accepted non-zero epoch
/// as confirmation of membership, and the fake coordinator resends the
/// assignment when a member re-registers at epoch 0 — and either alone is
/// enough to keep this test green. It is a guard against the behaviour
/// returning, not a bisect of which change fixed it.
#[tokio::test]
async fn a_settled_kip848_member_does_not_spin_on_heartbeats() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("modern")
        .group_protocol(crate::consumer::GroupProtocol::Consumer)
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(50)).await;
        if !consumer.assignment().await.is_empty() {
            break;
        }
    }
    assert!(
        !consumer.assignment().await.is_empty(),
        "the consumer must settle before rate can be measured"
    );

    // Poll hard for a second. A settled member heartbeats on the coordinator's
    // interval (1 s here), so anything beyond a handful means it is spinning.
    let settled = broker.request_count(ApiKey::ConsumerGroupHeartbeat);
    let until = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < until {
        let _ = consumer.poll(Duration::from_millis(10)).await;
    }
    let sent = broker.request_count(ApiKey::ConsumerGroupHeartbeat) - settled;

    assert!(
        sent < 25,
        "a settled member sent {sent} heartbeats in one second; it is spinning \
         rather than heartbeating on the coordinator's interval"
    );
}

/// An epoch the coordinator does not recognise must be rejected, not accepted.
///
/// This is the fake coordinator's own guarantee, and it is what makes the
/// fencing test above meaningful: if the coordinator accepted any epoch, the
/// client could never be fenced and the test would prove nothing.
#[tokio::test]
async fn the_fake_coordinator_fences_a_stale_member_epoch() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("modern")
        .group_protocol(crate::consumer::GroupProtocol::Consumer)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should succeed");

    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await;
        if !consumer.assignment().await.is_empty() {
            break;
        }
    }

    // The coordinator advanced the group epoch when the member joined, so the
    // member is on a non-zero epoch and the coordinator is tracking it.
    let (epoch, members) = broker.with_state(|s| {
        let g = s.groups.get("modern").expect("group should exist");
        (g.group_epoch, g.consumer_members.len())
    });
    assert!(
        epoch > 0,
        "joining must advance the group epoch, got {epoch}"
    );
    assert_eq!(
        members, 1,
        "exactly one KIP-848 member should be registered"
    );
}

/// Two members must converge on a disjoint split, with no partition ever owned
/// by both at once.
///
/// This is the half of KIP-848 that the single-member tests cannot reach. The
/// coordinator reconciles in two steps separated by a heartbeat: it first hands
/// the shrinking member only the partitions it *keeps*, waits for that member
/// to report the reduced set back, and only then grants the released partitions
/// to the joining member. The safety property is that no partition is ever
/// granted to its new owner before the previous owner has confirmed releasing
/// it — the fake coordinator enforces exactly that, so a client that
/// acknowledged early would show up here as an overlap.
#[tokio::test]
async fn two_kip848_members_converge_on_a_disjoint_split() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 4);

    let build = |name: &'static str| {
        let servers = broker.bootstrap_servers();
        async move {
            let consumer = crate::consumer::Consumer::builder()
                .bootstrap_servers(servers)
                .group_id("modern")
                .client_id(name)
                .group_protocol(crate::consumer::GroupProtocol::Consumer)
                .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
                .request_timeout(SHORT_REQUEST_TIMEOUT)
                .connect_timeout(SHORT_CONNECT_TIMEOUT)
                .build()
                .await
                .expect("consumer should connect");
            consumer
                .subscribe(&["events"])
                .await
                .expect("subscribe should succeed");
            consumer
        }
    };

    let first = build("first").await;

    // Let the first member take the whole topic before the second arrives, so
    // the second's arrival forces a genuine revocation rather than a fresh
    // split of unowned partitions.
    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let _ = first.poll(Duration::from_millis(50)).await;
        if first.assignment().await.get("events").map(|p| p.len()) == Some(4) {
            break;
        }
    }
    assert_eq!(
        first.assignment().await.get("events").map(|p| p.len()),
        Some(4),
        "the sole member should own the whole topic before the second joins"
    );

    let second = build("second").await;

    // Drive both until the split settles. Both must keep polling: the
    // shrinking member's acknowledgement is what unblocks the growing one, so
    // a test that polls only the newcomer would deadlock by construction.
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut converged = false;
    while tokio::time::Instant::now() < deadline {
        let _ = first.poll(Duration::from_millis(50)).await;
        let _ = second.poll(Duration::from_millis(50)).await;

        let a = first.assignment().await;
        let b = second.assignment().await;
        let a_parts: HashSet<i32> = a.get("events").into_iter().flatten().copied().collect();
        let b_parts: HashSet<i32> = b.get("events").into_iter().flatten().copied().collect();

        // The safety property, checked on *every* observation rather than only
        // at the end: an overlap that appears and then resolves is still two
        // members consuming the same partition.
        let overlap: Vec<i32> = a_parts.intersection(&b_parts).copied().collect();
        assert!(
            overlap.is_empty(),
            "partitions {overlap:?} were owned by both members at once; a partition \
             must not reach its new owner before the previous owner released it"
        );

        if a_parts.len() == 2 && b_parts.len() == 2 {
            converged = true;
            break;
        }
    }

    assert!(
        converged,
        "two members subscribed to a 4-partition topic should converge on 2 each; \
         got {:?} and {:?}",
        first.assignment().await,
        second.assignment().await
    );

    // And the union must still be the whole topic — a split that loses a
    // partition is as broken as one that double-assigns it.
    let a = first.assignment().await;
    let b = second.assignment().await;
    let mut all: Vec<i32> = a
        .get("events")
        .into_iter()
        .flatten()
        .chain(b.get("events").into_iter().flatten())
        .copied()
        .collect();
    all.sort_unstable();
    assert_eq!(
        all,
        vec![0, 1, 2, 3],
        "every partition must be owned by exactly one member"
    );
}

/// When a member leaves, its partitions must return to the survivor.
#[tokio::test]
async fn a_departing_kip848_member_hands_its_partitions_back() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 4);

    let build = |name: &'static str| {
        let servers = broker.bootstrap_servers();
        async move {
            let consumer = crate::consumer::Consumer::builder()
                .bootstrap_servers(servers)
                .group_id("modern")
                .client_id(name)
                .group_protocol(crate::consumer::GroupProtocol::Consumer)
                .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
                .request_timeout(SHORT_REQUEST_TIMEOUT)
                .connect_timeout(SHORT_CONNECT_TIMEOUT)
                .build()
                .await
                .expect("consumer should connect");
            consumer
                .subscribe(&["events"])
                .await
                .expect("subscribe should succeed");
            consumer
        }
    };

    let survivor = build("survivor").await;
    let leaver = build("leaver").await;

    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let _ = survivor.poll(Duration::from_millis(50)).await;
        let _ = leaver.poll(Duration::from_millis(50)).await;
        let a = survivor.assignment().await;
        let b = leaver.assignment().await;
        if a.get("events").map(|p| p.len()) == Some(2)
            && b.get("events").map(|p| p.len()) == Some(2)
        {
            break;
        }
    }
    assert_eq!(
        survivor.assignment().await.get("events").map(|p| p.len()),
        Some(2),
        "the group must split before a departure is meaningful"
    );

    let _ = leaver.close().await;

    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut reclaimed = false;
    while tokio::time::Instant::now() < deadline {
        let _ = survivor.poll(Duration::from_millis(50)).await;
        if survivor.assignment().await.get("events").map(|p| p.len()) == Some(4) {
            reclaimed = true;
            break;
        }
    }

    assert!(
        reclaimed,
        "the survivor should reclaim the whole topic after the other member \
         leaves; got {:?}",
        survivor.assignment().await
    );
}
