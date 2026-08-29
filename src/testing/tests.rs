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
            .send("events", None, Some(&[b'v', i]))
            .await
            .expect("send should be acknowledged");
    }

    assert_eq!(
        broker.next_offset("events", 0),
        Some(3),
        "three records should have been appended"
    );
}

/// Produce order must follow **enqueue** order, not the order acknowledgements
/// are awaited in.
///
/// This is the guarantee `enqueue()` exists to provide, and the reason a fused
/// `send_record()` future cannot provide it: a fused future does its append
/// somewhere inside its own polling, so N of them polled concurrently append in
/// poll order. Under buffer-memory backpressure the two orders diverge — a send
/// that cannot get its permit yields and a later one appends first.
///
/// The test awaits the handles in deliberately reversed order. If ordering were
/// established by the await rather than by the enqueue, the log would come back
/// reversed.
#[tokio::test]
async fn produce_order_follows_enqueue_order_not_await_order() {
    const RECORDS: usize = 64;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("producer should connect");

    // Enqueue in order 0..N. Ordering is fixed by these calls returning.
    let mut handles = Vec::with_capacity(RECORDS);
    for i in 0..RECORDS {
        let handle = producer
            .enqueue(
                crate::producer::ProducerRecord::new("events", vec![i as u8]).with_partition(0),
            )
            .await
            .expect("enqueue should succeed");
        assert_eq!(
            handle.partition(),
            0,
            "the partition is known at enqueue time"
        );
        handles.push((i, handle));
    }

    // Await them backwards.
    let mut offsets = vec![0i64; RECORDS];
    for (i, handle) in handles.into_iter().rev() {
        offsets[i] = handle
            .await
            .expect("every record must be acknowledged")
            .offset;
    }

    producer.close().await;

    assert_eq!(
        broker.next_offset("events", 0),
        Some(RECORDS as i64),
        "every record must be appended exactly once"
    );

    // The offset assigned to record `i` must increase with `i`: the broker
    // stored them in enqueue order.
    for window in offsets.windows(2) {
        assert!(
            window[0] < window[1],
            "records must be stored in enqueue order, got offsets {offsets:?}"
        );
    }
}

/// The **default** producer — `linger = 0`, idempotence on — must batch, and
/// concurrent sends to one partition must not corrupt its sequence stream.
///
/// Both properties come from the same place: every send goes through the record
/// accumulator, which keeps exactly one batch per partition on the wire and
/// coalesces everything that arrives during that round trip into the next one.
///
/// Before this, `linger = 0` bypassed the accumulator entirely for a
/// second, unbatched send path. That path issued one `Produce` request per
/// record — the throughput cost — and allowed up to five of them to race onto
/// the wire at once with no per-partition ordering, so an idempotent producer
/// could see its own sequences arrive out of order and fail permanently with
/// `OUT_OF_ORDER_SEQUENCE_NUMBER`. This test fails on that code in both
/// assertions.
#[tokio::test]
async fn the_default_producer_batches_concurrent_sends_to_one_partition() {
    const RECORDS: usize = 200;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = std::sync::Arc::new(
        Producer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .request_timeout(SHORT_REQUEST_TIMEOUT)
            .connect_timeout(SHORT_CONNECT_TIMEOUT)
            // No `.linger(..)`: this is the out-of-the-box configuration.
            .build()
            .await
            .expect("producer should connect"),
    );

    // Force every record onto one partition so they share one sequence stream.
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..RECORDS {
        let producer = producer.clone();
        tasks.spawn(async move {
            producer
                .send_record(
                    crate::producer::ProducerRecord::new("events", format!("v{i}").into_bytes())
                        .with_partition(0),
                )
                .await
        });
    }

    let mut acknowledged = 0usize;
    while let Some(joined) = tasks.join_next().await {
        let _ = joined
            .expect("send task should not panic")
            .expect("every send must be acknowledged");
        acknowledged += 1;
    }
    producer.close().await;

    assert_eq!(acknowledged, RECORDS);
    assert_eq!(
        broker.next_offset("events", 0),
        Some(RECORDS as i64),
        "every record must be appended exactly once"
    );

    let produce_requests = broker.request_count(ApiKey::Produce);
    assert!(
        produce_requests <= RECORDS / 10,
        "the default producer must coalesce concurrent sends: {produce_requests} Produce \
         requests for {RECORDS} records is barely batching (the unbatched path sent one \
         request per record; the accumulator sends a handful)"
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

/// The controller retry budget must be the *configured* one.
///
/// It used to be a hardcoded 5 attempts spaced by a flat 100 ms — no jitter, no
/// growth, and no way to change it. On a cluster whose controller elections
/// take longer than the ~500 ms that buys, `create_topics` during a rolling
/// controller restart failed with "the controller did not stabilise" when
/// waiting a little longer would have worked. The docs meanwhile claimed the
/// gap was `retry.backoff.ms`, a setting that did not exist.
#[tokio::test]
async fn the_controller_retry_budget_is_the_configured_one() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = AdminClient::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        // `retries` counts additional attempts, so this is four tries.
        .retries(3)
        .retry_backoff(Duration::from_millis(1))
        .build()
        .await
        .expect("admin client should connect");

    broker.on(ApiKey::CreateTopics, |_| {
        Control::Error(ErrorCode::NotController)
    });

    let outcome = admin
        .create_topics(vec![NewTopic::new("orders", 1, 1).unwrap()], SETTLE, false)
        .await;
    assert!(
        outcome.is_err(),
        "a permanent NOT_CONTROLLER must terminate"
    );

    assert_eq!(
        broker.request_count(ApiKey::CreateTopics),
        4,
        "retries(3) must mean three retries on top of the first attempt"
    );

    let message = outcome.expect_err("checked above").to_string();
    assert!(
        message.contains("retries"),
        "the error must name the setting to raise, got: {message}"
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
        .send("events", None, Some(b"before-the-move"))
        .await
        .expect("the first send should land on the original leader");

    broker.clear_requests();
    assert!(broker.set_leader("events", 0, 1));

    let _ = producer
        .send("events", None, Some(b"after-the-move"))
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

    let _ = producer
        .send("events", None, Some(b"before"))
        .await
        .unwrap();

    broker.clear_requests();
    assert!(broker.set_leader("events", 0, 1));

    let _ = producer
        .send("events", None, Some(b"after"))
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

    let _ = producer
        .send("events", None, Some(b"before"))
        .await
        .unwrap();

    broker.clear_requests();
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::NotLeaderForPartition)
    });

    let _ = producer
        .send("events", None, Some(b"after"))
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
        .send("events", None, Some(b"payload"))
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
        .send("events", None, Some(b"payload"))
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
        .send("events", None, Some(b"payload"))
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
        .send("events", None, Some(b"payload"))
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

// ── TransportConfig reaches the socket ───────────────────────────────────
//
// A review found eleven documented `ConnectionConfig` / `ConnectionPool`
// settings that no client builder could reach: every client constructed its
// config from four fields and called `ConnectionPool::new`, so the rest were
// pinned to their defaults forever. `TransportConfig` is the fix.
//
// Unit tests already assert the value survives the builder and lands on
// `ConnectionConfig`. That is not the same claim as "it changes what the socket
// does" — the previous defect was precisely a value that existed in a config
// struct and never reached the wire. These tests close that gap by observing
// the *behaviour* against a real TCP listener.

/// `max_response_size` must bound the frame the reader accepts.
///
/// A 1 KiB ceiling against a metadata response describing 128 partitions: the
/// connection must fail rather than accept the oversized frame. If the setting
/// never reached `Decoder::with_max_size`, the client would connect happily.
///
/// The partition count matters — an earlier draft of this test used eight
/// partitions, whose response fits comfortably inside 1 KiB, and passed for the
/// wrong reason.
#[tokio::test]
async fn transport_max_response_size_reaches_the_frame_decoder() {
    let broker = FakeBroker::start().await.unwrap();
    for topic in ["alpha", "bravo", "charlie", "delta"] {
        broker.create_topic(topic, 32);
    }

    let transport = crate::network::TransportConfig::builder()
        // 1 KiB is the enforced minimum, and far below a metadata response
        // describing 128 partitions.
        .max_response_size(1024)
        .build()
        .expect("valid transport config");

    let result = crate::admin::AdminClient::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .transport(transport)
        .build()
        .await;

    let err = result
        .err()
        .expect("a 1 KiB response ceiling must reject a 128-partition metadata response");
    let message = err.to_string();
    assert!(
        message.contains("exceeds maximum")
            || message.contains("connection closed")
            || message.contains("Connection reset"),
        "expected a frame-size rejection, got: {message}"
    );
}

/// The same cluster, with the default ceiling, must connect — otherwise the
/// test above would pass for the wrong reason (a broken fake broker, an
/// unrelated connect failure).
#[tokio::test]
async fn transport_default_response_size_still_connects() {
    let broker = FakeBroker::start().await.unwrap();
    for topic in ["alpha", "bravo", "charlie", "delta"] {
        broker.create_topic(topic, 32);
    }

    let admin = crate::admin::AdminClient::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .transport(crate::network::TransportConfig::default())
        .build()
        .await
        .expect("the default ceiling must not reject a normal metadata response");

    admin.close().await;
}

/// `max_connections` must bound the pool, not just live in the config struct.
///
/// A two-broker cluster with a cap of one. Producing to both partitions needs
/// two sockets — the partitions have different leaders — so the cap must refuse
/// one of them by name.
///
/// The refusal may land on the initial metadata refresh or on a later send,
/// depending on which broker the client bootstraps against and whether the
/// refresh needed the second node. Asserting on *either* keeps the test
/// deterministic; an earlier draft asserted the build must fail and passed
/// alone but failed under the full suite.
///
/// Without the cap reaching `ConnectionPool` — which it could not before,
/// because `with_max_total_connections` takes `self` by value and the pool is
/// `Arc`-wrapped on the next line — both sockets would open and nothing here
/// would be refused.
#[tokio::test]
async fn broker_throttle_is_honoured_and_counted() {
    let broker = FakeBroker::start().await.unwrap();

    let client = crate::client::KrafkaClient::builder(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("client should connect");

    let conn = client
        .pool()
        .get_connection(&broker.bootstrap_servers())
        .await
        .expect("connection to the fake broker");

    // A fresh connection is not throttled.
    assert!(
        conn.throttle_remaining().is_none(),
        "a connection starts un-throttled"
    );

    // KIP-219: the broker reports a throttle, the client records the deadline.
    conn.notify_throttle(60);
    let remaining = conn
        .throttle_remaining()
        .expect("the reported throttle must be pending");
    assert!(
        remaining <= Duration::from_millis(60) && remaining > Duration::from_millis(20),
        "the pending delay must reflect what the broker asked for, got {remaining:?}"
    );

    // A *shorter* throttle must not shorten a longer one already pending.
    conn.notify_throttle(5);
    assert!(
        conn.throttle_remaining().expect("still pending") > Duration::from_millis(20),
        "a later, smaller throttle must not cut a longer window short"
    );

    let metrics = client.pool().metrics();
    assert_eq!(metrics.snapshot().throttle_delays, 0, "nothing waited yet");

    // Waiting it out both sleeps and counts.
    let waited = conn
        .await_throttle()
        .await
        .expect("there was a delay to wait");
    assert!(waited > Duration::ZERO);
    assert!(
        conn.throttle_remaining().is_none(),
        "the window is spent once it has been waited out"
    );

    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.throttle_delays, 1);
    assert!(
        snapshot.throttle_delay_ms > 0,
        "a counted delay with zero duration is not a measurement"
    );

    // And an un-throttled connection neither sleeps nor counts.
    assert!(conn.await_throttle().await.is_none());
    assert_eq!(metrics.snapshot().throttle_delays, 1);
}

#[tokio::test]
async fn a_throttle_the_producer_waits_out_is_counted() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let client = crate::client::KrafkaClient::builder(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("client should connect");
    let pool = client.pool().clone();

    let producer = crate::producer::Producer::builder()
        .with_client(&client)
        .build()
        .await
        .expect("producer should connect");

    // One send establishes the connection to the leader.
    let _ = producer
        .send("events", None, Some(b"warm-up"))
        .await
        .expect("send should be acknowledged");

    let metrics = producer.connection_metrics();
    assert_eq!(
        metrics.snapshot().throttle_delays,
        0,
        "nothing has been throttled yet"
    );

    // Impose a throttle the way a broker would (KIP-219), then send again.
    // The pool is shared, so reaching the same connection through a client
    // built on it is enough to reach the producer's own socket.
    let conn = pool
        .get_connection(&broker.bootstrap_servers())
        .await
        .expect("the connection is already open");
    conn.notify_throttle(40);

    let _ = producer
        .send("events", None, Some(b"throttled"))
        .await
        .expect("a throttled send still succeeds, just later");

    producer.close().await;

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.throttle_delays, 1,
        "the producer waits out the throttle before dispatching, and that wait \
         has to be counted — it used to sleep on `throttle_remaining()` directly, \
         which consumed the window before the request path could record it, so \
         this metric read zero on the path most likely to be throttled"
    );
    assert!(
        snapshot.throttle_delay_ms > 0,
        "a counted delay with zero duration is not a measurement"
    );
}

#[tokio::test]
async fn transport_max_connections_bounds_the_pool() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 2);
    broker.set_leader("events", 0, 0);
    broker.set_leader("events", 1, 1);

    let transport = crate::network::TransportConfig::builder()
        .max_connections(Some(1))
        .build()
        .expect("valid transport config");

    let mut refusal: Option<String> = None;

    match crate::producer::Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .linger(Duration::from_millis(5))
        .transport(transport)
        .build()
        .await
    {
        Err(e) => refusal = Some(e.to_string()),
        Ok(producer) => {
            for partition in 0..2 {
                let record = crate::producer::ProducerRecord::new("events", b"payload".to_vec())
                    .with_partition(partition);
                if let Err(e) = producer.send_record(record).await {
                    refusal.get_or_insert_with(|| e.to_string());
                }
            }
            producer.close().await;
        }
    }

    let message = refusal.expect(
        "max_connections(1) must refuse a second broker connection somewhere; \
         if nothing was refused, the cap never reached ConnectionPool",
    );
    assert!(
        message.contains("connection pool limit reached"),
        "the refusal must come from the pool cap, not an unrelated failure: {message}"
    );
}

/// The same two-broker cluster with a cap that accommodates it must connect,
/// so the test above cannot pass because of a broken fixture.
#[tokio::test]
async fn transport_sufficient_max_connections_connects() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("events", 2);
    broker.set_leader("events", 0, 0);
    broker.set_leader("events", 1, 1);

    let transport = crate::network::TransportConfig::builder()
        .max_connections(Some(8))
        .build()
        .expect("valid transport config");

    let producer = crate::producer::Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .linger(Duration::from_millis(5))
        .transport(transport)
        .build()
        .await
        .expect("a cap of 8 must accommodate a two-broker cluster");

    for partition in 0..2 {
        let record = crate::producer::ProducerRecord::new("events", b"payload".to_vec())
            .with_partition(partition);
        let _metadata = producer
            .send_record(record)
            .await
            .expect("both partitions should be reachable under a sufficient cap");
    }

    producer.close().await;
}

// ── Streams groups (KIP-1071) ────────────────────────────────────────────

/// `describe_streams_groups` must decode a fully-populated response.
///
/// The value is in the shapes, not the data. This response carries two
/// nullable structs behind presence bytes (`Topology`, `UserEndpoint`), a
/// nullable array nested inside one of them (`Subtopologies`), and a `uint16`
/// port — each a place where a decoder that guesses desynchronises the rest of
/// the frame and produces plausible garbage rather than an error.
#[tokio::test]
async fn describe_streams_groups_decodes_topology_and_members() {
    use crate::testing::state::{StreamsGroupState, StreamsMemberState};

    let broker = FakeBroker::start().await.unwrap();
    broker.with_state(|s| {
        s.streams_groups.insert(
            "wordcount".to_string(),
            StreamsGroupState {
                group_state: "Stable".to_string(),
                group_epoch: 7,
                assignment_epoch: 7,
                topology_epoch: Some(3),
                subtopologies: Some(vec!["0".to_string(), "1".to_string()]),
                members: vec![
                    StreamsMemberState {
                        member_id: "m-1".to_string(),
                        member_epoch: 7,
                        topology_epoch: 3,
                        process_id: "proc-a".to_string(),
                        // Port above 32767: decoded as i16 this comes back
                        // negative, which is exactly the bug worth catching.
                        user_endpoint: Some(("iq.internal".to_string(), 61234)),
                        active_tasks: vec![("0".to_string(), vec![0, 1])],
                        target_active_tasks: vec![("0".to_string(), vec![0, 1])],
                    },
                    StreamsMemberState {
                        member_id: "m-2".to_string(),
                        member_epoch: 7,
                        // Behind the group's topology epoch of 3.
                        topology_epoch: 2,
                        process_id: "proc-b".to_string(),
                        user_endpoint: None,
                        active_tasks: vec![("1".to_string(), vec![0])],
                        // Mid-rebalance: target differs from current.
                        target_active_tasks: vec![("1".to_string(), vec![0, 1])],
                    },
                ],
            },
        );
    });

    let admin = admin_for(&broker).await;
    let groups = admin
        .describe_streams_groups(&["wordcount"])
        .await
        .expect("StreamsGroupDescribe should succeed");

    assert_eq!(groups.len(), 1);
    let group = &groups[0];
    assert_eq!(group.group_id, "wordcount");
    assert_eq!(group.group_state, "Stable");
    assert_eq!(group.group_epoch, 7);

    let topology = group.topology.as_ref().expect("topology must be present");
    assert_eq!(topology.epoch, 3);
    let subs = topology
        .subtopologies
        .as_ref()
        .expect("subtopologies must be present, not null");
    assert_eq!(subs.len(), 2);
    assert_eq!(subs[0].subtopology_id, "0");
    assert_eq!(subs[0].source_topics, vec!["source-topic".to_string()]);

    assert_eq!(group.members.len(), 2);

    let m1 = &group.members[0];
    let endpoint = m1
        .user_endpoint
        .as_ref()
        .expect("m-1 has an Interactive Queries endpoint");
    assert_eq!(endpoint.host, "iq.internal");
    assert_eq!(
        endpoint.port, 61234,
        "Endpoint.Port is uint16; decoding it signed wraps this negative"
    );
    assert_eq!(m1.assignment.active_tasks.len(), 1);
    assert_eq!(m1.assignment.active_tasks[0].partitions, vec![0, 1]);
    assert_eq!(
        m1.assignment, m1.target_assignment,
        "m-1 is settled on its target"
    );

    let m2 = &group.members[1];
    assert!(m2.user_endpoint.is_none(), "m-2 configured no endpoint");
    assert!(
        m2.topology_epoch < topology.epoch,
        "m-2 is still running an older topology"
    );
    assert_ne!(
        m2.assignment, m2.target_assignment,
        "m-2 has not finished rebalancing"
    );

    assert_eq!(
        group.authorized_operations,
        i32::MIN,
        "authorized operations were not requested, so the sentinel is returned"
    );
}

/// A null topology and a null subtopology array are different states, and both
/// must survive the decoder.
///
/// `Subtopologies: null` means "uninitialized, or source topics missing" —
/// materially different from a topology with zero subtopologies, and a decoder
/// that collapses them reports a broken application as an empty one.
#[tokio::test]
async fn describe_streams_groups_distinguishes_null_from_empty() {
    use crate::testing::state::StreamsGroupState;

    let broker = FakeBroker::start().await.unwrap();
    broker.with_state(|s| {
        s.streams_groups.insert(
            "no-topology".to_string(),
            StreamsGroupState {
                group_state: "Empty".to_string(),
                topology_epoch: None,
                ..Default::default()
            },
        );
        s.streams_groups.insert(
            "uninitialized".to_string(),
            StreamsGroupState {
                group_state: "NotReady".to_string(),
                topology_epoch: Some(1),
                subtopologies: None,
                ..Default::default()
            },
        );
        s.streams_groups.insert(
            "empty-topology".to_string(),
            StreamsGroupState {
                group_state: "Stable".to_string(),
                topology_epoch: Some(1),
                subtopologies: Some(Vec::new()),
                ..Default::default()
            },
        );
    });

    let admin = admin_for(&broker).await;
    let groups = admin
        .describe_streams_groups(&["no-topology", "uninitialized", "empty-topology"])
        .await
        .expect("all three should decode");

    let by_id: std::collections::HashMap<_, _> =
        groups.iter().map(|g| (g.group_id.as_str(), g)).collect();

    assert!(
        by_id["no-topology"].topology.is_none(),
        "a null Topology struct must decode as None"
    );
    assert!(
        by_id["uninitialized"]
            .topology
            .as_ref()
            .expect("topology present")
            .subtopologies
            .is_none(),
        "a null Subtopologies array must stay None, not become an empty Vec"
    );
    assert_eq!(
        by_id["empty-topology"]
            .topology
            .as_ref()
            .expect("topology present")
            .subtopologies
            .as_ref()
            .expect("present but empty")
            .len(),
        0,
        "an empty Subtopologies array is a different state from null"
    );
}

/// An unknown group must be reported per-group, not fail the whole call.
#[tokio::test]
async fn describe_streams_groups_reports_unknown_groups_individually() {
    use crate::testing::state::StreamsGroupState;

    let broker = FakeBroker::start().await.unwrap();
    broker.with_state(|s| {
        s.streams_groups.insert(
            "known".to_string(),
            StreamsGroupState {
                group_state: "Stable".to_string(),
                ..Default::default()
            },
        );
    });

    let admin = admin_for(&broker).await;
    let groups = admin
        .describe_streams_groups(&["known", "missing"])
        .await
        .expect("one unknown group must not fail the call");

    let by_id: std::collections::HashMap<_, _> =
        groups.iter().map(|g| (g.group_id.as_str(), g)).collect();
    assert!(by_id["known"].error_code.is_ok());
    assert_eq!(by_id["missing"].error_code, ErrorCode::GroupIdNotFound);
}

// ── Consumer wakeup and committed-offset lookup ──────────────────────────

/// `wakeup()` must interrupt a `poll()` that is already parked on the broker,
/// not merely the next one.
///
/// The broker is told to hold `Fetch` past the poll deadline, so a `poll()`
/// without `wakeup()` would sit for the full timeout. The assertion is on
/// *elapsed time*: a test that only checked the returned error would pass
/// against an implementation that waited out the fetch and reported the wakeup
/// afterwards, which is the bug worth catching.
#[tokio::test]
async fn wakeup_interrupts_a_poll_parked_on_a_fetch() {
    use std::sync::Arc;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = Arc::new(
        crate::consumer::Consumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .group_id("wakeup-group")
            .request_timeout(SHORT_REQUEST_TIMEOUT)
            .connect_timeout(SHORT_CONNECT_TIMEOUT)
            .build()
            .await
            .expect("consumer should connect"),
    );
    consumer.subscribe(&["events"]).await.unwrap();

    // Let the group settle so the poll below reaches the fetch stage.
    let _ = consumer.poll(Duration::from_secs(2)).await;

    broker.on(ApiKey::Fetch, |_| Control::Delay(Duration::from_secs(20)));

    let waker = Arc::clone(&consumer);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        waker.wakeup();
    });

    let started = tokio::time::Instant::now();
    let outcome = consumer.poll(Duration::from_secs(15)).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "wakeup() must cut the poll short, but it took {elapsed:?}"
    );
    assert!(
        outcome.is_err(),
        "an interrupted poll with no records must report the wakeup, got {outcome:?}"
    );

    broker.clear_hooks();
    // The consumer must remain usable, which is what separates wakeup() from
    // close(): the next poll proceeds normally rather than erroring again.
    let after = consumer.poll(Duration::from_secs(2)).await;
    assert!(
        after.is_ok(),
        "the consumer must stay usable after wakeup(), got {after:?}"
    );
}

/// A `wakeup()` that lands *before* `poll()` is called must still take effect.
///
/// A bare `Notify` only wakes tasks already waiting, so this call would be
/// swallowed; the flag is what makes it survive the race.
#[tokio::test]
async fn wakeup_before_poll_is_not_lost() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("wakeup-race-group")
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");

    consumer.wakeup();

    let outcome = consumer.poll(Duration::from_millis(500)).await;
    assert!(
        outcome.is_err(),
        "a wakeup() before poll() must not be swallowed, got {outcome:?}"
    );

    // Exactly one poll is interrupted — the flag is consumed, not sticky.
    let after = consumer.poll(Duration::from_millis(500)).await;
    assert!(
        after.is_ok(),
        "the wakeup flag must be consumed by one poll, got {after:?}"
    );
}

/// `committed()` must report what the group actually committed, and must
/// distinguish "never committed" from "committed at 0".
#[tokio::test]
async fn committed_reports_the_groups_offsets_from_the_coordinator() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);

    let producer = producer_for(&broker).await;
    for partition in 0..2i32 {
        for i in 0..3u8 {
            let record =
                crate::producer::ProducerRecord::new("events", vec![i]).with_partition(partition);
            let _ = producer.send_record(record).await.unwrap();
        }
    }
    producer.close().await;

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("committed-group")
        .auto_offset_reset(crate::consumer::AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");
    consumer.subscribe(&["events"]).await.unwrap();

    // Nothing committed yet: an absent entry, not a zero.
    let before = consumer
        .committed(&[("events", 0), ("events", 1)])
        .await
        .expect("committed() should reach the coordinator");
    assert!(
        before
            .get(&("events".to_string(), 0))
            .is_none_or(|p| p.offset < 0),
        "a group that has never committed must not report offset 0, got {before:?}"
    );

    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut seen = 0;
    while seen < 6 && tokio::time::Instant::now() < deadline {
        seen += consumer
            .poll(Duration::from_millis(200))
            .await
            .unwrap()
            .len();
    }
    assert_eq!(seen, 6, "all produced records should arrive");
    consumer.commit_sync().await.expect("commit should succeed");

    let after = consumer
        .committed(&[("events", 0), ("events", 1)])
        .await
        .expect("committed() should reach the coordinator");
    for partition in 0..2i32 {
        let pos = after
            .get(&("events".to_string(), partition))
            .unwrap_or_else(|| panic!("partition {partition} must have a committed offset"));
        assert_eq!(
            pos.offset, 3,
            "three records were consumed from partition {partition}"
        );
    }

    let _ = consumer.close().await;
}

/// `committed()` needs a coordinator, so an assign-only consumer must get a
/// clear error rather than an empty map that reads as "nothing committed".
#[tokio::test]
async fn committed_without_a_group_id_is_an_error() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = crate::consumer::Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("an assign-only consumer needs no group");

    let err = consumer
        .committed(&[("events", 0)])
        .await
        .expect_err("no group_id means no coordinator");
    assert!(
        err.to_string().contains("group_id"),
        "the error must name what is missing, got: {err}"
    );
}

// ── KIP-584 feature updates ──────────────────────────────────────────────

/// `validate_only` must be **refused** against a broker whose `UpdateFeatures`
/// predates the field, not silently downgraded.
///
/// This is a destructive-operation guard: `UpdateFeatures` v0 has no
/// `ValidateOnly` field, so sending the request anyway *applies* the change the
/// caller explicitly asked to only simulate. Downgrading a `metadata.version`
/// is data-lossy, so "the dry run turned out not to be one" is about the worst
/// outcome this API has.
///
/// The load-bearing assertion is the last one: **no request was sent**. An
/// implementation that sent the request and then complained would pass an
/// error-is-returned check while having already done the damage.
///
/// This test replaces one that computed `validate_only && version < 1` in the
/// test body and asserted the result. That version passed no matter what
/// `update_features` did — including if the guard were deleted outright.
#[tokio::test]
async fn validate_only_is_refused_by_a_broker_that_predates_the_field() {
    let broker = FakeBroker::start().await.unwrap();
    broker.set_api_versions(ApiKey::UpdateFeatures, 0, 0);

    let admin = admin_for(&broker).await;

    let outcome = admin
        .update_features(
            vec![crate::protocol::FeatureUpdateKey::upgrade(
                "metadata.version",
                17,
            )],
            true, // validate_only
        )
        .await;

    assert!(
        outcome.is_err(),
        "a v0 broker cannot honour validate_only, so this must not succeed"
    );
    assert_eq!(
        broker.request_count(ApiKey::UpdateFeatures),
        0,
        "the request must be refused before it is sent — a dry run that \
         reaches the controller has already stopped being one"
    );
    assert_eq!(
        broker.finalized_feature("metadata.version"),
        None,
        "nothing may have been applied"
    );
}

/// The same call against a current broker must reach the controller and, being
/// a dry run, change nothing.
///
/// Without this half the test above would pass against a client that refused
/// `validate_only` unconditionally.
#[tokio::test]
async fn validate_only_reaches_a_current_broker_and_applies_nothing() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    admin
        .update_features(
            vec![crate::protocol::FeatureUpdateKey::upgrade(
                "metadata.version",
                17,
            )],
            true, // validate_only
        )
        .await
        .expect("a v2 broker supports validate_only");

    assert_eq!(
        broker.request_count(ApiKey::UpdateFeatures),
        1,
        "the dry run must actually be validated by the controller"
    );
    assert_eq!(
        broker.finalized_feature("metadata.version"),
        None,
        "a dry run must not apply the update"
    );
}

/// A real update must be applied, and must go to the **controller**.
#[tokio::test]
async fn a_feature_update_is_applied_by_the_controller() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    broker.set_controller(2);

    let admin = admin_for(&broker).await;

    admin
        .update_features(
            vec![crate::protocol::FeatureUpdateKey::upgrade(
                "metadata.version",
                17,
            )],
            false,
        )
        .await
        .expect("the update should be applied");

    assert_eq!(
        broker.finalized_feature("metadata.version"),
        Some(17),
        "the controller must have applied the requested level"
    );
    assert_eq!(
        broker.request_nodes(ApiKey::UpdateFeatures),
        vec![2],
        "UpdateFeatures is controller-only; reaching any other broker is the \
         bug that made a controller failover look blanket-retriable"
    );
}

/// `describe_features` must report what `update_features` applied.
///
/// Both halves were previously tested only against themselves: `update_features`
/// by asserting its request encodes, `describe_features` not at all. Neither
/// could catch a mismatch between them, and KIP-584 has a specific trap for
/// that — `SupportedFeatures` carries `(min, max)` while `FinalizedFeatures`
/// carries `(max, min)`. A response with those transposed decodes cleanly and
/// reports the wrong levels.
#[tokio::test]
async fn describe_features_reports_what_update_features_applied() {
    let broker = FakeBroker::start().await.unwrap();
    let admin = admin_for(&broker).await;

    let before = admin
        .describe_features()
        .await
        .expect("describe_features should work on a cluster with no features");
    assert!(
        before.finalized_features.is_empty(),
        "a cluster that has finalized nothing must report nothing"
    );
    assert!(
        before.finalized_features_epoch < 0,
        "an absent epoch means the finalized list is not to be trusted, saw {}",
        before.finalized_features_epoch
    );

    admin
        .update_features(
            vec![crate::protocol::FeatureUpdateKey::upgrade(
                "metadata.version",
                17,
            )],
            false,
        )
        .await
        .expect("the update should be applied");

    let after = admin
        .describe_features()
        .await
        .expect("describe_features should work after an update");

    let finalized = after
        .finalized_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("the finalized feature must be reported back");
    assert_eq!(
        finalized.max_version_level, 17,
        "the level read back must be the level applied — transposing the \
         (max, min) pair here decodes without error and reports 1"
    );
    assert!(
        after.finalized_features_epoch >= 0,
        "finalized features are only valid alongside a non-negative epoch"
    );

    let supported = after
        .supported_features
        .iter()
        .find(|f| f.name == "metadata.version")
        .expect("the broker must also advertise what it supports");
    assert!(
        supported.max_version >= finalized.max_version_level,
        "a broker cannot finalize a level above what it supports: {} < {}",
        supported.max_version,
        finalized.max_version_level
    );
}

// ── Share groups (KIP-932) ───────────────────────────────────────────────
//
// The fake broker serves `ShareGroupHeartbeat`, `ShareFetch` and
// `ShareAcknowledge` at v1, including the share-partition state machine that
// replaces committed offsets: a start offset, an acquisition cursor, and a
// per-record delivery count. See `ShareGroupState` for what is and is not
// modelled — in particular, acquisition locks never expire here, so a record
// is redelivered only when it is explicitly released.

#[cfg(feature = "unstable-protocol")]
async fn share_consumer_for(
    broker: &FakeBroker,
    group_id: &str,
) -> crate::share_consumer::ShareConsumer {
    share_consumer_with(broker, group_id, |b| b).await
}

/// A share consumer with the short test timeouts, plus whatever `tune` adds.
#[cfg(feature = "unstable-protocol")]
async fn share_consumer_with(
    broker: &FakeBroker,
    group_id: &str,
    tune: impl FnOnce(
        crate::share_consumer::ShareConsumerBuilder,
    ) -> crate::share_consumer::ShareConsumerBuilder,
) -> crate::share_consumer::ShareConsumer {
    tune(
        crate::share_consumer::ShareConsumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .group_id(group_id)
            .request_timeout(SHORT_REQUEST_TIMEOUT)
            // Before this setter existed, a `request_timeout` below the 10 s
            // default `connect_timeout` was rejected at build time with an error
            // naming a value the builder had no way to change.
            .connect_timeout(SHORT_CONNECT_TIMEOUT),
    )
    .build()
    .await
    .expect("share consumer should connect")
}

/// Poll until `want` records have arrived or the deadline passes.
///
/// A share consumer's first poll is a heartbeat that returns no assignment, so
/// a single `poll()` proving nothing is expected rather than a failure.
#[cfg(feature = "unstable-protocol")]
async fn drain_share(
    consumer: &crate::share_consumer::ShareConsumer,
    want: usize,
) -> Vec<crate::consumer::ConsumerRecord> {
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut got = Vec::new();
    while got.len() < want && tokio::time::Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(200)).await {
            Ok(records) => got.extend(records),
            Err(e) => panic!("share poll failed: {e}"),
        }
    }
    got
}

/// A share consumer must receive the records a producer wrote, and its
/// delivery counters must move with them.
///
/// A share group used to be operable but not observable: the transport
/// counters showed requests and nothing showed records. A metric that exists
/// but is never incremented is worse than none, because it reads as "zero
/// records" rather than "not measured" — so this asserts the counters against
/// the records actually returned, not merely that they are non-zero.
#[cfg(feature = "unstable-protocol")]
#[tokio::test]
async fn a_share_consumer_receives_records_and_counts_them() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = producer_for(&broker).await;
    for i in 0..5u8 {
        let _ = producer
            .send("events", None, Some(&[b'v', i]))
            .await
            .expect("send should be acknowledged");
    }
    producer.close().await;

    let consumer = share_consumer_for(&broker, "delivery-group").await;
    let metrics = consumer.metrics();
    assert_eq!(metrics.records_received.get(), 0, "nothing polled yet");

    consumer
        .subscribe(&["events"])
        .await
        .expect("subscribe should reach the coordinator");

    let records = drain_share(&consumer, 5).await;
    assert_eq!(records.len(), 5, "every produced record must be delivered");

    let mut payloads: Vec<Vec<u8>> = records
        .iter()
        .filter_map(|r| r.value.as_ref().map(|v| v.to_vec()))
        .collect();
    payloads.sort();
    assert_eq!(
        payloads,
        (0..5u8).map(|i| vec![b'v', i]).collect::<Vec<_>>(),
        "delivered payloads must be the produced ones"
    );

    assert_eq!(
        metrics.records_received.get(),
        5,
        "records_received must match what poll() actually returned"
    );
    assert!(
        metrics.bytes_received.get() >= 10,
        "five two-byte values is at least ten bytes, saw {}",
        metrics.bytes_received.get()
    );
    assert!(
        metrics.polls.get() >= 1,
        "every poll() must be counted, empty or not"
    );

    let _ = consumer.close().await;
}

/// Accepted records must not be redelivered, and released records must be.
///
/// This is the property that replaces committed offsets in a share group. It
/// is the one thing a share consumer cannot be trusted without: an
/// acknowledgement that does not advance the share-partition start offset
/// turns every restart into a full replay, and a release that does not rewind
/// the cursor silently drops the record the application asked to retry.
#[cfg(feature = "unstable-protocol")]
#[tokio::test]
async fn accepting_retires_a_record_and_releasing_redelivers_it() {
    use crate::share_consumer::AcknowledgeType;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = producer_for(&broker).await;
    for i in 0..2u8 {
        let _ = producer.send("events", None, Some(&[i])).await.unwrap();
    }
    producer.close().await;

    // Implicit mode acknowledges everything the poll returned on the next
    // fetch, which would make "accept one, release the other" unexpressible.
    let consumer = share_consumer_with(&broker, "ack-group", |b| {
        b.acknowledgement_mode(crate::share_consumer::AcknowledgementMode::Explicit)
    })
    .await;
    consumer.subscribe(&["events"]).await.unwrap();

    let first = drain_share(&consumer, 2).await;
    assert_eq!(first.len(), 2);

    // Accept offset 0, release offset 1. Both acknowledgements are flushed on
    // the next fetch, which is where a real client piggybacks them too.
    for record in &first {
        let ack = if record.offset == 0 {
            AcknowledgeType::Accept
        } else {
            AcknowledgeType::Release
        };
        consumer
            .acknowledge(record, ack)
            .await
            .expect("acknowledgement should be accepted");
    }

    let redelivered = drain_share(&consumer, 1).await;
    assert!(
        !redelivered.is_empty(),
        "a released record must be handed out again"
    );
    assert!(
        redelivered.iter().all(|r| r.offset == 1),
        "only the released offset may come back, saw {:?}",
        redelivered.iter().map(|r| r.offset).collect::<Vec<_>>()
    );
    assert!(
        redelivered
            .iter()
            .all(|r| r.delivery_count.is_some_and(|c| c >= 2)),
        "a redelivery must report a delivery count above one, saw {:?}",
        redelivered
            .iter()
            .map(|r| r.delivery_count)
            .collect::<Vec<_>>()
    );

    let _ = consumer.close().await;
}

/// An accepted record must not come back to the next member of the group; an
/// unacknowledged one must.
///
/// This is the share-group replacement for "committed offsets survive a
/// restart", and it is the only assertion here that can tell an `ACCEPT`
/// apart from doing nothing. Within a single session it cannot: the
/// acquisition cursor has already moved past the record either way. The
/// difference only shows once the holder leaves and the in-flight records are
/// returned to the pool — at which point an accepted record is below the
/// share-partition start offset and an unacknowledged one is not.
#[cfg(feature = "unstable-protocol")]
#[tokio::test]
async fn an_accepted_record_is_not_redelivered_to_the_next_member() {
    use crate::share_consumer::{AcknowledgeType, AcknowledgementMode};

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = producer_for(&broker).await;
    for i in 0..2u8 {
        let _ = producer.send("events", None, Some(&[i])).await.unwrap();
    }
    producer.close().await;

    let first = share_consumer_with(&broker, "restart-group", |b| {
        b.acknowledgement_mode(AcknowledgementMode::Explicit)
    })
    .await;
    first.subscribe(&["events"]).await.unwrap();

    let records = drain_share(&first, 2).await;
    assert_eq!(
        records.len(),
        2,
        "both records should reach the first member"
    );

    // Accept offset 0 and nothing else. Offset 1 stays in flight.
    let accepted = records
        .iter()
        .find(|r| r.offset == 0)
        .expect("offset 0 should have been delivered");
    first
        .acknowledge(accepted, AcknowledgeType::Accept)
        .await
        .expect("acknowledgement should be accepted");
    // The acknowledgement is flushed on close; without it the accept would
    // never reach the broker and this test would prove nothing.
    first.close().await.expect("close should flush the ack");

    let second = share_consumer_for(&broker, "restart-group").await;
    second.subscribe(&["events"]).await.unwrap();

    let redelivered = drain_share(&second, 1).await;
    assert!(
        !redelivered.is_empty(),
        "the unacknowledged record must be handed to the next member"
    );
    assert!(
        redelivered.iter().all(|r| r.offset == 1),
        "an accepted record must never come back, saw offsets {:?}",
        redelivered.iter().map(|r| r.offset).collect::<Vec<_>>()
    );

    let _ = second.close().await;
}

/// Two members of one share group must divide the partitions between them,
/// and between them must see every record exactly once.
///
/// The interesting half is the second clause. A share group has no exclusive
/// ownership, so nothing in the protocol *prevents* the same record reaching
/// two members; what prevents it is the coordinator handing each partition's
/// share state to one member at a time. A client that ignored its assignment
/// and fetched every partition would still pass a "did I get records?" test
/// and fail this one.
#[cfg(feature = "unstable-protocol")]
#[tokio::test]
async fn two_share_group_members_split_the_partitions() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);

    let producer = producer_for(&broker).await;
    for partition in 0..2i32 {
        for i in 0..3u8 {
            let record = crate::producer::ProducerRecord::new("events", vec![partition as u8, i])
                .with_partition(partition);
            let _ = producer.send_record(record).await.unwrap();
        }
    }
    producer.close().await;

    let a = share_consumer_for(&broker, "split-group").await;
    let b = share_consumer_for(&broker, "split-group").await;
    a.subscribe(&["events"]).await.unwrap();
    b.subscribe(&["events"]).await.unwrap();

    // Let both members reach a steady assignment before inspecting it: the
    // first member is assigned everything until the second one joins.
    //
    // Records that arrive during this settling are kept, not discarded — the
    // whole point of the final assertion is that no record is delivered twice
    // and none is lost, and throwing away the first member's early deliveries
    // would hide both.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let drain = async |c: &crate::share_consumer::ShareConsumer, into: &mut Vec<Vec<u8>>| {
        if let Ok(records) = c.poll(Duration::from_millis(100)).await {
            into.extend(
                records
                    .into_iter()
                    .filter_map(|r| r.value.map(|v| v.to_vec())),
            );
        }
    };

    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        let (assign_a, assign_b) = (a.assignment().await, b.assignment().await);
        let count = |m: &ahash::AHashMap<String, Vec<crate::PartitionId>>| {
            m.values().map(Vec::len).sum::<usize>()
        };
        if count(&assign_a) == 1 && count(&assign_b) == 1 {
            break;
        }
        drain(&a, &mut seen).await;
        drain(&b, &mut seen).await;
    }

    let assign_a = a.assignment().await;
    let assign_b = b.assignment().await;
    let partitions = |m: &ahash::AHashMap<String, Vec<crate::PartitionId>>| {
        m.values().flatten().copied().collect::<HashSet<_>>()
    };
    let (pa, pb) = (partitions(&assign_a), partitions(&assign_b));
    assert_eq!(pa.len(), 1, "each member should hold one of two partitions");
    assert_eq!(pb.len(), 1);
    assert!(
        pa.is_disjoint(&pb),
        "the coordinator must not hand one partition to both members: {pa:?} vs {pb:?}"
    );

    let deadline = tokio::time::Instant::now() + SETTLE;
    while seen.len() < 6 && tokio::time::Instant::now() < deadline {
        drain(&a, &mut seen).await;
        drain(&b, &mut seen).await;
    }

    seen.sort();
    let expected: Vec<Vec<u8>> = (0..2u8)
        .flat_map(|p| (0..3u8).map(move |i| vec![p, i]))
        .collect();
    assert_eq!(
        seen, expected,
        "between them the two members must see every record exactly once"
    );

    let _ = a.close().await;
    let _ = b.close().await;
}

/// A poll with no subscription must be counted as an empty poll and deliver
/// nothing.
#[cfg(feature = "unstable-protocol")]
#[tokio::test]
async fn share_consumer_poll_metrics_are_wired() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = share_consumer_for(&broker, "metrics-share-group").await;

    let metrics = consumer.metrics();
    assert_eq!(metrics.polls.get(), 0, "no poll has happened yet");
    assert_eq!(metrics.empty_polls.get(), 0);

    // No subscription, so every poll legitimately returns nothing. That is
    // exactly the path `empty_polls` exists to count.
    for _ in 0..3 {
        let _ = consumer.poll(Duration::from_millis(20)).await;
    }

    assert_eq!(
        metrics.polls.get(),
        3,
        "every poll() must be counted, empty or not"
    );
    assert_eq!(
        metrics.empty_polls.get(),
        3,
        "a poll with no assignment is an empty poll"
    );
    assert_eq!(
        metrics.records_received.get(),
        0,
        "nothing was delivered, so nothing may be counted as delivered"
    );

    let _ = consumer.close().await;
}

// ══════════════════════════════════════════════════════════════════════════
// Transactions (KIP-98, KIP-360, KIP-447, KIP-890)
// ══════════════════════════════════════════════════════════════════════════
//
// These used to need Docker. The transactional paths — the two-phase commit,
// epoch fencing, `read_committed` isolation, offsets that move only when the
// transaction does — are the ones where a client bug costs data, and they were
// the ones the in-process broker could not reach: it served `InitProducerId`
// by minting a fresh producer ID and nothing else.
//
// Everything asserted below is client-observable. `transaction.version` is
// finalized through the same `ApiVersions` feature a real cluster uses, so the
// TV1/TV2 split is negotiated rather than injected.

use crate::consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crate::producer::{TopicPartitionOffset, TransactionVersion, TransactionalProducer};

async fn txn_producer_for(broker: &FakeBroker, transactional_id: &str) -> TransactionalProducer {
    TransactionalProducer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .transactional_id(transactional_id)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("transactional producer should connect")
}

/// A standalone consumer reading `topic` from the beginning at `isolation`.
async fn reader_for(broker: &FakeBroker, topic: &str, isolation: IsolationLevel) -> Consumer {
    let consumer = Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(isolation)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");
    consumer
        .assign(topic, vec![0])
        .await
        .expect("manual assignment should succeed");
    consumer
}

/// Poll until `deadline`, returning every record value seen.
async fn drain(consumer: &Consumer, polls: usize) -> Vec<String> {
    let mut values = Vec::new();
    for _ in 0..polls {
        if let Ok(records) = consumer.poll(Duration::from_millis(200)).await {
            for record in records {
                let value = record.value.as_deref().unwrap_or_default();
                values.push(String::from_utf8_lossy(value).into_owned());
            }
        }
    }
    values
}

/// A committed transaction must be visible to a `read_committed` consumer.
///
/// Negative control: making `end_txn` skip the commit marker and the
/// last-stable-offset release leaves the consumer with nothing, because the
/// fetch stops at the pinned LSO.
#[tokio::test]
async fn committed_transaction_becomes_visible_to_read_committed() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-visible").await;
    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");
    let _ = producer
        .send("orders", None, Some(b"committed-1"))
        .await
        .expect("send");

    // Before the commit the record is written but not stable: a
    // read_committed fetch must not be allowed past the transaction's first
    // offset.
    producer.flush().await.expect("flush");
    assert_eq!(
        broker.last_stable_offset("orders", 0),
        Some(0),
        "an open transaction must pin the last stable offset at its first record"
    );
    assert!(broker.transaction_is_open("txn-visible"));

    producer.commit_transaction().await.expect("commit");
    assert!(!broker.transaction_is_open("txn-visible"));

    let consumer = reader_for(&broker, "orders", IsolationLevel::ReadCommitted).await;
    let values = drain(&consumer, 6).await;
    assert_eq!(
        values,
        vec!["committed-1".to_string()],
        "a committed transaction must be delivered exactly once"
    );

    let _ = consumer.close().await;
    producer.close().await;
}

/// An aborted transaction must be invisible to a `read_committed` consumer,
/// and visible to a `read_uncommitted` one.
///
/// The two halves matter together: seeing nothing under `read_committed`
/// proves filtering happened only if the records were actually written, which
/// the `read_uncommitted` half establishes.
#[tokio::test]
async fn aborted_transaction_is_filtered_only_for_read_committed() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-abort").await;
    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");
    let _ = producer
        .send("orders", None, Some(b"doomed"))
        .await
        .expect("send");
    producer.abort_transaction().await.expect("abort");

    let (producer_id, _) = broker
        .transactional_producer("txn-abort")
        .expect("the coordinator knows this transactional id");
    assert_eq!(
        broker.aborted_transactions("orders", 0),
        vec![(producer_id, 0)],
        "the abort must be recorded so a read_committed fetch can report it"
    );

    let committed = reader_for(&broker, "orders", IsolationLevel::ReadCommitted).await;
    assert!(
        drain(&committed, 6).await.is_empty(),
        "read_committed must not surface records from an aborted transaction"
    );
    let _ = committed.close().await;

    let uncommitted = reader_for(&broker, "orders", IsolationLevel::ReadUncommitted).await;
    assert_eq!(
        drain(&uncommitted, 6).await,
        vec!["doomed".to_string()],
        "the records were written — read_uncommitted proves the filtering above \
         was filtering, not an empty log"
    );
    let _ = uncommitted.close().await;

    producer.close().await;
}

/// A committed transaction must not filter the *next* one from the same
/// producer.
///
/// The client clears a producer from its aborted set when it sees that
/// producer's control batch. A broker that writes no marker leaves the
/// producer flagged forever, so every later transaction silently disappears —
/// a failure that only shows up on the second transaction.
#[tokio::test]
async fn an_abort_does_not_poison_the_next_transaction() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-sequence").await;
    producer.init_transactions().await.expect("init");

    producer.begin_transaction().expect("begin 1");
    let _ = producer
        .send("orders", None, Some(b"aborted"))
        .await
        .expect("send");
    producer.abort_transaction().await.expect("abort");

    producer.begin_transaction().expect("begin 2");
    let _ = producer
        .send("orders", None, Some(b"committed"))
        .await
        .expect("send");
    producer.commit_transaction().await.expect("commit");

    let consumer = reader_for(&broker, "orders", IsolationLevel::ReadCommitted).await;
    assert_eq!(
        drain(&consumer, 8).await,
        vec!["committed".to_string()],
        "the second transaction must survive the first one's abort"
    );

    let _ = consumer.close().await;
    producer.close().await;
}

/// `committed_records` and `all_records` must differ by exactly the aborted
/// records — and must agree with what a real `read_committed` consumer sees.
///
/// The *difference* between the two accessors is what an exactly-once test is
/// actually asserting, which is why both exist. They read the broker's log
/// directly, so a test using them has no consumer, no bounded poll loop and no
/// iteration count to tune — the shape that quietly becomes flaky.
///
/// The last assertion is the one that matters: reading the log directly is only
/// useful if it agrees with the protocol path.
#[tokio::test]
async fn the_fake_brokers_two_record_views_differ_by_the_aborted_records() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-views").await;
    producer.init_transactions().await.expect("init");

    producer.begin_transaction().expect("begin 1");
    for i in 0..3 {
        let _ = producer
            .send("orders", None, Some(format!("aborted-{i}").as_bytes()))
            .await
            .expect("send");
    }
    producer.abort_transaction().await.expect("abort");

    producer.begin_transaction().expect("begin 2");
    for i in 0..2 {
        let _ = producer
            .send("orders", None, Some(format!("committed-{i}").as_bytes()))
            .await
            .expect("send");
    }
    producer.commit_transaction().await.expect("commit");
    producer.close().await;

    let committed = broker.committed_records("orders").expect("log decodes");
    let all = broker.all_records("orders").expect("log decodes");

    let values = |records: &[crate::consumer::ConsumerRecord]| -> Vec<String> {
        records
            .iter()
            .filter_map(|r| r.value.as_ref())
            .map(|v| String::from_utf8_lossy(v).into_owned())
            .collect()
    };

    assert_eq!(
        values(&committed),
        vec!["committed-0".to_string(), "committed-1".to_string()],
        "an aborted transaction must be invisible to the committed view"
    );
    assert_eq!(
        values(&all).len(),
        5,
        "the uncommitted view must show every record, aborted included"
    );
    assert!(
        !values(&all).iter().any(|v| v.starts_with("__")),
        "control batches are never records"
    );

    // The direct read must agree with the protocol path.
    let consumer = reader_for(&broker, "orders", IsolationLevel::ReadCommitted).await;
    assert_eq!(
        drain(&consumer, 8).await,
        values(&committed),
        "committed_records() must match what a read_committed consumer receives"
    );
    let _ = consumer.close().await;
}

/// Offsets sent to a transaction must move only when the transaction commits.
///
/// This is the consume-transform-produce guarantee: an aborted transaction
/// must leave the group's committed position exactly where it was, or the
/// records it read are lost.
#[tokio::test]
async fn transactional_offsets_move_only_on_commit() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-offsets").await;
    producer.init_transactions().await.expect("init");

    // KIP-447 requires a fenceable committer: the client refuses to stage
    // offsets carrying no generation, because such a commit could not be
    // rejected if this producer were a zombie. A real consumer supplies its
    // own metadata via `Consumer::group_metadata()`.
    let group = crate::consumer::ConsumerGroupMetadata::new("etl-group", 7, "member-1", None);
    let offsets = vec![TopicPartitionOffset::new("orders", 0, 42)];

    // Aborted: the group must not move.
    producer.begin_transaction().expect("begin 1");
    let _ = producer
        .send("orders", None, Some(b"x"))
        .await
        .expect("send");
    producer
        .send_offsets_to_transaction(&offsets, &group)
        .await
        .expect("stage offsets");
    producer.abort_transaction().await.expect("abort");
    assert_eq!(
        broker.committed_offset("etl-group", "orders", 0),
        None,
        "an aborted transaction must not commit the offsets it staged"
    );

    // Committed: the group moves to the staged position.
    producer.begin_transaction().expect("begin 2");
    let _ = producer
        .send("orders", None, Some(b"y"))
        .await
        .expect("send");
    producer
        .send_offsets_to_transaction(&offsets, &group)
        .await
        .expect("stage offsets");
    producer.commit_transaction().await.expect("commit");
    assert_eq!(
        broker.committed_offset("etl-group", "orders", 0),
        Some(42),
        "a committed transaction must apply the offsets it staged"
    );

    producer.close().await;
}

/// Re-initialising a transactional ID must fence the previous incarnation
/// (KIP-360).
///
/// The producer ID stays the same and the epoch rises; the old producer's
/// writes are then rejected with a fatal error it cannot abort out of.
#[tokio::test]
async fn re_initialising_fences_the_previous_producer() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let zombie = txn_producer_for(&broker, "txn-fenced").await;
    zombie.init_transactions().await.expect("init");
    let (first_pid, first_epoch) = broker.transactional_producer("txn-fenced").unwrap();

    let successor = txn_producer_for(&broker, "txn-fenced").await;
    successor.init_transactions().await.expect("init");
    let (second_pid, second_epoch) = broker.transactional_producer("txn-fenced").unwrap();

    assert_eq!(
        second_pid, first_pid,
        "the producer ID is the fencing identity and must be stable"
    );
    assert!(
        second_epoch > first_epoch,
        "a new incarnation must get a higher epoch ({second_epoch} vs {first_epoch})"
    );

    // The zombie is now writing with a stale epoch. Its next transactional
    // operation must fail fatally rather than silently interleaving.
    zombie.begin_transaction().expect("begin");
    let outcome = zombie.send("orders", None, Some(b"zombie")).await;
    let outcome = match outcome {
        Err(e) => Err(e),
        // The send may be accepted into the accumulator; the commit is where
        // the coordinator rejects the stale epoch.
        Ok(_) => zombie.commit_transaction().await.map(|()| unreachable!()),
    };
    assert!(
        outcome.is_err(),
        "a fenced producer must not be able to complete a transaction"
    );
    assert_eq!(
        zombie.state(),
        crate::producer::TransactionState::FatalError,
        "a fencing error is fatal: the producer must be recreated, not retried"
    );

    successor.close().await;
}

/// TV1 is the default, and it registers partitions explicitly.
#[tokio::test]
async fn tv1_registers_partitions_with_add_partitions_to_txn() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-tv1").await;
    producer.init_transactions().await.expect("init");
    assert_eq!(
        producer.transaction_version(),
        TransactionVersion::V1,
        "a cluster that has not finalized transaction.version is TV1"
    );

    producer.begin_transaction().expect("begin");
    let _ = producer
        .send("orders", None, Some(b"v1"))
        .await
        .expect("send");
    producer.commit_transaction().await.expect("commit");

    assert!(
        broker.request_count(ApiKey::AddPartitionsToTxn) > 0,
        "TV1 must register each partition with the coordinator before writing"
    );

    let consumer = reader_for(&broker, "orders", IsolationLevel::ReadCommitted).await;
    assert_eq!(drain(&consumer, 6).await, vec!["v1".to_string()]);
    let _ = consumer.close().await;
    producer.close().await;
}

/// TV2 (KIP-890) must skip `AddPartitionsToTxn` entirely and still commit.
///
/// Eliminating that coordinator round trip per partition per transaction is
/// the whole throughput point of TV2, so a client that sends it anyway is
/// wrong even though the transaction still works. Asserting the count is zero
/// is the only way to see that.
#[tokio::test]
async fn tv2_commits_without_add_partitions_to_txn() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    broker.set_transaction_version(2);

    let producer = txn_producer_for(&broker, "txn-tv2").await;
    producer.init_transactions().await.expect("init");
    assert_eq!(
        producer.transaction_version(),
        TransactionVersion::V2,
        "a cluster finalizing transaction.version=2 must negotiate TV2"
    );

    let (_, epoch_before) = broker.transactional_producer("txn-tv2").unwrap();

    producer.begin_transaction().expect("begin");
    let _ = producer
        .send("orders", None, Some(b"v2"))
        .await
        .expect("send");
    producer.commit_transaction().await.expect("commit");

    assert_eq!(
        broker.request_count(ApiKey::AddPartitionsToTxn),
        0,
        "TV2 carries the transactional ID on Produce; the extra round trip must be gone"
    );

    let (_, epoch_after) = broker.transactional_producer("txn-tv2").unwrap();
    assert!(
        epoch_after > epoch_before,
        "KIP-890 bumps the producer epoch at every transaction completion"
    );

    let consumer = reader_for(&broker, "orders", IsolationLevel::ReadCommitted).await;
    assert_eq!(drain(&consumer, 6).await, vec!["v2".to_string()]);
    let _ = consumer.close().await;
    producer.close().await;
}

/// A transaction spanning two partitions must commit atomically.
#[tokio::test]
async fn a_multi_partition_transaction_commits_atomically() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 2);

    let producer = txn_producer_for(&broker, "txn-multi").await;
    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");

    for partition in 0..2 {
        let record = crate::producer::ProducerRecord::new(
            "orders",
            bytes::Bytes::from(format!("p{partition}")),
        )
        .with_partition(partition);
        let _ = producer.send_record(record).await.expect("send");
    }
    producer.flush().await.expect("flush");

    for partition in 0..2 {
        assert_eq!(
            broker.last_stable_offset("orders", partition),
            Some(0),
            "every partition in the transaction must be pinned until the commit"
        );
    }

    producer.commit_transaction().await.expect("commit");

    for partition in 0..2 {
        assert!(
            broker.last_stable_offset("orders", partition) > Some(0),
            "the commit must release every partition, not just the first"
        );
    }
    producer.close().await;
}

/// An old abort must not filter a later committed transaction from the same
/// producer, once the consumer has read past the abort marker.
///
/// The client activates an aborted-transaction entry as soon as it scans a
/// batch at or past that entry's `first_offset`. A broker that reports every
/// abort it has ever seen — regardless of the range being fetched — therefore
/// re-flags the producer on a later fetch and silently drops its **committed**
/// records. The bug is invisible on the first poll and appears on the second,
/// which is exactly the shape that survives a casual test.
///
/// Negative control: making `aborted_transactions_from` return the whole list
/// instead of the overlapping ones fails this.
#[tokio::test]
async fn an_old_abort_is_not_reported_to_a_consumer_that_has_read_past_it() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = txn_producer_for(&broker, "txn-stale").await;
    producer.init_transactions().await.expect("init");

    producer.begin_transaction().expect("begin 1");
    let _ = producer
        .send("orders", None, Some(b"aborted"))
        .await
        .expect("send");
    producer.abort_transaction().await.expect("abort");

    producer.begin_transaction().expect("begin 2");
    let _ = producer
        .send("orders", None, Some(b"committed"))
        .await
        .expect("send");
    producer.commit_transaction().await.expect("commit");

    // Fetching from *after* the abort marker must see the committed record.
    // The abort lives at offsets 0–1, so offset 2 is past it.
    let marker_end = 2;
    assert!(
        broker
            .aborted_transactions("orders", 0)
            .iter()
            .all(|(_, first)| *first < marker_end),
        "the abort under test must lie below the fetch offset"
    );

    let consumer = Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .isolation_level(IsolationLevel::ReadCommitted)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");
    consumer
        .assign("orders", vec![0])
        .await
        .expect("manual assignment");
    consumer
        .seek("orders", 0, marker_end)
        .await
        .expect("seek past the abort marker");

    assert_eq!(
        drain(&consumer, 8).await,
        vec!["committed".to_string()],
        "a consumer that has read past an abort must still receive the \
         committed transaction that follows it"
    );

    let _ = consumer.close().await;
    producer.close().await;
}

// ── Prefetch buffer ────────────────────────────────────────────────────────

/// Records fetched past the delivery cap must be *parked*, not thrown away.
///
/// The consumer decodes one delivery's worth plus the buffer's free capacity,
/// so a fetch that returns more than `max_poll_records` fills the buffer and
/// the *next* poll is served from memory with no Fetch on the wire. The
/// previous design truncated the surplus and re-fetched it, which paid for the
/// same bytes twice — once in decode, once on the network.
///
/// Counting Fetch requests is what makes this a real assertion: comparing only
/// the records returned would pass just as well against the old behaviour.
#[tokio::test]
async fn a_second_poll_is_served_from_the_prefetch_buffer_without_a_fetch() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = crate::producer::Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("producer should connect");
    for i in 0..20u32 {
        let _ = producer
            .send("events", None, Some(format!("v{i}").as_bytes()))
            .await
            .expect("send");
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_records(5)
        .max_buffered_records(50)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");
    consumer
        .assign("events", vec![0])
        .await
        .expect("manual assignment");

    // First poll: goes to the broker and comes back with the delivery cap.
    let first = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("first poll");
    assert_eq!(first.len(), 5, "the delivery cap must still be honoured");
    let fetches_after_first = broker.request_count(ApiKey::Fetch);
    assert!(
        fetches_after_first >= 1,
        "the first poll has to reach the broker"
    );

    // Second poll: served entirely from the buffer the first poll filled.
    let second = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect("second poll");
    assert_eq!(second.len(), 5, "the buffer must serve a full batch");
    assert_eq!(
        broker.request_count(ApiKey::Fetch),
        fetches_after_first,
        "a poll served from the prefetch buffer must not issue a Fetch"
    );

    // Offsets are contiguous across the boundary: nothing skipped, nothing
    // duplicated by the park-and-serve round trip.
    let seen: Vec<i64> = first
        .iter()
        .chain(second.iter())
        .map(|r| r.offset)
        .collect();
    assert_eq!(seen, (0..10).collect::<Vec<i64>>());

    let _ = consumer.close().await;
}

/// Parking records must never let the commit run ahead of delivery.
///
/// The fetch position advances over everything fetched, including the parked
/// surplus. If the committed offset followed the fetch position, a crash after
/// the first poll would skip every parked record. `committable_positions`
/// holds the commit at the first undelivered offset instead, and this asserts
/// that end to end rather than through the helper.
#[tokio::test]
async fn a_commit_never_acknowledges_records_still_parked_in_the_buffer() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let producer = crate::producer::Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("producer should connect");
    for i in 0..20u32 {
        let _ = producer
            .send("events", None, Some(format!("v{i}").as_bytes()))
            .await
            .expect("send");
    }
    producer.close().await;

    let consumer = Consumer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .group_id("prefetch-commit-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .max_poll_records(5)
        .max_buffered_records(50)
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("consumer should connect");
    consumer.subscribe(&["events"]).await.unwrap();

    // Poll until a batch arrives; the surplus lands in the buffer.
    let mut delivered = 0usize;
    for _ in 0..10 {
        let records = consumer
            .poll(Duration::from_millis(300))
            .await
            .expect("poll");
        delivered += records.len();
        if delivered > 0 {
            break;
        }
    }
    assert!(delivered > 0, "the consumer must receive something");

    // `position()` reports where delivery is, `fetch_position()` where the
    // read-ahead is. They must differ by exactly what is parked, and the
    // commit must follow `position()`.
    let position = consumer
        .position("events", 0)
        .await
        .expect("position must be tracked");
    let fetch_position = consumer
        .fetch_position("events", 0)
        .await
        .expect("fetch position must be tracked");
    assert_eq!(
        position, delivered as i64,
        "position() must report the delivered offset, not the read-ahead"
    );
    assert!(
        fetch_position > position,
        "the consumer must have read ahead of delivery, got fetch={fetch_position} \
         position={position}"
    );

    consumer.commit().await.expect("commit");

    let committed = broker
        .committed_offset("prefetch-commit-group", "events", 0)
        .expect("the group must have a committed offset");
    assert_eq!(
        committed, delivered as i64,
        "the commit must acknowledge exactly what was delivered — a commit at \
         the fetch position would skip the parked surplus on restart"
    );
    assert_eq!(
        committed, position,
        "commit() and position() must never disagree"
    );

    let _ = consumer.close().await;
}

// ── Transaction state machine: the commit closes before it drains ──────────

/// A commit must stop admitting records *before* it drains the accumulator,
/// not after.
///
/// `send_record` admits a record when it observes `InTransaction`. The commit
/// path used to flush first and transition second, which left a window between
/// the flush completing and the state changing where a concurrent send was
/// still accepted — and its record was then still buffered when `EndTxn` went
/// out. It would either be rejected by the broker as `INVALID_TXN_STATE` or,
/// once `begin_transaction` had been called again, silently join the *next*
/// transaction: a record the application was told had been committed could
/// disappear when a later transaction aborted.
///
/// The test holds the commit inside its drain (by delaying `Produce`) and
/// asserts that a send issued during that window is refused. Under the old
/// ordering the state observed here is `InTransaction` and the send is
/// accepted.
#[tokio::test]
async fn a_commit_stops_admitting_records_before_it_drains() {
    use std::sync::Arc;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = Arc::new(
        TransactionalProducer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .transactional_id("txn-commit-ordering")
            // Batch, so the record below sits in the accumulator and the
            // commit's flush is what pushes it to the broker.
            .linger(Duration::from_millis(200))
            .request_timeout(SHORT_REQUEST_TIMEOUT)
            .connect_timeout(SHORT_CONNECT_TIMEOUT)
            .build()
            .await
            .expect("transactional producer should connect"),
    );
    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");

    // One record, buffered by the linger window.
    let buffered = Arc::clone(&producer);
    let send = tokio::spawn(async move { buffered.send("orders", None, Some(b"first")).await });

    // Hold the commit inside its flush.
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(600))
    });

    let committing = Arc::clone(&producer);
    let commit = tokio::spawn(async move { committing.commit_transaction().await });

    // Give the commit time to transition and enter its drain.
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert_eq!(
        producer.state(),
        crate::producer::TransactionState::Committing,
        "the commit must own the state while it drains, so no further record \
         can be admitted into a transaction that is already closing"
    );

    let refused = producer.send("orders", None, Some(b"too-late")).await;
    let error = refused.expect_err("a send during the commit's drain must be refused");
    assert!(
        error.to_string().contains("Committing"),
        "the refusal must name the state that caused it, got: {error}"
    );

    broker.clear_hooks();
    let _ = send.await.expect("send task should not panic");
    let _ = commit.await.expect("commit task should not panic");

    producer.close().await;
}

/// A commit must not write the `EndTxn` marker while `send_offsets_to_transaction`
/// is still in flight.
///
/// `send_offsets_to_transaction` is the join between the consumer's position
/// and the producer's output — the whole point of consume-transform-produce is
/// that the two commit atomically. It did not register with the in-flight
/// barrier, so a concurrent `commit_transaction()` could not see it: the commit
/// would transition, find the barrier idle, flush and send `EndTxn` while the
/// `TxnOffsetCommit` was still on the wire, leaving the offsets outside the
/// transaction.
///
/// # Why this needs two brokers
///
/// `TxnOffsetCommit` goes to the **group** coordinator and `EndTxn` to the
/// **transaction** coordinator. On a single node they share one connection, and
/// the broker's own per-connection serialisation masks the client-side race —
/// an earlier version of this test passed with the fix reverted for exactly
/// that reason. Splitting the two coordinators across nodes gives them
/// independent connections, which is the arrangement a real cluster has.
#[tokio::test]
async fn a_commit_waits_for_an_in_flight_offset_commit() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::consumer::ConsumerGroupMetadata;
    use crate::producer::TopicPartitionOffset;

    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("orders", 1);
    // Independent connections: the delayed offset commit cannot queue the
    // EndTxn behind it.
    broker.set_group_coordinator("g", 0);
    broker.set_txn_coordinator("txn-offsets-ordering", 1);

    let producer = Arc::new(
        TransactionalProducer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .transactional_id("txn-offsets-ordering")
            .request_timeout(SHORT_REQUEST_TIMEOUT)
            .connect_timeout(SHORT_CONNECT_TIMEOUT)
            .build()
            .await
            .expect("transactional producer should connect"),
    );
    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");
    let _ = producer
        .send("orders", None, Some(b"payload"))
        .await
        .expect("send");

    // Hold the offset commit on the wire, on the group coordinator only.
    broker.on(ApiKey::TxnOffsetCommit, |_| {
        Control::Delay(Duration::from_millis(500))
    });

    let done = Arc::new(AtomicBool::new(false));
    let offsets_done = Arc::clone(&done);
    let offsets_producer = Arc::clone(&producer);
    let offsets = tokio::spawn(async move {
        let metadata = ConsumerGroupMetadata::new("g", 1, "member-1", None);
        let result = offsets_producer
            .send_offsets_to_transaction(&[TopicPartitionOffset::new("orders", 0, 42)], &metadata)
            .await;
        offsets_done.store(true, Ordering::SeqCst);
        result
    });

    // Let the offset commit register with the barrier and reach the broker.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !done.load(Ordering::SeqCst),
        "the offset commit must still be in flight for this test to mean anything"
    );

    producer.commit_transaction().await.expect("commit");

    assert!(
        done.load(Ordering::SeqCst),
        "commit_transaction() returned while TxnOffsetCommit was still in flight —          the EndTxn marker would have been written with the offsets outside the          transaction"
    );

    let offsets_result = offsets.await.expect("offset task should not panic");
    assert!(
        offsets_result.is_ok(),
        "the offset commit should complete inside the transaction: {offsets_result:?}"
    );

    broker.clear_hooks();
    producer.close().await;
}

// ── Share consumer: a flush must not race a poll holding the acks ─────────

/// `commit_sync()` must not report success while a concurrent `poll()` is
/// holding the acknowledgements.
///
/// `poll()` drains every entry out of `pending_acks` into a `PendingAckGuard`
/// for the duration of its `ShareFetch`. During that window the map is empty,
/// so a `commit_sync()` (or the flush inside `close()`) would take nothing,
/// report success, and strand the acknowledgements the guard restores a moment
/// later — leaving the records to be redelivered even though the application
/// had explicitly acknowledged them.
///
/// The documented shutdown is `wakeup()` then `close()`, and `wakeup()` does
/// not wait for the poll it interrupts to unwind, so this interleaving is the
/// normal one rather than an exotic race.
#[cfg(feature = "unstable-protocol")]
#[tokio::test]
async fn a_flush_waits_for_a_poll_holding_the_acknowledgements() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);

    let consumer = Arc::new(share_consumer_for(&broker, "share-flush-race").await);
    consumer.subscribe(&["events"]).await.unwrap();
    // Let the group settle so the next poll reaches the fetch stage.
    let _ = consumer.poll(Duration::from_millis(300)).await;

    // Hold the poll inside its ShareFetch, with the acks drained out of the map.
    broker.on(ApiKey::ShareFetch, |_| {
        Control::Delay(Duration::from_millis(600))
    });

    let polling = Arc::clone(&consumer);
    let poll_done = Arc::new(AtomicBool::new(false));
    let poll_flag = Arc::clone(&poll_done);
    let poll = tokio::spawn(async move {
        let out = polling.poll(Duration::from_secs(2)).await;
        poll_flag.store(true, Ordering::SeqCst);
        out
    });

    // Let the poll register with the barrier and drain the acks.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !poll_done.load(Ordering::SeqCst),
        "the poll must still be in flight for this test to mean anything"
    );

    consumer.commit_sync().await.expect("commit_sync");

    assert!(
        poll_done.load(Ordering::SeqCst),
        "commit_sync() returned while a poll was still holding the pending \
         acknowledgements — it would have flushed an empty map and reported \
         success, stranding them"
    );

    broker.clear_hooks();
    let _ = poll.await.expect("poll task should not panic");
    let _ = consumer.close().await;
}

// ---------------------------------------------------------------------------
// Tombstones: a null value must survive the whole produce path
// ---------------------------------------------------------------------------

/// The end-to-end proof that krafka can write a Kafka tombstone.
///
/// Everything between `ProducerRecord::tombstone` and the log is exercised for
/// real here — interceptors, validation, size estimation, batch encoding, the
/// Produce request — and the record is read back off the broker's log through
/// the same decoder a consumer uses. The three records pin the distinction the
/// wire format makes: a null value, a zero-length value, and an ordinary value
/// must come back as three different things.
#[tokio::test]
async fn a_tombstone_reaches_the_log_as_a_null_value() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("users", 1);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("producer should connect");

    // 1. An ordinary record.
    let _ = producer
        .send("users", Some(b"user-42"), Some(b"alice"))
        .await
        .expect("valued send should be acknowledged");

    // 2. A zero-length value — compaction keeps this one.
    let _ = producer
        .send("users", Some(b"user-42"), Some(b""))
        .await
        .expect("empty send should be acknowledged");

    // 3. The tombstone — compaction deletes the key for this one.
    let _ = producer
        .send_record(
            crate::producer::ProducerRecord::tombstone("users", "user-42")
                .with_header("X-Reason", &b"gdpr-erasure"[..])
                .with_null_header("X-Flag"),
        )
        .await
        .expect("tombstone send should be acknowledged");

    let stored = broker.all_records("users").expect("log should be readable");
    assert_eq!(stored.len(), 3, "all three records should be in the log");

    assert_eq!(stored[0].value.as_deref(), Some(&b"alice"[..]));
    assert!(!stored[0].is_tombstone());

    assert_eq!(
        stored[1].value.as_deref(),
        Some(&b""[..]),
        "an empty value must not decode as null"
    );
    assert!(
        !stored[1].is_tombstone(),
        "a zero-length value is an ordinary record"
    );

    assert_eq!(stored[2].value, None, "the tombstone must decode as null");
    assert!(stored[2].is_tombstone());
    assert_eq!(stored[2].key.as_deref(), Some(&b"user-42"[..]));

    // Header nullness survives the same round trip.
    let headers = &stored[2].headers;
    assert_eq!(headers[0].0.as_ref(), b"X-Reason");
    assert_eq!(headers[0].1.as_deref(), Some(&b"gdpr-erasure"[..]));
    assert_eq!(headers[1].0.as_ref(), b"X-Flag");
    assert_eq!(headers[1].1, None, "a null header value must stay null");
}

/// A tombstone must land on the **same partition** as the records it deletes,
/// or compaction — which is per-partition — never sees the two together.
///
/// The default partitioner hashes the key, and a tombstone carries the same
/// key as the record it retires, so this holds without the caller pinning a
/// partition by hand. That is worth an assertion rather than an argument.
#[tokio::test]
async fn a_tombstone_routes_to_the_same_partition_as_its_key() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("users", 8);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("producer should connect");

    for key in ["user-1", "user-2", "user-3", "user-4"] {
        let valued = producer
            .send("users", Some(key.as_bytes()), Some(b"payload"))
            .await
            .expect("valued send should be acknowledged");
        let tombstone = producer
            .send("users", Some(key.as_bytes()), None)
            .await
            .expect("tombstone send should be acknowledged");

        assert_eq!(
            valued.partition, tombstone.partition,
            "the tombstone for {key} must share its record's partition"
        );
    }
}

/// Close the loop: records produced by krafka, read back off the log, and fed
/// to krafka's own `CompactedTable` must delete the key.
///
/// The table's own unit tests build `ConsumerRecord`s by hand. This one starts
/// from `ProducerRecord::tombstone` and goes through encoding and decoding, so
/// a null that collapsed anywhere on the produce path would leave the key in
/// the table instead of removing it.
#[tokio::test]
async fn a_produced_tombstone_deletes_a_key_from_a_compacted_table() {
    use crate::consumer::CompactedTable;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("users", 1);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .build()
        .await
        .expect("producer should connect");

    let _ = producer
        .send("users", Some(b"keep-me"), Some(b"v"))
        .await
        .expect("send should be acknowledged");
    let _ = producer
        .send("users", Some(b"delete-me"), Some(b"v"))
        .await
        .expect("send should be acknowledged");

    let mut table = CompactedTable::new();
    table.ingest(&broker.all_records("users").expect("log should be readable"));
    assert_eq!(table.len(), 2, "both keys should be present");

    let _ = producer
        .send_record(crate::producer::ProducerRecord::tombstone(
            "users",
            "delete-me",
        ))
        .await
        .expect("tombstone send should be acknowledged");

    let mut table = CompactedTable::new();
    let changes = table.apply(&broker.all_records("users").expect("log should be readable"));

    assert_eq!(table.len(), 1, "the tombstoned key should be gone");
    assert!(table.get_value(b"keep-me").is_some());
    assert!(table.get_value(b"delete-me").is_none());
    assert!(
        changes.iter().any(|c| c.is_delete()),
        "the tombstone should be reported as a deletion"
    );
}

// ---------------------------------------------------------------------------
// Producer interceptors: per-record state, and the on_send/on_acknowledgement
// pairing that makes it safe to hold
// ---------------------------------------------------------------------------

/// Records what an interceptor observed for every record it saw.
///
/// `on_send` parks a token in the record's context; `on_acknowledgement` takes
/// it back and logs it next to the terminal metadata. A missing token means the
/// context did not survive the trip, and a missing log entry means the record
/// never reached the terminal callback at all — the two failure modes these
/// tests exist to catch.
#[derive(Debug, Default)]
struct RecordingInterceptor {
    sends: std::sync::atomic::AtomicUsize,
    acks: std::sync::Mutex<Vec<AckObservation>>,
}

/// One terminal callback, as the interceptor saw it.
#[derive(Debug)]
struct AckObservation {
    /// The token `on_send` stored, if it came back.
    token: Option<String>,
    /// Header keys visible at acknowledgement time, in order.
    header_keys: Vec<String>,
    partition: crate::PartitionId,
    offset: i64,
    delivery: crate::producer::DeliveryConfirmation,
    failed: bool,
}

/// The value a `RecordingInterceptor` parks in each record's context.
struct SendToken(String);

impl RecordingInterceptor {
    fn sends(&self) -> usize {
        self.sends.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn acks(&self) -> std::sync::MutexGuard<'_, Vec<AckObservation>> {
        self.acks.lock().unwrap()
    }
}

impl crate::interceptor::ProducerInterceptor for RecordingInterceptor {
    fn on_send(
        &self,
        record: &mut crate::producer::ProducerRecord,
        ctx: &mut crate::interceptor::RecordContext,
    ) -> crate::interceptor::InterceptorResult {
        let n = self.sends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ctx.insert(SendToken(format!("{}#{n}", record.topic)));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        metadata: &crate::producer::RecordMetadata,
        error: Option<&crate::error::KrafkaError>,
        headers: &crate::producer::RecordHeaders,
        ctx: &mut crate::interceptor::RecordContext,
    ) -> crate::interceptor::InterceptorResult {
        self.acks().push(AckObservation {
            token: ctx.take::<SendToken>().map(|t| t.0),
            header_keys: headers.iter().map(|(k, _)| k.clone()).collect(),
            partition: metadata.partition,
            offset: metadata.offset,
            delivery: metadata.delivery,
            failed: error.is_some(),
        });
        Ok(())
    }
}

async fn producer_with(
    broker: &FakeBroker,
    interceptor: &std::sync::Arc<RecordingInterceptor>,
) -> Producer {
    Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .add_interceptor(std::sync::Arc::clone(interceptor) as std::sync::Arc<_>)
        .build()
        .await
        .expect("producer should connect")
}

/// The whole point of `RecordContext`: what `on_send` parks comes back to
/// `on_acknowledgement`, for the right record, with real delivery metadata.
#[tokio::test]
async fn interceptor_state_survives_from_on_send_to_on_acknowledgement() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = producer_with(&broker, &interceptor).await;

    for i in 0..3u8 {
        let _ = producer
            .send("events", None, Some(&[b'v', i]))
            .await
            .expect("send should be acknowledged");
    }

    let acks = interceptor.acks();
    assert_eq!(acks.len(), 3, "every record must reach on_acknowledgement");
    let tokens: Vec<_> = acks.iter().filter_map(|a| a.token.clone()).collect();
    assert_eq!(
        tokens,
        vec!["events#0", "events#1", "events#2"],
        "each record must get *its own* context back, in order"
    );
    for ack in acks.iter() {
        assert!(!ack.failed, "a successful send reports no error");
        assert_eq!(ack.delivery, crate::producer::DeliveryConfirmation::Offset);
        assert!(ack.offset >= 0, "a successful send carries a real offset");
    }
}

/// A record rejected by validation never reaches the accumulator. It still owes
/// an acknowledgement, because `on_send` already ran and may already be holding
/// a span.
#[tokio::test]
async fn a_record_rejected_by_validation_still_reaches_on_acknowledgement() {
    let broker = FakeBroker::start().await.unwrap();
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = producer_with(&broker, &interceptor).await;

    let mut record = crate::producer::ProducerRecord::new("events", b"v".to_vec());
    for i in 0..(crate::protocol::MAX_RECORD_HEADERS + 1) {
        record = record.with_header(format!("h{i}"), bytes::Bytes::from_static(b"x"));
    }

    let error = producer
        .send_record(record)
        .await
        .expect_err("a record over the header limit must be rejected");
    assert!(error.to_string().contains("headers"));

    let acks = interceptor.acks();
    assert_eq!(interceptor.sends(), 1);
    assert_eq!(acks.len(), 1, "the rejected record still owes a callback");
    assert_eq!(
        acks[0].token.as_deref(),
        Some("events#0"),
        "the context opened in on_send must come back, not be dropped"
    );
    assert!(acks[0].failed);
    assert_eq!(acks[0].partition, crate::producer::UNKNOWN_PARTITION);
    assert_eq!(acks[0].offset, -1);
    assert_eq!(
        acks[0].delivery,
        crate::producer::DeliveryConfirmation::Failed
    );
}

/// Same guarantee one step earlier in the path: a serializer that fails runs
/// after `on_send` and before anything else, and used to end the record's life
/// silently.
#[tokio::test]
async fn a_record_rejected_by_a_serializer_still_reaches_on_acknowledgement() {
    /// A serializer that always fails, standing in for a schema registry that
    /// rejects a payload.
    #[derive(Debug)]
    struct FailingSerializer;

    impl crate::serdes::Serializer for FailingSerializer {
        fn serialize(
            &self,
            _payload: bytes::Bytes,
            _topic: &str,
            _record_name: Option<&str>,
            _is_key: bool,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = crate::error::Result<bytes::Bytes>> + Send + '_>,
        > {
            Box::pin(async {
                Err(crate::error::KrafkaError::invalid_state(
                    "schema registry rejected the payload",
                ))
            })
        }
    }

    let broker = FakeBroker::start().await.unwrap();
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .add_interceptor(std::sync::Arc::clone(&interceptor) as std::sync::Arc<_>)
        .value_serializer(std::sync::Arc::new(FailingSerializer))
        .build()
        .await
        .expect("producer should connect");

    let _ = producer
        .send("events", None, Some(b"v"))
        .await
        .expect_err("the serializer must reject the record");

    let acks = interceptor.acks();
    assert_eq!(acks.len(), 1, "the rejected record still owes a callback");
    assert_eq!(acks[0].token.as_deref(), Some("events#0"));
    assert!(acks[0].failed);
    assert_eq!(acks[0].partition, crate::producer::UNKNOWN_PARTITION);
}

/// Dropping a `DeliveryHandle` discards the *caller's* view of the
/// acknowledgement. The interceptor's view is not the caller's, so an
/// interceptor holding a span must still be told how the record ended.
#[tokio::test]
async fn a_dropped_delivery_handle_does_not_suppress_on_acknowledgement() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = producer_with(&broker, &interceptor).await;

    for i in 0..3u8 {
        // Enqueued, then the handle is dropped on the spot.
        drop(
            producer
                .enqueue(crate::producer::ProducerRecord::new(
                    "events",
                    vec![b'v', i],
                ))
                .await
                .expect("enqueue should succeed"),
        );
    }
    producer
        .flush()
        .await
        .expect("flush should drain the buffer");

    let acks = interceptor.acks();
    assert_eq!(
        acks.len(),
        3,
        "dropping the handle must not cost the interceptor its callback"
    );
    for ack in acks.iter() {
        assert!(ack.token.is_some(), "the context must survive the drop");
        assert!(!ack.failed);
    }
}

/// The last of the pre-accumulator rejections: routing itself fails, so there
/// is not even a partition to report.
#[tokio::test]
async fn a_record_for_an_unknown_topic_still_reaches_on_acknowledgement() {
    let broker = FakeBroker::start().await.unwrap();
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = producer_with(&broker, &interceptor).await;

    let error = producer
        .send("no-such-topic", None, Some(b"v"))
        .await
        .expect_err("an unrouteable record must be rejected");
    assert!(error.to_string().contains("unknown topic"));

    let acks = interceptor.acks();
    assert_eq!(acks.len(), 1, "the rejected record still owes a callback");
    assert_eq!(acks[0].token.as_deref(), Some("no-such-topic#0"));
    assert!(acks[0].failed);
    assert_eq!(acks[0].partition, crate::producer::UNKNOWN_PARTITION);
    assert_eq!(
        acks[0].delivery,
        crate::producer::DeliveryConfirmation::Failed
    );
}

/// A batch rejected as too large is halved and both halves resubmitted. The
/// records move into *different* batches partway through their lives, so this
/// is where a context keyed to the batch rather than the record would come
/// apart — and where a record could plausibly be acknowledged twice, or not at
/// all.
#[tokio::test]
async fn a_split_batch_acknowledges_every_record_exactly_once_with_its_context() {
    const RECORDS: usize = 4;

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    // Reject the first Produce as oversized; the two halves that follow are
    // answered normally.
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::MessageTooLarge)
    });

    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        // Long enough that all four records share one batch.
        .linger(Duration::from_millis(200))
        .add_interceptor(std::sync::Arc::clone(&interceptor) as std::sync::Arc<_>)
        .build()
        .await
        .expect("producer should connect");

    for i in 0..RECORDS {
        drop(
            producer
                .enqueue(
                    crate::producer::ProducerRecord::new("events", vec![b'v', i as u8])
                        .with_partition(0),
                )
                .await
                .expect("enqueue should succeed"),
        );
    }
    producer
        .flush()
        .await
        .expect("flush should drain the buffer");

    assert!(
        broker.request_count(ApiKey::Produce) >= 3,
        "expected the rejected batch plus two halves, saw {}",
        broker.request_count(ApiKey::Produce)
    );

    let acks = interceptor.acks();
    assert_eq!(
        acks.len(),
        RECORDS,
        "exactly one acknowledgement per record — no losses, no duplicates"
    );
    let mut tokens: Vec<_> = acks.iter().filter_map(|a| a.token.clone()).collect();
    tokens.sort();
    assert_eq!(
        tokens,
        vec!["events#0", "events#1", "events#2", "events#3"],
        "every record must carry its own context across the split"
    );
    for ack in acks.iter() {
        assert!(!ack.failed, "both halves should have been accepted");
    }
}

/// The transactional producer has its own send path, its own failure modes and
/// its own copy of the obligation wiring. It must honour the same contract.
#[tokio::test]
async fn the_transactional_send_path_pairs_on_send_with_on_acknowledgement() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = crate::producer::TransactionalProducer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .transactional_id("txn-interceptor")
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .add_interceptor(std::sync::Arc::clone(&interceptor) as std::sync::Arc<_>)
        .build()
        .await
        .expect("transactional producer should connect");

    producer.init_transactions().await.expect("init");
    producer.begin_transaction().expect("begin");
    let _ = producer
        .send("orders", None, Some(b"committed"))
        .await
        .expect("send");

    // Rejected before the accumulator, inside an open transaction: the record
    // still owes an acknowledgement.
    let _ = producer
        .send("no-such-topic", None, Some(b"unrouteable"))
        .await
        .expect_err("an unrouteable record must be rejected");

    producer.commit_transaction().await.expect("commit");
    producer.close().await;

    let acks = interceptor.acks();
    assert_eq!(acks.len(), 2, "both records owe an acknowledgement");

    let committed = acks
        .iter()
        .find(|a| a.token.as_deref() == Some("orders#0"))
        .expect("the committed record must report with its own context");
    assert!(!committed.failed);
    assert_eq!(
        committed.delivery,
        crate::producer::DeliveryConfirmation::Offset
    );

    let rejected = acks
        .iter()
        .find(|a| a.token.as_deref() == Some("no-such-topic#1"))
        .expect("the rejected record must report with its own context");
    assert!(rejected.failed);
    assert_eq!(rejected.partition, crate::producer::UNKNOWN_PARTITION);
    assert_eq!(
        rejected.delivery,
        crate::producer::DeliveryConfirmation::Failed
    );
}

/// `on_acknowledgement` reports the record's **final** header set — including
/// headers written by interceptors *after* this one in the chain, which
/// `on_send` cannot show it because it runs first.
///
/// This is the one thing a `RecordContext` genuinely cannot provide, and it is
/// what the Java client's KIP-512 exists for.
#[tokio::test]
async fn on_acknowledgement_sees_headers_written_later_in_the_chain() {
    /// Runs after the recorder and adds a header the recorder never saw.
    #[derive(Debug)]
    struct LateHeaderInterceptor;

    impl crate::interceptor::ProducerInterceptor for LateHeaderInterceptor {
        fn on_send(
            &self,
            record: &mut crate::producer::ProducerRecord,
            _ctx: &mut crate::interceptor::RecordContext,
        ) -> crate::interceptor::InterceptorResult {
            record.headers.push((
                "added-last".to_string(),
                Some(bytes::Bytes::from_static(b"1")),
            ));
            Ok(())
        }
    }

    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .request_timeout(SHORT_REQUEST_TIMEOUT)
        .connect_timeout(SHORT_CONNECT_TIMEOUT)
        .add_interceptor(std::sync::Arc::clone(&interceptor) as std::sync::Arc<_>)
        .add_interceptor(std::sync::Arc::new(LateHeaderInterceptor))
        .build()
        .await
        .expect("producer should connect");

    let _ = producer
        .send_record(
            crate::producer::ProducerRecord::new("events", b"v".to_vec())
                .with_header("added-first", bytes::Bytes::from_static(b"0")),
        )
        .await
        .expect("send should be acknowledged");

    let acks = interceptor.acks();
    assert_eq!(acks.len(), 1);
    assert_eq!(
        acks[0].header_keys,
        vec!["added-first".to_string(), "added-last".to_string()],
        "the acknowledgement must carry the final header set, not the one this \
         interceptor saw in on_send"
    );
}

/// A record rejected before the accumulator reports the headers it had reached
/// by then, rather than an empty set.
#[tokio::test]
async fn a_rejected_record_still_reports_its_headers() {
    let broker = FakeBroker::start().await.unwrap();
    let interceptor = std::sync::Arc::new(RecordingInterceptor::default());
    let producer = producer_with(&broker, &interceptor).await;

    let _ = producer
        .send_record(
            crate::producer::ProducerRecord::new("no-such-topic", b"v".to_vec())
                .with_header("trace-id", bytes::Bytes::from_static(b"abc")),
        )
        .await
        .expect_err("an unrouteable record must be rejected");

    let acks = interceptor.acks();
    assert_eq!(acks.len(), 1);
    assert_eq!(
        acks[0].header_keys,
        vec!["trace-id".to_string()],
        "a pre-accumulator rejection must still report the record's headers"
    );
    assert!(acks[0].failed);
}
