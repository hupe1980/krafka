//! Metadata and admin routing against the fake broker: a single metadata
//! writer, topic-ID-scoped leader epochs, retriable leader elections,
//! rebootstrap, and an admin driver that routes each request to the node that
//! can answer it within a per-call deadline.
#![cfg(all(feature = "test-broker", feature = "internal"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use krafka::__private::metadata::ClusterMetadata;
use krafka::__private::network::{ConnectionConfig, ConnectionPool};
use krafka::Kafka;
use krafka::admin::{
    AdminClient, ConfigResource, CreateTopicsOptions, DescribeConfigsOptions,
    DescribeConsumerGroupsOptions, ListConsumerGroupOffsetsOptions, ListOffsetsOptions, NewTopic,
    OffsetSpec, TopicPartition,
};
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::error::{ErrorCode, KrafkaError};
use krafka::testing::ApiKey;
use krafka::testing::{Control, FakeBroker};

async fn shared(broker: &FakeBroker) -> (Kafka, AdminClient) {
    let client = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap();
    let admin = client.admin();
    (client, admin)
}

fn is_code<T: std::fmt::Debug>(result: &Result<T, KrafkaError>, expected: ErrorCode) -> bool {
    matches!(result, Err(KrafkaError::Broker { code, .. }) if *code == expected)
}

// ── Metadata ─────────────────────────────────────────────────────────────

/// A deleted and re-created topic restarts its leader epochs at 0; the client
/// must route it to the new leader, not keep the dead topic's partitions.
#[tokio::test]
async fn m1_recreated_topic_is_not_fenced_by_the_old_topics_epoch() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("t", 1);
    for _ in 0..5 {
        broker.bump_leader_epoch("t", 0);
    }
    let client = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap();
    let meta = krafka::__private::kafka_metadata(&client);
    assert_eq!(meta.leader("t", 0), Some(0));
    assert_eq!(meta.leader_epoch("t", 0), Some(5));
    let old_id = meta.topic_id_for_name("t");

    broker.with_state(|s| {
        s.topics.remove("t");
        s.create_topic("t", 1);
    });
    broker.set_leader("t", 0, 1);

    meta.refresh().await.unwrap();
    assert_ne!(meta.topic_id_for_name("t"), old_id, "topic id did update");
    assert_eq!(
        meta.leader("t", 0),
        Some(1),
        "client still routes the re-created topic to the deleted topic's leader \
         (cached epoch {:?})",
        meta.leader_epoch("t", 0)
    );
}

/// Concurrent refreshes for different topics on a multi-threaded runtime:
/// every topic a successful refresh returned must be in the cache.
async fn concurrent_partial_refreshes_keep_every_topic(bulk_topics: usize) {
    let broker = FakeBroker::start().await.unwrap();
    for i in 0..bulk_topics {
        broker.create_topic(&format!("bulk-{i}"), 1);
    }
    let client = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap();
    let meta = Arc::new(
        ClusterMetadata::new(
            vec![broker.bootstrap_servers()],
            krafka::__private::kafka_pool(&client).clone(),
            Duration::from_secs(300),
        )
        .with_retry_backoff(None::<Duration>),
    );
    meta.refresh().await.unwrap();

    let mut lost = Vec::new();
    for round in 0..20 {
        let names: Vec<String> = (0..32).map(|i| format!("new-{round}-{i}")).collect();
        for n in &names {
            broker.create_topic(n, 1);
        }
        let mut tasks = Vec::new();
        for n in names.clone() {
            let meta = Arc::clone(&meta);
            tasks.push(tokio::spawn(async move {
                meta.refresh_for_topics(Some(&[n.as_str()])).await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        lost.extend(names.into_iter().filter(|n| meta.topic(n).is_none()));
    }
    assert!(
        lost.is_empty(),
        "{} topic(s) whose refresh returned Ok are missing from the cache, e.g. {:?}",
        lost.len(),
        lost.iter().take(3).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn m2_concurrent_partial_refreshes_do_not_lose_topics() {
    concurrent_partial_refreshes_keep_every_topic(3000).await;
}

/// The same scenario on one thread, where no two updates can overlap.
#[tokio::test(flavor = "current_thread")]
async fn m2b_single_thread_control() {
    concurrent_partial_refreshes_keep_every_topic(3000).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn m2c_small_cache_also_keeps_every_topic() {
    concurrent_partial_refreshes_keep_every_topic(50).await;
}

/// A partition in a leader election (leader -1 for 500 ms) delays a send; on
/// default settings it does not fail it.
#[tokio::test]
async fn m3_send_survives_a_leader_election() {
    let broker = Arc::new(FakeBroker::start_cluster(2).await.unwrap());
    broker.create_topic("t", 1);
    broker.with_state(|s| s.topics.get_mut("t").unwrap().partitions[0].leader = -1);

    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    let elect = Arc::clone(&broker);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        elect.set_leader("t", 0, 0);
    });

    let result = producer.send(krafka::Record::new("t", "x")).await;
    assert!(
        result.is_ok(),
        "a 500 ms leader election failed the send: {:?}",
        result.err()
    );
}

/// A TCP reset during the TLS handshake is a retriable network error.
#[tokio::test]
async fn e1_tcp_reset_during_tls_handshake_is_retriable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let accepts = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepts);
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
            drop(sock);
        }
    });

    let mut config = ConnectionConfig::builder()
        .auth(AuthConfig::ssl(TlsConfig::insecure()))
        .build()
        .unwrap();
    config.init_tls().await.unwrap();
    let pool = ConnectionPool::new(config);
    let Err(err) = pool.get_connection(&addr.to_string()).await else {
        panic!("must fail");
    };

    assert!(
        err.is_retriable() && !matches!(err, KrafkaError::Auth { .. }),
        "connection reset classified as {err:?} (retriable={}), after {} attempt(s)",
        err.is_retriable(),
        accepts.load(Ordering::SeqCst)
    );
}

/// A bootstrap failure names the address it tried and keeps the cause.
#[tokio::test]
async fn e2_bootstrap_failure_keeps_the_underlying_cause() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            drop(sock);
        }
    });
    let err = krafka::Kafka::builder(addr.to_string())
        .security(AuthConfig::ssl(TlsConfig::insecure()))
        .connect()
        .await
        .expect_err("must fail");
    let text = err.to_string();
    assert!(
        text.contains(&addr.to_string()) || text.contains("TLS"),
        "bootstrap error has neither the address nor the cause: {err:?}"
    );
    assert!(
        std::error::Error::source(&err).is_some(),
        "the cause must stay in the error chain: {err:?}"
    );
}

// ── Admin routing ────────────────────────────────────────────────────────

/// Any-broker requests fail over: one broker dropping FindCoordinator must
/// not fail the call.
#[tokio::test]
async fn a1_any_broker_routing_fails_over_to_another_broker() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    let (client, admin) = shared(&broker).await;
    let victim = krafka::__private::kafka_metadata(&client).brokers()[0].id();
    let coordinator = (victim + 1) % 3;
    broker.set_group_coordinator("g", coordinator);
    broker.on(ApiKey::FindCoordinator, move |info| {
        if info.node_id == victim {
            Control::Disconnect
        } else {
            Control::Pass
        }
    });

    let result = admin
        .describe_consumer_groups(["g"], DescribeConsumerGroupsOptions::default())
        .await;
    assert!(
        result.is_ok(),
        "one unreachable broker (node {victim}) failed the call: {:?}",
        result.err()
    );
    let group = &result.unwrap()["g"];
    assert!(group.is_ok(), "the group failed: {group:?}");
}

/// One group's failed coordinator lookup fails that group only.
#[tokio::test]
async fn a2_one_bad_group_does_not_fail_the_whole_describe_batch() {
    let broker = FakeBroker::start().await.unwrap();
    let (_client, admin) = shared(&broker).await;
    broker.on(ApiKey::FindCoordinator, |info| {
        if info.api_call_index == 1 {
            Control::Error(ErrorCode::GroupAuthorizationFailed)
        } else {
            Control::Pass
        }
    });
    let result = admin
        .describe_consumer_groups(
            ["ok", "forbidden"],
            DescribeConsumerGroupsOptions::default(),
        )
        .await;
    assert!(
        result.is_ok(),
        "a per-group failure became a whole-batch failure: {:?}",
        result.err()
    );
    let results = result.unwrap();
    let failed: Vec<_> = results
        .values()
        .filter(|r| is_code(r, ErrorCode::GroupAuthorizationFailed))
        .collect();
    assert_eq!(
        failed.len(),
        1,
        "exactly one group was refused: {results:?}"
    );
    assert_eq!(results.values().filter(|r| r.is_ok()).count(), 1);
}

/// DeleteGroups goes to the group's coordinator.
#[tokio::test]
async fn a3_delete_consumer_groups_goes_to_the_group_coordinator() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    // Advertised before any connection exists, so every node's ApiVersions
    // answer includes it.
    broker.set_api_versions(ApiKey::DeleteGroups, 2, 2);
    // Advertised before any connection exists, so every node's ApiVersions
    // answer includes it.
    let (client, admin) = shared(&broker).await;
    let first = krafka::__private::kafka_metadata(&client).brokers()[0].id();
    let coordinator = (first + 1) % 3;
    broker.set_group_coordinator("g", coordinator);

    let _ = admin
        .delete_consumer_groups(
            ["g"],
            krafka::admin::DeleteConsumerGroupsOptions::default().timeout(Duration::from_secs(2)),
        )
        .await;
    assert_eq!(
        broker.request_nodes(ApiKey::DeleteGroups),
        vec![coordinator],
        "DeleteGroups was not routed to the coordinator"
    );
}

/// A broker-scoped DescribeConfigs goes to that broker.
#[tokio::test]
async fn a4_broker_config_is_described_by_that_broker() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    broker.set_api_versions(ApiKey::DescribeConfigs, 4, 4);
    let (client, admin) = shared(&broker).await;
    let first = krafka::__private::kafka_metadata(&client).brokers()[0].id();
    let target = (first + 1) % 3;

    let _ = admin
        .describe_configs(
            [ConfigResource::broker(target)],
            DescribeConfigsOptions::default().timeout(Duration::from_secs(1)),
        )
        .await;
    let mut nodes = broker.request_nodes(ApiKey::DescribeConfigs);
    nodes.dedup();
    assert_eq!(
        nodes,
        vec![target],
        "a broker-scoped DescribeConfigs went to the wrong broker"
    );
}

/// A coordinator still loading is retried, and a group-level error keeps its
/// code.
#[tokio::test]
async fn a5_offset_fetch_broker_error_keeps_its_code_and_retries() {
    let broker = FakeBroker::start().await.unwrap();
    let (_client, admin) = shared(&broker).await;
    broker.on_once(ApiKey::OffsetFetch, |_| {
        Control::Error(ErrorCode::CoordinatorLoadInProgress)
    });
    let result = admin
        .list_consumer_group_offsets("g", ListConsumerGroupOffsetsOptions::default())
        .await;
    match result {
        Ok(_) => {}
        Err(KrafkaError::Broker {
            code: ErrorCode::CoordinatorLoadInProgress,
            ..
        }) => {}
        Err(other) => panic!(
            "retriable coordinator error surfaced as {other:?} (retriable={})",
            other.is_retriable()
        ),
    }
    assert_eq!(
        broker.request_count(ApiKey::OffsetFetch),
        2,
        "COORDINATOR_LOAD_IN_PROGRESS must be retried once"
    );
}

/// A load that outlasts the deadline is reported with its code, inside the
/// deadline.
#[tokio::test]
async fn a_persistent_coordinator_load_reports_its_code_at_the_deadline() {
    let broker = FakeBroker::start().await.unwrap();
    let (_client, admin) = shared(&broker).await;
    broker.on(ApiKey::OffsetFetch, |_| {
        Control::Error(ErrorCode::CoordinatorLoadInProgress)
    });
    let started = Instant::now();
    let result = admin
        .list_consumer_group_offsets(
            "g",
            ListConsumerGroupOffsetsOptions::default().timeout(Duration::from_secs(1)),
        )
        .await;
    assert!(
        is_code(&result, ErrorCode::CoordinatorLoadInProgress),
        "got {result:?}"
    );
    assert!(result.unwrap_err().is_retriable());
    assert!(started.elapsed() < Duration::from_millis(1600));
}

/// Every broker unreachable: the call ends with `Timeout` at its deadline.
#[tokio::test]
async fn an_unreachable_cluster_times_out_at_the_deadline() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    let (_client, admin) = shared(&broker).await;
    broker.on(ApiKey::FindCoordinator, |_| Control::Disconnect);

    let started = Instant::now();
    let result = admin
        .describe_consumer_groups(
            ["g"],
            DescribeConsumerGroupsOptions::default().timeout(Duration::from_secs(2)),
        )
        .await
        .expect("only the group fails");
    let elapsed = started.elapsed();
    assert!(
        matches!(result["g"], Err(KrafkaError::Timeout { .. })),
        "got {:?}",
        result["g"]
    );
    assert!(
        elapsed < Duration::from_millis(2600),
        "the call outlived its deadline: {elapsed:?}"
    );
}

/// A write sent and never answered is not re-sent: it may have been applied.
#[tokio::test]
async fn an_unanswered_write_is_not_retried() {
    let broker = FakeBroker::start().await.unwrap();
    let (_client, admin) = shared(&broker).await;
    broker.on(ApiKey::CreateTopics, |_| Control::Disconnect);

    let results = admin
        .create_topics(
            [NewTopic::new("orders", 1, 1).unwrap()],
            CreateTopicsOptions::default().timeout(Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert!(
        matches!(results["orders"], Err(KrafkaError::Network(_))),
        "got {:?}",
        results["orders"]
    );
    assert_eq!(broker.request_count(ApiKey::CreateTopics), 1);
}

/// Creating an existing topic reports `TOPIC_ALREADY_EXISTS` for that topic
/// only.
#[tokio::test]
async fn an_existing_topic_is_a_typed_per_item_error() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("existing", 1);
    let (_client, admin) = shared(&broker).await;

    let results = admin
        .create_topics(
            [
                NewTopic::new("existing", 1, 1).unwrap(),
                NewTopic::new("fresh", 1, 1).unwrap(),
            ],
            CreateTopicsOptions::default(),
        )
        .await
        .unwrap();
    assert!(
        is_code(&results["existing"], ErrorCode::TopicAlreadyExists),
        "got {:?}",
        results["existing"]
    );
    assert!(results["fresh"].is_ok());
}

/// One partition without a leader fails alone in `list_offsets`.
#[tokio::test]
async fn one_unroutable_partition_does_not_fail_the_others() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("t", 3);
    broker.with_state(|s| s.topics.get_mut("t").unwrap().partitions[1].leader = -1);
    let (_client, admin) = shared(&broker).await;

    let results = admin
        .list_offsets(
            (0..3).map(|p| (TopicPartition::new("t", p), OffsetSpec::Latest)),
            ListOffsetsOptions::default().timeout(Duration::from_secs(1)),
        )
        .await
        .unwrap();
    assert!(results[&TopicPartition::new("t", 0)].is_ok());
    assert!(results[&TopicPartition::new("t", 2)].is_ok());
    assert!(
        is_code(
            &results[&TopicPartition::new("t", 1)],
            ErrorCode::LeaderNotAvailable
        ),
        "got {:?}",
        results[&TopicPartition::new("t", 1)]
    );
}

/// A broker that rejects every SASL handshake with
/// `UNSUPPORTED_SASL_MECHANISM`. Returns its address.
async fn sasl_rejecting_broker() -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut len = [0u8; 4];
                if stream.read_exact(&mut len).await.is_err() {
                    return;
                }
                let mut request = vec![0u8; u32::from_be_bytes(len) as usize];
                if stream.read_exact(&mut request).await.is_err() {
                    return;
                }
                // Response header (correlation ID), error code 33, no mechanisms.
                let mut response = Vec::new();
                response.extend_from_slice(&request[4..8]);
                response.extend_from_slice(&33i16.to_be_bytes());
                response.extend_from_slice(&0i32.to_be_bytes());
                let mut frame = (response.len() as u32).to_be_bytes().to_vec();
                frame.extend_from_slice(&response);
                let _ = stream.write_all(&frame).await;
                let _ = stream.flush().await;
            });
        }
    });
    addr
}

/// A SASL rejection at build time is an `Auth` error, not a network error —
/// even when other seed addresses fail with network errors.
#[tokio::test]
async fn a_sasl_rejection_at_build_is_an_auth_error() {
    let rejecting = sasl_rejecting_broker().await;
    // A port nothing listens on: connection refused.
    let refused = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    for bootstrap in [
        rejecting.to_string(),
        format!("{refused},{rejecting},{refused}"),
        format!("{rejecting},{refused}"),
    ] {
        let err = Kafka::builder(bootstrap.clone())
            .security(AuthConfig::sasl_scram_sha256("user", "wrong"))
            .connect()
            .await
            .expect_err("the broker rejects every handshake");
        assert!(
            matches!(err, KrafkaError::Auth { .. }),
            "bootstrap {bootstrap}: a SASL rejection surfaced as {err:?}"
        );
        assert!(!err.is_retriable());
    }
}

/// A bootstrap host that does not resolve is named in the error.
#[tokio::test]
async fn an_unresolvable_bootstrap_host_is_named() {
    let err = krafka::Kafka::builder("no-such-broker.invalid:9092")
        .connect()
        .await
        .expect_err("the host does not resolve");
    assert!(
        err.to_string().contains("no-such-broker.invalid"),
        "the error does not name the host: {err}"
    );
}
