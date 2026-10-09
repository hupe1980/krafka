//! A cluster that lacks a feature fails with an error naming the feature and
//! the setting that avoids it, keeping the error's kind and broker code.
//!
//! An API is withdrawn with `FakeBroker::set_api_versions` at a version range
//! no client speaks, so version negotiation finds nothing in common.
//!
//! Run: `cargo test --features test-broker --test missing_features`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use krafka::Compression;
use krafka::Kafka;
use krafka::consumer::GroupProtocol;
use krafka::error::{ErrorCode, KrafkaError, ProtocolErrorKind};
use krafka::producer::Record;
use krafka::testing::{ApiKey, Control, FakeBroker};

const T: Duration = Duration::from_secs(2);

/// Advertise `api` only at a version no client speaks.
fn withdraw(broker: &FakeBroker, api: ApiKey) {
    broker.set_api_versions(api, i16::MAX, i16::MAX);
}

async fn kafka(broker: &FakeBroker) -> Kafka {
    Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
}

fn assert_unknown_api_version(error: &KrafkaError) {
    assert_eq!(
        error.protocol_error_kind(),
        Some(ProtocolErrorKind::UnknownApiVersion),
        "{error}"
    );
}

fn assert_mentions(error: &KrafkaError, needles: &[&str]) {
    let text = error.to_string();
    for needle in needles {
        assert!(text.contains(needle), "{needle:?} missing from: {text}");
    }
}

fn broker_code(error: &KrafkaError) -> ErrorCode {
    match error {
        KrafkaError::Broker { code, .. } => *code,
        other => panic!("expected a broker error, got {other}"),
    }
}

/// Poll until an error comes back.
async fn first_poll_error<F, Fut, T>(mut poll: F) -> KrafkaError
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, KrafkaError>>,
{
    for _ in 0..50 {
        if let Err(error) = poll().await {
            return error;
        }
    }
    panic!("no error within 50 polls");
}

#[tokio::test]
async fn kip848_without_consumer_group_heartbeat_names_the_classic_protocol() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    withdraw(&broker, ApiKey::ConsumerGroupHeartbeat);
    let consumer = kafka(&broker)
        .await
        .consumer("g")
        .group_protocol(GroupProtocol::Consumer)
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let error = first_poll_error(|| consumer.poll(Duration::from_millis(200))).await;
    assert_unknown_api_version(&error);
    assert_mentions(
        &error,
        &[
            "does not provide KIP-848",
            "ConsumerGroupHeartbeat",
            "GroupProtocol::Classic",
        ],
    );
    let _ = consumer.close().await;
}

#[tokio::test]
async fn a_share_consumer_without_share_groups_says_the_cluster_lacks_them() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    withdraw(&broker, ApiKey::ShareGroupHeartbeat);
    let consumer = kafka(&broker)
        .await
        .share_consumer("g")
        .build()
        .await
        .unwrap();

    // `subscribe` joins the group, so it is the first call to fail.
    let error = consumer.subscribe(&["events"]).await.unwrap_err();
    assert_unknown_api_version(&error);
    assert_mentions(
        &error,
        &[
            "does not provide share groups (KIP-932)",
            "ShareGroupHeartbeat",
            "Kafka 4.2",
        ],
    );
    let _ = consumer.close().await;
}

#[tokio::test]
async fn a_share_consumer_without_share_fetch_says_the_cluster_lacks_share_groups() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    withdraw(&broker, ApiKey::ShareFetch);
    let consumer = kafka(&broker)
        .await
        .share_consumer("g")
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let error = first_poll_error(|| consumer.poll(Duration::from_millis(200))).await;
    assert_unknown_api_version(&error);
    assert_mentions(
        &error,
        &["does not provide share groups (KIP-932)", "ShareFetch"],
    );
    let _ = consumer.close().await;
}

#[tokio::test]
async fn an_idempotent_producer_without_init_producer_id_names_idempotent_false() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    withdraw(&broker, ApiKey::InitProducerId);

    // `build` fetches the producer id.
    let error = kafka(&broker).await.producer().build().await.unwrap_err();
    assert_unknown_api_version(&error);
    assert_mentions(&error, &["InitProducerId", ".idempotent(false)"]);
}

#[tokio::test]
async fn an_idempotent_producer_refused_a_producer_id_names_idempotent_false() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    broker.on(ApiKey::InitProducerId, |_| {
        Control::Error(ErrorCode::ClusterAuthorizationFailed)
    });

    let error = kafka(&broker).await.producer().build().await.unwrap_err();
    assert_eq!(broker_code(&error), ErrorCode::ClusterAuthorizationFailed);
    assert_mentions(&error, &["idempotent", ".idempotent(false)"]);
}

#[tokio::test]
async fn a_rejected_codec_names_the_codec_and_the_compression_setting() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    broker.on(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::UnsupportedCompressionType)
    });
    let producer = kafka(&broker)
        .await
        .producer()
        .compression(Compression::Lz4)
        .build()
        .await
        .unwrap();

    let error = producer
        .send(Record::new("events", b"v".to_vec()))
        .await
        .unwrap_err();
    assert_eq!(broker_code(&error), ErrorCode::UnsupportedCompressionType);
    assert_mentions(&error, &["Lz4", ".compression(", "events-0"]);
    let _ = producer.close().await;
}

#[tokio::test]
async fn a_transactional_producer_without_end_txn_says_the_cluster_lacks_transactions() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    withdraw(&broker, ApiKey::EndTxn);
    let error = kafka(&broker)
        .await
        .producer()
        .max_block(T)
        .build_transactional("tx")
        .await
        .unwrap_err();
    assert_unknown_api_version(&error);
    assert_mentions(&error, &["does not provide transactions", "EndTxn"]);
}

#[tokio::test]
async fn a_transactional_send_without_add_partitions_says_the_cluster_lacks_transactions() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    withdraw(&broker, ApiKey::AddPartitionsToTxn);
    let producer = kafka(&broker)
        .await
        .producer()
        .max_block(T)
        .build_transactional("tx")
        .await
        .unwrap();
    producer.begin().unwrap();

    let send = producer
        .send(Record::new("events", b"v".to_vec()).partition(0))
        .await;
    let error = match send {
        Err(error) => error,
        Ok(_) => producer.commit().await.unwrap_err(),
    };
    assert_mentions(
        &error,
        &["does not provide transactions", "AddPartitionsToTxn"],
    );
}
