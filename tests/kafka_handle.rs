//! The `Kafka` handle: one place for connection settings, shared by every
//! client built from it, and the shapes of `recv` and `close` that every
//! client shares.

#![cfg(all(feature = "test-broker", feature = "internal"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use krafka::admin::ListTopicsOptions;
use krafka::auth::AuthConfig;
use krafka::consumer::AutoOffsetReset;
use krafka::testing::{ApiKey, FakeBroker};
use krafka::{Kafka, KrafkaError, Record};

const T: Duration = Duration::from_secs(2);

fn sasl_kafka(broker: &FakeBroker, password: &str) -> krafka::KafkaBuilder {
    Kafka::builder(broker.bootstrap_servers())
        .client_id("orders")
        .security(AuthConfig::sasl_plain("alice", password))
        .request_timeout(T)
        .connect_timeout(T)
}

/// US1: a producer, a consumer, a share consumer and an admin client built
/// from one handle all authenticate with the handle's SASL settings and send
/// its client id. Every connection that carries a request authenticated
/// first, and every request carries `client_id = "orders"`.
#[tokio::test]
async fn every_client_uses_the_handles_security_and_client_id() {
    let broker = FakeBroker::start().await.unwrap();
    broker.require_sasl_plain("alice", "secret");
    broker.create_topic("orders", 1);

    let kafka = sasl_kafka(&broker, "secret").connect().await.unwrap();

    let producer = kafka.producer().build().await.unwrap();
    let _ = producer.send(Record::new("orders", "v")).await.unwrap();

    let consumer = kafka
        .consumer("g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.subscribe(["orders"]).await.unwrap();
    let record = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
        .await
        .expect("a record arrives")
        .unwrap()
        .expect("the consumer is open");
    assert_eq!(record.value_str(), Some("v"));

    let share = kafka.share_consumer("sg").build().await.unwrap();
    share.subscribe(["orders"]).await.unwrap();
    let _ = share.poll(Duration::from_millis(200)).await.unwrap();

    let admin = kafka.admin();
    assert!(
        admin
            .list_topics(ListTopicsOptions::default())
            .await
            .unwrap()
            .contains(&"orders".to_string())
    );

    let requests = broker.requests();
    let mut handshaken: BTreeSet<u64> = BTreeSet::new();
    let mut used: BTreeMap<u64, ApiKey> = BTreeMap::new();
    for request in &requests {
        assert_eq!(
            request.client_id.as_deref(),
            Some("orders"),
            "{:?} carried another client id",
            request.api_key
        );
        match request.api_key {
            ApiKey::SaslHandshake => {
                handshaken.insert(request.connection);
            }
            ApiKey::ApiVersions | ApiKey::SaslAuthenticate => {}
            other => {
                assert!(
                    handshaken.contains(&request.connection),
                    "{other:?} on connection {} before any SASL handshake",
                    request.connection
                );
                used.entry(request.connection).or_insert(other);
            }
        }
    }
    for api in [
        ApiKey::Produce,
        ApiKey::Fetch,
        ApiKey::ShareGroupHeartbeat,
        ApiKey::Metadata,
    ] {
        assert!(
            broker.request_count(api) > 0,
            "no {api:?} reached the broker"
        );
    }
    assert!(!used.is_empty());

    share.close().await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    admin.close().await.unwrap();
}

/// Negative control for the test above: the broker does enforce SASL, so a
/// client that skipped the handle's security, or sent other credentials,
/// would fail it.
#[tokio::test]
async fn the_sasl_listener_refuses_other_credentials_and_none() {
    let broker = FakeBroker::start().await.unwrap();
    broker.require_sasl_plain("alice", "secret");

    let err = sasl_kafka(&broker, "wrong").connect().await.unwrap_err();
    assert!(matches!(err, KrafkaError::Auth { .. }), "{err:?}");

    let unauthenticated = Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await;
    assert!(unauthenticated.is_err(), "a client without SASL connected");
}

/// US1: clones of a handle share one pool; a second handle is a second pool.
#[tokio::test]
async fn separate_pools_are_separate_handles() {
    let broker = FakeBroker::start().await.unwrap();
    let connect = || {
        Kafka::builder(broker.bootstrap_servers())
            .request_timeout(T)
            .connect_timeout(T)
            .connect()
    };
    let first = connect().await.unwrap();
    let second = connect().await.unwrap();
    assert!(Arc::ptr_eq(
        krafka::__private::kafka_pool(&first),
        krafka::__private::kafka_pool(&first.clone()),
    ));
    assert!(!Arc::ptr_eq(
        krafka::__private::kafka_pool(&first),
        krafka::__private::kafka_pool(&second),
    ));
}

/// US3: `close()` from another task ends a waiting `recv()` with `Ok(None)`,
/// after the records already delivered; never with an error.
#[tokio::test]
async fn closing_a_consumer_ends_recv_with_none() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    let kafka = Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap();
    let producer = kafka.producer().build().await.unwrap();
    for i in 0..10 {
        let _ = producer
            .send(Record::new("orders", format!("v{i}")))
            .await
            .unwrap();
    }
    producer.close().await.unwrap();

    let consumer = Arc::new(
        kafka
            .consumer("g")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .unwrap(),
    );
    consumer.subscribe(["orders"]).await.unwrap();

    let reader = {
        let consumer = Arc::clone(&consumer);
        tokio::spawn(async move {
            let mut received = 0;
            while let Some(_record) = consumer.recv().await? {
                received += 1;
            }
            Ok::<_, KrafkaError>(received)
        })
    };
    // Wait until everything was handed out, then close from this task while
    // the reader waits in `recv()` for an eleventh record.
    tokio::time::timeout(Duration::from_secs(10), async {
        while consumer.position("orders", 0).await != Some(10) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all ten records are delivered");
    consumer.close().await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("recv returns once the consumer is closed")
        .unwrap()
        .expect("a clean close is Ok(None), not an error");
    assert_eq!(received, 10);
    assert!(consumer.recv().await.unwrap().is_none());
}

/// US4: the share consumer ends `recv()` the same way.
#[tokio::test]
async fn closing_a_share_consumer_ends_recv_with_none() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("jobs", 1);
    let kafka = Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap();
    let share = kafka.share_consumer("workers").build().await.unwrap();
    share.subscribe(["jobs"]).await.unwrap();

    let reader = {
        let share = share.clone();
        tokio::spawn(async move { share.recv().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    share.close().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("recv returns once the share consumer is closed")
        .unwrap();
    assert!(matches!(result, Ok(None)), "{result:?}");
}

/// FR-007: a produced record's key, value, headers, partition and timestamp
/// come back unchanged, with a public `TimestampType`.
#[tokio::test]
async fn a_record_round_trips_field_by_field() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 2);
    let kafka = Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap();
    let producer = kafka.producer().build().await.unwrap();
    let _ = producer
        .send(
            Record::new("orders", "value")
                .key("key")
                .header("trace", "abc")
                .null_header("flag")
                .partition(1)
                .timestamp(1_700_000_000_000),
        )
        .await
        .unwrap();
    producer.close().await.unwrap();

    let consumer = kafka.consumer_without_group().build().await.unwrap();
    consumer.assign("orders", vec![1]).await.unwrap();
    consumer.seek("orders", 1, 0).await.unwrap();
    let record = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&*record.topic, "orders");
    assert_eq!(record.partition, 1);
    assert_eq!(record.key_str(), Some("key"));
    assert_eq!(record.value_str(), Some("value"));
    assert_eq!(record.timestamp, 1_700_000_000_000);
    assert_eq!(record.timestamp_type, krafka::TimestampType::CreateTime);
    let expected: krafka::Headers = vec![
        ("trace".to_string(), Some(bytes::Bytes::from_static(b"abc"))),
        ("flag".to_string(), None),
    ];
    assert_eq!(record.headers, expected);
    consumer.close().await.unwrap();
}
