//! End to end: what a client did reaches its `metrics()`, the `Kafka`
//! sum and the Prometheus text.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use crate::Kafka;
use crate::consumer::{AutoOffsetReset, Consumer};
use crate::testing::FakeBroker;

async fn connect(broker: &FakeBroker, client_id: &str) -> Kafka {
    Kafka::builder(broker.bootstrap_servers())
        .client_id(client_id)
        .request_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
}

async fn poll_n(consumer: &Consumer, n: usize) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut got = 0;
    while got < n && tokio::time::Instant::now() < deadline {
        got += consumer
            .poll(Duration::from_millis(200))
            .await
            .unwrap()
            .len();
    }
    got
}

/// The value of a series in Prometheus text, matched by name and labels.
fn sample(text: &str, series: &str) -> Option<u64> {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

/// Reverted-line controls: with the producer's records-sent write removed
/// the first assertion reads 0; with the pool's connection counters
/// added per client in `Kafka::metrics` the handle reports twice the
/// pool's connections.
#[tokio::test]
async fn what_the_clients_did_reaches_their_metrics_and_the_handle_sum() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    let kafka = connect(&broker, "orders-service").await;

    let producer = kafka.producer().build().await.unwrap();
    for i in 0..100u32 {
        let _ = producer
            .send(crate::Record::new("orders", i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }
    let metrics = producer.metrics();
    assert_eq!(metrics.producer.records_sent, 100);
    assert!(metrics.connections.connections_created >= 1);
    assert_eq!(metrics.client_id.as_deref(), Some("orders-service"));
    let text = metrics.prometheus_text();
    assert_eq!(
        sample(
            &text,
            "krafka_producer_records_sent_total{client_id=\"orders-service\"}"
        ),
        Some(100)
    );
    assert_eq!(
        sample(
            &text,
            "krafka_connections_created_total{client_id=\"orders-service\"}"
        ),
        Some(metrics.connections.connections_created)
    );

    // An owned snapshot: later sends do not change it.
    for i in 0..100u32 {
        let _ = producer
            .send(crate::Record::new("orders", i.to_be_bytes().to_vec()))
            .await
            .unwrap();
    }
    assert_eq!(metrics.producer.records_sent, 100);
    assert_eq!(producer.metrics().producer.records_sent, 200);

    let consumer = kafka
        .consumer("g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.subscribe(["orders"]).await.unwrap();
    assert_eq!(poll_n(&consumer, 200).await, 200);
    assert_eq!(consumer.metrics().consumer.records_received, 200);

    let total = kafka.metrics();
    assert_eq!(total.client_id, None);
    assert_eq!(total.producer.records_sent, 200);
    assert_eq!(total.consumer.records_received, 200);
    // One pool, counted once.
    assert_eq!(total.connections, kafka.admin().metrics().connections);
    assert_eq!(
        total.connections.connections_created,
        consumer.metrics().connections.connections_created
    );
    let text = total.prometheus_text();
    assert_eq!(
        sample(&text, "krafka_producer_records_sent_total"),
        Some(200)
    );
    assert!(!text.contains("client_id"));

    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

/// Every client returns its own snapshot synchronously; a
/// dropped client leaves the handle's sum.
#[tokio::test]
async fn every_client_reports_its_own_activity() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    let kafka = connect(&broker, "svc").await;

    let txn = kafka.producer().build_transactional("t").await.unwrap();
    txn.begin().unwrap();
    let _ = txn.send(crate::Record::new("orders", "v")).await.unwrap();
    txn.commit().await.unwrap();
    assert_eq!(txn.metrics().producer.records_sent, 1);

    let share = kafka.share_consumer("s").build().await.unwrap();
    let _ = share.poll(Duration::from_millis(20)).await;
    assert_eq!(share.metrics().consumer.polls, 1);
    assert_eq!(share.metrics().producer.records_sent, 0);

    let admin = kafka.admin();
    assert!(admin.metrics().connections.connections_created >= 1);
    assert_eq!(admin.metrics().producer, Default::default());

    assert_eq!(kafka.metrics().producer.records_sent, 1);
    txn.close().await.unwrap();
    drop(txn);
    assert_eq!(kafka.metrics().producer.records_sent, 0);
    share.close().await.unwrap();
}
