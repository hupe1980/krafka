//! A custom partitioner's answer is range-checked before the record is
//! accepted.
//!
//! Without the check, a partition the topic does not have is accepted,
//! batched, and only fails later as an unroutable batch.

#![cfg(feature = "test-broker")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use krafka::PartitionId;
use krafka::producer::Partitioner;
use krafka::testing::FakeBroker;

/// Always answers the same partition, whatever the topic has.
struct Fixed(PartitionId);

impl Partitioner for Fixed {
    fn partition(&self, _topic: &str, _key: Option<&[u8]>, _count: usize) -> PartitionId {
        self.0
    }
}

#[tokio::test]
async fn a_partition_outside_the_topic_is_rejected_at_send() {
    let broker = FakeBroker::start().await.expect("fake broker starts");
    broker.create_topic("events", 2);

    for bad in [2, -1] {
        let producer = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .expect("producer connects")
            .producer()
            .max_block(Duration::from_secs(5))
            .partitioner(Fixed(bad))
            .build()
            .await
            .expect("producer connects");

        let error = producer
            .send(krafka::Record::new("events", "v").key("k"))
            .await
            .expect_err("the partitioner's answer is outside [0, 2)");
        assert!(
            matches!(error, krafka::KrafkaError::Config { .. }),
            "expected a configuration error, got: {error}"
        );
        assert!(
            error.to_string().contains("not in the range [0, 2)"),
            "the error must name the valid range, got: {error}"
        );
        producer.close().await.unwrap();
    }
}

/// The transactional producer shares the resolution path; a wrong answer
/// must not reach a batch there either.
#[tokio::test]
async fn the_transactional_producer_checks_the_partitioner_too() {
    let broker = FakeBroker::start().await.expect("fake broker starts");
    broker.create_topic("events", 2);

    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .expect("producer connects")
        .producer()
        .max_block(Duration::from_secs(5))
        .partitioner(Fixed(5))
        .build_transactional("txn-range")
        .await
        .expect("producer connects");
    producer.begin().expect("begin");

    let error = producer
        .send(krafka::Record::new("events", "v").key("k"))
        .await
        .expect_err("partition 5 does not exist on a 2-partition topic");
    assert!(
        error.to_string().contains("not in the range [0, 2)"),
        "got: {error}"
    );
}

/// A partitioner that stays in range is unaffected.
#[tokio::test]
async fn an_in_range_answer_is_delivered_there() {
    let broker = FakeBroker::start().await.expect("fake broker starts");
    broker.create_topic("events", 2);

    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .expect("producer connects")
        .producer()
        .partitioner(Fixed(1))
        .build()
        .await
        .expect("producer connects");
    let metadata = producer
        .send(krafka::Record::new("events", "v").key("k"))
        .await
        .expect("partition 1 exists");
    assert_eq!(metadata.partition, 1);
    producer.close().await.unwrap();
}
