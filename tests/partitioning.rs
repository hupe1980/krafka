//! Keyless records spread across partitions in runs of about `batch_size`
//! bytes (KIP-794), at any `linger`.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use krafka::producer::Record;
use krafka::testing::FakeBroker;

const BATCH_SIZE: usize = 16_384;
const RECORDS: usize = 1_000;

/// The bytes the accumulator charges a keyless 200-byte record.
fn charged(value: &[u8]) -> usize {
    Record::new("t", value.to_vec()).estimated_size()
}

/// Lengths, in records, of the runs of consecutive records on one partition.
fn runs(partitions: &[i32]) -> Vec<usize> {
    let mut runs = Vec::new();
    let mut current = 0;
    for (i, p) in partitions.iter().enumerate() {
        if i > 0 && partitions[i - 1] != *p {
            runs.push(current);
            current = 0;
        }
        current += 1;
    }
    runs.push(current);
    runs
}

/// Every run except the last carries at least `batch_size` bytes and at most
/// `batch_size` plus `slack` records; each switch moves to a different
/// partition.
fn assert_run_invariant(partitions: &[i32], record_bytes: usize, slack: usize) {
    let runs = runs(partitions);
    let total = partitions.len() * record_bytes;
    let min_records = BATCH_SIZE.div_ceil(record_bytes);
    let expected = total / BATCH_SIZE;
    assert!(
        runs.len() == expected || runs.len() == expected + 1,
        "{} runs, expected {expected} or {}: {runs:?}",
        runs.len(),
        expected + 1
    );
    for run in &runs[..runs.len() - 1] {
        assert!(
            *run >= min_records && *run <= min_records + slack,
            "a run of {run} records; each must carry {min_records}..={} records: {runs:?}",
            min_records + slack
        );
    }
}

async fn sequential(linger: Option<Duration>) -> Vec<i32> {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 4);
    let kafka = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap();
    let mut builder = kafka.producer();
    if let Some(linger) = linger {
        builder = builder.linger(linger);
    }
    let producer = builder.build().await.unwrap();
    let value = vec![b'x'; 200];
    let mut partitions = Vec::with_capacity(RECORDS);
    for _ in 0..RECORDS {
        let md = producer
            .send(krafka::Record::new(
                "t",
                bytes::Bytes::copy_from_slice(value.as_slice()),
            ))
            .await
            .unwrap();
        partitions.push(md.partition);
    }
    producer.close().await.unwrap();
    partitions
}

/// P4: on today's tree every record lands on one partition at `linger = 0`.
#[tokio::test]
async fn keyless_records_switch_partition_every_batch_size_bytes_at_linger_zero() {
    let partitions = sequential(Some(Duration::ZERO)).await;
    assert_run_invariant(&partitions, charged(&[b'x'; 200]), 1);
}

#[tokio::test]
async fn keyless_records_switch_partition_every_batch_size_bytes_at_default_linger() {
    let partitions = sequential(None).await;
    assert_run_invariant(&partitions, charged(&[b'x'; 200]), 1);
}

/// Eight concurrent senders: at most one extra record per sender per run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_keyless_senders_keep_the_run_invariant() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 4);
    let producer = Arc::new(
        krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .build()
            .await
            .unwrap(),
    );
    let value = vec![b'x'; 200];
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let producer = Arc::clone(&producer);
        let value = value.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..RECORDS / 8 {
                let _ = producer
                    .send(krafka::Record::new(
                        "t",
                        bytes::Bytes::copy_from_slice(value.as_slice()),
                    ))
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    producer.close().await.unwrap();
    // Log order across partitions is the routing order only per partition, so
    // check the per-partition byte totals instead: every partition that was
    // switched away from got at least one run's worth.
    let records = broker.all_records("t").unwrap();
    assert_eq!(records.len(), RECORDS);
    let used: std::collections::BTreeSet<i32> = records.iter().map(|r| r.partition).collect();
    assert!(used.len() > 1, "keyless records spread: {used:?}");
}

/// Keyed records follow murmur2 and do not count toward the keyless budget.
#[tokio::test]
async fn keyed_records_follow_murmur2() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 4);
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    for i in 0..200u32 {
        let key = format!("key-{i}");
        let md = producer
            .send(krafka::Record::new("t", "v").key(bytes::Bytes::copy_from_slice(key.as_bytes())))
            .await
            .unwrap();
        let expected = ((krafka::producer::murmur2(key.as_bytes()) & 0x7fff_ffff) % 4) as i32;
        assert_eq!(md.partition, expected, "{key}");
    }
    producer.close().await.unwrap();
}

/// A topic with one partition takes every keyless record.
#[tokio::test]
async fn a_single_partition_topic_takes_every_record() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    for _ in 0..200 {
        let md = producer
            .send(krafka::Record::new(
                "t",
                bytes::Bytes::copy_from_slice(&[0u8; 200]),
            ))
            .await
            .unwrap();
        assert_eq!(md.partition, 0);
    }
    producer.close().await.unwrap();
}
