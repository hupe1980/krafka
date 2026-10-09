//! End-to-end consume-path benchmarks against the in-process fake broker.
//!
//! Run with: `cargo bench --bench consume_path --features test-broker`
//!
//! # What these measure
//!
//! A **regression gate**, not a comparison, on the same terms as
//! `send_path`: krafka against krafka across two runs, where the fake
//! broker's constant overhead cancels. No case ranks two settings or two APIs
//! against each other, and no figure belongs in the documentation.
//!
//! Every iteration does identical work. Each case reads a fixed log produced
//! once before measurement, through a manually assigned consumer (no group
//! membership traffic), rewinds to offset 0, and stops as soon as the fixed
//! record count is delivered — the fake broker answers an empty fetch at
//! once, so polling past the end would measure a spin, not a fetch.

#![allow(missing_docs, clippy::panic, clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use krafka::consumer::Consumer;
use krafka::testing::FakeBroker;

const PAYLOAD: &[u8] = &[b'x'; 100];
const RECORD_COUNTS: [usize; 2] = [1_000, 10_000];

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime")
}

fn topic(records: usize) -> String {
    format!("consume-{records}")
}

/// A fake broker holding one single-partition topic per record count, each
/// filled once with `records` 100-byte values.
async fn broker_with_logs() -> FakeBroker {
    let broker = FakeBroker::start().await.expect("fake broker");
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .client_id("bench-fill")
        .connect()
        .await
        .expect("connect")
        .producer()
        .linger(Duration::from_millis(5))
        .batch_size(1 << 20)
        .build()
        .await
        .expect("producer");
    for records in RECORD_COUNTS {
        let topic = topic(records);
        broker.create_topic(&topic, 1);
        let mut handles = Vec::with_capacity(records);
        for _ in 0..records {
            handles.push(
                producer
                    .enqueue(krafka::Record::new(topic.as_str(), PAYLOAD).partition(0))
                    .await
                    .expect("enqueue"),
            );
        }
        producer.flush().await.expect("flush");
        for h in handles {
            let _ = h.await.expect("delivery");
        }
    }
    producer.close().await.expect("close");
    broker
}

/// A consumer manually assigned partition 0 of `topic`.
async fn assigned_consumer(broker: &FakeBroker, topic: &str) -> Consumer {
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .client_id("bench-consume")
        .connect()
        .await
        .expect("connect")
        .consumer_without_group()
        .build()
        .await
        .expect("consumer");
    consumer.assign(topic, vec![0]).await.expect("assign");
    consumer
}

/// `poll(timeout)` until the whole log has been delivered.
fn bench_poll(c: &mut Criterion) {
    let rt = runtime();
    let broker = rt.block_on(broker_with_logs());
    let mut group = c.benchmark_group("consume_poll");
    group.sample_size(20);

    for records in RECORD_COUNTS {
        let topic = topic(records);
        let consumer = rt.block_on(assigned_consumer(&broker, &topic));
        group.throughput(Throughput::Elements(records as u64));
        group.bench_with_input(BenchmarkId::new("records", records), &records, |b, &n| {
            let consumer = &consumer;
            let topic = topic.as_str();
            b.to_async(&rt).iter(|| async move {
                consumer.seek(topic, 0, 0).await.expect("seek");
                let mut delivered = 0;
                while delivered < n {
                    let batch = consumer.poll(Duration::from_secs(5)).await.expect("poll");
                    delivered += batch.len();
                    let _ = black_box(batch);
                }
            });
        });
    }
    group.finish();
}

/// `recv()` one record at a time until the whole log has been delivered.
fn bench_recv(c: &mut Criterion) {
    let rt = runtime();
    let broker = rt.block_on(broker_with_logs());
    let mut group = c.benchmark_group("consume_recv");
    group.sample_size(20);

    for records in RECORD_COUNTS {
        let topic = topic(records);
        let consumer = rt.block_on(assigned_consumer(&broker, &topic));
        group.throughput(Throughput::Elements(records as u64));
        group.bench_with_input(BenchmarkId::new("records", records), &records, |b, &n| {
            let consumer = &consumer;
            let topic = topic.as_str();
            b.to_async(&rt).iter(|| async move {
                consumer.seek(topic, 0, 0).await.expect("seek");
                for _ in 0..n {
                    let record = consumer.recv().await.expect("recv").expect("open");
                    let _ = black_box(record);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_poll, bench_recv);
criterion_main!(benches);
