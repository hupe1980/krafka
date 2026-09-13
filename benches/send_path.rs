//! End-to-end send-path benchmarks against the in-process fake broker.
//!
//! Run with: `cargo bench --bench send_path --features test-broker`
//!
//! # What these measure
//!
//! A **regression gate**, not a comparison. These are krafka-vs-krafka numbers:
//! the fake broker holds a single lock and keeps its log in memory, so nothing
//! here supports an absolute claim and no figure belongs in the README.
//!
//! The distinction that makes this work is the axis. Ranking two things *within*
//! one run fails here — an earlier benchmark could not separate the compression
//! codecs, because the harness constant swamped the difference. Comparing krafka
//! against krafka *across* two runs works, because that constant sits on both
//! sides and cancels. So never add a case that ranks settings against each other.
//!
//! `just bench-check` fails when a measurement regresses against the baseline.
//! The accumulator is the first target: it is the most performance-relevant
//! subsystem in the crate and had no performance evidence at all.

#![allow(missing_docs, clippy::panic, clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

use krafka::producer::Producer;
use krafka::testing::FakeBroker;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime")
}

/// A producer wired to a fresh fake broker, with `linger` under the caller's
/// control so the batching path is what varies between cases.
async fn producer_for(linger: Duration, batch_size: usize) -> (FakeBroker, Producer) {
    let broker = FakeBroker::start().await.expect("fake broker");
    broker.create_topic("bench", 4);
    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .client_id("bench")
        .linger(linger)
        .batch_size(batch_size)
        .build()
        .await
        .expect("producer");
    (broker, producer)
}

/// The single-record send path, at the two `linger` settings that exercise
/// different accumulator behaviour.
///
/// `linger = 0` is the configuration that used to take the deleted unbatched
/// path, so it is the one most worth watching for a regression.
fn bench_send_single(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("send_single");
    group.sample_size(30);

    for linger_ms in [0_u64, 5] {
        let (_broker, producer) =
            rt.block_on(producer_for(Duration::from_millis(linger_ms), 16 * 1024));
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("linger_ms", linger_ms),
            &linger_ms,
            |b, _| {
                let producer = &producer;
                b.to_async(&rt).iter(|| async move {
                    let md = producer
                        .send("bench", Some(b"k"), Some(b"v"))
                        .await
                        .expect("send");
                    let _ = black_box(md);
                });
            },
        );
    }
    group.finish();
}

/// Concurrent sends over one `Producer`, which is the shape a real service
/// uses: `Arc<Producer>` shared across tasks, all contending on the
/// accumulator's per-partition batches and the shared send semaphore.
fn bench_send_concurrent(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("send_concurrent");
    group.sample_size(20);

    for tasks in [8_usize, 64] {
        let (_broker, producer) = rt.block_on(producer_for(Duration::from_millis(2), 64 * 1024));
        let producer = std::sync::Arc::new(producer);
        group.throughput(Throughput::Elements(tasks as u64));
        group.bench_with_input(BenchmarkId::new("tasks", tasks), &tasks, |b, &n| {
            b.to_async(&rt).iter(|| {
                let producer = producer.clone();
                async move {
                    let mut handles = Vec::with_capacity(n);
                    for _ in 0..n {
                        let p = producer.clone();
                        handles.push(tokio::spawn(async move {
                            p.send("bench", Some(b"k"), Some(b"value")).await
                        }));
                    }
                    for h in handles {
                        let _ = black_box(h.await.expect("join").expect("send"));
                    }
                }
            });
        });
    }
    group.finish();
}

/// Batched sends: how much per-record cost the accumulator adds as a batch
/// fills. A regression here is the clearest signal that batching has degraded.
fn bench_send_batched(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("send_batched");
    group.sample_size(20);

    for records in [100_usize, 1000] {
        let (_broker, producer) = rt.block_on(producer_for(Duration::from_millis(10), 1 << 20));
        group.throughput(Throughput::Elements(records as u64));
        group.bench_with_input(BenchmarkId::new("records", records), &records, |b, &n| {
            // Keys outlive the futures that borrow them: `send` takes
            // `Option<&[u8]>`, so a per-iteration temporary would be dropped
            // while the future still holds it.
            let keys: Vec<[u8; 4]> = (0..n).map(|i| (i as u32).to_be_bytes()).collect();
            let producer = &producer;
            let keys = &keys;
            b.to_async(&rt).iter(|| async move {
                // `join_all`, not a sequential await loop. Rust futures are
                // lazy, so `send(..)` enqueues nothing until first polled:
                // awaiting a Vec of them one at a time measures N *sequential*
                // sends, each paying a full `linger`, and reports `n * linger`
                // — which looks like a batching collapse and proves nothing.
                let sends = keys
                    .iter()
                    .map(|key| producer.send("bench", Some(key), Some(b"payload")));
                let results = futures::future::join_all(sends).await;
                for r in results {
                    let _ = black_box(r.expect("send"));
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_send_single,
    bench_send_concurrent,
    bench_send_batched
);
criterion_main!(benches);
