//! CPU time per record on the send path, against the in-process fake broker.
//!
//! Run with: `cargo bench --bench send_cpu --features test-broker`
//! (`KRAFKA_SEND_CPU_RECORDS` sets the record count; default 200 000).
//!
//! # What this measures
//!
//! The process's user + system CPU time divided by records, for a streaming
//! caller: records go through `enqueue` with at most [`WINDOW`] handles
//! outstanding, awaited in order. Three configurations run in one process:
//!
//! - `floor`: build the same records and drop them; no Kafka work.
//! - `no-subscriber`: the send path with no `tracing` subscriber.
//! - `fmt-info`: the same with a `tracing_subscriber::fmt` subscriber at
//!   `info`, writing to a sink — the logging setup most applications have.
//!
//! The fake broker runs in this process, so its CPU is inside every figure.
//! Like the other send benches this compares krafka against krafka across
//! two runs on one machine; no figure here is quotable.

#![allow(missing_docs, clippy::panic, clippy::expect_used, clippy::unwrap_used)]

use std::collections::VecDeque;
use std::hint::black_box;
use std::time::{Duration, Instant};

use krafka::producer::{Producer, Record};
use krafka::testing::FakeBroker;

/// Outstanding handles a streaming caller keeps before awaiting the oldest.
const WINDOW: usize = 1024;

const DEFAULT_RECORDS: usize = 200_000;

/// A CDC-sized JSON row.
const VALUE: &[u8] = br#"{"op":"c","table":"orders","id":123456,"customer":"c-00042","amount":"129.95","currency":"EUR","status":"new","ts_ms":1763100000000}"#;

fn records() -> usize {
    std::env::var("KRAFKA_SEND_CPU_RECORDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_RECORDS)
}

fn record(i: usize) -> Record {
    Record::new("bench", VALUE).key(format!("{i}"))
}

/// User + system CPU time of this process.
#[cfg(unix)]
fn process_cpu() -> Option<Duration> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` writes a `rusage` into the pointer it is given; the
    // pointer is to a zeroed, properly aligned `rusage` that outlives the call.
    #[allow(unsafe_code)]
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `getrusage` returned 0, so it initialised the struct.
    #[allow(unsafe_code)]
    let usage = unsafe { usage.assume_init() };
    let tv = |t: libc::timeval| {
        Duration::from_secs(t.tv_sec as u64) + Duration::from_micros(t.tv_usec as u64)
    };
    Some(tv(usage.ru_utime) + tv(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu() -> Option<Duration> {
    None
}

struct Figure {
    name: &'static str,
    cpu_us_per_record: f64,
    wall_us_per_record: f64,
}

fn measure(name: &'static str, n: usize, run: impl FnOnce()) -> Option<Figure> {
    let cpu0 = process_cpu()?;
    let wall0 = Instant::now();
    run();
    let wall = wall0.elapsed();
    let cpu = process_cpu()?.saturating_sub(cpu0);
    Some(Figure {
        name,
        cpu_us_per_record: cpu.as_secs_f64() * 1e6 / n as f64,
        wall_us_per_record: wall.as_secs_f64() * 1e6 / n as f64,
    })
}

async fn stream(producer: &Producer, n: usize) {
    let mut window = VecDeque::with_capacity(WINDOW);
    for i in 0..n {
        if window.len() == WINDOW {
            let handle: krafka::producer::DeliveryHandle =
                window.pop_front().expect("window is full");
            let _ = black_box(handle.await.expect("delivered"));
        }
        window.push_back(producer.enqueue(record(i)).await.expect("enqueued"));
    }
    for handle in window {
        let _ = black_box(handle.await.expect("delivered"));
    }
    producer.flush().await.expect("flushed");
}

fn main() {
    if process_cpu().is_none() {
        println!("send_cpu: CPU time unsupported on this platform");
        return;
    }
    let n = records();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime");
    let (broker, producer) = rt.block_on(async {
        let broker = FakeBroker::start().await.expect("fake broker");
        broker.create_topic("bench", 4);
        let producer = krafka::Kafka::builder(broker.bootstrap_servers())
            .client_id("bench")
            .connect()
            .await
            .expect("connect")
            .producer()
            .build()
            .await
            .expect("producer");
        // Warm the connection, the metadata and the producer id.
        rt_warm(&producer).await;
        (broker, producer)
    });

    let mut figures = Vec::new();
    figures.extend(measure("floor", n, || {
        for i in 0..n {
            let _ = black_box(record(i));
        }
    }));
    figures.extend(measure("no-subscriber", n, || {
        rt.block_on(stream(&producer, n));
    }));
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::sink)
        .init();
    figures.extend(measure("fmt-info", n, || {
        rt.block_on(stream(&producer, n));
    }));

    println!("send_cpu: {n} records, window {WINDOW}");
    for f in &figures {
        println!(
            "  {:<14} {:>8.3} µs CPU/record  {:>8.3} µs wall/record",
            f.name, f.cpu_us_per_record, f.wall_us_per_record
        );
    }
    let json: Vec<String> = figures
        .iter()
        .map(|f| {
            format!(
                "\"{}\":{{\"cpu_us\":{:.4},\"wall_us\":{:.4}}}",
                f.name, f.cpu_us_per_record, f.wall_us_per_record
            )
        })
        .collect();
    println!("{{\"records\":{n},{}}}", json.join(","));

    rt.block_on(async {
        producer.close().await.expect("close");
    });
    drop(broker);
}

async fn rt_warm(producer: &Producer) {
    for i in 0..1000 {
        let _ = producer.send(record(i)).await.expect("warm-up send");
    }
}
