//! Client metrics and the Prometheus text format.
//!
//! Produces and consumes a few records, then reads the counters from one
//! client's `Metrics` snapshot and prints `Kafka::metrics()` — the sum over
//! every client of the handle — as Prometheus text, ready to serve from a
//! `/metrics` endpoint.
//!
//! Needs a broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) with
//! `auto.create.topics.enable=true` (the broker default), or an existing
//! `example-metrics` topic.
//!
//! Run with:
//! ```sh
//! cargo run --example metrics
//! ```

use std::time::Duration;

use krafka::consumer::AutoOffsetReset;
use krafka::{Kafka, Record};

const TOPIC: &str = "example-metrics";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-metrics-example")
        // Ask the broker to create the topic on first use.
        .allow_auto_create_topics(true)
        .connect()
        .await?;

    let producer = kafka.producer().build().await?;
    for i in 0..10 {
        let _ = producer
            .send(Record::new(TOPIC, format!("event {i}")))
            .await?;
    }

    let consumer = kafka
        .consumer("krafka-metrics-example")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;
    consumer.subscribe([TOPIC]).await?;
    let mut received = 0;
    for _ in 0..10 {
        received += consumer.poll(Duration::from_secs(1)).await?.len();
        if received >= 10 {
            break;
        }
    }

    // One client's snapshot: plain data, read field by field.
    let metrics = producer.metrics();
    println!(
        "producer: {} records, {} bytes, {} errors, mean send latency {:?}",
        metrics.producer.records_sent,
        metrics.producer.bytes_sent,
        metrics.producer.errors,
        metrics.producer.send_latency.mean(),
    );
    let metrics = consumer.metrics();
    println!(
        "consumer: {} records in {} polls, lag {}",
        metrics.consumer.records_received, metrics.consumer.polls, metrics.consumer.lag,
    );

    // Every client of the handle, connections counted once.
    println!("\n{}", kafka.metrics().prometheus_text());

    consumer.close().await?;
    producer.close().await?;
    Ok(())
}
