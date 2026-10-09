//! Producing records: `send` and `enqueue`.
//!
//! - `send` waits for the broker's acknowledgement of one record and returns
//!   its partition and offset.
//! - `enqueue` returns once the record is queued and hands back a
//!   `DeliveryHandle`; awaiting the handles later lets many records share one
//!   batch. Produce order is enqueue order.
//!
//! Needs a broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) with
//! `auto.create.topics.enable=true` (the broker default), or an existing
//! `example-orders` topic.
//!
//! Run with:
//! ```sh
//! cargo run --example producer
//! ```

use std::time::Duration;

use krafka::producer::Acks;
use krafka::{Kafka, Record};

const TOPIC: &str = "example-orders";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-producer-example")
        // Ask the broker to create the topic on first use.
        .allow_auto_create_topics(true)
        .connect()
        .await?;
    let producer = kafka
        .producer()
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        .build()
        .await?;

    // `send`: one record, one acknowledgement. The key picks the partition,
    // so every record for one order lands on the same partition, in order.
    let metadata = producer
        .send(
            Record::new(TOPIC, "order 1 created")
                .key("order-1")
                .header("source", "producer-example"),
        )
        .await?;
    println!(
        "send: {}-{} at offset {}",
        metadata.topic, metadata.partition, metadata.offset
    );

    // `enqueue`: queue many records first, then collect the outcomes.
    let mut handles = Vec::new();
    for i in 2..=11 {
        let record = Record::new(TOPIC, format!("order {i} created")).key(format!("order-{i}"));
        handles.push(producer.enqueue(record).await?);
    }
    for handle in handles {
        let metadata = handle.await?;
        println!(
            "enqueue: {}-{} at offset {}",
            metadata.topic, metadata.partition, metadata.offset
        );
    }

    // `close` flushes what is still queued.
    producer.close().await?;
    Ok(())
}
