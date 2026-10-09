//! A consumer group member that commits offsets itself.
//!
//! Auto-commit is off: the example commits after it has processed each
//! batch, so a crash re-delivers at most the batch in progress
//! (at-least-once). It stops after `MAX_RECORDS` records, or when no record
//! arrives for ten seconds, and leaves the group on `close`.
//!
//! Needs a broker at `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) and
//! records in `example-orders`, for instance from the `producer` example.
//!
//! Run with:
//! ```sh
//! cargo run --example producer
//! cargo run --example consumer
//! ```

use std::time::Duration;

use krafka::Kafka;
use krafka::consumer::AutoOffsetReset;

const TOPIC: &str = "example-orders";
const MAX_RECORDS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-consumer-example")
        .connect()
        .await?;
    let consumer = kafka
        .consumer("krafka-consumer-example")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await?;
    consumer.subscribe([TOPIC]).await?;

    let mut processed = 0;
    let mut idle = Duration::ZERO;
    while processed < MAX_RECORDS && idle < Duration::from_secs(10) {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        if records.is_empty() {
            idle += Duration::from_secs(1);
            continue;
        }
        idle = Duration::ZERO;

        for record in &records {
            println!(
                "{}-{}@{} key={:?} value={:?}",
                record.topic,
                record.partition,
                record.offset,
                record.key_str(),
                record.value_str(),
            );
            processed += 1;
        }

        // Commit the positions after the batch: the offset of the next
        // record to read on every assigned partition.
        consumer.commit().await?;
    }

    for (partition, lag) in consumer.lag().await {
        println!("lag {}-{}: {:?}", partition.topic, partition.partition, lag);
    }

    consumer.close().await?;
    println!("processed {processed} records");
    Ok(())
}
