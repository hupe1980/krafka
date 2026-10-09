//! A share-group consumer (KIP-932) that settles every record explicitly.
//!
//! In a share group the broker hands each record to one member at a time and
//! tracks it per record, so members of one group can outnumber the
//! partitions. With `AcknowledgementMode::Explicit` the application settles
//! each delivered record: `ack` (processed), `release` (deliver again, maybe
//! to another member) or `reject` (never deliver again). `commit` sends the
//! acknowledgements and reports the outcome per partition.
//!
//! Needs a Kafka 4.2+ broker at
//! `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`), and records in
//! `example-orders`, for instance from the `producer` example. A new share
//! group starts at the log end, so produce after this example has joined.
//!
//! Run with:
//! ```sh
//! cargo run --example share_consumer
//! ```

use std::time::Duration;

use krafka::Kafka;
use krafka::share_consumer::AcknowledgementMode;

const TOPIC: &str = "example-orders";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-share-consumer-example")
        .connect()
        .await?;
    let consumer = kafka
        .share_consumer("krafka-share-example")
        .acknowledgement_mode(AcknowledgementMode::Explicit)
        .build()
        .await?;
    consumer.subscribe([TOPIC]).await?;
    println!("joined share group; waiting up to 30 s for records");

    let mut idle = Duration::ZERO;
    while idle < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        if records.is_empty() {
            idle += Duration::from_secs(1);
            continue;
        }
        idle = Duration::ZERO;

        for record in &records {
            match record.value_str() {
                // Processed: archive it.
                Some(value) if !value.is_empty() => {
                    println!(
                        "{}-{}@{} {value}",
                        record.topic, record.partition, record.offset
                    );
                    consumer.ack(record)?;
                }
                // Not processable by anyone: archive it without processing.
                _ => consumer.reject(record)?,
            }
        }

        for (partition, outcome) in consumer.commit().await? {
            if let Err(error) = outcome {
                eprintln!(
                    "acknowledging {}-{} failed: {error}",
                    partition.topic, partition.partition
                );
            }
        }
    }

    consumer.close().await?;
    Ok(())
}
