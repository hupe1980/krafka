//! Exactly-once consume-transform-produce with a transactional producer.
//!
//! Each batch read from `example-orders` is transformed and written to
//! `example-orders-upper`, and the consumer's offsets are committed inside the
//! same transaction: the output records and the input positions become
//! visible together, or not at all.
//!
//! - The consumer reads `read_committed` and never commits on its own.
//! - `send_offsets` takes the consumer's `ConsumerGroupMetadata`, read again
//!   for every transaction: its generation and member id let the group
//!   coordinator refuse the commit of an instance that has lost its
//!   partitions to a rebalance.
//! - A failed send or offset commit aborts the transaction, and the batch is
//!   read again after the consumer seeks back to its committed offsets.
//!
//! Stops after ten seconds without input. Needs a broker at
//! `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) and records in
//! `example-orders`, for instance from the `producer` example.
//!
//! Run with:
//! ```sh
//! cargo run --example exactly_once
//! ```

use std::time::Duration;

use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use krafka::producer::{TopicPartitionOffset, TransactionalProducer};
use krafka::{Kafka, Record};

const INPUT_TOPIC: &str = "example-orders";
const OUTPUT_TOPIC: &str = "example-orders-upper";
const GROUP_ID: &str = "krafka-exactly-once-example";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".into());
    let kafka = Kafka::builder(bootstrap)
        .client_id("krafka-exactly-once-example")
        .connect()
        .await?;

    let consumer = kafka
        .consumer(GROUP_ID)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .enable_auto_commit(false)
        .build()
        .await?;

    // Building registers the transactional id: it fences any earlier
    // instance with the same id and aborts the transaction it left open.
    // Keep delivery_timeout at or below transaction_timeout.
    let producer = kafka
        .producer()
        .transaction_timeout(Duration::from_secs(60))
        .delivery_timeout(Duration::from_secs(45))
        .build_transactional("krafka-exactly-once-example-0")
        .await?;

    consumer.subscribe([INPUT_TOPIC]).await?;
    println!("{INPUT_TOPIC} -> {OUTPUT_TOPIC}");

    let mut idle = Duration::ZERO;
    while idle < Duration::from_secs(10) {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        if records.is_empty() {
            idle += Duration::from_secs(1);
            continue;
        }
        idle = Duration::ZERO;

        producer.begin()?;
        match process(&consumer, &producer, &records).await {
            Ok(()) => {
                producer.commit().await?;
                println!("committed {} records", records.len());
            }
            Err(error) => {
                eprintln!("aborting: {error}");
                producer.abort().await?;
                rewind(&consumer, &records).await?;
            }
        }
    }

    producer.close().await?;
    consumer.close().await?;
    Ok(())
}

/// Write the transformed batch and stage the consumer's offsets in the open
/// transaction.
async fn process(
    consumer: &Consumer,
    producer: &TransactionalProducer,
    records: &[ConsumerRecord],
) -> krafka::Result<()> {
    let mut offsets = Vec::new();
    for record in records {
        if let Some(value) = &record.value {
            let mut output = Record::new(OUTPUT_TOPIC, value.to_ascii_uppercase());
            output.key = record.key.clone();
            // The transaction, not the send, decides visibility.
            let _ = producer.send(output).await?;
        }
        // The offset to commit is the next one to read.
        offsets.push(TopicPartitionOffset::new(
            &*record.topic,
            record.partition,
            record.offset + 1,
        ));
    }

    // Not a group member right now (mid-rebalance): the commit could not be
    // fenced, so give the batch up.
    let Some(group_metadata) = consumer.group_metadata().await else {
        return Err(krafka::KrafkaError::transaction_abortable(
            "not a group member",
        ));
    };
    producer.send_offsets(&offsets, &group_metadata).await
}

/// Seek every partition in the aborted batch back to its first record, so it
/// is read and processed again.
async fn rewind(consumer: &Consumer, records: &[ConsumerRecord]) -> krafka::Result<()> {
    let mut first = std::collections::HashMap::new();
    for record in records {
        first
            .entry((record.topic.clone(), record.partition))
            .or_insert(record.offset);
    }
    for ((topic, partition), offset) in first {
        consumer.seek(&topic, partition, offset).await?;
    }
    Ok(())
}
