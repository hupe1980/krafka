//! Exactly-once read-process-write with a transactional producer.
//!
//! This is the pattern behind "exactly-once stream processing": consume from an
//! input topic, transform, produce to an output topic, and commit the consumer's
//! offsets *inside the same transaction* as the output records. Either both land
//! or neither does.
//!
//! # The part people get wrong
//!
//! [`TransactionalProducer::send_offsets_to_transaction`] takes a
//! [`ConsumerGroupMetadata`], not a bare group ID. That metadata is what lets
//! the group coordinator fence a **zombie**: an instance that was partitioned
//! away, lost its partitions to a rebalance, and then came back still holding a
//! transaction. Without the generation and member ID, the coordinator accepts
//! the zombie's commit unconditionally and it overwrites the position of the
//! member that now owns the partition — so the new owner skips records nobody
//! processed, or reprocesses records that were already handled.
//!
//! Re-read the metadata for **every** transaction. The generation changes on
//! every rebalance, so a value captured once and cached stops fencing correctly
//! at exactly the moment it matters.
//!
//! Run with:
//! ```sh
//! cargo run --example exactly_once
//! ```

use std::time::Duration;

use krafka::consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krafka::producer::{TopicPartitionOffset, TransactionalProducer};

const INPUT_TOPIC: &str = "input-events";
const OUTPUT_TOPIC: &str = "output-events";
const GROUP_ID: &str = "exactly-once-processor";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The consumer MUST read committed data only. With read_uncommitted a
    // failed upstream transaction's records would be processed and forwarded,
    // which defeats the whole exercise.
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id(GROUP_ID)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        // Offsets are committed by the producer inside the transaction, so the
        // consumer must never commit them on its own timer.
        .enable_auto_commit(false)
        .build()
        .await?;

    let producer = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("exactly-once-processor-0")
        .build()
        .await?;

    // Fences any previous incarnation of this transactional ID and aborts the
    // transactions it left open. Call once per producer.
    producer.init_transactions().await?;

    consumer.subscribe(&[INPUT_TOPIC]).await?;
    println!("Processing {INPUT_TOPIC} -> {OUTPUT_TOPIC} with exactly-once semantics");

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        if records.is_empty() {
            continue;
        }

        producer.begin_transaction()?;

        let mut offsets: Vec<TopicPartitionOffset> = Vec::new();
        for record in &records {
            let Some(value) = &record.value else {
                continue; // tombstone
            };

            let transformed = transform(value);
            // The per-record metadata is not needed here: the transaction, not
            // the individual send, is what determines whether this record is
            // visible to a read_committed consumer.
            let _ = producer
                .send(OUTPUT_TOPIC, record.key.as_deref(), &transformed)
                .await?;

            // Commit the offset of the NEXT record to consume, not this one.
            offsets.push(TopicPartitionOffset::new(
                &record.topic,
                record.partition,
                record.offset + 1,
            ));
        }

        // Re-read on every transaction: the generation changes on every
        // rebalance, and a stale snapshot silently stops fencing zombies.
        let group_metadata = match consumer.group_metadata().await {
            Some(m) => m,
            None => {
                // Not currently a live group member (mid-rebalance, or never
                // joined). A commit now could not be fenced, so abort rather
                // than write offsets the coordinator would accept blindly.
                producer.abort_transaction().await?;
                continue;
            }
        };

        match producer
            .send_offsets_to_transaction(&offsets, &group_metadata)
            .await
        {
            Ok(()) => {
                producer.commit_transaction().await?;
                println!("Committed {} records", records.len());
            }
            Err(e) => {
                // A fenced or otherwise failed offset commit must not be
                // committed alongside the output records.
                eprintln!("Offset commit failed, aborting transaction: {e}");
                producer.abort_transaction().await?;
            }
        }
    }
}

fn transform(value: &[u8]) -> Vec<u8> {
    value.to_ascii_uppercase()
}
