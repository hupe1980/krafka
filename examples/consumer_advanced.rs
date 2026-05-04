//! Advanced consumer example.
//!
//! Demonstrates advanced consumer features:
//! - Pause and resume partition consumption
//! - Seek to specific offsets
//! - Manual offset commits
//! - Position tracking
//!
//! Run with:
//! ```
//! cargo run --example consumer_advanced
//! ```

use std::time::Duration;

use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krafka::error::Result;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("krafka=debug")
        .init();

    // Create consumer with manual commit for precise offset control
    let consumer: Consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("krafka-advanced-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false) // Manual commit for precise control
        .build()
        .await?;

    println!("Consumer connected!");

    // Subscribe to topics
    consumer.subscribe(&["test-topic"]).await?;

    // Get assigned partitions
    let assignments = consumer.assignment().await;
    println!("Assigned partitions: {:?}", assignments);

    let mut message_count = 0;
    let mut batch_count = 0;

    loop {
        let poll_result: Result<Vec<ConsumerRecord>> =
            consumer.poll(Duration::from_millis(500)).await;
        match poll_result {
            Ok(records) => {
                if records.is_empty() {
                    continue;
                }

                batch_count += 1;
                println!("\n--- Batch {} ---", batch_count);

                for record in &records {
                    message_count += 1;
                    println!(
                        "[{}] {} partition={} offset={}: {:?}",
                        message_count,
                        record.topic,
                        record.partition,
                        record.offset,
                        record.value_str().unwrap_or("<binary>"),
                    );

                    // Example: Pause partition after receiving 5 messages from it
                    if message_count == 5 {
                        println!(
                            "\n>>> Pausing partition {} for demonstration...",
                            record.partition
                        );
                        consumer.pause(&record.topic, &[record.partition]).await;

                        // Show paused partitions
                        let paused = consumer.paused_partitions().await;
                        println!(">>> Paused partitions: {:?}", paused);
                    }

                    // Example: Resume after 10 messages
                    if message_count == 10 {
                        println!("\n>>> Resuming all paused partitions...");
                        let paused = consumer.paused_partitions().await;
                        for (topic, partition) in paused {
                            consumer.resume(&topic, &[partition]).await;
                        }
                    }

                    // Example: Seek to beginning after 15 messages
                    if message_count == 15 {
                        println!(
                            "\n>>> Seeking to beginning of partition {}...",
                            record.partition
                        );
                        consumer
                            .seek_to_beginning(&record.topic, record.partition)
                            .await?;
                    }
                }

                // Manual commit after processing batch
                println!("Committing offsets for {} records...", records.len());
                consumer.commit().await?;

                // Show current positions
                for (topic, partitions) in consumer.assignment().await {
                    for partition in partitions {
                        if let Some(pos) = consumer.position(&topic, partition).await {
                            println!("  Position {}-{}: {}", topic, partition, pos);
                        }
                    }
                }

                // Stop after processing 20 messages for demo
                if message_count >= 20 {
                    println!("\n>>> Demo complete! Processed {} messages.", message_count);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error polling: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // Clean shutdown
    consumer.commit().await?;
    consumer.close().await?;
    println!("Consumer closed gracefully.");

    Ok(())
}
