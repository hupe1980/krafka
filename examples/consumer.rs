//! Consumer example.
//!
//! Demonstrates how to consume messages from Kafka using Krafka.
//!
//! Run with:
//! ```
//! cargo run --example consumer
//! ```

use std::time::Duration;

use krafka::consumer::{AutoOffsetReset, Consumer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("krafka=debug")
        .init();

    // Create consumer with fluent builder API
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("krafka-consumer-example")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(true)
        .build()
        .await?;

    println!("Consumer connected to Kafka!");

    // Subscribe to topics
    consumer.subscribe(&["test-topic"]).await?;
    println!("Subscribed to test-topic");

    // Poll for messages
    println!("Polling for messages (Ctrl+C to stop)...");

    loop {
        match consumer.poll(Duration::from_millis(100)).await {
            Ok(records) => {
                for record in records {
                    println!(
                        "Received: topic={}, partition={}, offset={}, key={:?}, value={:?}",
                        record.topic,
                        record.partition,
                        record.offset,
                        record.key_str(),
                        record.value_str(),
                    );

                    // Print headers if any
                    if !record.headers.is_empty() {
                        println!("  Headers:");
                        for (key, value) in &record.headers {
                            let key_str = std::str::from_utf8(key).unwrap_or("<binary>");
                            match value {
                                Some(v) => println!(
                                    "    {}: {:?}",
                                    key_str,
                                    std::str::from_utf8(v).unwrap_or("<binary>")
                                ),
                                None => println!("    {}: <null>", key_str),
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Error polling: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
