//! Producer example.
//!
//! Demonstrates how to produce messages to Kafka using Krafka.
//!
//! Run with:
//! ```
//! cargo run --example producer
//! ```

use std::time::Duration;

use krafka::producer::{Acks, Producer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("krafka=debug")
        .init();

    // Create producer with fluent builder API
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .client_id("krafka-producer-example")
        .acks(Acks::All)
        .retries(3)
        .batch_size(16384)
        .linger(Duration::from_millis(5))
        .build()
        .await?;

    println!("Producer connected to Kafka!");

    // Send messages
    let topic = "test-topic";

    // Simple send (with key and value as bytes)
    let metadata = producer.send(topic, None, b"Hello, Kafka!").await?;
    println!(
        "Sent message to partition {} at offset {}",
        metadata.partition, metadata.offset
    );

    // Send with key
    let metadata = producer
        .send(topic, Some(b"key-1"), b"Message with key")
        .await?;
    println!(
        "Sent keyed message to partition {} at offset {}",
        metadata.partition, metadata.offset
    );

    // Send with headers
    let headers = vec![
        ("trace-id".to_string(), bytes::Bytes::from_static(b"abc123")),
        (
            "source".to_string(),
            bytes::Bytes::from_static(b"producer-example"),
        ),
    ];

    let metadata = producer
        .send_with_headers(topic, Some(b"key-2"), b"Message with headers", headers)
        .await?;
    println!(
        "Sent message with headers to partition {} at offset {}",
        metadata.partition, metadata.offset
    );

    // Flush and close
    producer.close().await;
    println!("Producer closed gracefully");

    Ok(())
}
