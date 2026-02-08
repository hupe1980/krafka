---
layout: default
title: Getting Started
nav_order: 2
description: "Get up and running with Krafka in minutes"
---

# Getting Started with Krafka

This guide will help you get up and running with Krafka in just a few minutes.

## Installation

Add Krafka to your `Cargo.toml`:

```toml
[dependencies]
krafka = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Prerequisites

- Rust 1.70 or later
- A running Kafka cluster (or use Docker)

### Running Kafka with Docker

```bash
# Start Kafka with Docker Compose
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  confluentinc/cp-kafka:latest
```

Or use our `docker-compose.yml`:

```yaml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

## Your First Producer

```rust
use krafka::producer::Producer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a producer
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .client_id("my-producer")
        .build()
        .await?;

    // Send a message
    let metadata = producer
        .send("my-topic", Some(b"key"), b"Hello, Kafka!")
        .await?;

    println!(
        "Message sent to partition {} at offset {}",
        metadata.partition, metadata.offset
    );

    // Close the producer
    producer.close().await;

    Ok(())
}
```

## Your First Consumer

```rust
use krafka::consumer::Consumer;
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a consumer
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("my-group")
        .client_id("my-consumer")
        .build()
        .await?;

    // Subscribe to topics
    consumer.subscribe(&["my-topic"]).await?;

    // Poll for messages
    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        
        for record in records {
            println!(
                "Received: topic={}, partition={}, offset={}, key={:?}, value={:?}",
                record.topic,
                record.partition,
                record.offset,
                record.key.map(|k| String::from_utf8_lossy(&k).to_string()),
                String::from_utf8_lossy(&record.value)
            );
        }
    }
}
```

## Using the Admin Client

```rust
use krafka::admin::{AdminClient, NewTopic};
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let admin = AdminClient::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // Create a topic
    let topic = NewTopic::new("new-topic", 3, 1)
        .with_config("retention.ms", "86400000");

    let results = admin
        .create_topics(vec![topic], Duration::from_secs(30))
        .await?;

    for result in results {
        match result.error {
            None => println!("Created topic: {}", result.name),
            Some(e) => println!("Failed to create {}: {}", result.name, e),
        }
    }

    // List topics
    let topics = admin.list_topics().await?;
    println!("Topics: {:?}", topics);

    Ok(())
}
```

## Configuration Options

See the [Configuration Reference](configuration.md) for all available options.

### Common Producer Options

```rust
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::All)                          // Wait for all replicas
    .compression(Compression::Lz4)             // Use LZ4 compression
    .batch_size(65536)                         // 64KB batches
    .linger(Duration::from_millis(5))          // Wait up to 5ms for batching
    .build()
    .await?;
```

### Common Consumer Options

```rust
use krafka::consumer::{Consumer, AutoOffsetReset};
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .auto_offset_reset(AutoOffsetReset::Earliest)  // Start from beginning
    .enable_auto_commit(true)                       // Auto-commit offsets
    .auto_commit_interval(Duration::from_secs(5))   // Commit every 5 seconds
    .build()
    .await?;
```

## Next Steps

- [Producer Guide](producer.md) - Advanced producer usage
- [Consumer Guide](consumer.md) - Consumer groups and offset management
- [Configuration Reference](configuration.md) - All configuration options
- [Architecture Overview](architecture.md) - How Krafka works internally
