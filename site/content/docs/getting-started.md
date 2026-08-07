+++
title = "Getting Started"
description = "Install krafka and produce and consume your first records in under five minutes."
weight = 10

[extra]
slug_id = "getting-started"
+++

## Installation

Add krafka to your `Cargo.toml`:

```toml
[dependencies]
krafka = "0.17.0"
tokio = { version = "1", features = ["full"] }
```

## Prerequisites

- Rust 1.88 or later (MSRV 1.88)
- **Apache Kafka 3.9 or later** (older brokers are not supported)
- A running Kafka cluster (or use Docker)

### Running Kafka with Docker

```bash
# Start Kafka in KRaft mode (no ZooKeeper required)
docker run -d --name kafka \
  -p 9092:9092 \
  apache/kafka-native:3.9.0
```

Or use a `docker-compose.yml`:

```yaml
services:
  kafka:
    image: apache/kafka-native:3.9.0
    ports:
      - "9092:9092"
```

## The prelude

Every example below spells out its imports so you can see where each type comes
from. In your own code, one glob import covers the common ones:

```rust,compile
use krafka::prelude::*;
```

`Result` is deliberately *not* in the prelude: krafka's alias takes one type
parameter, so importing it would shadow `std::result::Result` and break every
`Result<T, E>` in the same module. Write `krafka::Result<T>` when you want it.

## Your First Producer

```rust,compile
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

```rust,compile
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
                record
                    .value
                    .as_deref()
                    .map(String::from_utf8_lossy)
                    .unwrap_or_default()
            );
        }
    }
}
```

## Using the Admin Client

```rust,compile
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
    // `new` validates the topic name, so it returns a Result.
    let topic = NewTopic::new("new-topic", 3, 1)?
        .with_config("retention.ms", "86400000");

    let results = admin
        .create_topics(vec![topic], Duration::from_secs(30), false)
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

See the [Configuration Reference](@/docs/configuration.md) for all available options.

### Common Producer Options

```rust,compile
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

```rust,compile
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

- [Producer Guide](@/docs/producer.md) - Advanced producer usage
- [Consumer Guide](@/docs/consumer.md) - Consumer groups and offset management
- [Configuration Reference](@/docs/configuration.md) - All configuration options
- [Architecture Overview](@/docs/architecture.md) - How krafka works internally
