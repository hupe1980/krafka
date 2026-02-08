# 🦀 Krafka

[![CI](https://github.com/hupe1980/krafka/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/krafka/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/krafka.svg)](https://crates.io/crates/krafka)
[![Documentation](https://docs.rs/krafka/badge.svg)](https://docs.rs/krafka)
[![MSRV](https://img.shields.io/badge/MSRV-1.85-blue.svg)](https://github.com/rust-lang/rust/releases/tag/1.85.0)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A pure Rust, async-native Apache Kafka client designed for high performance, safety, and ease of use.

## ✨ Features

- 🦀 **Pure Rust**: No librdkafka or C dependencies
- ⚡ **Async-native**: Built on Tokio for true async I/O
- 🔒 **Zero unsafe**: Safe Rust by default
- 🚀 **High performance**: Zero-copy buffers, inline hot paths, efficient batching
- 📦 **Full protocol support**: Kafka protocol with all compression codecs
- 🔐 **TLS/SSL encryption**: Using rustls for secure connections
- 🔑 **SASL authentication**: PLAIN, SCRAM-SHA-256/512 mechanisms
- 💯 **Transactions**: Exactly-once semantics with transactional producer
- ☁️ **Cloud-native**: First-class AWS MSK support including IAM auth

## 🚀 Quick Start

Add Krafka to your `Cargo.toml`:

```toml
[dependencies]
krafka = "0.1"
tokio = { version = "1", features = ["full"] }

# For AWS MSK IAM authentication with full SDK support:
# krafka = { version = "0.1", features = ["aws-msk"] }
```

### Producer

```rust
use krafka::producer::Producer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .client_id("my-producer")
        .build()
        .await?;

    // Send a message
    let metadata = producer
        .send("my-topic", Some(b"key"), b"Hello, Kafka!")
        .await?;
    
    println!("Sent to partition {} at offset {}", 
             metadata.partition, metadata.offset);

    producer.close().await;
    Ok(())
}
```

### Consumer

```rust
use krafka::consumer::{Consumer, AutoOffsetReset};
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("my-consumer-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;

    consumer.subscribe(&["my-topic"]).await?;

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in records {
            if let Some(ref value) = record.value {
                println!(
                    "Received: topic={}, partition={}, offset={}, value={:?}",
                    record.topic,
                    record.partition,
                    record.offset,
                    String::from_utf8_lossy(value)
                );
            }
        }
    }
}
```

### Admin Client

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
    let topic = NewTopic::new("new-topic", 6, 3)
        .with_config("retention.ms", "604800000");

    admin.create_topics(vec![topic], Duration::from_secs(30)).await?;

    // List topics
    let topics = admin.list_topics().await?;
    println!("Topics: {:?}", topics);

    Ok(())
}
```

### Transactional Producer

For exactly-once semantics across multiple partitions:

```rust
use krafka::producer::TransactionalProducer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("my-transaction")
        .build()
        .await?;

    // Initialize transactions (once per producer)
    producer.init_transactions().await?;

    // Atomic transaction
    producer.begin_transaction()?;
    producer.send("topic-a", Some(b"key"), b"value1").await?;
    producer.send("topic-b", Some(b"key"), b"value2").await?;
    producer.commit_transaction().await?;

    Ok(())
}
```

## 📦 Modules

| Module | Description |
|--------|-------------|
| `producer` | High-throughput message production with batching and compression |
| `consumer` | Consumer groups with rebalancing and offset management |
| `admin` | Cluster administration (create/delete topics, describe cluster) |
| `protocol` | Kafka wire protocol implementation |
| `auth` | Authentication (SASL/PLAIN, SASL/SCRAM, AWS MSK IAM) |

## 🗜️ Compression

Krafka supports all Kafka compression codecs:

```rust
use krafka::producer::Producer;
use krafka::protocol::Compression;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .compression(Compression::Lz4)  // Fast compression
    .build()
    .await?;
```

| Codec | Crate | Characteristics |
|-------|-------|-----------------|
| `Compression::Gzip` | flate2 | Best ratio, slower |
| `Compression::Snappy` | snap | Good balance |
| `Compression::Lz4` | lz4_flex | Fastest |
| `Compression::Zstd` | zstd | Best modern choice |

## ⚡ Performance Tuning

### High Throughput Producer

```rust
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::Leader)
    .compression(Compression::Lz4)
    .batch_size(1048576)                  // 1MB batches
    .linger(Duration::from_millis(10))    // Allow batching
    .build()
    .await?;
```

### Low Latency Consumer

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("low-latency")
    .fetch_min_bytes(1)
    .fetch_max_wait(Duration::from_millis(10))
    .build()
    .await?;
```

## 📚 Documentation

Full documentation is available at **[hupe1980.github.io/krafka](https://hupe1980.github.io/krafka)**

- [Getting Started](https://hupe1980.github.io/krafka/getting-started)
- [Producer Guide](https://hupe1980.github.io/krafka/producer)
- [Consumer Guide](https://hupe1980.github.io/krafka/consumer)
- [Admin Client](https://hupe1980.github.io/krafka/admin)
- [Configuration Reference](https://hupe1980.github.io/krafka/configuration)
- [Performance Tuning](https://hupe1980.github.io/krafka/performance)
- [Architecture Overview](https://hupe1980.github.io/krafka/architecture)
- [Metrics & Observability](https://hupe1980.github.io/krafka/metrics)
- [Error Handling](https://hupe1980.github.io/krafka/errors)
- [Authentication](https://hupe1980.github.io/krafka/authentication)

## 🎮 Examples

Run the examples with:

```bash
# Producer example
cargo run --example producer

# Consumer example
cargo run --example consumer

# Advanced consumer example (pause/resume, seek, manual commits)
cargo run --example consumer_advanced

# Admin client example
cargo run --example admin

# Transactional producer example
cargo run --example transactional_producer

# Authentication examples (SASL, SCRAM, MSK IAM)
cargo run --example authentication
```

## 📊 Status

Krafka is feature-complete and production-ready.

**Features:**
- ✅ Protocol layer (all message types, compression, ACL messages, transactions)
- ✅ Network layer (async connections, pooling, TLS/SSL)
- ✅ Producer (batching with linger timer, partitioning, compression, retry policy, idempotence)
- ✅ Consumer (polling, offset management, seek, pause/resume, rebalance listeners, cooperative sticky assignor)
- ✅ Admin Client (topic CRUD, partitions, configuration, ACL management)
- ✅ Authentication (SASL/PLAIN, SASL/SCRAM-SHA-256/512, AWS MSK IAM with SDK support)
- ✅ TLS/SSL encryption (rustls, mTLS support)
- ✅ Transactions (exactly-once semantics with transactional producer)
- ✅ Metrics (counters, gauges, latency tracking)
- ✅ Tracing (OpenTelemetry-compatible spans)

## 🤝 Contributing

Contributions are welcome!

## 📄 License

Licensed under the [MIT License](LICENSE).
