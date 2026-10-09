+++
title = "Getting Started"
description = "Install krafka, connect, and produce and consume your first records."
weight = 10

[extra]
slug_id = "getting-started"
+++

## Quick start

```rust,compile
use krafka::{Kafka, Record};

let kafka = Kafka::builder("localhost:9092").connect().await?;
let producer = kafka.producer().build().await?;
producer.send(Record::new("orders", "hello").key("k")).await?;

let consumer = kafka.consumer("my-group").build().await?;
consumer.subscribe(["orders"]).await?;
while let Some(rec) = consumer.recv().await? { println!("{rec:?}"); }
```

One `Kafka` handle holds the connection settings and the connection pool;
the producer, consumers and admin client are built from it. The rest of this
page takes the same steps one at a time.

## Installation

```sh
cargo add krafka
cargo add tokio --features full

# For AWS MSK IAM authentication with the full SDK credential chain:
cargo add krafka --features aws-msk
```

### Cargo features

| Feature | Default | Adds |
|---|---|---|
| `ring` | yes | The `ring` TLS crypto backend |
| `rustls-aws-lc-rs` | | The `aws-lc-rs` TLS backend, with post-quantum key exchange; selected over `ring` when both are on |
| `zstd` | | Zstd *compression* on the producer (Zstd decompression is always available) |
| `aws-msk` | | AWS MSK IAM with the AWS SDK credential chain |
| `oauth-oidc` | | The built-in OIDC token provider for SASL/OAUTHBEARER |
| `tls-encrypted-keys` | | Passphrase-encrypted PKCS#8 client keys |
| `native-tls-roots` | | The operating system's root certificates |
| `test-broker` | | The in-process fake broker in `krafka::testing` |
| `unstable-protocol` | | Protocol versions Kafka marks unstable |

gzip, Snappy and LZ4 compression, the share consumer, SOCKS5 and client
telemetry are always compiled in.

krafka links no C library and needs no system dependency. The default build
compiles C only for `ring`; `zstd`, `rustls-aws-lc-rs` and `aws-msk` compile
more.

## Prerequisites

- Rust 1.95 or later (MSRV 1.95)
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

`Result` is not in the prelude: krafka's alias takes one type parameter and
would shadow `std::result::Result`. Write `krafka::Result<T>`.

## Connect

Connection settings (bootstrap servers, client id, TLS and SASL, timeouts,
transport) are set once on a `Kafka` handle. Every client is built from it and
shares its connection pool:

```rust,compile
use krafka::Kafka;

let kafka = Kafka::builder("localhost:9092")
    .client_id("my-app")
    .connect()
    .await?;
```

`connect()` validates the settings and fetches the cluster metadata, so a
wrong address or credential fails here. The handle is cheap to clone.

## Your First Producer

```rust,compile
use krafka::{Kafka, Record};

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let producer = kafka.producer().build().await?;

    // `send` waits for the broker to acknowledge the record.
    let metadata = producer
        .send(Record::new("my-topic", "Hello, Kafka!").key("key"))
        .await?;
    println!(
        "Message sent to partition {} at offset {}",
        metadata.partition, metadata.offset
    );

    producer.close().await
}
```

## Your First Consumer

```rust,compile
use krafka::Kafka;

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let consumer = kafka.consumer("my-group").build().await?;
    consumer.subscribe(["my-topic"]).await?;

    // `recv` returns `None` once the consumer is closed.
    while let Some(record) = consumer.recv().await? {
        println!(
            "Received: topic={}, partition={}, offset={}, key={:?}, value={:?}",
            record.topic,
            record.partition,
            record.offset,
            record.key_str(),
            record.value_str(),
        );
    }
    Ok(())
}
```

## Using the Admin Client

```rust,compile
use krafka::Kafka;
use krafka::admin::{CreateTopicsOptions, ListTopicsOptions, NewTopic};

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let admin = kafka.admin();

    // `new` validates the topic name, so it returns a Result.
    let topic = NewTopic::new("new-topic", 3, 1)?
        .with_config("retention.ms", "86400000");

    let results = admin
        .create_topics([topic], CreateTopicsOptions::default())
        .await?;

    // One result per topic.
    for (name, result) in results {
        match result {
            Ok(()) => println!("Created topic: {name}"),
            Err(e) => println!("Failed to create {name}: {e}"),
        }
    }

    let topics = admin.list_topics(ListTopicsOptions::default()).await?;
    println!("Topics: {topics:?}");
    admin.close().await
}
```

## Configuration Options

See the [Configuration Reference](@/docs/configuration.md) for all available options.

### Common Producer Options

```rust,compile
use krafka::producer::Acks;
use krafka::Compression;
use std::time::Duration;

let producer = kafka
    .producer()
    .acks(Acks::All) // wait for all in-sync replicas
    .compression(Compression::Lz4)
    .batch_size(64 * 1024)
    .linger(Duration::from_millis(5))
    .build()
    .await?;
```

### Common Consumer Options

```rust,compile
use krafka::consumer::AutoOffsetReset;
use std::time::Duration;

let consumer = kafka
    .consumer("my-group")
    .auto_offset_reset(AutoOffsetReset::Earliest) // start from the beginning
    .enable_auto_commit(true)
    .auto_commit_interval(Duration::from_secs(5))
    .build()
    .await?;
```

## Next Steps

- [Producer Guide](@/docs/producer.md) - Batching, delivery and transactions
- [Consumer Guide](@/docs/consumer.md) - Consumer groups and offset management
- [Configuration Reference](@/docs/configuration.md) - All configuration options
- [Architecture Overview](@/docs/architecture.md) - How krafka works internally
