---
layout: home
title: Home
nav_order: 1
description: "Krafka - A pure Rust, async-native Apache Kafka client"
permalink: /
---

# 🦀 Krafka
{: .fs-9 }

A pure Rust, async-native Apache Kafka client designed for high performance, safety, and ease of use.
{: .fs-6 .fw-300 }

[Get Started]({{ site.baseurl }}/getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/hupe1980/krafka){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## ✨ Features

| Feature | Description |
|:--------|:------------|
| 🦀 **Pure Rust** | No librdkafka or C dependencies |
| ⚡ **Async-native** | Built on Tokio for true async I/O |
| 🔒 **Zero unsafe** | Safe Rust by default |
| 🚀 **High performance** | Zero-copy buffers, inline hot paths, efficient batching |
| 📦 **Full protocol** | Kafka protocol with all compression codecs |
| 🔐 **TLS/SSL** | Using rustls for secure connections |
| 🔑 **SASL** | PLAIN, SCRAM-SHA-256/512 mechanisms |
| 💯 **Transactions** | Exactly-once semantics |
| ☁️ **Cloud-native** | AWS MSK support with IAM auth |
| 🛡️ **Security hardened** | Secret zeroization, constant-time auth, decompression limits |
| 🌐 **SOCKS5 proxy** | Route connections through SOCKS5 proxies (VPN/bastion) |

---

## 🚀 Quick Start

Add Krafka to your `Cargo.toml`:

```toml
[dependencies]
krafka = "0.8.0"
tokio = { version = "1", features = ["full"] }
```

### Producer Example

```rust
use krafka::producer::Producer;

#[tokio::main]
async fn main() -> krafka::error::Result<()> {
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    producer.send("my-topic", Some(b"key"), b"Hello, Kafka!").await?;
    producer.close().await;
    Ok(())
}
```

### Consumer Example

```rust
use krafka::consumer::{Consumer, AutoOffsetReset};
use std::time::Duration;

#[tokio::main]
async fn main() -> krafka::error::Result<()> {
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("my-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;

    consumer.subscribe(&["my-topic"]).await?;

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in records {
            println!("{:?}", record);
        }
    }
}
```

---

## 🏗️ Architecture

Krafka is designed with performance and safety as primary goals:

- **Zero-copy buffers** using the `bytes` crate
- **Inline hot paths** for minimal overhead
- **Lazy deserialization** for on-demand record parsing
- **Priority request channels** prevent group ejection during backpressure
- **Multi-connection bundles** for extreme high-throughput scenarios
- **Connection pooling** for efficient broker communication
- **murmur2 hashing** matching Java client for consistent partitioning

---

## 📄 License

Krafka is licensed under the [MIT License](https://github.com/hupe1980/krafka/blob/main/LICENSE).
