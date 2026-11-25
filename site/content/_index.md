+++
title = "krafka — a pure-Rust Apache Kafka client"
description = "A pure-Rust async Apache Kafka client on Tokio, with producers, transactions, consumer groups, share groups and admin APIs."
template = "index.html"

[extra]
tagline = "An async Apache Kafka client in pure Rust."
lede = "Produce, consume and manage Kafka topics on Tokio. Supports transactions, consumer groups and share groups."

[[extra.pillars]]
title = "Build requirements"
body = "The default build needs Rust and a C compiler for `ring`. It links neither librdkafka nor OpenSSL."

[[extra.pillars]]
title = "Bounded decoding"
body = "Decoders check lengths against the available bytes and return an error for malformed broker responses."

[[extra.pillars]]
title = "Consumer groups"
body = "Choose classic or KIP-848 consumer groups for partition assignment, or use share groups to acknowledge individual records."

[[extra.pillars]]
title = "Delivery guarantees"
body = "Producers are idempotent by default. A delivery timeout tells you whether the record may have been written."

[[extra.highlights]]
label = "Protocol"
value = "Kafka 4.3"
note = "checked against Kafka schemas"

[[extra.highlights]]
label = "Unsafe blocks"
value = "0"
note = "denied crate-wide"

[[extra.highlights]]
label = "System libraries to install"
value = "0"
note = "only ring compiles C by default"

[[extra.highlights]]
label = "Fake broker"
value = "built in"
note = "test your code without Docker"
+++

## Install

```sh
cargo add krafka
cargo add tokio --features full
```

## Produce

```rust,compile
use krafka::{Kafka, Record};

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let producer = kafka.producer().build().await?;

    producer.send(Record::new("orders", "hello").key("key")).await?;
    producer.close().await
}
```

## Consume

```rust,compile
use krafka::Kafka;
use krafka::consumer::AutoOffsetReset;

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let consumer = kafka
        .consumer("order-processor")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;

    consumer.subscribe(["orders"]).await?;
    while let Some(record) = consumer.recv().await? {
        println!("{}-{} @ {}", record.topic, record.partition, record.offset);
    }
    Ok(())
}
```

## Guides

- [Authentication](@/docs/authentication.md) covers TLS, SASL and credential refresh. For managed clusters, see [Cloud platforms](@/docs/cloud.md).
- [Metrics](@/docs/metrics.md) covers Prometheus output, tracing spans and broker telemetry.
- [Testing](@/docs/testing.md) shows how to inject broker failures in process.
- [Protocol support](@/docs/protocol.md) lists supported APIs, broker versions and limits.
- [Migrating from rdkafka](@/docs/migrating-from-rdkafka.md) maps configuration and client calls.
