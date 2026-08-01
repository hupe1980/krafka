# 🦀 Krafka

[![CI](https://github.com/hupe1980/krafka/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/krafka/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/krafka.svg)](https://crates.io/crates/krafka)
[![Documentation](https://docs.rs/krafka/badge.svg)](https://docs.rs/krafka)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://github.com/rust-lang/rust/releases/tag/1.88.0)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

A pure Rust, async-native Apache Kafka client designed for high performance, safety, and ease of use.

## ✨ Features

- 🦀 **Pure Rust by default**: No librdkafka or C dependencies; the optional `zstd` compression feature requires a C toolchain via `zstd-sys`
- ⚡ **Async-native**: Built on Tokio for true async I/O
- 🔒 **Zero unsafe**: Safe Rust by default
- 🚀 **High performance**: Zero-copy buffers, inline hot paths, efficient batching, concurrent batch flushing
- 📦 **Full protocol support**: Kafka protocol with all compression codecs
- 🤝 **Real version negotiation**: `ApiVersions` is negotiated, not pinned — KIP-511 client identity reaches the broker and KIP-584 cluster feature levels are cached per connection
- 🔄 **Incremental fetch sessions**: KIP-227 fetch sessions for bandwidth-efficient multi-partition consumers
- 🧩 **KIP-848 consumer groups**: server-side assignment with validated reconciliation — revoke-before-assign, epoch fencing, and no partition ever owned by two members at once
- 🔐 **TLS/SSL encryption**: Using rustls for secure connections
- 🔑 **SASL authentication**: PLAIN, SCRAM-SHA-256/512, OAUTHBEARER mechanisms
- 💯 **Transactions**: Exactly-once semantics with KIP-447 zombie fencing, and a state machine that refuses the KAFKA-17754 abort-after-commit-timeout hazard
- 🧭 **Truncation detection (KIP-320)**: leader epochs are sent on Fetch *and* ListOffsets *and* persisted through OffsetCommit, so the check survives restarts and rebalances
- ☁️ **Cloud-native**: First-class AWS MSK support including IAM auth
- 🛡️ **Security hardened**: Secret zeroization, constant-time auth (`subtle`), decompression bomb protection, decode loop bounds (`MAX_DECODE_ARRAY_LEN`), RFC 3986 path encoding on every outbound HTTP target
- 🔄 **Built-in retry**: Exponential backoff with metadata refresh on leader changes
- 📊 **Metrics**: Lock-free counters/gauges/latency with bounded per-topic cardinality
- 🧪 **Fuzz + property tested**: 6 cargo-fuzz targets and proptest round-trips across the protocol layer

> **Minimum Broker Version:** Krafka requires **Apache Kafka 3.9+**. Protocol versions older than the Kafka 3.9 baseline have been removed.

## 🚀 Quick Start

Add Krafka to your `Cargo.toml`:

```toml
[dependencies]
krafka = "0.14.0"
tokio = { version = "1", features = ["full"] }

# For AWS MSK IAM authentication with full SDK support:
# krafka = { version = "0.14.0", features = ["aws-msk"] }
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

    admin.create_topics(vec![topic], Duration::from_secs(30), false).await?;

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

### Authentication

Connect to secured Kafka clusters with SASL, SCRAM, OAUTHBEARER, or AWS MSK IAM — available on all client types:

```rust
use krafka::producer::Producer;
use krafka::consumer::Consumer;
use krafka::AdminClient;

// Producer with SASL/SCRAM-SHA-256
let producer = Producer::builder()
    .bootstrap_servers("broker:9093")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// Consumer with SASL/PLAIN
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9092")
    .group_id("secure-group")
    .sasl_plain("username", "password")
    .build()
    .await?;

// Producer with SASL/OAUTHBEARER
let producer = Producer::builder()
    .bootstrap_servers("broker:9093")
    .sasl_oauthbearer("your-jwt-token")
    .build()
    .await?;

// Admin with AWS MSK IAM
use krafka::auth::AuthConfig;
let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let admin = AdminClient::builder()
    .bootstrap_servers("broker:9094")
    .auth(auth)
    .build()
    .await?;
```

## 📦 Modules

| Module | Description |
|--------|-------------|
| `producer` | High-throughput message production with batching and compression |
| `consumer` | Consumer groups with rebalancing, offset management, and static membership |
| `admin` | Cluster administration (topics, groups, records, configuration, ACLs) |
| `interceptor` | Producer and consumer interceptor hooks for observability |
| `protocol` | Kafka wire protocol implementation |
| `auth` | Authentication (SASL/PLAIN, SASL/SCRAM, SASL/OAUTHBEARER, AWS MSK IAM) |

## 🗜️ Compression

Krafka supports all Kafka compression codecs, individually feature-gated:

```rust
use krafka::producer::Producer;
use krafka::protocol::Compression;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .compression(Compression::Lz4)  // Fast compression
    .build()
    .await?;
```

| Codec | Cargo Feature | Crate | Characteristics |
|-------|---------------|-------|-----------------|
| `Compression::Gzip` | `gzip` | flate2 | Best ratio, slower |
| `Compression::Snappy` | `snappy` | snap | Good balance |
| `Compression::Lz4` | `lz4` | lz4_flex | Fastest |
| `Compression::Zstd` | `zstd` | zstd | Best modern choice (requires C toolchain) |

The default `compression` feature enables the pure-Rust codecs: gzip, snappy,
and LZ4. Zstd remains available through the explicit `zstd` or
`compression-all` feature because it requires a C toolchain via `zstd-sys`.
To select only what you need:

```toml
# Option 1: enable only the codecs you need
# `default-features = false` also drops the default `ring` TLS backend, so a
# crypto backend must be named explicitly.
krafka = { version = "0.14.0", default-features = false, features = ["lz4", "snappy", "ring"] }

# Option 2: enable all compression codecs, including zstd
# krafka = { version = "0.14.0", features = ["compression-all"] }
```

### TLS crypto backend

Krafka uses `rustls` and needs exactly one crypto backend. `ring` is the
default; `rustls-aws-lc-rs` selects aws-lc-rs instead, which is the better
choice on AWS Graviton and in FIPS-oriented deployments:

```toml
krafka = { version = "0.14.0", default-features = false, features = ["rustls-aws-lc-rs", "compression"] }
```

The two backends are **additive**, not mutually exclusive — a transitive
dependency may well enable the other one, and Cargo would have no way to
resolve a conflict if they were exclusive. When both are compiled in,
aws-lc-rs deterministically wins. Krafka always selects the provider
explicitly rather than letting `rustls` infer it from crate features, so the
combination cannot produce a runtime panic. Installing a process-wide provider
with `CryptoProvider::install_default()` overrides the choice for the whole
application, Krafka included.

## 🛠️ Development

Tasks are driven by [`just`](https://just.systems). The `justfile` is the single
source of truth for what the checks are — CI calls the same recipes, so a check
cannot pass locally and fail in CI because the two drifted apart.

```bash
just              # list every recipe
just ci           # everything CI runs, except the Docker-backed suites
just ci-full      # ci + supply-chain audit + Docker integration tests
just pre-commit   # the fast subset (fmt, clippy, check)
just install-hooks  # wire pre-commit into .git/hooks
just t <pattern>  # run one test by name, with output
```

Individual recipes mirror one CI job each: `fmt-check`, `clippy`, `check`,
`api-parity`, `test`, `test-ring`, `test-cross-platform`, `minimal-features`,
`doc`, `deny`, `integration`, `msrv`.

`just api-parity` is worth calling out: it checks mechanically that every option
on an internal `*ConfigBuilder` is reachable from the public `*Builder` that
`Client::builder()` returns. An option present on one and not the other is
implemented, documentable and uncallable — which is how KIP-848 selection and
the producer's dead-letter queue both shipped unreachable.

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

## 🎯 Delivery Semantics

Pick the mode that matches what your data is worth.

| Mode | Guarantee | Cost |
|------|-----------|------|
| `acks=0` | At-most-once. No durability — the record may never reach the log. | Lowest latency |
| `acks=all` + idempotence (**default**) | At-least-once, ordered and gap-free per partition. Batches for a partition are serialised in seal order; sequence numbers are monotonic and never reused. | One round trip to the ISR |
| Transactions + `send_offsets_to_transaction` | Exactly-once across a read-process-write cycle. | Transaction coordinator round trips |

Under `acks=0`, `RecordMetadata::confirmation` reports `Unacknowledged` — do not
mistake the returned metadata for a durability guarantee.

### Exactly-once

`send_offsets_to_transaction` takes a [`ConsumerGroupMetadata`], not a bare group
ID. That metadata is what lets the group coordinator fence a **zombie**: an
instance that was partitioned away, lost its partitions to a rebalance, and came
back still holding a transaction. Without it the coordinator accepts the zombie's
commit and it overwrites the position of the member that now owns the partition.

```rust
// Re-read for every transaction. The generation changes on every rebalance,
// so a cached value stops fencing at exactly the moment it matters.
let group_metadata = consumer
    .group_metadata()
    .await
    .ok_or("consumer has not joined the group yet")?;

producer.begin_transaction()?;
producer.send("out-topic", Some(b"key"), b"value").await?;
producer.send_offsets_to_transaction(&offsets, &group_metadata).await?;
producer.commit_transaction().await?;
```

See [`examples/exactly_once.rs`](examples/exactly_once.rs) for the full
read-process-write loop.

## 🧩 Consumer Groups

Both rebalance protocols are supported: the classic JoinGroup/SyncGroup protocol
and KIP-848 (`group.protocol = consumer`).

Four assignors ship: `Range`, `RoundRobin`, `Sticky` (eager), and
`CooperativeSticky`. The default is the preference list
`[Range, CooperativeSticky]`, matching the Java client — every member advertises
both, so a group moves from eager to cooperative rebalancing in a single rolling
bounce rather than a full stop-the-world restart.

Rebalances do not wait on your poll loop. The background group task keeps
heartbeating through a rebalance and sends `JoinGroup`/`SyncGroup` itself, so a
consumer that is idle or busy between `poll()` calls does not hold the rest of
the group up. The new assignment is applied — and your rebalance listener
called — on the next `poll()`, so callbacks and record delivery stay on one
thread and an offset commit cannot race a revocation.

`max.poll.interval.ms` is still enforced: an application that genuinely stops
polling leaves the group so its partitions are reassigned promptly, rather than
holding them while a background task vouches for it. Static members
(`group.instance.id`) instead keep their assignment until the session expires,
so a restart can reclaim it.

## 📐 Scope

Krafka speaks the **client** side of the Kafka protocol: 60+ API keys covering
produce, fetch, group coordination, transactions, and administration.
Broker-internal APIs (`LeaderAndIsr`, `UpdateMetadata`, `Vote`, `FetchSnapshot`,
`BrokerHeartbeat`, …) are deliberately absent — a client does not speak them.

The schema-registry module implements the Confluent and AWS Glue **wire formats**
(magic byte, schema ID framing, caching). Bring your own Avro/Protobuf/JSON-Schema
codec; krafka does not impose one.

Tokio is the async runtime.

### Authentication

SASL/PLAIN, SASL/SCRAM-SHA-256/512 (with RFC 5929 channel binding), SASL/OAUTHBEARER
(with proactive token refresh), AWS MSK IAM, and mTLS.

GSSAPI/Kerberos is outside the scope of this client.

## 🧪 Testing Against a Fake Broker

Enable the `test-broker` feature to get an in-process Kafka broker your tests can
drive directly. Real `Producer`/`Consumer`/`AdminClient` instances connect to it
over a real TCP socket, so you exercise the actual client — no Docker, no
containers, and failure modes you cannot reproduce against a healthy cluster.

```rust
use krafka::testing::{Control, FakeBroker};
use krafka::protocol::ApiKey;
use krafka::error::ErrorCode;

let broker = FakeBroker::start().await?;

// Make the next CreateTopics land on a non-controller and assert the client
// refreshes metadata and retries instead of surfacing the error.
broker.on(ApiKey::CreateTopics, |_| Control::Error(ErrorCode::NotController));

let admin = AdminClient::builder()
    .bootstrap_servers(broker.bootstrap_servers())
    .build()
    .await?;
```

`Control` covers `Error`, `Delay`, `Disconnect`, `Silence` and pass-through, and
the cluster can be manipulated mid-test: `set_leader`, `bump_leader_epoch`,
`set_group_coordinator`, `set_txn_coordinator`, `set_controller`,
`set_broker_online`. That makes leader moves, coordinator failover, late
responses and controller churn ordinary unit tests.

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
- [Interceptors](https://hupe1980.github.io/krafka/interceptors)
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

# Exactly-once read-process-write (KIP-447 zombie fencing)
cargo run --example exactly_once

# Authentication examples (SASL, SCRAM, MSK IAM)
cargo run --example authentication
```

## 🤝 Contributing

Contributions are welcome!

## 📄 License

Licensed under either the [MIT License](LICENSE-MIT) or the [Apache License 2.0](LICENSE-APACHE), at your option.
