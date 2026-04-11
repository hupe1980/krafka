---
layout: default
title: Configuration
nav_order: 7
description: "Complete configuration reference for all options"
---

# Configuration Reference

Complete reference for all Krafka configuration options.

## Producer Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrap_servers` | String | Required | Comma-separated list of host:port pairs |
| `client_id` | String | `"krafka"` | Client identifier sent with requests |
| `acks` | Acks | `Leader` | Acknowledgment level for durability |
| `compression` | Compression | `None` | Compression codec for messages |
| `batch_size` | usize | `16384` | Maximum bytes per batch (must be >= 1) |
| `linger` | Duration | `0ms` | Time to wait for batching |
| `request_timeout` | Duration | `30s` | Timeout for broker requests |
| `retries` | u32 | `3` | Number of retries on failure |
| `retry_backoff` | Duration | `100ms` | Wait between retries |
| `max_in_flight` | usize | `5` | Max concurrent in-flight requests per connection |
| `metadata_max_age` | Duration | `5m` | Max age before metadata refresh |
| `enable_idempotence` | bool | `false` | Enable exactly-once semantics |

### Acks Values

```rust
use krafka::producer::Acks;

Acks::None    // 0: Don't wait for acknowledgment
Acks::Leader  // 1: Wait for leader acknowledgment
Acks::All     // -1: Wait for all in-sync replicas
```

### Compression Values

```rust
use krafka::protocol::Compression;

Compression::None    // No compression
Compression::Gzip    // Gzip compression
Compression::Snappy  // Snappy compression
Compression::Lz4     // LZ4 compression
Compression::Zstd    // Zstandard compression
```

### Producer Builder Example

```rust
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("kafka1:9092,kafka2:9092")
    .client_id("my-producer")
    .acks(Acks::All)
    .compression(Compression::Lz4)
    .batch_size(65536)
    .linger(Duration::from_millis(5))
    .request_timeout(Duration::from_secs(30))
    .retries(5)
    .retry_backoff(Duration::from_millis(200))
    .enable_idempotence(true)
    .build()
    .await?;
```

## Consumer Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrap_servers` | String | Required | Comma-separated list of host:port pairs |
| `group_id` | String | Optional | Consumer group ID |
| `group_instance_id` | String | Optional | Static membership instance ID (KIP-345) |
| `client_id` | String | `"krafka"` | Client identifier sent with requests |
| `auto_offset_reset` | AutoOffsetReset | `Latest` | Where to start when no offset |
| `enable_auto_commit` | bool | `true` | Auto-commit offsets |
| `auto_commit_interval` | Duration | `5s` | Auto-commit interval |
| `fetch_min_bytes` | i32 | `1` | Min bytes to return from fetch |
| `fetch_max_bytes` | i32 | `52428800` | Max bytes per fetch response |
| `max_partition_fetch_bytes` | i32 | `1048576` | Max bytes per partition |
| `fetch_max_wait` | Duration | `500ms` | Max time to wait for fetch |
| `max_poll_records` | i32 | `500` | Max records per poll (strictly enforced) |
| `session_timeout` | Duration | `10s` | Group session timeout |
| `heartbeat_interval` | Duration | `3s` | Heartbeat interval |
| `max_poll_interval` | Duration | `5m` | Max time between polls (also used as the rebalance timeout) |
| `isolation_level` | IsolationLevel | `ReadUncommitted` | Transaction isolation |
| `group_protocol` | GroupProtocol | `Classic` | Group protocol: `Classic` or `Consumer` (KIP-848) |
| `request_timeout` | Duration | `30s` | Timeout for broker requests |
| `metadata_max_age` | Duration | `5m` | Max age before metadata refresh |

### AutoOffsetReset Values

```rust
use krafka::consumer::AutoOffsetReset;

AutoOffsetReset::Earliest  // Start from the earliest offset
AutoOffsetReset::Latest    // Start from the latest offset
AutoOffsetReset::None      // Error if no committed offset (strictly enforced)
```

### IsolationLevel Values

```rust
use krafka::consumer::IsolationLevel;

IsolationLevel::ReadUncommitted  // Read all messages
IsolationLevel::ReadCommitted    // Only read committed transaction messages
```

### Consumer Builder Example

```rust
use krafka::consumer::{Consumer, AutoOffsetReset, IsolationLevel};
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("kafka1:9092,kafka2:9092")
    .group_id("my-consumer-group")
    .client_id("my-consumer")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .enable_auto_commit(false)
    .fetch_min_bytes(1024)
    .fetch_max_bytes(10485760)
    .max_partition_fetch_bytes(1048576)
    .fetch_max_wait(Duration::from_millis(100))
    .max_poll_records(1000)
    .session_timeout(Duration::from_secs(30))
    .heartbeat_interval(Duration::from_secs(10))
    .isolation_level(IsolationLevel::ReadCommitted)
    .group_instance_id("instance-1")  // Static group membership
    .build()
    .await?;
```

## Admin Client Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrap_servers` | String | Required | Comma-separated list of host:port pairs |
| `client_id` | String | `"krafka-admin"` | Client identifier |
| `request_timeout` | Duration | `30s` | Timeout for admin operations |

### Admin Client Builder Example

```rust
use krafka::admin::AdminClient;
use std::time::Duration;

let admin = AdminClient::builder()
    .bootstrap_servers("kafka1:9092,kafka2:9092")
    .client_id("my-admin-client")
    .request_timeout(Duration::from_secs(60))
    .build()
    .await?;
```

## Connection Configuration

Internal connection settings (advanced):

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connect_timeout` | Duration | `10s` | TCP connection timeout |
| `request_timeout` | Duration | `30s` | Request timeout |
| `max_message_size` | usize | `10MB` | Maximum message size |
| `max_response_size` | usize | `100MB` | Maximum response size from broker |

## SOCKS5 Proxy

Route all broker connections through a SOCKS5 proxy. This is useful for
VPN/bastion setups where brokers are not directly reachable. The proxy handles
DNS resolution, so broker hostnames are sent as-is (not pre-resolved).

Enable the `socks5` feature:

```toml
krafka = { version = "0.4", features = ["socks5"] }
```

### Proxy Without Authentication

```rust
use krafka::network::ProxyConfig;

let consumer = Consumer::builder()
    .bootstrap_servers("kafka.internal:9092")
    .group_id("my-group")
    .proxy(ProxyConfig::new("socks5-proxy.corp:1080"))
    .build()
    .await?;
```

### Proxy With Authentication

```rust
use krafka::network::ProxyConfig;

let producer = Producer::builder()
    .bootstrap_servers("kafka.internal:9092")
    .proxy(ProxyConfig::with_credentials(
        "socks5-proxy.corp:1080",
        "proxy-user",
        "proxy-password",
    ))
    .build()
    .await?;
```

Proxy credentials are zeroized from memory on drop and redacted in `Debug` output.

### Proxy With TLS/SASL

Proxy and authentication can be combined — the SOCKS5 tunnel is established first,
then TLS and/or SASL negotiation proceeds over the tunneled connection:

```rust
use krafka::auth::AuthConfig;
use krafka::network::ProxyConfig;

let consumer = Consumer::builder()
    .bootstrap_servers("kafka.secure.internal:9093")
    .group_id("secure-group")
    .auth(AuthConfig::tls())
    .proxy(ProxyConfig::new("bastion:1080"))
    .build()
    .await?;
```

## Topic Configuration

For `NewTopic` when creating topics:

```rust
use krafka::admin::NewTopic;

let topic = NewTopic::new("my-topic", 12, 3)
    .with_config("cleanup.policy", "compact")
    .with_config("retention.ms", "604800000")      // 7 days
    .with_config("segment.bytes", "1073741824")    // 1GB
    .with_config("min.insync.replicas", "2");
```

### Common Topic Configs

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `cleanup.policy` | String | `delete` | `delete` or `compact` |
| `retention.ms` | Long | `-1` | Message retention time |
| `retention.bytes` | Long | `-1` | Partition size limit |
| `segment.bytes` | Int | `1GB` | Segment file size |
| `min.insync.replicas` | Int | `1` | Min replicas for write |
| `compression.type` | String | `producer` | Server compression |
| `max.message.bytes` | Int | `1MB` | Max message size |

## Environment Variables

Krafka can be configured via environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092
export KAFKA_CLIENT_ID=my-app
export KAFKA_GROUP_ID=my-group
```

Note: Environment variable support requires explicit configuration in your application.

## Performance Tuning Profiles

### High Throughput Producer

```rust
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::Leader)
    .compression(Compression::Lz4)
    .batch_size(1048576)                  // 1MB batches
    .linger(Duration::from_millis(50))    // Allow batching
    .build()
    .await?;
```

### Low Latency Producer

```rust
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::None)
    .batch_size(1)
    .linger(Duration::ZERO)
    .build()
    .await?;
```

### High Throughput Consumer

```rust
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("high-throughput")
    .fetch_max_bytes(104857600)           // 100MB
    .max_partition_fetch_bytes(10485760)  // 10MB
    .max_poll_records(10000)
    .build()
    .await?;
```

### Low Latency Consumer

```rust
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("low-latency")
    .fetch_min_bytes(1)
    .fetch_max_wait(Duration::from_millis(10))
    .max_poll_records(10)
    .build()
    .await?;
```
