+++
title = "Configuration"
description = "Every producer, consumer, admin and transport option, with defaults and the reason each default was chosen."
weight = 20

[extra]
slug_id = "configuration"
+++

## Producer Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrap_servers` | String | Required | Comma-separated list of host:port pairs |
| `client_id` | String | `"krafka"` | Client identifier sent with requests |
| `acks` | Acks | `All` | Acknowledgment level for durability (default changed to All for idempotent) |
| `compression` | Compression | `None` | Compression codec for messages |
| `batch_size` | usize | `16384` | Maximum bytes per batch (must be >= 1) |
| `linger` | Duration | `0ms` | How long a partial batch may *wait* for more records. `0` still batches — see [Producer › What `linger` actually controls](@/docs/producer.md) |
| `request_timeout` | Duration | `30s` | Timeout for broker requests |
| `delivery_timeout` | Duration | `120s` | Total time budget for queueing, sending, and retries |
| `retries` | u32 | `u32::MAX` | Number of retries on failure; bounded by `delivery_timeout` |
| `retry_backoff` | Duration | `100ms` | Wait between retries |
| `metadata_max_age` | Duration | `5m` | Max age before metadata refresh |
| `metadata_topic_cache_ttl` | `Option<Duration>` | `Some(5m)` | How long a topic entry may sit **idle** before a partial refresh evicts it (`metadata.max.idle.ms`). Any use — producing, resolving a leader, a partition-count lookup, naming it in a refresh — resets the timer. `None` disables eviction. |
| `buffer_memory` | usize | `32MiB` | Total bytes the producer may buffer for unsent records |
| `allow_auto_create_topics` | bool | `false` | Let the broker create a topic this producer sends to but the cluster does not have (`allow.auto.create.topics`). The broker must also have `auto.create.topics.enable=true` |
| `max_block` | Duration | `60s` | One budget for everything `send()` blocks on: fetching metadata for an unresolved topic, then waiting for `buffer_memory` (`max.block.ms`) |
| `idempotent` | bool | `true` | Enable idempotent production (KIP-679, requires acks=All) |
| `metadata_recovery_strategy` | MetadataRecoveryStrategy | `Rebootstrap` | Recovery strategy when metadata refresh fails (KIP-899, extended by KIP-1102) |
| `metadata_recovery_rebootstrap_trigger` | Duration | `5m` | Duration after which failing refreshes trigger a rebootstrap |

### Acks Values

```rust
use krafka::producer::Acks;

Acks::None    // 0: Don't wait for acknowledgment
Acks::Leader  // 1: Wait for leader acknowledgment
Acks::All     // -1: Wait for all in-sync replicas
```

### Compression Values

Each codec requires its corresponding Cargo feature (`gzip`, `snappy`, `lz4`, `zstd`).
The default `compression` feature enables gzip, snappy, and LZ4. Zstd is
explicitly opt-in through `zstd` or `compression-all` because it requires a C
toolchain via `zstd-sys`.
Use [`Compression::is_available()`] to check at runtime.

```rust
use krafka::protocol::Compression;

Compression::None    // No compression (always available)
Compression::Gzip    // Gzip compression (feature = "gzip")
Compression::Snappy  // Snappy compression (feature = "snappy")
Compression::Lz4     // LZ4 compression (feature = "lz4")
Compression::Zstd    // Zstandard compression (feature = "zstd")
```

### Producer Builder Example

```rust,compile
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("kafka1:9092,kafka2:9092")
    .client_id("my-producer")
    .compression(Compression::Lz4)
    .batch_size(65536)
    .linger(Duration::from_millis(5))
    .request_timeout(Duration::from_secs(30))
    .delivery_timeout(Duration::from_secs(120))
    .retries(5)
    .retry_backoff(Duration::from_millis(200))
    .metadata_topic_cache_ttl(Duration::from_secs(300))
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
| `fetch_max_wait` | Duration | `500ms` | How long a broker holds a fetch waiting for `fetch_min_bytes`. Independent of the `poll()` timeout: `poll()` long-polls client-side until its own deadline. |
| `max_poll_records` | i32 | `500` | Max records per poll; `-1` = unlimited; `0` and other negative values rejected |
| `session_timeout` | Duration | `45s` | Group session timeout. Matches the Java client and librdkafka defaults, raised from 10s upstream because short timeouts caused spurious rebalances under GC pauses and cloud network blips. |
| `heartbeat_interval` | Duration | `3s` | Heartbeat interval |
| `max_poll_interval` | Duration | `5m` | Max time between `poll()` calls, and the rebalance timeout. **Enforced**: exceeding it stops heartbeating, leaves the group so the partitions are reassigned, and fails the next `poll()`. |
| `isolation_level` | IsolationLevel | `ReadUncommitted` | Transaction isolation |
| `group_protocol` | GroupProtocol | `Classic` | Group protocol: `Classic` or `Consumer` (KIP-848) |
| `partition_assignment_strategies` | `Vec<PartitionAssignmentStrategy>` | `[Range, CooperativeSticky]` | Preference-ordered assignor list, advertised in JoinGroup. Matching the Java default lets a group migrate eager → cooperative in a single rolling bounce. |
| `idle_poll_backoff` | Duration | `10ms` | Backoff between polls when no partition assignment is active. Set to `Duration::ZERO` for minimum latency. |
| `request_timeout` | Duration | `30s` | Timeout for broker requests |
| `metadata_max_age` | Duration | `5m` | Max age before metadata refresh |
| `metadata_topic_cache_ttl` | `Option<Duration>` | `Some(5m)` | How long a topic entry may sit **idle** before a partial refresh evicts it (`metadata.max.idle.ms`). Any use — producing, resolving a leader, a partition-count lookup, naming it in a refresh — resets the timer. `None` disables eviction. |
| `allow_auto_create_topics` | bool | `false` | Let the broker create a subscribed or assigned topic the cluster does not have (`allow.auto.create.topics`). Java defaults this to `true`; krafka does not — see [Consumer](@/docs/consumer.md) |
| `metadata_recovery_strategy` | MetadataRecoveryStrategy | `Rebootstrap` | Recovery strategy when metadata refresh fails (KIP-899, extended by KIP-1102) |
| `metadata_recovery_rebootstrap_trigger` | Duration | `5m` | Duration after which failing refreshes trigger a rebootstrap |

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

```rust,compile
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
| `connect_timeout` | Duration | `10s` | TCP establishment budget; also the floor on `request_timeout` |
| `metadata_max_age` | Duration | `5m` | Max age before metadata refresh |
| `retries` | u32 | `5` | *Additional* attempts for a controller- or coordinator-routed request, as in the Java admin client — `retries(0)` still makes one attempt, the default makes six. Raise it on a cluster whose controller elections are slow. |
| `retry_backoff` | Duration | `100ms` | Initial backoff between those attempts. Exponential (2×) to a 10 s ceiling with 10 % jitter; `retry_backoff_policy(..)` replaces the whole policy. |
| `metadata_recovery_strategy` | MetadataRecoveryStrategy | `Rebootstrap` | Recovery strategy when metadata refresh fails (KIP-899, extended by KIP-1102) |
| `metadata_recovery_rebootstrap_trigger` | Duration | `5m` | Duration after which failing refreshes trigger a rebootstrap |

### Controller and coordinator retries

A `NOT_CONTROLLER` answer means the controller moved, and the only correct
response is to re-resolve it and try again — `create_topics` during a rolling
controller restart hits this routinely. krafka refreshes metadata, reconnects,
and retries up to `retries` times.

Both settings used to be compile-time constants: five attempts spaced by a flat
100 ms, with the documentation claiming they were `retry.backoff.ms`. Two things
were wrong with that. The budget is about a second of real time, which is short
for a KRaft election, and the flat sleep had **no jitter** — so every admin
client watching one election retried in lockstep and arrived at the newly
elected controller as a single wave.

```rust,compile
use krafka::admin::AdminClient;
use std::time::Duration;

let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    // A cluster whose elections take a few seconds.
    .retries(15)
    .retry_backoff(Duration::from_millis(250))
    .build()
    .await?;
```

### Admin Client Builder Example

```rust,compile
use krafka::admin::AdminClient;
use std::time::Duration;

let admin = AdminClient::builder()
    .bootstrap_servers("kafka1:9092,kafka2:9092")
    .client_id("my-admin-client")
    .request_timeout(Duration::from_secs(60))
    .build()
    .await?;
```

## Validating a configuration without a broker

Every client builder has two terminals over one validator:

| Terminal | Returns | Connects? |
|----------|---------|-----------|
| `build_config()` | `Result<Config>` | no |
| `build()` | `Result<Client>` | yes |

`build()` runs exactly the checks `build_config()` runs, so a config that
passes the synchronous terminal will not be rejected later for a configuration
reason. That makes settings testable at startup, in a unit test, or in a
config-linting tool — none of which want a live cluster.

```rust,compile
use krafka::producer::Producer;

// Fails immediately: the `zstd` codec is not compiled in.
let err = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .compression(krafka::protocol::Compression::Zstd)
    .build_config()
    .unwrap_err();
```

Validation also *normalises* — a compression level is clamped into the selected
codec's range, for example — so the returned config may differ from what was
set.

> **There is exactly one builder per client.** krafka used to ship a second,
> internal `*ConfigBuilder` alongside each public builder. They duplicated 72
> methods between them and their validation had diverged — the public path,
> the only one anybody used, silently skipped six checks including the
> compression-codec availability test. The duplicates are gone, and
> `tests/builder_surface.rs` asserts at compile time that every builder keeps
> both terminals.

## Transport Configuration

Socket- and pool-level settings live on a single `TransportConfig`, accepted by
**every** builder via `.transport(..)` — `Producer`, `Consumer`, `AdminClient`,
`TransactionalProducer`, `ShareConsumer` and `KrafkaClient`.

The defaults reproduce krafka's historical behaviour exactly, so adopting the
type is never itself a behaviour change.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tcp_nodelay` | bool | `true` | Disable Nagle. Kafka already batches, so Nagle only adds latency. |
| `tcp_keepalive` | `Option<Duration>` | `Some(60s)` | Keeps NAT/firewall state alive. Set below the middlebox idle timeout — this is the fix for "the consumer stops receiving after exactly N minutes". |
| `max_response_size` | usize | `100 MiB` | Largest accepted response frame. **Raise** it above the topic's `max.message.bytes`: Kafka returns at least one full record batch even when it exceeds `fetch.max.bytes`, so a larger message stalls the partition permanently. **Lower** it to bound memory. |
| `max_in_flight_requests` | usize | `10` | Per-connection in-flight cap. Real backpressure — submitters wait, they are not rejected. Worst-case memory is `max_response_size × max_in_flight_requests`. Unlike the Java client this has no bearing on ordering: krafka keeps one batch per partition on the wire regardless. |
| `socket_send_buffer` | `Option<usize>` | `None` (OS default) | `SO_SNDBUF` for every broker socket — the Java client's `send.buffer.bytes`. Raise it on a high bandwidth-delay-product link, where the socket buffer rather than the network is the throughput ceiling. |
| `socket_receive_buffer` | `Option<usize>` | `None` (OS default) | `SO_RCVBUF` — the Java client's `receive.buffer.bytes`. The fetch-side counterpart, and the one that matters for a consumer on a long link. |
| `high_priority_channel_capacity` | usize | `64` | Depth of the heartbeat/metadata command channel. |
| `normal_priority_channel_capacity` | usize | `256` | Depth of the produce/fetch command channel. |
| `max_high_priority_bypasses_per_round` | usize | `4` | How far heartbeats may cut ahead of data traffic before one normal-priority drain is forced. |
| `connection_attempt_delay` | Duration | `250ms` | Happy Eyeballs stagger (RFC 8305 §5), clamped to 100 ms – 2 s. |
| `connections_max_idle` | `Option<Duration>` | `Some(9min)` | Idle-eviction window, matching the Java client's `connections.max.idle.ms`. `None` disables eviction. |
| `max_connections` | `Option<usize>` | `None` | Cap on live connections across all brokers. Set it on clusters whose broker count can jump, to bound file descriptors. |
| `tls_reload_interval` | `Option<Duration>` | `None` | Re-read TLS certificate files from disk on a timer (KIP-1288). |

```rust,compile
use krafka::network::TransportConfig;
use krafka::consumer::Consumer;
use std::time::Duration;

let transport = TransportConfig::builder()
    .tcp_keepalive(Some(Duration::from_secs(30)))
    .max_response_size(200 * 1024 * 1024)
    .socket_receive_buffer(Some(4 * 1024 * 1024))   // long-haul fetch
    .max_connections(Some(64))
    .tls_reload_interval(Some(Duration::from_secs(3600)))
    .build()?;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .transport(transport)
    .build()
    .await?;
```

`build()` rejects values that cannot be honoured — a zero `max_in_flight_requests`
(nothing could ever be sent), a `max_response_size` below 1 KiB, or a zero
interval where `None` is the way to switch a period off.

### One configuration surface across every client

`Producer`, `Consumer`, `AdminClient`, `TransactionalProducer`, `ShareConsumer`
and `KrafkaClient` accept the same core settings — `client_id`,
`request_timeout`, `connect_timeout`, `metadata_max_age`,
`metadata_recovery_strategy`, `transport`, `auth` and the SASL convenience
helpers.

They did not always. `ShareConsumerBuilder` exposed `request_timeout` but not
`connect_timeout`, and because `build()` rejects `request_timeout <
connect_timeout`, any share consumer wanting a sub-10-second timeout failed at
construction with an error naming a value the builder could not change.
`TransactionalProducer` had no KIP-899 recovery configuration at all, and
`AdminClient` hard-coded its metadata age. `tests/builder_surface.rs` now
asserts the matrix at compile time.

Two checks keep it that way, and they answer different questions:

- **`tests/builder_surface.rs`** — *do these specific methods exist on every
  client?* Each line fails to compile if the method it names disappears. This
  is the right tool for a cross-client judgement ("both consumers should accept
  a deserializer"), which no script can infer.
- **`just config-reachability`** — *is any field unreachable?* It walks every
  config struct's fields and requires each to have a same-named builder setter
  and public accessor, or an entry in an exception list carrying a reason.

The second exists because the first is blind by construction to the defect it
was written for: a setting nobody remembered to expose is also a setting nobody
remembered to add a line for. `ShareConsumerConfig` carried five such fields —
including all four of KIP-932's fetch knobs, read when the `ShareFetch` request
was built and settable by no one — while passing `builder_surface.rs`.

### Per-client timeouts

These stay on the client builders, because they are request semantics rather
than transport tuning:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connect_timeout` | Duration | `10s` | TCP connection timeout |
| `request_timeout` | Duration | `30s` | Request timeout. Must be ≥ `connect_timeout`. |

## SOCKS5 Proxy

Route all broker connections through a SOCKS5 proxy. This is useful for
VPN/bastion setups where brokers are not directly reachable. The proxy handles
DNS resolution, so broker hostnames are sent as-is (not pre-resolved).

Enable the `socks5` feature:

```sh
cargo add krafka --features socks5
```

### Proxy Without Authentication

```rust,compile
use krafka::network::ProxyConfig;

let consumer = Consumer::builder()
    .bootstrap_servers("kafka.internal:9092")
    .group_id("my-group")
    .proxy(ProxyConfig::new("socks5-proxy.corp:1080"))
    .build()
    .await?;
```

### Proxy With Authentication

```rust,compile
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

krafka can be configured via environment variables:

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
    .linger(Duration::ZERO)    // The default: never wait, but still coalesce under load
    .build()
    .await?;
```

### High Throughput Consumer

```rust,compile
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

```rust,compile
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("low-latency")
    .fetch_min_bytes(1)
    .fetch_max_wait(Duration::from_millis(10))
    .max_poll_records(10)
    .build()
    .await?;
```
