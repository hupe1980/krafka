+++
title = "Configuration"
description = "Every connection, producer, consumer, admin and transport option, with their defaults."
weight = 20

[extra]
slug_id = "configuration"
+++

krafka has one place for each setting. **Connection settings** — how the
process reaches the cluster — live on the `Kafka` builder and apply to every
client built from that handle. **Role settings** live on the role builders:
`kafka.producer()`, `kafka.consumer(group)`, `kafka.consumer_without_group()`,
`kafka.share_consumer(group)`, and on the `AdminClient` that `kafka.admin()`
returns. Where a setting has a Java client equivalent, its description names it.

```rust,compile
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("kafka1:9092,kafka2:9092") // connection settings
    .client_id("orders-service")
    .request_timeout(Duration::from_secs(30))
    .connect()
    .await?;

let producer = kafka.producer().linger(Duration::from_millis(5)).build().await?; // role settings
let consumer = kafka.consumer("orders").build().await?;
```

No setter returns a `Result`: `connect()` and `build()` validate, and an
invalid value is a `KrafkaError::Config` naming the setting. A second identity,
other credentials or a separate connection pool is a second `Kafka` handle.

## Cargo Features

Generated from `Cargo.toml` and `xtask/kips.toml`; *Default* follows
`default = [...]`, and the gated API versions come from the `api_versions!`
table.

<!-- generated:kips:features -->
| Feature | Default | Gates API versions | Description |
|---------|---------|--------------------|-------------|
| `zstd` | no | — | Zstd compression on the producer via `zstd`, which compiles C (`zstd-sys`). Zstd decompression is pure Rust and always on. |
| `aws-msk` | no | — | AWS MSK IAM authentication with the SDK credential chain; compiles C and needs CMake (`aws-lc-sys`). |
| `oauth-oidc` | no | — | Built-in OIDC token provider for SASL/OAUTHBEARER: the `client_credentials` grant (KIP-768) and RFC 7523 client assertions (KIP-1258). Adds no cryptography dependency. |
| `native-tls-roots` | no | — | Load platform-native root certificates via `rustls-native-certs`. |
| `tls-encrypted-keys` | no | — | Passphrase-encrypted PKCS#8 client keys (`ssl.key.password`) via the RustCrypto `pkcs8` crate. |
| `unstable-protocol` | no | `ApiVersions` v5, `InitProducerId` v6 | Protocol versions Kafka marks `latestVersionUnstable`; a released broker advertises them only with `unstable.api.versions.enable=true`. No semver promise. |
| `test-broker` | no | — | In-process fake Kafka broker (`krafka::testing`) for testing your own code against a real client. Not for production builds; no semver promise. |
| `internal` | no | — | Tooling only: exposes `krafka::__private` to krafka's own benches and fuzz targets. Not a user feature; no semver promise. |
| `ring` | **yes** | — | rustls crypto backend using `ring`. |
| `rustls-aws-lc-rs` | no | — | rustls crypto backend using `aws-lc-rs`; offers post-quantum X25519MLKEM768 key exchange first. Compiles C and needs CMake (`aws-lc-sys`). |
<!-- /generated -->

## Connection Configuration (`Kafka::builder`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `bootstrap_servers` | String | Required | The argument of `Kafka::builder`: comma-separated `host:port` pairs |
| `client_id` | String | `"krafka"` | Client identifier sent with every request of every client (`client.id`) |
| `security` | `AuthConfig` | plaintext | TLS and SASL; see [Authentication](@/docs/authentication.md) |
| `request_timeout` | Duration | `30s` | How long one request may wait for its response (`request.timeout.ms`). Must be ≥ `connect_timeout` |
| `connect_timeout` | Duration | `10s` | Budget for one connection attempt to one broker: TCP, then the TLS and SASL handshakes. The floor on `request_timeout` |
| `metadata_max_age` | Duration | `5m` | Max age before a metadata refresh (`metadata.max.age.ms`) |
| `metadata_topic_cache_ttl` | `Option<Duration>` | `Some(5m)` | How long a topic entry may sit **idle** before a partial refresh evicts it (`metadata.max.idle.ms`). Any use resets the timer. `None` disables eviction |
| `allow_auto_create_topics` | bool | `false` | Let the broker create a topic a client asks about but the cluster does not have (`allow.auto.create.topics`). The broker must also have `auto.create.topics.enable=true` |
| `metadata_recovery_strategy` | MetadataRecoveryStrategy | `Rebootstrap` | What to do when every known broker is unreachable (`metadata.recovery.strategy`, KIP-899) |
| `metadata_recovery_rebootstrap_trigger` | Duration | `5m` | How long metadata refreshes may fail before a rebootstrap (`metadata.recovery.rebootstrap.trigger.ms`, KIP-1102) |
| `proxy` | `ProxyConfig` | direct | SOCKS5 route for every connection; see [SOCKS5 Proxy](#socks5-proxy) |

The transport settings below are connection settings too.

### Transport

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tcp_nodelay` | bool | `true` | Disable Nagle's algorithm; the client batches already. |
| `tcp_keepalive` | `Option<Duration>` | `Some(60s)` | Keeps NAT/firewall state alive. Set it below the middlebox's idle timeout, or an idle consumer stops receiving after that timeout. |
| `max_response_size` | usize | `100 MiB` | Largest accepted response frame. **Raise** it above the topic's `max.message.bytes`: Kafka returns at least one full record batch even when it exceeds `fetch.max.bytes`, so a larger message stalls the partition permanently. **Lower** it to bound memory. |
| `max_in_flight_requests` | usize | `10` | Per-connection in-flight cap, for every request type (Java's `max.in.flight.requests.per.connection` is a producer setting). Senders wait at the cap; they are not rejected. Worst-case memory is `max_response_size × max_in_flight_requests`. No bearing on ordering: krafka keeps one batch per partition on the wire regardless. |
| `socket_send_buffer` | `Option<usize>` | `None` (OS default) | `SO_SNDBUF` for every broker socket (Java `send.buffer.bytes`). Raise it on a high bandwidth-delay-product link. |
| `socket_receive_buffer` | `Option<usize>` | `None` (OS default) | `SO_RCVBUF` (Java `receive.buffer.bytes`); the one that matters for a consumer on a long link. |
| `connection_attempt_delay` | Duration | `250ms` | Happy Eyeballs stagger (RFC 8305 §5), clamped to 100 ms – 2 s. |
| `connections_max_idle` | `Option<Duration>` | `Some(9min)` | Idle-eviction window (Java `connections.max.idle.ms`). `None` disables eviction. |
| `max_connections` | `Option<usize>` | `None` | Cap on live connections across all brokers, coordination connections included. Replacing a dead or session-expired connection never counts as growth; a connection beyond the cap fails with a retriable `Network` error, and a coordination connection falls back to the broker's data connection. |
| `tls_reload_interval` | `Option<Duration>` | `None` | Re-read TLS certificate files from disk on a timer (KIP-1288). `kafka.refresh_tls()` reloads on demand. |

```rust,compile
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("localhost:9092")
    .tcp_keepalive(Some(Duration::from_secs(30)))
    .max_response_size(200 * 1024 * 1024)
    .socket_receive_buffer(Some(4 * 1024 * 1024)) // long-haul fetch
    .max_connections(Some(64))
    .tls_reload_interval(Some(Duration::from_secs(3600)))
    .connect()
    .await?;
```

`connect()` rejects values that cannot be honoured — a zero
`max_in_flight_requests` (nothing could ever be sent), a `max_response_size`
below 1 KiB, a zero interval where `None` is the way to switch a period off,
or a `request_timeout` below `connect_timeout`.

Every client of the handle shares the pool, and with it the proxy, the
descriptor cap and the reload interval.

## Producer Configuration (`kafka.producer()`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `acks` | Acks | `All` | Acknowledgment level for durability |
| `compression` | Compression | `None` | Compression codec |
| `compression_level` | `Option<i32>` | `None` | Codec level (Gzip 0–9, Zstd); rejected with Snappy or LZ4 |
| `topic_compression` | per topic | — | Override the codec for one topic |
| `batch_size` | usize | `16384` | Maximum bytes per batch (must be >= 1) |
| `linger` | Duration | `5ms` | How long a partial batch may *wait* for more records (KIP-1030). `0` still batches — see [Producer](@/docs/producer.md) |
| `delivery_timeout` | Duration | `120s` | Bound from a batch's creation to its records' outcome (`delivery.timeout.ms`); the only bound on retries. Must be ≥ `linger + request_timeout` |
| `retry_backoff` | Duration | `100ms` | First retry delay (`retry.backoff.ms`); doubles per retry, ±20 % jitter, capped at 1 s |
| `max_request_size` | usize | `100MiB` | Largest encoded Produce request |
| `buffer_memory` | usize | `32MiB` | Total bytes the producer may buffer for unsent records (`buffer.memory`, must be >= 1) |
| `max_block` | Duration | `60s` | One budget for everything `send()`/`enqueue()` block on: metadata for an unresolved topic, then `buffer_memory` (`max.block.ms`). Also bounds each transaction coordinator call |
| `idempotent` | bool | `true` | Idempotent production (KIP-679, requires acks=All) |
| `client_rack` | String | none | The producer's rack (`client.rack`), read by `partitioner_rack_aware` |
| `partitioner_rack_aware` | bool | `false` | Keyless records go only to partitions led in `client_rack` (KIP-1123) |
| `partitioner` | `impl Partitioner` | built-in | Custom partitioner |
| `interceptor` | `impl ProducerInterceptor` | none | Append an interceptor to the chain |
| `transaction_timeout` | Duration | `60s` | `build_transactional` only: how long the coordinator lets a transaction stay open |
| `two_phase_commit` | bool | `false` | `build_transactional` only: KIP-939 external two-phase commit |
| `metrics_push` | bool | `true` | Push metrics to brokers that subscribe to them (KIP-714, `enable.metrics.push`); see [Metrics](@/docs/metrics.md) |

### Acks Values

```rust
use krafka::producer::Acks;

Acks::None    // 0: Don't wait for acknowledgment
Acks::Leader  // 1: Wait for leader acknowledgment
Acks::All     // -1: Wait for all in-sync replicas
```

### Compression Values

Every codec decodes in every build (zstd through the pure-Rust `ruzstd`).
Gzip, Snappy and LZ4 also encode in every build; zstd *encoding* is opt-in
through the `zstd` feature, which compiles the C zstd library through `zstd-sys`.
Use `Compression::is_available()` to check whether a codec can encode;
`build()` rejects a codec that cannot.

```rust
use krafka::Compression;

Compression::None    // No compression
Compression::Gzip    // Gzip compression
Compression::Snappy  // Snappy compression
Compression::Lz4     // LZ4 compression
Compression::Zstd    // Zstandard compression (encoding needs feature = "zstd")
```

### Producer Builder Example

```rust,compile
use krafka::producer::Acks;
use krafka::{Compression, Kafka};
use std::time::Duration;

let kafka = Kafka::builder("kafka1:9092,kafka2:9092")
    .client_id("my-producer")
    .connect()
    .await?;
let producer = kafka
    .producer()
    .acks(Acks::All)
    .compression(Compression::Lz4)
    .batch_size(64 * 1024)
    .linger(Duration::from_millis(5))
    .delivery_timeout(Duration::from_secs(120))
    .retry_backoff(Duration::from_millis(200))
    .build()
    .await?;
```

## Consumer Configuration (`kafka.consumer(group)`)

`kafka.consumer(group)` joins consumer group `group`;
`kafka.consumer_without_group()` builds a consumer that assigns its own
partitions and commits nothing.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `group_instance_id` | String | none | Static membership instance ID (KIP-345) |
| `auto_offset_reset` | AutoOffsetReset | `Latest` | Where to start when no offset |
| `enable_auto_commit` | bool | `true` | Auto-commit offsets |
| `auto_commit_interval` | Duration | `5s` | Auto-commit interval |
| `fetch_min_bytes` | i32 | `1` | Min bytes to return from fetch |
| `fetch_max_bytes` | i32 | `52428800` | Max bytes per fetch response |
| `max_partition_fetch_bytes` | i32 | `1048576` | Max bytes per partition |
| `topic_fetch_max_bytes` | per topic | — | Override `max_partition_fetch_bytes` for one topic |
| `fetch_max_wait` | Duration | `500ms` | How long a broker holds a fetch waiting for `fetch_min_bytes`. Independent of the `poll()` timeout: `poll()` long-polls client-side until its own deadline. |
| `max_poll_records` | i32 | `500` | Max records per poll; `-1` = unlimited; `0` and other negative values rejected |
| `max_buffered_records` | i32 | `500` | Fetched records held before they are handed out; `0` = no cap |
| `session_timeout` | Duration | `45s` | Group session timeout (`session.timeout.ms`) |
| `heartbeat_interval` | Duration | `3s` | Heartbeat interval; must be below `session_timeout` |
| `max_poll_interval` | Duration | `5m` | Max time between `poll()` calls, and the rebalance timeout. **Enforced**: exceeding it leaves the group so the partitions are reassigned; the next `poll()` reports the loss and rejoins. |
| `isolation_level` | IsolationLevel | `ReadUncommitted` | Transaction isolation |
| `group_protocol` | GroupProtocol | `Classic` | Group protocol: `Classic` or `Consumer` (KIP-848) |
| `group_remote_assignor` | String | none | Server-side assignor for `GroupProtocol::Consumer` (`group.remote.assignor`), e.g. `uniform` or `range`; rejected with `Classic` |
| `partition_assignment_strategies` | `Vec<PartitionAssignmentStrategy>` | `[Range, CooperativeSticky]` | Preference-ordered assignor list, advertised in JoinGroup (`partition.assignment.strategy`). The Java default; it lets a group move from eager to cooperative in one rolling bounce. `partition_assignment_strategy` sets a single one. |
| `client_rack` | String | none | Rack for closest-replica fetching (KIP-392) |
| `lag_staleness_threshold` | Duration | `60s` | Watermarks older than this are reported stale by `lag()` |
| `idle_poll_backoff` | Duration | `10ms` | Backoff between polls when nothing can be fetched. Set to `Duration::ZERO` for minimum latency. |
| `max_decompressed_size` | usize | `128MiB` | Decompression-bomb limit per record batch |
| `initial_offsets` | `(TopicPartition, Offset)` pairs | — | Start offsets used before `auto_offset_reset` |
| `rebalance_listener` | `impl ConsumerRebalanceListener` | none | Assignment-change callbacks |
| `interceptor` | `impl ConsumerInterceptor` | none | Append an interceptor to the chain |
| `key_deserializer` / `value_deserializer` | `impl Deserializer` | none | Decode keys/values before they are returned |
| `metrics_push` | bool | `true` | Push metrics to brokers that subscribe to them (KIP-714, `enable.metrics.push`) |

### AutoOffsetReset Values

```rust
use krafka::consumer::AutoOffsetReset;

AutoOffsetReset::Earliest  // Start from the earliest offset
AutoOffsetReset::Latest    // Start from the latest offset
AutoOffsetReset::ByDuration(Duration::from_secs(86_400))
                           // First record of the last 24 h, else the log end (KIP-1106)
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
use krafka::Kafka;
use krafka::consumer::{AutoOffsetReset, IsolationLevel};
use std::time::Duration;

let kafka = Kafka::builder("kafka1:9092,kafka2:9092")
    .client_id("my-consumer")
    .connect()
    .await?;
let consumer = kafka
    .consumer("my-consumer-group")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .enable_auto_commit(false)
    .fetch_min_bytes(1024)
    .fetch_max_bytes(10 * 1024 * 1024)
    .max_partition_fetch_bytes(1024 * 1024)
    .fetch_max_wait(Duration::from_millis(100))
    .max_poll_records(1000)
    .session_timeout(Duration::from_secs(30))
    .heartbeat_interval(Duration::from_secs(10))
    .isolation_level(IsolationLevel::ReadCommitted)
    .group_instance_id("instance-1")
    .build()
    .await?;
```

## Share Consumer Configuration (`kafka.share_consumer(group)`)

`kafka.share_consumer(group)` builds a KIP-932 share-group member; see
[Share Consumer](@/docs/share-consumer.md).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `acknowledgement_mode` | AcknowledgementMode | `Implicit` | `Implicit` accepts a poll's records when the next `poll()`/`recv()` starts; `Explicit` settles each record with `ack`, `release` or `reject` |
| `acquire_mode` | AcquireMode | `BatchOptimized` | `RecordLimit` makes `max_poll_records` a hard limit (KIP-1206, Kafka 4.2+) |
| `max_poll_records` | i32 | `500` | Records per `poll()`, and the `MaxRecords` of every `ShareFetch`; at least 1 |
| `batch_size` | i32 | `500` | Acquisition batch-size hint, capped at `max_poll_records` |
| `fetch_min_bytes` | i32 | `1` | Bytes a broker must have before answering a `ShareFetch` |
| `fetch_max_bytes` | i32 | `52428800` | Max bytes per `ShareFetch` response |
| `fetch_max_wait` | Duration | `500ms` | How long a broker may hold a `ShareFetch`; capped by the `poll()` timeout |
| `client_rack` | String | none | The consumer's rack |
| `max_decompressed_size` | usize | `128MiB` | Decompression-bomb limit per record batch |
| `acknowledgement_commit_callback` | `Fn(&AcknowledgementCommit)` | none | Outcome of every acknowledgement request, per partition |
| `key_deserializer` / `value_deserializer` | `impl Deserializer` | none | Decode keys/values before they are returned |
| `metrics_push` | bool | `true` | Push metrics to brokers that subscribe to them (KIP-714, `enable.metrics.push`) |

## Admin Client Configuration (`kafka.admin()`)

`kafka.admin()` returns an `AdminClient` directly; it contacts no broker until
its first call. Its settings are set on the client:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `default_api_timeout` | Duration | `60s` | Bound on one admin call — lookups, attempts and backoff — when its options set no `timeout`, as Java's `default.api.timeout.ms` |
| `retry_backoff` | Duration | `100ms` | Initial backoff between attempts inside one call (`retry.backoff.ms`). Exponential (2×) to a 1 s ceiling with 20 % jitter. |
| `metrics_push` | bool | `false` | Push metrics to brokers that subscribe to them (KIP-714, `enable.metrics.push`). Off by default, as in the Java admin client |

### Retries and the call deadline

On `NOT_CONTROLLER` the client fetches metadata, finds the new controller and
sends again. Retries continue until the call's deadline: its options'
`timeout`, else `default_api_timeout`. See
[Admin Client](@/docs/admin.md) for what is retried.

```rust,compile
use std::time::Duration;

// A cluster whose elections take a while.
let admin = kafka
    .admin()
    .default_api_timeout(Duration::from_secs(120))
    .retry_backoff(Duration::from_millis(250));
```

## Validation

Every setting is checked where it takes effect: connection settings by
`Kafka::builder(..).connect()`, role settings by the role builder's `build()`
(or `build_transactional`). The error is a `KrafkaError::Config` that names
the setting:

```rust,compile
// Without the `zstd` feature this fails at build time, not on the first send.
let result = kafka
    .producer()
    .compression(krafka::Compression::Zstd)
    .build()
    .await;
```

Two checks keep the surface complete:

- **`tests/builder_surface.rs`** — *do these methods exist, with these
  shapes?* Each line fails to compile if the method it names disappears.
- **`just config-reachability`** — *is any field unreachable?* It walks every
  config struct's fields and requires each to have a same-named setter on its
  builder, or an entry in an exception list carrying a reason.

## SOCKS5 Proxy

`proxy` routes every broker connection through a SOCKS5 proxy, for brokers
reachable only through a bastion. The proxy resolves broker hostnames; the
client sends them unresolved.

### Proxy Without Authentication

```rust,compile
use krafka::{Kafka, ProxyConfig};

let kafka = Kafka::builder("kafka.internal:9092")
    .proxy(ProxyConfig::new("socks5-proxy.corp:1080"))
    .connect()
    .await?;
```

### Proxy With Authentication

```rust,compile
use krafka::{Kafka, ProxyConfig};

let kafka = Kafka::builder("kafka.internal:9092")
    .proxy(ProxyConfig::with_credentials(
        "socks5-proxy.corp:1080",
        "proxy-user",
        "proxy-password",
    ))
    .connect()
    .await?;
```

Proxy credentials are zeroized from memory on drop and redacted in `Debug` output.

### Proxy With TLS/SASL

Proxy and authentication combine — the SOCKS5 tunnel is established first,
then TLS and/or SASL negotiation proceeds over the tunneled connection:

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::{Kafka, ProxyConfig};

let kafka = Kafka::builder("kafka.secure.internal:9093")
    .security(AuthConfig::ssl(TlsConfig::new()))
    .proxy(ProxyConfig::new("bastion:1080"))
    .connect()
    .await?;
```

## Topic Configuration

For `NewTopic` when creating topics:

```rust,compile
use krafka::admin::NewTopic;

let topic = NewTopic::new("my-topic", 12, 3)?
    .with_config("cleanup.policy", "compact")
    .with_config("retention.ms", "604800000")      // 7 days
    .with_config("segment.bytes", "1073741824")    // 1GB
    .with_config("min.insync.replicas", "2");
```

### Common Topic Configs

| Config | Type | Default | Description |
|--------|------|---------|-------------|
| `cleanup.policy` | String | `delete` | `delete` or `compact` |
| `retention.ms` | Long | `604800000` (7 days) | Message retention time; `-1` keeps forever |
| `retention.bytes` | Long | `-1` | Partition size limit |
| `segment.bytes` | Int | `1GB` | Segment file size |
| `min.insync.replicas` | Int | `1` | Min replicas for write |
| `compression.type` | String | `producer` | Server compression |
| `max.message.bytes` | Int | `1048588` | Max record batch size |

## Environment Variables

`AuthConfig::from_env()` builds the security settings from the standard
`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*` and `KAFKA_SSL_*` variables; see
[Authentication](@/docs/authentication.md). Everything else is read by your
application and passed to the builders.

## Performance Tuning Profiles

### High Throughput Producer

```rust,compile
use krafka::producer::Acks;
use krafka::Compression;
use std::time::Duration;

let producer = kafka
    .producer()
    .acks(Acks::All)
    .compression(Compression::Lz4)
    .batch_size(1024 * 1024)
    .linger(Duration::from_millis(50))
    .build()
    .await?;
```

### Low Latency Producer

```rust,compile
use krafka::producer::Acks;
use std::time::Duration;

let producer = kafka
    .producer()
    .acks(Acks::Leader)
    .idempotent(false) // idempotence needs acks = All
    .linger(Duration::ZERO)
    .build()
    .await?;
```

### High Throughput Consumer

```rust,compile
let consumer = kafka
    .consumer("high-throughput")
    .fetch_max_bytes(100 * 1024 * 1024)
    .max_partition_fetch_bytes(10 * 1024 * 1024)
    .max_poll_records(10_000)
    .build()
    .await?;
```

### Low Latency Consumer

```rust,compile
use std::time::Duration;

let consumer = kafka
    .consumer("low-latency")
    .fetch_min_bytes(1)
    .fetch_max_wait(Duration::from_millis(10))
    .max_poll_records(10)
    .build()
    .await?;
```
