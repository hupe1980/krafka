+++
title = "Producer"
description = "Batching, compression, partitioning, idempotence and exactly-once transactions."
weight = 30

[extra]
slug_id = "producer"
+++

## Overview

The krafka producer is an async-native, high-performance message producer for Apache Kafka. Key features include:

- Async/await API with Tokio
- Automatic batching for throughput
- Multiple compression codecs (gzip, snappy, lz4, zstd)
- Flexible partitioning strategies
- Automatic metadata refresh
- Interceptor hooks for observability

## Basic Usage

```rust,compile
use krafka::producer::Producer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // Simple send
    producer.send("topic", None, b"value").await?;

    // Send with key (for partitioning)
    producer.send("topic", Some(b"key"), b"value").await?;

    producer.close().await;
    Ok(())
}
```

## Authentication

Connect to secured Kafka clusters using SASL or TLS:

```rust,compile
use krafka::producer::Producer;

// SASL/SCRAM-SHA-256
let producer = Producer::builder()
    .bootstrap_servers("broker:9093")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// AWS MSK IAM
use krafka::auth::AuthConfig;
let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let producer = Producer::builder()
    .bootstrap_servers("broker:9094")
    .auth(auth)
    .build()
    .await?;
```

See the [Authentication Guide](@/docs/authentication.md) for all supported mechanisms.

## Producer Configuration

### Acknowledgments

Control durability vs. latency with the `acks` setting:

```rust,compile
use krafka::producer::{Producer, Acks};

// Fire and forget (lowest latency, risk of data loss)
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::None)
    .build()
    .await?;

// Wait for leader (balanced)
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::Leader)
    .build()
    .await?;

// Wait for all in-sync replicas (highest durability)
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::All)
    .build()
    .await?;
```

### Compression

Choose the right compression codec for your workload:

```rust,compile
use krafka::producer::Producer;
use krafka::protocol::Compression;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .compression(Compression::Lz4)  // Fast compression
    .build()
    .await?;
```

| Codec | Cargo Feature | Speed | Ratio | Use Case |
|-------|---------------|-------|-------|----------|
| None | — | N/A | 1:1 | Low CPU, high bandwidth |
| Gzip | `gzip` | Slow | Best | Archival, infrequent writes |
| Snappy | `snappy` | Fast | Good | General purpose |
| LZ4 | `lz4` | Fastest | Good | High-throughput, real-time |
| Zstd | `zstd` | Medium | Best | Best balance of speed/ratio |

The default `compression` convenience feature enables the pure-Rust codecs:
gzip, snappy, and LZ4. Zstd remains available through the explicit `zstd` or
`compression-all` feature because it requires a C toolchain via `zstd-sys`.

To trim binary size further, disable defaults and select only the codecs you need:

```toml
# Option 1: enable only the codecs you need
# `default-features = false` also drops the default `ring` TLS backend, so a
# crypto backend must be named explicitly.
krafka = { version = "0.18.0", default-features = false, features = ["lz4", "ring"] }

# Option 2: enable all compression codecs, including zstd
# krafka = { version = "0.18.0", features = ["compression-all"] }
```

#### Compression level

`Gzip` and `Zstd` accept a level. `Snappy` has none in its format, and krafka
encodes LZ4 with `lz4_flex`, whose frame encoder exposes none.

```rust,compile
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .compression(Compression::Zstd)
    .compression_level(Some(1))   // favour throughput over ratio
    .build()
    .await?;
```

| Codec | Range | Default |
|-------|-------|---------|
| Gzip | 0–9 | 6 |
| Zstd | what the linked libzstd reports — negative "fast" levels through 22 | 3 |
| Snappy, LZ4 | takes no level | — |

Setting a level alongside a codec that takes none is **rejected at build
time**, as is a level outside the codec's range, and per-topic codec overrides
are validated against it too. Neither case is silently ignored: a tuning knob
that quietly does nothing is how a deployment ships believing it was tuned.

The level applies on both send paths — batched (`linger > 0`) and direct
(`linger = 0`) — and on the `TransactionalProducer`, which always batches. The
same rules are enforced by one shared validator, so a codec check cannot exist
on one producer and not the other.

Higher is not better. zstd's output size is **not monotonic** in level — the
match-finding strategy changes as levels rise, and on realistic record payloads
level 3 can be *larger* than level 1. Above roughly level 9 the CPU cost climbs
much faster than the byte savings, so on a throughput-bound producer the high
levels are usually a net loss. Measure against your own payloads.

### Batching

Batching improves throughput by combining multiple messages:

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .batch_size(65536)                      // Max bytes per batch (64KB)
    .linger(Duration::from_millis(5))       // Wait up to 5ms for more messages
    .build()
    .await?;
```

### Linger Timer

When `linger` is set (> 0ms), the producer uses a background accumulator to batch records:

- Records are accumulated per partition
- Batches are sent when either:
  - The batch reaches `batch_size` bytes, or
  - The `linger` timer expires
- This reduces the number of requests, improving throughput

For ultra-low latency (linger = 0), records are sent immediately without batching.

> **Note:** `batch_size` must be at least 1. Setting `batch_size` to 0 will cause the builder to return a configuration error.

### Request Size Cap

Use `max_request_size` when you want the producer to fail locally before sending a Produce request frame larger than your broker or network budget:

```rust,compile
use krafka::producer::Producer;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .max_request_size(1 * 1024 * 1024)      // 1 MiB encoded Produce frame cap
    .build()
    .await?;
```

The producer encodes the final request using the negotiated Produce API version and rejects frames that exceed `max_request_size` before any broker I/O. The default is 100 MiB, matching Kafka's protocol request-size ceiling. Leave some headroom between `batch_size` and `max_request_size` for request headers and topic names; the builder rejects configurations where `batch_size > max_request_size`. The same knob is available on `TransactionalProducer::builder()`.

```rust,compile
// High-throughput configuration
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .batch_size(131072)                     // 128KB batches
    .linger(Duration::from_millis(10))      // Wait up to 10ms
    .compression(Compression::Lz4)          // Fast compression
    .build()
    .await?;

// Low-latency configuration
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .linger(Duration::from_millis(0))       // No batching, send immediately
    .build()
    .await?;
```

### Memory Backpressure

The producer limits memory usage to prevent unbounded growth under high load:

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .buffer_memory(64 * 1024 * 1024)        // 64MB buffer limit
    .max_block(Duration::from_secs(30))     // Wait up to 30s when buffer full
    .build()
    .await?;
```

| Option | Default | Description |
|--------|---------|-------------|
| `buffer_memory` | 32 MB | Maximum total memory for buffering records |
| `max_block` | 60s | Maximum time to block when buffer is full |

The `buffer_memory` and `max_block` settings apply to both batching (`linger > 0`) and direct-send mode (`linger = 0`). Once a record is admitted, it holds a share of the producer memory budget until it is acknowledged or fails, so direct sends and accumulator batches obey the same backpressure contract. If memory is unavailable, `send()` blocks the caller for up to `max_block` before returning an error. This matches the Kafka Java client's `max.block.ms` behavior and prevents both OOM conditions and unnecessary record loss under bursty load.

## Flushing

Call `flush()` whenever you need a durability barrier over records that have already been handed to the producer. This now covers both linger-based batching and direct-send mode (`linger = 0`):

```rust,compile
// Send multiple records
for i in 0..100 {
    producer.send("topic", Some(format!("key-{}", i).as_bytes()), b"value").await?;
}

// Ensure all records are sent before closing
producer.flush().await?;
producer.close().await;
```

## Partitioning

### Default Partitioner

The default partitioner uses murmur2 hashing (Java-compatible) for keyed messages and round-robin for null keys:

```rust,compile
// Messages with the same key go to the same partition
producer.send("topic", Some(b"user-123"), b"event1").await?;
producer.send("topic", Some(b"user-123"), b"event2").await?;  // Same partition

// Messages without keys are distributed round-robin
producer.send("topic", None, b"event").await?;
```

### Custom Partitioners

krafka provides several built-in partitioners:

```rust,compile
use krafka::producer::{
    DefaultPartitioner,
    RoundRobinPartitioner,
    StickyPartitioner,
    HashPartitioner,
};

// Round-robin: ignores keys, distributes evenly
let partitioner = RoundRobinPartitioner::new();

// Sticky: sticks to one partition, auto-advances after batch_threshold records (default 100)
let partitioner = StickyPartitioner::new();

// Sticky with custom batch threshold
let partitioner = StickyPartitioner::with_batch_threshold(500);

// Hash: uses Rust's default hasher instead of murmur2
let partitioner = HashPartitioner::new();
```

### Implementing Custom Partitioners

```rust,compile
use krafka::producer::Partitioner;
use krafka::PartitionId;

struct RegionPartitioner {
    region_to_partition: std::collections::HashMap<String, PartitionId>,
}

impl Partitioner for RegionPartitioner {
    fn partition(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        partition_count: usize,
    ) -> PartitionId {
        if let Some(key) = key {
            if let Ok(region) = std::str::from_utf8(key) {
                if let Some(&partition) = self.region_to_partition.get(region) {
                    return partition % partition_count as i32;
                }
            }
        }
        // Fallback to first partition
        0
    }
}
```

## Metadata Topic Cache TTL

During a partial metadata refresh for produced topics, krafka caches topic metadata between refreshes. By default, a topic entry is evicted after **5 minutes** without a successful refresh, matching Java's `metadata.max.idle.ms`, so topic churn does not grow the cache indefinitely.

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .metadata_topic_cache_ttl(Duration::from_secs(600))
    .build()
    .await?;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .disable_metadata_topic_cache_ttl()
    .build()
    .await?;
```

A full metadata refresh still replaces the cache unconditionally.

## Error Handling

### Record Validation

Before sending, each `ProducerRecord` is validated against Kafka wire-format limits:

- **Topic name**: max 32,767 bytes (i16 limit)
- **Key**: max 2,147,483,647 bytes (i32 limit)
- **Value**: max 2,147,483,647 bytes (i32 limit)
- **Header keys**: max 2,147,483,647 bytes (i32 limit)
- **Header values**: max 2,147,483,647 bytes (i32 limit)

Oversized data returns a descriptive `KrafkaError::protocol` error instead of panicking.

### Built-in Retry

The producer automatically retries transient failures (e.g., `NotLeaderForPartition`, network timeouts) using the configured retry policy. On each retriable error, the producer refreshes metadata to discover the new partition leader before retrying with exponential backoff.

Configure retries via the builder:

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .retries(5)                                      // Max retry attempts; defaults to u32::MAX
    .retry_backoff(Duration::from_millis(100))        // Initial backoff
    .build()
    .await?;

// send() automatically retries on transient failures
producer.send("topic", None, b"value").await?;
```

### Delivery Timeout

The `delivery_timeout` setting (analogous to the Java client's `delivery.timeout.ms`) caps the total time from when a record enters the producer to when it must be acknowledged. This includes time spent in the accumulator's linger window, backpressure waits, and all retry attempts.

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .delivery_timeout(Duration::from_secs(120))  // Total delivery budget
    .linger(Duration::from_millis(5))             // Batching window
    .retries(u32::MAX)                            // Retry until timeout
    .build()
    .await?;
```

The producer defaults to `delivery_timeout = 120s` and `retries = u32::MAX`, so transient failures are retried until the delivery budget is exhausted. Backoff durations are clamped to the remaining budget so the producer does not overshoot. If the budget is exhausted, the send fails immediately regardless of the remaining retry count.

> **Note:** By default `linger` is `0` (no batching delay), so the delivery timeout is nearly equivalent to network time + retry time. With `linger > 0`, add the maximum linger window to your delivery timeout budget.

### Manual Retry

For additional retry control beyond the built-in behavior, handle errors explicitly:

```rust,compile
use krafka::producer::Producer;
use krafka::error::{KrafkaError, Result};

async fn send_with_retry(
    producer: &Producer,
    topic: &str,
    key: Option<&[u8]>,
    value: &[u8],
    max_retries: u32,
) -> Result<()> {
    let mut attempts = 0;
    
    loop {
        match producer.send(topic, key, value).await {
            Ok(metadata) => {
                println!("Sent to {}:{}", metadata.partition, metadata.offset);
                return Ok(());
            }
            Err(e) if e.is_retriable() && attempts < max_retries => {
                println!("Send failed (attempt {}): {}", attempts + 1, e);
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(100 * attempts as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Using RetryPolicy

For more sophisticated retry handling with exponential backoff:

```rust,compile
use krafka::producer::{Producer, RetryPolicy, RetryContext};
use krafka::error::Result;

async fn send_with_policy(
    producer: &Producer,
    topic: &str,
    value: &[u8],
) -> Result<()> {
    let policy = RetryPolicy::new()
        .with_max_retries(5)
        .with_initial_backoff(std::time::Duration::from_millis(100))
        .with_max_backoff(std::time::Duration::from_secs(10))
        .with_backoff_multiplier(2.0)
        .with_jitter_factor(0.1);  // Add 10% jitter to prevent thundering herd
    
    let mut ctx = RetryContext::new(policy, "send_message");
    
    loop {
        match producer.send(topic, None, value).await {
            Ok(metadata) => {
                ctx.record_success();
                return Ok(());
            }
            Err(e) => {
                if let Some(backoff) = ctx.record_failure(&e) {
                    ctx.wait(backoff).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
}
```

## Performance Tips

### High Throughput

For maximum throughput:

```rust,compile
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::Leader)                     // Don't wait for all replicas
    .compression(Compression::Lz4)           // Fast compression
    .batch_size(1048576)                     // 1MB batches
    .linger(Duration::from_millis(10))       // Allow batching
    .build()
    .await?;
```

### Low Latency

For minimum latency:

```rust,compile
use krafka::producer::{Producer, Acks};
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::None)                        // Don't wait for acks
    .batch_size(1)                           // No batching
    .linger(Duration::ZERO)                  // Send immediately
    .build()
    .await?;
```

> **`acks=0` also gives up quota feedback.** The broker sends no response, so
> there is no `throttle_time_ms` to read (KIP-219) and a producer sending
> *only* `acks=0` traffic never learns it is being throttled. A throttle
> learned from any other API on the same connection is still honoured, but a
> pure `acks=0` client keeps writing at full rate until the broker mutes the
> channel itself. Combined with the loss of delivery confirmation, `acks=0`
> gives up more than durability alone — prefer `acks=1` unless you have
> measured that the difference matters.

### Durability

For maximum durability:

```rust,compile
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .retries(10)                             // Retry on failure
    .build()
    .await?;
```

> **Idempotent by default (KIP-679):** Since Kafka 3.0, idempotent production is the default.
> The regular `Producer` now obtains a Producer ID via `InitProducerId` at startup,
> tracks sequence numbers per partition, and de-duplicates retries automatically.
> `acks = All` is required when idempotent is enabled. If `max_in_flight` is set
> above 5, it is **automatically capped to 5** (matching Java client and librdkafka
> behaviour), with an `info!` log so operators can see the adjustment.
> The `InitProducerId` call retries on retriable errors (e.g. `CoordinatorLoadInProgress`)
> with exponential backoff, rotating through available brokers on each attempt.
>
> **Error handling:**
> - `OutOfOrderSequenceNumber` triggers a sequence reset and batch rebuild before retrying.
> - `DuplicateSequenceNumber` is treated as success (broker already committed the batch;
>   idempotent dedup worked). The returned offset is `-1` since the broker does not echo
>   the original offset for duplicates.
> - Multi-record batches acknowledge the *last* sequence (`base + count − 1`), matching
>   the Kafka Java client's `ProducerBatch.lastSequence()` semantics.
>
> For cross-session exactly-once semantics (transactions), use `TransactionalProducer`.

### Concurrency Control

The producer enforces `max_in_flight` to limit concurrent in-flight produce requests.
This is critical for ordering guarantees and is implemented via a semaphore:

```rust,compile
use krafka::producer::{Producer, Acks};

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::All)
    .max_in_flight(1)    // Strict ordering (at most 1 concurrent send)
    .build()
    .await?;
```

## Graceful Shutdown

Always close producers properly to flush pending messages. The `close()` method is a barrier over all started sends, not just batches still resident in the accumulator. It blocks new sends, waits for buffered and already-in-flight work to finish, then tears down connections. Calling `close()` more than once is a no-op:

```rust
use krafka::producer::Producer;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .build()
    .await?;

// ... send messages ...

// Flush and close — waits for all in-flight batches to complete
producer.flush().await?;
producer.close().await;
```

If you need a bounded shutdown window, use `close_with_timeout()` instead. On timeout, krafka tears down the connection pool and returns a timeout error, causing any remaining in-flight work to fail fast instead of hanging shutdown indefinitely:

```rust,compile
use std::time::Duration;

producer.close_with_timeout(Duration::from_secs(10)).await?;
```

## Transactional Producer

For exactly-once semantics across multiple partitions and topics, use the `TransactionalProducer`.
This is the **recommended** approach for idempotent and exactly-once production.

The transactional producer:
- Automatically obtains a Producer ID (PID) and epoch from the broker via `InitProducerId`
- Sets `producer_id`, `producer_epoch`, and `base_sequence` on every record batch
- Marks batches as transactional (attribute bit 0x10)
- Tracks sequence numbers per topic-partition for idempotent delivery

### Basic Usage

```rust,compile
use krafka::producer::TransactionalProducer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Create transactional producer with unique ID
    let producer = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("my-unique-transaction-id")
        .build()
        .await?;

    // Initialize transactions (once per producer)
    producer.init_transactions().await?;

    // Start transaction
    producer.begin_transaction()?;

    // Send messages atomically
    producer.send("topic-a", Some(b"key1"), b"value1").await?;
    producer.send("topic-b", Some(b"key2"), b"value2").await?;

    // Commit transaction (all or nothing)
    producer.commit_transaction().await?;

    Ok(())
}
```

### Configuration

`TransactionalProducerBuilder` mirrors `ProducerBuilder` setter for setter:
compression and compression levels, delivery timeout, interceptors, a
dead-letter queue, a state store, `with_client`, the metadata cache TTLs, and
the synchronous `build_config()` terminal. `tests/builder_surface.rs` asserts
that at compile time, so the two builders cannot drift apart.

```rust,compile
use krafka::producer::TransactionalProducer;
use krafka::protocol::Compression;
use std::time::Duration;

let producer = TransactionalProducer::builder()
    .bootstrap_servers("localhost:9092")
    .transactional_id("order-processor-1")
    .client_id("my-app")
    .transaction_timeout(Duration::from_secs(60))          // coordinator's deadline
    .request_timeout(Duration::from_secs(30))
    .delivery_timeout(Duration::from_secs(45))             // bound on one batch in flight
    .compression(Compression::Zstd)
    .compression_level(Some(1))
    .build()
    .await?;
```

Two setters are **deliberately absent**, because the transactional protocol
fixes both:

| Absent setter | Why |
|---|---|
| `acks` | Fixed to `Acks::All`. The coordinator can only guarantee atomicity over fully replicated writes, so a weaker setting would silently break the guarantee the type exists to provide. |
| `idempotent` | Always on. A transactional producer *is* an idempotent producer with a stable `transactional.id`; there is nothing to disable. |

#### Delivery timeout

`delivery_timeout` bounds how long one batch may spend in flight, including
batching, retries and backoff. It matters more here than on the plain producer:
a batch that keeps retrying holds the transaction open, and an open transaction
blocks every `read_committed` consumer at its first offset.

Keep it at or below `transaction_timeout` — the coordinator aborts at that point
regardless. `build()` and `build_config()` warn when the two disagree.

#### Validating without a broker

`build_config()` runs exactly the checks `build()` runs and returns the
validated `TransactionalProducerConfig` without connecting — for a
`validate-config` subcommand, a startup check, or a unit test:

```rust,compile
let config = TransactionalProducer::builder()
    .bootstrap_servers("localhost:9092")
    .transactional_id("order-processor-1")
    .compression(Compression::Zstd)
    .compression_level(Some(1))
    .build_config()?;      // no cluster required

assert_eq!(config.compression_level(), Some(1));
```

#### Flushing

`flush()` dispatches every buffered record and waits for the in-flight sends to
complete. You do **not** need it before `commit_transaction()`, which flushes
first and must — a commit marker written while records were still buffered would
leave them outside the transaction they were sent in.

It is there for two other reasons: forcing buffered records onto the wire
mid-transaction so their failures surface with the record's context rather than
at commit time, and writing code generic over "a producer" without special-casing
which of the two you hold.

Unlike `Producer::flush`, it does not make the records visible to a
`read_committed` consumer — only `commit_transaction()` does.

### Authentication

Connect a transactional producer to secured Kafka clusters:

```rust
use krafka::producer::TransactionalProducer;

// SASL/SCRAM-SHA-256 over cleartext (development only)
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9093")
    .transactional_id("my-txn-id")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// SASL_SSL + SCRAM-SHA-512 — what a managed cluster almost always wants
use krafka::auth::{AuthConfig, TlsConfig};
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9093")
    .transactional_id("my-txn-id")
    .auth(AuthConfig::sasl_scram_sha512_ssl("username", "password", TlsConfig::new()))
    .build()
    .await?;

// Or use AuthConfig for advanced auth (e.g., AWS MSK IAM)
use krafka::auth::AuthConfig;
let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9094")
    .transactional_id("my-txn-id")
    .auth(auth)
    .build()
    .await?;
```

See the [Authentication Guide](@/docs/authentication.md) for all supported mechanisms.

### Transaction Lifecycle

1. **Initialize**: Call `init_transactions()` once when producer starts
2. **Begin**: Call `begin_transaction()` to start a new transaction
3. **Send**: Send messages with `send()` or `send_record()`
4. **End**: Call `commit_transaction()` or `abort_transaction()`
5. **Close**: Call `close()` when done — aborts any active transaction and cleans up resources

```rust
// Error handling with abort
producer.begin_transaction()?;

match do_work(&producer).await {
    Ok(()) => producer.commit_transaction().await?,
    Err(e) => {
        producer.abort_transaction().await?;
        return Err(e);
    }
}

// When finished with the producer, always close it
producer.close().await;
```

> **`send_offsets_to_transaction` must complete inside the transaction.** A
> commit waits for an offset commit that is already in flight, so the `EndTxn`
> marker is never written around one. An offset commit started *after* the
> commit has begun is refused with the same `Committing` error as a send. Both
> orderings are safe; there is no arrangement in which the offsets land outside
> the transaction.

> **A commit closes the transaction before it drains it.** The moment
> `commit_transaction()` is entered it transitions out of `InTransaction`, so
> any concurrent `send()` from another task is refused with an
> `InvalidState` error naming the `Committing` state. This is deliberate: a
> record admitted after the drain had begun would still be buffered when
> `EndTxn` went out, and would land in the *next* transaction — vanishing if
> that one aborted. If you share a `TransactionalProducer` across tasks, treat
> that error as "the transaction closed under me" and retry the record in the
> next one.

> **Never abort after a commit times out.** If `commit_transaction()` fails with
> a timeout or a connection loss, the coordinator may already have committed —
> the response was simply lost. Aborting then is the
> [KAFKA-17754](https://issues.apache.org/jira/browse/KAFKA-17754) trigger: the
> delayed `EndTxn` can be applied to a *later* transaction and tear it. The Java
> client's documentation recommends aborting in this case; that advice predates
> KAFKA-17754 and krafka deliberately does not follow it.
>
> krafka enforces this rather than relying on you to remember it. A commit whose
> outcome is unknown moves the producer to `TransactionState::CommitIndeterminate`,
> from which:
>
> - `abort_transaction()` returns an error explaining why, instead of performing
>   an abort that could silently corrupt data;
> - `close()` leaves the transaction alone rather than auto-aborting it, and logs
>   that it did so;
> - `commit_transaction()` may be retried — `EndTxn` is idempotent for the same
>   producer id and epoch, so a duplicate commit either lands or is recognised by
>   the coordinator as the one it already applied.
>
> If you cannot retry, drop the producer. The coordinator resolves the
> transaction on its own via `transaction.timeout.ms`.
>
> A commit that fails with a *broker error code* is different: the coordinator
> answered and declined, so the transaction is definitively still open and the
> producer returns to `InTransaction`, where aborting is safe.

### Graceful Shutdown (Transactional)

Always close transactional producers properly. The `close()` method:
- Blocks new sends and waits for already-started transactional produce requests to finish
- Aborts any active transaction to avoid dangling open transactions on the broker
- Transitions the producer to `FatalError` state, preventing further use
- Closes the underlying connection pool
- Is idempotent — calling it more than once is a no-op

```rust,compile
// Graceful shutdown
producer.close().await;
// Producer is no longer usable after close()
```

For bounded shutdown windows, `close_with_timeout()` provides the same semantics with an explicit deadline:

```rust,compile
use std::time::Duration;

producer.close_with_timeout(Duration::from_secs(10)).await?;
```

### Built-in Retry Logic

The transactional producer automatically retries sends on transient failures:
- Uses the shared `RetryPolicy` (default: 3 retries, exponential backoff with jitter)
- Metadata is refreshed on transient errors before retrying
- `OutOfOrderSequenceNumber` errors trigger a sequence number reset and batch rebuild with a fresh sequence before retrying
- Sequence numbers and the batch are allocated once and reused across normal retries to maintain idempotent semantics
- Non-retriable errors (auth failures, invalid topics) fail immediately

### Coordinator Re-discovery

All coordinator RPCs (`InitProducerId`, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `EndTxn`)
automatically handle coordinator failover:

- On `NotCoordinator`, `CoordinatorNotAvailable`, or `CoordinatorLoadInProgress` the cached
  coordinator is invalidated and a fresh `FindCoordinator` is issued before retrying.
- Network and timeout errors to the coordinator trigger the same invalidation + re-discovery flow.
- The retry uses the producer's `RetryPolicy` for exponential backoff between attempts.
- Fatal errors (`TransactionCoordinatorFenced`, `ProducerFenced`, `InvalidProducerEpoch`,
  `InvalidTxnState`) are never retried.
- If no coordinator is cached (e.g. after invalidation), `coordinator_connection()` auto-discovers
  one transparently before returning the connection.

### KIP-890 Epoch Bumping (Kafka 3.7+)

Kafka 3.7+ brokers implement **KIP-890 epoch bumping**: after every successful `EndTxn` (commit
or abort) the broker increments the producer epoch and returns the new `ProducerId` and
`ProducerEpoch` in the `EndTxn` v4+ response. krafka reads these fields and automatically applies
them to the local identity, so subsequent `AddPartitionsToTxn` requests use the correct epoch.

For brokers that do not support `EndTxn` v4+ (Kafka < 3.7), the response omits these fields and
krafka continues with the unchanged epoch — the pre-KIP-890 protocol is used transparently.

### Persisting Producer State

`ProducerStateStore` is a hook for saving and restoring the producer's identity
— its producer ID, epoch and per-partition sequence numbers — across restarts.
Attach one with `state_store()` on either producer builder:

```rust,compile
use krafka::producer::{ProducerIdentitySnapshot, ProducerStateStore, TransactionalProducer};

struct FileStateStore {
    path: std::path::PathBuf,
}

impl ProducerStateStore for FileStateStore {
    async fn load(&self) -> krafka::Result<Option<ProducerIdentitySnapshot>> {
        // Read and deserialise the snapshot; `Ok(None)` on first run.
        Ok(None)
    }

    async fn store(&self, snapshot: &ProducerIdentitySnapshot) -> krafka::Result<()> {
        // Persist it. Errors are logged at WARN and never fail the send.
        Ok(())
    }
}

let producer = TransactionalProducer::builder()
    .bootstrap_servers("localhost:9092")
    .transactional_id("orders-processor-1")
    .state_store(FileStateStore { path: "/var/lib/app/producer.json".into() })
    .build()
    .await?;
```

`load()` is called once during `build()`; `store()` is called after each
successful batch acknowledgement.

> **A restored snapshot is only honoured when it is safe to honour.** krafka
> applies it only if the stored `producer_id` **and** `producer_epoch` match
> what the broker returned from `InitProducerId`. For a plain idempotent
> producer that can never happen — the broker issues a fresh PID with epoch 0
> on every call — so restored sequences are ignored and the store is useful
> only for observability. It carries real weight for a **transactional**
> producer with a stable `transactional.id`, where the broker may hand back the
> same PID with a bumped epoch.

### Timestamps

Both `Producer` and `TransactionalProducer` propagate the `timestamp` field from `ProducerRecord` to the Kafka record batch. If set, the timestamp is used as the `base_timestamp` of the record batch:

```rust,compile
use krafka::producer::ProducerRecord;

let mut record = ProducerRecord::new("my-topic", b"value".to_vec());
record.timestamp = Some(1700000000000); // epoch millis
producer.send_record(record).await?;
```

> **Note:** If `timestamp` is not set, the broker defaults apply (typically `LogAppendTime` or `CreateTime` depending on topic configuration).

### Consume-Transform-Produce (Exactly-Once)

For read-process-write patterns with exactly-once guarantees:

```rust
use krafka::producer::TransactionalProducer;
use std::collections::HashMap;

// Commit consumer offsets atomically with produce
producer.begin_transaction()?;

// Process records and produce output
for record in consumer_records {
    let output = transform(&record)?;
    producer.send("output-topic", record.key, &output).await?;
}

// Commit offsets as part of the transaction.
//
// KIP-447: pass the consumer's live group metadata so the group coordinator
// can fence a zombie committer. Re-read it every transaction — the generation
// changes on every rebalance, and a cached value defeats the fencing.
let offsets = [TopicPartitionOffset::new(topic, partition, next_offset)];
let group_metadata = consumer.group_metadata().await?;
producer.send_offsets_to_transaction(&offsets, &group_metadata).await?;

// Atomic commit of messages and offsets
producer.commit_transaction().await?;
```

### Transaction States

The producer maintains a state machine with atomic CAS (compare-and-swap) transitions for thread safety:

| State | Description |
|-------|-------------|
| `Uninitialized` | Producer created, `init_transactions()` not called |
| `Ready` | Ready to begin a new transaction |
| `InTransaction` | Transaction in progress |
| `Committing` | Transaction being committed |
| `Aborting` | Transaction being aborted |
| `FatalError` | Unrecoverable error, producer must be recreated |

> **Note:** State transitions are protected by atomic compare-and-swap operations, preventing race conditions when multiple tasks interact with the transactional producer concurrently.

## Producer Interceptors

Interceptors allow you to observe and modify records before they are sent, and
observe the acknowledgement (or error) after a send completes.
See the [Interceptors Guide](@/docs/interceptors.md) for full details.

```rust
use krafka::interceptor::{InterceptorResult, ProducerInterceptor};
use krafka::producer::{Producer, ProducerRecord, RecordMetadata};
use krafka::error::KrafkaError;
use std::sync::Arc;

#[derive(Debug)]
struct AuditInterceptor;

impl ProducerInterceptor for AuditInterceptor {
    fn on_send(&self, record: &mut ProducerRecord) -> InterceptorResult {
        // Add a tracing header to every record
        record.headers.push(("x-trace-id".to_string(), b"abc123".to_vec()));
        Ok(())
    }

    fn on_acknowledgement(&self, metadata: &RecordMetadata, error: Option<&KrafkaError>) -> InterceptorResult {
        if let Some(err) = error {
            eprintln!("Send failed: {}", err);
        } else {
            println!("Sent to {}:{}", metadata.topic, metadata.partition);
        }
        Ok(())
    }
}

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .interceptor(Arc::new(AuditInterceptor))
    .build()
    .await?;
```

## Next Steps

- [Dead Letter Queue](@/docs/errors.md#dead-letter-queue) - Route permanently-failed records to an error topic
- [Interceptors Guide](@/docs/interceptors.md) - Producer and consumer interceptor hooks
- [Consumer Guide](@/docs/consumer.md) - Learn about consuming messages
- [Configuration Reference](@/docs/configuration.md) - All producer options
- [Architecture Overview](@/docs/architecture.md) - How the producer works internally
