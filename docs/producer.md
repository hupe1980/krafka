---
layout: default
title: Producer
nav_order: 3
description: "High-performance message production with batching and compression"
---

# Producer Guide

This guide covers advanced producer usage, including batching, partitioning, compression, and error handling.

## Overview

The Krafka producer is an async-native, high-performance message producer for Apache Kafka. Key features include:

- Async/await API with Tokio
- Automatic batching for throughput
- Multiple compression codecs (gzip, snappy, lz4, zstd)
- Flexible partitioning strategies
- Automatic metadata refresh
- Interceptor hooks for observability

## Basic Usage

```rust
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

```rust
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

See the [Authentication Guide](authentication.md) for all supported mechanisms.

## Producer Configuration

### Acknowledgments

Control durability vs. latency with the `acks` setting:

```rust
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

```rust
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
krafka = { version = "0.12.0", default-features = false, features = ["lz4"] }

# Option 2: enable all compression codecs, including zstd
# krafka = { version = "0.12.0", features = ["compression-all"] }
```

### Batching

Batching improves throughput by combining multiple messages:

```rust
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

```rust
use krafka::producer::Producer;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .max_request_size(1 * 1024 * 1024)      // 1 MiB encoded Produce frame cap
    .build()
    .await?;
```

The producer encodes the final request using the negotiated Produce API version and rejects frames that exceed `max_request_size` before any broker I/O. The default is 100 MiB, matching Kafka's protocol request-size ceiling. Leave some headroom between `batch_size` and `max_request_size` for request headers and topic names; the builder rejects configurations where `batch_size > max_request_size`. The same knob is available on `TransactionalProducer::builder()`.

```rust
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

```rust
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

```rust
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

```rust
// Messages with the same key go to the same partition
producer.send("topic", Some(b"user-123"), b"event1").await?;
producer.send("topic", Some(b"user-123"), b"event2").await?;  // Same partition

// Messages without keys are distributed round-robin
producer.send("topic", None, b"event").await?;
```

### Custom Partitioners

Krafka provides several built-in partitioners:

```rust
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

```rust
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

During a partial metadata refresh for produced topics, Krafka caches topic metadata between refreshes. By default, a topic entry is evicted after **5 minutes** without a successful refresh, matching Java's `metadata.max.idle.ms`, so topic churn does not grow the cache indefinitely.

```rust
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

```rust
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

```rust
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

```rust
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

```rust
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

```rust
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

```rust
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

### Durability

For maximum durability:

```rust
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

```rust
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

If you need a bounded shutdown window, use `close_with_timeout()` instead. On timeout, Krafka tears down the connection pool and returns a timeout error, causing any remaining in-flight work to fail fast instead of hanging shutdown indefinitely:

```rust
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

```rust
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

```rust
use krafka::producer::TransactionalProducer;
use krafka::protocol::Compression;
use std::time::Duration;

let producer = TransactionalProducer::builder()
    .bootstrap_servers("localhost:9092")
    .transactional_id("order-processor-1")
    .client_id("my-app")
    .transaction_timeout(Duration::from_secs(60))          // 60 second timeout
    .request_timeout(Duration::from_secs(30))
    .compression(Compression::Lz4)
    .build()
    .await?;
```

### Authentication

Connect a transactional producer to secured Kafka clusters:

```rust
use krafka::producer::TransactionalProducer;

// SASL/SCRAM-SHA-256
let producer = TransactionalProducer::builder()
    .bootstrap_servers("broker:9093")
    .transactional_id("my-txn-id")
    .sasl_scram_sha256("username", "password")
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

See the [Authentication Guide](authentication.md) for all supported mechanisms.

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

### Graceful Shutdown (Transactional)

Always close transactional producers properly. The `close()` method:
- Blocks new sends and waits for already-started transactional produce requests to finish
- Aborts any active transaction to avoid dangling open transactions on the broker
- Transitions the producer to `FatalError` state, preventing further use
- Closes the underlying connection pool
- Is idempotent — calling it more than once is a no-op

```rust
// Graceful shutdown
producer.close().await;
// Producer is no longer usable after close()
```

For bounded shutdown windows, `close_with_timeout()` provides the same semantics with an explicit deadline:

```rust
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
`ProducerEpoch` in the `EndTxn` v4+ response. Krafka reads these fields and automatically applies
them to the local identity, so subsequent `AddPartitionsToTxn` requests use the correct epoch.

For brokers that do not support `EndTxn` v4+ (Kafka < 3.7), the response omits these fields and
Krafka continues with the unchanged epoch — the pre-KIP-890 protocol is used transparently.

### Timestamps

Both `Producer` and `TransactionalProducer` propagate the `timestamp` field from `ProducerRecord` to the Kafka record batch. If set, the timestamp is used as the `base_timestamp` of the record batch:

```rust
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
See the [Interceptors Guide](interceptors.md) for full details.

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

- [Dead Letter Queue](errors.md#dead-letter-queue) - Route permanently-failed records to an error topic
- [Interceptors Guide](interceptors.md) - Producer and consumer interceptor hooks
- [Consumer Guide](consumer.md) - Learn about consuming messages
- [Configuration Reference](configuration.md) - All producer options
- [Architecture Overview](architecture.md) - How the producer works internally
