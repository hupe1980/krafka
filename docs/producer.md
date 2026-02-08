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

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| None | N/A | 1:1 | Low CPU, high bandwidth |
| Gzip | Slow | Best | Archival, infrequent writes |
| Snappy | Fast | Good | General purpose |
| LZ4 | Fastest | Good | High-throughput, real-time |
| Zstd | Medium | Best | Best balance of speed/ratio |

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

When the buffer memory limit is reached, `send()` will return an error. This provides backpressure to prevent OOM conditions under sustained high load.

## Flushing

When using linger-based batching, call `flush()` to ensure all pending records are sent:

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

// Sticky: sticks to one partition until batch is full
let partitioner = StickyPartitioner::new();

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

## Error Handling

Handle send failures gracefully:

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
    .acks(Acks::All)                         // Wait for all ISR
    .enable_idempotence(true)                // Exactly-once semantics
    .retries(10)                             // Retry on failure
    .build()
    .await?;
```

## Graceful Shutdown

Always close producers properly to flush pending messages:

```rust
use krafka::producer::Producer;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .build()
    .await?;

// ... send messages ...

// Flush and close
producer.flush().await?;
producer.close().await;
```

## Transactional Producer

For exactly-once semantics across multiple partitions and topics, use the `TransactionalProducer`:

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
    .transaction_timeout_ms(60000)          // 60 second timeout
    .request_timeout(Duration::from_secs(30))
    .compression(Compression::Lz4)
    .build()
    .await?;
```

### Transaction Lifecycle

1. **Initialize**: Call `init_transactions()` once when producer starts
2. **Begin**: Call `begin_transaction()` to start a new transaction
3. **Send**: Send messages with `send()` or `send_record()`
4. **End**: Call `commit_transaction()` or `abort_transaction()`

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
```

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

// Commit offsets as part of transaction
let mut offsets = HashMap::new();
offsets.insert(topic_partition, offset_and_metadata);
producer.send_offsets_to_transaction(&offsets, "consumer-group").await?;

// Atomic commit of messages and offsets
producer.commit_transaction().await?;
```

### Transaction States

The producer maintains a state machine:

| State | Description |
|-------|-------------|
| `Uninitialized` | Producer created, `init_transactions()` not called |
| `Ready` | Ready to begin a new transaction |
| `InTransaction` | Transaction in progress |
| `Committing` | Transaction being committed |
| `Aborting` | Transaction being aborted |
| `FatalError` | Unrecoverable error, producer must be recreated |

## Next Steps

- [Consumer Guide](consumer.md) - Learn about consuming messages
- [Configuration Reference](configuration.md) - All producer options
- [Architecture Overview](architecture.md) - How the producer works internally
