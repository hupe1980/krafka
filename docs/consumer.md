---
layout: default
title: Consumer
nav_order: 4
description: "Consumer groups with rebalancing and offset management"
---

# Consumer Guide

This guide covers consumer usage, including consumer groups, offset management, partition assignment, and error handling.

## Overview

The Krafka consumer is an async-native, feature-rich Kafka consumer with:

- Consumer group coordination
- Automatic offset management
- Multiple partition assignment strategies
- Manual offset control
- Seek operations
- Static group membership (KIP-345)
- Interceptor hooks
- Log compaction awareness

## Basic Usage

```rust
use krafka::consumer::Consumer;
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("my-group")
        .build()
        .await?;

    consumer.subscribe(&["my-topic"]).await?;

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in records {
            println!("Received: {:?}", record);
        }
    }
}
```

## Authentication

Connect to secured Kafka clusters using SASL or TLS:

```rust
use krafka::consumer::Consumer;

// SASL/SCRAM-SHA-256
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9093")
    .group_id("secure-group")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;

// AWS MSK IAM
use krafka::auth::AuthConfig;
let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9094")
    .group_id("msk-group")
    .auth(auth)
    .build()
    .await?;
```

See the [Authentication Guide](authentication.md) for all supported mechanisms.

## Consumer Configuration

### Auto Offset Reset

Control behavior when no committed offset exists:

```rust
use krafka::consumer::{Consumer, AutoOffsetReset};

// Start from the earliest available message
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .build()
    .await?;

// Start from the latest message (only new messages)
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .auto_offset_reset(AutoOffsetReset::Latest)
    .build()
    .await?;

// Error if no committed offset exists
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .auto_offset_reset(AutoOffsetReset::None)
    .build()
    .await?;
// poll() will return an error for partitions without committed offsets
```

> **Note:** After a consumer group rebalance, Krafka automatically fetches previously committed offsets from the group coordinator (OffsetFetch). Partitions without committed offsets use the configured `auto_offset_reset` policy.

### Offset Commit

Control how offsets are committed. When auto-commit is enabled (the default), Krafka automatically commits offsets during each `poll()` call when the commit interval has elapsed, and also during `close()`:

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

// Auto-commit (default)
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .enable_auto_commit(true)
    .auto_commit_interval(Duration::from_secs(5))
    .build()
    .await?;

// Manual commit
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .enable_auto_commit(false)
    .build()
    .await?;
```

### Fetch Configuration

Control message fetching behavior:

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .fetch_min_bytes(1)                          // Min bytes before returning
    .fetch_max_bytes(52428800)                   // Max bytes per fetch (50MB)
    .max_partition_fetch_bytes(1048576)          // Max bytes per partition (1MB)
    .max_poll_records(500)                       // Max records per poll (strictly enforced)
    .fetch_max_wait(Duration::from_millis(500))  // Max wait time
    .build()
    .await?;
```

### Isolation Level

Control visibility of transactional records. When consuming from topics that receive transactional writes, set `isolation_level` to `read_committed` to only see committed records:

```rust
use krafka::consumer::{Consumer, IsolationLevel};

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .isolation_level(IsolationLevel::ReadCommitted) // Only see committed txn records
    .build()
    .await?;
```

| Level | Description |
|-------|-------------|
| `ReadUncommitted` (default) | See all records, including uncommitted transactional records |
| `ReadCommitted` | Only see committed records; uncommitted transactional records are filtered |

> **Note:** `isolation_level` affects both data fetches and offset resolution (ListOffsets). Krafka uses ListOffsets v2 protocol to pass the isolation level to the broker.

## Consumer Groups

### How Consumer Groups Work

1. Consumers with the same `group_id` form a consumer group
2. Partitions are distributed among group members
3. Each partition is consumed by exactly one consumer
4. When consumers join/leave, partitions are rebalanced

```rust
use krafka::consumer::Consumer;

// Multiple consumers in the same group share partitions
let consumer1 = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("processing-group")
    .build()
    .await?;

let consumer2 = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("processing-group")
    .build()
    .await?;

// Both subscribe to the same topic - partitions are split between them
consumer1.subscribe(&["events"]).await?;
consumer2.subscribe(&["events"]).await?;
```

### Partition Assignment Strategies

Krafka supports multiple assignment strategies. Configure the strategy via the builder:

```rust
use krafka::consumer::{Consumer, PartitionAssignmentStrategy};

// Range assignor (default)
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategy(PartitionAssignmentStrategy::Range)
    .build()
    .await?;

// Round-robin for balanced distribution
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin)
    .build()
    .await?;

// Cooperative sticky for minimal partition movement during rebalances
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
    .build()
    .await?;
```

The underlying assignor implementations are also available directly:

```rust
use krafka::consumer::{RangeAssignor, RoundRobinAssignor, CooperativeStickyAssignor, PartitionAssignor};

// Range assignor (default)
// Assigns partition ranges to consumers: [0,1,2] [3,4,5]
// Best for: Co-partitioned topics
let range = RangeAssignor;
assert_eq!(range.name(), "range");

// Round-robin assignor
// Distributes partitions evenly across all consumers
// Best for: Balanced load across many consumers
let round_robin = RoundRobinAssignor;
assert_eq!(round_robin.name(), "roundrobin");

// Cooperative sticky assignor
// Minimizes partition movement during rebalances (incremental cooperative)
// Best for: Production workloads needing minimal disruption
let cooperative = CooperativeStickyAssignor::new();
assert_eq!(cooperative.name(), "cooperative-sticky");
```

#### Cooperative Sticky Assignor

The `CooperativeStickyAssignor` provides incremental cooperative rebalancing, minimizing
partition movement when consumers join or leave:

```rust
use krafka::consumer::{CooperativeStickyAssignor, PartitionAssignor};

let assignor = CooperativeStickyAssignor::new();

// Key features:
// - Maintains stickiness: partitions stay with their current owner when possible
// - Balanced distribution: ensures fair partition allocation across consumers
// - Incremental rebalance: only moves partitions that need to move

// Track which partitions were revoked during rebalance
// (for implementing incremental cooperative protocol)
// let revoked = assignor.get_partitions_to_revoke("member-id", &new_assignment);
```

### Rebalance Listener

Get notified when partition assignments change during rebalances. Register a listener via the builder:

```rust
use krafka::consumer::{Consumer, ConsumerRebalanceListener, TopicPartition};
use std::sync::Arc;

struct MyRebalanceListener;

impl ConsumerRebalanceListener for MyRebalanceListener {
    fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
        println!("Assigned: {:?}", partitions);
        // Initialize state for new partitions
        // Load any existing checkpoints from external storage
    }

    fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        println!("Revoked: {:?}", partitions);
        // Commit offsets synchronously before losing partitions
        // Save any in-memory state to external storage
    }

    fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
        // Called when partitions are lost unexpectedly (e.g., session timeout)
        // Unlike revoked, offsets may already be committed by another consumer
        println!("Lost: {:?}", partitions);
    }
}

// Wire into the consumer via the builder:
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .rebalance_listener(Arc::new(MyRebalanceListener))
    .build()
    .await?;

// Use the NoOpRebalanceListener for a no-op implementation (default):
use krafka::consumer::NoOpRebalanceListener;
let _listener = NoOpRebalanceListener;
```

The listener is automatically invoked during `poll()` (before/after rebalance) and `close()` (partitions lost). Callbacks are useful for:
- Committing offsets before partition loss
- Saving processing state to external storage
- Initializing resources when new partitions are assigned
- Proper cleanup during consumer group rebalances

> **Note:** After rebalance completes, Krafka automatically issues `OffsetFetch` to the group coordinator to retrieve committed offsets for all assigned partitions. This ensures seamless resumption from the last committed position.

## Offset Management

### Manual Commit

For precise control over offset commits:

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .enable_auto_commit(false)
    .build()
    .await?;

consumer.subscribe(&["orders"]).await?;

loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    
    for record in &records {
        // Process each record
        process_order(&record).await?;
    }
    
    // Commit after processing
    if !records.is_empty() {
        consumer.commit().await?;
    }
}
```

### Async Commit

For non-blocking commits:

```rust
// Commit asynchronously (spawns a background task)
// The commit is tracked and errors are logged if it fails.
consumer.commit_async();
```

### Commit with Metadata

Commit specific offsets with application-specific metadata:

```rust
use std::collections::HashMap;
use krafka::consumer::{Consumer, OffsetAndMetadata, TopicPartition};

// Commit specific offsets with metadata
let mut offsets = HashMap::new();
offsets.insert(
    TopicPartition::new("orders", 0),
    OffsetAndMetadata::with_metadata(1500, "checkpoint-abc123"),
);
offsets.insert(
    TopicPartition::new("orders", 1),
    OffsetAndMetadata::new(2000),
);

consumer.commit_with_metadata(offsets).await?;
```

This is useful for:
- Storing application checkpoints
- Recording processing state
- Debugging offset issues (metadata is visible in Kafka tools)

### Position and Seeking

Query and control consumer position:

```rust
// Get current position
let offset = consumer.position("topic", 0).await;
println!("Current position: {:?}", offset);

// Seek to a specific offset
consumer.seek("topic", 0, 1000).await?;

// Seek to the beginning (earliest available)
consumer.seek_to_beginning("topic", 0).await?;

// Seek to the end (latest, only receive new messages)
consumer.seek_to_end("topic", 0).await?;
```

### Pause and Resume

Temporarily pause consumption of specific partitions:

```rust
// Pause specific partitions
consumer.pause("orders", &[0, 1]).await;

// Check which partitions are paused
let paused = consumer.paused_partitions().await;
println!("Paused partitions: {:?}", paused);

// Resume consumption
consumer.resume("orders", &[0, 1]).await;
```

Paused partitions are skipped during `poll()` until resumed. This is useful for:
- Back-pressure handling when downstream is slow
- Prioritizing certain partitions
- Implementing rate limiting

## Manual Partition Assignment

For direct partition control (without consumer groups):

> **Note:** Manual assignment and group subscription are mutually exclusive.
> Calling `assign()` on a consumer with a `group_id` configured will return an error.

```rust
use krafka::consumer::Consumer;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    // Note: no group_id for manual assignment
    .build()
    .await?;

// Assign specific partitions
consumer.assign("topic", vec![0, 1, 2]).await?;
```

## Subscription Management

### Subscribe to Multiple Topics

`subscribe()` **replaces** the current subscription (it does not append):

```rust
// Subscribe to initial topics
consumer.subscribe(&["orders", "payments"]).await?;

// This REPLACES the subscription — only "shipments" is subscribed now
consumer.subscribe(&["shipments"]).await?;
```

### Check Subscriptions and Assignments

```rust
// Get subscribed topics
let topics = consumer.subscription().await;
println!("Subscribed to: {:?}", topics);

// Get assigned partitions
let assignments = consumer.assignment().await;
println!("Assigned partitions: {:?}", assignments);
```

### Unsubscribe

Calling `unsubscribe()` performs a full cleanup: revokes partitions (notifying
the rebalance listener), leaves the consumer group, and clears all internal
state (offsets, paused partitions, buffered records).

```rust
consumer.unsubscribe().await;
```

### Pause and Resume

Temporarily pause consumption of specific partitions without disconnecting:

```rust
// Pause partitions 0 and 1 of "orders" topic
consumer.pause("orders", &[0, 1]).await;

// These partitions will be skipped during poll()
let records = consumer.poll(Duration::from_secs(1)).await?;
// Only records from non-paused partitions are returned

// Check which partitions are paused
let paused = consumer.paused_partitions().await;
println!("Paused partitions: {:?}", paused);

// Resume consumption
consumer.resume("orders", &[0, 1]).await;
```

Use cases for pause/resume:
- **Backpressure handling**: Pause when downstream systems are slow
- **Priority processing**: Pause low-priority partitions during high load
- **Graceful degradation**: Pause non-essential partitions when resources are constrained

## Error Handling

### Handling Poll Errors

```rust
use krafka::consumer::Consumer;
use krafka::error::KrafkaError;
use std::time::Duration;

async fn consume_with_error_handling(consumer: &Consumer) {
    loop {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                for record in records {
                    process_record(record).await;
                }
            }
            Err(KrafkaError::Timeout(_)) => {
                // Normal - no messages available
                continue;
            }
            Err(e) => {
                eprintln!("Error polling: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
```

### Streaming with `recv()`

The `recv()` method returns individual records as a stream-like API.
It internally buffers records fetched by `poll()` and returns them one by one,
ensuring no data loss even when `poll()` returns multiple records.
Errors propagate to the caller rather than being silently swallowed:

```rust
use krafka::consumer::Consumer;
use krafka::error::Result;

async fn consume_stream(consumer: &Consumer) -> Result<()> {
    while let Some(record) = consumer.recv().await? {
        println!(
            "topic={}, partition={}, offset={}, timestamp_type={}",
            record.topic, record.partition, record.offset, record.timestamp_type
        );
        // timestamp_type: 0 = CreateTime, 1 = LogAppendTime
    }
    Ok(())
}
```

### Graceful Shutdown

Always close consumers properly:

```rust
use tokio::signal;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .build()
    .await?;

consumer.subscribe(&["topic"]).await?;

tokio::select! {
    _ = signal::ctrl_c() => {
        println!("Shutting down...");
    }
    _ = async {
        loop {
            let records = consumer.poll(Duration::from_secs(1)).await?;
            for record in records {
                process_record(record).await;
            }
        }
        #[allow(unreachable_code)]
        Ok::<(), KrafkaError>(())
    } => {}
}

// Commit final offsets and close
consumer.commit().await?;
consumer.close().await;
```

## Poll Architecture

### Batch Fetch by Broker

Krafka optimizes the `poll()` operation by batching fetch requests per broker. Instead of sending 
one request per partition (O(n) round trips), it groups partitions by their leader broker and sends 
one request per broker (O(k) round trips, where k = number of unique leaders).

```
  Consumer.poll()
         │
         ▼
  ┌──────────────────────────────┐
  │ Group partitions by leader   │
  │                              │
  │ Broker 1: [p0, p1, p2]       │
  │ Broker 2: [p3, p4]           │
  │ Broker 3: [p5]               │
  └──────────────────────────────┘
         │
         ▼
  ┌──────────────────────────────┐
  │ One FetchRequest per broker  │
  │                              │
  │ Request 1 → Broker 1         │
  │ Request 2 → Broker 2         │
  │ Request 3 → Broker 3         │
  └──────────────────────────────┘
         │
         ▼
    Merge results
```

This optimization significantly improves throughput when consuming from topics with many partitions 
spread across multiple brokers.

## Performance Tips

### High Throughput

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("high-throughput")
    .fetch_max_bytes(104857600)              // 100MB max fetch
    .max_partition_fetch_bytes(10485760)     // 10MB per partition
    .max_poll_records(10000)                 // Many records per poll
    .fetch_max_wait(Duration::from_millis(100))
    .build()
    .await?;
```

### Low Latency

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("low-latency")
    .fetch_min_bytes(1)                      // Return immediately when data available
    .fetch_max_wait(Duration::from_millis(10))
    .max_poll_records(1)                     // Process one at a time
    .build()
    .await?;
```

### Memory Efficiency

```rust
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("memory-efficient")
    .fetch_max_bytes(1048576)                // Limit to 1MB
    .max_partition_fetch_bytes(262144)       // 256KB per partition
    .max_poll_records(100)                   // Limit in-memory records
    .build()
    .await?;
```

## Static Group Membership (KIP-345)

Static group membership allows consumers to maintain a persistent identity across restarts,
avoiding unnecessary rebalances. When a consumer with a `group_instance_id` disconnects and
reconnects (within the session timeout), it automatically gets the same partition assignment
without triggering a rebalance for the entire group.

### Enabling Static Membership

```rust
use krafka::consumer::Consumer;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .group_instance_id("instance-1")  // Stable identity
    .session_timeout(Duration::from_secs(300))  // Longer timeout for restarts
    .build()
    .await?;

consumer.subscribe(&["my-topic"]).await?;
```

### How It Works

| Behavior | Dynamic (default) | Static (with `group_instance_id`) |
|----------|-------------------|-----------------------------------|
| **Disconnect** | Immediate rebalance | No rebalance until session timeout |
| **Reconnect** | New member, rebalance | Same member, no rebalance |
| **Rolling restart** | N rebalances | Zero rebalances |
| **Protocol version** | JoinGroup v0 | JoinGroup v5 |

When `group_instance_id` is set, Krafka automatically:
- Uses JoinGroup v5 and Heartbeat v3 protocol versions
- Includes the instance ID in all group coordinator requests (Join, Sync, Heartbeat, OffsetCommit, Leave)
- Uses LeaveGroup v3 with member identity on graceful shutdown

### Best Practices

- Assign a **unique** `group_instance_id` per consumer instance (e.g., hostname, pod name)
- Increase `session_timeout` to cover restart duration (e.g., 5 minutes for rolling deployments)
- Use with `CooperativeSticky` assignor for minimal partition movement

```rust
use krafka::consumer::{Consumer, PartitionAssignmentStrategy};
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .group_instance_id("pod-abc-123")
    .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
    .session_timeout(Duration::from_secs(300))
    .build()
    .await?;
```

## Consumer Interceptors

Interceptors allow you to observe records after they are fetched and monitor offset commits.
See the [Interceptors Guide](interceptors.md) for full details.

```rust
use krafka::interceptor::ConsumerInterceptor;
use krafka::consumer::{Consumer, ConsumerRecord};
use std::sync::Arc;

#[derive(Debug)]
struct MetricsInterceptor;

impl ConsumerInterceptor for MetricsInterceptor {
    fn on_consume(&self, records: &[ConsumerRecord]) {
        println!("Consumed {} records", records.len());
    }
}

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .interceptor(Arc::new(MetricsInterceptor))
    .build()
    .await?;
```

## Log Compaction Awareness

Krafka correctly handles log-compacted topics where records may have been deleted within a batch.
Record offsets are calculated using each record's `offset_delta` rather than sequential indices,
ensuring accurate offset tracking even when records within a batch have been removed by compaction.

This means:
- `consumer.position()` always returns the correct offset, even on compacted topics
- Offset commits are accurate — no risk of re-processing or skipping records
- No special configuration needed; compaction awareness is built-in

## Next Steps

- [Interceptors Guide](interceptors.md) - Producer and consumer interceptor hooks
- [Producer Guide](producer.md) - Learn about producing messages
- [Configuration Reference](configuration.md) - All consumer options
- [Architecture Overview](architecture.md) - How the consumer works internally
