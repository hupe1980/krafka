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
```

### Offset Commit

Control how offsets are committed:

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
    .max_poll_records(500)                       // Max records per poll
    .fetch_max_wait(Duration::from_millis(500))  // Max wait time
    .build()
    .await?;
```

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

Krafka supports multiple assignment strategies:

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

Get notified when partition assignments change during rebalances:

```rust
use krafka::consumer::{ConsumerRebalanceListener, TopicPartition};

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

// Use the NoOpRebalanceListener for a no-op implementation:
use krafka::consumer::NoOpRebalanceListener;
let _listener = NoOpRebalanceListener;
```

The listener callbacks are useful for:
- Committing offsets before partition loss
- Saving processing state to external storage
- Initializing resources when new partitions are assigned
- Proper cleanup during consumer group rebalances

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
// Commit asynchronously (fire and forget)
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

```rust
consumer.subscribe(&["orders", "payments", "shipments"]).await?;
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

## Next Steps

- [Producer Guide](producer.md) - Learn about producing messages
- [Configuration Reference](configuration.md) - All consumer options
- [Architecture Overview](architecture.md) - How the consumer works internally
