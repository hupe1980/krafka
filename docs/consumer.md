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
- Incremental fetch sessions (KIP-227)
- Closest-replica fetching (KIP-392)
- Static group membership (KIP-345)
- Interceptor hooks
- Log compaction awareness with [`CompactedTable`](#compactedtable) and [`CompactedTopicConsumer`](#compactedtopicconsumer) for key→value tables
- Per-partition offset lag tracking

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

> **OffsetOutOfRange Recovery:** If the broker returns `OffsetOutOfRange` during a fetch (e.g., because a partition was truncated or the consumer fell behind log retention), Krafka automatically applies the configured `auto_offset_reset` policy to recover the partition instead of stalling. This works for both group-based and standalone (manually assigned) consumers.

> **Offset Resolution:** When multiple partitions need offset resolution (e.g., after a rebalance or on first poll), Krafka batches `ListOffsets` requests by leader broker — resolving 50 partitions in 2-3 RPCs instead of 50. Failed offset resolutions use per-partition exponential backoff (100ms base, 30s cap) to prevent retry storms under sustained broker unavailability.

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
    .max_poll_records(500)                       // Max records per poll
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

The `CooperativeStickyAssignor` implements the incremental cooperative rebalancing
protocol (KIP-429), minimizing partition movement and avoiding stop-the-world
rebalances when consumers join or leave the group.

**Key features:**
- **Incremental two-phase rebalance**: Only the partitions being moved are revoked
  and cleaned up — unaffected partitions retain their state and do not go through
  a full revoke/reassign cycle.
- **Stickiness**: Partitions stay with their current owner when possible, reducing
  unnecessary movement.
- **Balanced distribution**: Ensures fair partition allocation across consumers.
- **Owned-partition metadata (v1)**: Encodes each member's current assignment in
  JoinGroup metadata so the leader can compute minimal revocations.
- **Proper revocation semantics**: `on_partitions_revoked` is called only for the
  diff (partitions being moved) during normal rebalances (including topic deletion),
  while `on_partitions_lost` is used when ownership may already have been transferred
  (e.g., session timeout, fencing, or graceful shutdown via `close()`).

```rust
use krafka::consumer::{ConsumerBuilder, PartitionAssignmentStrategy};

let consumer = ConsumerBuilder::default()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
    .build()
    .await?;

// During a rebalance, only affected partitions are revoked/released from this consumer.
// Unaffected partitions keep their assignment and continue being consumed.
```

**How it works:**

1. A rebalance is triggered (new member joins, member leaves, etc.).
2. Phase 1: All members join and receive new target assignments.
3. Each member computes which partitions to revoke (old − new).
4. Revoked partitions are released and `on_partitions_revoked` fires.
5. Phase 2: Members rejoin with updated owned-partition metadata.
6. Final assignments are distributed and `on_partitions_assigned` fires
   with the full post-rebalance assignment (committed offsets are only
   fetched for newly acquired partitions).

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
// If offset lock contention occurs, the commit cycle is skipped
// and a warning is logged (rather than silently dropping the commit).
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

> **Standalone Recovery:** Standalone consumers have the same `OffsetOutOfRange` recovery as group consumers — the configured `auto_offset_reset` policy is applied automatically to recover stalled partitions.

```rust
use krafka::consumer::Consumer;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    // Note: no group_id for manual assignment
    .auto_offset_reset(krafka::consumer::AutoOffsetReset::Earliest)
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

### Incremental Fetch Sessions (KIP-227)

Krafka implements [KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability) fetch sessions to minimize fetch request sizes. Instead of sending the full partition list on every `poll()`, the broker tracks per-session state and the client sends only partition changes.

**How it works:**

1. On the first fetch to a broker, Krafka sends a full fetch request (epoch 0) with all partitions
2. The broker establishes a session and returns a `session_id`
3. On subsequent fetches, Krafka computes a diff against the previous session state:
   - **Changed partitions**: Only partitions with new offsets or different `max_bytes`
   - **Forgotten topics**: Partitions removed since the last fetch (e.g., after rebalance)
4. The broker applies the diff to its session state and returns data for all tracked partitions

```
  First poll()              Subsequent poll()
  (full fetch)              (incremental)
  ┌──────────────┐          ┌──────────────┐
  │ session_id: 0│          │ session_id: 42│
  │ epoch: 0     │          │ epoch: 1      │
  │ topics:      │          │ topics:       │
  │   p0, p1, p2 │    →     │   p1 (changed)│
  │   p3, p4     │          │ forgotten:    │
  └──────────────┘          │   p4 (removed)│
                            └──────────────┘
```

**Benefits:**

- **Reduced bandwidth**: With 100 partitions, incremental fetches can be 10-100x smaller
- **Lower broker CPU**: Broker parses smaller requests
- **Automatic fallback**: Falls back to Fetch v4 (full requests) for brokers that don't support v7+

**Error recovery:**

- `FetchSessionIdNotFound` or `InvalidFetchSessionEpoch` errors automatically reset the session
- The next fetch sends a full request to re-establish the session
- All sessions are reset on consumer group rebalance

Fetch sessions are enabled automatically when the broker supports Fetch API v7+. No configuration is needed.

### Closest-Replica Fetching (KIP-392)

Krafka implements [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) to allow consumers to fetch from the closest replica rather than always from the partition leader. This is especially useful in multi-datacenter or multi-availability-zone deployments where cross-rack traffic is expensive.

**Configuration:**

Set `client_rack` to the rack or availability zone of the consumer:

```rust
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .client_rack("us-east-1a")
    .build()
    .await?;
```

**How it works:**

1. The consumer includes its `rack_id` in Fetch requests (Fetch API v11+)
2. The broker compares the consumer's rack with each partition's replica placement
3. If a replica exists in the same rack, the broker returns it as `preferred_read_replica`
4. On subsequent polls, Krafka routes that partition's fetch to the preferred replica
5. The mapping expires after `metadata_max_age` (default 5 minutes), causing a fresh lookup

**Error fallback:**

- If a non-leader replica returns an error, the preferred replica mapping is cleared
- The next poll falls back to the partition leader
- On rebalance or unsubscribe, all preferred replica mappings are cleared

**Requirements:**

- Broker must support Fetch API v11 (Kafka 2.4+)
- Brokers must be configured with `broker.rack`
- When `client_rack` is not set, Krafka negotiates up to Fetch v10 (sessions + leader epoch fencing) but does not send a rack ID

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

### Tombstone Detection

Records in compacted topics with a key but no value are **tombstones** — deletion markers that eventually cause the key to be removed from the log. Use `ConsumerRecord::is_tombstone()` to detect them:

```rust
let records = consumer.poll(Duration::from_secs(1)).await?;
for record in &records {
    if record.is_tombstone() {
        println!("Key {:?} was deleted", record.key);
    } else {
        println!("Key {:?} = {:?}", record.key, record.value);
    }
}
```

### CompactedTable

`CompactedTable` is a standalone, Kafka-agnostic data structure that maintains an in-memory key→value snapshot from consumer records. It handles tombstones automatically and tracks changes via `TableChange`. Because it is decoupled from the consumer, it composes with **any** consumer setup — group-coordinated, standalone, or manually assigned:

```rust
use krafka::consumer::{Consumer, CompactedTable};
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .build()
    .await?;
consumer.subscribe(&["user-profiles"]).await?;

let mut table = CompactedTable::new();
loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    let changes = table.apply(&records);
    for change in &changes {
        if change.is_delete() {
            println!("Deleted: {:?}", change.key);
        } else if change.is_insert() {
            println!("New: {:?} = {:?}", change.key, change.new_value);
        } else {
            println!("Updated: {:?} = {:?}", change.key, change.new_value);
        }
    }
}
```

Key behaviors:
- **Tombstone handling** — keys are removed from the table when a null-valued record arrives
- **Keyless records** — silently skipped (compacted topics require keys)
- **Metrics** — `records_processed()` and `tombstones_processed()` are available for monitoring
- **Read access** — `get()`, `contains_key()`, `keys()`, `values()`, `iter()`, `snapshot()`, `len()`, `is_empty()`
- **Bulk load** — `ingest()` applies records without building a change list (ideal for initial scans)
- **Reset** — `clear()` removes all entries and resets counters (useful during rebalances)
- **Clone** — `table.clone()` produces a full copy including counters; `table.snapshot()` clones only the entries
- **Equality** — two tables are equal (`PartialEq`/`Eq`) when they contain the same entries; processing counters are ignored
- **IntoIterator** — `for (key, value) in &table { ... }` or `for (key, value) in table { ... }` (consuming)

`TableChange` derives `PartialEq` and `Eq`, so changes can be compared directly with `assert_eq!` in tests.

### CompactedTopicConsumer

For the common case of scanning an entire compacted topic from the beginning, `CompactedTopicConsumer` bundles a `Consumer` and `CompactedTable` together with built-in caught-up detection:

```rust
use krafka::consumer::CompactedTopicConsumer;
use std::time::Duration;

let mut ctc = CompactedTopicConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .topic("user-profiles")
    .build()
    .await?;

// Build the initial snapshot (blocks until caught up)
ctc.scan(Duration::from_secs(1)).await?;
assert!(ctc.is_caught_up());

// Read individual keys via the table
if let Some(value) = ctc.table().get(b"user-123") {
    println!("User profile: {:?}", value);
}

// Get the full snapshot
let snapshot = ctc.table().snapshot();
println!("{} keys in table", snapshot.len());

// Tail for live updates
loop {
    let changes = ctc.poll(Duration::from_secs(1)).await?;
    for change in &changes {
        if change.is_delete() {
            println!("Deleted: {:?}", change.key);
        } else if change.is_insert() {
            println!("New: {:?} = {:?}", change.key, change.new_value);
        } else {
            println!("Updated: {:?} = {:?}", change.key, change.new_value);
        }
    }
}
```

Key behaviors:
- **No consumer group** — uses standalone assignment of all partitions
- **Starts from earliest** — `auto_offset_reset` is set to `Earliest` internally
- **Caught-up detection** — `scan()` returns when all partitions reach their high watermarks; `poll()` also updates the flag. Because the high watermark is refreshed on each fetch, `scan()` may block indefinitely on actively written topics — treat it as a best-effort catch-up rather than a bounded snapshot
- **Table access** — `table()` and `table_mut()` give direct access to the underlying `CompactedTable`

For custom consumer setups (e.g., consumer groups, manual offsets), use `CompactedTable` directly.

#### From an Existing Consumer

If you need full control over the consumer configuration (TLS, auth, custom timeouts), build the consumer yourself and pass it in:

```rust
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9093")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .enable_auto_commit(false)
    .auth(AuthConfig::sasl_scram_sha256("user", "password"))
    .build()
    .await?;
consumer.assign("config-topic", vec![0, 1, 2]).await?;

let mut ctc = CompactedTopicConsumer::from_consumer(consumer, "config-topic");
ctc.scan(Duration::from_secs(1)).await?;
```

#### Authentication

Pass an `AuthConfig` to connect to secured clusters:

```rust
use krafka::auth::AuthConfig;

let mut ctc = CompactedTopicConsumer::builder()
    .bootstrap_servers("broker:9093")
    .topic("config-topic")
    .auth(AuthConfig::sasl_scram_sha256("user", "password"))
    .build()
    .await?;
```

## Offset Lag Tracking

Krafka tracks consumer lag automatically by caching the high watermark returned in every fetch response. When the broker supports Fetch v5+, the log start offset is also cached. No additional network calls are needed.

Lag values are returned as `u64` (always non-negative, clamped at zero when the position is ahead of the watermark) to match the internal metrics representation.

```rust
// Per-partition lag (returns None if no fetch has completed for this partition)
if let Some(lag) = consumer.current_lag("my-topic", 0).await {
    println!("Partition 0 lag: {} records", lag);
}

// All partition lags at once
let lags = consumer.lag().await;
for ((topic, partition), lag) in &lags {
    println!("{}-{}: {} records behind", topic, partition, lag);
}

// Cached beginning/end offsets (no network call)
if let Some(start) = consumer.cached_beginning_offset("my-topic", 0).await {
    println!("Earliest available offset: {}", start);
}
if let Some(end) = consumer.cached_end_offset("my-topic", 0).await {
    println!("High watermark: {}", end);
}
```

Lag is also exposed via metrics (recomputed after every offset or high-watermark mutation — seek, commit, poll, offset reset, revocation):

| Metric | Description |
|--------|-------------|
| `lag` | Total lag across all assigned partitions |
| `lag_max` | Maximum per-partition lag |

High watermarks and log start offsets are automatically cleared when partitions are revoked or the consumer unsubscribes. Lag metrics are recomputed accordingly.

## Next Steps

- [Interceptors Guide](interceptors.md) - Producer and consumer interceptor hooks
- [Producer Guide](producer.md) - Learn about producing messages
- [Configuration Reference](configuration.md) - All consumer options
- [Architecture Overview](architecture.md) - How the consumer works internally
