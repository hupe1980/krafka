---
layout: default
title: Share Consumer
nav_order: 5
description: "Queue-like consumption with KIP-932 share groups"
---

# Share Consumer Guide

{: .label .label-yellow }
**Unstable** — requires the `unstable-protocol` feature flag.

This guide covers the share consumer, which provides queue-like consumption semantics via [KIP-932 share groups](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka). Share groups are available in Apache Kafka 4.0+.

## Overview

Share groups differ from traditional consumer groups in several key ways:

| Feature | Consumer Group | Share Group |
|---|---|---|
| Assignment | Client or server-side | Server-side only |
| Offset tracking | Per-partition committed offsets | Per-record acknowledgements |
| Delivery | Exactly-once (with transactions) | At-least-once |
| Record sharing | One consumer per partition | Multiple consumers per partition |
| Redelivery | Seek / reset offsets | Automatic (release/reject) |

Multiple consumers in the same share group receive **non-overlapping subsets of records** from the same partition — the server handles all assignment and delivery tracking.

## Basic Usage

```rust
use krafka::share_consumer::{ShareConsumer, AcknowledgementMode};
use std::time::Duration;

let consumer = ShareConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-share-group")
    .build()
    .await?;

consumer.subscribe(&["events"]).await?;

loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    for record in &records {
        process(record);
    }
    // In Implicit mode (default), records are auto-accepted on next poll()
}
```

## Acknowledgement Modes

### Implicit (Default)

Records fetched by the previous `poll()` are automatically accepted when the next `poll()` is called. This is the simplest mode — no application-level acknowledgement logic is needed. Consecutive offsets for the same partition are coalesced into contiguous ranges to reduce wire overhead.

### Explicit

The application controls acknowledgement per record. **All records from the previous `poll()` must be acknowledged before calling `poll()` again** — otherwise `poll()` returns an error. This prevents accidentally losing records:

```rust
use krafka::share_consumer::{ShareConsumer, AcknowledgementMode, AcknowledgeType};

let consumer = ShareConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-share-group")
    .acknowledgement_mode(AcknowledgementMode::Explicit)
    .build()
    .await?;

consumer.subscribe(&["events"]).await?;

let records = consumer.poll(Duration::from_secs(1)).await?;
for record in &records {
    match try_process(record) {
        Ok(_) => consumer.acknowledge(record, AcknowledgeType::Accept).await?,
        Err(_) => consumer.acknowledge(record, AcknowledgeType::Release).await?,
    }
}
consumer.commit_sync().await?;
```

### Acknowledge Types

| Type | Value | Meaning |
|---|---|---|
| `Accept` | 1 | Record processed successfully |
| `Release` | 2 | Record released for redelivery to another consumer |
| `Reject` | 3 | Record rejected (moved to dead-letter after max retries) |

## Delivery Count

Each `ConsumerRecord` includes a `delivery_count` field (populated from the server's acquired-records metadata). This tells you how many times the record has been delivered, which is useful for implementing retry limits:

```rust
for record in &records {
    if let Some(count) = record.delivery_count {
        if count > 5 {
            consumer.acknowledge(record, AcknowledgeType::Reject).await?;
            continue;
        }
    }
    process(record);
}
```

## Async Commit

For fire-and-forget acknowledgement (errors logged, not returned). If lock contention or a missing coordinator prevents the snapshot, pending acks are **preserved** for the next commit cycle rather than silently dropped:

```rust
consumer.commit_async();
```

## Streaming API

The share consumer also supports a `Stream`-based API:

```rust
use tokio_stream::StreamExt;

let mut stream = consumer.stream();
while let Some(record) = stream.next().await {
    let record = record?;
    process(&record);
}
```

## Configuration

| Option | Default | Description |
|---|---|---|
| `bootstrap_servers` | (required) | Comma-separated broker addresses |
| `group_id` | (required) | Share group identifier |
| `client_id` | `"krafka-share-consumer"` | Client identifier |
| `acknowledgement_mode` | `Implicit` | `Implicit` or `Explicit` |
| `fetch_min_bytes` | `1` | Minimum bytes per fetch |
| `fetch_max_bytes` | `52_428_800` | Maximum bytes per fetch |
| `max_poll_records` | `500` | Maximum records per poll |
| `max_records` | `-1` | Server-side max records (`-1` = no limit) |
| `batch_size` | `0` | Server-side batch size hint (`0` = default) |
| `fetch_max_wait_ms` | `500` | Maximum wait time for fetch responses |
| `request_timeout` | `30s` | Request timeout |
| `session_timeout` | `45s` | Session timeout for group membership |
| `heartbeat_interval` | `5s` | Heartbeat interval (must be < session_timeout) |
| `metadata_max_age` | `5min` | Metadata cache TTL |
| `metadata_topic_cache_ttl` | `Some(5min)` | TTL for topic entries in the partial-refresh cache. `None` disables eviction. Use `disable_metadata_topic_cache_ttl()` to opt out. |
| `client_rack` | `None` | Rack ID for rack-aware fetching |

### Metadata Topic Cache TTL

During a partial metadata refresh (where only the subscribed topics are re-fetched rather than the entire cluster), Krafka caches each topic's metadata between refreshes. By default, a topic entry is evicted from this cache after **5 minutes** of not being successfully refreshed — matching Java's `metadata.max.idle.ms` — to prevent unbounded growth when topics are deleted or subscriptions change.

```rust
use krafka::share_consumer::ShareConsumer;
use std::time::Duration;

// Use a custom TTL (e.g. 10 minutes):
let consumer = ShareConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-share-group")
    .metadata_topic_cache_ttl(Duration::from_secs(600))
    .build()
    .await?;

// Opt out of TTL eviction entirely (topics persist until the cache is flushed):
let consumer = ShareConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-share-group")
    .disable_metadata_topic_cache_ttl()
    .build()
    .await?;
```

> **Note:** TTL eviction only affects the partial-refresh cache. A full metadata refresh (triggered by `metadata_max_age` expiry or an explicit refresh) always replaces the cache unconditionally.

## Session Management

Share sessions (similar to fetch sessions from KIP-227) track per-broker state with epoch-based sequencing:

- **Epoch 0**: Opens a new session (full fetch)
- **Epoch 1..N**: Incremental fetches
- **Epoch -1**: Closes the session

Sessions are managed automatically. They reset on errors or assignment changes.

## Concurrent Fetching

Each `poll()` issues ShareFetch requests to all assigned brokers **concurrently** using a `tokio::task::JoinSet`. Pending acknowledgements are piggybacked on fetch requests to reduce round trips. If a broker fetch fails, records from other brokers are still returned — the error is logged and the session for the failed broker is reset.

## Coordinator Handling

The share consumer discovers its group coordinator via `FindCoordinator` (key type = GROUP). The coordinator is cached and re-discovered automatically when:

- A heartbeat fails
- A `NOT_COORDINATOR` error is received
- `unsubscribe()` or `close()` is called

## Lifecycle

```rust
// Create
let consumer = ShareConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .build()
    .await?;

// Subscribe
consumer.subscribe(&["topic1", "topic2"]).await?;

// Consume
let records = consumer.poll(Duration::from_secs(1)).await?;

// Unsubscribe (leaves group, generates a new member ID)
consumer.unsubscribe().await?;

// Close (idempotent)
consumer.close().await?;
```

### Close Semantics

`close()` performs best-effort cleanup:

1. **Implicit mode**: all pending accept acks are converted to **releases** so acquired records return to the pool for redelivery by other consumers.
2. **Explicit mode**: pending acks (accept/release/reject) are flushed as-is.
3. Sends a leave-group heartbeat.
4. Clears all local state and closes connections.

### Unsubscribe Semantics

`unsubscribe()` leaves the group, clears all partition state (pending acks, sessions, coordinator), and generates a fresh member ID. The consumer can be resubscribed afterwards.

## Wire Protocol

The share consumer uses four Kafka APIs (all feature-gated behind `unstable-protocol`):

| API | Key | Versions | Purpose |
|---|---|---|---|
| ShareGroupHeartbeat | 76 | v1 | Group membership and assignment |
| ShareGroupDescribe | 77 | v1 | Describe share group state |
| ShareFetch | 78 | v1–v2 | Fetch records with acquisition tracking |
| ShareAcknowledge | 79 | v1–v2 | Acknowledge processed records |

See the [Protocol Reference](protocol.md) for wire format details.
