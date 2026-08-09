+++
title = "Share Consumer"
description = "Queue-like consumption with KIP-932 share groups: per-record acknowledgement without partition ownership."
weight = 50

[extra]
slug_id = "share-consumer"
+++

Share groups ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)) give Kafka queue-like semantics: records are acknowledged individually and a partition is not owned by one member. Stable as of Apache Kafka 4.0.

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

The application controls acknowledgement per record. **All records from the previous `poll()` must be acknowledged before calling `poll()` again** — otherwise `poll()` returns an error. `acknowledge()` is one-shot per record: acknowledging the same record twice returns an error instead of sending duplicate broker intent. If a later `commit_sync()` or `commit_async()` flush fails, the consumer restores that batch locally and later `poll()` calls keep returning an error until the commit is retried successfully or the local share-consumer state is cleared.

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

To acknowledge by topic/partition/offset directly — useful when a record fails to deserialize and you have no `ConsumerRecord` to pass:

```rust
consumer.acknowledge_by_offset("events", partition, offset, AcknowledgeType::Reject).await?;
```

For a timeout-bounded flush:

```rust
consumer.commit_sync_with_timeout(Duration::from_secs(5)).await?;
```

### Acknowledge Types

| Type | Value | Meaning |
|---|---|---|
| `Accept` | 1 | Record processed successfully |
| `Release` | 2 | Record released for redelivery to another consumer |
| `Reject` | 3 | Record rejected (moved to dead-letter after max retries) |
| `Renew` | 4 | Extend the acquisition lock without completing the record (KIP-1222, Kafka 4.2+) |

### Renewing an acquisition lock

A record you have been given is *acquired*, not consumed: the broker holds a
lock on it for `group.share.record.lock.duration.ms` and redelivers it to
another member if that expires. `Renew` extends the lock for work that takes
longer than the lock lasts.

That requires knowing when the lock expires — and the duration is a
**broker-side** setting, so it cannot be read from the client's own
configuration. The broker reports it on every `ShareFetch`, and
`acquisition_lock_timeout()` is where it surfaces:

```rust,ignore
use krafka::share_consumer::AcknowledgeType;
use std::time::{Duration, Instant};

// `None` before the first fetch, and on brokers older than Kafka 4.2.
let lock = consumer
    .acquisition_lock_timeout()
    .unwrap_or(Duration::from_secs(30));
// Renew once a record is halfway to losing its lock.
let renew_after = lock / 2;

for record in consumer.poll(Duration::from_secs(1)).await? {
    let started = Instant::now();
    // ... long-running work, renewing as it goes ...
    if started.elapsed() >= renew_after {
        consumer.acknowledge(&record, AcknowledgeType::Renew).await?;
    }
    consumer.acknowledge(&record, AcknowledgeType::Accept).await?;
}
```

The lock starts when the broker *acquires* the record — when it builds the
fetch response — not when `poll()` returns. Treat the value as an upper bound
on the time remaining and renew with margin.

Brokers older than Kafka 4.2 reject an entire acknowledgement batch containing
an unknown type, so krafka drops `Renew` acknowledgements when the negotiated
`ShareFetch`/`ShareAcknowledge` version is below 2 and logs a warning. The lock
then simply expires, which is the same outcome as not renewing.

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

> **A flush waits for an in-flight poll.** `poll()` holds the pending
> acknowledgements out of the internal map while its `ShareFetch` is on the
> wire. `commit_sync()` and `close()` wait for that poll to finish before
> draining, so neither can flush an empty map and report success while
> acknowledgements are still in flight. This matters for the usual shutdown —
> `wakeup()` then `close()` — because `wakeup()` does not wait for the poll it
> interrupts to unwind.

## Async Commit

`commit_async()` returns a handle that resolves to the final commit outcome. This keeps the send off the caller's immediate path while still surfacing transport, decode, and broker errors explicitly. If any failure occurs, the batch is restored locally for the next commit cycle rather than silently dropped:

```rust
consumer.commit_async().await?;
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

Every option below has a builder setter and a matching accessor on
`ShareConsumerConfig`, checked in CI by `just config-reachability` — four of
the fetch knobs were previously declared, sent on the wire, and settable by
nobody.

| Option | Type | Default | Description |
|---|---|---|---|
| `bootstrap_servers` | String | (required) | Comma-separated broker addresses |
| `group_id` | String | (required) | Share group identifier |
| `client_id` | String | `"krafka"` | Client identifier sent with requests |
| `acknowledgement_mode` | AcknowledgementMode | `Implicit` | `Implicit` or `Explicit` |
| `fetch_min_bytes` | i32 | `1` | Minimum bytes a broker must have before answering a `ShareFetch` |
| `fetch_max_bytes` | i32 | `52_428_800` | Maximum bytes one `ShareFetch` response may carry (50 MiB) |
| `fetch_max_wait` | Duration | `500ms` | How long a broker may hold a `ShareFetch` waiting for `fetch_min_bytes`. Capped by the `poll()` timeout |
| `max_poll_records` | i32 | `500` | Maximum records handed to the application per `poll()` (must be ≥ 1) |
| `max_buffered_records` | i32 | `500` | Soft threshold on the internal receive buffer; `0` disables the cap |
| `max_records` | i32 | `5000` | Maximum records the broker may **acquire** for this member per `ShareFetch` (KIP-932 `MaxRecords`) |
| `batch_size` | i32 | `500` | Acquisition batch-size hint sent to the broker (KIP-932 `BatchSize`) |
| `request_timeout` | Duration | `30s` | Per-request timeout |
| `connect_timeout` | Duration | `10s` | How long TCP establishment to one broker may take; also the floor on `request_timeout` |
| `session_timeout` | Duration | `45s` | Session timeout for group membership |
| `heartbeat_interval` | Duration | `5s` | Heartbeat interval (must be < `session_timeout`) |
| `metadata_max_age` | Duration | `5min` | Metadata cache TTL |
| `metadata_topic_cache_ttl` | `Option<Duration>` | `Some(5min)` | TTL for topic entries in the partial-refresh cache. `None` disables eviction; use `disable_metadata_topic_cache_ttl()` to opt out |
| `metadata_recovery_strategy` | MetadataRecoveryStrategy | `Rebootstrap` | What to do when every known broker becomes unreachable (KIP-899) |
| `metadata_recovery_rebootstrap_trigger` | Duration | `5min` | How long refreshes may keep failing before re-bootstrapping |
| `client_rack` | `Option<String>` | `None` | Rack ID for closest-replica fetching (KIP-392) |
| `max_decompressed_size` | usize | 128 MiB | Decompression-bomb ceiling for record batches |
| `key_deserializer` / `value_deserializer` | `Arc<dyn Deserializer>` | `None` | Applied to every consumed record — the same hook as the subscription consumer |

### `max_records` is not `max_poll_records`

They bound different things and it matters here more than on a subscription
consumer:

- **`max_poll_records`** caps what one `poll()` call hands *the application*.
  Surplus stays in the client's receive buffer.
- **`max_records`** caps what the broker *acquires* for this member. An acquired
  record holds an acquisition lock until it is acknowledged or the lock expires,
  so this bounds how much of the share group's backlog one member can hold
  hostage — and therefore how much work is stalled if the member dies.

Lower `max_records` for faster failover between members; raise it for
throughput when members are long-lived.

### Deserializers

A share consumer hands back the same `ConsumerRecord` as a subscription
consumer, so it takes the same
[`Deserializer`](https://docs.rs/krafka/latest/krafka/serdes/trait.Deserializer.html)
hook:

```rust,compile
use bytes::Bytes;
use krafka::serdes::Deserializer;
use krafka::share_consumer::ShareConsumer;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Strips a 5-byte Confluent-style framing header.
struct StripHeader;

impl Deserializer for StripHeader {
    fn deserialize(
        &self,
        payload: Bytes,
        _topic: &str,
        _is_key: bool,
    ) -> Pin<Box<dyn Future<Output = krafka::Result<Bytes>> + Send + '_>> {
        Box::pin(async move {
            if payload.len() < 5 {
                return Err(krafka::KrafkaError::serialization("payload is not framed"));
            }
            Ok(payload.slice(5..))
        })
    }
}

let consumer = ShareConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-share-group")
    .value_deserializer(Arc::new(StripHeader))
    .build()
    .await?;
```

A record the decoder rejects fails the `poll()` with
`KrafkaError::RecordDeserialization { topic, partition, offset, .. }`. Unlike a
subscription consumer there is no `seek()` to skip it — the share-group remedy
is to reject the offset so the broker stops redelivering it:

```rust
consumer
    .acknowledge_by_offset(&topic, partition, offset, AcknowledgeType::Reject)
    .await?;
```

That call needs the record to be registered as pending, so deserialization runs
*after* registration. The consequence is worth knowing: in `Implicit` mode the
batch has already been queued for `Accept` by the time the failure surfaces,
because that is what implicit mode means. Use `Explicit` mode when the
application needs to arbitrate poison records.

### Metadata Topic Cache TTL

During a partial metadata refresh (where only the subscribed topics are re-fetched rather than the entire cluster), krafka caches each topic's metadata between refreshes. By default, a topic entry is evicted from this cache after **5 minutes** of not being successfully refreshed — matching Java's `metadata.max.idle.ms` — to prevent unbounded growth when topics are deleted or subscriptions change.

```rust,compile
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

Each `poll()` issues ShareFetch requests to all assigned brokers **concurrently** by spawning one Tokio task per broker and awaiting the handles directly. Pending acknowledgements are piggybacked on fetch requests to reduce round trips. If a broker fetch fails, records from other brokers are still returned, the session for the failed broker is reset, and the unsent piggyback acknowledgements are restored for the next commit cycle.

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
consumer.unsubscribe().await;

// Close (idempotent)
consumer.close().await?;
```

### Close Semantics

`close()` is terminal and returns the first cleanup error after local state and connections have still been closed:

1. **Implicit mode**: all pending accept acks are converted to **releases** so acquired records return to the pool for redelivery by other consumers.
2. **Explicit mode**: pending acks (accept/release/reject) are flushed as-is.
3. Sends and validates a leave-group heartbeat.
4. Clears all local state and closes connections.

Use `close_with_timeout(duration)` to bound each cleanup phase. If a phase exceeds `duration / 2`, it returns `Err(KrafkaError::Timeout)` but still closes local state and connections.

### Wakeup & Cancellation

Call `wakeup()` from any thread or task to interrupt an in-progress `poll()` call:

```rust,compile
// In another task:
consumer.wakeup();

// poll() returns Err with "wakeup() was called"
// The consumer remains fully usable for subsequent poll() calls.
```

`wakeup()` is safe to call concurrently with any other consumer method.

### Unsubscribe Semantics

`unsubscribe()` attempts a best-effort leave-group heartbeat, logs any leave failure internally, clears all partition state (pending acks, sessions, coordinator), and generates a fresh member ID. The consumer can be resubscribed afterwards.

## Observability

`ShareConsumer::metrics()` returns the same [`ConsumerMetrics`] a classic
consumer exposes — a share consumer polls, receives, acknowledges and errors in
the same shapes, so the counters mean the same thing:

```rust,compile
let m = consumer.metrics();
println!(
    "polls={} empty={} records={} bytes={} acks={} errors={}",
    m.polls.get(),
    m.empty_polls.get(),
    m.records_received.get(),
    m.bytes_received.get(),
    m.commits.get(),      // acknowledgement flushes
    m.errors.get(),
);
```

`commits` counts successful acknowledgement flushes — the share-group analogue
of an offset commit. Rebalance, lag and partition gauges stay at zero: the
coordinator owns assignment, and there is no per-partition position to lag
behind.

Transport-level counters (connections, requests, throttles) are separate, on
[`connection_metrics()`](https://docs.rs/krafka/latest/krafka/share_consumer/struct.ShareConsumer.html).

[`ConsumerMetrics`]: https://docs.rs/krafka/latest/krafka/metrics/struct.ConsumerMetrics.html

## Operating a Share Group

A running share group is not the same as an operable one. Reading its
start offsets, resetting them, and cleaning up after a retired topic are
`AdminClient` operations (Kafka 4.2+):

```rust,compile
// Lag monitoring — `lag` requires Kafka 4.3 (KIP-1226); older brokers report None.
let described = admin.describe_share_group_offsets("my-share-group", None).await?;
for p in &described.partitions {
    println!("{}-{} start={} lag={:?}", p.topic, p.partition, p.start_offset, p.lag);
}

// Reset to the beginning. The group must be empty.
admin
    .alter_share_group_offsets("my-share-group", &[("my-topic", &[(0, 0)][..])])
    .await?;

// Drop state for a topic that no longer exists. The group must be empty.
admin.delete_share_group_offsets("my-share-group", &["retired-topic"]).await?;
```

See [Admin Client → Share Group Offset Administration](@/docs/admin.md) for the full
reference.

## Wire Protocol

The share consumer uses four Kafka APIs (all feature-gated behind `unstable-protocol`):

| API | Key | Versions | Purpose |
|---|---|---|---|
| ShareGroupHeartbeat | 76 | v1 | Group membership and assignment |
| ShareGroupDescribe | 77 | v1 | Describe share group state |
| ShareFetch | 78 | v1–v2 | Fetch records with acquisition tracking |
| ShareAcknowledge | 79 | v1–v2 | Acknowledge processed records |

See the [Protocol Reference](@/docs/protocol.md) for wire format details.

## Testing a Share Consumer

The `test-broker` feature's in-process broker serves all three share-group data
APIs at v1, so a share consumer can be tested end to end without a cluster:

```rust
let broker = FakeBroker::start().await?;
broker.create_topic("events", 2);

let consumer = ShareConsumer::builder()
    .bootstrap_servers(broker.bootstrap_servers())
    .group_id("my-share-group")
    .acknowledgement_mode(AcknowledgementMode::Explicit)
    .build()
    .await?;
consumer.subscribe(&["events"]).await?;

for record in consumer.poll(Duration::from_millis(200)).await? {
    consumer.acknowledge(&record, AcknowledgeType::Accept).await?;
}
```

It models the share-partition state machine that replaces committed offsets: a
start offset, an acquisition cursor, and a per-record delivery count. `Accept`
and `Reject` advance the start offset; `Release` returns the record to the pool
with a higher delivery count; records left in flight come back when the member
holding them leaves the group.

It deliberately does **not** model acquisition-lock expiry, the archived state,
`group.share.delivery.attempts`, or `Renew` (KIP-1222) — which is accepted and
has no effect, because with no lock timer there is nothing to extend. A test
that needs any of those needs a real broker. v2 (KIP-1206 `ShareAcquireMode`,
KIP-1222 renew-ack) is not advertised for the same reason: advertising a version
whose semantics the fake broker does not implement would make tests pass for the
wrong reason.
