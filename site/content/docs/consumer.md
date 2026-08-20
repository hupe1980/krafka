+++
title = "Consumer"
description = "Consumer groups, cooperative and eager rebalancing, offset management and KIP-848."
weight = 40

[extra]
slug_id = "consumer"
+++

## Overview

The krafka consumer is an async-native, feature-rich Kafka consumer with:

- Consumer group coordination
- Automatic offset management
- Multiple partition assignment strategies
- Manual offset control
- Seek operations
- Incremental fetch sessions (KIP-227)
- Closest-replica fetching (KIP-392)
- Static group membership (KIP-345)
- KIP-848 consumer group protocol (server-side assignment)
- Interceptor hooks
- Log compaction awareness with [`CompactedTable`](#compactedtable) and [`CompactedTopicConsumer`](#compactedtopicconsumer) for key→value tables
- Per-partition offset lag tracking

## Basic Usage

```rust,compile
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

```rust,compile
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

See the [Authentication Guide](@/docs/authentication.md) for all supported mechanisms.

## Consumer Configuration

### Auto Offset Reset

Control behavior when no committed offset exists:

```rust,compile
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

> **Note:** After a consumer group rebalance, krafka automatically fetches previously committed offsets from the group coordinator (OffsetFetch). Partitions without committed offsets use the configured `auto_offset_reset` policy.

> **OffsetOutOfRange Recovery:** If the broker returns `OffsetOutOfRange` during a fetch (e.g., because a partition was truncated or the consumer fell behind log retention), krafka automatically applies the configured `auto_offset_reset` policy to recover the partition instead of stalling. This works for both group-based and standalone (manually assigned) consumers.

> **Offset Resolution:** When multiple partitions need offset resolution (e.g., after a rebalance or on first poll), krafka batches `ListOffsets` requests by leader broker — resolving 50 partitions in 2-3 RPCs instead of 50. Failed offset resolutions use per-partition exponential backoff (100ms base, 30s cap) to prevent retry storms under sustained broker unavailability.

> **Corrupt Record Batches:** A trailing batch cut short by the fetch size limit is normal — krafka delivers the decodable prefix and re-requests the rest on the next fetch. A batch that fails to decode *at the current fetch position* is different: the partition cannot advance past it. krafka treats that as a partition-level fault:
>
> - Other partitions sharing the same leader still decode normally — one corrupt partition does not discard the whole broker's fetch.
> - `poll()` returns the decode error (`CrcMismatch`, `UnsupportedMagic`, `InvalidValue`, …) naming the topic, partition and offset, and stating both remedies.
> - No offset is advanced, so nothing is skipped; the records collected in that round are simply re-fetched.
> - `batch_decode_errors_total` is incremented, so this is alertable without log scraping.
>
> Failing the poll is deliberate. Re-fetching the same offset returns the same bytes, so there is no recovery to automate, and a fault the application cannot see is worse than one it can. Two remedies are available:
>
> ```rust
> match consumer.poll(Duration::from_secs(1)).await {
>     Ok(records) => { /* ... */ }
>     Err(e) if e.protocol_error_kind() == Some(ProtocolErrorKind::CrcMismatch) => {
>         // Keep consuming everything else while you investigate...
>         consumer.pause("events", &[0]).await;
>         // ...or skip the corrupt data once you have decided it is lost:
>         // consumer.seek("events", 0, next_good_offset).await?;
>     }
>     Err(e) => return Err(e),
> }
> ```
>
> `pause()` excludes the partition at request-build time, so every other partition keeps flowing.

### KIP-848 consumer group protocol

`GroupProtocol::Consumer` selects the KIP-848 protocol, where the coordinator
computes assignments server-side and `ConsumerGroupHeartbeat` is the only
membership channel — no JoinGroup, no SyncGroup, no client-side assignor.

```rust,compile
use krafka::consumer::{Consumer, GroupProtocol};

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .group_protocol(GroupProtocol::Consumer)   // recommended; needs Kafka 4.0+
    .build()
    .await?;
```

> **Prefer this over `Classic`.** KIP-848 was declared production ready in
> Apache Kafka 4.0, and Apache Kafka 4.3 began *deprecating* the classic
> protocol (KIP-1274: warn in 4.3, the default flips in 5.0, removal in 6.0).
> krafka logs the same deprecation notice, once per process, when a group
> starts on the classic protocol.

**The default is still `Classic`** — not because it is the better choice, but
because krafka supports Kafka 3.9 brokers and KIP-848 needs 4.0 (or 3.7–3.9
with `group.coordinator.new.enable=true`). Flipping the default would turn a
client upgrade into a protocol migration that fails against a supported broker.
The default will change when krafka's broker floor moves to 4.0.

krafka's
KIP-848 support is validated against a reconciling in-process coordinator that
covers membership, epoch fencing, server-side assignment, leave-by-epoch, and
**multi-member reconciliation** — the coordinator revokes before it assigns,
waits for the shrinking member to acknowledge, and only then releases those
partitions to their new owner. The test suite asserts on every observation that
no partition is ever owned by two members at once, and that assertion is
verified to fail if the safety rule is removed.

Two behaviours are worth knowing because they differ from the classic protocol:

- **Fencing revokes everything.** When the coordinator returns
  `FENCED_MEMBER_EPOCH` or `UNKNOWN_MEMBER_ID`, the member immediately drops
  *all* its partitions, fires `on_partitions_lost` (not `on_partitions_revoked`
  — do not commit from it), and rejoins at epoch 0 with the same member ID. It
  keeps its partitions only long enough to hand them back, so an auto-commit
  can never write offsets for a partition another member already owns.
- **A null assignment means "no change".** The coordinator sends the assignment
  field only when it moves; a steady-state heartbeat carries none. That is
  confirmation of membership, not an absence of it.
- **Reconciliation is acknowledged, not assumed.** When the coordinator shrinks
  a member's assignment, the member revokes locally and then reports its
  remaining partitions in the next heartbeat. Until that report arrives the
  coordinator will not hand those partitions to anyone else, so a slow
  `on_partitions_revoked` callback delays the rebalance rather than causing an
  overlap. Keep that callback fast.

KIP-848 requires Metadata v10+ so topic UUIDs in assignments can be resolved to
names. Against an older broker the client reports that explicitly rather than
silently consuming nothing.

### Offset Commit

> **Leader epochs are committed (KIP-320).** krafka stores the leader epoch each position was read at alongside the committed offset, and reads it back on `OffsetFetch`. That is what lets the next owner of a partition — after a restart or a rebalance — ask the broker whether the log still contains that `(offset, epoch)` pair before consuming from it. Committing a bare offset would disable truncation detection at every commit boundary, which is precisely the window an unclean leader election opens. `OffsetAndMetadata::leader_epoch` is honoured when you commit explicitly; positions that came from a `seek()` or an offset reset commit `-1`, because the consumer has no epoch it can honestly vouch for.

Control how offsets are committed. When auto-commit is enabled (the default), krafka automatically commits offsets during each `poll()` call when the commit interval has elapsed, during `close()`, and **before partition revocations** during rebalances (so the new partition owner sees up-to-date committed positions). `close().await` still tears down local state before returning; final auto-commit failures that only indicate the member already lost the group during a rebalance are treated as best-effort shutdown races, while other close-time commit failures still surface:

> **At-least-once caveat:** auto-commit advances to the offset of the last record
> *delivered to your code*, not the last record your code finished *processing*.
> Records that were delivered but not yet processed when the application crashes
> are skipped on restart.
>
> krafka does guarantee that an offset is never committed for a record that was
> fetched but not yet handed to you: the committable position is clamped to the
> lowest offset still sitting in the internal buffer, so buffered-but-undelivered
> records are never committed away, including across a rebalance. `poll()` is
> also cancellation-safe — dropping the future (a `select!` arm, an elapsed
> `timeout`) never advances the position past records you did not receive.
>
> For strict at-least-once, disable auto-commit and call `commit()` after
> processing each batch.

```rust,compile
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

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .fetch_min_bytes(1)                          // Min bytes before returning
    .fetch_max_bytes(52428800)                   // Max bytes per fetch (50MB)
    .max_partition_fetch_bytes(1048576)          // Max bytes per partition (1MB)
    .max_poll_records(500)                       // Max records per poll
    .max_buffered_records(500)                   // Buffer cap for recv()
    .fetch_max_wait(Duration::from_millis(500))  // Max wait time
    .build()
    .await?;
```

### Buffer Cap

When using `recv()`, records from `poll()` that are not immediately returned are buffered internally. The `max_buffered_records` setting controls the maximum number of records held in this buffer. When the buffer reaches the limit, `poll()` skips fetching new data until the buffer drains below the threshold. Auto-commit and rebalance handling still run so the consumer remains healthy in the group.

For single-caller `recv()` usage the buffer is naturally bounded by `max_poll_records` (one `poll()` batch minus the record returned to the caller). The cap adds an additional guard for:
- Mixed `poll()` / `recv()` usage on the same consumer
- Multiple tasks calling `recv()` concurrently

Set to `0` to disable the buffer cap (unlimited). Defaults to `500`.

### Read-ahead

The buffer is not only an overflow area — it is a prefetch pipeline.

Each fetch decodes one delivery's worth (`max_poll_records`) **plus** whatever
capacity the buffer has free, and parks the surplus. The next `poll()` finds
the buffer stocked and returns immediately, without a single byte on the wire.
In steady state that halves the number of Fetch round trips and removes the
network latency from every other poll.

```text
poll 1   fetch → decode 1000 → deliver 500, park 500
poll 2   buffer → deliver 500                        (no network)
poll 3   fetch → decode 1000 → deliver 500, park 500
```

A fetch response may carry up to `fetch_max_bytes` (50 MB by default). Decoding
all of it to return 500 records would throw the rest away and re-decode the same
bytes next poll; decoding exactly 500 would fix the waste but pay a round trip
every poll. Decoding `max_poll_records + free buffer capacity` fixes both.

Nothing is ever dropped, so the fetch position advances over everything
decoded — and the commit is held behind whatever is still parked (see
[Position vs fetch position](#position-vs-fetch-position)).

Partitions take turns at the front of the fetch order, rotating by one position
per poll. Both the broker's `fetch_max_bytes` accounting and the
`max_poll_records` cap consume partitions in request order, so the rotation is
what stops a busy partition at the front from starving the rest.

### Position vs fetch position

Because the consumer reads ahead, two offsets exist per partition and they mean
different things:

| Accessor | Meaning |
|---|---|
| `position()` | The offset of the next record that will be **delivered** to you. This is what a commit writes. |
| `fetch_position()` | The offset the next **fetch** starts from. Runs ahead by whatever is parked. |

`position()`, `lag()`, `current_lag()`, `is_caught_up()` and `commit()` are all
derived from the same boundary — the lowest offset still awaiting delivery — so
they can never disagree about a partition. In particular, records read ahead
into the buffer still count as lag: they have been fetched, but the application
has not seen them.

```rust
let delivered = consumer.position("orders", 0).await;      // commit follows this
let read_ahead = consumer.fetch_position("orders", 0).await; // >= delivered
```

A crash between the two loses nothing: the commit never acknowledges a record
that was not returned from `poll()`.

```rust,compile
use krafka::consumer::Consumer;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .max_buffered_records(1000) // Allow up to 1000 buffered records
    .build()
    .await?;
```

### Isolation Level

Control visibility of transactional records. When consuming from topics that receive transactional writes, set `isolation_level` to `read_committed` to only see committed records:

```rust,compile
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

> **Note:** `isolation_level` affects both data fetches and offset resolution (ListOffsets). krafka passes the isolation level to the broker via ListOffsets (v2+, up to v11).

### Metadata Topic Cache TTL

During a partial metadata refresh (where only the subscribed topics are re-fetched rather than the entire cluster), krafka caches each topic's metadata between refreshes. By default, a topic entry is evicted from this cache after **5 minutes** of not being successfully refreshed — matching Java's `metadata.max.idle.ms` — to prevent unbounded growth when topics are deleted or subscriptions change.

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

// Use a custom TTL (e.g. 10 minutes):
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .metadata_topic_cache_ttl(Duration::from_secs(600))
    .build()
    .await?;

// Opt out of TTL eviction entirely (topics persist until the cache is flushed):
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .disable_metadata_topic_cache_ttl()
    .build()
    .await?;
```

> **Note:** TTL eviction only affects the partial-refresh cache. A full metadata refresh (triggered by `metadata_max_age` expiry or an explicit refresh) always replaces the cache unconditionally.

## Consumer Groups

### How Consumer Groups Work

1. Consumers with the same `group_id` form a consumer group
2. Partitions are distributed among group members
3. Each partition is consumed by exactly one consumer
4. When consumers join/leave, partitions are rebalanced

```rust,compile
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

krafka supports multiple assignment strategies. Configure the strategy via the builder:

```rust,compile
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

// Eager sticky: minimises partition movement, but still revokes everything
// before each rebalance round.
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategy(PartitionAssignmentStrategy::Sticky)
    .build()
    .await?;

// Cooperative sticky: minimal partition movement AND no stop-the-world
// revocation — members keep consuming partitions they retain.
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
    .build()
    .await?;
```

### Migrating from eager to cooperative

The default is the preference list `[Range, CooperativeSticky]`, matching the
Java client. Every member advertises both protocols in JoinGroup and the
coordinator picks the first one *all* members support, so a group can move from
eager to cooperative rebalancing in a **single rolling bounce**: as soon as the
last `Range`-only member is replaced, the group negotiates `CooperativeSticky`
on its own.

```rust
// Explicit preference order. The first strategy supported by every member wins.
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .partition_assignment_strategies([
        PartitionAssignmentStrategy::CooperativeSticky,
        PartitionAssignmentStrategy::Range,
    ])
    .build()
    .await?;
```

Setting a single strategy with `partition_assignment_strategy()` is shorthand
for a one-element list — which forces a full stop-the-world restart to change
protocols later.

The underlying assignor implementations are also available directly:

```rust,compile
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
### Rebalances do not block on your poll loop

The background group task keeps heartbeating through a rebalance and issues
`JoinGroup`/`SyncGroup` itself. A consumer that is idle, or busy processing
between `poll()` calls, therefore does not hold up the other members — the
coordinator can complete the rebalance for the whole group immediately.

The new assignment is *applied* on this consumer's next `poll()`: offsets are
committed, `on_partitions_revoked` runs, partition state is updated, then
`on_partitions_assigned` runs. Keeping that on the poll path is deliberate — it
means your rebalance listener never runs concurrently with your own use of the
consumer, and an offset commit cannot race a revocation.

If an application stops polling entirely, `max.poll.interval.ms` applies: the
consumer leaves the group so its partitions are reassigned promptly. A static
member (`group.instance.id`) instead stops heartbeating and keeps its assignment
until the session expires, so a restarting instance can reclaim it.

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

```rust,compile
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
   with **only the newly acquired partitions** (delta vs previous round).
   Committed offsets are fetched only for the newly acquired partitions.

> **Java client parity:** `on_partitions_assigned` follows the same
> delta semantics as the Java `ConsumerRebalanceListener.onPartitionsAssigned`.
> To get the **full** post-rebalance assignment call `consumer.assignment()`
> from inside the callback.

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

> **Note:** After rebalance completes, krafka automatically issues `OffsetFetch` to the group coordinator to retrieve committed offsets for all assigned partitions. Consumption resumes from the last committed offset.

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

`commit_async()` is **not** an `async fn` — it returns an `OffsetCommitHandle`
immediately and runs the commit in the background. That is the point: you can
keep processing while the commit is in flight and collect the outcome later.

```rust
// Start the commit and carry on processing.
let commit = consumer.commit_async();

let records = consumer.poll(Duration::from_secs(1)).await?;
process(&records);

// Collect the outcome when it suits you. Snapshot, transport and broker
// failures all surface here; retriable coordinator failures use the same
// short backoff loop as `commit()`.
commit.await?;
```

Awaiting it straight away is legal but equivalent to `commit()`:

```rust
consumer.commit_async().await?;
```

Dropping the handle detaches the background task — the commit still runs, but
its result is discarded. The type is `#[must_use]` so this has to be
deliberate.

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

In group mode, only currently assigned partitions are committed. Retriable
coordinator failures use the same short retry loop as `commit()` and
`commit_async()`.

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

// Seek multiple partitions atomically (one lock acquisition)
use std::collections::HashMap;
consumer.seek_many(&HashMap::from([
    (("orders".to_string(), 0), 1_000),
    (("orders".to_string(), 1), 2_000),
])).await?;

// Seek to the beginning (earliest available)
consumer.seek_to_beginning("topic", 0).await?;

// Seek to the end (latest, only receive new messages)
consumer.seek_to_end("topic", 0).await?;
```

Every seek is a hard reposition. Records already fetched for the affected
partitions are discarded, so the next `poll()` / `recv()` returns data from the
new position and nothing from before it — and, just as importantly, the next
commit reflects the position that was sought to rather than being dragged back
onto a stale buffered record. The same applies when `auto_offset_reset` moves a
partition after `OFFSET_OUT_OF_RANGE`.

The recorded leader epoch is invalidated too, so the consumer re-validates the
new position against the leader's log (KIP-320) before fetching from it rather
than reporting a `(position, epoch)` pair it never actually held.

### Starting from Known Offsets (Exactly-Once Recovery)

Use `initial_offsets` on the builder to set per-partition start positions before
`auto_offset_reset` is applied. This is ideal for recovery pipelines that
checkpoint positions externally:

```rust
use std::collections::HashMap;
use krafka::consumer::Consumer;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .initial_offsets(HashMap::from([
        (("orders".to_string(), 0), 1_234),
        (("orders".to_string(), 1), 5_678),
    ]))
    .build()
    .await?;
```

Initial offsets are applied when a partition is first assigned and has no
committed group offset. They override `auto_offset_reset` for the matching
partitions; unmatched partitions still follow `auto_offset_reset`.

### Pause and Resume

Temporarily pause consumption of specific partitions:

```rust,compile
// Pause specific partitions
consumer.pause("orders", &[0, 1]).await;

// Check which partitions are paused
let paused = consumer.paused_partitions().await;
println!("Paused partitions: {:?}", paused);

// Resume consumption
consumer.resume("orders", &[0, 1]).await;
```

Paused partitions are skipped until resumed — by `poll()`, and equally by
`recv()`, `batch_recv()` and `stream()`, which withhold any records that were
already buffered for a partition when it was paused. `pause()` therefore means
the same thing whichever read API you use.

Withheld records are *held*, not discarded: the fetch position has already
advanced past them, so they are delivered on `resume()` rather than re-fetched,
and a commit taken while paused stays behind them so nothing is acknowledged
before the application has seen it.

This is useful for:
- Back-pressure handling when downstream is slow
- Prioritizing certain partitions
- Implementing rate limiting
- Isolating a partition with a corrupt batch while everything else keeps flowing

> **Rebalance behavior:** Pause state is preserved for partitions that remain assigned to the same consumer across both eager and cooperative rebalances. Only revoked partitions lose their pause state. `unsubscribe()` and `close()` still clear all pause state.

## Manual Partition Assignment

For direct partition control (without consumer groups):

> **Note:** Manual assignment and group subscription are mutually exclusive.
> Calling `assign()` on a consumer with a `group_id` configured will return an error.

> **Standalone Recovery:** Standalone consumers have the same `OffsetOutOfRange` recovery as group consumers — the configured `auto_offset_reset` policy is applied automatically to recover stalled partitions.

```rust,compile
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

```rust,compile
// Subscribe to initial topics
consumer.subscribe(&["orders", "payments"]).await?;

// This REPLACES the subscription — only "shipments" is subscribed now
consumer.subscribe(&["shipments"]).await?;
```

### Check Subscriptions and Assignments

```rust,compile
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
state (offsets, paused partitions, buffered records). It returns a leave-group
error after local state has still been cleared.

```rust
consumer.unsubscribe().await?;
```

### Pause and Resume

Temporarily pause consumption of specific partitions without disconnecting:

```rust,compile
// Pause partitions 0 and 1 of "orders" topic
consumer.pause("orders", &[0, 1]).await;

// These partitions are skipped by every read API — poll(), recv(),
// batch_recv() and stream() — including records already buffered for them.
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

`recv()` returns `Result<ConsumerRecord, RecvError>` instead of `Result<Option<ConsumerRecord>>`:
- `Ok(record)` — a record was received.
- `Err(RecvError::Closed)` — the consumer was shut down.
- `Err(RecvError::Error(e))` — a broker or network error occurred.

```rust,compile
use krafka::consumer::Consumer;
use krafka::error::Result;
use krafka::RecvError;

async fn consume_stream(consumer: &Consumer) -> Result<()> {
    loop {
        match consumer.recv().await {
            Ok(record) => println!(
                "topic={}, partition={}, offset={}",
                record.topic, record.partition, record.offset
            ),
            Err(RecvError::Closed)   => break,
            Err(RecvError::Error(e)) => return Err(e),
            Err(_) => break,
        }
    }
    Ok(())
}
```

### High-Throughput Batch Receive

`batch_recv(max_records, timeout)` collects up to `max_records` in one call,
returning an explicit [`BatchRecvOutcome`] so timeout/close/empty-request are
unambiguous:

```rust,compile
use std::time::Duration;
use krafka::consumer::{BatchRecvOutcome, Consumer};
use krafka::error::Result;

async fn process_batches(consumer: &Consumer) -> Result<()> {
    loop {
        match consumer.batch_recv(100, Duration::from_millis(200)).await? {
            BatchRecvOutcome::Records(batch) => {
                for record in batch {
                    println!("offset={}", record.offset);
                }
            }
            BatchRecvOutcome::TimedOut => continue,
            BatchRecvOutcome::Closed => break,
            BatchRecvOutcome::EmptyRequest => continue,
            _ => continue,
        }
    }
    Ok(())
}
```

### Async `Stream` API

The `stream()` method returns a [`futures_core::Stream`](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html)
of `Result<ConsumerRecord>`, enabling use with `tokio-stream` combinators
(`.map()`, `.filter()`, `.take()`, `.buffer_unordered()`, etc.):

```rust
use krafka::consumer::Consumer;
use krafka::error::Result;
use tokio_stream::StreamExt; // requires tokio-stream dependency

async fn consume_with_stream(consumer: &Consumer) -> Result<()> {
    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        let record = result?;
        println!(
            "topic={}, partition={}, offset={}",
            record.topic, record.partition, record.offset
        );
    }
    Ok(())
}
```

The stream terminates when the consumer is closed. Internally it delegates to
`recv()`, so all features (auto-commit, rebalancing, fetch sessions, buffering)
work identically.

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
consumer.close().await?;
```

## Poll Architecture

### Batch Fetch by Broker

krafka optimizes the `poll()` operation by batching fetch requests per broker. Instead of sending 
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

krafka implements [KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability) fetch sessions to minimize fetch request sizes. Instead of sending the full partition list on every `poll()`, the broker tracks per-session state and the client sends only partition changes.

**How it works:**

1. On the first fetch to a broker, krafka sends a full fetch request (epoch 0) with all partitions
2. The broker establishes a session and returns a `session_id`
3. On subsequent fetches, krafka computes a diff against the previous session state:
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

krafka implements [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392%3A+Allow+consumers+to+fetch+from+closest+replica) to allow consumers to fetch from the closest replica rather than always from the partition leader. This is especially useful in multi-datacenter or multi-availability-zone deployments where cross-rack traffic is expensive.

**Configuration:**

Set `client_rack` to the rack or availability zone of the consumer:

```rust,compile
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
4. On subsequent polls, krafka routes that partition's fetch to the preferred replica
5. The mapping expires after `metadata_max_age` (default 5 minutes), causing a fresh lookup

**Error fallback:**

- If a non-leader replica returns an error, the preferred replica mapping is cleared
- The next poll falls back to the partition leader
- On rebalance or unsubscribe, all preferred replica mappings are cleared

**Requirements:**

- Broker must support Fetch API v11 (Kafka 2.4+)
- Brokers must be configured with `broker.rack`
- When `client_rack` is not set, krafka negotiates up to Fetch v10 (sessions + leader epoch fencing) but does not send a rack ID

## Performance Tips

### High Throughput

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("high-throughput")
    .fetch_max_bytes(104857600)              // 100MB max fetch
    .max_partition_fetch_bytes(10485760)     // 10MB per partition
    .max_poll_records(10000)                 // Many records per poll
    .max_buffered_records(10000)              // Match poll batch size
    .fetch_max_wait(Duration::from_millis(100))
    .build()
    .await?;
```

### Low Latency

```rust,compile
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

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("memory-efficient")
    .fetch_max_bytes(1048576)                // Limit to 1MB
    .max_partition_fetch_bytes(262144)       // 256KB per partition
    .max_poll_records(100)                   // Limit in-memory records
    .max_buffered_records(200)               // Tight buffer cap
    .build()
    .await?;
```

## Static Group Membership (KIP-345)

Static group membership allows consumers to maintain a persistent identity across restarts,
avoiding unnecessary rebalances. When a consumer with a `group_instance_id` disconnects and
reconnects (within the session timeout), it automatically gets the same partition assignment
without triggering a rebalance for the entire group.

### Enabling Static Membership

```rust,compile
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

When `group_instance_id` is set, krafka automatically:
- Uses JoinGroup v5 and Heartbeat v3 protocol versions
- Includes the instance ID in all group coordinator requests (Join, Sync, Heartbeat, OffsetCommit, Leave)
- Uses LeaveGroup v3 with member identity on graceful shutdown

### Best Practices

- Assign a **unique** `group_instance_id` per consumer instance (e.g., hostname, pod name)
- Increase `session_timeout` to cover restart duration (e.g., 5 minutes for rolling deployments)
- Use with `CooperativeSticky` assignor for minimal partition movement

```rust,compile
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

## KIP-848 Consumer Group Protocol

KIP-848 introduces a new consumer group protocol where the server performs
partition assignment instead of the group leader. This eliminates the
JoinGroup/SyncGroup round-trip and replaces it with a single
`ConsumerGroupHeartbeat` API (key 68, v0–v1).

### Enabling KIP-848

Set `GroupProtocol::Consumer` on the builder to use the KIP-848 consumer
protocol. Requires Kafka 4.0+ (or 3.7–3.9 with
`group.coordinator.new.enable=true`, where it is early-access rather than
production-stable). This is the **recommended** protocol; the classic one is
deprecated upstream from Kafka 4.3 (KIP-1274).

```rust,compile
use krafka::consumer::{Consumer, GroupProtocol};

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .group_protocol(GroupProtocol::Consumer)  // KIP-848
    .build()
    .await?;

consumer.subscribe(&["my-topic"]).await?;
```

### How It Works

| Classic Protocol | KIP-848 Consumer Protocol |
|-----------------|--------------------------|
| JoinGroup + SyncGroup + Heartbeat | ConsumerGroupHeartbeat only |
| Client-side assignment (group leader) | Server-side assignment |
| Generation ID | Member epoch |
| `generation_id = -1` (unjoined) | `member_epoch = 0` (join) |
| LeaveGroup request | `member_epoch = -1` (permanent leave) or `-2` (static member temporary leave) |

With the consumer protocol:
1. A member joins by sending a heartbeat with `member_epoch = 0`
2. The coordinator assigns partitions and returns the assignment in the response
3. Members maintain their session by sending periodic heartbeats
4. The heartbeat task updates the local assignment and state when the broker returns new assignments
5. The consumer layer computes an incremental diff to determine revoked vs. newly assigned partitions. `on_partitions_revoked` is fired for the affected revoked partitions, while `on_partitions_assigned` receives the **full post-rebalance assignment** (consistent with the cooperative and eager paths in this crate)
6. To leave, a dynamic member sends `member_epoch = -1` (permanent). A static member (with `group_instance_id`) sends `member_epoch = -2` (temporary leave — the broker retains the assignment for the session-timeout window so the instance can rejoin quickly)

### Subscription Changes

If `subscribe()` is called with a different topic list while the consumer is
already active (state `Stable`), the existing heartbeat task is stopped and the
next `poll()` sends a full heartbeat with all fields (including the new topic
list). This mirrors the cooperative-rebalance subscription-change detection.

### Topic UUID Resolution

The ConsumerGroupHeartbeat response uses 16-byte topic UUIDs in assignments.
krafka resolves these UUIDs to topic names with a two-level lookup order:

1. **Cluster metadata lookup** — first consult `ClusterMetadata::topic_name_for_id`.
   In Metadata v10 and later, brokers can return topic UUID → name mappings
   in metadata responses, and krafka uses automatic API version negotiation
   to take advantage of that when supported.
2. **Local topic names cache** — if metadata does not contain the mapping,
   fall back to a local UUID → name cache built from previously resolved
   assignments.  This cache survives metadata cache flushes and mirrors the
   Java client's `AbstractMembershipManager` behavior once a name has been
   learned.

Successfully resolved names are cached locally. Unresolvable UUIDs still
trigger an automatic metadata refresh.

If topic UUIDs
remain unresolved after a metadata refresh during the initial heartbeat
response handling, the client returns a protocol error rather than silently
operating with an empty or partial assignment. Inside the background heartbeat
task, unresolved UUIDs produce a `warn!` log and the assignment is retained
for re-resolution on the next tick. The raw target assignment (with UUIDs) is
always retained so resolution can be retried after future updates or once a
UUID → name mapping becomes available.

The `StaleMemberEpoch` error (113) is handled as a transient condition: the
member epoch is updated from the response and the heartbeat retries on the
next tick without triggering a rebalance.

### Dynamic Heartbeat Interval

The coordinator may adjust the heartbeat interval over time by returning a
different `heartbeat_interval_ms` in the ConsumerGroupHeartbeat response. The
KIP-848 heartbeat task honours these updates: after each successful response,
the current interval is compared with the response value and, if changed, the
timer is reset to the new duration (with a minimum floor of 1 000 ms).

### Version Notes

- **v0** — Base version; compatible with Kafka 3.7+ (EA) and 4.0+ (GA)
- **v1** — Adds `SubscribedTopicRegex` for regex-based topic subscription (KIP-848) and requires consumer-generated member IDs (KIP-1082); available on Kafka 4.0+

Both v0 and v1 are supported (`CONSUMER_GROUP_HEARTBEAT_MIN = 0`, `CONSUMER_GROUP_HEARTBEAT_MAX = 1`).

### Error Handling

The ConsumerGroupHeartbeat response may return these KIP-848-specific errors:

| Error Code | Name | Handling |
|-----------|------|----------|
| 8 | `RebalanceInProgress` | Signal rebalance; consumer processes assignment diff on next poll |
| 14 | `CoordinatorLoadInProgress` | Transient — retry on next heartbeat tick |
| 15 | `NotCoordinator` | Clear cached coordinator, trigger rediscovery |
| 16 | `CoordinatorNotAvailable` | Clear cached coordinator, trigger rediscovery |
| 110 | `FencedMemberEpoch` | Fenced — heartbeat task stops, member preserves its `member_id` and rejoins with epoch 0 via a full heartbeat (all top-level fields) |
| 111 | `UnreleasedInstanceId` | Static member instance ID held by another member — same fencing recovery as `FencedMemberEpoch` |
| 112 | `UnsupportedAssignor` | Server-side assignor not recognized |
| 113 | `StaleMemberEpoch` | Update local epoch from response, retry on next heartbeat |
| 128 | `InvalidRegularExpression` | Regex subscription (v1+) is malformed |

### Fencing Recovery

When the heartbeat task receives `FencedMemberEpoch`, `UnknownMemberId`, or
`UnreleasedInstanceId`, it:

1. Signals the consumer layer (member invalidated + rebalance needed)
2. Stops the heartbeat task (no more skinny heartbeats with stale state)

On the next `poll()`, the consumer detects the fencing via `needs_rejoin()`:

1. Resets `member_epoch` to 0 and clears assignment/target state
2. **Preserves `member_id`** — per KIP-848, a fenced member must "rejoin with
   the same member id and epoch 0"
3. Sets state to `Unjoined`

The `handle_group_rebalance()` path then calls `ensure_active_membership()`,
which sends a **full heartbeat** (subscription, rebalance timeout, all
top-level fields) and starts a fresh heartbeat task.

### Requirements and Compatibility

- **Minimum broker version**: Kafka 4.0 (GA). Earlier brokers (3.7–3.9) expose
  KIP-848 behind `group.coordinator.new.enable=true` but it is **not production-stable
  before 4.0**. From 4.0 onward it is production ready and is the protocol
  Apache is standardising on. The client selects between classic (`GroupProtocol::Classic`) and
  KIP-848 (`GroupProtocol::Consumer`) at runtime via the `group_protocol` builder
  option — no Cargo feature flag is required.
- **Protocol stability**: `GroupProtocol::Classic` (the default) works with all
  Kafka versions ≥ 0.10, but is **deprecated upstream** — Apache Kafka 4.3
  warns on it (KIP-1274 phase 1), 5.0 flips the default, 6.0 removes it. Use
  `GroupProtocol::Consumer` whenever every broker in the cluster runs Kafka
  4.0+.
- **Migrate the whole group together.** On pre-4.0 brokers the two protocols
  cannot mix within one group. Either upgrade the cluster first, or move every
  member in one coordinated deploy.
- **Required API key**: API key 68 (`ConsumerGroupHeartbeat`), versions 0–1.
- **Regex subscriptions** require `ConsumerGroupHeartbeat` v1 (Kafka 4.0+).
- **Assignment is server-side.** Under KIP-848 the broker computes assignments,
  so the client-side assignors (`range`, `roundrobin`, `sticky`,
  `cooperative-sticky`) do not apply. Configure the assignor on the broker via
  `group.consumer.assignors`.
- **Transactional offset commits work on both protocols.**
  `Consumer::group_metadata()` reports the member epoch under KIP-848 and the
  generation ID under the classic protocol; both occupy the same wire field, so
  `send_offsets_to_transaction` fences zombies identically either way.

### Describing KIP-848 Groups

To inspect a KIP-848 consumer group (state, epochs, member assignments), use
the AdminClient's `describe_consumer_groups()` method which auto-detects the
group type and dispatches to the appropriate API. See the
[Admin Client Guide](@/docs/admin.md#describing-consumer-groups) for details.

## Consumer Interceptors

Interceptors allow you to observe records after they are fetched and monitor offset commits.
See the [Interceptors Guide](@/docs/interceptors.md) for full details.

```rust
use krafka::interceptor::{ConsumerInterceptor, InterceptorResult};
use krafka::consumer::{Consumer, ConsumerRecord};
use std::sync::Arc;

#[derive(Debug)]
struct MetricsInterceptor;

impl ConsumerInterceptor for MetricsInterceptor {
    fn on_consume(&self, records: &[ConsumerRecord]) -> InterceptorResult {
        println!("Consumed {} records", records.len());
        Ok(())
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

krafka correctly handles log-compacted topics where records may have been deleted within a batch.
Record offsets are calculated using each record's `offset_delta` rather than sequential indices,
ensuring accurate offset tracking even when records within a batch have been removed by compaction.

This means:
- `consumer.position()` always returns the correct offset, even on compacted topics
- Offset commits are accurate — no risk of re-processing or skipping records
- No special configuration needed; compaction awareness is built-in

### Tombstone Detection

Records in compacted topics with a key but no value are **tombstones** — deletion markers that eventually cause the key to be removed from the log. Use `ConsumerRecord::is_tombstone()` to detect them:

```rust,compile
use std::time::Duration;

// Assuming `consumer` is an already-configured Consumer instance
let records = consumer.poll(Duration::from_secs(1)).await?;
for record in &records {
    if record.is_tombstone() {
        println!("Key {:?} was deleted", record.key);
    } else {
        println!("Key {:?} = {:?}", record.key, record.value);
    }
}
```

A tombstone is a **null** value, not an empty one: `record.value` is `None`
rather than `Some(Bytes::new())`. The wire format distinguishes the two and
compaction treats them as opposites — a null deletes the key, a zero-length
value is ordinary data it preserves. To *write* one, see
[Tombstones and Compacted Topics](@/docs/producer.md).

### CompactedTable

`CompactedTable` is a standalone, Kafka-agnostic data structure that maintains an in-memory key→value snapshot from consumer records. It handles tombstones automatically and tracks changes via `TableChange`. Because it is decoupled from the consumer, it composes with **any** consumer setup — group-coordinated, standalone, or manually assigned:

```rust,compile
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
- **Read access** — `get()` → `Option<&CompactedEntry>` (value + offset + timestamp + partition), `get_value()` → `Option<&Bytes>` (value only), `contains_key()`, `keys()`, `values()`, `iter()`, `snapshot()`, `len()`, `is_empty()`
- **Bulk load** — `ingest()` applies records without building a change list (ideal for initial scans)
- **Reset** — `clear()` removes all entries and resets counters (useful during rebalances)
- **Clone** — `table.clone()` produces a full copy including counters; `table.snapshot()` clones only the entries
- **Equality** — two tables are equal (`PartialEq`/`Eq`) when they contain the same entries; processing counters are ignored
- **IntoIterator** — `for (key, entry) in &table { ... }` or `for (key, entry) in table { ... }` (consuming); yields `(&Bytes, &CompactedEntry)` / `(Bytes, CompactedEntry)`

`TableChange` derives `PartialEq` and `Eq`, so changes can be compared directly with `assert_eq!` in tests.

### CompactedTopicConsumer

For the common case of scanning an entire compacted topic from the beginning, `CompactedTopicConsumer` bundles a `Consumer` and `CompactedTable` together with built-in caught-up detection:

```rust,compile
use krafka::consumer::{CompactedTopicConsumer, Consumer};
use std::time::Duration;

let mut ctc = CompactedTopicConsumer::from_consumer_builder(
    Consumer::builder().bootstrap_servers("localhost:9092"),
    "user-profiles",
)
.await?;

// Build the initial snapshot (blocks until caught up)
ctc.scan(Duration::from_secs(1)).await?;
assert!(ctc.is_caught_up());

// Read a single key — get() returns Option<&CompactedEntry> with full provenance
if let Some(entry) = ctc.table().get(b"user-123") {
    println!("User profile value: {:?}", entry.value);
    println!("  at offset {} partition {} ts {}ms", entry.offset, entry.partition, entry.timestamp_ms);
}

// Use get_value() when you only need the raw bytes
if let Some(bytes) = ctc.table().get_value(b"user-123") {
    println!("User profile: {:?}", bytes);
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
- **Consumer access** — `consumer()` and `consumer_mut()` expose the underlying `Consumer` for seek, pause, commit, or metrics; `into_parts()` decomposes the wrapper into `(Consumer, CompactedTable)`

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

let mut ctc = CompactedTopicConsumer::from_consumer_builder(
    Consumer::builder()
        .bootstrap_servers("broker:9093")
        .auth(AuthConfig::sasl_scram_sha256("user", "password")),
    "config-topic",
)
.await?;
```

## Offset Lag Tracking

krafka tracks consumer lag automatically by caching the end offset returned in
every fetch response. When the broker supports Fetch v5+, the log start offset
is also cached. No additional network calls are needed.

Lag values are returned as `u64` (always non-negative, clamped at zero when the
position is ahead of the end offset) to match the internal metrics
representation.

### Lag under `read_committed`

The end offset lag is measured against **depends on the isolation level**, and
the difference matters as soon as transactions are involved:

| Isolation level | End offset used | Why |
|---|---|---|
| `ReadUncommitted` (default) | high watermark | Every appended record is deliverable |
| `ReadCommitted` | **last stable offset (LSO)** | The broker will not deliver a record at or above the LSO |

Measuring a `read_committed` consumer against the high watermark reports lag it
can never close: with a transaction open on the partition, the gap between the
LSO and the high watermark is the size of that open transaction, and it stays
there until the transaction commits or aborts no matter how much the consumer
reads. `is_caught_up()` would never return `true`, and a lag-based autoscaler
would scale out against a consumer that is fully drained.

`current_lag()`, `lag()`, `is_caught_up()`, `cached_end_offset()` and the
`lag` / `lag_max` metrics all respect the configured isolation level. Two
accessors expose the raw values when you want them explicitly:

```rust,compile
// Isolation-aware: the LSO under read_committed, the high watermark otherwise.
let end = consumer.cached_end_offset("orders", 0).await;

// Always the log-end offset, including records inside open transactions.
let hw = consumer.cached_high_watermark("orders", 0).await;

// The first offset belonging to an open transaction, when the broker
// reported one (Fetch v4+).
let lso = consumer.cached_last_stable_offset("orders", 0).await;
```

The gap between `cached_high_watermark` and `cached_last_stable_offset` is
exactly the volume of in-flight transactional data on that partition — a useful
signal for spotting a transaction that is taking too long to complete.

```rust,compile
// Per-partition lag (returns None if no fetch has completed for this partition)
if let Some(lag) = consumer.current_lag("my-topic", 0).await {
    println!("Partition 0 lag: {} records", lag);
}

// All partition lags at once. `lag()` returns a `LagResult`, not a bare map:
// alongside the per-partition values it names the partitions whose cached
// watermark is older than `lag_staleness_threshold` (default 60 s), so a lag
// of "0" that is simply out of date is distinguishable from a real zero.
let lags = consumer.lag().await;
for ((topic, partition), lag) in &lags.lag {
    println!("{}-{}: {} records behind", topic, partition, lag);
}
for (topic, partition) in &lags.stale_partitions {
    println!("{}-{}: lag value may be outdated", topic, partition);
}

// Cached beginning/end offsets (no network call)
if let Some(start) = consumer.cached_beginning_offset("my-topic", 0).await {
    println!("Earliest available offset: {}", start);
}
if let Some(end) = consumer.cached_end_offset("my-topic", 0).await {
    // Under read_committed this is the last stable offset, not the high
    // watermark — see "Lag under read_committed" above.
    println!("End offset: {}", end);
}
```

Lag is also exposed via metrics (recomputed after every offset or high-watermark mutation — seek, commit, poll, offset reset, revocation):

| Metric | Description |
|--------|-------------|
| `lag` | Total lag across all assigned partitions |
| `lag_max` | Maximum per-partition lag |

High watermarks and log start offsets are automatically cleared when partitions are revoked or the consumer unsubscribes. Lag metrics are recomputed accordingly.

> **Staleness caveat** — End offsets are only updated when a fetch
> response is received from the broker. If the consumer is paused, slow, or
> not polling, the cached values (and therefore `current_lag`, `lag()` and the
> `lag`/`lag_max` metrics) can become stale and undercount the true lag. Treat
> lag values as eventually consistent rather than real-time. For a precise
> answer, call `fetch_end_offset()`, which issues a live `ListOffsets` RPC.

## Next Steps

- [Interceptors Guide](@/docs/interceptors.md) - Producer and consumer interceptor hooks
- [Producer Guide](@/docs/producer.md) - Learn about producing messages
- [Configuration Reference](@/docs/configuration.md) - All consumer options
- [Architecture Overview](@/docs/architecture.md) - How the consumer works internally

## Interrupting a poll

`wakeup()` interrupts a `poll()` that is already parked on a broker fetch, from
any task:

```rust
use std::sync::Arc;

// `tokio::signal` needs tokio's `signal` feature; any shutdown source works.
let consumer = Arc::new(consumer);
let shutdown = Arc::clone(&consumer);
tokio::spawn(async move {
    tokio::signal::ctrl_c().await.ok();
    shutdown.wakeup();
});

while let Ok(records) = consumer.poll(Duration::from_secs(30)).await {
    for record in records {
        process(record);
    }
}
```

A `wakeup()` that lands *before* `poll()` is called is not lost — it interrupts
the next one. Exactly one poll is interrupted; the flag is consumed, not
sticky, and the consumer stays usable afterwards. Records already fetched by
the interrupted poll are still returned rather than discarded, because throwing
them away would advance offsets with nothing delivered.

Dropping the `poll()` future also cancels it, but only from the task that owns
the future. `wakeup()` works from any task, which is the case a shutdown
handler actually has. This mirrors Java's `KafkaConsumer.wakeup()` and
[`ShareConsumer::wakeup`](@/docs/share-consumer.md).

## Reading a group's committed offsets

`committed()` asks the group coordinator where the *group* is, as opposed to
`position()`, which reports where *this consumer* will read next:

```rust,compile
let committed = consumer.committed(&[("orders", 0), ("orders", 1)]).await?;
for ((topic, partition), pos) in &committed {
    println!("{topic}-{partition} committed at {}", pos.offset);
}
```

The two differ by exactly the records consumed but not yet committed — the
window an at-least-once pipeline replays after a crash.

A partition the group has **never** committed is absent from the map rather
than reported as `0`. Those are very different states, and conflating them is
how a dashboard reports a healthy group that is actually sitting at the start
of a topic.

Requires a `group_id`; an assign-only consumer has no coordinator to ask and
gets an error naming what is missing.
