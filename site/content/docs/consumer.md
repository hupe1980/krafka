+++
title = "Consumer"
description = "Consumer groups, cooperative and eager rebalancing, offset management and KIP-848."
weight = 40

[extra]
slug_id = "consumer"
+++

## Overview

The consumer joins a group (classic or KIP-848 protocol), commits offsets
automatically or on demand, seeks, pauses, tracks lag, fetches from the
closest replica, and builds key→value tables from compacted topics with
[`CompactedTable`](#compactedtable) and
[`CompactedTopicConsumer`](#compactedtopicconsumer).

## Basic Usage

```rust,compile
use krafka::consumer::Consumer;
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let consumer = krafka::Kafka::builder("localhost:9092")
        .connect()
        .await?
        .consumer("my-group")
        .build()
        .await?;

    consumer.subscribe(["my-topic"]).await?;

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in records {
            println!("Received: {:?}", record);
        }
    }
}
```

`subscribe()` records the subscription and returns; the group join runs in the
background and `poll()` / `recv()` applies the assignment it produces.

Security (TLS, SASL, AWS MSK IAM) is set on the `Kafka` builder — see the
[Authentication Guide](@/docs/authentication.md).

## Consumer Configuration

### Auto Offset Reset

Where a partition with no committed offset starts:

| `AutoOffsetReset` | Starts at |
|---|---|
| `Latest` (default) | the end of the log |
| `Earliest` | the log start |
| `ByDuration(d)` | the first record timestamped at or after now − `d` (KIP-1106); the end if there is none |
| `None` | nowhere: `poll()` fails with `KrafkaError::NoOffset` |

```rust,compile
use krafka::consumer::{Consumer, AutoOffsetReset};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .auto_offset_reset(AutoOffsetReset::ByDuration(std::time::Duration::from_secs(24 * 3600)))
    .build()
    .await?;
```

For a new group, `ByDuration` does not skip records written before the
consumer first saw a partition, as long as they are younger than `d`.

The same policy recovers a partition after `OFFSET_OUT_OF_RANGE` (truncation,
or the consumer fell behind retention), for group and standalone consumers.

### Corrupt record batches

A batch that fails to decode at the current fetch position stops that
partition only:

- `poll()` returns the decode error (`CrcMismatch`, `UnsupportedMagic`,
  `InvalidValue`, …) naming the topic, partition and offset;
- other partitions, including those on the same broker, keep decoding;
- no offset advances, so nothing is skipped;
- the `batch_decode_errors` metric is incremented.

Re-fetching returns the same bytes, so krafka does not retry. Pause the
partition, or seek past the data once you decide it is lost:

```rust,compile
use krafka::ProtocolErrorKind;
use std::time::Duration;

match consumer.poll(Duration::from_secs(1)).await {
    Ok(records) => { /* ... */ }
    Err(e) if e.protocol_error_kind() == Some(ProtocolErrorKind::CrcMismatch) => {
        // Keep consuming everything else while you investigate...
        consumer.pause("events", &[0]).await;
        // ...or skip the corrupt data once you have decided it is lost:
        // consumer.seek("events", 0, next_good_offset).await?;
    }
    Err(e) => return Err(e),
}
```

A trailing batch cut short by the fetch size limit is normal and is completed
on the next fetch.

### Choosing the group protocol

`GroupProtocol::Consumer` selects KIP-848: the coordinator computes
assignments and `ConsumerGroupHeartbeat` is the only membership API.

```rust,compile
use krafka::consumer::{Consumer, GroupProtocol};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .group_protocol(GroupProtocol::Consumer)
    .build()
    .await?;
```

**The default is `Classic`**, because KIP-848 needs Kafka 4.0 (or 3.7–3.9
with `group.coordinator.new.enable=true`) and krafka supports 3.9 brokers.
Prefer `Consumer` where the brokers support it: as of 2026-10-09,
[KIP-1274](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1274%3A+Deprecate+and+remove+support+for+the+classic+rebalance+protocol+in+KafkaConsumer)
deprecates the classic protocol in the Java consumer. krafka logs a
deprecation warning once per process when a group starts on `Classic`.
See [KIP-848 Consumer Group Protocol](#kip-848-consumer-group-protocol).

### Offset Commit

With auto-commit (the default, every 5 s), krafka commits during `poll()` once
the interval has elapsed, before partitions are revoked in a rebalance, and in
`close()`. `close()` returns a commit error, except one that only says the
member already left the group (`UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION`,
`REBALANCE_IN_PROGRESS`, `FENCED_MEMBER_EPOCH`, `STALE_MEMBER_EPOCH`).

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

// Auto-commit (default)
let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .enable_auto_commit(true)
    .auto_commit_interval(Duration::from_secs(5))
    .build()
    .await?;

// Manual commit
let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .enable_auto_commit(false)
    .build()
    .await?;
```

Guarantees:

- A commit writes the **position**: the offset after the last record
  *returned to your code*. Records fetched but not returned are never
  committed. Records returned but not yet processed when the process crashes
  are skipped on restart — for strict at-least-once, disable auto-commit and
  `commit()` after processing.
- Committing never moves the position.
- Commits from one consumer, manual and automatic, are sent one at a time in
  order, so a retried older commit never lands after a newer one.
- The leader epoch of each position is committed with it (KIP-320), so the
  next owner can detect log truncation. Positions from `seek()` or an offset
  reset commit epoch `-1`.
- `poll()`, `recv()` and the stream are cancel safe: dropping the future loses
  no record and moves no position. `commit()`, `commit_offsets()` and
  `close()` are not: a dropped commit may or may not have been applied
  (committing again is safe), and a dropped `close()` leaves the consumer
  closed without the final commit or group leave.

### Fetch Configuration

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .fetch_min_bytes(1)                  // default 1
    .fetch_max_bytes(52428800)           // default 50 MiB per fetch
    .max_partition_fetch_bytes(1048576)  // default 1 MiB per partition
    .max_poll_records(500)               // default 500 per poll
    .max_buffered_records(500)           // default 500; 0 = unlimited
    .fetch_max_wait(Duration::from_millis(500))
    .build()
    .await?;
```

See [Performance](@/docs/performance.md) for throughput, latency and memory
tuning.

### Buffer and read-ahead

Records fetched but not yet returned are buffered per partition, up to
`max_buffered_records`. Each fetch decodes `max_poll_records` plus the free
buffer space and parks the surplus, so the next `poll()` can return without a
network round trip. A partition with parked records is not fetched again until
they are returned. When the buffer is full — in practice, with paused
partitions holding records — `poll()` skips fetching but still runs
auto-commit and rebalances.

Partitions rotate through the front of the fetch order, so a busy partition
cannot starve the rest.

### Position vs fetch position

| Accessor | Meaning |
|---|---|
| `position()` | The offset of the next record that will be **delivered** to you. This is what a commit writes. |
| `fetch_position()` | The offset the next **fetch** starts from. Runs ahead by whatever is parked. |

`position()`, `lag()` and `commit()` read the same position. Parked records
still count as lag.

```rust,compile
let delivered = consumer.position("orders", 0).await;      // commit follows this
let read_ahead = consumer.fetch_position("orders", 0).await; // >= delivered
```

### Isolation Level

```rust,compile
use krafka::consumer::{Consumer, IsolationLevel};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .isolation_level(IsolationLevel::ReadCommitted)
    .build()
    .await?;
```

| Level | Description |
|-------|-------------|
| `ReadUncommitted` (default) | All records, including those of open and aborted transactions |
| `ReadCommitted` | Only committed transactional records |

The level applies to fetches, offset resolution (`ListOffsets`) and lag.

### Metadata and topic creation

`metadata_topic_cache_ttl` (default 5 minutes) and `allow_auto_create_topics`
(default `false`) are set on the `Kafka` builder and behave as for the
producer — see [Metadata Topic Cache TTL](@/docs/producer.md#metadata-topic-cache-ttl)
and [Letting the broker create the topic](@/docs/producer.md#letting-the-broker-create-the-topic).
With auto-creation on, subscribing to a missing topic creates it if the broker
runs with `auto.create.topics.enable=true`.

## Consumer Groups

Consumers with the same group id share the partitions of the topics they
subscribe to; each partition is consumed by one member at a time.

### Partition Assignment Strategies

The default is the preference list `[Range, CooperativeSticky]`, as in the
Java client. The coordinator picks the first strategy every member supports,
so a group moves from eager to cooperative rebalancing in one rolling bounce
once no `Range`-only member remains.

```rust,compile
use krafka::consumer::PartitionAssignmentStrategy;

// Explicit preference order. The first strategy supported by every member wins.
let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .partition_assignment_strategies([
        PartitionAssignmentStrategy::CooperativeSticky,
        PartitionAssignmentStrategy::Range,
    ])
    .build()
    .await?;
```

`partition_assignment_strategy(s)` sets a one-element list; changing protocol
later then needs a full stop of the group.

| Strategy | Protocol name | Placement |
|----------|---------------|-----------|
| `Range` | `range` | Per topic, contiguous ranges over the members subscribed to it; suits co-partitioned topics; eager |
| `RoundRobin` | `roundrobin` | Every partition in turn to the next member subscribed to its topic; eager |
| `CooperativeSticky` | `cooperative-sticky` | Keeps partitions with their owner where balance allows; members with the same subscription differ by at most one partition; incremental (KIP-429) |

A member only receives partitions of topics it subscribed to. Custom assignors
are not supported. Under KIP-848 these strategies do not apply.

### When a rebalance runs

Every assignment change is applied inside `poll()`, in one order: commit (with
auto-commit), `on_partitions_revoked`, drop the revoked partitions, add the new
ones, `on_partitions_assigned`.

- **Eager** (`Range`, `RoundRobin`): the whole assignment is revoked before
  rejoining. Two members never consume a partition at the same time.
- **Cooperative** (`CooperativeSticky`): only partitions that move are revoked.
  Moving partitions are withheld in the first round and assigned in a follow-up
  round on a later `poll()`; the background task heartbeats and runs the first
  round, so a busy member keeps consuming what it retains.
- `on_partitions_assigned` receives only the **newly added** partitions, as in
  Java; call `consumer.assignment()` for the full set.

The join runs on its own task, so dropping a `poll()` or `recv()` that waits
for it does not lose its outcome.

### Poll interval (`max_poll_interval`)

If the application does not poll for longer than `max_poll_interval` (default
5 minutes), the member leaves the group and stops heartbeating, so its
partitions are reassigned at once. A static classic member sends no
`LeaveGroup` and keeps its assignment until its session expires.

The next `poll()` reports the partitions to `on_partitions_lost` once, rejoins
and continues — it does not return an error. Lost partitions are never
reported to `on_partitions_revoked`. The expiry is logged at `warn`.

### Rebalance Listener

```rust,compile
use krafka::consumer::{ConsumerRebalanceListener, TopicPartition};

struct MyRebalanceListener;

impl ConsumerRebalanceListener for MyRebalanceListener {
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
        // Initialize state for new partitions, load checkpoints, or seek.
        println!("Assigned: {partitions:?}");
    }

    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        // Still owned here: flush state and commit before they are released.
        println!("Revoked: {partitions:?}");
    }

    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
        // Already gone (poll-interval expiry, fencing): do not commit.
        println!("Lost: {partitions:?}");
    }
}

let consumer = kafka
    .consumer("my-group")
    .rebalance_listener(MyRebalanceListener)
    .build()
    .await?;
```

Every method is `async`; `on_partitions_lost` defaults to a no-op. The
listener runs inside `poll()`, `unsubscribe()` and `close()` and is awaited to
completion with no consumer lock held, so it may call `commit()`, `position()`
or `seek()`. The rebalance timeout (`max_poll_interval`) is the only bound.

A newly assigned partition starts at the group's committed offset, else at an
[initial offset](#starting-from-known-offsets), else at `auto_offset_reset` —
unless `on_partitions_assigned` seeks it.

## Offset Management

### Manual Commit

```rust,compile
use std::time::Duration;

let consumer = kafka
    .consumer("my-group")
    .enable_auto_commit(false)
    .build()
    .await?;

consumer.subscribe(["orders"]).await?;

loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    for record in &records {
        println!("processing {}@{}", record.partition, record.offset);
    }
    // Commit the positions of everything returned so far.
    if !records.is_empty() {
        consumer.commit().await?;
    }
}
```

`commit()` writes every assigned partition's position.

### Commit with Metadata

```rust,compile
use krafka::consumer::{OffsetAndMetadata, TopicPartition};

// Any iterator of (TopicPartition, OffsetAndMetadata) pairs, a `&HashMap` included.
let offsets = [
    (
        TopicPartition::new("orders", 0),
        OffsetAndMetadata::with_metadata(1500, "checkpoint-abc123"),
    ),
    (TopicPartition::new("orders", 1), OffsetAndMetadata::new(2000)),
];
consumer.commit_offsets(offsets).await?;
```

The offsets are sent as given, whether or not the partitions are still
assigned; the coordinator decides. No position moves.
`OffsetAndMetadata::leader_epoch` is committed when set. The metadata string
is visible in Kafka tooling.

### Position and Seeking

```rust,compile
use krafka::consumer::TopicPartition;

// `None` when the partition is not assigned.
let offset = consumer.position("orders", 0).await;
println!("Current position: {offset:?}");

consumer.seek("orders", 0, 1000).await?;

// Several partitions at once: all or none.
consumer
    .seek_many([(TopicPartition::new("orders", 0), 1_000), (TopicPartition::new("orders", 1), 2_000)])
    .await?;

consumer.seek_to_beginning("orders", 0).await?;
consumer.seek_to_end("orders", 0).await?;
```

- A seek applies only to a partition assigned to this consumer; otherwise it
  fails with `KrafkaError::IllegalState`. Seek a newly assigned partition from
  `on_partitions_assigned`.
- `seek_to_beginning` / `seek_to_end` are resolved with `ListOffsets` by the
  next `poll()`, or at once by `position()`.
- Every seek discards records already fetched for the partition, and the next
  commit writes the sought position. The leader epoch is cleared, so the new
  position is validated against the leader's log (KIP-320) before fetching.
- `seek_to_timestamp` and `offsets_for_times` look up offsets by timestamp.

### Starting from Known Offsets

`initial_offsets` sets start positions for partitions that have no committed
group offset, overriding `auto_offset_reset` for those partitions — for
pipelines that checkpoint positions externally:

```rust,compile
use krafka::consumer::TopicPartition;

let start = [(TopicPartition::new("orders", 0), 1_234), (TopicPartition::new("orders", 1), 5_678)];
let consumer = kafka
    .consumer("my-group")
    .initial_offsets(start)
    .build()
    .await?;
```

### Pause and Resume

```rust,compile
consumer.pause("orders", &[0, 1]).await;

let paused = consumer.paused_partitions().await;
println!("Paused partitions: {:?}", paused);

consumer.resume("orders", &[0, 1]).await;
```

A paused partition is not fetched and its buffered records are withheld from
`poll()`, `recv()` and `stream()` alike. Withheld records are delivered on
`resume()`, not re-fetched, and the position stays behind them. Pausing an
unassigned partition does nothing. Pause state survives rebalances for
partitions that stay assigned; `unsubscribe()` and `close()` clear it.

## Manual Partition Assignment

```rust,compile
use krafka::consumer::Consumer;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer_without_group()
    .auto_offset_reset(krafka::consumer::AutoOffsetReset::Earliest)
    .build()
    .await?;

consumer.assign("topic", vec![0, 1, 2]).await?;
```

`assign()` on a consumer with a group id is an error.

## Subscription Management

### Subscribing without a group

A consumer with no group id may still `subscribe()`; it then takes every
partition of every subscribed topic. `poll()` keeps that assignment current
from cluster metadata: new topics and added partitions are picked up, and a
deleted topic's partitions are dropped with their buffered records and
positions. Subscribing to a topic that does not exist yet is not an error. A
failed metadata refresh leaves the assignment unchanged.

`assign()` takes a topic out of this loop: after
`consumer.assign("orders", vec![0])` the caller owns that topic's partition
list, even if `subscribe()` named it.

### Subscribe, inspect, unsubscribe

`subscribe()` **replaces** the current subscription:

```rust,compile
consumer.subscribe(["orders", "payments"]).await?;

// Only "shipments" is subscribed now.
consumer.subscribe(["shipments"]).await?;

let topics = consumer.subscription().await;
let assignments = consumer.assignment().await;
println!("{topics:?} {assignments:?}");

consumer.unsubscribe().await?;
```

`unsubscribe()` revokes the partitions (calling the listener), leaves the
group and clears positions, paused partitions and buffered records. It returns
a leave-group error after the local state has been cleared.

## Error Handling

### Handling Poll Errors

`poll()` returns an empty `Vec` when its timeout elapses with no records; a
timeout is not an error.

```rust,compile
use krafka::consumer::Consumer;
use krafka::KrafkaError;
use std::time::Duration;

async fn consume_with_error_handling(consumer: &Consumer) -> krafka::Result<()> {
    loop {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                for record in records {
                    println!("{}@{}", record.partition, record.offset);
                }
            }
            // `close()` from another task, or `wakeup()`: stop polling.
            Err(KrafkaError::Closed { .. } | KrafkaError::Wakeup) => return Ok(()),
            Err(e) if e.is_retriable() => {
                eprintln!("retriable error, polling again: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

### Streaming with `recv()`

`recv()` returns the records `poll()` would, one at a time, in the same order:

- `Ok(Some(record))` — a record;
- `Ok(None)` — the consumer was closed;
- `Err(e)` — a broker or network error.

```rust,compile
use krafka::consumer::Consumer;

async fn consume_stream(consumer: &Consumer) -> krafka::Result<()> {
    while let Some(record) = consumer.recv().await? {
        println!(
            "topic={}, partition={}, offset={}",
            record.topic, record.partition, record.offset
        );
    }
    Ok(())
}
```

### Async `Stream` API

`stream()` returns a
[`futures_core::Stream`](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html)
of `Result<ConsumerRecord>` built on `recv()`; it ends when the consumer is
closed.

```rust,compile
use futures::StreamExt; // or tokio_stream::StreamExt
use krafka::consumer::Consumer;

async fn consume_with_stream(consumer: &Consumer) -> krafka::Result<()> {
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

### Interrupting a poll

`wakeup()` interrupts a waiting `poll()` or `recv()` from any task with
`KrafkaError::Wakeup`:

```rust,compile
use std::sync::Arc;

let consumer = Arc::new(kafka.consumer("my-group").build().await?);
consumer.subscribe(["orders"]).await?;

// Hand `stop` to whatever decides to shut down (a signal handler, ...).
let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
let handle = Arc::clone(&consumer);
tokio::spawn(async move {
    if stopped.await.is_ok() {
        handle.wakeup();
    }
});

loop {
    match consumer.poll(Duration::from_secs(30)).await {
        Ok(records) => {
            for record in records {
                println!("{}@{}", record.partition, record.offset);
            }
        }
        Err(KrafkaError::Wakeup) => break,
        Err(e) => return Err(e),
    }
}
consumer.close().await?;
```

A `wakeup()` before `poll()` is called interrupts the next one. Exactly one
call is interrupted; the consumer stays usable, and records the interrupted
poll fetched are returned by the next one.

### Graceful Shutdown

`close()` commits (with auto-commit), reports the partitions still assigned to
`on_partitions_revoked`, and leaves the group, within 30 s. A `recv()` waiting
in another task returns `Ok(None)`:

```rust,compile
use std::sync::Arc;

let consumer = Arc::new(kafka.consumer("my-group").build().await?);
consumer.subscribe(["orders"]).await?;

let worker = Arc::clone(&consumer);
let task = tokio::spawn(async move {
    while let Some(record) = worker.recv().await? {
        println!("{}@{}", record.partition, record.offset);
    }
    Ok::<(), KrafkaError>(())
});

// On shutdown (a signal handler, a channel, ...):
consumer.close().await?;
task.await.ok();
```

`close_with` chooses how to leave the group (KIP-1092):

```rust,compile
use krafka::{CloseOptions, GroupMembershipOperation};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .build()
    .await?;

// Shutting down for good: leave now, even as a static member.
consumer
    .close_with(
        CloseOptions::new().group_membership_operation(GroupMembershipOperation::LeaveGroup),
    )
    .await?;
```

| Option | Classic protocol | Consumer protocol (KIP-848) |
|---|---|---|
| `Default`, dynamic member | `LeaveGroup` | leave heartbeat, epoch −1 |
| `Default`, static member | nothing | leave heartbeat, epoch −2 |
| `LeaveGroup` | `LeaveGroup` (with the instance id) | leave heartbeat, epoch −1 |
| `RemainInGroup` | nothing | nothing |

A member that remains keeps its partitions until its session times out, so a
quick redeploy of a static member does not rebalance the group.

## Fetching

Each `poll()` sends one Fetch request per leader broker. Incremental fetch
sessions (KIP-227) are always on: after the first full fetch only changed and
removed partitions are sent. A session error resets it to a full fetch, and
sessions are closed on the broker on rebalance, `unsubscribe()` and `close()`.

### Closest-Replica Fetching (KIP-392)

Set `client_rack` to the consumer's rack or availability zone:

```rust,compile
let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .client_rack("us-east-1a")
    .build()
    .await?;
```

When the broker names a `preferred_read_replica` in the same rack, later
fetches for that partition go to it. The choice expires after
`metadata_max_age` (default 5 minutes) and is dropped on any error from the
replica, on rebalance and on unsubscribe; fetches then go to the leader.
Brokers need `broker.rack` and a `replica.selector.class`. Without
`client_rack`, every fetch goes to the leader.

## Static Group Membership (KIP-345)

A member with a `group_instance_id` keeps its identity across restarts: if it
returns within the session timeout it gets its partitions back without a
rebalance.

```rust,compile
use krafka::consumer::{Consumer, PartitionAssignmentStrategy};
use std::time::Duration;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .group_instance_id("pod-abc-123") // unique per instance
    .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
    .session_timeout(Duration::from_secs(300)) // cover a restart
    .build()
    .await?;
```

| Behavior | Dynamic (default) | Static |
|----------|-------------------|--------|
| Disconnect | Immediate rebalance | No rebalance until session timeout |
| Reconnect | New member, rebalance | Same member, no rebalance |
| Rolling restart | N rebalances | No rebalances |

`close()` keeps a static member's membership; see
[Graceful Shutdown](#graceful-shutdown) to leave anyway. A second live process
with the same instance id is fenced.

## KIP-848 Consumer Group Protocol

With `GroupProtocol::Consumer`, the coordinator assigns partitions and
`ConsumerGroupHeartbeat` replaces JoinGroup, SyncGroup and Heartbeat. A
background task heartbeats at the interval the coordinator returns (at least
1 s); an assignment it receives is applied on the next `poll()`, with
`on_partitions_revoked` for partitions moving away and
`on_partitions_assigned` for new ones, and the next heartbeat acknowledges
what the member owns. A changed `subscribe()` is sent with the next heartbeat.

Behaviour to know:

- **Fencing revokes everything.** `FENCED_MEMBER_EPOCH`, `UNKNOWN_MEMBER_ID`
  or `STALE_MEMBER_EPOCH` drops all partitions, reports them to
  `on_partitions_lost` (do not commit there) and rejoins at epoch 0.
- **Revocation is acknowledged.** The coordinator does not hand a partition to
  a new owner until the old one reports it released, so a slow
  `on_partitions_revoked` delays the rebalance instead of causing overlap.
- **Topic IDs.** Assignments name topics by ID, resolved from metadata
  (Metadata v10+). An ID that stays unresolved after a refresh fails the
  joining `poll()` rather than consuming a partial assignment.

The coordinator uses its default server-side assignor unless the member names
one; Apache Kafka brokers ship `uniform` and `range`:

```rust,compile
use krafka::consumer::GroupProtocol;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .group_protocol(GroupProtocol::Consumer)
    .group_remote_assignor("range")
    .build()
    .await?;
```

`build()` rejects `group_remote_assignor` with the classic protocol.

| Error | Handling |
|---|---|
| `FENCED_MEMBER_EPOCH`, `UNKNOWN_MEMBER_ID`, `STALE_MEMBER_EPOCH` | Partitions lost; the next `poll()` reports them to `on_partitions_lost` once and rejoins at epoch 0 with the same member ID |
| `NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE`, `COORDINATOR_LOAD_IN_PROGRESS`, `REBALANCE_IN_PROGRESS` | Coordinator rediscovered; the next heartbeat carries every field |
| `UNRELEASED_INSTANCE_ID` | Another live process uses the same `group_instance_id`; `poll()` fails with `KrafkaError::Fenced` |
| `UNSUPPORTED_ASSIGNOR` | `poll()` fails with a non-retriable error naming the `group_remote_assignor` |
| `GROUP_AUTHORIZATION_FAILED`, `INVALID_GROUP_ID`, `GROUP_MAX_SIZE_REACHED`, `UNSUPPORTED_VERSION`, `INVALID_REQUEST` | `poll()` fails with a non-retriable error |

Requirements:

- **Broker**: Kafka 4.0+, or 3.7–3.9 with `group.coordinator.new.enable=true`.
  Without `ConsumerGroupHeartbeat` the join fails with an error suggesting
  `GroupProtocol::Classic`.
- **Migrate a group together** on pre-4.0 brokers, where the two protocols
  cannot mix in one group.
- **Transactional offset commits** work on both protocols:
  `group_metadata()` carries the member epoch or the generation.
- Describe KIP-848 groups with the admin client's `describe_consumer_groups()`
  — see the [Admin Client Guide](@/docs/admin.md#consumer-groups).

## Consumer Interceptors

Interceptors observe records after they are fetched and offset commits. See the
[Interceptors Guide](@/docs/interceptors.md).

```rust,compile
use krafka::interceptor::{ConsumerInterceptor, InterceptorResult};
use krafka::consumer::ConsumerRecord;

#[derive(Debug)]
struct MetricsInterceptor;

impl ConsumerInterceptor for MetricsInterceptor {
    fn on_consume(&self, records: &[ConsumerRecord]) -> InterceptorResult {
        println!("Consumed {} records", records.len());
        Ok(())
    }
}

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .interceptor(MetricsInterceptor)
    .build()
    .await?;
```

## Compacted Topics

Offsets on compacted topics are exact: positions and commits are correct even
where compaction removed records from a batch.

### Tombstone Detection

A record with a key and a **null** value (`record.value == None`) is a
tombstone. A zero-length value is ordinary data. To write one, see
[Tombstones and Compacted Topics](@/docs/producer.md#tombstones-and-compacted-topics).

```rust,compile
use std::time::Duration;

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

`CompactedTable` keeps an in-memory key→value snapshot from consumer records
and reports each change as a `TableChange`. It is independent of the consumer,
so it works with any consumer setup:

```rust,compile
use krafka::consumer::{Consumer, CompactedTable};
use std::time::Duration;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("my-group")
    .build()
    .await?;
consumer.subscribe(["user-profiles"]).await?;

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

- A tombstone removes the key; keyless records are skipped.
- `get()` returns a `CompactedEntry` (value, offset, timestamp, partition);
  `get_value()` returns only the value. `contains_key()`, `keys()`, `values()`,
  `iter()`, `snapshot()`, `len()` and `is_empty()` are also available, and
  `&table` / `table` iterate.
- `ingest()` applies records without building a change list (for an initial
  scan); `clear()` empties the table and resets the counters.
- `records_processed()` and `tombstones_processed()` count what was applied.
- Tables compare equal when their entries do (counters are ignored);
  `TableChange` is `Eq` too.

### CompactedTopicConsumer

`CompactedTopicConsumer` combines a group-less consumer, assigned every
partition from the earliest offset, with a `CompactedTable`:

```rust,compile
use krafka::consumer::CompactedTopicConsumer;
use std::time::Duration;

let mut ctc = CompactedTopicConsumer::from_consumer_builder(
    kafka.consumer_without_group(),
    "user-profiles",
)
.await?;

// Build the initial snapshot.
ctc.scan(Duration::from_secs(1)).await?;
assert!(ctc.is_caught_up());

if let Some(entry) = ctc.table().get(b"user-123") {
    println!("{:?} at offset {} partition {} ts {}ms", entry.value, entry.offset, entry.partition, entry.timestamp_ms);
}

// Tail for live updates
loop {
    let changes = ctc.poll(Duration::from_secs(1)).await?;
    for change in &changes {
        println!("{:?} -> {:?}", change.key, change.new_value);
    }
}
```

`scan()` snapshots the high watermarks first and returns once every partition
reaches its snapshot, so writes during the scan do not prolong it. It gives up
after 5 minutes with an error naming each partition still behind;
`scan_with_timeout()` chooses the bound. `table()` / `table_mut()` and
`consumer()` / `consumer_mut()` expose the parts, and `into_parts()` splits
them.

To configure the consumer yourself, build it and pass it in:

```rust,compile
use krafka::consumer::CompactedTopicConsumer;
use std::time::Duration;

let consumer = kafka
    .consumer_without_group()
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .enable_auto_commit(false)
    .build()
    .await?;
consumer.assign("config-topic", vec![0, 1, 2]).await?;

let mut ctc = CompactedTopicConsumer::from_consumer(consumer, "config-topic");
ctc.scan(Duration::from_secs(1)).await?;
```

## Offset Lag Tracking

Lag is computed from the end offsets every fetch response carries; it costs no
extra requests. The end offset depends on the isolation level:

| Isolation level | Lag measured against |
|---|---|
| `ReadUncommitted` (default) | high watermark |
| `ReadCommitted` | last stable offset (LSO) — records at or above it are not deliverable |

```rust,compile
for (tp, lag) in consumer.lag().await {
    // `None` until a fetch has reported both ends.
    println!("{tp:?}: {:?} records behind (stale: {})", lag.lag, lag.stale);

    let hw = lag.high_watermark;         // log end, including open transactions
    let lso = lag.last_stable_offset;    // first offset of an open transaction
    let start = lag.log_start_offset;    // earliest offset still in the log
    let _ = (hw, lso, start);
}
```

The gap between `high_watermark` and `last_stable_offset` is the data of open
transactions on the partition. Lag is a `u64`, clamped at zero.

The `lag` (sum) and `lag_max` (largest partition) metrics follow the same
rule. End offsets update only when a fetch response arrives, so a paused or
idle consumer's lag goes stale; `stale` is set when they are older than
`lag_staleness_threshold` (default 60 s). `fetch_watermarks()` asks the
brokers directly.

## Reading a group's committed offsets

`committed()` asks the group coordinator where the *group* is;
`position()` reports where *this consumer* reads next:

```rust,compile
let committed = consumer.committed(&[("orders", 0), ("orders", 1)]).await?;
for ((topic, partition), pos) in &committed {
    println!("{topic}-{partition} committed at {}", pos.offset);
}
```

A partition the group has never committed is absent from the map, not `0`.
`committed()` requires a group id.

## Next Steps

- [Interceptors Guide](@/docs/interceptors.md) - Producer and consumer interceptor hooks
- [Producer Guide](@/docs/producer.md) - Learn about producing messages
- [Configuration Reference](@/docs/configuration.md) - All consumer options
