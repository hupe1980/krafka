+++
title = "Producer"
description = "Batching, compression, partitioning, idempotence and exactly-once transactions."
weight = 30

[extra]
slug_id = "producer"
+++

## Overview

The producer is idempotent by default, batches per partition, compresses with
gzip, snappy, lz4 or zstd, partitions keys as the Java client does, and
supports exactly-once transactions, typed serializers, interceptors, metrics
and `tracing` spans.

## Basic Usage

```rust,compile
use krafka::producer::Producer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = krafka::Kafka::builder("localhost:9092")
        .connect()
        .await?
        .producer()
        .build()
        .await?;

    // Simple send
    producer.send(krafka::Record::new("topic", "value")).await?;

    // Send with key (for partitioning)
    producer.send(krafka::Record::new("topic", "value").key("key")).await?;

    producer.close().await?;
    Ok(())
}
```

Security (TLS, SASL, AWS MSK IAM) is set once on the `Kafka` builder and applies
to every producer built from it — see the
[Authentication Guide](@/docs/authentication.md).

## Producer Configuration

### Acknowledgments

| `acks` | Waits for | Notes |
|---|---|---|
| `Acks::All` (default) | every in-sync replica | required by idempotence |
| `Acks::Leader` | the partition leader | needs `.idempotent(false)` |
| `Acks::None` | nothing | needs `.idempotent(false)`; no delivery confirmation and no quota feedback |

```rust,compile
use krafka::producer::{Producer, Acks};

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .acks(Acks::Leader)
    .idempotent(false) // idempotence requires Acks::All
    .build()
    .await?;
```

With `Acks::None` the broker sends no response, so the producer never sees a
`throttle_time_ms` (KIP-219) and keeps writing at full rate until the broker
mutes the connection. Prefer `Acks::Leader` unless you measured the difference.

### Compression

Choose the right compression codec for your workload:

```rust,compile
use krafka::producer::Producer;
use krafka::Compression;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .compression(Compression::Lz4)
    .build()
    .await?;
```

| Codec | Cargo feature | Speed | Ratio |
|-------|---------------|-------|-------|
| None (default) | — | — | 1:1 |
| Gzip | always on | slow | high |
| Snappy | always on | fast | good |
| LZ4 | always on | fastest | good |
| Zstd | `zstd` (encode only) | medium | high |

Encoding Zstd needs the `zstd` feature, which compiles libzstd (C);
selecting `Compression::Zstd` without it fails at `build()`. Every codec,
Zstd included, decodes in pure Rust in every build.

```sh
cargo add krafka --features zstd
```

#### Compression level

`Gzip` and `Zstd` accept a level; Snappy and LZ4 take none.

```rust,compile
let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .compression(Compression::Zstd)
    .compression_level(Some(1))
    .build()
    .await?;
```

| Codec | Range | Default |
|-------|-------|---------|
| Gzip | 0–9 | 6 |
| Zstd | what the linked libzstd reports — negative "fast" levels through 22 | 3 |
| Snappy, LZ4 | takes no level | — |

A level on a codec that takes none, or outside the codec's range, is rejected
at `build()` (and `build_transactional`); per-topic codec overrides are checked
against it too. Higher zstd levels are not reliably smaller and cost much more
CPU above about 9 — measure against your own payloads.

### Batching

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .batch_size(65536)                  // bytes per partition batch (default 16384, ≥ 1)
    .linger(Duration::from_millis(5))   // default 5 ms
    .max_request_size(1024 * 1024)      // default 100 MiB, ≥ batch_size
    .build()
    .await?;
```

Records are accumulated per partition. A batch is sent when it reaches
`batch_size`, when its `linger` window expires, when `flush()`, `close()` or a
transaction commit seals it, or — with `linger = 0` — as soon as the partition
has no batch in flight. `linger` bounds how long a batch may wait for more
records; it never turns batching off.

### One request per broker, one batch per partition

Each partition has exactly one batch in flight, sent in order, so a retry
cannot reorder a partition. Ready batches for all partitions a broker leads
travel in one Produce request (at most 5 in flight per broker), up to
`max_request_size`. A request that would exceed `max_request_size` fails
locally before any I/O.

### Memory Backpressure

The producer limits memory usage to prevent unbounded growth under high load:

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .buffer_memory(64 * 1024 * 1024) // 64MB buffer limit
    .max_block(Duration::from_secs(30))
    .build()
    .await?;
```

| Option | Default | Description |
|--------|---------|-------------|
| `buffer_memory` | 32 MiB | Maximum total memory for buffering records; must be ≥ 1 |
| `max_block` | 60 s | Total time `send()`/`enqueue()` may wait: resolving the topic plus waiting for buffer memory |

A record holds its share of `buffer_memory` until it has an outcome; when the
budget is exhausted, `enqueue()` waits (Java `max.block.ms`). An `enqueue()`
future dropped while waiting queued nothing. A `send()` future dropped after
the record was queued does not cancel delivery — sending the record again may
write it twice.

## Flushing

`flush()` sends everything queued before the call without waiting for
`linger`, and returns when each of those records has its outcome:

```rust,compile
for i in 0..100 {
    producer.enqueue(krafka::producer::Record::new("topic", format!("v{i}"))).await?;
}
producer.flush().await?;
producer.close().await?;
```

It covers exactly the sends queued before it: a send queued afterwards neither
holds it up nor, by completing first, ends it early. It never blocks other
sends — partitions on healthy brokers keep flowing while a flush waits on a
slow one. It is cancel safe: dropping it stops the wait, not the sends.

## Tombstones and Compacted Topics

On a `cleanup.policy=compact` topic, a record with a **null value** is a
*tombstone*: it marks its key for deletion. A zero-length value is not null and
does not delete. Values are `Option<Bytes>` on both sides.

```rust,compile
use krafka::producer::Record;

// A tombstone needs a key: a null value on a keyless record deletes nothing.
producer
    .send(Record::tombstone("users", "user-42"))
    .await?;

// A zero-length value is NOT a tombstone — compaction keeps this record.
producer.send(krafka::Record::new("users", "").key("user-42")).await?;
```

`without_value()` turns an existing record into one, keeping its key and
headers; `value(..)` sets a value again. `is_tombstone()` reports whether a
record has a key and no value, using the same rule as
`ConsumerRecord::is_tombstone()`.

`TypedProducer` does not serialize a `None` value, so a typed tombstone stays
null. A tombstone must land in the same partition as the records it retires;
with the default partitioner the same key is enough.

### Null header values

Header values carry the same distinction, as `Vec<(String, Option<Bytes>)>`:

```rust,compile
use krafka::producer::Record;

let record = Record::new("events", b"payload".to_vec())
    .header("X-Source", &b"api"[..])   // an ordinary header value
    .null_header("X-Flag");            // null, not zero-length
```

On the read side, `ConsumerRecord::is_tombstone()` classifies a record and
[`CompactedTable`](@/docs/consumer.md) applies the semantics — the key is
removed from the table and reported as a `TableChange` with `is_delete()`.

## Partitioning

### Default partitioning

Without a `.partitioner(..)`, the producer partitions as Java's built-in
partitioner does (KIP-794):

- a keyed record goes to `murmur2(key) mod partitions`, the partition a Java
  producer picks;
- keyless records stick to one partition until at least `batch_size` bytes
  were routed to it, then switch to a different partition chosen at random —
  at any `linger`, however batches are sealed.

```rust,compile
// Messages with the same key go to the same partition
producer.send(krafka::Record::new("topic", "event1").key("user-123")).await?;
producer.send(krafka::Record::new("topic", "event2").key("user-123")).await?;  // Same partition

// Keyless messages stay on one partition for about `batch_size` bytes
producer.send(krafka::Record::new("topic", "event")).await?;
```

The keyless switch is uniform: it is not weighted by broker queue size and does
not avoid slow brokers (Java's `partitioner.adaptive.partitioning.enable` and
`partitioner.availability.timeout.ms` have no equivalent).

### Rack-aware partitioning (KIP-1123)

With `client_rack` set and `partitioner_rack_aware(true)`, a keyless switch
chooses only partitions whose current leader is in the client's rack, which
keeps produce traffic in one availability zone. When no partition of the topic
is led from that rack, keyless records spread over all partitions. Keyed
records always follow their key's hash.

```rust,compile
use krafka::producer::Producer;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .client_rack("eu-west-1a")
    .partitioner_rack_aware(true)
    .build()
    .await?;
```

Rack-aware partitioning without a `client_rack`, or together with a custom
partitioner, is a configuration error.

### Custom Partitioners

`RoundRobinPartitioner` is the one built-in alternative:

```rust,compile
use krafka::producer::{Producer, RoundRobinPartitioner};

// Round-robin: ignores keys, distributes evenly
let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .partitioner(RoundRobinPartitioner::new())
    .build()
    .await?;
```

A custom partitioner sees only topic, key and partition count. A partition it
returns outside `[0, partition_count)` fails the send with
`KrafkaError::Config` before the record is queued.

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

Topic metadata idle for longer than `metadata_topic_cache_ttl` (default
5 minutes, Java `metadata.max.idle.ms`) is evicted; producing to a topic or
refreshing it resets the timer. `None` disables eviction.

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = krafka::Kafka::builder("localhost:9092")
    .metadata_topic_cache_ttl(Some(Duration::from_secs(600)))
    .connect()
    .await?
    .producer()
    .build()
    .await?;
```

## Topic Resolution

`send()` to a topic the cache does not hold fetches its metadata, retrying until it resolves or the [`max_block`](#memory-backpressure) budget expires.

A topic the cluster will not resolve within `max_block` fails with the broker's own reason:

```rust,compile
use krafka::error::{ErrorCode, KrafkaError};
use krafka::producer::Producer;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .build()
    .await?;

match producer.send(krafka::Record::new("maybe-missing", "v")).await {
    Ok(metadata) => println!("wrote to partition {}", metadata.partition),
    Err(KrafkaError::Broker { code: ErrorCode::TopicAuthorizationFailed, .. }) => {
        // The topic exists; this principal may not write to it.
    }
    Err(KrafkaError::Broker { code: ErrorCode::UnknownTopicOrPartition, .. }) => {
        // The cluster does not have this topic.
    }
    Err(e) => eprintln!("send failed: {e}"),
}
```

[`partitions_for`](https://docs.rs/krafka/latest/krafka/producer/struct.Producer.html#method.partitions_for) inspects a topic directly, fetching on a cache miss under the same budget:

```rust,compile
use krafka::producer::Producer;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .build()
    .await?;

for partition in producer.partitions_for("events").await? {
    println!("partition {} led by {}", partition.partition, partition.leader);
}
```

An explicit partition outside the topic's range is rejected by `send()`.

### Letting the broker create the topic

`allow_auto_create_topics` sets `allow.auto.create.topics` on the metadata requests the send path issues, so the broker creates a missing topic on demand:

```rust,compile
use krafka::producer::Producer;

let producer = krafka::Kafka::builder("localhost:9092")
    .allow_auto_create_topics(true)
    .connect()
    .await?
    .producer()
    .build()
    .await?;
```

The broker must also run with `auto.create.topics.enable=true`. The flag is **off by default** (the Java producer always asks), so a misspelled topic name fails instead of creating a topic. It is set on the `Kafka` builder and applies to every client built from the handle.

## Error Handling

### Record Validation

An empty topic name, a topic name over 32,767 bytes, a key, value or header
over `i32::MAX` bytes, or more than 10,000 headers fails the send with
`KrafkaError::Protocol` before the record is queued.

### Retries and delivery timeout

Retriable failures (`NOT_LEADER_OR_FOLLOWER`, `NOT_ENOUGH_REPLICAS`, a timeout,
a lost connection) are retried with the batch's original sequence numbers, so
the broker de-duplicates a retry of a batch it already appended. Backoff starts
at `retry_backoff` (default 100 ms), doubles per attempt with ±20 % jitter,
and is capped at **1 s**. There is no retry count: `delivery_timeout` (Java
`delivery.timeout.ms`, default 120 s) bounds the time from a batch's creation
to its records' outcome, and every record resolves by then.

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .retry_backoff(Duration::from_millis(100))
    .delivery_timeout(Duration::from_secs(120))
    .build()
    .await?;
```

It must be at least `linger + request_timeout`; a smaller value is a
configuration error, as in Java.

A record that times out fails with
`KrafkaError::DeliveryTimeout { possibly_written, .. }`. `possibly_written` is
`false` only when its batch never reached a connection, or every attempt that
did was answered with an error that proves it was not appended. A timeout, a
lost connection, `NOT_ENOUGH_REPLICAS_AFTER_APPEND` or `REQUEST_TIMED_OUT`
leave it `true`: the broker may hold the record, so resending it yourself may
duplicate it.

### Resending after a failure

The producer has already retried every retriable failure when `send()`
returns, so a resend is a decision about duplicates, not a retry loop:

```rust,compile
use krafka::Record;
use krafka::error::KrafkaError;
use krafka::producer::Producer;

async fn send_once_more(producer: &Producer, record: Record) -> krafka::Result<()> {
    match producer.send(record.clone()).await {
        Ok(_) => Ok(()),
        // Never written: a resend cannot duplicate it.
        Err(KrafkaError::DeliveryTimeout { possibly_written: false, .. }) => {
            producer.send(record).await.map(|_| ())
        }
        // Possibly written: a resend may duplicate it. Keep the error, or
        // resend only if the consumer de-duplicates.
        Err(e) => Err(e),
    }
}
```

## Idempotence

The producer is idempotent by default (KIP-679): every batch carries
`(producer id, epoch, base sequence)`, unchanged across retries, so an
acknowledged record is written exactly once.

- A batch that fails for good moves the producer to the next epoch (KIP-360)
  once nothing stamped under the old one is unresolved.
- `OUT_OF_ORDER_SEQUENCE_NUMBER` (the broker lost an earlier batch) fails the
  batch with the non-fatal `KrafkaError::OutOfOrderSequence`, bumps the epoch
  and increments the `data_loss_detected` metric.
- `UNKNOWN_PRODUCER_ID` after retention removed the producer's state bumps the
  epoch and resends; otherwise it is handled as an out-of-order sequence.
- `DUPLICATE_SEQUENCE_NUMBER` is success.

For exactly-once across producer restarts, use `TransactionalProducer`.
In-flight limits are fixed (one batch per partition, 5 requests per broker);
the connection-level [`max_in_flight_requests`](@/docs/configuration.md)
applies underneath. See [Performance](@/docs/performance.md) for tuning.

## Graceful Shutdown

`close()` refuses new sends, sends everything queued, waits for every record's
outcome and closes the interceptors. Calling `close()` more than once is a
no-op:

```rust,compile
use krafka::producer::Producer;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .build()
    .await?;

// ... send messages ...

producer.close().await?;
```

`close_with(CloseOptions::new().timeout(..))` bounds the wait; records still
without an outcome then fail with `KrafkaError::Closed`:

```rust,compile
use krafka::CloseOptions;
use std::time::Duration;

producer
    .close_with(CloseOptions::new().timeout(Duration::from_secs(10)))
    .await?;
```

A producer dropped without `close()`, or a `close()` future dropped after its
first poll, still delivers what is buffered in the background, but nothing
waits for it. Bound the wait with `close_with`, not by dropping the future.

## Transactional Producer

`TransactionalProducer` writes records across partitions and topics — and
consumer offsets — atomically. Building it registers the transactional id and
fences any earlier instance with the same id.

### Basic Usage

```rust,compile
use krafka::producer::TransactionalProducer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = krafka::Kafka::builder("localhost:9092")
        .connect()
        .await?
        .producer()
        .build_transactional("my-unique-transaction-id")
        .await?;

    producer.begin()?;
    producer.send(krafka::Record::new("topic-a", "value1").key("key1")).await?;
    producer.send(krafka::Record::new("topic-b", "value2").key("key2")).await?;

    producer.commit().await?;

    Ok(())
}
```

### Configuration

A transactional producer is built from the same `ProducerBuilder` as a plain
one — `kafka.producer()…build_transactional(id)` — so every producer setter
applies: compression and compression levels, delivery timeout, interceptors,
partitioning (including `client_rack` and `partitioner_rack_aware`). Two
setters only take effect there: `transaction_timeout` and `two_phase_commit`.

```rust,compile
use krafka::Compression;
use std::time::Duration;

let producer = kafka
    .producer()
    .transaction_timeout(Duration::from_secs(60)) // coordinator's deadline
    .delivery_timeout(Duration::from_secs(45)) // bound on one batch in flight
    .compression(Compression::Zstd)
    .compression_level(Some(1))
    .build_transactional("order-processor-1")
    .await?;
```

Two settings are **fixed** by the transactional protocol; `build_transactional`
rejects a builder that changed them:

| Setting | Fixed to |
|---|---|
| `acks` | `Acks::All` |
| `idempotent` | `true` |

`transaction_timeout` defaults to 60 s. Keep `delivery_timeout` at or below it:
a retrying batch holds the transaction open (blocking `read_committed`
consumers), and the coordinator aborts at `transaction_timeout` anyway.
`build_transactional` logs a warning when `delivery_timeout` is larger.

`commit()` flushes by itself. `flush()` mid-transaction surfaces send failures
early; it does not make records visible — only `commit()` does.

### Transaction Lifecycle

`build_transactional(id)` → `begin()` → `send()`/`enqueue()` → `commit()` or
`abort()` → … → `close()`.

```rust,compile
use krafka::producer::TransactionalProducer;

async fn do_work(producer: &TransactionalProducer) -> krafka::Result<()> {
    producer.send(krafka::Record::new("orders", "created")).await?;
    Ok(())
}

let producer = kafka.producer().build_transactional("orders-writer").await?;
producer.begin()?;

match do_work(&producer).await {
    Ok(()) => producer.commit().await?,
    Err(e) => {
        producer.abort().await?;
        return Err(e);
    }
}

producer.close().await?;
```

- **Commit drains.** A send belongs to the transaction once it is queued.
  `commit()` refuses new sends and `send_offsets` calls (`Committing` state),
  sends what is buffered, waits for every queued send and any in-flight
  `send_offsets`, then sends `EndTxn`.
- **A failed send fails the transaction.** If any send of the transaction
  failed, awaited or not, later sends and `commit()` return
  `KrafkaError::TransactionAbortable` carrying the first failure. Abort and
  start again.
- **Abort drops buffered records.** `abort()` fails buffered records with
  `TransactionAbortable`, waits for those already on the wire, then sends
  `EndTxn(abort)`.
- **Commit outcome unknown.** If any `EndTxn(commit)` attempt went unanswered,
  the coordinator may have committed, and aborting could tear a later
  transaction ([KAFKA-17754](https://issues.apache.org/jira/browse/KAFKA-17754)).
  The producer enters `TransactionState::CommitUnknown`: `abort()` returns an
  error without sending anything, `close()` leaves the transaction to the
  coordinator, and `commit()` may be retried (it is idempotent). Under TV1 the
  producer also bumps its epoch before the next transaction, fencing a late
  `EndTxn`. A commit whose every attempt was answered with an error returns to
  `Open`, where aborting is safe.
- **Cancellation.** `commit()` and `abort()` are not cancel safe but leave a
  state to continue from: call the same method again. A `send_offsets()`
  dropped after it started makes the transaction abortable.

### Graceful Shutdown (Transactional)

`close()` refuses new sends, aborts an open transaction, leaves a
`CommitUnknown` or `Prepared` one to the coordinator, and closes the
interceptors. It is idempotent; later calls on the producer fail with
`KrafkaError::Closed`. `close_with(CloseOptions::new().timeout(..))` bounds it.

### Retries and fencing

Sends retry as on the plain producer, bounded by `delivery_timeout`. A batch
that fails for good makes the transaction abortable. The coordinator RPCs (`InitProducerId`, `AddPartitionsToTxn`,
`AddOffsetsToTxn`, `TxnOffsetCommit`, `EndTxn`) retry until `max_block`, with
the same backoff (from `retry_backoff`, capped at 1 s); there is no retry
count.

- On `NotCoordinator`, `CoordinatorNotAvailable`, `CoordinatorLoadInProgress`,
  a timeout or a lost connection, the cached coordinator is dropped and
  re-discovered before the next attempt.
- Fatal errors are never retried. A fenced producer (`ProducerFenced`,
  `InvalidProducerEpoch`, `TransactionCoordinatorFenced`) reports
  `KrafkaError::Fenced` and moves to `TransactionState::Fatal`.

### Transaction version

krafka negotiates one transaction protocol level for the cluster:

| Level | `transaction.version` | What changes |
|---|---|---|
| `TV1` | 0 or 1 | Classic. `AddPartitionsToTxn` per partition; the epoch bumps only on `InitProducerId` |
| `TV2` | 2 | KIP-890. Partitions register implicitly via `Produce`; the epoch bumps on every `EndTxn` |
| `TV3` | 3 | KIP-939. Everything TV2 does, plus the coordinator honours `enable2Pc` |

A level is used only when the finalized feature is set **and** every broker
serves the API versions it needs (`Produce`, `TxnOffsetCommit`, `EndTxn` for
TV2; `InitProducerId` v6 for TV3), so one lagging broker holds the cluster at
the lower level. `two_phase_commit(true)` below TV3 fails at
`build_transactional` with a message naming the feature level, API version and
ACL required.

### Timestamps

Each record keeps its own timestamp: `Record::timestamp`, or the send time
when unset.

```rust,compile
use krafka::producer::Record;

let record = Record::new("my-topic", b"value".to_vec()).timestamp(1700000000000);
producer.send(record).await?;
```

### Consume-Transform-Produce (Exactly-Once)

```rust,compile
use krafka::Record;
use krafka::producer::TopicPartitionOffset;

let producer = kafka.producer().build_transactional("enricher-1").await?;
let records = consumer.poll(Duration::from_secs(1)).await?;

// Commit consumer offsets atomically with produce
producer.begin()?;

// Process records and produce output
let mut offsets = Vec::new();
for record in &records {
    let mut output = Record::new("output-topic", record.value.clone().unwrap_or_default());
    output.key = record.key.clone();
    producer.send(output).await?;
    // The offset of the NEXT record to consume.
    offsets.push(TopicPartitionOffset::new(&*record.topic, record.partition, record.offset + 1));
}

// KIP-447: pass the consumer's live group metadata so the group coordinator
// can fence a zombie committer. Re-read it every transaction — the generation
// changes on every rebalance.
let Some(group_metadata) = consumer.group_metadata().await else {
    producer.abort().await?;
    return Ok(());
};
producer.send_offsets(&offsets, &group_metadata).await?;

// Atomic commit of messages and offsets
producer.commit().await?;
```

After an abort, the consumer's position is already past the aborted records.
Seek each partition back to its committed offset (`consumer.committed(..)`)
before the next poll, or those records are skipped. See the
[Cookbook](@/docs/cookbook.md#exactly-once-consume-transform-produce).

### Transaction States

| State | Description |
|-------|-------------|
| `Uninitialized` | Before `build_transactional` registered the transactional id |
| `Initializing` | `build_transactional` is registering the transactional id |
| `Ready` | Ready to begin a new transaction |
| `Open` | A transaction accepts sends |
| `Committing` | `commit()` is running; sends are refused |
| `CommitUnknown` | A commit attempt went unanswered; only another commit is allowed |
| `Aborting` | `abort()` is running |
| `Prepared` | Prepared under two-phase commit; awaiting an external decision |
| `Fatal` | Unrecoverable error, producer must be recreated |

### Two-phase commit (KIP-939)

*Requires the `unstable-protocol` feature (`InitProducerId` v6), broker
`transaction.version` 3, and both `WRITE` and `TWO_PHASE_COMMIT` on the
transactional-id resource.*

Two-phase commit lets an external coordinator (a database, an XA manager)
own the commit decision while Kafka holds the transaction prepared.
`two_phase_commit(true)` sends `enable2Pc` on `InitProducerId`, so the broker
never times these transactions out.

```rust,ignore
use krafka::producer::{PreparedTxnState, TransactionOutcome, TransactionalProducer};

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .two_phase_commit(true)
    .build_transactional("orders-sink")
    .await?;

producer.begin()?;
producer.send(krafka::Record::new("orders", "...")).await?;

// Prepare: flush everything, then stop accepting records. Sends no request —
// the prepare *is* the flush, and the coordinator was already told to hold.
let prepared: PreparedTxnState = producer.prepare().await?;

// Store it in the SAME external transaction as the rest of your work.
db.execute("INSERT INTO kafka_prepared (id, state) VALUES ($1, $2)",
           &[&"orders-sink", &prepared.to_string()])?;
db.commit()?;

producer.commit().await?;
```

If the process dies between the prepare and the database commit, the
replacement compares what the coordinator holds with the stored state:

```rust,ignore
let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .two_phase_commit(true)
    .build_transactional("orders-sink")
    .await?;

// With two_phase_commit, build_transactional keeps what the previous
// incarnation left prepared instead of aborting it.
if let Some(_ongoing) = producer.prepared_transaction() {
    let stored: PreparedTxnState = db
        .query_one("SELECT state FROM kafka_prepared WHERE id = $1", &[&"orders-sink"])?
        .get::<_, String>(0)
        .parse()?;

    match producer.complete(stored).await? {
        // The stored state names the transaction still open: the prepare was
        // durably recorded, so the external side committed and this must match.
        TransactionOutcome::Committed => println!("recovered and committed"),
        // It names an older one: the prepare never got recorded, the external
        // side rolled back, and this must abort. A *normal* outcome of a crash
        // in the window, not an error.
        TransactionOutcome::Aborted => println!("recovered and aborted"),
    }
}
```

`PreparedTxnState` renders as `producer_id:epoch` through `Display` and parses
back through `FromStr`, so storing it needs no bespoke serialisation.

> **A prepared transaction with no stored state never times out** and blocks
> `read_committed` consumers on its partitions until an operator resolves it.
> Store the state durably before reporting the prepare as successful.

## Typed keys and values

`TypedProducer<K, V>` serializes typed keys and values with a
`Serializer<K>` and a `Serializer<V>` before the record enters the producer.
A serializer is synchronous and may add headers — where a schema registry
puts its schema id. `BytesSerializer` passes byte values through,
`StringSerializer` encodes strings as UTF-8, and `NoKey` is the key serializer
of a producer that never sends keys. The key is serialized first, then the
value; the interceptors see the encoded record. A `None` key or value is not
serialized, so a `None` value is a tombstone. A serializer error fails the send
with `KrafkaError::Serialization` before anything is reserved or queued.
`producer()` reaches the producer underneath for `flush` and `metrics`.

```rust,compile
use bytes::Bytes;
use krafka::Headers;
use krafka::producer::{Producer, TypedProducer};
use krafka::serdes::{Serializer, StringSerializer};

/// Big-endian `u64`, tagged with a content type.
struct BigEndian;

impl Serializer<u64> for BigEndian {
    fn serialize(
        &self,
        _topic: &str,
        headers: &mut Headers,
        value: &u64,
    ) -> krafka::Result<Bytes> {
        headers.push(("content-type".into(), Some(Bytes::from_static(b"u64-be"))));
        Ok(Bytes::copy_from_slice(&value.to_be_bytes()))
    }
}

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .build()
    .await?;
let counters: TypedProducer<str, u64> = TypedProducer::new(producer, StringSerializer, BigEndian);
counters.send("counters", Some("page-views"), Some(&42)).await?;
counters.close().await?;
```

## Producer Interceptors

Interceptors allow you to observe and modify records before they are sent, and
observe the outcome after a send completes: `on_acknowledgement` receives
`Result<&RecordMetadata, &KrafkaError>` exactly once per record, and both
producers call `close()` once. A panic in `on_send` fails that record's send
(it is not produced); an `Err` is logged and the chain continues. Each record
carries a `RecordContext` from one hook to the other, so an interceptor can hold
a span or a timer across the send — see the
[Interceptors Guide](@/docs/interceptors.md) for full details.

```rust,compile
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::Headers;
use krafka::producer::{Producer, Record, RecordMetadata};
use krafka::error::KrafkaError;

#[derive(Debug)]
struct AuditInterceptor;

impl ProducerInterceptor for AuditInterceptor {
    fn on_send(&self, record: &mut Record, _ctx: &mut RecordContext) -> InterceptorResult {
        // Add a tracing header to every record
        record.headers.push(("x-trace-id".to_string(), Some(b"abc123".to_vec().into())));
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        topic: &str,
        partition: i32,
        result: Result<&RecordMetadata, &KrafkaError>,
        _headers: &Headers,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
        match result {
            Ok(metadata) => println!("Sent to {topic}:{partition} at {}", metadata.offset),
            Err(err) => eprintln!("Send to {topic}:{partition} failed: {err}"),
        }
        Ok(())
    }
}

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .interceptor(AuditInterceptor)
    .build()
    .await?;
```

## Failed sends and dead-letter topics

A send that fails returns its error to the caller; the producer does not
reroute records anywhere. `send` and `enqueue` take the record by value, so
clone it first if you want to write it somewhere else on failure. To
dead-letter a *consumed* record that cannot be processed, build the record with
`krafka::dlq::record_for` and send it with an ordinary producer — see
[Dead Letter Queue](@/docs/errors.md#dead-letter-queue).

## Metrics and telemetry

`producer.metrics()` returns an owned `krafka::metrics::Metrics` snapshot;
`prometheus_text()` renders it. Each record's send is a `tracing` span. The
producer pushes its metrics to the brokers when the cluster subscribes to them
(KIP-714); `metrics_push(false)` turns that off. See [Metrics](@/docs/metrics.md).

```rust,compile
let producer = kafka.producer().metrics_push(false).build().await?;
let snapshot = producer.metrics();
println!("{}", snapshot.prometheus_text());
```

## Next Steps

- [Interceptors Guide](@/docs/interceptors.md) - Producer and consumer interceptor hooks
- [Consumer Guide](@/docs/consumer.md) - Learn about consuming messages
- [Configuration Reference](@/docs/configuration.md) - All producer options
