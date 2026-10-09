+++
title = "Share Consumer"
description = "Queue-like consumption with KIP-932 share groups: per-record acknowledgement without partition ownership."
weight = 50

[extra]
slug_id = "share-consumer"
+++

Share groups ([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)) give Kafka queue-like semantics: records are acknowledged individually and a partition is not owned by one member.

Always compiled in. Needs a **Kafka 4.2+** broker (as of 2026-10-09, share groups are production ready from Apache Kafka 4.2). Against an older broker, or one without share groups such as Redpanda, calls fail with `UnknownApiVersion`.

## Overview

| Feature | Consumer Group | Share Group |
|---|---|---|
| Assignment | Client or server-side | Server-side only |
| Offset tracking | Per-partition committed offsets | Per-record acknowledgements |
| Delivery | Exactly-once (with transactions) | At-least-once |
| Record sharing | One consumer per partition | Multiple consumers per partition |
| Redelivery | Seek / reset offsets | Automatic (release/reject) |

Members of a share group receive **non-overlapping subsets of records** from the same partition; the broker tracks assignment and delivery.

## Basic Usage

```rust,compile
use krafka::share_consumer::ShareConsumer;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .share_consumer("my-share-group")
    .build()
    .await?;
consumer.subscribe(["events"]).await?;

// `recv()` returns `Ok(None)` only once the consumer is closed.
while let Some(record) = consumer.recv().await? {
    println!("{}-{}@{}", record.topic, record.partition, record.offset);
    // Implicit mode (default): accepted when the next recv()/poll() starts.
}
```

`subscribe()` joins the share group before it returns. `poll(timeout)`
returns up to `max_poll_records` records instead of one.
`stream()` wraps `recv()` as a `Stream` that ends when the consumer closes.

## Acknowledgement Modes

### Implicit (default)

The records a `poll()`/`recv()` returned are accepted when the next
`poll()`/`recv()` starts, and by `commit()` and `close()`. No acknowledgement
calls are needed.

### Explicit

The application settles every delivered record with `ack` (accept), `release`
(redeliver) or `reject` (archive, never redeliver). Every record the previous
`poll()` returned must be settled before the next `poll()`, which otherwise
fails with `IllegalState`.

```rust,compile
use krafka::share_consumer::{AcknowledgementMode, ShareConsumer};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .share_consumer("my-share-group")
    .acknowledgement_mode(AcknowledgementMode::Explicit)
    .build()
    .await?;
consumer.subscribe(["events"]).await?;

for record in consumer.poll(Duration::from_secs(1)).await? {
    if record.value.is_some() {
        consumer.ack(&record)?;
    } else {
        consumer.reject(&record)?;
    }
}
for (partition, result) in consumer.commit().await? {
    if let Err(error) = result {
        eprintln!("{}-{}: {error}", partition.topic, partition.partition);
    }
}
```

### Where acknowledgements go

An acknowledgement rides on the next `ShareFetch` to the broker that
**acquired** the record, or goes in a `ShareAcknowledge` when no fetch is due
or `commit()` asks for it. It is never sent to another broker: if the
acquiring broker no longer leads the partition, it fails with
`NOT_LEADER_OR_FOLLOWER`.

Failed acknowledgements are handled by class:

| Error | Handling |
|---|---|
| Network error, timeout, `INVALID_SHARE_SESSION_EPOCH`, `SHARE_SESSION_NOT_FOUND`, `SHARE_SESSION_LIMIT_REACHED` | Resent to the same broker once its share session is re-established, until the `commit()` deadline or, in the background, the acquisition-lock duration |
| `NOT_LEADER_OR_FOLLOWER`, `FENCED_LEADER_EPOCH`, `UNKNOWN_TOPIC_OR_PARTITION` | Reported; metadata is refreshed |
| Anything else (e.g. `INVALID_RECORD_STATE` after the lock expired) | Reported and dropped |

### Commit results and the callback

`commit()` sends everything pending and returns `CommitResults`: one
`Result<(), KrafkaError>` per partition it sent acknowledgements for, and only
those (empty when nothing was pending). It is bounded by `request_timeout`;
what is unanswered by then is reported as `Timeout`.

The acknowledgement-commit callback receives the outcome of **every**
acknowledgement request — implicit, explicit, piggybacked on a fetch, sent by
`commit()` or `close()` — once per partition per request:

```rust,compile
use krafka::share_consumer::ShareConsumer;

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .share_consumer("my-share-group")
    .acknowledgement_commit_callback(|commit| {
        if let Err(error) = &commit.result {
            eprintln!("{}-{} {:?}: {error}", commit.topic, commit.partition, commit.offsets);
        }
    })
    .build()
    .await?;
```

It runs on a background task, so keep it short; a panic in it is caught and
logged.

### Renewing an acquisition lock

A delivered record is *acquired*, not consumed: the broker holds a lock on it
for `group.share.record.lock.duration.ms` and redelivers it when the lock
expires. `renew(&record)` extends the lock (KIP-1222, Kafka 4.2+). The record
stays pending; settle it later with `ack`, `release` or `reject`. The lock
duration is a broker setting; `acquisition_lock_timeout()` returns what the
broker last reported.

```rust,compile
use krafka::share_consumer::{AcknowledgementMode, ShareConsumer};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .share_consumer("my-share-group")
    .acknowledgement_mode(AcknowledgementMode::Explicit)
    .build()
    .await?;
consumer.subscribe(["events"]).await?;

let lock = consumer.acquisition_lock_timeout().unwrap_or(Duration::from_secs(30));
for record in consumer.poll(Duration::from_secs(1)).await? {
    let mut renewed = Instant::now();
    for _step in 0..10 {
        // ... a slice of slow work ...
        if renewed.elapsed() >= lock / 2 {
            consumer.renew(&record)?;
            renewed = Instant::now();
        }
    }
    consumer.ack(&record)?;
}
```

The lock starts when the broker builds the fetch response, so the value is an
upper bound on the time left. Against a broker older than 4.2, `renew`
fails with an error naming KIP-1222 and the record stays pending.

## What a fetch delivers

- Only records inside the response's acquired ranges are delivered; the rest
  of a returned batch is skipped.
- Transaction control records are skipped; their offsets, and acquired
  offsets with no record (compacted away), are acknowledged as GAP.
- Offsets in a batch that fails to decode are released for redelivery; the
  broker archives a record once its delivery count reaches
  `group.share.delivery.count.limit`.
- Each record carries `delivery_count` from its acquired range. It is
  approximate: the broker does not persist it exactly.

### Acquisition bound

Every `ShareFetch` asks for at most `max_poll_records` records, and a broker
is not fetched from again while records it handed out are still buffered.
Buffered records hold acquisition locks, so keep `max_poll_records` near what
the application processes within one lock duration.

With `acquire_mode(AcquireMode::BatchOptimized)` (the default) the broker may
finish a record batch beyond the limit. `AcquireMode::RecordLimit` (KIP-1206)
makes the limit exact; it needs `ShareFetch` v2 (Kafka 4.2+), and against an
older broker the first `poll()` fails with a `Config` error naming KIP-1206.

### Deserializers

The share consumer takes the same
[`Deserializer`](https://docs.rs/krafka/latest/krafka/serdes/trait.Deserializer.html)
hook as the consumer:

```rust,compile
use bytes::Bytes;
use krafka::Headers;
use krafka::serdes::Deserializer;

/// Strips a 5-byte Confluent-style framing header.
struct StripHeader;

impl Deserializer for StripHeader {
    fn deserialize(
        &self,
        _topic: &str,
        _headers: &Headers,
        payload: Bytes,
        _is_key: bool,
    ) -> krafka::Result<Bytes> {
        if payload.len() < 5 {
            return Err(krafka::KrafkaError::serialization("payload is not framed"));
        }
        Ok(payload.slice(5..))
    }
}

let consumer = kafka
    .share_consumer("my-share-group")
    .value_deserializer(StripHeader)
    .build()
    .await?;
```

A record the decoder rejects ends the batch: `poll()` returns the records
before it, the next `poll()` returns
`KrafkaError::RecordDeserialization { topic, partition, offset, .. }`, and the
record is released for redelivery. The records after it follow in later polls,
in both acknowledgement modes. No record is accepted before it was delivered.

## Membership

A background task heartbeats at the interval the coordinator returns.
`subscribe()` retries coordinator-not-ready errors (up to five attempts).

- **Fenced** (`FENCED_MEMBER_EPOCH`, `UNKNOWN_MEMBER_ID`): the member drops
  all buffered and delivered records and pending acknowledgements (reported to
  the callback) and rejoins with its full subscription.
- **Assignment change**: buffered records of revoked partitions are released
  and their delivered records forgotten; acknowledgements already queued are
  still sent.
- **Leader moved** (`NOT_LEADER_OR_FOLLOWER` on a fetch): the partition backs
  off (100 ms doubling to 1 s) and fetching resumes on the new leader.

## Cancellation and wakeup

`poll()`, `recv()`, the stream and `commit()` are cancel safe. A dropped
`poll()`/`recv()` accepts nothing; its records come with a later call.
`ack`, `release`, `reject` and `renew` record the acknowledgement at once and
a background task sends it, so a dropped `poll()` or `commit()` neither loses
nor duplicates one; a dropped `commit()` only stops waiting for the outcome. `close()` is not cancel safe: dropped, the
consumer is closed but the final acknowledgements and the group leave may not
have happened. `poll()`/`recv()` calls on clones are serialized.

`wakeup()` interrupts a waiting `poll()`/`recv()` (or the next one) with
`KrafkaError::Wakeup`; the consumer stays usable.

## Close

`close()` is `close_with(CloseOptions::new())`, a 30 s budget. Idempotent.

```rust,compile
use krafka::CloseOptions;
use std::time::Duration;

let consumer = kafka.share_consumer("my-share-group").build().await?;
consumer.close_with(CloseOptions::new().timeout(Duration::from_secs(10))).await?;
```

A share consumer always leaves its group; it ignores
`CloseOptions::group_membership_operation`.

It accepts the last `poll()`'s records in implicit mode, sends the remaining
acknowledgements and closes each share session (the broker then releases
everything the member still holds; outcomes reach the callback), leaves the
group, and clears local state.

`unsubscribe()` commits (best effort), leaves the group, clears local state and
takes a fresh member id; the consumer can subscribe again.

## Configuration

Every option below is a setter on the `kafka.share_consumer(group)` builder.
Connection settings — client id, security,
timeouts, metadata — come from the `Kafka` handle; see
[Configuration](@/docs/configuration.md).

| Option | Type | Default | Description |
|---|---|---|---|
| `acknowledgement_mode` | AcknowledgementMode | `Implicit` | `Implicit` or `Explicit` |
| `acquire_mode` | AcquireMode | `BatchOptimized` | `RecordLimit` makes `max_poll_records` exact (KIP-1206, Kafka 4.2+) |
| `max_poll_records` | i32 | `500` | Records per `poll()`, and `MaxRecords` of every `ShareFetch` (≥ 1) |
| `batch_size` | i32 | `500` | Acquisition batch-size hint (`BatchSize`), capped at `max_poll_records` |
| `fetch_min_bytes` | i32 | `1` | Minimum bytes a broker must have before answering a `ShareFetch` |
| `fetch_max_bytes` | i32 | `52_428_800` | Maximum bytes one `ShareFetch` response may carry (50 MiB) |
| `fetch_max_wait` | Duration | `500ms` | How long a broker may hold a `ShareFetch` |
| `client_rack` | `Option<String>` | `None` | Rack id sent with heartbeats |
| `max_decompressed_size` | usize | 128 MiB | Decompression-bomb ceiling for record batches |
| `metrics_push` | bool | `true` | Push KIP-714 client telemetry to brokers that subscribe to it |

Builder-only hooks: `key_deserializer` / `value_deserializer` (the same
`Deserializer` hook as the subscription consumer) and
`acknowledgement_commit_callback`.

## Observability

`ShareConsumer::metrics()` returns the same `Metrics` snapshot as every
client. Its `consumer` section counts `polls`, `empty_polls`,
`records_received`, `bytes_received`, `commits` (a `commit()` whose partitions
all succeeded) and `errors`; its `connections` section is the shared pool.
`poll`/`recv` and `commit()` emit the same `poll` and `commit` spans as the
consumer, and the share consumer pushes KIP-714 telemetry by default — see
[Metrics](@/docs/metrics.md).

## Operating a Share Group

Reading, resetting and deleting a share group's start offsets are
`AdminClient` operations (Kafka 4.2+):

```rust,compile
// Lag monitoring — `lag` requires Kafka 4.3 (KIP-1226); older brokers report None.
use krafka::admin::TopicPartition;

let described = admin
    .describe_share_group_offsets("my-share-group", Default::default())
    .await?;
for (tp, state) in &described {
    if let Ok(state) = state {
        println!("{}-{} start={} lag={:?}", tp.topic, tp.partition, state.start_offset, state.lag);
    }
}

// Reset to the beginning. The group must be empty.
admin
    .alter_share_group_offsets(
        "my-share-group",
        [(TopicPartition::new("my-topic", 0), 0)],
        Default::default(),
    )
    .await?;

// Drop state for a topic that no longer exists. The group must be empty.
admin
    .delete_share_group_offsets("my-share-group", ["retired-topic"], Default::default())
    .await?;
```

See [Admin Client → Share groups](@/docs/admin.md) for the full
reference.

## Wire Protocol

| API | Key | Versions | Purpose |
|---|---|---|---|
| ShareGroupHeartbeat | 76 | v1 | Membership and assignment |
| ShareFetch | 78 | v1–v2 | Fetch records, piggyback acknowledgements; v2 adds `ShareAcquireMode` (KIP-1206) and `IsRenewAck` (KIP-1222) |
| ShareAcknowledge | 79 | v1–v2 | Acknowledge records; v2 adds `IsRenewAck` |

`AdminClient` describes share groups with ShareGroupDescribe (key 77, v1).
See the [Protocol Reference](@/docs/protocol.md) for wire format details.

## Testing a Share Consumer

The `test-broker` feature's in-process broker serves the share-group APIs, so
a share consumer can be tested without a cluster. It tracks acquired records,
their holders and delivery counts, and validates share sessions. It does
**not** model acquisition-lock expiry or `group.share.delivery.count.limit`;
tests of those need a real broker.
