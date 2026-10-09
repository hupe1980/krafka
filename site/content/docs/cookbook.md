+++
title = "Cookbook"
description = "Task-oriented recipes for common Kafka application patterns."
weight = 15

[extra]
slug_id = "cookbook"
+++

The rest of the documentation is organised by module — [Producer](@/docs/producer.md),
[Consumer](@/docs/consumer.md), [Admin](@/docs/admin.md). The recipes below are
organised by outcome. Each sample compiles against the current API; `kafka`,
`producer` and `consumer` stand for a connected handle and clients built from
it.

## Recipes

- [Exactly-once consume-transform-produce](#exactly-once-consume-transform-produce)
- [At-least-once with manual commits](#at-least-once-with-manual-commits)
- [Backpressure: stop reading when the sink stalls](#backpressure-stop-reading-when-the-sink-stalls)
- [Replay from a point in time](#replay-from-a-point-in-time)
- [Build an in-memory table from a compacted topic](#build-an-in-memory-table-from-a-compacted-topic)
- [Delete a key from a compacted topic](#delete-a-key-from-a-compacted-topic)
- [Use a schema registry](#use-a-schema-registry)
- [Route poison records to a dead-letter topic](#route-poison-records-to-a-dead-letter-topic)
- [Share one connection pool across clients](#share-one-connection-pool-across-clients)
- [Rotate TLS certificates without a restart](#rotate-tls-certificates-without-a-restart)
- [Export lag to Prometheus](#export-lag-to-prometheus)
- [Test without Docker](#test-without-docker)

---

## Exactly-once consume-transform-produce

The canonical Kafka pipeline: read from one topic, transform, write to another,
and have the whole thing be atomic. The offsets must be committed **inside** the
transaction — that is what makes the read and the write one unit.

```rust,compile
use krafka::consumer::{AutoOffsetReset, ConsumerRecord, IsolationLevel};
use krafka::producer::TopicPartitionOffset;
use std::time::Duration;

fn transform(record: &ConsumerRecord) -> bytes::Bytes {
    record.value.clone().unwrap_or_default()
}

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("transformer")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    // Never read a record whose transaction has not committed, or the pipeline
    // propagates writes that were later rolled back.
    .isolation_level(IsolationLevel::ReadCommitted)
    // The transaction owns the offsets; an auto-commit racing it would commit
    // outside the transaction and break atomicity.
    .enable_auto_commit(false)
    .build()
    .await?;
consumer.subscribe(["orders"]).await?;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .build_transactional("orders-transformer-1")
    .await?;

loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    if records.is_empty() {
        continue;
    }

    producer.begin()?;
    let mut offsets = Vec::new();
    for record in &records {
        let mut output = krafka::Record::new("orders-enriched", transform(record));
        output.key = record.key.clone();
        producer.send(output).await?;
        // Commit the offset of the NEXT record to consume.
        offsets.push(TopicPartitionOffset::new(&*record.topic, record.partition, record.offset + 1));
    }

    // Offsets go to the *group coordinator* as part of this transaction.
    let metadata = consumer.group_metadata().await
        .ok_or_else(|| krafka::KrafkaError::illegal_state("consumer is not in a group"))?;
    producer.send_offsets(&offsets, &metadata).await?;

    producer.commit().await?;
}
```

If `commit()` fails with an error for which `e.requires_abort()` is true, call
`abort()`. The consumer's position is already past the aborted records, so
rewind it to the group's committed offsets before the next poll; otherwise
those records are skipped:

```rust,compile
use krafka::producer::TransactionalProducer;

async fn abort_and_rewind(
    producer: &TransactionalProducer,
    consumer: &Consumer,
) -> krafka::Result<()> {
    producer.abort().await?;
    let assignment = consumer.assignment().await;
    let mut partitions = Vec::new();
    for (topic, ps) in &assignment {
        for &partition in ps {
            partitions.push((topic.as_str(), partition));
        }
    }
    for ((topic, partition), committed) in consumer.committed(&partitions).await? {
        consumer.seek(&topic, partition, committed.offset).await?;
    }
    Ok(())
}
```

A partition the group never committed is absent from `committed`; seek it to
the start offset your reset policy implies. When `e.is_fatal()` is true, build
a new producer. See [Transaction States](@/docs/producer.md#transaction-states).

Full example: [`examples/exactly_once.rs`](https://github.com/hupe1980/krafka/blob/main/examples/exactly_once.rs).

---

## At-least-once with manual commits

Commit only after the work is durable: process, *then* commit.

```rust,compile
use krafka::consumer::ConsumerRecord;

async fn write_to_database(record: &ConsumerRecord) -> krafka::Result<()> {
    Ok(())
}

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("billing")
    .enable_auto_commit(false)
    .build()
    .await?;
consumer.subscribe(["invoices"]).await?;

loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    if records.is_empty() {
        continue;
    }

    for record in &records {
        write_to_database(record).await?;   // if this fails, we do not commit
    }

    consumer.commit().await?;
}
```

`commit()` acknowledges the records `poll()` actually returned — never the ones
krafka has read ahead into its buffer. A crash between `poll()` and `commit()`
re-delivers, which is the "at least" in at-least-once.

To checkpoint application state alongside the offset, use
[`commit_offsets`](@/docs/consumer.md#commit-with-metadata).

---

## Backpressure: stop reading when the sink stalls

`pause()` is the right tool, not sleeping or dropping records. It stops delivery
for the named partitions while the rest keep flowing, and the consumer stays
alive in its group — no rebalance.

```rust,compile
/// Your downstream: a channel, a connection pool, a write buffer.
struct Sink;
impl Sink {
    fn is_backed_up(&self) -> bool { false }
    fn has_drained(&self) -> bool { true }
}
let sink = Sink;

let assignment = consumer.assignment().await;

if sink.is_backed_up() {
    for (topic, partitions) in &assignment {
        consumer.pause(topic, partitions).await;
    }
}

// poll() still has to be called: it heartbeats and keeps the member alive.
// It just returns nothing for paused partitions.
let records = consumer.poll(Duration::from_secs(1)).await?;

if sink.has_drained() {
    for (topic, partitions) in &assignment {
        consumer.resume(topic, partitions).await;
    }
}
```

**Keep calling `poll()` while paused.** A consumer that stops polling for longer
than `max_poll_interval` is ejected from its group and its partitions are
reassigned.

Records already buffered for a paused partition are withheld, not discarded:
they are delivered on `resume()` without a re-fetch.

---

## Replay from a point in time

`seek_to_timestamp` resolves a wall-clock instant to an offset with `ListOffsets`
and repositions there.

```rust,compile
use std::time::{SystemTime, UNIX_EPOCH};

// One hour ago, in epoch milliseconds.
let since = (SystemTime::now() - Duration::from_secs(3600))
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_millis() as i64;

// Assignment must exist before seeking — subscribe() is lazy, so poll once.
consumer.poll(Duration::from_secs(5)).await?;

for (topic, partitions) in &consumer.assignment().await {
    for &partition in partitions {
        consumer.seek_to_timestamp(topic, partition, since).await?;
    }
}
```

A seek discards anything already fetched for that partition, so the next
`poll()` returns data from the new position and the next commit reflects it.
A partition with no record that new fails with `KrafkaError::NoOffset`.
`seek_many()` repositions several partitions at once: all or none.

---

## Build an in-memory table from a compacted topic

`CompactedTopicConsumer` bundles a consumer, a key→value table and caught-up
detection.

```rust,compile
use krafka::consumer::CompactedTopicConsumer;

let mut table = CompactedTopicConsumer::from_consumer_builder(
    kafka.consumer_without_group(),
    "user-profiles",
)
.await?;

// Read from the beginning until every partition reaches its end offset.
table.scan(Duration::from_secs(1)).await?;

if let Some(profile) = table.table().get(b"user-123") {
    println!("{:?}", profile.value);
}

// Then keep it live.
loop {
    for change in table.poll(Duration::from_secs(1)).await? {
        println!("{:?} -> {:?}", change.key, change.new_value);
    }
}
```

Tombstones (null values) delete the key. On an actively written topic `scan()`
is best-effort rather than a bounded snapshot — see
[Compacted Topics](@/docs/consumer.md).

---

## Delete a key from a compacted topic

A record with a **null value** — a tombstone — retires a key from a
`cleanup.policy=compact` topic. Compaction removes every earlier record for the
key, then the tombstone itself after `delete.retention.ms`.

```rust,compile
use krafka::producer::Record;

producer
    .send(
        Record::tombstone("users", "user-42")
            .header("X-Reason", &b"gdpr-erasure"[..]),
    )
    .await?;

// Or, without building a record:
producer.send(krafka::Record::tombstone("users", "user-42")).await?;
```

`Some(b"")` is **not** a deletion — a zero-length value is ordinary data that
compaction preserves. Only `None` deletes. Details, including partition
co-location and serializer behaviour, are in
[Tombstones and Compacted Topics](@/docs/producer.md#tombstones-and-compacted-topics);
the read side is [Build an in-memory table from a compacted topic](#build-an-in-memory-table-from-a-compacted-topic).

---

## Use a schema registry

krafka has no schema-registry client and no Avro, Protobuf or JSON codec. It
provides the hooks: [`Serializer<T>`] for [`TypedProducer`] and
[`Deserializer`] for the consumer. Pair them with a registry crate, such as
[`schemreg`](https://crates.io/crates/schemreg), through a small adapter.

`Serializer<T>` is synchronous, so register the schema before sending (the
registry client caches it) and encode per record. The serializer may write the
schema id into a header. The adapter below compiles as shown; `Encode` and
`Decode` stand for whatever your registry crate provides:

```rust,compile
use bytes::Bytes;
use krafka::Headers;
use krafka::producer::TypedProducer;
use krafka::serdes::{Deserializer, Serializer, StringSerializer};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Stand-in for your registry crate's encoder: frames `value` with its schema id.
pub trait Encode<T: ?Sized>: Send + Sync {
    fn encode(&self, topic: &str, value: &T) -> Result<Vec<u8>, BoxError>;
}

/// Stand-in for your registry crate's decoder.
pub trait Decode: Send + Sync {
    fn decode(&self, topic: &str, payload: &[u8]) -> Result<Vec<u8>, BoxError>;
}

/// Bridges a registry encoder into krafka's typed producer.
pub struct SchemaSerializer<E>(pub E);

impl<T: ?Sized, E: Encode<T>> Serializer<T> for SchemaSerializer<E> {
    fn serialize(&self, topic: &str, _headers: &mut Headers, value: &T) -> krafka::Result<Bytes> {
        self.0
            .encode(topic, value)
            .map(Bytes::from)
            .map_err(|e| krafka::KrafkaError::serialization(e.to_string()))
    }
}

/// Bridges a registry decoder into the consumer.
pub struct SchemaDeserializer<D>(pub D);

impl<D: Decode> Deserializer for SchemaDeserializer<D> {
    fn deserialize(
        &self,
        topic: &str,
        _headers: &Headers,
        payload: Bytes,
        _is_key: bool,
    ) -> krafka::Result<Bytes> {
        self.0
            .decode(topic, &payload)
            .map(Bytes::from)
            .map_err(|e| krafka::KrafkaError::serialization(e.to_string()))
    }
}

pub struct Order {
    pub id: u64,
}

async fn wire<E, D>(kafka: &Kafka, encoder: E, decoder: D) -> krafka::Result<()>
where
    E: Encode<Order> + 'static,
    D: Decode + 'static,
{
    let producer: TypedProducer<str, Order> = TypedProducer::new(
        kafka.producer().build().await?,
        StringSerializer,
        SchemaSerializer(encoder),
    );
    producer.send("orders", Some("order-1"), Some(&Order { id: 1 })).await?;

    let consumer = kafka
        .consumer("orders")
        .value_deserializer(SchemaDeserializer(decoder))
        .build()
        .await?;
    Ok(())
}
```

A deserializer error reaches the application as
`KrafkaError::RecordDeserialization`, carrying the record's topic, partition
and offset so it can [skip past it](@/docs/errors.md#handling-poll-errors).

> **Map the error type.** The adapter above maps every registry
> failure to `KrafkaError::serialization`, and `is_retriable()` is false for
> it. A registry that is *unreachable* is worth retrying and a schema that is
> *incompatible* is not: match on your registry crate's error and map transport
> failures to `KrafkaError::network` so the caller can tell them apart.

### Beyond schemas

A serializer is any `T -> Bytes` encoding, so the same hook covers envelope
encryption, an application-level compression scheme, or a bare `serde_json`
encoding.

[`Serializer<T>`]: https://docs.rs/krafka/latest/krafka/serdes/trait.Serializer.html
[`TypedProducer`]: https://docs.rs/krafka/latest/krafka/producer/struct.TypedProducer.html
[`Deserializer`]: https://docs.rs/krafka/latest/krafka/serdes/trait.Deserializer.html

---

## Route poison records to a dead-letter topic

When a consumed record cannot be processed, send it to a dead-letter topic
and keep consuming. `krafka::dlq::record_for` builds the record: it keeps the
key, value and headers and adds `__krafka.dlq.original.topic`,
`__krafka.dlq.original.partition`, `__krafka.dlq.original.offset` and
`__krafka.dlq.exception.message`, so a replay job can tell where it came from
and why it is here. The source partition index is not carried over, because
the dead-letter topic has its own partition count.

```rust,compile
use krafka::consumer::Consumer;
use krafka::producer::Producer;
use std::time::Duration;

// A *dedicated* producer, so dead-letter writes do not queue behind the
// application's own sends.
let dlq = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .build()
    .await?;

for record in consumer.poll(Duration::from_secs(1)).await? {
    if let Err(error) = std::str::from_utf8(record.value.as_deref().unwrap_or_default()) {
        dlq.send(krafka::dlq::record_for("orders.DLQ", &record, &error))
            .await?;
    }
}
```

The producer itself has no dead-letter path: a failed `send()` returns its
error, and `send` takes the record by value, so clone it first to route it
elsewhere on failure. See
[Dead Letter Queue](@/docs/errors.md#dead-letter-queue).

---

## Share one connection pool across clients

A producer, a consumer and an admin client built from one `Kafka` handle share
one connection pool and one metadata cache.

```rust,compile
let kafka = krafka::Kafka::builder("localhost:9092").connect().await?;

let producer = kafka.producer().build().await?;
let consumer = kafka.consumer("g").build().await?;
let admin = kafka.admin();
```

A client's `close()` leaves the pool alone, so shutting one client down does
not tear out its siblings' connections. The pool closes when the handle and
every client built from it are gone.

---

## Rotate TLS certificates without a restart

`refresh_tls()` re-reads the certificate files from disk and atomically swaps
the connector. Existing sessions keep the connector they handshook with; every
new connection uses the new material.

```rust,compile
use std::time::Duration;

let rotating = kafka.clone();
tokio::spawn(async move {
    let mut ticker = tokio::time::interval(Duration::from_secs(3600));
    loop {
        ticker.tick().await;
        if let Err(e) = rotating.refresh_tls().await {
            // On failure the old connector stays active.
            tracing::warn!("TLS reload failed: {e}");
        }
    }
});
```

The connection pool can also do this for you on a timer — see
[TLS certificate rotation](@/docs/authentication.md).

---

## Export lag to Prometheus

Every client's `metrics()` returns an owned snapshot, and `Kafka::metrics()`
sums the clients of a handle with the shared pool counted once. Render the sum
in the Prometheus text format:

```rust,compile
use krafka::Kafka;

fn scrape(kafka: &Kafka) -> String {
    kafka.metrics().prometheus_text()
}
```

Before alerting on the output:

- **Lag counts records read ahead into the buffer.** Fetched is not delivered,
  so the number reflects what the application still has to process.
- **Under `read_committed`, lag is measured against the last stable offset**,
  not the high watermark, so an open transaction does not hold a drained
  consumer at non-zero lag.

`consumer.lag()` additionally marks each partition `stale` when its watermarks
are old, so a lag value that is merely out of date is distinguishable from a
real one. See
[Metrics](@/docs/metrics.md).

---

## Test without Docker

The `test-broker` feature ships a real TCP listener speaking the real wire
protocol, with fault injection — including the full transaction protocol, so
exactly-once paths are testable in a unit test.

```rust,compile
use krafka::testing::{Control, FakeBroker};
use krafka::testing::ApiKey;

#[tokio::test]
async fn the_consumer_survives_a_coordinator_failover() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 3);

    // Fail the next two heartbeats, then behave.
    broker.on_times(ApiKey::Heartbeat, 2, |_| {
        Control::Error(krafka::error::ErrorCode::NotCoordinator)
    });

    // ... assert the consumer recovers.
}
```

See [Testing](@/docs/testing.md) for the full hook and cluster-manipulation API.
