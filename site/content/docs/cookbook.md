+++
title = "Cookbook"
description = "Task-oriented recipes: the shortest correct way to do the things people actually build."
weight = 15

[extra]
slug_id = "cookbook"
+++

The rest of the documentation is organised by module — [Producer](@/docs/producer.md),
[Consumer](@/docs/consumer.md), [Admin](@/docs/admin.md). That is the right shape
for looking something up, and the wrong shape when you know the *outcome* you
want and not which module owns it.

Each recipe below is a complete, runnable shape with the reasoning that matters
kept inline and everything else linked.

## Recipes

- [Exactly-once consume-transform-produce](#exactly-once-consume-transform-produce)
- [At-least-once with manual commits](#at-least-once-with-manual-commits)
- [Backpressure: stop reading when the sink stalls](#backpressure-stop-reading-when-the-sink-stalls)
- [Replay from a point in time](#replay-from-a-point-in-time)
- [Build an in-memory table from a compacted topic](#build-an-in-memory-table-from-a-compacted-topic)
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

```rust
use krafka::consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krafka::producer::TransactionalProducer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("transformer")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    // Never read a record whose transaction has not committed, or the pipeline
    // propagates writes that were later rolled back.
    .isolation_level(IsolationLevel::ReadCommitted)
    // The transaction owns the offsets; an auto-commit racing it would commit
    // outside the transaction and break atomicity.
    .enable_auto_commit(false)
    .build()
    .await?;
consumer.subscribe(&["orders"]).await?;

let producer = TransactionalProducer::builder()
    .bootstrap_servers("localhost:9092")
    // Stable across restarts — this is what lets the broker fence a zombie
    // instance of this same processor (KIP-360).
    .transactional_id("orders-transformer-1")
    .build()
    .await?;
producer.init_transactions().await?;

loop {
    let records = consumer.poll(Duration::from_secs(1)).await?;
    if records.is_empty() {
        continue;
    }

    producer.begin_transaction()?;
    for record in &records {
        let transformed = transform(record);
        producer.send("orders-enriched", record.key.clone(), &transformed).await?;
    }

    // Offsets go to the *group coordinator* as part of this transaction.
    let metadata = consumer.group_metadata().await
        .ok_or_else(|| krafka::KrafkaError::invalid_state("consumer is not in a group"))?;
    producer.send_offsets_to_transaction(&consumer, &metadata).await?;

    producer.commit_transaction().await?;
}
```

If `commit_transaction()` fails with an abortable error, call
`abort_transaction()` and continue — the consumer will re-deliver. Fatal errors
require a new producer. See [Transaction States](@/docs/producer.md#transaction-states).

Full example: [`examples/exactly_once.rs`](https://github.com/hupe1980/krafka/blob/main/examples/exactly_once.rs).

---

## At-least-once with manual commits

Commit only after the work is durable. The ordering is the whole recipe:
process, *then* commit.

```rust
let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("billing")
    .enable_auto_commit(false)
    .build()
    .await?;
consumer.subscribe(&["invoices"]).await?;

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
[`commit_with_metadata`](@/docs/consumer.md#commit-with-metadata).

---

## Backpressure: stop reading when the sink stalls

`pause()` is the right tool, not sleeping or dropping records. It stops delivery
for the named partitions while the rest keep flowing, and the consumer stays
alive in its group — no rebalance.

```rust
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
reassigned — the exact outcome backpressure was meant to avoid.

Records already buffered for a paused partition are withheld, not discarded:
they are delivered on `resume()` without a re-fetch.

---

## Replay from a point in time

`seek_to_timestamp` resolves a wall-clock instant to an offset with `ListOffsets`
and repositions there.

```rust
use std::time::{SystemTime, UNIX_EPOCH};

// One hour ago, in epoch milliseconds.
let since = (SystemTime::now() - Duration::from_secs(3600))
    .duration_since(UNIX_EPOCH)?
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
Use `seek_many()` to reposition many partitions under one lock.

---

## Build an in-memory table from a compacted topic

`CompactedTopicConsumer` bundles a consumer, a key→value table and caught-up
detection. This is the shape behind most configuration and lookup-table
use cases.

```rust,compile
use krafka::consumer::CompactedTopicConsumer;

let mut table = CompactedTopicConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .topic("user-profiles")
    .build()
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

## Use a schema registry

krafka does not ship a schema-registry client, and that is deliberate: every
comparable client draws the same line. Java's `kafka-clients` has none
(`kafka-avro-serializer` is a separate artifact), librdkafka has none
(`libschemaregistry` is a separate library), and franz-go keeps `pkg/sr` out of
`kgo`. A registry is a different service with a different protocol, auth model
and release cadence; coupling it to the Kafka client means a registry API change
forces a Kafka client release.

What krafka provides is the *hook* — [`Serializer`] and [`Deserializer`], the
equivalent of Java's `key.serializer` / `value.serializer`. Pair it with
[`schemreg`](https://crates.io/crates/schemreg), which covers the Confluent
registry, AWS Glue, Apicurio, and Avro / Protobuf / JSON codecs:

```toml
[dependencies]
krafka   = "0.18"
schemreg = { version = "0.4", features = ["confluent", "avro"] }
```

The two traits do not know about each other, so bridge them with a newtype.
This is the whole adapter:

```rust
use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use krafka::serdes::{Deserializer, Serializer};

/// Bridges a `schemreg` encoder into krafka's producer hook.
struct SchemaSerializer<T>(T);

impl<T> Serializer for SchemaSerializer<T>
where
    T: schemreg::traits::SchemaEncoder + Send + Sync,
{
    fn serialize(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = krafka::Result<Bytes>> + Send + '_>> {
        let topic = topic.to_owned();
        let record_name = record_name.map(str::to_owned);
        Box::pin(async move {
            self.0
                .encode(payload, &topic, record_name.as_deref(), is_key)
                .await
                .map_err(|e| krafka::KrafkaError::config(e.to_string()))
        })
    }
}
```

Write the mirror image for `Deserializer` (it takes no `record_name` — on the
read path the framing identifies the schema), then wire both in:

```rust
let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .value_serializer(Arc::new(SchemaSerializer(encoder)))
    .build()
    .await?;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("orders")
    .value_deserializer(Arc::new(SchemaDeserializer(decoder)))
    .build()
    .await?;
```

From here `send()` frames every value on the way out and `poll()` unframes it on
the way in — the application only ever sees decoded bytes.

> **Map the error type deliberately.** The adapter above collapses every
> registry failure into `KrafkaError::config`, which is fine for getting
> started and wrong for production: a registry that is *unreachable* is
> retriable, a schema that is *incompatible* is not, and `is_retriable()` cannot
> tell them apart once both are `Config`. Match on `schemreg`'s error and map
> the transport cases to `KrafkaError::network` so krafka's retry logic can act
> on them.

### Beyond schemas

The traits are plain `Bytes -> Bytes`, so the same hook covers envelope
encryption, an application-level compression scheme, or a bare `serde_json`
round-trip. Nothing about it is schema-specific.

[`Serializer`]: https://docs.rs/krafka/latest/krafka/serdes/trait.Serializer.html
[`Deserializer`]: https://docs.rs/krafka/latest/krafka/serdes/trait.Deserializer.html

---

## Route poison records to a dead-letter topic

A `DeadLetterQueue` receives records that exhaust their retries, on both send
paths and on both producers — so a record cannot be lost because it failed in
the batching path rather than the direct one.

Routing dead letters back into Kafka is the common case, so it ships in the
crate:

```rust,compile
use std::sync::Arc;

use krafka::dlq::KafkaDeadLetterQueue;
use krafka::producer::Producer;

// A *dedicated* producer. Sharing the one whose sends are failing puts the
// dead-letter write behind the same stalled broker that caused the failure.
let dlq = Arc::new(KafkaDeadLetterQueue::new(
    Producer::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?,
    "orders.DLQ",
));

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    // `dlq.clone()` and not `Arc::clone(&dlq)`: the method call is a coercion
    // site, so `Arc<KafkaDeadLetterQueue>` unsizes to `Arc<dyn DeadLetterQueue>`.
    // The free function fixes its own return type first and will not coerce.
    .dead_letter_queue(dlq.clone())
    .build()
    .await?;

// Alert on this: a non-zero value means the safety net itself is failing, and
// nothing else reports it.
assert_eq!(dlq.failures(), 0);
```

The record keeps its key, value and headers, and gains
`__krafka.dlq.original.topic` and `__krafka.dlq.exception.message` so a replay
job can tell where it came from and why it is here. The source partition index
is dropped, because the dead-letter topic has its own partition count.

For any other destination — S3, a database, a file — implement
[`DeadLetterQueue`](https://docs.rs/krafka/latest/krafka/dlq/trait.DeadLetterQueue.html)
yourself; it is one method.

For the consumer side, `krafka::dlq::build_dlq_record` turns a `ConsumerRecord`
into a `ProducerRecord` carrying provenance headers (original topic, partition,
offset and the error), so a replay job can reconstruct where each record came
from.

The DLQ is invoked *before* the error reaches the caller, so code reacting to a
failed `send()` can rely on the record already being safe. See
[Dead Letter Queues](@/docs/errors.md).

---

## Share one connection pool across clients

A producer, a consumer and an admin client pointed at the same cluster do not
need three connection pools and three metadata caches.

```rust
use krafka::client::KrafkaClient;

let client = KrafkaClient::builder()
    .bootstrap_servers("localhost:9092")
    .build()
    .await?;

let producer = Producer::builder().with_client(&client).build().await?;
let consumer = Consumer::builder().with_client(&client).group_id("g").build().await?;
let admin    = AdminClient::builder().with_client(&client).build().await?;
```

Each client reports `owns_pool()`; a borrowed pool is left alone by `close()`,
so shutting one client down does not tear out its siblings' connections. Close
the `KrafkaClient` last.

---

## Rotate TLS certificates without a restart

`refresh_tls()` re-reads the certificate files from disk and atomically swaps
the connector. Existing sessions keep the connector they handshook with; every
new connection uses the new material.

```rust
let producer = Arc::new(producer);
let rotating = Arc::clone(&producer);

tokio::spawn(async move {
    let mut ticker = tokio::time::interval(Duration::from_secs(3600));
    loop {
        ticker.tick().await;
        if let Err(e) = rotating.refresh_tls().await {
            // The old connector stays active on failure — nothing breaks.
            tracing::warn!("TLS reload failed: {e}");
        }
    }
});
```

The connection pool can also do this for you on a timer — see
[TLS certificate rotation](@/docs/authentication.md).

---

## Export lag to Prometheus

Every client exposes shared metrics handles that render themselves in the
Prometheus text format. The producer's is `metrics_handle()` (`metrics()`
returns a plain value snapshot); the consumer's is `metrics()`; the transport's
is `connection_metrics()`, and it is shared by every client on the same pool:

```rust,compile
use krafka::metrics::MetricsVisitable;

fn scrape(producer: &Producer, consumer: &Consumer) -> String {
    let mut body = String::new();
    body.push_str(&producer.metrics_handle().to_prometheus_text("krafka_producer"));
    body.push_str(&consumer.metrics().to_prometheus_text("krafka_consumer"));
    body.push_str(&consumer.connection_metrics().to_prometheus_text("krafka_connection"));
    body
}
```

Two things worth knowing before you alert on the output:

- **Lag counts records read ahead into the buffer.** Fetched is not delivered,
  so the number reflects what the application still has to process.
- **Under `read_committed`, lag is measured against the last stable offset**,
  not the high watermark — otherwise an open transaction pins a fully drained
  consumer at permanent non-zero lag and an autoscaler chases it forever.

`consumer.lag()` additionally reports `stale_partitions`, so a lag value that is
merely out of date is distinguishable from a real one. See
[Metrics](@/docs/metrics.md).

---

## Test without Docker

The `test-broker` feature ships a real TCP listener speaking the real wire
protocol, with fault injection — including the full transaction protocol, so
exactly-once paths are testable in a unit test.

```rust,compile
use krafka::testing::{Control, FakeBroker};
use krafka::protocol::ApiKey;

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
