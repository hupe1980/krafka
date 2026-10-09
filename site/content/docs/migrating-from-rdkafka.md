+++
title = "Migrating from rdkafka"
description = "Move an rdkafka application to krafka: librdkafka properties mapped to builder settings, and FutureProducer and StreamConsumer equivalents."
weight = 24

[extra]
slug_id = "migrating-from-rdkafka"
+++

This guide maps an application written against
[`rdkafka`](https://crates.io/crates/rdkafka) 0.39.0 (the latest release on
crates.io as of 2026-10-08) to krafka. `rdkafka` 0.39.0 builds `rdkafka-sys`
4.10.0, which bundles librdkafka 2.12.1; the librdkafka defaults quoted below
come from that version's
[CONFIGURATION.md](https://github.com/confluentinc/librdkafka/blob/v2.12.1/CONFIGURATION.md)
and are unchanged in librdkafka 2.16.0, the latest release as of 2026-10-09
([CONFIGURATION.md at v2.16.0](https://github.com/confluentinc/librdkafka/blob/v2.16.0/CONFIGURATION.md)).

The shape of the change:

- `ClientConfig` with string keys becomes typed setters. Connection settings go
  on `Kafka::builder(bootstrap)`; role settings go on `kafka.producer()`,
  `kafka.consumer(group)` or `kafka.admin()`. Nothing is validated until
  `connect()` or `build()`, which fail with `KrafkaError::Config` naming the
  setting.
- Every client built from one `Kafka` handle shares its connection pool and
  metadata cache. An rdkafka client is a separate librdkafka instance with its
  own connections and threads.
- Every call is `async`. Delivery results are futures, not callbacks; there is
  no poll loop to keep a producer alive and no background thread per client.

## Property mapping

"Same" in the default column means the krafka default equals the librdkafka
one. Where they differ, both are given, librdkafka's as of 2.12.1/2.16.0
(2026-10-09, sources above).

### Global properties

| librdkafka property | krafka | Default (librdkafka → krafka) |
|---|---|---|
| `bootstrap.servers` | `Kafka::builder("host1:9092,host2:9092")` | required in both |
| `client.id` | `KafkaBuilder::client_id` — one id for every client of the handle; build a second handle for a second id | `rdkafka` → `"krafka"` |
| `security.protocol` | `KafkaBuilder::security(AuthConfig)`: `plaintext` is the default; `ssl` is `AuthConfig::ssl(TlsConfig)`; `sasl_plaintext` is an `AuthConfig::sasl_*` constructor; `sasl_ssl` is the same constructor followed by `.with_tls(TlsConfig)` | `plaintext`, same |
| `sasl.mechanism` / `sasl.mechanisms` | the `AuthConfig` constructor: `sasl_plain`, `sasl_scram_sha256`, `sasl_scram_sha512`, `sasl_oauthbearer` (and `sasl_oauthbearer_provider` for refreshing tokens). See [Authentication](@/docs/authentication.md) | `GSSAPI` → none: the mechanism is the constructor you call |
| `sasl.username`, `sasl.password` | the two arguments of `AuthConfig::sasl_plain` / `sasl_scram_sha256` / `sasl_scram_sha512` | — |
| `sasl.kerberos.*` | **No equivalent.** GSSAPI/Kerberos is not implemented; see the [protocol page](@/docs/protocol.md#not-implemented). Use OAUTHBEARER if the cluster offers it | — |
| `ssl.ca.location` | `TlsConfig::with_ca_cert(path)` — pins the trust store to that bundle | OpenSSL's default CA path (or `probe`) → Mozilla (WebPKI) roots compiled into the binary; `TlsConfig::with_native_roots` (feature `native-tls-roots`) reads the platform store |
| `ssl.certificate.location`, `ssl.key.location` | `TlsConfig::with_client_cert(cert_path, key_path)` (PEM) | — |
| `ssl.key.password` | `TlsConfig::with_client_key_password` (feature `tls-encrypted-keys`) | — |
| `request.timeout.ms` | `KafkaBuilder::request_timeout`. In librdkafka this is the producer's broker-enforced ack timeout; in krafka it is the client-side bound on every request of every client, and must be at least `connect_timeout` | 30 s, same |
| `socket.timeout.ms` | `KafkaBuilder::request_timeout` (above) is the closest equivalent | 60 s → 30 s |
| `retry.backoff.ms` | `ProducerBuilder::retry_backoff` and `AdminClient::retry_backoff`; doubles per retry up to 1 s, with jitter | 100 ms, same |
| `client.rack` | `ProducerBuilder::client_rack` and `ConsumerBuilder::client_rack` — a role setting, not a handle setting | unset, same |
| `socket.keepalive.enable` | `KafkaBuilder::tcp_keepalive(Some(interval))`; `None` turns keepalive off | `false` → on, 60 s |
| `metadata.max.age.ms` | `KafkaBuilder::metadata_max_age` | 15 min → 5 min |
| `max.in.flight.requests.per.connection` | `KafkaBuilder::max_in_flight_requests` — a per-connection cap for every request type. It does not affect produce ordering: the producer keeps at most one batch per partition on the wire and at most 5 produce requests per broker, whatever this is set to. See [Producer › One request per broker, one batch per partition](@/docs/producer.md#one-request-per-broker-one-batch-per-partition) | 1 000 000 → 10 |
| `api.version.request` | **No equivalent.** `ApiVersions` negotiation always runs; there is no fallback version to configure. See [Protocol › Version Negotiation](@/docs/protocol.md#version-negotiation) | `true` → always on |
| `statistics.interval.ms` | **No equivalent.** There is no timer and no JSON callback: call `metrics()` on any client (or on the `Kafka` handle for the sum) when you want a `krafka::metrics::Metrics` snapshot, and `prometheus_text()` to render it. See [Metrics](@/docs/metrics.md) | `0` (off) → on demand |
| `enable.metrics.push` | `metrics_push(bool)` on `ProducerBuilder`, `ConsumerBuilder` and `AdminClient` (KIP-714) | `true` → `true` for producer and consumer, `false` for the admin client |

### Producer properties

| librdkafka property | krafka (`kafka.producer()`) | Default (librdkafka → krafka) |
|---|---|---|
| `acks` / `request.required.acks` | `acks(Acks::All)`; `Acks::Leader` is `1`, `Acks::None` is `0` | `-1` (all), same |
| `enable.idempotence` | `idempotent(bool)`; requires `Acks::All` | `false` → `true` |
| `compression.type` / `compression.codec` | `compression(Compression::…)`; the topic-level `compression.codec` is `topic_compression(topic, codec)`. Encoding zstd needs the `zstd` feature | `none`, same |
| `linger.ms` / `queue.buffering.max.ms` | `linger(Duration)` | 5 ms, same |
| `batch.size` | `batch_size(bytes)` | 1 000 000 B → 16 KiB |
| `batch.num.messages` | **No equivalent.** A batch is bounded by bytes (`batch_size`) only | 10 000 → — |
| `message.timeout.ms` / `delivery.timeout.ms` | `delivery_timeout(Duration)`; must be at least `linger` plus the handle's `request_timeout` | 300 s → 120 s |
| `retries` / `message.send.max.retries` | **No equivalent.** Retries are bounded by `delivery_timeout` alone; an idempotent producer retries until then | `2147483647` → until `delivery_timeout` |
| `queue.buffering.max.kbytes` | `buffer_memory(bytes)`; `send`/`enqueue` wait for space up to `max_block`, then fail with `KrafkaError::Timeout` | 1 048 576 KiB → 32 MiB |
| `transactional.id` | `build_transactional(id)` instead of `build()`, returning a `TransactionalProducer` | unset, same |
| `transaction.timeout.ms` | `transaction_timeout(Duration)` | 60 s, same |
| `partitioner` | `partitioner(impl Partitioner)` for a custom one (`RoundRobinPartitioner` is built in); `partitioner_rack_aware(true)` with `client_rack` for KIP-1123 | `consistent_random` (CRC32 of the key) → murmur2 of the key, sticky batches for keyless records — librdkafka's `murmur2_random`. **Keyed records land on different partitions** than under librdkafka's default; switch the rdkafka application to `partitioner=murmur2_random` before migrating if per-key placement must carry over |

### Consumer properties

| librdkafka property | krafka (`kafka.consumer(group)`) | Default (librdkafka → krafka) |
|---|---|---|
| `group.id` | the argument of `kafka.consumer(group)`; `kafka.consumer_without_group()` for a consumer that assigns its own partitions and commits nothing | — |
| `group.instance.id` | `group_instance_id(id)` | unset, same |
| `group.protocol` | `group_protocol(GroupProtocol::Classic \| GroupProtocol::Consumer)`; `group.remote.assignor` is `group_remote_assignor(name)` | `classic`, same |
| `auto.offset.reset` | `auto_offset_reset(AutoOffsetReset::Earliest \| Latest \| None)`; `error` is `AutoOffsetReset::None` | `largest`, same (`Latest`) |
| `enable.auto.commit` | `enable_auto_commit(bool)`. Auto-commit writes each partition's position: the offset after the last record handed to the application | `true`, same |
| `auto.commit.interval.ms` | `auto_commit_interval(Duration)` | 5 s, same |
| `session.timeout.ms` | `session_timeout(Duration)` | 45 s, same |
| `heartbeat.interval.ms` | `heartbeat_interval(Duration)` | 3 s, same |
| `max.poll.interval.ms` | `max_poll_interval(Duration)`. Exceeding it leaves the group; the next `poll()`/`recv()` reports the loss and rejoins | 300 s, same |
| `fetch.min.bytes` | `fetch_min_bytes(i32)` | 1, same |
| `fetch.wait.max.ms` | `fetch_max_wait(Duration)` | 500 ms, same |
| `fetch.max.bytes` | `fetch_max_bytes(i32)` | 52 428 800, same |
| `max.partition.fetch.bytes` | `max_partition_fetch_bytes(i32)`; per topic with `topic_fetch_max_bytes(topic, bytes)` | 1 048 576, same |
| `isolation.level` | `isolation_level(IsolationLevel::ReadCommitted \| ReadUncommitted)` | **`read_committed` → `ReadUncommitted`.** Set `ReadCommitted` explicitly if the rdkafka application relied on the default to hide aborted transactions |
| `partition.assignment.strategy` | `partition_assignment_strategies([..])` (preference order) or `partition_assignment_strategy(one)`; `Range`, `RoundRobin`, `CooperativeSticky` | `range,roundrobin` → `[Range, CooperativeSticky]`. Both advertise `range`, so a group of rdkafka and krafka members agrees on it during a rolling migration |
| `client.rack` | `client_rack(rack)` (fetch from the closest replica, KIP-392) | unset, same |

## The same settings in code

Each block starts from `Kafka::builder(bootstrap)` and sets every krafka
setting the tables name for that client. Values are examples, not
recommendations.

### Producer

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::producer::{Acks, RoundRobinPartitioner};
use krafka::{Compression, Kafka, Record};
use std::time::Duration;

// ssl.ca.location, ssl.certificate.location, ssl.key.location, ssl.key.password
let tls = TlsConfig::new()
    .with_ca_cert("/etc/kafka/ca.pem")
    .with_client_cert("/etc/kafka/client.pem", "/etc/kafka/client.key")
    .with_client_key_password("key-passphrase");

// security.protocol=SASL_SSL, sasl.mechanism=SCRAM-SHA-512, sasl.username, sasl.password
let security = AuthConfig::sasl_scram_sha512("orders", "secret").with_tls(tls);

let kafka = Kafka::builder("kafka1:9093,kafka2:9093") // bootstrap.servers
    .client_id("orders-service") // client.id
    .security(security)
    .request_timeout(Duration::from_secs(30)) // request.timeout.ms, socket.timeout.ms
    .tcp_keepalive(Some(Duration::from_secs(60))) // socket.keepalive.enable
    .metadata_max_age(Duration::from_secs(300)) // metadata.max.age.ms
    .max_in_flight_requests(10) // max.in.flight.requests.per.connection
    .connect()
    .await?;

let producer = kafka
    .producer()
    .acks(Acks::All) // acks
    .idempotent(true) // enable.idempotence
    .compression(Compression::Lz4) // compression.type
    .topic_compression("audit", Compression::Gzip) // topic compression.codec
    .linger(Duration::from_millis(5)) // linger.ms
    .batch_size(64 * 1024) // batch.size
    .delivery_timeout(Duration::from_secs(120)) // delivery.timeout.ms
    .retry_backoff(Duration::from_millis(100)) // retry.backoff.ms
    .buffer_memory(32 * 1024 * 1024) // queue.buffering.max.kbytes
    .max_block(Duration::from_secs(60))
    .client_rack("use1-az1") // client.rack
    .partitioner_rack_aware(true)
    .metrics_push(true) // enable.metrics.push
    .build()
    .await?;

// partitioner: a custom one replaces the built-in murmur2 partitioner.
let round_robin = kafka
    .producer()
    .partitioner(RoundRobinPartitioner::new())
    .build()
    .await?;

// transactional.id, transaction.timeout.ms
let transactional = kafka
    .producer()
    .transaction_timeout(Duration::from_secs(60))
    .build_transactional("orders-service-tx-1")
    .await?;

producer.send(Record::new("orders", "created").key("order-42")).await?;
```

### Consumer

```rust,compile
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::consumer::{
    AutoOffsetReset, GroupProtocol, IsolationLevel, PartitionAssignmentStrategy,
};
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("kafka1:9093,kafka2:9093") // bootstrap.servers
    .client_id("orders-reader") // client.id
    .security(AuthConfig::sasl_plain("reader", "secret").with_tls(TlsConfig::new()))
    .connect()
    .await?;

let consumer = kafka
    .consumer("orders") // group.id
    .group_instance_id("orders-reader-1") // group.instance.id
    .group_protocol(GroupProtocol::Classic) // group.protocol
    .auto_offset_reset(AutoOffsetReset::Earliest) // auto.offset.reset
    .enable_auto_commit(true) // enable.auto.commit
    .auto_commit_interval(Duration::from_secs(5)) // auto.commit.interval.ms
    .session_timeout(Duration::from_secs(45)) // session.timeout.ms
    .heartbeat_interval(Duration::from_secs(3)) // heartbeat.interval.ms
    .max_poll_interval(Duration::from_secs(300)) // max.poll.interval.ms
    .fetch_min_bytes(1) // fetch.min.bytes
    .fetch_max_wait(Duration::from_millis(500)) // fetch.wait.max.ms
    .fetch_max_bytes(50 * 1024 * 1024) // fetch.max.bytes
    .max_partition_fetch_bytes(1024 * 1024) // max.partition.fetch.bytes
    .topic_fetch_max_bytes("images", 8 * 1024 * 1024)
    .isolation_level(IsolationLevel::ReadCommitted) // isolation.level
    .partition_assignment_strategies([
        PartitionAssignmentStrategy::Range,
        PartitionAssignmentStrategy::CooperativeSticky,
    ]) // partition.assignment.strategy
    .client_rack("use1-az1") // client.rack
    .metrics_push(true) // enable.metrics.push
    .build()
    .await?;

// group.protocol=consumer with group.remote.assignor (KIP-848)
let next_gen = kafka
    .consumer("orders-v2")
    .group_protocol(GroupProtocol::Consumer)
    .group_remote_assignor("uniform")
    .build()
    .await?;

// A single strategy, and a consumer with no group.id
let eager = kafka
    .consumer("audit")
    .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin)
    .build()
    .await?;
let standalone = kafka.consumer_without_group().build().await?;

// statistics.interval.ms: pull a snapshot when you want one
let metrics = consumer.metrics();
let scrape = metrics.prometheus_text();
```

### Admin client

```rust,compile
use krafka::admin::{CreateTopicsOptions, ListTopicsOptions, NewTopic};
use krafka::auth::{AuthConfig, TlsConfig};
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("kafka1:9093,kafka2:9093") // bootstrap.servers
    .client_id("orders-admin") // client.id
    .security(AuthConfig::ssl(TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem")))
    .connect()
    .await?;

let admin = kafka
    .admin()
    .default_api_timeout(Duration::from_secs(60))
    .retry_backoff(Duration::from_millis(100)) // retry.backoff.ms
    .metrics_push(true); // enable.metrics.push

let results = admin
    .create_topics(
        [NewTopic::new("orders", 6, 3)?],
        CreateTopicsOptions::default().timeout(Duration::from_secs(30)),
    )
    .await?;
for (topic, result) in results {
    result?;
}
let topics = admin.list_topics(ListTopicsOptions::default()).await?;
```

## Type mapping

The rdkafka fragments are shown for orientation and are not compiled.

### `ClientConfig` → `Kafka::builder` and a role builder

```rust,ignore
// rdkafka
let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", "kafka1:9092").set("linger.ms", "5").create()?;
let consumer: StreamConsumer = ClientConfig::new().set("bootstrap.servers", "kafka1:9092").set("group.id", "orders").create()?;
```

```rust,compile
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("kafka1:9092").connect().await?;
let producer = kafka.producer().linger(Duration::from_millis(5)).build().await?;
let consumer = kafka.consumer("orders").build().await?;
```

The two clients share one connection pool and one metadata cache, so the
connection settings are written once; a second identity or other credentials
is a second `Kafka` handle.

### `FutureProducer` → `Producer::send` / `Producer::enqueue`

```rust,ignore
// rdkafka
let delivery = producer.send(FutureRecord::to("orders").key("order-42").payload("created"), Duration::from_secs(5)).await;
```

```rust,compile
use krafka::Record;
use std::time::Duration;

// Wait for the acknowledgement, like awaiting FutureProducer::send.
let metadata = producer.send(Record::new("orders", "created").key("order-42")).await?;
println!("partition {} offset {}", metadata.partition, metadata.offset);

// Return once queued, like FutureProducer::send_result: the DeliveryHandle
// resolves to the broker's answer. Produce order is enqueue order.
let handle = producer.enqueue(Record::new("orders", "updated").key("order-42")).await?;
let partition = handle.partition();
let metadata = tokio::time::timeout(Duration::from_secs(30), handle)
    .await
    .map_err(|_| KrafkaError::timeout("waiting for the acknowledgement"))??;
```

There is no per-call `queue_timeout`: waiting for metadata and buffer memory is
bounded by the producer's `max_block`, and a failed delivery returns a
`KrafkaError`, not the record (rdkafka hands back the `OwnedMessage`), so keep
the record yourself if you resend it. `KrafkaError::DeliveryTimeout` says
whether the record may have been written (`possibly_written`).

### `StreamConsumer` → `Consumer::recv` and `Consumer::stream`

```rust,ignore
// rdkafka
loop {
    let message = consumer.recv().await?;
}
```

```rust,compile
// StreamExt comes from the `futures` crate.
use futures::StreamExt;

consumer.subscribe(["orders"]).await?;
while let Some(record) = consumer.recv().await? {
    println!("{}-{}@{}", record.topic, record.partition, record.offset);
}

let mut stream = consumer.stream();
while let Some(record) = stream.next().await {
    let record = record?;
}
```

`recv()` returns `Ok(None)` once the consumer is closed, from this task or
another, so the loop ends instead of blocking; both are cancel safe: a dropped
`recv()` loses no record and moves no position.

### `BaseConsumer` → `Consumer::poll`

```rust,ignore
// rdkafka
if let Some(result) = consumer.poll(Duration::from_millis(100)) {
    let message = result?;
}
```

```rust,compile
use std::time::Duration;

for record in consumer.poll(Duration::from_millis(100)).await? {
    println!("{:?}", record.value_str());
}
```

`poll` returns a batch of up to `max_poll_records` (default 500) instead of one
message, and an empty batch when the timeout passes; rebalances, rebalance
listener callbacks and auto-commit run inside it.

### `AdminClient` → `kafka.admin()`

```rust,ignore
// rdkafka
let admin: AdminClient<DefaultClientContext> = ClientConfig::new().set("bootstrap.servers", "kafka1:9092").create()?;
let results = admin.create_topics(&[NewTopic::new("orders", 6, TopicReplication::Fixed(3))], &AdminOptions::new()).await?;
```

```rust,compile
use krafka::admin::{CreateTopicsOptions, NewTopic};

let admin = kafka.admin();
let results = admin
    .create_topics([NewTopic::new("orders", 6, 3)?], CreateTopicsOptions::default())
    .await?;
```

`kafka.admin()` creates no connection of its own and contacts no broker until
its first call. `AdminOptions` becomes one options struct per call, whose
`timeout` bounds the whole call; results come back keyed by topic name. See
[Admin Client](@/docs/admin.md).

### `Message` / `BorrowedMessage` → `ConsumerRecord`

```rust,ignore
// rdkafka
let payload = message.payload_view::<str>();
let owned = message.detach();
```

```rust,compile
if let Some(record) = consumer.recv().await? {
    let value: Option<&str> = record.value_str();
    let key = record.key.clone(); // Option<bytes::Bytes>
    let trace = record.header_str("traceparent");
    let (topic, partition, offset) = (record.topic.clone(), record.partition, record.offset);
}
```

A `ConsumerRecord` is owned: its fields are public, key and value are
`bytes::Bytes`, and it outlives the consumer call that returned it, so there is
no `detach()`.

### `OwnedHeaders` → `krafka::Headers`

```rust,ignore
// rdkafka
let headers = OwnedHeaders::new().insert(Header { key: "traceparent", value: Some("00-…") });
```

```rust,compile
use krafka::{Headers, Record};

let record = Record::new("orders", "created")
    .header("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
    .null_header("deleted-by");
let headers: &Headers = &record.headers;
```

`Headers` is `Vec<(String, Option<Bytes>)>`, the same type on a produced
`Record` and a `ConsumerRecord`: order and duplicate keys are kept, and `None`
is a null value, distinct from an empty one.

### rdkafka error codes → `KrafkaError` predicates

```rust,ignore
// rdkafka
match error.rdkafka_error_code() {
    Some(RDKafkaErrorCode::QueueFull) => { /* back off */ }
    _ => {}
}
```

```rust,compile
use krafka::Record;

match producer.send(Record::new("orders", "created")).await {
    Ok(_) => {}
    Err(KrafkaError::DeliveryTimeout { possibly_written, .. }) => {
        // The whole delivery budget is spent; possibly_written says whether a resend may duplicate.
    }
    Err(e) if e.is_fatal() => return Err(e), // rebuild the client
    Err(e) if e.is_retriable() => {}         // try the operation again
    Err(e) if e.requires_abort() => {}       // abort the open transaction
    Err(e) => return Err(e),
}
```

`is_retriable()`, `is_fatal()` and `requires_abort()` correspond to
`RDKafkaError::is_retriable`, `is_fatal` and `txn_requires_abort`. A broker
error code is `KrafkaError::Broker { code, .. }` with `code` a
`krafka::error::ErrorCode`. There is no `QueueFull`: `send` and `enqueue` wait
for buffer memory up to `max_block` and then fail with `KrafkaError::Timeout`.
See [Error Handling](@/docs/errors.md).

### `commit_consumer_state` / `CommitMode` → `commit` / `commit_offsets`

```rust,ignore
// rdkafka
consumer.commit_consumer_state(CommitMode::Async)?;
consumer.commit_message(&message, CommitMode::Sync)?;
```

```rust,compile
use krafka::consumer::{OffsetAndMetadata, TopicPartition};

// commit_consumer_state: the position of every assigned partition.
consumer.commit().await?;

// commit / commit_message: exactly these offsets (the next offset to read).
if let Some(record) = consumer.recv().await? {
    let partition = TopicPartition::new(record.topic.to_string(), record.partition);
    let next = OffsetAndMetadata::new(record.offset + 1);
    consumer.commit_offsets([(partition, next)]).await?;
}
```

There is no `CommitMode`: each commit is a future that completes when the
coordinator answers, and commits from one consumer are sent one at a time in
call order. Periodic background commits are `enable_auto_commit`.
