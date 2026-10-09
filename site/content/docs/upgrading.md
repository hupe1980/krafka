+++
title = "Upgrading to 0.27"
description = "Every breaking change in krafka 0.27 with the old API, the new API and what to do: the Kafka handle, send/enqueue, recv, metrics."
weight = 22

[extra]
slug_id = "upgrading"
+++

This page maps the 0.26 API to 0.27, area by area. The complete list is the
release's `Breaking` and `Removed` sections in
[CHANGELOG.md](https://github.com/hupe1980/krafka/blob/main/CHANGELOG.md).
Start with construction, then each client you use, then metrics and tests.

## Building clients

Connection settings are on `KafkaBuilder`; each client is built from the
handle and shares its connection pool.

| 0.26 | 0.27 |
|---|---|
| `Producer::builder().bootstrap_servers(b)` | `Kafka::builder(b).connect().await?` then `kafka.producer()` |
| `Consumer::builder().bootstrap_servers(b).group_id(g)` | `kafka.consumer(g)` |
| a consumer without a group | `kafka.consumer_without_group()` |
| `ShareConsumer::builder().group_id(g)` | `kafka.share_consumer(g)` |
| `AdminClient::builder()…build()` | `kafka.admin()` |
| `KrafkaClient`, `with_client(&client)`, `owns_pool` | the `Kafka` handle; a second pool is a second `Kafka` |
| `client_id`, `auth`/`sasl_*`, `proxy`, `transport`/`TransportConfig`, `request_timeout`, `connect_timeout`, metadata settings on a role builder | the same setting on `KafkaBuilder` (`security` replaces `auth`) |
| `refresh_tls`, `update_seed_brokers`, `rebootstrap` on a client | the same method on `Kafka` |
| `Producer::close()` returning `()`; `close_with_timeout(d)` | `close()` returns `Result<()>`; `close_with(CloseOptions::new().timeout(d))` |
| `Arc<dyn Interceptor>` and friends | pass by value (`impl Trait`); an `Arc<T>` still works |

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};
use std::time::Duration;

let kafka = Kafka::builder("broker:9093")
    .client_id("payments")
    .security(AuthConfig::sasl_scram_sha512("user", "secret").with_tls(TlsConfig::new()))
    .request_timeout(Duration::from_secs(30))
    .connect()
    .await?;

let producer = kafka.producer().build().await?;
let consumer = kafka.consumer("payments-group").build().await?;
let admin = kafka.admin();

producer.close().await?;
consumer.close().await?;
admin.close().await?;
```

`protocol`, `network` and `metadata` are private modules. `BrokerInfo`,
`PartitionInfo`, `TopicInfo`, `MetadataRecoveryStrategy`, `ProxyConfig`,
`TimestampType` and `Compression` are re-exported at the crate root.

## Cargo features

| 0.26 | 0.27 |
|---|---|
| `gzip`, `snappy`, `lz4`, share consumer, SOCKS5, telemetry features | always compiled in; remove them from `features = [...]` |
| zstd needed the `zstd` feature to read | zstd decodes in every build; `zstd` is needed only to *encode* |
| FIPS wording on `rustls-aws-lc-rs` | no FIPS claim; the feature offers post-quantum `X25519MLKEM768` first |

The features are `ring` (default), `rustls-aws-lc-rs`, `zstd`, `aws-msk`,
`oauth-oidc`, `native-tls-roots`, `tls-encrypted-keys`, `unstable-protocol`
and `test-broker`.

## Errors

| 0.26 | 0.27 |
|---|---|
| `KrafkaError::InvalidState` | `IllegalState` |
| `KrafkaError::Http` | an OIDC failure is `Auth` with a `source` |
| fencing reported as a broker code | `Fenced` |
| abort-required reported as a message | `TransactionAbortable`; check `requires_abort()` |
| no committed offset with `auto_offset_reset = None` | `NoOffset` |
| `max_connections` reached: `Config` | retriable `Network` |
| TLS handshake reset or EOF: `Auth` | retriable `Network` |

New kinds: `Closed`, `Wakeup`, `UnknownTopic`,
`DeliveryTimeout { possibly_written }` and `OutOfOrderSequence`. Branch on
`is_retriable()`, `is_fatal()` and `requires_abort()` rather than on kinds
where you can. See [Error Handling](@/docs/errors.md).

## Producer

| 0.26 | 0.27 |
|---|---|
| `send("t", key, value)`, `send_with_headers`, `send_record(rec)` | `send(Record::new("t", value).key(k).header(..))` |
| `ProducerRecord` | `Record` (`key`, `header`, `null_header`, `partition`, `timestamp`) |
| `RecordHeaders` | `krafka::Headers` (`Vec<(String, Option<Bytes>)>`) |
| `retries(n)` | removed: retries run until `delivery_timeout` |
| `linger` default 0 on `Producer` | 5 ms on every producer |
| `buffer_memory(0)` = unbounded | rejected; pass a size |
| `key_serializer`/`value_serializer`, async `Serializer` | `TypedProducer<K, V>` with the synchronous `serdes::Serializer<T>` |
| `dead_letter_queue`, `DeadLetterQueue`, `KafkaDeadLetterQueue` | removed from the producer; build a dead-letter record from a consumed one with `dlq::record_for` |
| `DefaultPartitioner`, `StickyPartitioner`, `HashPartitioner`, `UniformStickyPartitioner` | the built-in partitioner (no type to name); implement `Partitioner` for custom routing |
| `Partitioner::on_new_batch` | removed |
| `state_store`, `ProducerStateStore` | removed |
| `metrics_handle()`, `connection_metrics()` | `metrics()` |
| `on_acknowledgement(.., DeliveryConfirmation::Failed ..)` | `on_acknowledgement(topic, partition, result: Result<&RecordMetadata, &KrafkaError>, headers, ctx)` |

`delivery_timeout` must be at least `linger + request_timeout`, or `build()`
fails. A partitioner that returns a partition outside `[0, partition_count)`
fails that send with `Config`.

## Transactions

| 0.26 | 0.27 |
|---|---|
| `TransactionalProducer::builder()…transactional_id(id).build()` then `init_transactions()` | `kafka.producer().….build_transactional(id).await?` (registers the id) |
| `init_transactions_keeping_prepared()` | `build_transactional`, then `prepared_transaction()` |
| `begin_transaction`, `commit_transaction`, `abort_transaction` | `begin`, `commit`, `abort` |
| `prepare_transaction`, `complete_transaction` | `prepare`, `complete` |
| `send_record(rec)` | `send(rec)` |
| `send_offsets_to_transaction` | `send_offsets(&offsets, &group_metadata)` |
| `TransactionalDeliveryHandle` | `DeliveryHandle` from `enqueue` |

A commit after any failed send of the transaction refuses with
`TransactionAbortable`, whether or not you awaited that send's handle; call
`abort()`. `abort()` fails the transaction's buffered records instead of
sending them.

```rust,compile
use krafka::{Kafka, Record};

let kafka = Kafka::builder("localhost:9092").connect().await?;
let producer = kafka.producer().build_transactional("orders-tx").await?;

producer.begin()?;
producer.send(Record::new("orders", "o-1").key("c-7")).await?;
match producer.commit().await {
    Ok(()) => {}
    Err(e) if e.requires_abort() => producer.abort().await?,
    Err(e) => return Err(e),
}
```

## Consumer

| 0.26 | 0.27 |
|---|---|
| `recv() -> Result<ConsumerRecord>`, `RecvError` | `recv() -> Result<Option<ConsumerRecord>>`; `Ok(None)` means closed |
| `batch_recv(n, timeout)`, `BatchRecvOutcome` | `poll(timeout)` |
| `subscribe(&["a", "b"])` | `subscribe(["a", "b"])`, any iterator of strings |
| `commit_sync()` | `commit()` |
| `commit_async()`, `OffsetCommitHandle` | spawn `commit()` on a task, or commit less often |
| `commit_with_metadata(..)` | `commit_offsets(offsets)` with `OffsetAndMetadata::with_metadata`; any iterator of pairs, `&HashMap` included |
| `seek_many(&HashMap<(String, i32), i64>)`, `initial_offsets(HashMap<(String, i32), i64>)` | an iterator of `(TopicPartition, offset)` pairs |
| `ahash` maps and sets in return types | `std::collections::HashMap` / `HashSet` |
| `current_lag()`, `is_caught_up()`, `fetch_end_offset()`, `cached_*` | `lag()` → `HashMap<TopicPartition, PartitionLag>` |
| `revocation_timeout`, `max_cooperative_rebalance_rounds` | removed; `on_partitions_revoked` is awaited to completion |
| `PartitionAssignmentStrategy::Sticky` | `CooperativeSticky`, `Range` or `RoundRobin` |
| `PartitionAssignor` trait, `ConsumerGroup`, `GroupCoordinator` | removed; assignment strategies are the enum |
| `AutoOffsetReset::to_offset()` | removed; `AutoOffsetReset::ByDuration(d)` is new |
| `ConsumerRecord::topic: String` | `Arc<str>` |
| async `Deserializer` | synchronous `deserialize(&self, topic, headers, payload, is_key)` |

Behaviour to check in your code:

- **Position.** A partition's position is the next record `poll`/`recv` will
  hand out. It moves only when records are returned; committing never moves
  it.
- **Seeking.** `seek` and its variants fail with `IllegalState` for a
  partition that is not assigned. `seek_to_beginning` goes to the log start
  offset.
- **Subscribing.** `subscribe()` returns before the group is joined;
  `poll()` applies the assignment and calls your listener.
- **Poll-interval expiry.** The next `poll()` reports the partitions to
  `on_partitions_lost` once, rejoins and returns normally.

```rust,compile
use krafka::Kafka;
use krafka::consumer::{OffsetAndMetadata, TopicPartition};

let kafka = Kafka::builder("localhost:9092").connect().await?;
let consumer = kafka.consumer("billing").build().await?;
consumer.subscribe(["invoices"]).await?;

while let Some(record) = consumer.recv().await? {
    let offsets = [(
        TopicPartition::new(record.topic.as_ref(), record.partition),
        OffsetAndMetadata::with_metadata(record.offset + 1, "billing-v2"),
    )];
    consumer.commit_offsets(offsets).await?;
}
```

## Share consumer

| 0.26 | 0.27 |
|---|---|
| `acknowledge(&rec, AcknowledgeType::Accept).await` | `ack(&rec)` (synchronous) |
| `AcknowledgeType::Release` / `Reject` | `release(&rec)` / `reject(&rec)` |
| no renewal | `renew(&rec)` (Kafka 4.2+ with KIP-1222) |
| `acknowledge_by_offset` | removed; acknowledge the record |
| `commit_sync()`, `commit_sync_with_timeout`, `commit_async`, `ShareCommitHandle` | `commit()` → `CommitResults`, one `Result` per partition |
| `close_with_timeout(d)` | `close_with(CloseOptions::new().timeout(d))` |
| `max_buffered_records`, `max_records`, `session_timeout`, `heartbeat_interval` | removed; `max_poll_records` sets `MaxRecords` on every fetch |

Implicit mode accepts the previous delivery when the next `poll()`/`recv()`
starts, and on `close()`. See [Share Consumer](@/docs/share-consumer.md).

## Admin client

Every operation is one method taking krafka types and an `*Options` struct
(`Default`, with a `timeout`). Multi-item operations return
`HashMap<Item, Result<T, KrafkaError>>`.

| 0.26 | 0.27 |
|---|---|
| `describe_consumer_group_offsets` | `list_consumer_group_offsets` (`require_stable` replaces `OffsetVisibility`) |
| `alter_topic_config` | `incremental_alter_configs` with `ConfigResource` and `ConfigOp` |
| `describe_configs(topic names)`, `describe_configs_per_resource`, `topic_config` | `describe_configs` over `ConfigResource`s |
| `describe_topic_partitions`, `describe_topic`, `partition_count` | `describe_topics` |
| `alter_partition_reassignments_opts`, `abort_transaction_with_epoch` | an option of `alter_partition_reassignments` / `abort_transaction` |
| `list_client_metrics_resources` | `list_config_resources` |
| `GroupListing` | `ListConsumerGroupsOptions` |
| `retries` on the admin builder | removed; calls are bounded by `default_api_timeout` (60 s) or the options' `timeout` |
| `write_txn_markers`, `get_controller_connection`, `pool` | removed |

`list_consumer_groups` and `list_transactions` return a result per broker.
`default_api_timeout` and `retry_backoff` are set on the client
`kafka.admin()` returns. See [Admin Client](@/docs/admin.md).

## Security

| 0.26 | 0.27 |
|---|---|
| `AuthConfig::sasl_plain_ssl(u, p, tls)` and the other `_ssl` constructors | `AuthConfig::sasl_plain(u, p).with_tls(tls)` |
| `AuthConfig::sasl_plain(..)?` (fallible) | infallible; bad credentials fail at `connect()` |
| `with_scram_channel_binding`, `ChannelBinding` | removed; SCRAM sends `n,,` |
| `OAuthBearerTokenProvider`, `AwsMskIamCredentialProvider` | `auth::CredentialProvider<C>`; an async closure still works |
| `AssertionSource` variants | `AssertionSource::file`, `fixed`, `provider` |
| a key passphrase without a client certificate was ignored | `TlsConfig` build fails |

See [Authentication](@/docs/authentication.md).

## Metrics and tracing

| 0.26 | 0.27 |
|---|---|
| `KrafkaMetrics`, per-client snapshot types, `metrics_handle()`, `connection_metrics()` | `metrics()` → one owned `krafka::metrics::Metrics` with `producer`, `consumer`, `connections`; `kafka.metrics()` sums the handle |
| `PrometheusExporter`, `JsonExporter`, `MetricsExporter` | `Metrics::prometheus_text()`; `Metrics` is plain data for any other format |
| `LatencySnapshot` (min, percentiles) | `Latency { count, sum, max }` |
| Prometheus names | `krafka_producer_*`, `krafka_consumer_*`, `krafka_connections_*`, each with a `client_id` label; update dashboards |
| `tracing_ext` module | spans are emitted by the clients; see [Metrics](@/docs/metrics.md) |
| KIP-714 telemetry off | on by default for producers and consumers; `metrics_push(false)` turns it off |

## The fake broker

`krafka::testing` is outside semver.

| 0.26 | 0.27 |
|---|---|
| `TransactionState` | `BrokerTransaction` (`status: TxnStatus`, `is_open()`) |
| `ApiKey` from the protocol module | `krafka::testing::ApiKey` |
| `RecordedRequest` | gains an `at` field |
| lenient sequence, transaction and share-session checks | enforced as Kafka does; a request Kafka rejects is rejected |

See [Testing](@/docs/testing.md).
