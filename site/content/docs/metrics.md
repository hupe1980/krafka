+++
title = "Metrics"
description = "Each client's metrics snapshot, the Prometheus text format, the spans krafka emits, and KIP-714 client telemetry."
weight = 100

[extra]
slug_id = "metrics"
+++

## Overview

krafka reports what its clients do in three ways:

- **Metrics.** Every client's `metrics()` returns one owned `Metrics`
  snapshot. `Kafka::metrics()` sums the clients of a handle, and
  `Metrics::prometheus_text()` renders a snapshot for a scrape endpoint.
- **Spans.** The clients emit spans through `tracing`, named and attributed
  per the OpenTelemetry messaging semantic conventions.
- **KIP-714 telemetry.** Producers and consumers push their metrics to the
  brokers when the cluster operator has subscribed to them.

## Reading a client's metrics

`metrics()` is synchronous on every client — `Producer`,
`TransactionalProducer`, `Consumer`, `ShareConsumer` and `AdminClient` — and on
`Kafka`. It returns the same type everywhere: a `Metrics` value with a
`producer`, a `consumer` and a `connections` section. The sections a client
does not use read zero, and `connections` is the pool the client shares with
every client of its `Kafka` handle.

```rust,compile
use krafka::{Kafka, Record};

let kafka = Kafka::builder("localhost:9092").client_id("orders").connect().await?;
let producer = kafka.producer().build().await?;
producer.send(Record::new("orders", "v")).await?;

let metrics = producer.metrics();
println!(
    "{} records sent, {} connections opened, mean send latency {:?}",
    metrics.producer.records_sent,
    metrics.connections.connections_created,
    metrics.producer.send_latency.mean(),
);
```

A snapshot is a copy: later activity does not change it. Counters are
monotonic for a client's lifetime and there is no reset; take two snapshots
and subtract to get a rate.

A latency is a `Latency { count, sum, max }`: the number of samples, their
total and the largest one. `mean()` is `sum / count`, or `None` without
samples. There are no percentiles; a histogram belongs to the metrics backend
the snapshot is exported to.

`Kafka::metrics()` adds up the producer and consumer counters of every live
client built from the handle and counts the shared pool's connections once.
A dropped client's counters leave the sum. Its `client_id` is `None`.

## Prometheus

`prometheus_text()` renders a snapshot in the Prometheus text exposition
format under `krafka_*` names. A client's snapshot labels every series with
`client_id`; the `Kafka` sum carries no client label. Serve the sum. In a web
framework the handler is `kafka.metrics().prometheus_text()`; with nothing but
Tokio it is a few lines:

```rust,compile
use krafka::Kafka;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn serve_metrics(kafka: Kafka, addr: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let body = kafka.metrics().prometheus_text();
        tokio::spawn(async move {
            let mut request = [0u8; 1024];
            let _ = socket.read(&mut request).await;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

// Scrape http://<host>:9464/metrics.
tokio::spawn(serve_metrics(kafka.clone(), "0.0.0.0:9464"));
```

Every client of one handle shares its `client_id`, so rendering two clients
of one handle into one scrape produces duplicate series. Scrape
`Kafka::metrics()`, or give separate handles distinct `client_id`s.

The snapshot's fields are public, so any other format — StatsD, JSON, an
OpenTelemetry meter — is a function of the snapshot in the application.

Latencies are summaries (`_seconds_sum`, `_seconds_count`) with a `_max_seconds`
gauge beside each:

```promql
# Mean send latency over 5 minutes
rate(krafka_producer_send_latency_seconds_sum[5m])
  / rate(krafka_producer_send_latency_seconds_count[5m])
```

### Producer

| Prometheus name | KIP-714 name (`org.apache.kafka.producer.`…) | Type | Description |
|---|---|---|---|
| `krafka_producer_records_sent_total` | `record.send.total` | counter | Records the broker acknowledged |
| `krafka_producer_bytes_sent_total` | `record.byte.total` | counter | Estimated bytes of the acknowledged records |
| `krafka_producer_batches_sent_total` | `batch.send.total` | counter | Batches the broker acknowledged |
| `krafka_producer_errors_total` | `batch.error.total` | counter | Batches that failed with a terminal error |
| `krafka_producer_retries_total` | `batch.retry.total` | counter | Batch retries |
| `krafka_producer_data_loss_detected_total` | `data.loss.detected.total` | counter | Batches the broker reported lost |
| `krafka_producer_compressed_bytes_total` | `batch.compressed.byte.total` | counter | Estimated encoded bytes of compressed batches |
| `krafka_producer_uncompressed_bytes_total` | `batch.uncompressed.byte.total` | counter | The same batches before compression |
| `krafka_producer_buffered_records` | `buffered.records` | gauge | Records held under `buffer_memory` |
| `krafka_producer_send_latency_seconds` | `record.send.latency.avg`/`.max` (ms) | summary | First record of a batch to its acknowledgement |
| `krafka_producer_topic_records_sent_total{topic}` | — | counter | Per topic |
| `krafka_producer_topic_bytes_sent_total{topic}` | — | counter | Per topic |
| `krafka_producer_topic_errors_total{topic}` | — | counter | Per topic |

At most 1000 topics get their own series; the rest are folded into
`topic="__other__"`. `ProducerMetrics::compression_ratio()` is
`compressed / uncompressed`.

### Consumer and share consumer

| Prometheus name | KIP-714 name (`org.apache.kafka.consumer.`…) | Type | Description |
|---|---|---|---|
| `krafka_consumer_records_received_total` | `fetch.manager.records.consumed.total` | counter | Records handed to the application |
| `krafka_consumer_bytes_received_total` | `fetch.manager.bytes.consumed.total` | counter | Value bytes handed to the application |
| `krafka_consumer_fetches_total` | `fetch.manager.fetch.total` | counter | Fetch requests |
| `krafka_consumer_polls_total` | `poll.total` | counter | `poll`/`recv` rounds |
| `krafka_consumer_empty_polls_total` | `poll.empty.total` | counter | Rounds that returned nothing |
| `krafka_consumer_commits_total` | `coordinator.commit.total` | counter | Offset or acknowledgement commits |
| `krafka_consumer_errors_total` | `error.total` | counter | Errors returned from `poll`/`recv` |
| `krafka_consumer_rebalances_total` | `coordinator.rebalance.total` | counter | Assignment changes applied |
| `krafka_consumer_seeks_total` | `seek.total` | counter | Partitions repositioned |
| `krafka_consumer_batch_decode_errors_total` | `fetch.manager.batch.decode.error.total` | counter | Corrupt record batches |
| `krafka_consumer_lag` | `fetch.manager.records.lag.total` | gauge | Records behind, summed over assigned partitions |
| `krafka_consumer_lag_max` | `fetch.manager.records.lag.max` | gauge | Largest per-partition lag |
| `krafka_consumer_assigned_partitions` | `coordinator.assigned.partitions` | gauge | Assigned partitions |
| `krafka_consumer_paused_partitions` | `paused.partitions` | gauge | Paused partitions |
| `krafka_consumer_buffered_records` | `buffered.records` | gauge | Fetched, not yet handed out |
| `krafka_consumer_poll_latency_seconds` | `poll.latency.avg`/`.max` (ms) | summary | `poll` rounds |
| `krafka_consumer_fetch_latency_seconds` | `fetch.manager.fetch.latency.avg`/`.max` (ms) | summary | Fetch round trips |

**Alert on `krafka_consumer_batch_decode_errors_total`.** It counts batches
whose bytes are corrupt — not batches cut short by the fetch size, which are
re-requested. A partition whose batch will not decode cannot advance; `poll()`
returns the error, and every increment logs a `warn!` naming the topic,
partition and offset.

Under `read_committed`, lag is measured against the last stable offset.

### Connections

| Prometheus name | KIP-714 name (`org.apache.kafka.<client>.`…) | Type | Description |
|---|---|---|---|
| `krafka_connections_created_total` | `connection.creation.total` | counter | Connections opened |
| `krafka_connections_closed_total` | `connection.close.total` | counter | Connections closed |
| `krafka_connection_errors_total` | `connection.error.total` | counter | Connections that failed |
| `krafka_connections_active` | `connection.count` | gauge | Connections open now |
| `krafka_throttle_delays_total` | `throttle.delay.total` | counter | Requests held back by a broker throttle (KIP-219) |
| `krafka_throttle_delay_ms_total` | `throttle.delay.ms.total` | counter | Milliseconds held back |
| `krafka_connections_stalled_total` | `connection.stalled.total` | counter | Connections closed on a request timeout |
| `krafka_coordination_fallbacks_total` | `coordination.fallback.total` | counter | Coordination requests sent on the data connection at the connection cap |
| `krafka_tls_handshake_latency_seconds` | `tls.handshake.latency.avg`/`.max` (ms) | summary | TLS handshakes |
| `krafka_oauth_token_fetches_total` | `oauth.token.fetch.total` | counter | OAUTHBEARER token fetches |
| `krafka_oauth_token_fetch_failures_total` | `oauth.token.fetch.failure.total` | counter | Fetches that failed |
| `krafka_oauth_token_fetch_latency_seconds` | `oauth.token.fetch.latency.avg`/`.max` (ms) | summary | Successful fetches |
| `krafka_oauth_token_expiry_epoch_ms` | `oauth.token.expiry.epoch.ms` | gauge | Cached token expiry; `0` when unknown |

The `oauth_*` metrics are populated only with an OAUTHBEARER token
**provider**. They make a misconfigured `token_endpoint` visible: without them
it looks like an unreachable broker. A failed fetch counts in both
`oauth_token_fetches_total` and `oauth_token_fetch_failures_total` and leaves
the expiry alone.

```promql
rate(krafka_oauth_token_fetch_failures_total[5m]) > 0
(krafka_oauth_token_expiry_epoch_ms / 1000) - time()
```

## Spans

The clients emit spans through `tracing`, following OpenTelemetry semantic
conventions **v1.44.0** (`krafka::OTEL_SEMCONV_VERSION`). The messaging
conventions are still marked *Development* there; krafka follows one named
version and moves to a newer one as a breaking change.

| Span | When | `otel.kind` | `messaging.operation.type` |
|---|---|---|---|
| `send {topic}` | One per record, from `send`/`enqueue` to the record's outcome, across retries | `producer` | `send` |
| `poll {topic}` | One per `poll`/`recv` of a consumer or share consumer | `client` | `receive` |
| `commit {topic}` | One per offset commit (auto-commit included) or share `commit()` | `client` | `settle` |
| `rebalance {group}` | One per assignment change, classic or KIP-848 | `internal` | — |

The topic is left out of a `poll` or `commit` name when more than one topic
applies. There is no `process` span: krafka hands records to the application
and does not run its processing.

Attributes: `messaging.system` (`kafka`), `messaging.operation.name`,
`messaging.operation.type`, `messaging.destination.name`,
`messaging.destination.partition.id` (a string), `messaging.kafka.offset`
(once acknowledged), `messaging.kafka.message.tombstone` (tombstones only),
`messaging.consumer.group.name`, `messaging.client.id`,
`messaging.batch.message_count` (records a `poll` returned), and `error.type`
with `otel.status_code = error` on failure. `error.type` is the Kafka error
name where the broker sent one (`INVALID_RECORD`), otherwise krafka's error
kind (`delivery_timeout`, `closed`). krafka's own attributes are under
`krafka.*`: `krafka.message.key.size`, and on `rebalance` the protocol,
generation, and the partitions assigned, revoked and held.

Record keys and values are never recorded, and neither is anything derived
from a key but its size.

The span name and kind travel in the `otel.name` and `otel.kind` fields, which
the `tracing-opentelemetry` bridge reads. krafka depends on no OpenTelemetry
crate (`just no-otel` checks the dependency graph); the subscriber, the bridge
and the SDK belong to the application. Without a subscriber interested in the
`krafka` targets no span is constructed, so the cost is one callsite check per
operation. To drop krafka's spans, filter its targets in the subscriber — for
example `EnvFilter::new("info,krafka=warn")` with `tracing-subscriber`.

The spans are at `INFO` level, under the targets `krafka::producer` and
`krafka::consumer`. To see them without an OpenTelemetry pipeline, log each
one when it closes:

```rust,compile
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

let filter = EnvFilter::new("warn,krafka=info");
tracing_subscriber::fmt().with_env_filter(filter).with_span_events(FmtSpan::CLOSE).init();
```

To export them, add the `tracing-opentelemetry` layer to the subscriber with
the application's OpenTelemetry tracer.

## KIP-714 client telemetry

Every client can push its metrics to the brokers, as Java clients do with
`enable.metrics.push`. A client asks a broker for its subscription
(`GetTelemetrySubscriptions`), pushes the subscribed metrics of its `Metrics`
snapshot as OTLP at the interval the broker sets (`PushTelemetry`), and sends
one last push marked `terminating` when it closes, within the close timeout.

| Client | Default |
|---|---|
| `Producer`, `TransactionalProducer` | on |
| `Consumer`, `ShareConsumer` | on |
| `AdminClient` | off |

The switch is `metrics_push` on every role builder:

```rust,compile
use krafka::Kafka;

let kafka = Kafka::builder("localhost:9092").connect().await?;
let producer = kafka.producer().metrics_push(false).build().await?;
let consumer = kafka.consumer("my-group").metrics_push(false).build().await?;
let admin = kafka.admin().metrics_push(true);
```

With the switch off a client never sends API key 71 or 72. With it on,
nothing is sent to a cluster without a client-telemetry plugin: such a broker
does not advertise the two APIs, and the reporter stops at once, logging only
at `debug`. A subscription that requests no metrics is re-polled once per push
interval and never pushed to.

Pushed names are `org.apache.kafka.<client type>.<metric>` with
`producer`, `consumer` (the share consumer too) or `admin` as the client type —
the KIP-714 names in the tables above, Java's where Java has the same metric.
A subscription prefix such as `org.apache.kafka.producer.` selects them.
Latencies are pushed as `.avg` and `.max` in milliseconds.

The broker-assigned client instance id lets an operator find a client's
metrics:

```rust,compile
use std::time::Duration;
use krafka::Kafka;

let kafka = Kafka::builder("localhost:9092").connect().await?;
let producer = kafka.producer().build().await?;
match producer.client_instance_id(Duration::from_secs(5)).await? {
    Some(id) => println!("client instance id {id}"),
    None => println!("the cluster does not support client telemetry"),
}
```

It fails with `KrafkaError::IllegalState` when `metrics_push` is off and with
`KrafkaError::Timeout` when no broker answers in time.

## Next Steps

- [Producer Guide](@/docs/producer.md)
- [Consumer Guide](@/docs/consumer.md)
- [Interceptors](@/docs/interceptors.md) — per-record context from `on_send` to its outcome
- [Configuration Reference](@/docs/configuration.md)
