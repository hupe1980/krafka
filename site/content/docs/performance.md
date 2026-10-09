+++
title = "Performance"
description = "What krafka optimises by default, which settings move throughput, and how to measure."
weight = 110

[extra]
slug_id = "performance"
+++

## Benchmarking

| Command | What it measures |
|---------|------------------|
| `just bench` | The criterion suite in `benches/`: varints, CRC32C, murmur2, record-batch encode/decode, compression, the partitioners, and the send and consume paths against `krafka::testing::FakeBroker` |
| `just bench-baseline` | Saves a baseline for `bench-check`; run it on a known-good commit |
| `just bench-check` | Compares `benches/send_path.rs` and `benches/consume_path.rs` against the baseline; fails when a measurement regresses by more than 10 % with a confidence interval excluding zero. It never overwrites the baseline |

Run `bench-check` after touching the send path, the accumulator, the codecs or
the consume path.

The fake broker keeps its log in memory and handles one request at a time, so
its cost dominates every measurement. Two runs of it compare (the constant
cancels); one run gives no absolute figure and cannot rank two settings — all
codecs measure the same against it. Throughput numbers need a real cluster with
published hardware and configuration. This documentation publishes none.

## Coordination Traffic Has Its Own Connection

A broker reads one request per connection at a time, so a heartbeat written
behind a long-polling fetch on the same socket waits for that fetch. The
connection pool keeps a separate **coordination connection** to a group
coordinator next to the data connection to the same broker. Group heartbeats,
`JoinGroup`, `SyncGroup`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch` and
share-group heartbeats use it. Requests on each connection are written in
submission order.

At the `max_connections` cap, coordination requests fall back to the data
connection; `krafka_coordination_fallbacks_total` (`coordination_fallbacks` in
the `connections` section of `metrics()`) counts them and the pool logs one
warning.

## Connection Model

krafka opens **one data connection per broker**, plus the coordination
connection to a coordinator. Request concurrency comes from pipelining rather
than from extra sockets: up to `max_in_flight_requests` requests are outstanding on a single
connection at any time, and responses are demultiplexed by correlation ID.

There is no connections-per-broker setting: per-partition ordering relies on a
partition's batches travelling over one socket in dispatch order.

If you need more parallelism to one broker, connect more `Kafka` handles:
each handle owns one pool, and every client built from a handle shares it.

#### Closing a shared pool

A client's `close()` shuts that client down and leaves the pool alone, so
siblings keep their connections and in-flight requests. The pool closes when
the last clone of the `Kafka` handle and the last client built from it are
dropped.

```rust,compile
let producer = kafka.producer().build().await?;
let consumer = kafka.consumer("g").build().await?;

producer.close().await?; // the consumer keeps working
consumer.close().await?;
```

Every client of a handle shares its network path: proxy, keepalive,
descriptor cap.

A connection attempt is bounded by `connect_timeout`, handshake included;
a broker that refuses is retried with per-address backoff from 50 ms to 1 s.

### Tuning the connection

`max_in_flight_requests` and the rest of the socket- and pool-level settings
are connection settings on the `Kafka` builder:

```rust,compile
use krafka::Kafka;
use std::time::Duration;

let kafka = Kafka::builder("localhost:9092")
    // Deeper pipelining on a high-latency link.
    .max_in_flight_requests(16)
    // …but remember the worst-case memory ceiling this implies:
    //   max_response_size × max_in_flight_requests (each frame is
    //   reserved once, at its declared size)
    // 16 × 100 MiB = 1.6 GiB. Lower one or the other on a memory-tight host.
    .max_response_size(32 * 1024 * 1024)
    // On a high bandwidth-delay-product link the socket buffer, not the
    // network, is the ceiling. `None` (the default) leaves the OS value.
    .socket_send_buffer(Some(4 * 1024 * 1024))
    .socket_receive_buffer(Some(4 * 1024 * 1024))
    // Match the broker's connections.max.idle.ms.
    .connections_max_idle(Some(Duration::from_secs(9 * 60)))
    // Bound file descriptors on a cluster whose broker count can jump.
    .max_connections(Some(64))
    .connect()
    .await?;
```

Two of these fail in non-obvious ways:

- **`max_response_size` too low permanently stalls a partition.** Kafka returns
  at least one complete record batch per partition *even when it exceeds
  `fetch.max.bytes`*. If a topic's `max.message.bytes` is above this ceiling,
  the client rejects the frame — and the same bytes come back on every retry.
  Keep it above the largest `max.message.bytes` you consume.
- **`tcp_keepalive` too high looks like a broker problem.** A stateful firewall
  or cloud load balancer that reaps idle flows produces a consumer that stops
  receiving after exactly N minutes, with nothing in the broker logs. Set the
  keepalive below the middlebox's idle timeout.

See [Configuration → Transport Configuration](@/docs/configuration.md) for the full
table.

## Buffers and Allocations

Measured by `tests/record_decode_allocations.rs`:

- **Consume decode**: 1000 uncompressed 100-byte records decode with one
  allocation (the record list, 0.89× the wire size). Keys, values and header
  values are slices of the fetched response; a record kept alive keeps that
  response alive.
- **Response frames**: a 16 MiB frame is read into one allocation of 1.01×
  the frame, reserved when its length prefix arrives.
- **Producer record pipeline**: `Record` key and value use `Bytes`, so
  batching clones the reference count instead of copying data. The encoded
  batch is copied once into the request frame.

## Batch Optimization

### Producer Batching

The producer sends one Produce request per broker per wave, carrying the next
batch of every ready partition that broker leads, so writing to many partitions
of one broker costs one round trip, not one per partition. Configure batching:

```rust,compile
let producer = kafka
    .producer()
    .batch_size(64 * 1024) // 64 KiB batches
    .linger(Duration::from_millis(5)) // the default: wait up to 5 ms to fill a batch
    .build()
    .await?;
```

A partition has one batch on the wire at a time and at most 5 requests are in
flight per broker; see [Producer](@/docs/producer.md#one-request-per-broker-one-batch-per-partition).

### Consumer Fetch Optimization

The consumer automatically batches fetch requests by leader broker, and issues
every broker's fetch concurrently against one shared deadline — so a poll on an
N-broker cluster costs one round trip, not N:

```rust,compile
let consumer = kafka
    .consumer("my-group")
    .fetch_min_bytes(1024) // Wait for at least 1 KiB
    .fetch_max_bytes(1024 * 1024) // Max 1 MiB per fetch
    .fetch_max_wait(Duration::from_millis(100)) // Max broker wait
    .build()
    .await?;
```

### Read-ahead

A fetch response can carry `fetch_max_bytes` (50 MiB by default) while
`max_poll_records` (500) caps what one `poll()` may return. The consumer
decodes **`max_poll_records` + the receive buffer's free capacity** and parks
the surplus:

```text
poll 1   fetch → decode 1000 → deliver 500, park 500
poll 2   buffer → deliver 500                        (no network)
poll 3   fetch → decode 1000 → deliver 500, park 500
```

Nothing is decoded twice and nothing is dropped.

Read-ahead depth is `max_buffered_records` (default 500). Raise it to pipeline
deeper on high-throughput consumers; lower it to bound resident memory:

```rust,compile
let consumer = kafka
    .consumer("my-group")
    .max_poll_records(500)
    .max_buffered_records(2000) // read up to 4 polls ahead
    .build()
    .await?;
```

Because the consumer reads ahead of delivery, its *fetch* position runs ahead of
its *delivered* position. Commits, `position()` and lag all follow the delivered
one, so a crash never acknowledges a record `poll()` did not return — see
[Position vs fetch position](@/docs/consumer.md#position-vs-fetch-position).

### Partition fairness

Both the broker's `fetch_max_bytes` accounting and the `max_poll_records` cap
consume partitions in request order, so a fixed order starves whatever sits at
the tail. krafka sorts the assigned partitions and rotates them by one position
per poll, so every partition takes its turn at the front.

### Batched Offset Resolution

When several partitions need offset resolution (after a rebalance, or on the
first poll), krafka groups them by leader broker and sends one `ListOffsets`
request per broker, so the cost is one round trip per broker, not per
partition.

A failed resolution backs off per partition, exponentially up to 30 s.

### Incremental Fetch Sessions (KIP-227)

When the broker supports Fetch v7 or later, krafka uses incremental fetch
sessions: after the first full request, a fetch names only the partitions that
changed or were removed. Sessions need no configuration; a session error
resets the session and the next fetch is a full one.

## Memory Backpressure

`buffer_memory` bounds the bytes the producer holds. When it is full,
`enqueue()` waits for queued records to resolve (Java's `max.block.ms`). `max_block` is one budget for the whole
call — resolving the topic, then waiting for memory:

```rust,compile
use krafka::producer::Producer;
use std::time::Duration;

let producer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .producer()
    .buffer_memory(32 * 1024 * 1024) // 32MB max buffer
    .max_block(Duration::from_secs(5))
    .build()
    .await?;
```

## Benchmarking Tips

1. **Use release builds**: `cargo build --release`
2. **Pre-warm connections**: Establish connections and fetch metadata before measuring
3. **Account for broker GC pauses**: the brokers run on the JVM
4. **Measure end-to-end latency**: Include network round trips
5. **Monitor broker metrics**: Check CPU, disk I/O, and network saturation
