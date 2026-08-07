+++
title = "Performance"
description = "What krafka optimises by default, which knobs actually move throughput, and how to measure honestly."
weight = 110

[extra]
slug_id = "performance"
+++

## Benchmarking

`just bench` runs the criterion suite in `benches/`: varint encoding, CRC32C,
murmur2, record-batch encode/decode, and the partitioners. These are
**micro-benchmarks**. They are the right tool for catching a regression in one
function and the wrong tool for answering "how fast is this client".

**There is deliberately no end-to-end throughput benchmark, and no published
comparison against other clients.** That is an open gap, not an oversight — see
the note below on what it would take to close it honestly.

### Why the in-process broker cannot serve as a benchmark peer

An end-to-end benchmark driving the real client against
`krafka::testing::FakeBroker` was built and then removed, because it measured
the wrong thing. The giveaway: with the fake broker as the peer, all five
compression codecs reported the same throughput to within noise. A benchmark
that cannot separate gzip from lz4 is not measuring compression — the fake
broker's per-request handling dominated, so the numbers described the test
double rather than the client.

The fake broker is an excellent correctness harness and a poor performance one.
It holds a single lock across request handling and keeps its log in memory; it
was never built to be fast.

### What a credible benchmark would require

- A **real broker**, because fsync, replication and the page cache are most of
  what a produce path waits on.
- **Published hardware and configuration** on both sides — broker *and* client.
  The Redpanda-vs-Kafka dispute turned entirely on
  `log.flush.interval.messages=1` forcing an fsync per batch.
- **Raw artifacts and a reproduction script**, not summary numbers.
- **Percentiles from a verified histogram.** The OpenMessaging Benchmark carried
  a histogram bug that invalidated published latency percentiles for years.

The ecosystem's history here is cautionary rather than encouraging: franz-go
withdrew its own "4× faster" claims from its README. Until krafka can meet the
bar above, this documentation describes the *design* choices that should make it
fast — zero-copy buffers, per-partition pipelining, batching, lock-free metrics
— and claims no measured outcome.

## Request Priority Channels

krafka implements priority-based request scheduling to prevent consumer group ejection during backpressure.

### How It Works

Each connection maintains two channels:
- **High-priority channel**: Heartbeats, metadata refreshes, coordinator discovery
- **Normal-priority channel**: Produce, fetch, and other data requests

The connection task always checks the high-priority channel first, ensuring time-sensitive requests are never starved by data traffic.

### Priority Assignment

Priority is automatically assigned based on API key:

| Priority | API Keys |
|----------|----------|
| High | `Heartbeat`, `ConsumerGroupHeartbeat`, `ShareGroupHeartbeat`, `JoinGroup`, `SyncGroup`, `LeaveGroup`, `OffsetCommit`, `Metadata`, `FindCoordinator`, `LeaderAndIsr`, `ApiVersions` |
| Normal | `Produce`, `Fetch`, and all others |

### Configuration

```rust,compile
use krafka::network::ConnectionConfig;

let config = ConnectionConfig::builder()
    .high_priority_channel_capacity(64)   // Default: 64
    .normal_priority_channel_capacity(256) // Default: 256
    .build();
```

### Explicit Priority Override

For special cases, you can explicitly set request priority:

```rust
use krafka::network::{RequestPriority, BrokerConnection};

// Force high priority for a specific request
conn.send_request_with_priority(
    ApiKey::OffsetCommit,
    8,
    RequestPriority::High, // Override automatic assignment
    |buf| request.encode_v8(buf),
).await?;
```

### Monitoring Priority Usage

Connection statistics track priority channel usage:

```rust
let stats = conn.stats();

println!("High-priority requests: {}", stats.high_priority_count());
println!("Normal-priority requests: {}", stats.normal_priority_count());
println!("Priority bypasses: {}", stats.bypass_count()); // Direct non-blocking sends
```

## Connection Model

krafka opens **one TCP connection per broker**, matching the Apache Kafka Java
client. Request concurrency comes from pipelining rather than from extra
sockets: up to `max_in_flight_requests` requests are outstanding on a single
connection at any time, and responses are demultiplexed by correlation ID.

A previous `connections_per_broker` setting and its `BrokerConnectionBundle`
type have been **removed**. They were never wired into the connection pool, so
setting them had no effect. They are not coming back in that form, because
spreading one partition's produce traffic across several sockets would break
the idempotent producer's ordering guarantee — `max.in.flight.requests` bounds
reordering *per connection*, so a partition's batches must stay on one socket.

If you need more parallelism to one broker today, run more clients: each
`Producer`/`Consumer` built without `.with_client(...)` owns its own pool. Use
`KrafkaClient` when you want the opposite — several clients sharing one pool
and one connection per broker. `with_client` is available on every client
builder: `Producer`, `TransactionalProducer`, `Consumer`, `ShareConsumer` and
`AdminClient`.

#### Who closes a shared pool

A client built with `.with_client(...)` **borrows** its pool. Its `close()`
shuts the client down and leaves the connections alone; `owns_pool()` reports
`false`. Close the `KrafkaClient` to release the sockets.

This matters because the alternative is silent: a client that tore down a
borrowed pool would kill every sibling's connections and fail their in-flight
Produce and Fetch requests, undoing exactly what sharing the client was for.
`AdminClient` handled this correctly from the start; its four siblings called
`close_all()` unconditionally until this release.

```rust
let client = KrafkaClient::builder("localhost:9092").build().await?;

let producer = Producer::builder().with_client(&client).build().await?;
let consumer = Consumer::builder().group_id("g").with_client(&client).build().await?;

assert!(!producer.owns_pool());

producer.close().await;   // consumer keeps working
consumer.close().await;
client.close().await;     // now the sockets go
```

If you pass a `TransportConfig` to a `KrafkaClient`, every client sharing it
inherits that network path — no per-client `.transport(...)` needed, and no way
for one of them to quietly bypass the proxy.

> **Note:** The connection pool uses a read-lock fast path for hot-path lookups.
> During reconnection, all locks are dropped before performing network I/O,
> preventing deadlocks and enabling concurrent access to other brokers while one
> broker is being reconnected.

### Tuning the connection

`max_in_flight_requests` and the rest of the socket- and pool-level settings
live on `TransportConfig`, accepted by every builder:

```rust,compile
use krafka::network::TransportConfig;
use std::time::Duration;

let transport = TransportConfig::builder()
    // Deeper pipelining on a high-latency link.
    .max_in_flight_requests(16)
    // …but remember the worst-case memory ceiling this implies:
    //   max_response_size × max_in_flight_requests
    // 16 × 100 MiB = 1.6 GiB. Lower one or the other on a memory-tight host.
    .max_response_size(32 * 1024 * 1024)
    // Match the broker's connections.max.idle.ms.
    .connections_max_idle(Some(Duration::from_secs(9 * 60)))
    // Bound file descriptors on a cluster whose broker count can jump.
    .max_connections(Some(64))
    .build()?;
```

Two of these have failure modes worth knowing:

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

## Zero-Copy Message Handling

krafka uses `bytes::Bytes` throughout for zero-copy buffer management:

- **Producer record pipeline**: `ProducerRecord` key and value use `Bytes`, so batching clones the reference count (O(1)) instead of copying data
- Record batches share underlying memory
- Slicing operations don't copy data
- Custom compression codecs can provide their own buffers

## Batch Optimization

### Producer Batching

Configure the producer accumulator for optimal batching:

```rust
let producer = ProducerBuilder::new()
    .batch_size(64 * 1024)     // 64KB batches
    .linger(Duration::from_millis(5))  // Wait up to 5ms to fill batches
    .build();
```

### Consumer Fetch Optimization

The consumer automatically batches fetch requests by leader broker, and issues
every broker's fetch concurrently against one shared deadline — so a poll on an
N-broker cluster costs one round trip, not N:

```rust
let consumer = ConsumerBuilder::new()
    .fetch_min_bytes(1024)      // Wait for at least 1KB
    .fetch_max_bytes(1024 * 1024)  // Max 1MB per fetch
    .fetch_max_wait(Duration::from_millis(100))  // Max wait time
    .build();
```

### Read-ahead

A fetch response can carry `fetch_max_bytes` (50 MB by default) while
`max_poll_records` (500) caps what one `poll()` may return. Rather than decode
the whole response and discard the excess, or decode exactly the cap and pay a
round trip every poll, krafka decodes **`max_poll_records` + the receive
buffer's free capacity** and parks the surplus:

```text
poll 1   fetch → decode 1000 → deliver 500, park 500
poll 2   buffer → deliver 500                        (no network)
poll 3   fetch → decode 1000 → deliver 500, park 500
```

In steady state that halves the number of Fetch round trips and removes network
latency from every other poll. Nothing is decoded twice and nothing is dropped.

Read-ahead depth is `max_buffered_records` (default 500). Raise it to pipeline
deeper on high-throughput consumers; lower it to bound resident memory:

```rust
let consumer = ConsumerBuilder::new()
    .max_poll_records(500)
    .max_buffered_records(2000)   // read up to 4 polls ahead
    .build();
```

Because the consumer reads ahead of delivery, its *fetch* position runs ahead of
its *delivered* position. Commits, `position()` and lag all follow the delivered
one, so a crash never acknowledges a record `poll()` did not return — see
[Position vs fetch position](@/docs/consumer.md#position-vs-fetch-position).

### Partition fairness

Both the broker's `fetch_max_bytes` accounting and the `max_poll_records` cap
consume partitions in request order, so a fixed order starves whatever sits at
the tail. krafka sorts the assigned partitions and rotates them by one position
per poll, giving every partition its turn at the front — the same guarantee the
Java client gets from `PartitionStates.moveToEnd`.

### Batched Offset Resolution

When multiple partitions need offset resolution (e.g., after rebalance or on first poll), krafka groups partitions by leader broker and sends one batched `ListOffsets` RPC per broker. This reduces 50 partitions from 50 round-trips down to 2-3, significantly improving consumer startup and rebalance time.

Failed offset resolutions use per-partition exponential backoff (100ms base, 30s cap) to prevent retry storms under sustained broker unavailability.

### Incremental Fetch Sessions (KIP-227)

When the broker supports Fetch API v7+, krafka uses incremental fetch sessions to reduce request payload sizes. Instead of sending the full partition list on every `poll()`, only changed partitions and removed partitions are sent. For consumers with many partitions, this can reduce fetch request sizes by 10-100x.

Fetch sessions are enabled automatically — no configuration needed. Error recovery (session reset + full re-fetch) is handled transparently.

## Memory Backpressure

Configure memory limits to prevent OOM during high throughput. When batching is enabled (`linger > 0`) and the buffer is full, `send()` blocks the caller for up to `max_block` waiting for in-flight batches to drain, matching the Kafka Java client's `max.block.ms` semantics:

```rust
use krafka::producer::AccumulatorConfig;

let config = AccumulatorConfig {
    buffer_memory: 32 * 1024 * 1024,  // 32MB max buffer
    max_block_ms: 5000,                // Block up to 5s when full
    ..Default::default()
};
```

## Benchmarking Tips

1. **Use release builds**: `cargo build --release`
2. **Pre-warm connections**: Establish connections before measuring
3. **Account for GC pauses**: Kafka brokers have their own GC
4. **Measure end-to-end latency**: Include network round trips
5. **Monitor broker metrics**: Check CPU, disk I/O, and network saturation
