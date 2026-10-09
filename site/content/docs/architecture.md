+++
title = "Architecture"
description = "Module boundaries, the connection pool, the record accumulator and the concurrency model."
weight = 140

[extra]
slug_id = "architecture"
+++

## Design Principles

### 1. No C library
- No C library to install and no system dependency: TLS is rustls, every
  codec decodes in Rust
- The default build compiles C only inside `ring`, rustls' crypto backend;
  the `zstd` (encoding), `rustls-aws-lc-rs` and `aws-msk` features compile more
- `just no-c` checks the dependency graph

### 2. Tokio
- Built on Tokio; krafka is not runtime-agnostic. The connection loop, the
  send engine, the timers and the idle evictor use Tokio primitives directly,
  and cancellation safety is reasoned about in Tokio's semantics
- Requests are pipelined on one connection per broker

### 3. No unsafe code
- `#![deny(unsafe_code)]` on the crate; memory safety comes from the type system

### 4. Shared Buffers
- Uses the `bytes` crate for buffer management
- Decoded records are slices of the fetched response
- Each response frame is read into one allocation

### 5. Security Hardened
- Secrets zeroized on drop (SCRAM passwords, AWS credentials)
- Constant-time comparison via `subtle` crate (timing-attack resistant)
- PBKDF2 iteration count validated to prevent DoS
- Protocol allocations capped to prevent OOM from malicious brokers
- Decompression bomb protection (128 MiB default, configurable)
- Debug output redacts all credentials

## Module Architecture

Public modules are the API; `client`, `protocol`, `network`, `metadata` and
`telemetry` are private. Their types reach users only through re-exports at the
crate root (`Kafka`, `KafkaBuilder`, `Compression`, `TopicInfo`, …).

```
krafka/src/
├── client.rs          # Kafka handle and KafkaBuilder: the pool, the metadata cache, the role builders
├── protocol/          # (private) Kafka wire protocol
│   ├── primitives.rs  # Strings, arrays, varints, tagged fields
│   ├── record.rs      # Record batches and compression
│   ├── messages/      # One file per API's request/response types
│   ├── api.rs         # API keys, ApiVersions
│   ├── header.rs      # Request/response headers
│   └── codec.rs       # Framing
├── network/           # (private) Connections
│   ├── connection.rs  # One broker connection: request channel, I/O loop, correlation
│   ├── connector.rs   # TCP, SOCKS5, TLS and SASL setup
│   ├── happy_eyeballs.rs # RFC 8305 address racing
│   ├── secure.rs      # SASL handshake
│   ├── transport.rs   # Socket options
│   └── pool.rs        # ConnectionPool, keyed by (address, ConnectionPurpose)
├── metadata.rs        # (private) ClusterMetadata: the cache and its single writer task
├── producer/          # Producer
│   ├── mod.rs         # Producer API, ProducerBuilder
│   ├── config.rs      # Producer configuration
│   ├── partitioner.rs # Built-in (KIP-794, KIP-1123) and custom partitioning
│   ├── accumulator.rs # Byte budget, admission, DeliveryHandle
│   ├── engine.rs      # The send engine: per-broker drain, deadlines, retries
│   ├── batch.rs       # Batches and their encoding
│   ├── identity.rs    # Producer id, epoch, sequence stamps, error classification
│   ├── gate.rs        # Transaction gate: state, pending sends, first failure
│   ├── transaction.rs # TransactionalProducer
│   ├── typed.rs       # TypedProducer
│   ├── record.rs      # Record
│   └── retry.rs       # The retry backoff (capped at 1 s)
├── consumer/          # Consumer
│   ├── mod.rs         # Consumer API
│   ├── builder.rs     # ConsumerBuilder
│   ├── fetcher/       # Fetch planning, per-broker Fetch requests, decoding
│   ├── fetch_session.rs # KIP-227 fetch sessions
│   ├── group/         # Classic and KIP-848 group membership, heartbeats
│   ├── assignor/      # Range, RoundRobin, CooperativeSticky
│   ├── rebalance/     # Applying assignments, rebalance listeners
│   ├── offsets/       # Commit, reset, validation (KIP-320)
│   └── compacted.rs   # Compacted-topic table
├── share_consumer/    # KIP-932 share consumer
├── admin/             # AdminClient: one file per area, driver.rs routes and retries
├── auth/              # AuthConfig, TLS, SCRAM, OAUTHBEARER/OIDC, AWS MSK IAM
├── interceptor.rs     # Producer and consumer interceptors
├── serdes.rs          # Serializers and consumer byte transforms
├── dlq.rs             # Dead-letter record helper
├── error.rs           # KrafkaError
├── metrics.rs         # The Metrics snapshot and Prometheus text
├── telemetry/         # (private) KIP-714 reporter and OTLP encoding
├── testing/           # In-process fake broker (feature `test-broker`)
└── util.rs            # CRC, varints
```

## Protocol Layer

### Wire Protocol

krafka implements the Kafka binary protocol:

```
+----------------+----------------+----------------+
| Size (4 bytes) | API Key (2)    | API Version (2)|
+----------------+----------------+----------------+
| Correlation ID | Client ID      | Request Body   |
+----------------+----------------+----------------+
```

### Record Batch Format (v2)

```
+----------------+----------------+----------------+
| Base Offset    | Batch Length   | Partition Leader Epoch |
+----------------+----------------+----------------+
| Magic         | CRC            | Attributes     |
+----------------+----------------+----------------+
| Last Offset   | Base Timestamp | Max Timestamp  |
+----------------+----------------+----------------+
| Producer ID   | Producer Epoch | Base Sequence  |
+----------------+----------------+----------------+
| Records Count | Records...                      |
+----------------+---------------------------------+
```

### Compression

All four Kafka compression codecs decode in pure Rust in every build. Gzip,
Snappy and LZ4 also encode in every build; zstd encoding needs the `zstd`
feature.

| Codec | Implementation | Characteristics |
|-------|---------------|-----------------|
| Gzip | `flate2` | Best ratio, slowest |
| Snappy | `snap`, snappy-java stream format (as the Java client writes it); raw snappy also decodes | Good balance |
| LZ4 | `lz4_flex` | Fastest |
| Zstd | `ruzstd` (decode), `zstd` (encode, feature) | High ratio, fast decode |

## Network Layer

### Shared Transport: the `Kafka` handle

`Kafka::builder(..).connect()` creates one connection pool and one metadata
cache. Every client built from the handle shares both, so an application with
one producer and two consumers against a 5-broker cluster opens 5 data
connections, not 15:

```rust,compile
// One pool + one metadata cache for the whole process.
let kafka = krafka::Kafka::builder("broker1:9092,broker2:9092")
    .connect()
    .await?;

let producer = kafka.producer().build().await?;
let consumer = kafka.consumer("g1").build().await?;
let admin = kafka.admin();
// Data connections: one per broker, shared by all three clients.
```

The idle-connection evictor and (when configured) the OAUTHBEARER
proactive-refresh task are started once by `connect()` and shared by every
client of the handle. A second identity or separate pool is a second handle.

### Connection Architecture

The pool keys connections by address and `ConnectionPurpose`: one `Data`
connection per broker, shared by every client of the handle, plus one
`Coordination` connection per group coordinator. Concurrency comes from
request pipelining, not from extra sockets: responses are demultiplexed by
correlation ID, and up to `max_in_flight_requests` requests may be outstanding
at once.

```
  ┌───────────────────────────────────────────────────────────────┐
  │                 ConnectionPool  (address, purpose)            │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
  │  │ broker 1, Data   │  │ broker 2, Data   │  │ broker 2,    │ │
  │  │  in-flight ≤ N   │  │  in-flight ≤ N   │  │ Coordination │ │
  │  │  FIFO, one loop  │  │  FIFO, one loop  │  │  (heartbeats)│ │
  │  └──────────────────┘  └──────────────────┘  └──────────────┘ │
  └───────────────────────────────────────────────────────────────┘
```

Each connection has one FIFO request channel, drained by one I/O loop; there
are no request priorities. Data connections mirror the Apache Kafka Java client: one per broker, so a
partition's in-flight batches travel one socket in dispatch order. The broker
reads one request per connection at a time, so coordination requests
(`JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, the KIP-848 and share
heartbeats, `OffsetCommit`, `OffsetFetch`) get their own connection to the
coordinator, as the Java client does with its separate coordinator node. The
consumer and the share consumer take it from the pool for every request, so a
dead or session-expired one is replaced like any other. A transport error on it
makes the member find the coordinator again; it keeps its assignment and
rejoins only if the coordinator has moved to another broker. `FindCoordinator` and the transactional producer's
coordinator requests use data connections. At the `max_connections` cap a
coordination request falls back to the broker's data connection, counted in
`Metrics::connections.coordination_fallbacks`.

**Request timeout.** A request's timeout starts when it is written. The first
request to time out fails with `Timeout`, the connection is closed, and every
other request pending on it fails with a retriable `Network` error: responses
arrive in order, so nothing behind it can be answered. The next request gets a
new connection.

**Cancellation.** A request whose caller drops the future before it is written
is never written. One already written is processed by the broker; its response
is discarded and the connection stays usable.

### KIP-219: Client-Side Throttle Compliance

When a response carries `throttle_time_ms > 0`, the connection is muted for
that time (capped at five minutes): nothing is written on it until the mute
ends. Waiting requests keep their full request timeout, which starts at the
write, so a written request is never reported as timed out because of a
throttle. A caller whose own deadline passes during the mute gets an error and
its request is not sent. A mute on one connection does not delay another, so a
throttled data connection does not hold back the coordination connection.

### Connecting and Reconnecting

The pool makes at most one connection attempt per lookup, bounded by
`connect_timeout` (TCP, TLS, SASL and the `ApiVersions` handshake together).
Concurrent callers for the same connection share the attempt, and a caller's
own deadline applies by dropping its future. After a failed attempt the
address is in **reconnect backoff** — 50 ms, doubling to 1 s, with 20 % jitter,
as the Java client's `reconnect.backoff.ms` / `reconnect.backoff.max.ms`: a call
inside the window fails immediately with a retriable error (or the original
error, if that was not retriable). A successful attempt resets the backoff.
Retrying is the caller's job, inside the caller's deadline: the producer's
`delivery_timeout`, the consumer's poll loop, the admin call's `timeout`.

A connection past its SASL re-authentication point (KIP-368) is replaced on
the next lookup; the old one closes once its pending requests complete.
Replacing a dead or expired connection never counts against
`max_connections`.

### Request/Response Flow

1. The caller builds a request and the connection negotiates its version within
   the client's `[MIN, MAX]` and the broker's advertised range
2. The request is encoded for that version, given a correlation ID and queued
   on the connection's channel, waiting if `max_in_flight_requests` are
   outstanding
3. The I/O loop writes it; its request timeout starts then
4. The loop reads each response frame into one buffer, matches it to its
   request by correlation ID and hands it back
5. The caller decodes it for the same version; an unsupported version is a
   `KrafkaError::Protocol` error

## Metadata Management

### Metadata Caching

```
  ┌─────────────────────────────────────────────────────┐
  │          ClusterMetadata (one writer task)           │
  │  ┌──────────────────────────────────────────────┐   │
  │  │               Broker Cache                    │   │
  │  │  { broker_id -> (host, port, rack) }         │   │
  │  └──────────────────────────────────────────────┘   │
  │  ┌──────────────────────────────────────────────┐   │
  │  │               Topic Cache                     │   │
  │  │  { topic -> [partition metadata] }           │   │
  │  └──────────────────────────────────────────────┘   │
  │  ┌──────────────────────────────────────────────┐   │
  │  │              Leader Cache                     │   │
  │  │  { (topic, partition) -> broker_id }         │   │
  │  └──────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────┘
```

### Metadata Refresh

- One writer task per metadata cache fetches and applies metadata. Callers ask
  it for topics — forcing a fetch when a broker said the cache is wrong — and
  wait for the fetch that covers them; requests arriving together become one
  `Metadata` request, spaced by an exponential, jittered backoff
- Leader hints from Fetch/Produce responses (KIP-951) and rebootstraps go
  through the same serialized write path, so no update overwrites one it did
  not see; readers load the current snapshot without locking
- Within one topic ID a cached leader epoch is never replaced by an older one
  (KIP-320); a topic re-created under the same name (new topic ID) takes the
  new partitions as they are
- Every response replaces the broker map; a topic reported unknown leaves the
  cache
- A partition without a leader, or whose leader is not in the broker map, is a
  retriable `LEADER_NOT_AVAILABLE`: a send waits the election out inside
  `delivery_timeout`
- API version negotiation: the highest mutually supported Metadata version (v1-v13)

### Metadata Recovery (Rebootstrap)

Clients default to `MetadataRecoveryStrategy::Rebootstrap`. The client drops
its view of the cluster and rediscovers it from the bootstrap servers when no
known broker is reachable, when no metadata fetch has succeeded for the
rebootstrap trigger (default 5 min), or when a broker returns
`REBOOTSTRAP_REQUIRED` (error code **129**) in a Metadata v13+ response.
In-flight requests are not aborted; a fetch that was in flight is discarded.
Seed addresses are resolved at dial time, so brokers that moved to new IPs
behind the same names are found. `update_seed_brokers()` replaces the seed list
at runtime.

A failed bootstrap names every address it tried and keeps the last failure as
the error's source.

Rebootstrap follows KIP-899 and KIP-1102.

## Producer Architecture

### Send Path

```
  send / enqueue                 engine task (one per producer)          brokers
       │                                   │                                │
  interceptors, validation                 │                                │
  partition (murmur2 / sticky by bytes)    │                                │
  reserve buffer_memory ──── record ─────► │ per-partition queues           │
       │                                   │ seal: batch_size, linger,      │
       │                                   │       flush, commit            │
       │                                   │ per broker: one Produce with   │
       │                                   │ the head batch of every ready  │
       │                                   │ partition (≤ 5 in flight,      │
       │                                   │ one batch per partition) ────► │
       │                                   │ ◄──────── per-partition answers│
  DeliveryHandle ◄──── outcome ─────────── │ ack / retry (same stamp) /     │
                                           │ fail + epoch bump / split      │
```

The engine owns every piece of send state — queues, producer identity,
in-flight requests — and polls its requests inline, so a producer runs one task
however many partitions it writes to. Each batch resolves by
`created + delivery_timeout`, checked before every attempt and by the engine's
timer; a flush only seals, it never blocks the engine.

### Partitioning

A keyed record goes where the Java client sends it — murmur2 of the key:

```rust,compile
use krafka::producer::murmur2;

fn partition_for(key: &[u8], partition_count: u32) -> i32 {
    ((murmur2(key) & 0x7fff_ffff) % partition_count) as i32
}
```

A keyless record sticks to one partition until `batch_size` bytes have gone
to it, then a partition is chosen at random (KIP-794, partly: the choice is
not weighted by broker queue sizes). With `partitioner_rack_aware` only
partitions led in `client_rack` are chosen (KIP-1123).

## Consumer Architecture

### Poll Path

```
  User Code                     Consumer                     Broker
      │                            │                            │
      │  poll(timeout)             │                            │
      │ ─────────────────────────> │                            │
      │                            │                            │
      │                 ┌──────────┴──────────┐                 │
      │                 │ One Fetch per leader│                 │
      │                 │ broker, concurrently│                 │
      │                 └──────────┬──────────┘                 │
      │                            │    FetchRequest            │
      │                            │ ─────────────────────────> │
      │                            │                            │
      │                            │    FetchResponse           │
      │                            │ <───────────────────────── │
      │                 ┌──────────┴──────────┐                 │
      │                 │ Decompress &        │                 │
      │                 │ Decode Records      │                 │
      │                 └──────────┬──────────┘                 │
      │  Vec<ConsumerRecord>       │                            │
      │ <───────────────────────── │                            │
```

### Fetch Sessions (KIP-227)

With Fetch v7+, the consumer uses incremental fetch sessions. A per-broker `FetchSessionState` tracks the partitions registered with the broker's session.
On each `poll()`, the consumer computes a diff against the previous state:

- **New/changed partitions** go in the `topics` field (only offset and `max_bytes` changes)
- **Removed partitions** go in the `forgotten_topics` field
- The `session_id` and incrementing `session_epoch` (a per-session epoch, not the partition
  leader epoch) maintain session continuity

If the broker returns `FetchSessionIdNotFound` or `InvalidFetchSessionEpoch`, the session is reset
and the next poll sends a full fetch. On rebalance, `unsubscribe()` and `close()`, every session
is closed on its broker with a final-epoch fetch carrying the consumer's fetch settings.

### Consumer Group Protocol

```
  ┌────────────────────────────────────────────────────────────┐
  │                    Consumer Group Lifecycle                 │
  │                                                            │
  │  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
  │  │ Unjoined │───>│ Joining  │───>│ Awaiting │              │
  │  └──────────┘    └──────────┘    │   Sync   │              │
  │       ▲                          └────┬─────┘              │
  │       │                               │                    │
  │       │                               ▼                    │
  │       │          ┌──────────┐    ┌──────────┐              │
  │       └──────────│ Preparing│<───│  Stable  │<─ Heartbeat  │
  │                  │ Rebalance│    └──────────┘              │
  │                  └──────────┘                              │
  └────────────────────────────────────────────────────────────┘
```

## Performance Optimizations

### Hot and cold paths

`#[inline]` sits on the protocol primitives (varints, fixed-width integers),
request and response headers, record encode and decode, murmur2, CRC32C and
the partitioners. Every `KrafkaError` constructor is `#[cold]`, so error paths
stay out of the hot code's layout.

### Buffers

- **Shared buffers**: `Bytes` for shared ownership. Decoded record keys,
  values and header values are slices of the response buffer (uncompressed) or
  of the decompressed buffer.
- **One allocation per frame**: the reader reserves each response frame once,
  at the size its length prefix declares.
- **Owned on the way in**: `Record::new` and its builders take
  `impl Into<Bytes>`, so a `Vec<u8>`, `String` or `Bytes` value moves into the
  record without a copy; it is copied once, into the batch.

### Header-first decoding

A record batch's 61-byte header is parsed before anything else. The consumer
skips aborted transactional batches from the header alone, without
decompressing them; other batches are CRC-checked, decompressed and decoded
into records that slice the buffer.

### Allocation caps

A length decoded from the wire is checked against `MAX_DECODE_ARRAY_LEN`
(100,000) before it is used, and a `Vec` is pre-sized to at most the bytes
left in the buffer — every element takes at least one byte — so a hostile
length cannot reserve memory the response does not contain.

## Error Handling

### Error Hierarchy

```rust
pub enum KrafkaError {
    Network(Arc<io::Error>),                // Connecting, I/O, unreachable broker
    Protocol { kind, message },             // Wire protocol errors; kind drives retry policy
    Broker { code: ErrorCode, message },    // Kafka error codes
    Auth { message, source },               // SASL, TLS certificate, OIDC endpoint
    Timeout { operation },                  // Operation timeouts
    DeliveryTimeout { possibly_written, message },
    Config { message },                     // Configuration errors
    Closed { message },                     // The client was closed
    Fenced { message },                     // Another producer took over
    TransactionAbortable { message },       // Abort the open transaction
    NoOffset { partitions },                // No offset and no reset policy
    UnknownTopic { topic },
    OutOfOrderSequence { topic, partition, message },
    IllegalState { message },               // Call not valid in this state
    // … and Compression, Serialization, RecordDeserialization, Wakeup
}
```

`is_retriable()`, `requires_abort()` and `is_fatal()` classify any of them; see
[Error Handling](@/docs/errors.md).

### Retriable Errors

`is_retriable()` is true for transport failures and for broker codes Kafka
marks retriable. The clients retry them inside their deadline, refreshing
the leader or coordinator first when the code says the cached one is wrong
(`NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE`, `NOT_COORDINATOR`).

## Thread Safety

The client types are `Send + Sync`:

- `Producer`: `Send + Sync` - can be shared across tasks
- `Consumer`: `Send + Sync` - can be shared across tasks
- `ShareConsumer`: `Send + Sync` - can be shared across tasks (needs a Kafka 4.2+ broker)
- `AdminClient`: `Send + Sync` - can be shared across tasks

Shared state sits behind locks, atomics for flags such as the closed state, and `Arc` for what background tasks
share with the client (the group coordinator with its heartbeat task, the
producer with its engine). The connection pool's lookups take a read lock;
dials run in their own task with no lock held.

## Benchmarks

Criterion benchmarks in `benches/`:

- **`producer.rs`**: record batch encoding (1–1000 records), every codec,
  murmur2, varints, encode/decode round trip, keyed and round-robin
  partitioning
- **`consumer.rs`**: record batch decoding, decompression per codec, record
  iteration, and full decode against a header-only peek
- **`protocol.rs`**: primitives, varints, CRC32C, request headers, error-code
  and API-key conversions
- **`send_path.rs`**, **`consume_path.rs`** (feature `test-broker`): the whole
  send and consume paths against the in-process fake broker. They are a
  regression gate (`just bench-check`), not a source of absolute numbers.

```bash
cargo bench
cargo bench --bench send_path --features test-broker
```
