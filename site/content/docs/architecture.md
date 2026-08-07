+++
title = "Architecture"
description = "Module boundaries, the connection pool, the record accumulator and the concurrency model."
weight = 140

[extra]
slug_id = "architecture"
+++

## Design Principles

### 1. Pure Rust
- No C bindings or FFI
- Full control over all code paths
- No FFI overhead or complexity

### 2. Async-Native
- Built on Tokio from the ground up
- Non-blocking I/O everywhere
- Efficient connection multiplexing

**Tokio is a hard dependency, deliberately.** krafka is not runtime-agnostic and
does not intend to be. Supporting `smol`, `async-std` or `glommio` would mean
abstracting timers, TCP and task spawning behind traits that every hot path
would then go through — the connection event loop, the accumulator's
per-partition pipeline, the `DelayQueue` timer wheel and the idle evictor all
use Tokio primitives directly, and several correctness properties (cancellation
safety at specific await points, `DelayQueue` ordering) are reasoned about in
Tokio's semantics.

The cost of that abstraction is paid by every user; the benefit accrues to the
minority not already on Tokio, which is the ecosystem default and what the vast
majority of Kafka-adjacent Rust services run. If you need a different runtime,
this is the wrong client.

### 3. Zero Unsafe
- Memory safety guaranteed by Rust's type system
- No undefined behavior risks
- Security by design

### 4. Zero-Copy Where Possible
- Uses `bytes` crate for buffer management
- Avoids unnecessary copies in hot paths
- Efficient protocol parsing

### 5. Security Hardened
- Secrets zeroized on drop (SCRAM passwords, AWS credentials)
- Constant-time comparison via `subtle` crate (timing-attack resistant)
- PBKDF2 iteration count validated to prevent DoS
- Protocol allocations capped to prevent OOM from malicious brokers
- Decompression bomb protection (128 MiB default, configurable)
- Debug output redacts all credentials

## Module Architecture

```
krafka/
├── protocol/          # Kafka wire protocol
│   ├── primitives.rs  # Basic types (strings, arrays, varints)
│   ├── record.rs      # Record batches and compression
│   ├── messages.rs    # API request/response types (incl. ACL messages)
│   ├── api.rs         # API keys and versions
│   ├── header.rs      # Request/response headers
│   └── codec.rs       # Framing encoder/decoder
├── network/           # Networking layer
│   ├── connection.rs  # Async TCP connections
│   ├── secure.rs      # TLS/SASL authentication
│   └── pool.rs        # Connection pooling
├── metadata.rs        # Cluster metadata management
├── producer/          # Producer implementation
│   ├── mod.rs         # Producer API
│   ├── config.rs      # Producer configuration
│   ├── partitioner.rs # Partitioning strategies
│   ├── batch.rs       # Record batching
│   ├── accumulator.rs # Record accumulator with linger timer
│   ├── record.rs      # Producer records
│   ├── retry.rs       # Retry policy with exponential backoff
│   └── idempotent.rs  # Idempotent producer (PID, sequence tracking)
├── consumer/          # Consumer implementation
│   ├── mod.rs         # Consumer API
│   ├── config.rs      # Consumer configuration
│   ├── group.rs       # Consumer group coordination (rebalance listeners, heartbeat)
│   ├── offset.rs      # Offset management
│   └── record.rs      # Consumer records
├── admin.rs           # Admin client (topics, partitions, configs, ACLs, delegation tokens, quotas)
├── auth/              # Authentication
│   ├── mod.rs         # Auth module (SASL mechanisms)
│   ├── scram.rs       # SCRAM-SHA-256/512 implementation
│   ├── msk_iam.rs     # AWS MSK IAM authentication (Signature v4)
│   └── tls.rs         # TLS/SSL connections with rustls
├── error.rs           # Error types
├── metrics.rs         # Metrics (counters, gauges, latency tracking)
├── tracing_ext.rs     # Tracing (OpenTelemetry-compatible spans)
└── util.rs            # Utilities (CRC, varints)
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

All four Kafka compression codecs are supported when their features are enabled.
The default `compression` feature keeps the dependency stack pure-Rust by
enabling gzip, snappy, and LZ4; zstd is available through the explicit `zstd`
or `compression-all` feature.

| Codec | Implementation | Characteristics |
|-------|---------------|-----------------|
| Gzip | `flate2` | Best ratio, slowest |
| Snappy | `snap` | Good balance |
| LZ4 | `lz4_flex` | Fastest |
| Zstd | `zstd` | Best modern choice |

## Network Layer

### Shared Transport: `KrafkaClient`

By default every `Producer`, `Consumer`, and `AdminClient` creates its own `ConnectionPool`
and `ClusterMetadata`. An application with one producer and two consumers against a 5-broker
cluster therefore opens **15** TCP connections (3 clients × 5 brokers).

`KrafkaClient` solves this by wrapping a single `Arc<ConnectionPool>` + `Arc<ClusterMetadata>`
that is shared across all clients:

```rust,compile
// One pool + one metadata cache for the whole process.
let client = KrafkaClient::builder("broker1:9092,broker2:9092")
    .build()
    .await?;

let producer = Producer::builder().with_client(&client).build().await?;
let consumer = Consumer::builder().with_client(&client).group_id("g1").build().await?;
let admin    = AdminClient::builder().with_client(&client).build().await?;
// Connection count: 2 brokers × 1 pool = 2 connections, not 6.
```

The idle-connection evictor and (when configured) the OAUTHBEARER proactive-refresh task are
started once inside `KrafkaClient::build()` and shared by all attached clients.

### Connection Architecture

One TCP connection per broker, shared by every client attached to the pool.
Concurrency comes from request pipelining on that connection, not from extra
sockets: responses are demultiplexed by correlation ID, and up to
`max_in_flight_requests` requests may be outstanding at once.

```
  ┌───────────────────────────────────────────────────────────────┐
  │                       ConnectionPool                          │
  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
  │  │ BrokerConn(1)    │  │ BrokerConn(2)    │  │ BrokerConn   │ │
  │  │  in-flight ≤ N   │  │  in-flight ≤ N   │  │   (N...)     │ │
  │  │  correlation-id  │  │  correlation-id  │  │              │ │
  │  │  demultiplexing  │  │  demultiplexing  │  │              │ │
  │  └──────────────────┘  └──────────────────┘  └──────────────┘ │
  └───────────────────────────────────────────────────────────────┘
```

This mirrors the Apache Kafka Java client. Multiple connections per broker are
deliberately not offered: the idempotent producer's ordering guarantee bounds
reordering *per connection*, so a partition's in-flight batches must all travel
the same socket.

### Priority Channels

Each connection maintains two request channels to prevent consumer group ejection during backpressure:

```
  ┌─────────────────────────────────────────────────────┐
  │                   BrokerConnection                   │
  │  ┌───────────────────┐  ┌─────────────────────────┐ │
  │  │ High-Priority Ch  │  │   Normal-Priority Ch    │ │
  │  │ (Heartbeat, Meta, │  │ (Produce, Fetch, etc.)  │ │
  │  │  GroupHeartbeat,  │  │                         │ │
  │  │  JoinGroup, etc.) │  │                         │ │
  │  └─────────┬─────────┘  └───────────┬─────────────┘ │
  │            │   biased select!       │               │
  │            └─────────►◄─────────────┘               │
  │                       │                             │
  │                       ▼                             │
  │                   TCP Stream                        │
  └─────────────────────────────────────────────────────┘
```

High-priority requests (Heartbeat, ConsumerGroupHeartbeat, ShareGroupHeartbeat,
JoinGroup, SyncGroup, LeaveGroup, OffsetCommit, Metadata, FindCoordinator,
LeaderAndIsr, ApiVersions) are always processed
first, ensuring consumer group membership is maintained even under heavy produce/fetch load.

### KIP-219: Client-Side Throttle Compliance

When a broker returns `throttle_time_ms > 0` in a response, the client voluntarily delays
subsequent normal-priority requests by the indicated duration. High-priority requests (heartbeats,
metadata) are never delayed, preserving group membership. Throttle state is tracked per
`BrokerConnection` using a `parking_lot::Mutex<Instant>` deadline.

### Automatic Reconnection

When connections fail, krafka automatically attempts to reconnect with exponential backoff:

- **Max Retries**: 3 (configurable)
- **Initial Backoff**: 100ms
- **Max Backoff**: 10 seconds
- **Backoff Multiplier**: 2.0x

```
  Connection Failure
          │
          ▼
  ┌───────────────┐
  │ Wait 100ms    │───► Retry 1
  └───────────────┘
          │ fail
          ▼
  ┌───────────────┐
  │ Wait 200ms    │───► Retry 2
  └───────────────┘
          │ fail
          ▼
  ┌───────────────┐
  │ Wait 400ms    │───► Retry 3
  └───────────────┘
          │ fail
          ▼
    Return Error
```

The reconnection logic checks `is_retriable()` on errors to avoid retrying non-transient failures
(e.g., authentication errors, configuration errors).

### Request/Response Flow

1. Caller creates request struct
2. Request is encoded via `VersionedEncode::encode_versioned(version, buf)` — dispatches to the correct `encode_vN` method
3. Correlation ID is assigned
4. Request is sent over TCP
5. Response is received and framed
6. Response is decoded via `VersionedDecode::decode_versioned(version, buf)` — dispatches to the correct `decode_vN` method

The core protocol request/response type pairs in `protocol::messages` implement the `VersionedEncode`/`VersionedDecode` traits, providing unified version dispatch with unsupported-version error handling.

```rust
// Internal flow
async fn send_request<R>(&self, request: R) -> Result<Response>
where
    R: Into<Bytes>,
{
    let correlation_id = self.correlation_id_gen.next();
    let header = RequestHeader::new(api_key, version, correlation_id);
    
    // Encode and send
    let encoded = encode_request(header, request);
    self.writer.write_all(&encoded).await?;
    
    // Receive and decode
    let response_bytes = self.read_response().await?;
    decode_response(response_bytes)
}
```

## Metadata Management

### Metadata Caching

```
  ┌─────────────────────────────────────────────────────┐
  │                   ClusterMetadata                    │
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

- Automatic refresh when cache is stale (configurable TTL)
- Forced refresh on NotLeaderForPartition errors
- Topic-specific refresh when subscribing
- API version negotiation: negotiates the highest mutually supported Metadata version (v1-v13); versions are cumulative (rack since v1, cluster_id since v2, offline replicas since v5, leader_epoch since v7, topic UUIDs since v10)

### Metadata Recovery (Rebootstrap)

Clients default to `MetadataRecoveryStrategy::Rebootstrap`. When no broker is
reachable for longer than the rebootstrap trigger (default 5 min), the client
automatically closes all connections, clears the metadata cache, and falls back
to bootstrap servers to re-discover the cluster. This handles scenarios like
full-cluster rolling restarts where every cached broker IP becomes stale.

A broker can also request a rebootstrap directly by returning
`REBOOTSTRAP_REQUIRED` (error code **129**) in the top-level `error_code` of a
Metadata v13+ response. Against older brokers only the local timeout trigger
applies. Runtime seed-broker updates are supported via `update_seed_brokers()`.

The strategy itself comes from KIP-899 (Kafka 3.8); the timeout trigger, the
`REBOOTSTRAP_REQUIRED` error code, and defaulting to `Rebootstrap` come from
KIP-1102 (Kafka 4.0).

## Producer Architecture

### Send Path

```
  User Code                     Producer                     Broker
      │                            │                            │
      │  send(topic, key, value)   │                            │
      │ ─────────────────────────> │                            │
      │                            │                            │
      │                 ┌──────────┴──────────┐                 │
      │                 │ 1. Partition        │                 │
      │                 │    (murmur2 hash)   │                 │
      │                 └──────────┬──────────┘                 │
      │                            │                            │
      │                 ┌──────────┴──────────┐                 │
      │                 │ 2. Build RecordBatch│                 │
      │                 │    (compression)    │                 │
      │                 └──────────┬──────────┘                 │
      │                            │                            │
      │                 ┌──────────┴──────────┐                 │
      │                 │ 3. Get Leader Conn  │                 │
      │                 └──────────┬──────────┘                 │
      │                            │                            │
      │                            │    ProduceRequest          │
      │                            │ ─────────────────────────> │
      │                            │                            │
      │                            │    ProduceResponse         │
      │                            │ <───────────────────────── │
      │  RecordMetadata            │                            │
      │ <───────────────────────── │                            │
```

### Partitioning

```rust
// DefaultPartitioner (murmur2, Java-compatible)
fn partition(key: &[u8], partition_count: usize) -> i32 {
    let hash = murmur2(key);
    (hash as usize % partition_count) as i32
}
```

## Consumer Architecture

### Poll Path

```
  User Code                     Consumer                     Broker
      │                            │                            │
      │  poll(timeout)             │                            │
      │ ─────────────────────────> │                            │
      │                            │                            │
      │                 ┌──────────┴──────────┐                 │
      │                 │ For each assigned   │                 │
      │                 │ partition:          │                 │
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

When the broker supports Fetch API v7+, krafka uses incremental fetch sessions to reduce request
sizes. A per-broker `FetchSessionState` tracks the partitions registered with the broker's session.
On each `poll()`, the consumer computes a diff against the previous state:

- **New/changed partitions** go in the `topics` field (only offset and `max_bytes` changes)
- **Removed partitions** go in the `forgotten_topics` field
- The `session_id` and incrementing `session_epoch` (a per-session epoch, not the partition
  leader epoch) maintain session continuity

If the broker returns `FetchSessionIdNotFound` or `InvalidFetchSessionEpoch`, the session is reset
and the next poll sends a full fetch. All sessions are cleared on consumer group rebalance.

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

### Hot Path Inlining

`#[inline]` annotations on critical paths:
- **Protocol primitives**: varint/varlong encoding and decoding
- **Protocol primitives**: i8, i16, i32, u32, i64, bool  
- **Request/response headers**: encode_v0/v1/v2, decode_v0/v1
- **Record encoding/decoding**: Record::encode, Record::decode, RecordHeader encode/decode
- **Hash functions**: murmur2 for partition assignment
- **Accessor methods**: Consumer/Producer record getters
- **Enum conversions**: ApiKey, Compression, TimestampType, RecordBatchAttributes
- **Error handling**: ErrorCode to/from i16 conversions
- **Utilities**: CRC32C checksum, correlation ID generation
- **Partitioners**: All 4 partitioner implementations
- **Batch operations**: try_add, would_fit, track, size checking methods
- **Metadata lookups**: partition_count, partition, leader lookups
- **Predicates**: is_empty, is_null, is_closed, is_retriable, is_ok, is_leader, is_alive
- **Retry policy**: calculate_backoff, should_retry, max_retries_reached
- **Heartbeat controller**: interval, session_timeout, is_running accessors

### Cold Path Optimization

`#[cold]` annotations on error creation paths:
- **Error constructors**: protocol, auth, timeout, broker, config, compression, invalid_state, serialization
- Tells the compiler these paths are unlikely, improving branch prediction on hot paths

### Zero-Copy Design

1. **Zero-copy buffers**: `Bytes` for shared ownership without copying
2. **Pre-allocated buffers**: Capacity hints for vectors
3. **Efficient hashing**: murmur2 for partitioning (Java-compatible)

### Memory Model

```
  Producer Record Journey
  
  User Data (owned)      Producer (borrowed)     Wire (owned)
       │                       │                      │
       ▼                       ▼                      ▼
  ┌─────────┐             ┌─────────┐           ┌─────────┐
  │ Vec<u8> │  ─borrow─>  │  &[u8]  │  ─copy─>  │  Bytes  │
  └─────────┘             └─────────┘           └─────────┘
```

### Lazy Deserialization

`LazyRecordBatch` defers individual record parsing until access:

```rust
use krafka::protocol::LazyRecordBatch;

// Decode batch header but not records
let lazy = LazyRecordBatch::decode(&mut buf)?;

// Iterate and decode on demand
for result in lazy.records() {
    let record = result?;
    if should_process(&record) {
        process(record);
    }
}

// Or convert to eager batch if needed
let batch = lazy.into_record_batch()?;
```

Benefits:
- Avoids parsing records that will be filtered out
- Reduced memory allocation for streaming consumers
- Useful when filtering by offset before accessing key/value

### Pre-allocation

`Vec::with_capacity` used throughout for known-size collections, capped at 10,000 elements to protect against malicious broker responses:
- Record batch building
- Response decoding
- Header collection

All protocol decoding paths cap `Vec::with_capacity(len.min(10_000))` to prevent OOM from broker-supplied lengths.

## Error Handling

### Error Hierarchy

```rust
pub enum KrafkaError {
    Protocol { kind: ProtocolErrorKind, message: String }, // Wire protocol errors; kind drives retry policy
    Broker { code: ErrorCode, message },    // Kafka error codes
    Auth { message: String },               // Authentication failures
    Timeout { operation: String },          // Operation timeouts
    Compression { codec, source },          // Compression errors
    Config { message: String },             // Configuration errors
    InvalidState { message: String },       // State machine errors
    Serialization { message, source },      // Encoding/decoding errors
}
```

### Retriable Errors

Some errors are automatically retriable:
- `NotLeaderForPartition` - Triggers metadata refresh
- `LeaderNotAvailable` - Wait and retry
- Network timeouts - Retry with backoff

## Thread Safety

All krafka types are designed for concurrent use:

- `Producer`: `Send + Sync` - can be shared across tasks
- `Consumer`: `Send + Sync` - can be shared across tasks
- `ShareConsumer`: `Send + Sync` - can be shared across tasks (unstable-protocol feature)
- `AdminClient`: `Send + Sync` - can be shared across tasks

Internal state is protected by:
- `RwLock<T>` for read-heavy data (metadata, offsets)
- `AtomicBool` for flags (closed state)
- `AtomicU8` with `compare_exchange` for transaction state machine
- `Arc<T>` for shared ownership (coordinator state shared with heartbeat task)

Connection pool uses a read-lock fast path for hot-path lookups, dropping all locks before network I/O during reconnection.

## Benchmarks

krafka includes comprehensive Criterion benchmarks in `benches/`:

### Producer Benchmarks (`benches/producer.rs`)
- **Record batch encoding**: 1, 10, 100, 1000 records
- **Compression codecs**: None, Gzip, Snappy, LZ4, Zstd
- **murmur2 hashing**: Various key sizes (8, 32, 128, 512 bytes)
- **Varint encoding**: Signed and unsigned values
- **Roundtrip latency**: Single record encode/decode
- **Partitioners**: Default, RoundRobin, Sticky, Hash strategies

### Consumer Benchmarks (`benches/consumer.rs`)
- **Record batch decoding**: 1, 10, 100, 500 records
- **Decompression**: All 4 compression codecs
- **Record iteration**: Iteration overhead for various batch sizes
- **Lazy vs eager**: Comparison showing 7.5x speedup for streaming

### Protocol Benchmarks (`benches/protocol.rs`)
- **Primitive encode/decode**: i32, i64, bool operations
- **Varint detailed**: 1-5 byte encoding/decoding performance
- **CRC32C checksum**: 64B to 16KB data sizes
- **Request headers**: v0, v1, v2 encoding
- **Error code conversions**: from_i16, to_i16, is_retriable
- **API key conversions**: from_i16, to_i16

Run benchmarks with:
```bash
cargo bench
```

## Implemented Features

krafka includes the following production-ready features:

- ✅ **Transactional Producer**: Exactly-once semantics with `TransactionalProducer`
- ✅ **Incremental Fetch Sessions**: KIP-227 — bandwidth-efficient incremental fetches with per-broker session tracking
- ✅ **TLS/SSL encryption**: Secure connections with rustls and mTLS support
- ✅ **AWS MSK IAM authentication**: Native support with optional SDK integration
- ✅ **SASL/SCRAM Authentication**: SHA-256 and SHA-512 mechanisms
- ✅ **Session Reauthentication (KIP-368)**: Proactive session lifetime tracking with automatic connection replacement before SASL session expiry
- ✅ **Metrics and Observability**: Producer, consumer, and connection metrics
- ✅ **ACL Management**: Create, describe, and delete ACLs
- ✅ **Security Hardening**: Secret zeroization, constant-time auth, PBKDF2 validation, decompression limits, allocation caps
- ✅ **SOCKS5 Proxy**: Route all broker connections through a SOCKS5 proxy (VPN/bastion setups)
