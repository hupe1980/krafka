# Performance Tuning Guide

This guide covers Krafka's built-in performance optimizations and how to tune them for extreme high-throughput scenarios.

## Request Priority Channels

Krafka implements priority-based request scheduling to prevent consumer group ejection during backpressure.

### How It Works

Each connection maintains two channels:
- **High-priority channel**: Heartbeats, metadata refreshes, coordinator discovery
- **Normal-priority channel**: Produce, fetch, and other data requests

The connection task always checks the high-priority channel first, ensuring time-sensitive requests are never starved by data traffic.

### Priority Assignment

Priority is automatically assigned based on API key:

| Priority | API Keys |
|----------|----------|
| High | `Heartbeat`, `Metadata`, `FindCoordinator`, `ApiVersions`, `LeaderAndIsr` |
| Normal | `Produce`, `Fetch`, `OffsetCommit`, `OffsetFetch`, and all others |

### Configuration

```rust
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

## Multi-Connection Bundles

For extreme high-throughput scenarios (>100k messages/second per broker), multiple TCP connections can parallelize I/O operations.

### When to Use

- Single connection saturates at ~50-100k msg/s depending on message size
- You're CPU-bound on serialization/deserialization
- Network latency is variable and you want to pipeline more requests
- You have multiple producer/consumer threads targeting the same broker

### Configuration

```rust
use krafka::network::ConnectionConfig;

let config = ConnectionConfig::builder()
    .connections_per_broker(4)  // 4 parallel connections
    .build();
```

### Recommended Values

| Scenario | Connections | Notes |
|----------|-------------|-------|
| Standard workloads | 1 (default) | Sufficient for most use cases |
| High throughput | 2-4 | Good for >50k msg/s per broker |
| Extreme throughput | 4-8 | For >100k msg/s per broker |
| Latency-sensitive | 2 | Reduces head-of-line blocking |

### Using Connection Bundles

The `BrokerConnectionBundle` provides round-robin connection selection:

```rust
use krafka::network::BrokerConnectionBundle;

// Create a bundle with the configured number of connections
let bundle = BrokerConnectionBundle::connect("broker:9092", config).await?;

// Get a connection using round-robin selection
let conn = bundle.select();

// Or select by specific index for request affinity
let conn = bundle.get(0).unwrap();

// Check bundle health
println!("Alive connections: {}/{}", bundle.alive_count(), bundle.len());
```

### Automatic Selection

When using the connection pool, bundles are managed automatically:

```rust
use krafka::network::ConnectionPool;

let config = ConnectionConfig::builder()
    .connections_per_broker(4)
    .build();

let pool = ConnectionPool::new(config);

// Pool internally uses bundles, returns connections transparently
let conn = pool.get_connection("broker:9092").await?;
```

> **Note:** The connection pool uses a read-lock fast path for hot-path lookups. During reconnection, all locks are dropped before performing network I/O, preventing deadlocks and enabling concurrent access to other brokers while one broker is being reconnected.

## Zero-Copy Message Handling

Krafka uses `bytes::Bytes` throughout for zero-copy buffer management:

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

The consumer automatically batches fetch requests by leader broker:

```rust
let consumer = ConsumerBuilder::new()
    .fetch_min_bytes(1024)      // Wait for at least 1KB
    .fetch_max_bytes(1024 * 1024)  // Max 1MB per fetch
    .fetch_max_wait(Duration::from_millis(100))  // Max wait time
    .build();
```

## Memory Backpressure

Configure memory limits to prevent OOM during high throughput. When the buffer is full, `send()` blocks the caller for up to `max_block_ms` waiting for in-flight batches to drain, matching the Kafka Java client's `max.block.ms` semantics:

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
