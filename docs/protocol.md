---
layout: default
title: Protocol & Versions
nav_order: 9
description: "Kafka protocol implementation and version negotiation"
---

# Protocol & Versions Guide

This guide covers Krafka's Kafka protocol implementation and API version negotiation.

## Overview

Krafka implements the Kafka wire protocol with support for:

- Automatic API version negotiation
- Multiple protocol versions per API
- All standard compression codecs
- Zero-copy message handling

## Version Negotiation

On connection, Krafka automatically fetches the broker's supported API versions and stores them.
This enables dynamic version negotiation for optimal compatibility and feature usage.

### How It Works

1. Client connects to broker
2. Client sends `ApiVersions` request
3. Broker responds with supported API version ranges
4. Client stores version ranges for future requests
5. Each request can negotiate the best version

### Using Version Negotiation

```rust
use krafka::protocol::ApiKey;

// After connection, negotiate the best Fetch version
// Client supports v4-v12, broker might support v0-v13
let version = conn.negotiate_api_version(ApiKey::Fetch, 12, 4).await;

match version {
    Some(v) => println!("Using Fetch v{}", v),
    None => println!("No compatible version found!"),
}

// Convenience method with min=0
let version = conn.negotiate_api_version_max(ApiKey::Produce, 9).await;
```

### Client Supported Versions

Krafka supports the following API version ranges:

| API | Min | Max | Key Features |
|-----|-----|-----|--------------|
| Produce | 0 | 9 | v3+ transactions, v5+ headers |
| Fetch | 0 | 12 | v4+ leader epoch |
| Metadata | 0 | 12 | v1+ controller info |
| OffsetCommit | 0 | 8 | v2+ retention |
| OffsetFetch | 0 | 8 | v1+ group coordinator |
| FindCoordinator | 0 | 4 | v1+ key type |
| JoinGroup | 0 | 9 | v1+ rebalance timeout |
| Heartbeat | 0 | 4 | Standard heartbeat |
| SyncGroup | 0 | 5 | Group sync |
| LeaveGroup | 0 | 5 | Leave group |
| CreateTopics | 0 | 7 | Topic creation |
| DeleteTopics | 0 | 6 | Topic deletion |
| DescribeConfigs | 0 | 4 | Config reading |
| AlterConfigs | 0 | 2 | Config updates |
| InitProducerId | 0 | 4 | Idempotent/transactional |

### Version Constants

Client-supported versions are defined in `krafka::protocol::versions`:

```rust
use krafka::protocol::versions;

// Maximum versions the client supports
let max_fetch = versions::FETCH_MAX;        // 12
let max_produce = versions::PRODUCE_MAX;    // 9
let max_metadata = versions::METADATA_MAX;  // 12
```

## Record Batches

Krafka uses Kafka's v2 record batch format with:

- Magic byte 2 (modern format)
- CRC32C checksums (validated on decode)
- Variable-length encoding for efficiency
- Optional compression (gzip, snappy, lz4, zstd)

### Creating Records

```rust
use krafka::protocol::{RecordBatchBuilder, Compression};

let batch = RecordBatchBuilder::new()
    .compression(Compression::Snappy)
    .add_record(Some(b"key"), Some(b"value"), vec![])
    .add_record(None, Some(b"value-only"), vec![])
    .build()?;
```

### Compression Support

| Codec | Feature | Notes |
|-------|---------|-------|
| None | Default | No compression |
| Gzip | Always | Good compression, slower |
| Snappy | Always | Fast, moderate compression |
| LZ4 | Always | Very fast, good compression |
| Zstd | Always | Best compression, fast |

## Wire Protocol

### Request/Response Framing

```text
+----------------+----------------+
|  Size (4B)     |  Data (N bytes)|
+----------------+----------------+
```

All messages are length-prefixed with a 4-byte big-endian size field.

### Request Header

```text
+----------+----------+---------------+-------------+
| API Key  | Version  | Correlation ID| Client ID   |
| (2 bytes)| (2 bytes)| (4 bytes)     | (variable)  |
+----------+----------+---------------+-------------+
```

### Response Header

```text
+---------------+
| Correlation ID|
| (4 bytes)     |
+---------------+
```

## Zero-Copy Design

Krafka uses `bytes::Bytes` throughout for zero-copy buffer management:

- Incoming data is parsed without copying
- Record payloads share underlying buffers
- Memory is released when last reference drops

## Next Steps

- [Producer Guide](producer.md) - Sending messages
- [Consumer Guide](consumer.md) - Receiving messages
- [Configuration Reference](configuration.md) - All settings
