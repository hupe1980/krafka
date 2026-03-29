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

Krafka supports the following API version ranges (clamped to match actual encode/decode implementations):

| API | Min | Max | Key Features |
|-----|-----|-----|--------------|
| Produce | 0 | 3 | v3 transactions, headers |
| Fetch | 0 | 4, 7 | v4 isolation level, v7 fetch sessions (KIP-227); v5/v6 not supported |
| ListOffsets | 0 | 2 | v2 isolation level |
| Metadata | 0 | 1 | v1 controller info |
| OffsetCommit | 0 | 2 | v2 retention |
| OffsetFetch | 0 | 1 | v1 group coordinator |
| FindCoordinator | 0 | 1 | Group/txn coordinator lookup |
| JoinGroup | 0 | 5 | v5 group instance id |
| Heartbeat | 0 | 0 | Standard heartbeat |
| SyncGroup | 0 | 3 | v3 group instance id |
| LeaveGroup | 0 | 1 | v1 with response |
| CreateTopics | 0 | 3 | Topic creation |
| DeleteTopics | 0 | 1 | Topic deletion |
| DescribeConfigs | 0 | 1 | Config reading |
| AlterConfigs | 0 | 1 | Config updates |
| InitProducerId | 0 | 1 | Idempotent/transactional |

### Version Constants

Client-supported versions are defined in `krafka::protocol::versions`:

```rust
use krafka::protocol::versions;

// Maximum versions the client supports
let max_fetch = versions::FETCH_MAX;        // 7 (only v0-4 and v7; use try-v7-else-v4 pattern)
let max_produce = versions::PRODUCE_MAX;    // 3
let max_metadata = versions::METADATA_MAX;  // 1
```

## Record Batches

Krafka uses Kafka's v2 record batch format with:

- Magic byte 2 (modern format)
- CRC32C checksums (validated on decode)
- Variable-length encoding for efficiency
- Optional compression (gzip, snappy, lz4, zstd)

### Unified Version Dispatch

All message types exposed via `krafka::protocol` implement `VersionedEncode` and `VersionedDecode` traits that dispatch to the correct `encode_vN`/`decode_vN` method based on the protocol version number:

```rust
use krafka::protocol::{VersionedEncode, VersionedDecode, MetadataRequest, MetadataResponse};

let request = MetadataRequest::all_topics();
let mut buf = bytes::BytesMut::new();

// Encode for a specific protocol version — dispatches to the right encoder
request.encode_versioned(1, &mut buf)?;

// Decode response for a specific version
let response = MetadataResponse::decode_versioned(1, &mut response_buf)?;
```

Unsupported version numbers (including negative values) return a descriptive `KrafkaError::protocol` error.

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

> **Note:** Decompression output is capped at 128 MiB to protect against compression bombs. Compressed payloads that expand beyond this limit will return a `KrafkaError::compression` error.

## Protocol Safety

Krafka protects against malicious or corrupted broker responses:

- **Allocation caps**: All `Vec::with_capacity()` calls in protocol decoding are capped at 10,000 elements, preventing OOM from broker-supplied lengths
- **Decompression limits**: Decompressed record data is limited to 128 MiB via streaming `.take()` limits and post-decompression size checks
- **Record headers**: Record headers are preserved during batch building — no silent data loss
- **Encode validation**: The `TryEncode` trait provides fallible encoding for protocol primitives (`KafkaString`, `KafkaBytes`, `KafkaArray<T>` where `T: TryEncode`, `TaggedFields`), returning errors instead of panicking on oversized data. `ProducerRecord::validate()` checks wire-format limits at the API boundary before encoding

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
