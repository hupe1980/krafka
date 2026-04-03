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

// Prefer Fetch v7..=v11; fall back to v4 if the broker doesn't support v7+.
let fetch_version = match conn.negotiate_api_version(ApiKey::Fetch, 11, 7).await {
    Some(v) => v,
    None => conn.negotiate_api_version(ApiKey::Fetch, 4, 4).await
        .expect("broker does not support any usable Fetch version"),
};
println!("Using Fetch v{}", fetch_version);

// Convenience method with min=0
let version = conn.negotiate_api_version_max(ApiKey::Produce, 3).await;
```

### Client Supported Versions

Krafka supports the following API version ranges (clamped to match actual encode/decode implementations):

| API | Min | Max | Key Features |
|-----|-----|-----|--------------|
| Produce | 0 | 3 | v3 transactions, headers |
| Fetch | 0 | 11 | v0-4, v7-v11 (v5/v6 unsupported); v4 isolation level, v7 fetch sessions (KIP-227), v9 leader epoch fencing (KIP-320), v11 closest-replica fetching (KIP-392) |
| ListOffsets | 0 | 2 | v2 isolation level |
| Metadata | 0 | 8 | v1 controller + rack, v2 cluster_id, v3 throttle, v5 offline replicas, v7 leader epoch, v8 adds cluster/topic authorized-operations (decoded and discarded) |
| OffsetCommit | 0 | 2 | v2 retention |
| OffsetFetch | 0 | 1 | v1 group coordinator |
| FindCoordinator | 0 | 1 | Group/txn coordinator lookup |
| JoinGroup | 0 | 5 | v5 group instance id |
| Heartbeat | 0 | 3 | v3 group instance id (KIP-345) |
| SyncGroup | 0 | 3 | v3 group instance id |
| LeaveGroup | 0 | 3 | v3 batch leave (KIP-345) |
| CreateTopics | 0 | 2 | Topic creation |
| DeleteTopics | 0 | 1 | Topic deletion |
| CreatePartitions | 0 | 0 | Partition management |
| DescribeConfigs | 0 | 0 | Config reading |
| AlterConfigs | 0 | 0 | Config updates |
| DescribeAcls | 0 | 0 | ACL queries |
| CreateAcls | 0 | 0 | ACL creation |
| DeleteAcls | 0 | 0 | ACL deletion |
| DescribeGroups | 0 | 1 | Consumer group inspection |
| ListGroups | 0 | 1 | Consumer group listing |
| DeleteRecords | 0 | 0 | Log truncation |
| OffsetForLeaderEpoch | 0 | 2 | Leader epoch validation |
| InitProducerId | 0 | 0 | Idempotent/transactional |

### Version Constants

Client-supported versions are defined in `krafka::protocol::versions`:

```rust
use krafka::protocol::versions;

// Maximum versions the client supports
let max_fetch = versions::FETCH_MAX;        // 11 (v0-4 and v7-v11; v5/v6 unsupported)
let max_produce = versions::PRODUCE_MAX;    // 3
let max_metadata = versions::METADATA_MAX;  // 8
```

## Record Batches

Krafka uses Kafka's v2 record batch format with:

- Magic byte 2 (modern format)
- CRC32C checksums (validated on decode)
- Variable-length encoding for efficiency
- Optional compression (gzip, snappy, lz4, zstd)

### Header Versioning

Every Kafka request/response is prefixed with a header whose format depends on
whether the API version uses flexible encoding:

| Header state | Request header | Response header |
|-------------|----------------|-----------------|
| Non-flexible | v1 — standard `KafkaString` for client_id | v0 — correlation_id only |
| Flexible | v2 — compact string for client_id + tagged fields | v1 — correlation_id + tagged fields |

The transition version varies per API (e.g., Fetch becomes flexible at v12,
Produce at v9). `ApiKey::flexible_version()` returns the threshold for each API,
and the header is selected automatically by `RequestHeader::encode()` /
`ResponseHeader::decode()`.

**Note:** `ApiVersions` response always uses header v0 regardless of the API
version (needed for protocol bootstrapping).

### Unified Version Dispatch

Core request/response message types in `krafka::protocol` implement the `VersionedEncode` and `VersionedDecode` traits, which dispatch to the correct `encode_vN`/`decode_vN` method based on the protocol version number:

```rust
use krafka::protocol::{VersionedEncode, VersionedDecode, MetadataRequest, MetadataResponse};

let request = MetadataRequest::all_topics();
let mut buf = bytes::BytesMut::new();

// Encode for a specific protocol version — dispatches to the right encoder
request.encode_versioned(1, &mut buf)?;

// In real usage, `response_buf` would be filled with bytes read from the network.
let mut response_buf = buf.freeze();

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

- **Decode array bounds**: Every array-length field decoded from the wire is validated against `MAX_DECODE_ARRAY_LEN` (100,000), typically via `check_decode_array_len()` and in some specialized decode paths (e.g., `KafkaArray::decode`, record batch counts) via equivalent local checks. These checks reject negative counts and oversized counts across all 63+ protocol-message decode sites, `KafkaArray` decode paths, and record batch/header counts. The validation runs *before* any `Vec::with_capacity()` allocation, preventing both OOM and runaway decode loops.
- **Decompression limits**: Decompressed record data is limited to 128 MiB via streaming `.take()` limits and post-decompression size checks
- **Record headers**: Record headers are preserved during batch building — no silent data loss
- **Encode validation**: The `TryEncode` trait provides fallible encoding for protocol primitives (`KafkaString`, `KafkaBytes`, `KafkaArray<T>` where `T: TryEncode`, `TaggedFields`), returning errors instead of panicking on oversized data. `ProducerRecord::validate()` checks wire-format limits at the API boundary before encoding
- **Fuzz testing**: The `fuzz/` directory provides [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) targets for `KafkaArray` decode, `RecordBatch` decode, and response message decode across multiple API versions. See `fuzz/README.md` for usage.

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
