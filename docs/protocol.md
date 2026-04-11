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
5. Each request negotiates the best version within the client's `[MIN, MAX]` range

### Using Version Negotiation

```rust
use krafka::protocol::ApiKey;

// negotiate_api_version(api_key, max, min) clamps to client MIN..MAX and broker range.
let fetch_version = conn
    .negotiate_api_version(ApiKey::Fetch, 12, 4)
    .await
    .expect("broker does not support any usable Fetch version");
println!("Using Fetch v{}", fetch_version);
```

### Minimum Broker Version

Krafka **requires Apache Kafka 3.9 or later**. The MIN constants for all APIs
are set so that pre-3.9 protocol features (e.g., Metadata v0, Produce v0-v2,
Fetch v0-v3) are no longer supported. Connecting to an older broker will fail
version negotiation for most APIs.

### Client Supported Versions

Every API has a `MIN` and `MAX` constant in `krafka::protocol::versions`.
The client only encodes/decodes versions within `[MIN, MAX]`; versions outside
this range are rejected with a protocol error.

| API | Min | Max | Key Features |
|-----|-----|-----|--------------|
| Produce | 3 | 11 | v3 transactions, v9 flexible encoding, v11 ZStd compression |
| Fetch | 4 | 12 | v4 isolation level, v7 fetch sessions (KIP-227), v9 leader epoch (KIP-320), v11 closest-replica (KIP-392), v12 flexible |
| ListOffsets | 1 | 8 | v1 timestamp queries, v2 isolation level, v4 leader epoch, v6 flexible, v7 max_timestamp, v8 tiered-storage |
| Metadata | 1 | 12 | v1 controller + rack, v7 leader epoch, v8 authorized-ops, v9 flexible, v10 topic UUIDs, v12 topic-ID lookup |
| OffsetCommit | 2 | 9 | v2 retention, v5 drops retention_time, v6 leader epoch, v8 flexible, v9 KIP-848 |
| OffsetFetch | 1 | 9 | v1 group coordinator, v2 top-level error, v6 flexible, v8 batched groups, v9 KIP-848 |
| FindCoordinator | 1 | 6 | v1 key_type, v3 flexible, v4 batched keys (KIP-699), v6 share groups (KIP-932) |
| JoinGroup | 4 | 9 | v4 group_instance_id (KIP-345), v6 flexible, v8 reason (KIP-800) |
| Heartbeat | 3 | 4 | v3 group_instance_id (KIP-345), v4 flexible |
| SyncGroup | 3 | 5 | v3 group_instance_id, v4 flexible, v5 protocol_type/name (KIP-559) |
| LeaveGroup | 3 | 5 | v3 batch leave (KIP-345), v4 flexible, v5 reason (KIP-800) |
| CreateTopics | 2 | 7 | v2 topic validation, v5 flexible, v7 topic_id (KIP-464, KIP-525) |
| DeleteTopics | 1 | 6 | v1 baseline, v4 flexible, v6 topic-ID-based deletion |
| CreatePartitions | 0 | 3 | v0 baseline, v2 flexible, v3 KIP-599 |
| DescribeConfigs | 0 | 4 | v1 synonyms, v3 config_type + documentation, v4 flexible |
| IncrementalAlterConfigs | 0 | 1 | v0 non-flexible, v1 flexible encoding |
| DescribeAcls | 1 | 3 | v1 prefixed ACLs, v2 flexible, v3 user resource type |
| CreateAcls | 1 | 3 | v1 prefixed ACLs, v2 flexible, v3 user resource type |
| DeleteAcls | 1 | 3 | v1 prefixed ACLs, v2 flexible, v3 user resource type |
| DescribeGroups | 1 | 6 | v3 authorized_operations, v4 static members, v5 flexible, v6 KIP-1043 |
| ListGroups | 1 | 5 | v3 flexible, v4 state filter (KIP-518), v5 type filter (KIP-848) |
| DeleteRecords | 0 | 2 | v0 baseline, v2 flexible encoding |
| OffsetForLeaderEpoch | 2 | 4 | v2 leader epoch validation, v3 replica_id, v4 flexible |
| InitProducerId | 0 | 4 | v0 idempotent, v2 flexible, v3 epoch recovery, v4 latest stable |
| AddPartitionsToTxn | 0 | 3 | v0 baseline, v3 flexible encoding |
| AddOffsetsToTxn | 0 | 3 | v0 baseline, v3 flexible encoding |
| EndTxn | 0 | 3 | v0 baseline, v3 flexible encoding |
| TxnOffsetCommit | 0 | 3 | v0 baseline, v2 leader epoch, v3 flexible + consumer fields |
| CreateDelegationToken | 1 | 3 | v2 flexible, v3 owner override |
| RenewDelegationToken | 1 | 2 | v2 flexible encoding |
| ExpireDelegationToken | 1 | 2 | v2 flexible encoding |
| DescribeDelegationToken | 1 | 3 | v2 flexible, v3 token requester |
| DescribeClientQuotas | 0 | 1 | v1 flexible encoding |
| AlterClientQuotas | 0 | 1 | v1 flexible encoding |
| DeleteGroups | 0 | 2 | Consumer group deletion |
| DescribeCluster | 0 | 2 | Cluster metadata |
| ApiVersions | 0 | 4 (5¹) | API version negotiation |
| ConsumerGroupHeartbeat | 0 | 1 | KIP-848 consumer group protocol, v1 KIP-1082 regex |
| ConsumerGroupDescribe | 0 | 1 | KIP-848 group description |
| DescribeTopicPartitions | 0 | 0 | Topic partition metadata (KIP-966) |
| UpdateFeatures | 0 | 1 | Cluster feature versioning (KIP-584), v1 UpgradeType + ValidateOnly |
| GetTelemetrySubscriptions² | 0 | 0 | KIP-714 client telemetry subscription discovery |
| PushTelemetry² | 0 | 0 | KIP-714 client telemetry push |
| ShareGroupHeartbeat¹ | 1 | 1 | KIP-932 share group heartbeat |
| ShareGroupDescribe¹ | 1 | 1 | KIP-932 share group description |
| ShareFetch¹ | 1 | 2 | KIP-932 share fetch, v2 acquire mode (KIP-1206) + renew ack (KIP-1222) |
| ShareAcknowledge¹ | 1 | 2 | KIP-932 share acknowledge, v2 renew ack (KIP-1222) |

> ¹ Requires `unstable-protocol` feature flag. Max shown in parentheses is the feature-gated max.
>
> ² Requires `telemetry` feature flag.
>
> **Note:** Encode/decode implementations exist for even higher versions of some APIs (e.g., Produce up to v13, Fetch up to v18, Metadata up to v13) but the negotiated MAX is set conservatively for topic-UUID-based paths until those have been integration-tested against a real broker.

### Version Constants

Client-supported versions are defined in `krafka::protocol::versions`:

```rust
use krafka::protocol::versions;

// Each API has both MIN and MAX constants
let min_fetch = versions::FETCH_MIN;        // 4  (Kafka 3.9+ baseline)
let max_fetch = versions::FETCH_MAX;        // 12 (v12 flexible encoding)
let min_produce = versions::PRODUCE_MIN;    // 3  (v3+ transactions)
let max_produce = versions::PRODUCE_MAX;    // 11 (v11 ZStd compression)
let max_metadata = versions::METADATA_MAX;  // 12 (v12 topic-ID lookup)
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
