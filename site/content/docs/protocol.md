+++
title = "Protocol Support"
description = "Supported Kafka APIs and versions, negotiation, and how parity with Apache Kafka is enforced in CI."
weight = 130

[extra]
slug_id = "protocol"
+++

## Overview

krafka implements the Kafka wire protocol with support for:

- Automatic API version negotiation
- Multiple protocol versions per API
- All standard compression codecs
- Zero-copy message handling

## Version Negotiation

On connection, krafka automatically fetches the broker's supported API versions and stores them.
This enables dynamic version negotiation for optimal compatibility and feature usage.

### How It Works

1. Client connects to broker
2. Client sends `ApiVersions` request
3. Broker responds with supported API version ranges
4. Client stores version ranges for future requests
5. Each request negotiates the best version within the client's `[MIN, MAX]` range

### Bootstrapping `ApiVersions` itself

`ApiVersions` is the one API whose version cannot be negotiated from a previous
`ApiVersions` response — it *is* the negotiation. krafka therefore sends
`versions::API_VERSIONS_MAX` (v4 by default) and, if the broker answers
`UNSUPPORTED_VERSION`, retries at the ceiling that rejection advertises. The
protocol mandates that a rejection is encoded with the **v0** response layout
precisely so a client that guessed too high can still parse the reply, so the
fallback costs exactly one extra round trip and never fails the handshake.

The default ceiling is the highest version a *released* Kafka supports, not the
highest krafka can encode: sending v5 (KIP-1242, unreleased) to a Kafka 4.x
broker would cost a rejected round trip on **every** connection. v5 is available
behind the `unstable-protocol` feature for testing against unreleased builds.

Negotiating v3+ rather than pinning v0 is what puts two things on the wire:

- **KIP-511** — `ClientSoftwareName` / `ClientSoftwareVersion`, which is how a
  broker's `client.software.name` / `client.software.version` metrics identify
  krafka. These fields do not exist below v3.
- **KIP-584** — `SupportedFeatures` and `FinalizedFeatures`, carried in v3+
  tagged fields. krafka caches them per connection; read them with
  [`BrokerConnection::broker_features()`], which exposes
  `finalized_level(name)` and `supported_range(name)` so callers can gate
  optional behaviour on a cluster-wide feature level without a second round
  trip.

[`BrokerConnection::broker_features()`]: https://docs.rs/krafka/latest/krafka/network/struct.BrokerConnection.html#method.broker_features

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

### Leader epochs end to end (KIP-320)

KIP-320 only detects log truncation if the leader epoch travels with the
position everywhere it goes. krafka sends it on all three legs:

| Leg | Field | What it buys |
|---|---|---|
| `Fetch` | `current_leader_epoch`, `last_fetched_epoch` | The broker reports `diverging_epoch` when the client's log no longer matches its own. |
| `ListOffsets` | `current_leader_epoch` | A reset resolved against a stale leader is rejected with `FENCED_LEADER_EPOCH` instead of returning an offset from a log the client cannot vouch for. |
| `OffsetCommit` → `OffsetFetch` | `committed_leader_epoch` | The check survives a restart or a rebalance: the next owner of the partition resumes with the epoch the position was read at. |

Missing any one leg silently degrades the guarantee rather than breaking
visibly — the client keeps working and simply stops noticing truncation. The
commit leg is the easiest to overlook, because its absence is invisible until a
consumer restarts.

A fenced `ListOffsets` forces a metadata refresh before the retry, so the
epoch check converges instead of failing identically forever.

`-1` remains the correct value where the client genuinely has no epoch: a
position that came from a `seek()` or an offset reset, or an offset set
administratively through `AdminClient::alter_consumer_group_offsets`. Inventing
one there would defeat the check it feeds.

### Minimum Broker Version

krafka **requires Apache Kafka 3.9 or later**. The MIN constants for all APIs
are set so that pre-3.9 protocol features (e.g., Metadata v0, Produce v0-v2,
Fetch v0-v3) are no longer supported. Connecting to an older broker will fail
version negotiation for most APIs.

### Client Supported Versions

Every API has a `MIN` and `MAX` constant in `krafka::protocol::versions`.
The client only encodes/decodes versions within `[MIN, MAX]`; versions outside
this range are rejected with a protocol error.

| API | Min | Max | Key Features |
|-----|-----|-----|--------------|
| Produce | 3 | 13 | v3 transactions, v9 flexible encoding, v11 ZStd compression, v13 topic UUIDs (KIP-516) |
| Fetch | 4 | 18 | v4 isolation level, v7 fetch sessions (KIP-227), v9 leader epoch (KIP-320), v11 closest-replica (KIP-392), v12 flexible, v13 topic UUIDs (KIP-516), v15 remove ReplicaId (KIP-903), v17 directory ID (KIP-853), v18 high-watermark (KIP-1166) |
| ListOffsets | 1 | 11 | v1 timestamp queries, v2 isolation level, v4 leader epoch, v6 flexible, v7 max_timestamp, v8 tiered-storage, v9 KIP-1005, v10 KIP-1075 timeout, v11 KIP-1023 |
| Metadata | 1 | 13 | v1 controller + rack, v7 leader epoch, v8 authorized-ops, v9 flexible, v10 topic UUIDs, v12 topic-ID lookup, v13 top-level error_code |
| OffsetCommit | 2 | 10 | v2 retention, v5 drops retention_time, v6 leader epoch, v8 flexible, v9 KIP-848 member_epoch, v10 topic UUIDs (KIP-848) |
| OffsetFetch | 1 | 10 | v1 group coordinator, v2 top-level error, v6 flexible, v8 batched groups, v9 KIP-848 member_epoch, v10 topic UUIDs (KIP-848) |
| FindCoordinator | 1 | 6 | v1 key_type, v3 flexible, v4 batched keys (KIP-699), v6 share groups (KIP-932) |
| JoinGroup | 4 | 9 | v4 group_instance_id (KIP-345), v6 flexible, v8 reason (KIP-800) |
| Heartbeat | 3 | 4 | v3 group_instance_id (KIP-345), v4 flexible |
| SyncGroup | 3 | 5 | v3 group_instance_id, v4 flexible, v5 protocol_type/name (KIP-559) |
| LeaveGroup | 3 | 5 | v3 batch leave (KIP-345), v4 flexible, v5 reason (KIP-800) |
| CreateTopics | 2 | 7 | v2 topic validation, v5 flexible, v7 topic_id (KIP-464, KIP-525) |
| DeleteTopics | 1 | 6 | v1 baseline, v4 flexible, v6 topic-ID-based deletion |
| CreatePartitions | 0 | 3 | v0 baseline, v2 flexible, v3 KIP-599 |
| DescribeConfigs | 1 | 4 | v1 synonyms, v3 config_type + documentation, v4 flexible (Kafka 4.0 removed v0) |
| IncrementalAlterConfigs | 0 | 1 | v0 non-flexible, v1 flexible encoding |
| DescribeAcls | 1 | 3 | v1 prefixed ACLs, v2 flexible, v3 user resource type |
| CreateAcls | 1 | 3 | v1 prefixed ACLs, v2 flexible, v3 user resource type |
| DeleteAcls | 1 | 3 | v1 prefixed ACLs, v2 flexible, v3 user resource type |
| DescribeGroups | 1 | 6 | v3 authorized_operations, v4 static members, v5 flexible, v6 KIP-1043 |
| ListGroups | 1 | 5 | v3 flexible, v4 state filter (KIP-518), v5 type filter (KIP-848) |
| DeleteRecords | 0 | 2 | v0 baseline, v2 flexible encoding |
| OffsetForLeaderEpoch | 2 | 4 | v2 leader epoch validation, v3 replica_id, v4 flexible |
| InitProducerId | 0 | 5 (6¹) | v0 idempotent, v2 flexible, v3 epoch recovery, v4 latest stable, v5 KIP-890 txn_state, v6 KIP-939 two-phase commit |
| AddPartitionsToTxn | 0 | 5 | v0 baseline, v3 flexible encoding, v4–v5 KIP-890 Transactions array format |
| AddOffsetsToTxn | 0 | 4 | v0 baseline, v3 flexible encoding, v4 KIP-890 error codes |
| EndTxn | 0 | 5 | v0 baseline, v3 flexible encoding, v4–v5 KIP-890 epoch bump + txn_state |
| TxnOffsetCommit | 0 | 5 | v0 baseline, v2 leader epoch, v3 flexible + consumer fields, v4–v5 KIP-890 fields |
| WriteTxnMarkers | 1 | 2 | Broker-facing transaction marker write; v2 flexible |
| DescribeProducers | 0 | 0 | Active producer state per partition (KIP-664), for diagnosing hung transactions |
| DescribeTransactions | 0 | 0 | Transaction state by transactional ID (KIP-664) |
| CreateDelegationToken | 1 | 3 | v2 flexible, v3 owner override |
| RenewDelegationToken | 1 | 2 | v2 flexible encoding |
| ExpireDelegationToken | 1 | 2 | v2 flexible encoding |
| DescribeDelegationToken | 1 | 3 | v2 flexible, v3 token requester |
| DescribeUserScramCredentials | 0 | 0 | SCRAM credential *metadata* — mechanism and iteration count only; Kafka never returns salt or stored key |
| AlterUserScramCredentials | 0 | 0 | Create, update or delete SCRAM credentials (KIP-554) |
| DescribeClientQuotas | 0 | 1 | v1 flexible encoding |
| AlterClientQuotas | 0 | 1 | v1 flexible encoding |
| DeleteGroups | 0 | 2 | Consumer group deletion |
| OffsetDelete | 0 | 0 | Delete committed offsets for specific partitions without deleting the group |
| DescribeCluster | 0 | 2 | Cluster metadata |
| DescribeLogDirs | 1 | 5 | v2 flexible, v3 top-level error_code, v4 TotalBytes + UsableBytes, v5 IsCordoned (KIP-1066) |
| AlterReplicaLogDirs | 1 | 2 | Move a replica between log directories on a broker; v2 flexible |
| AlterPartitionReassignments | 0 | 1 | v0 KIP-455, v1 AllowReplicationFactorChange |
| ListPartitionReassignments | 0 | 0 | v0 only (KIP-455) |
| DescribeQuorum | 0 | 2 | v0 KRaft quorum state, v1 replica timestamps (KIP-836), v2 Nodes + ReplicaDirectoryId + error messages (KIP-853) |
| ElectLeaders | 0 | 2 | v0 preferred-only, v1 ElectionType (KIP-460), v2 flexible |
| ListTransactions | 0 | 2 | v0 KIP-664, v1 DurationFilter (KIP-994), v2 TransactionalIdPattern (KIP-1152) |
| ListConfigResources | 0 | 1 | v0 client metrics (KIP-714), v1 arbitrary resource types (KIP-1142) |
| ApiVersions | 0 | 4 (5¹) | API version negotiation |
| ConsumerGroupHeartbeat | 0 | 1 | KIP-848 consumer group protocol, v1 KIP-1082 regex |
| ConsumerGroupDescribe | 0 | 1 | KIP-848 group description |
| DescribeTopicPartitions | 0 | 0 | Topic partition metadata (KIP-966) |
| UpdateFeatures | 0 | 2 | Cluster feature versioning (KIP-584), v1 UpgradeType + ValidateOnly, v2 drops per-feature results |
| GetTelemetrySubscriptions² | 0 | 0 | KIP-714 client telemetry subscription discovery |
| PushTelemetry² | 0 | 0 | KIP-714 client telemetry push |
| ShareGroupHeartbeat¹ | 1 | 1 | KIP-932 share group heartbeat |
| ShareGroupDescribe¹ | 1 | 1 | KIP-932 share group description |
| ShareFetch¹ | 1 | 2 | KIP-932 share fetch, v2 acquire mode (KIP-1206) + renew ack (KIP-1222) |
| ShareAcknowledge¹ | 1 | 2 | KIP-932 share acknowledge, v2 renew ack (KIP-1222) |
| DescribeShareGroupOffsets | 0 | 1 | KIP-932 share-partition start offsets, v1 Lag (KIP-1226) |
| AlterShareGroupOffsets | 0 | 0 | KIP-932 share-group offset reset (group must be empty) |
| StreamsGroupDescribe | 0 | 0 | KIP-1071 Streams group describe — topology, members, task assignments and changelog offsets |
| DeleteShareGroupOffsets | 0 | 0 | KIP-932 share-group offset deletion (group must be empty) |

> ¹ Requires the `unstable-protocol` feature flag. Where a max is shown in
> parentheses, that is the feature-gated ceiling.
>
> ² Requires the `telemetry` feature flag.
>
> The share-group *offset administration* APIs (keys 90–92) are **not** behind
> `unstable-protocol`: they are ordinary `AdminClient` operations and are
> compiled unconditionally. Only the `ShareConsumer` itself is gated.
>
> `StreamsGroupDescribe` (key 89) is likewise ungated — it is an ordinary
> `AdminClient` operation. Its sibling `StreamsGroupHeartbeat` (key 88) is
> **deliberately not implemented**: see below.

### KIP-1071: why only the describe half

KIP-1071 adds two APIs, and krafka implements exactly one of them.

`StreamsGroupHeartbeat` (key 88) is a Streams **runtime** API. Its request
carries the application topology — subtopologies, repartition topics, changelog
topics — and the coordinator assigns tasks from it. Sending it requires *being*
a Streams runtime. A client that fabricated a topology would not merely be
wrong locally: the topology is group-wide state, so it would corrupt what every
real member of that group is assigned from. krafka has no Streams layer, so it
does not send this.

`StreamsGroupDescribe` (key 89) is purely observational and is what an operator
actually needs — see
[Admin Client → Streams Groups](@/docs/admin.md).

### How this table stays honest

Two mechanisms, neither of which is "someone remembered".

**Within the crate**, the rows above are generated from the same `api_versions!`
macro tokens that initialise the `*_MIN` / `*_MAX` constants, so the published
table cannot understate or overstate what the client negotiates.

**Against Kafka**, `just protocol-parity` diffs the table against Apache Kafka's
own message schemas and fails CI on any of five conditions:

| Check | Catches |
|-------|---------|
| Name and key agree | A protocol rename applied to one place only — e.g. `ListClientMetricsResources` → `ListConfigResources` in Kafka 4.1 |
| MIN is still valid | A version Kafka removed in a major release, which every broker now rejects |
| MAX does not overstate | An ungated row naming a version Kafka marks `latestVersionUnstable` |
| MAX does not understate | A stable Kafka version this client silently declines to negotiate |
| Flexible boundary matches | An off-by-one in `ApiKey::flexible_version()`, which would make every request unparseable |

The check reads a vendored snapshot (`xtask/kafka_protocol_snapshot.json`), so
it needs no network and cannot flake. Track a newer Kafka release deliberately:

```sh
just refresh-protocol-snapshot 4.3   # rewrite the snapshot; review the diff
just protocol-parity                 # see what krafka must do about it
```

Deliberate omissions live in a `DELIBERATE_GAPS` table in the script, each with
a written reason — broker-internal APIs, the KIP-1071 Streams protocol, the
legacy `AlterConfigs` that `IncrementalAlterConfigs` supersedes.

#### Why some ceilings stop short

A schema marked `latestVersionUnstable: true` is **not advertised by a released
broker** unless it was started with `unstable.api.versions.enable=true`, so
implementing it buys nothing and costs a rejected round trip on every
connection.

In Kafka 4.3 exactly one API carries that flag: `InitProducerId` v6 (KIP-939
two-phase commit), which is why it sits behind `unstable-protocol`.
`ListOffsets` v11, `AddPartitionsToTxn` v5, `EndTxn` v5 and `DescribeQuorum` v2
set it explicitly to `false` — they are stable, and krafka negotiates all of
them.

### Version Constants

Client-supported versions are defined in `krafka::protocol::versions`:

```rust
use krafka::protocol::versions;

// Each API has both MIN and MAX constants
let min_fetch = versions::FETCH_MIN;        // 4  (Kafka 3.9+ baseline)
let max_fetch = versions::FETCH_MAX;        // 18 (v18 KIP-1166 high-watermark)
let min_produce = versions::PRODUCE_MIN;    // 3  (v3+ transactions)
let max_produce = versions::PRODUCE_MAX;    // 13 (v13 topic UUIDs, KIP-516)
let max_metadata = versions::METADATA_MAX;  // 13 (v13 top-level error_code)
```

## Record Batches

krafka uses Kafka's v2 record batch format with:

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
| Gzip | `gzip` via default `compression` | Good compression, slower |
| Snappy | `snappy` via default `compression` | Fast, moderate compression |
| LZ4 | `lz4` via default `compression` | Very fast, good compression |
| Zstd | `zstd` or `compression-all` | Best compression, fast; requires a C toolchain via `zstd-sys` |

> **Note:** Decompression output is capped at 128 MiB by default to protect against compression bombs. This limit is configurable via `ConsumerConfig::max_decompressed_size()`. Compressed payloads that expand beyond the limit will return a `KrafkaError::compression` error.

## Protocol Safety

krafka protects against malicious or corrupted broker responses:

- **Decode array bounds**: Every array-length field decoded from the wire is validated against `MAX_DECODE_ARRAY_LEN` (100,000), typically via `check_decode_array_len()` and in some specialized decode paths (e.g., `KafkaArray::decode`, record batch counts) via equivalent local checks. These checks reject negative counts and oversized counts across all 63+ protocol-message decode sites, `KafkaArray` decode paths, and record batch/header counts. The validation runs *before* any `Vec::with_capacity()` allocation, preventing both OOM and runaway decode loops.
- **Decompression limits**: Decompressed record data is limited to 128 MiB (configurable) via streaming `.take()` limits and post-decompression size checks
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

krafka uses `bytes::Bytes` throughout for zero-copy buffer management:

- Incoming data is parsed without copying
- Record payloads share underlying buffers
- Memory is released when last reference drops

## Next Steps

- [Producer Guide](@/docs/producer.md) - Sending messages
- [Consumer Guide](@/docs/consumer.md) - Receiving messages
- [Configuration Reference](@/docs/configuration.md) - All settings
