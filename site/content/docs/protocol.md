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
- All standard compression codecs, decoded in pure Rust
- Decoded records that share the fetched buffer

## Version Negotiation

On connection, krafka fetches the broker's supported API versions and picks,
per request, the highest version both sides support.

### How It Works

1. Client connects to broker
2. Client sends `ApiVersions` request
3. Broker responds with supported API version ranges
4. Client stores version ranges for future requests
5. Each request negotiates the best version within the client's `[MIN, MAX]` range

### Bootstrapping `ApiVersions` itself

krafka sends `ApiVersions` at its own ceiling (v4; v5 with the
`unstable-protocol` feature). A broker that answers `UNSUPPORTED_VERSION`
encodes the rejection in the v0 layout with its own range, and krafka retries
once at that ceiling.

v3 and later carry:

- **KIP-511** — `ClientSoftwareName` / `ClientSoftwareVersion`, which is how a
  broker's `client.software.name` / `client.software.version` metrics identify
  krafka. These fields do not exist below v3.
- **KIP-584** — `SupportedFeatures` and `FinalizedFeatures`, carried in v3+
  tagged fields. krafka caches them per connection and reads them itself — the
  transactional producer picks its transaction protocol from the finalized
  `transaction.version`. Applications read them with
  [`AdminClient::describe_features`](@/docs/admin.md#features-kip-584).

Negotiation is internal: the connection layer is not part of the public API.

### Leader epochs end to end (KIP-320)

KIP-320 only detects log truncation if the leader epoch travels with the
position everywhere it goes. krafka sends it on all three legs:

| Leg | Field | What it buys |
|---|---|---|
| `Fetch` | `current_leader_epoch`, `last_fetched_epoch` | The broker reports `diverging_epoch` when the client's log diverges from its own. |
| `ListOffsets` | `current_leader_epoch` | A reset resolved against a stale leader is rejected with `FENCED_LEADER_EPOCH` instead of returning an offset from a log the client cannot vouch for. |
| `OffsetCommit` → `OffsetFetch` | `committed_leader_epoch` | The check survives a restart or a rebalance: the next owner of the partition resumes with the epoch the position was read at. |

A fenced `ListOffsets` refreshes metadata before the retry.

The epoch is `-1` where the client has none: a position from a `seek()` or an
offset reset, or an offset set through
`AdminClient::alter_consumer_group_offsets`.

### Minimum Broker Version

krafka **requires Apache Kafka 3.9 or later**. Each API's minimum version is
one a Kafka 3.9 broker accepts (for example Metadata v1, Produce v3, Fetch v4),
so an older broker fails version negotiation for most APIs.

### Client Supported Versions

Every API has a minimum and a maximum supported version.
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
| GetTelemetrySubscriptions | 0 | 0 | KIP-714 client telemetry subscription discovery |
| PushTelemetry | 0 | 0 | KIP-714 client telemetry push |
| ShareGroupHeartbeat | 1 | 1 | KIP-932 share group heartbeat |
| ShareGroupDescribe | 1 | 1 | KIP-932 share group description |
| ShareFetch | 1 | 2 | KIP-932 share fetch, v2 acquire mode (KIP-1206) + renew ack (KIP-1222) |
| ShareAcknowledge | 1 | 2 | KIP-932 share acknowledge, v2 renew ack (KIP-1222) |
| DescribeShareGroupOffsets | 0 | 1 | KIP-932 share-partition start offsets, v1 Lag (KIP-1226) |
| AlterShareGroupOffsets | 0 | 0 | KIP-932 share-group offset reset (group must be empty) |
| StreamsGroupDescribe | 0 | 0 | KIP-1071 Streams group describe — topology, members, task assignments and changelog offsets |
| DeleteShareGroupOffsets | 0 | 0 | KIP-932 share-group offset deletion (group must be empty) |

> ¹ Requires the `unstable-protocol` feature flag — the version Kafka marks
> `latestVersionUnstable`. Where a max is shown in parentheses, that is the
> feature-gated ceiling.
>
> The share-group APIs are **not** behind `unstable-protocol` — Kafka marks none
> of them unstable, and krafka negotiates them on every build.
>
> `StreamsGroupDescribe` (key 89) is likewise ungated; see
> [Admin Client → Streams groups](@/docs/admin.md#streams-groups-kip-1071).
> `StreamsGroupHeartbeat` (key 88) is not implemented: its request carries the
> application topology, which only a Streams runtime can supply.

### How the table is checked

Two checks, both in CI.

**Against the crate**, `just site-check` compares every row above with the
`api_versions!` table in `src/protocol/mod.rs`, which defines the `MIN` and
`MAX` constants the client negotiates with: an implemented API missing from
this page, a different minimum, a ceiling the page does not mention, or a row
for an API the crate does not implement fails it.

**Against Kafka**, `just protocol-parity` (`xtask/protocol_parity.py`) diffs the
`api_versions!` table against Apache Kafka's own message schemas and fails on
any of six conditions:

| Check | Catches |
|-------|---------|
| 1. Name and key agree | An API missing from Kafka's schemas under that name, or a different API key |
| 2. MIN is still valid | A minimum below Kafka's floor: a version Kafka removed in a major release, which every broker rejects |
| 3. MAX does not overstate | An ungated row reaching a version Kafka marks `latestVersionUnstable` |
| 4. MAX does not understate | A stable Kafka version the client does not negotiate, unless the API is in `DELIBERATE_GAPS` |
| 5. The `unstable-protocol` gate is needed | A row gated on `unstable-protocol` whose ceiling Kafka already ships as stable |
| 6. Flexible boundary matches | An off-by-one in `ApiKey::flexible_version()`, which decides the header version and every compact field |

It also fails when Kafka has an API with neither a row in `api_versions!` nor
an entry in `DELIBERATE_GAPS`, and when Kafka has removed an API the client
still negotiates.

The check reads a vendored snapshot (`xtask/kafka_protocol_snapshot.json`) and
needs no network. To track a newer Kafka release:

```sh
just refresh-protocol-snapshot 4.3   # rewrite the snapshot; review the diff
just protocol-parity                 # see what krafka must do about it
```

Deliberate omissions live in a `DELIBERATE_GAPS` table in the script, each with
a written reason — broker-internal APIs, the KIP-1071 Streams protocol, the
legacy `AlterConfigs` that `IncrementalAlterConfigs` supersedes.

#### Unstable versions

A released broker advertises a version marked `latestVersionUnstable` only with
`unstable.api.versions.enable=true`. Such versions sit behind the
`unstable-protocol` feature: in Kafka 4.3 that is `InitProducerId` v6 (KIP-939
two-phase commit). `ApiVersions` v5 (KIP-1242), which no released Kafka
has, is gated the same way.

### KIP support

Every KIP this documentation names, generated from `xtask/kips.toml`. Each
status cites the evidence that holds it — an `api_versions!` row, a Cargo
feature or a public item — and `just claims-check` fails if the evidence does
not resolve, or if this table is edited by hand. Tracked against
<!-- generated:kips:kafka-ref -->Apache Kafka 4.3<!-- /generated -->.

<!-- generated:kips:kip-table -->
| KIP | Title | Status | Evidence | Note |
|-----|-------|--------|----------|------|
| KIP-219 | Improve quota communication | implemented | `Produce` v6+, `Fetch` v8+ |  |
| KIP-227 | Introduce incremental FetchRequests to increase partition scalability | implemented | `Fetch` v7+ |  |
| KIP-255 | OAuth authentication via SASL/OAUTHBEARER | implemented | `auth::OAuthBearerToken`, `AuthConfig::sasl_oauthbearer_token` |  |
| KIP-320 | Allow fetchers to detect and handle log truncation | implemented | `Fetch` v9+, `OffsetForLeaderEpoch` v2+, `ListOffsets` v4+, `OffsetCommit` v6+, `consumer::OffsetAndMetadata`, `admin::EpochEndOffset` |  |
| KIP-345 | Introduce static membership protocol to reduce consumer rebalances | implemented | `JoinGroup` v4+, `Heartbeat` v3+, `LeaveGroup` v3+ |  |
| KIP-360 | Improve reliability of idempotent/transactional producer | implemented | `InitProducerId` v3+, `producer::TransactionalProducer` |  |
| KIP-368 | Allow SASL connections to periodically re-authenticate | partial | `auth::AuthConfig` | The broker-reported session lifetime is honoured by replacing a pooled connection before it expires; in-band re-authentication of a live connection is not implemented. |
| KIP-373 | Allow users to create delegation tokens for other users | implemented | `CreateDelegationToken` v3+, `DescribeDelegationToken` v3+, `CreateDelegationTokenOptions::owner`, `admin::DelegationToken` |  |
| KIP-392 | Allow consumers to fetch from closest replica | implemented | `Fetch` v11+, `OffsetForLeaderEpoch` v3+ |  |
| KIP-405 | Kafka tiered storage | implemented | `ListOffsets` v8+, `OffsetSpec::EarliestLocal` | Tiered storage itself is broker-side; the client part is the local log-start offset spec and the OFFSET_MOVED_TO_TIERED_STORAGE error. |
| KIP-429 | Kafka consumer incremental rebalance protocol | implemented | `PartitionAssignmentStrategy::CooperativeSticky`, `consumer::ConsumerRebalanceListener` |  |
| KIP-447 | Producer scalability for exactly once semantics | implemented | `TxnOffsetCommit` v3+, `TransactionalProducer::send_offsets`, `consumer::ConsumerGroupMetadata` |  |
| KIP-455 | Create an administrative API for replica reassignment | implemented | `AlterPartitionReassignments` v0+, `ListPartitionReassignments` v0+, `admin::AlterPartitionReassignmentsOptions`, `admin::PartitionReassignment` |  |
| KIP-460 | Admin leader election RPC | implemented | `ElectLeaders` v1+, `admin::ElectionType` |  |
| KIP-464 | Defaults for AdminClient#createTopic | implemented | `CreateTopics` v4+, `admin::NewTopic` |  |
| KIP-511 | Collect and expose client's name and version in the brokers | implemented | `ApiVersions` v3+ |  |
| KIP-512 | Make record headers available in onAcknowledgement | implemented | `interceptor::ProducerInterceptor` |  |
| KIP-516 | Topic identifiers | implemented | `Metadata` v10+, `Fetch` v13+, `Produce` v13+, `DeleteTopics` v6+ |  |
| KIP-518 | Allow listing consumer groups per state | implemented | `ListGroups` v4+, `ListConsumerGroupsOptions::states` |  |
| KIP-525 | Return topic metadata and configs in CreateTopics response | partial | `CreateTopics` v5+ | CreateTopics v5+ is negotiated, but create_topics returns only per-topic success, so the partition count, replication factor and configs in the response are not surfaced. |
| KIP-554 | Add broker-side SCRAM config API | implemented | `DescribeUserScramCredentials` v0+, `AlterUserScramCredentials` v0+, `admin::ScramCredentialInfo`, `admin::ScramCredentialUpsertion` |  |
| KIP-559 | Make the Kafka protocol friendlier with L7 proxies | implemented | `JoinGroup` v7+, `SyncGroup` v5+ |  |
| KIP-584 | Versioning scheme for features | implemented | `ApiVersions` v3+, `UpdateFeatures` v1+, `admin::FeatureMetadata`, `admin::UpdateFeaturesOptions` |  |
| KIP-599 | Throttle create topic, create partition and delete topic operations | implemented | `CreateTopics` v6+, `CreatePartitions` v3+, `DeleteTopics` v5+ |  |
| KIP-664 | Provide tooling to detect and abort hanging transactions | implemented | `DescribeProducers` v0+, `DescribeTransactions` v0+, `ListTransactions` v0+, `WriteTxnMarkers` v1+, `admin::ProducerState`, `admin::TransactionDescription`, `admin::TransactionListing`, `admin::AbortTransactionOptions` |  |
| KIP-679 | Producer will enable the strongest delivery guarantee by default | implemented | `ProducerBuilder::idempotent` |  |
| KIP-699 | Update FindCoordinator to resolve multiple coordinators at a time | implemented | `FindCoordinator` v4+ |  |
| KIP-714 | Client metrics and observability | implemented | `GetTelemetrySubscriptions` v0+, `PushTelemetry` v0+, `ListConfigResources` v0+, `ProducerBuilder::metrics_push`, `Consumer::client_instance_id` |  |
| KIP-734 | Improve AdminClient.listOffsets to return timestamp and offset for the record with the largest timestamp | implemented | `ListOffsets` v7+, `OffsetSpec::MaxTimestamp` |  |
| KIP-768 | Extend SASL/OAUTHBEARER with support for OIDC | implemented | feature `oauth-oidc`, `auth::OidcTokenProvider`, `ClientCredentials::Secret` |  |
| KIP-794 | Strictly uniform sticky partitioner | partial | `ProducerBuilder::batch_size` | Keyless records stick to a partition for batch_size bytes and then switch at random, but the next partition is not weighted by per-broker queue size (partitioner.adaptive.partitioning.enable) and slow brokers are not avoided (partitioner.availability.timeout.ms). |
| KIP-800 | Add reason to JoinGroupRequest and LeaveGroupRequest | partial | `JoinGroup` v8+, `LeaveGroup` v5+ | JoinGroup v8 and LeaveGroup v5 are negotiated, but the reason field is always sent as null. |
| KIP-836 | Expose replication information of the cluster metadata | implemented | `DescribeQuorum` v1+, `admin::QuorumReplica` |  |
| KIP-848 | The next generation of the consumer rebalance protocol | implemented | `ConsumerGroupHeartbeat` v0+, `ConsumerGroupDescribe` v0+, `OffsetCommit` v9+, `GroupProtocol::Consumer`, `admin::ConsumerGroupDescription` |  |
| KIP-853 | KRaft controller membership changes | partial | `DescribeQuorum` v2+, `Fetch` v17+, `admin::QuorumNode` | Quorum membership can be described, but the AddRaftVoter and RemoveRaftVoter admin RPCs are not implemented. |
| KIP-890 | Transactions server-side defense | implemented | `Produce` v12+, `EndTxn` v5+, `InitProducerId` v5+, `AddOffsetsToTxn` v4+, `TxnOffsetCommit` v5+, `producer::TransactionVersion` |  |
| KIP-899 | Allow producer and consumer clients to rebootstrap | implemented | `krafka::MetadataRecoveryStrategy`, `Kafka::rebootstrap`, `KafkaBuilder::metadata_recovery_strategy` |  |
| KIP-903 | Replicas with stale broker epoch should not be allowed to join the ISR | implemented | `Fetch` v15+ | Broker-side change; the client only negotiates Fetch v15, which moves ReplicaId into ReplicaState. |
| KIP-932 | Queues for Kafka | partial | `ShareGroupHeartbeat` v1+, `ShareFetch` v1+, `ShareAcknowledge` v1+, `FindCoordinator` v6+, `DescribeShareGroupOffsets` v0+, `AlterShareGroupOffsets` v0+, `DeleteShareGroupOffsets` v0+, `share_consumer::ShareConsumer`, `admin::SharePartitionOffset` | The share consumer and share-group offset administration are implemented, but ShareGroupDescribe is never sent, so no admin call describes or lists share groups' members. |
| KIP-939 | Support participation in 2PC | implemented | `InitProducerId` v6+, feature `unstable-protocol`, `ProducerBuilder::two_phase_commit`, `TransactionalProducer::prepare`, `TransactionalProducer::complete`, `producer::PreparedTxnState` |  |
| KIP-951 | Leader discovery optimisations for the client | implemented | `Fetch` v16+, `Produce` v10+ |  |
| KIP-966 | Eligible leader replicas | implemented | `DescribeTopicPartitions` v0+, `admin::PartitionDescription` |  |
| KIP-994 | Minor enhancements to ListTransactions and DescribeTransactions APIs | implemented | `ListTransactions` v1+, `ListTransactionsOptions::min_duration` |  |
| KIP-1005 | Expose EarliestLocalOffset and TieredOffset | implemented | `ListOffsets` v9+, `OffsetSpec::LatestTiered` |  |
| KIP-1023 | Follower fetch from tiered offset | implemented | `ListOffsets` v11+, `OffsetSpec::EarliestPendingUpload` |  |
| KIP-1030 | Change constraints and default values for various configurations | implemented | `ProducerBuilder::linger` |  |
| KIP-1043 | Administration of groups | implemented | `DescribeGroups` v6+, `ListGroups` v5+, `admin::GroupType`, `ListConsumerGroupsOptions::types` |  |
| KIP-1066 | Mechanism to cordon brokers and log directories | implemented | `DescribeLogDirs` v5+, `admin::LogDirDescription` |  |
| KIP-1071 | Streams rebalance protocol | partial | `StreamsGroupDescribe` v0+, `admin::DescribedStreamsGroup`, `admin::DescribeStreamsGroupsOptions` | Streams groups can be described, but StreamsGroupHeartbeat is not implemented because its request carries an application topology that only a Streams runtime can supply. |
| KIP-1075 | Introduce delayed remote list offsets purgatory to make LIST_OFFSETS async | implemented | `ListOffsets` v10+ |  |
| KIP-1082 | Require client-generated IDs over the ConsumerGroupHeartbeat RPC | implemented | `ConsumerGroupHeartbeat` v1+ |  |
| KIP-1092 | Extend Consumer#close with an option to leave the group or not | implemented | `GroupMembershipOperation::RemainInGroup`, `CloseOptions::group_membership_operation` |  |
| KIP-1102 | Enable clients to rebootstrap based on timeout or error code | implemented | `Metadata` v13+, `KafkaBuilder::metadata_recovery_rebootstrap_trigger` |  |
| KIP-1106 | Add duration based offset reset option for consumer clients | implemented | `AutoOffsetReset::ByDuration` |  |
| KIP-1123 | Rack-aware partitioning for Kafka producer | implemented | `ProducerBuilder::partitioner_rack_aware` |  |
| KIP-1142 | Allow to list non-existent group which has dynamic config | implemented | `ListConfigResources` v1+, `admin::ListedConfigResource`, `admin::ListConfigResourcesOptions` |  |
| KIP-1152 | Add transactional ID pattern filter to ListTransactions API | implemented | `ListTransactions` v2+, `ListTransactionsOptions::transactional_id_pattern` |  |
| KIP-1160 | Enable returning supported features from a specific broker | implemented | `DescribeFeaturesOptions::node_id` |  |
| KIP-1166 | Improve high-watermark replication | implemented | `Fetch` v18+ | The new field is populated only by follower fetches; the client negotiates Fetch v18. |
| KIP-1206 | Strict max fetch records in share fetch | implemented | `ShareFetch` v2+, `AcquireMode::RecordLimit`, `ShareConsumerBuilder::acquire_mode` |  |
| KIP-1222 | Acquisition lock timeout renewal in share consumer explicit mode | implemented | `ShareFetch` v2+, `ShareAcknowledge` v2+, `ShareConsumer::renew`, `ShareConsumer::acquisition_lock_timeout` |  |
| KIP-1226 | Share partition lag persistence and retrieval | implemented | `DescribeShareGroupOffsets` v1+, `admin::SharePartitionOffset` |  |
| KIP-1242 | Detection and handling of misrouted connections | partial | `ApiVersions` v5+, feature `unstable-protocol` | ApiVersions v5 is encoded behind unstable-protocol, but the ClusterId and NodeId fields are never populated and REBOOTSTRAP_REQUIRED from ApiVersions does not trigger a rebootstrap. |
| KIP-1258 | Add support for OAuth client assertion to client_credentials grant type | implemented | feature `oauth-oidc`, `auth::AssertionSource`, `ClientCredentials::Assertion` |  |
| KIP-1274 | Deprecate and remove support for the classic rebalance protocol in KafkaConsumer | implemented | `GroupProtocol::Classic`, `GroupProtocol::Consumer` | As of Kafka 4.3 the client part is a deprecation warning when the classic protocol is used, which krafka emits once per process. |
| KIP-1288 | SSL certificate hot reload | implemented | `Kafka::refresh_tls`, `KafkaBuilder::tls_reload_interval` |  |
<!-- /generated -->

#### Not implemented

<!-- generated:kips:not-implemented -->
- **SASL/GSSAPI (Kerberos) authentication** (GSSAPI) — No mature pure-Rust GSSAPI implementation exists, and linking system Kerberos libraries would add a C dependency for every user.
- **Schema registry client** (schema-registry) — A schema registry is a separate service with its own protocol; krafka provides the serdes::Serializer and Deserializer hooks instead.
- **Async runtimes other than Tokio** (runtime-agnostic) — krafka is built on Tokio and does not abstract over the async runtime.
- **Kafka Streams runtime** (streams-runtime) — krafka is a client library with no stream-processing runtime, which is also why StreamsGroupHeartbeat is not implemented.
- **Broker-, controller- and KRaft-internal APIs** (broker-internal-apis) — APIs such as LeaderAndIsr, UpdateMetadata, Vote and the share-group state persister are spoken between brokers, not by clients.
- **KIP-368 Allow SASL connections to periodically re-authenticate**, partly — The broker-reported session lifetime is honoured by replacing a pooled connection before it expires; in-band re-authentication of a live connection is not implemented.
- **KIP-525 Return topic metadata and configs in CreateTopics response**, partly — CreateTopics v5+ is negotiated, but create_topics returns only per-topic success, so the partition count, replication factor and configs in the response are not surfaced.
- **KIP-794 Strictly uniform sticky partitioner**, partly — Keyless records stick to a partition for batch_size bytes and then switch at random, but the next partition is not weighted by per-broker queue size (partitioner.adaptive.partitioning.enable) and slow brokers are not avoided (partitioner.availability.timeout.ms).
- **KIP-800 Add reason to JoinGroupRequest and LeaveGroupRequest**, partly — JoinGroup v8 and LeaveGroup v5 are negotiated, but the reason field is always sent as null.
- **KIP-853 KRaft controller membership changes**, partly — Quorum membership can be described, but the AddRaftVoter and RemoveRaftVoter admin RPCs are not implemented.
- **KIP-932 Queues for Kafka**, partly — The share consumer and share-group offset administration are implemented, but ShareGroupDescribe is never sent, so no admin call describes or lists share groups' members.
- **KIP-1071 Streams rebalance protocol**, partly — Streams groups can be described, but StreamsGroupHeartbeat is not implemented because its request carries an application topology that only a Streams runtime can supply.
- **KIP-1242 Detection and handling of misrouted connections**, partly — ApiVersions v5 is encoded behind unstable-protocol, but the ClusterId and NodeId fields are never populated and REBOOTSTRAP_REQUIRED from ApiVersions does not trigger a rebootstrap.
- `AlterConfigs` is implemented below Kafka's ceiling — superseded by IncrementalAlterConfigs, which krafka uses instead; the legacy whole-config replace is not exposed
- `SaslHandshake` is implemented below Kafka's ceiling — pinned at v1 by the handshake path; v0 has no mechanism list
- `SaslAuthenticate` is implemented below Kafka's ceiling — pinned at v1: v2 only adds flexible encoding, and the pre-auth reader is deliberately version-pinned so an unauthenticated peer cannot steer it
- Not spoken by a client (broker-, controller- and KRaft-internal): `LeaderAndIsr`, `StopReplica`, `UpdateMetadata`, `ControlledShutdown`, `Vote`, `BeginQuorumEpoch`, `EndQuorumEpoch`, `AlterPartition`, `Envelope`, `FetchSnapshot`, `BrokerRegistration`, `BrokerHeartbeat`, `UnregisterBroker`, `AllocateProducerIds`, `ControllerRegistration`, `AssignReplicasToDirs`, `UpdateRaftVoter`, `InitializeShareGroupState`, `ReadShareGroupState`, `WriteShareGroupState`, `DeleteShareGroupState`, `ReadShareGroupStateSummary`
<!-- /generated -->

## Record Batches

krafka uses Kafka's v2 record batch format with:

- Magic byte 2
- CRC32C checksums, validated on decode
- Varint-encoded record fields
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

Every request and response type encodes and decodes each supported version
through one entry point that dispatches on the negotiated version number.
Unsupported version numbers (including negative values) return a descriptive
`KrafkaError::Protocol` error.

The protocol layer is internal: it is not part of krafka's public API and is
not covered by semver.

### Compression Support

| Codec | Feature | Notes |
|-------|---------|-------|
| None | Default | No compression |
| Gzip | Always on | Good compression, slower |
| Snappy | Always on | Fast, moderate compression. Encoded and decoded in snappy-java's stream format, as the Java client writes it; raw snappy also decodes |
| LZ4 | Always on | Very fast, good compression |
| Zstd | Decode always on; encode with `zstd` | High ratio, fast decode. Decoding is pure Rust (`ruzstd`); encoding compiles the C zstd library through `zstd-sys` |

> **Note:** Decompression output is capped at 128 MiB by default to protect against compression bombs. This limit is configurable with the consumer builder's `max_decompressed_size`. Compressed payloads that expand beyond the limit will return a `KrafkaError::compression` error.

## Protocol Safety

Limits on what a malicious or corrupted broker response can make the client do:

- **Decode array bounds**: Every array length decoded from the wire is checked against `MAX_DECODE_ARRAY_LEN` (100,000) before anything is allocated for it.
- **Record batch bounds**: A record batch has no record-count limit; it is bounded by its bytes. A declared count larger than the record bytes can hold (7 bytes per record at least) is rejected before anything is allocated for it, and decompressed output is bounded by the size limit below.
- **Decompression limits**: Decompressed record data is limited to 128 MiB (configurable) via streaming `.take()` limits and post-decompression size checks
- **Encode validation**: The `TryEncode` trait provides fallible encoding for protocol primitives (`KafkaString`, `KafkaBytes`, `KafkaArray<T>` where `T: TryEncode`, `TaggedFields`), returning an error on oversized data. `Record::validate()` checks wire-format limits at the API boundary before encoding
- **Fuzz testing**: The `fuzz/` directory provides [cargo-fuzz](https://rust-fuzz.github.io/book/cargo-fuzz.html) targets for the framing header and primitives, `KafkaArray` decode, `RecordBatch` decode, every response decoder at every version in the table above, request encode, the SCRAM exchange, the OIDC token client's HTTP response parser, and record-batch decompression under a small cap in every codec. `just fuzz-coverage` fails when a version in the table has no fuzz path. Every pull request runs each target for 60 seconds from committed seeds. See `fuzz/README.md` for usage.

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

## Buffers

- The reader reserves each response frame once, at the size its length prefix
  declares, and reads socket data straight into it: a 16 MiB frame is one
  allocation of 1.01× its size.
- A record batch's header is parsed first; the consumer skips aborted
  transactional batches from the header, without decompressing them.
- Decoded record keys, values and header values are slices of the response
  buffer (uncompressed) or of the decompressed buffer. Decoding 1000
  uncompressed 100-byte records makes one allocation (the record list),
  0.89× the wire size.
- A record kept alive keeps the buffer it came from alive. Copy the bytes you
  retain long-term if the rest of the response should be freed.

## Broker Compatibility

krafka negotiates every API version rather than pinning them, so it works
against any broker that speaks the Kafka wire protocol at the negotiated
range — Apache Kafka 3.9+, and Kafka-compatible systems whose advertised
versions overlap krafka's floors.

### Apache Kafka 3.9 → 4.3

Apache Kafka is the reference target: the version table above is diffed
against Kafka's own message schemas in CI (`just protocol-parity`), and the
Docker integration suite can be run against every supported minor in one
command:

```sh
just integration-matrix                  # Kafka 3.9.0 → 4.3.0
just integration-sasl-matrix             # SASL suite on Kafka 3.9.0 and 4.3.1
just integration-matrix "4.2.0 4.3.0"    # a subset
KAFKA_VERSION=4.3.0 just integration     # a single version
```

### Redpanda

Redpanda needs no configuration and no feature flag:

- **Version negotiation** lands inside Redpanda's advertised ranges for every
  API krafka's clients need.
- **Transactions fall back to TV1 automatically.** Redpanda does not
  implement server-side KIP-890 transaction version 2; krafka's
  `TransactionalProducer` probes the cluster's finalized
  `transaction.version` feature in `build_transactional` and uses the
  classic explicit-`AddPartitionsToTxn` protocol when the feature is absent,
  as the Java 4.x client does.
- **APIs Redpanda does not implement** (KIP-932 share groups, log-dir
  administration, `DescribeQuorum`) fail with `UnknownApiVersion`; see
  [A cluster without a feature](#a-cluster-without-a-feature).

The Redpanda suite covers a produce/consume round trip, the admin topic
lifecycle, the TV1 transaction fallback with `read_committed` visibility, and
closing a consumer without wedging a shared connection. CI gates every merge on
the Redpanda release pinned in `tests/redpanda/Dockerfile`; the same suite runs
locally against `latest`:

```sh
just integration-redpanda                           # the pinned release
REDPANDA_VERSION=latest just integration-redpanda   # the current release
```

### A cluster without a feature

A broker that lacks an API, or refuses a feature, fails the call with an error
whose message names the missing feature and, where one exists, the setting that
avoids it. The error keeps its kind (`UnknownApiVersion` when the API cannot be
negotiated) and the broker's error code.

| The cluster lacks | Fails in | Error | Avoid it with |
|---|---|---|---|
| KIP-848 consumer groups (`ConsumerGroupHeartbeat`) | `poll` / `recv` | `Protocol(UnknownApiVersion)` | `.group_protocol(GroupProtocol::Classic)` |
| Share groups (`ShareGroupHeartbeat`, `ShareFetch`, `ShareAcknowledge`) | share consumer `subscribe` / `poll` | `Protocol(UnknownApiVersion)` | none: share groups need Apache Kafka 4.2+ |
| Idempotent producers (`InitProducerId` missing or refused) | `ProducerBuilder::build`, then every pending `send` | `Protocol(UnknownApiVersion)`, or `Broker` with the broker's code | `.idempotent(false)` |
| The batch's compression codec (`UNSUPPORTED_COMPRESSION_TYPE`) | `send` | `Broker(UnsupportedCompressionType)`, naming the codec | `.compression(..)` / `.topic_compression(..)` |
| Transactions (`InitProducerId`, `EndTxn`, `AddPartitionsToTxn`, `AddOffsetsToTxn`) | `build_transactional`, or the first transactional send | `Protocol(UnknownApiVersion)` | none: use a non-transactional producer |

## Next Steps

- [Producer Guide](@/docs/producer.md) - Sending messages
- [Consumer Guide](@/docs/consumer.md) - Receiving messages
- [Configuration Reference](@/docs/configuration.md) - All settings
