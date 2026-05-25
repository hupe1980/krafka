---
applyTo: "src/admin/**"
description: "Use when editing admin client: API version constraints, result error handling, and destructive operation awareness."
---

# Admin Client Rules

## Result Error Handling

Admin operations return **per-resource results** (e.g., `Vec<CreateTopicResult>`), each with an `error: Option<String>`.
Callers must check individual results — a successful RPC does not mean every resource succeeded.

## API Versions

Admin requests use **automatic version negotiation** via `negotiate_api_version(api_key, max, min)` on each broker connection.
The negotiated version is clamped to the client's supported `[min, max]` range for each API.
When adding or updating an admin RPC, pass the API key plus the client-supported max and the minimum version required by that request shape/behavior rather than negotiating only against a max bound.
Multi-version encode/decode dispatch is implemented for: CreateTopics (v2–v7), DeleteTopics (v1–v6), FindCoordinator (v1–v6), DescribeGroups (v1–v6), ListGroups (v1–v5), OffsetForLeaderEpoch (v2–v4), DescribeAcls (v1–v3), CreateAcls (v1–v3), DeleteAcls (v1–v3), ConsumerGroupDescribe (v0–v1), CreatePartitions (v0–v3), DeleteRecords (v0–v2), DescribeLogDirs (v1–v4), DescribeTopicPartitions (v0), DeleteGroups (v0–v2), DescribeCluster (v0–v2), DescribeClientQuotas (v0–v1), AlterClientQuotas (v0–v1), CreateDelegationToken (v1–v3), RenewDelegationToken (v1–v2), ExpireDelegationToken (v1–v2), DescribeDelegationToken (v1–v3), DescribeConfigs (v0–v4), IncrementalAlterConfigs (v0–v1), UpdateFeatures (v0–v1), ElectLeaders (v0–v2), AlterPartitionReassignments (v0), ListPartitionReassignments (v0), AlterReplicaLogDirs (v1–v2), OffsetDelete (v0), DescribeUserScramCredentials (v0), AlterUserScramCredentials (v0), DescribeProducers (v0), DescribeTransactions (v0), ListTransactions (v0–v1), ListClientMetricsResources (v0), WriteTxnMarkers (v1), DescribeQuorum (v0).
When adding a new version, update the version constant in `src/protocol/mod.rs::versions`, add the `encode_vN`/`decode_vN` methods in `src/protocol/messages/`, and add version dispatch in the admin method.

## Destructive Operations

- `alter_topic_config()` uses IncrementalAlterConfigs to SET individual keys without replacing the entire config.
- `create_partitions()` can only **increase** count; never decreases.
- `delete_topics()` and `delete_acls()` are irreversible.
- `delete_offsets()` removes committed offsets for a consumer group (irreversible).
- `alter_user_scram_credentials()` can delete SCRAM credentials (breaks authentication).
- `alter_replica_log_dirs()` triggers inter-directory data movement on brokers.
- `write_txn_markers()` / `abort_transaction()` force-writes COMMIT/ABORT markers — can corrupt in-flight transactions if used incorrectly.

## Routing Patterns

- **Any broker**: Most admin commands (topics, configs, ACLs, features, etc.).
- **Fan-out to all brokers**: `describe_log_dirs`, `alter_replica_log_dirs`, `list_transactions`, `describe_producers` — each broker only knows its own data.
- **Coordinator routing**: `describe_consumer_groups` (group coordinator), `delete_offsets` (group coordinator), `describe_transactions` (txn coordinator) — uses FindCoordinator to route to the right broker.

## Connection Strategy

Admin commands are sent to **any** broker (not necessarily the controller).
No built-in retry — callers must retry on retriable errors.
