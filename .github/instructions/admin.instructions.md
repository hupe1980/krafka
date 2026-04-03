---
applyTo: "src/admin.rs"
description: "Use when editing admin client: API version constraints, result error handling, and destructive operation awareness."
---

# Admin Client Rules

## Result Error Handling

Admin operations return **per-resource results** (e.g., `Vec<CreateTopicResult>`), each with an `error: Option<String>`.
Callers must check individual results — a successful RPC does not mean every resource succeeded.

## API Versions

Admin requests use **automatic version negotiation** via `negotiate_api_version_max` on each broker connection.
The negotiated version is clamped to the client's maximum supported version for each API.
Multi-version encode/decode dispatch is implemented for: CreateTopics (v0–v2), DeleteTopics (v0–v1), FindCoordinator (v0–v1), DescribeGroups (v0–v1), ListGroups (v0–v1), OffsetForLeaderEpoch (v0–v2).
Single-version APIs (v0 only): CreatePartitions, DescribeConfigs, AlterConfigs, DescribeAcls, CreateAcls, DeleteAcls, DeleteRecords.
When adding a new version, update the version constant in `src/protocol/mod.rs::versions`, add the `encode_vN`/`decode_vN` methods in `src/protocol/messages.rs`, and add version dispatch in the admin method.

## Destructive Operations

- `alter_topic_config()` **replaces all dynamic configs** — always fetch-modify-update.
- `create_partitions()` can only **increase** count; never decreases.
- `delete_topics()` and `delete_acls()` are irreversible.

## Connection Strategy

Admin commands are sent to **any** broker (not necessarily the controller).
No built-in retry — callers must retry on retriable errors.
