+++
title = "Admin Client"
description = "Topics, partitions, configs, ACLs, quotas, consumer groups, delegation tokens and cluster features."
weight = 60

[extra]
slug_id = "admin"
+++

## Overview

The AdminClient provides cluster administration capabilities:

- Topic management (create, delete, describe, list)
- Consumer group management (describe, list, KIP-848 describe)
- Topic partition details (paginated describe with ELR)
- Record deletion (delete records before an offset)
- Leader epoch queries (detect log truncation)
- Cluster information
- Partition management
- ACL management
- Delegation token management (create, describe, renew, expire)
- Client quota management (describe, alter)
- Cluster feature versioning (describe, update — KIP-584)
- Log directory inspection (describe log dirs with volume capacity)
- Move replicas between log directories
- Delete consumer group committed offsets
- SCRAM credential management (describe, alter — KIP-554)
- Transaction debugging (describe producers, describe/list transactions — KIP-664)
- Client metrics resource discovery (KIP-714)

### API Version Negotiation

The AdminClient automatically negotiates the best API version for each RPC
using the broker's `ApiVersions` response. This ensures forward compatibility
with newer Kafka releases while gracefully falling back to older protocol
versions on legacy brokers. If a broker does not support a required API, the
client returns a clear protocol error.

## Basic Usage

```rust,compile
use krafka::admin::AdminClient;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let admin = AdminClient::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // List all topics
    let topics = admin.list_topics().await?;
    println!("Topics: {:?}", topics);

    Ok(())
}
```

## Authentication

The AdminClient supports all SASL authentication mechanisms:

### SASL/PLAIN

```rust
use krafka::admin::AdminClient;

let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    .sasl_plain("username", "password")
    .build()
    .await?;
```

### SASL/SCRAM-SHA-256

```rust,compile
let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;
```

### SASL/SCRAM-SHA-512

```rust,compile
let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    .sasl_scram_sha512("username", "password")
    .build()
    .await?;
```

### Generic AuthConfig

For AWS MSK IAM or advanced configurations:

```rust,compile
use krafka::admin::AdminClient;
use krafka::auth::AuthConfig;

let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let admin = AdminClient::builder()
    .bootstrap_servers("msk-broker:9098")
    .auth(auth)
    .build()
    .await?;
```

## Topic Management

### Creating Topics

```rust
use krafka::admin::{AdminClient, NewTopic};
use std::time::Duration;

let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    .build()
    .await?;

// Simple topic creation
let topic = NewTopic::new("my-topic", 6, 3);

let results = admin
    .create_topics(vec![topic], Duration::from_secs(30), false)
    .await?;

for result in results {
    match result.error {
        None => println!("Created: {}", result.name),
        Some(e) => println!("Failed to create {}: {}", result.name, e),
    }
}
```

### Creating Topics with Configuration

```rust
use krafka::admin::{AdminClient, NewTopic};
use std::time::Duration;

let topic = NewTopic::new("compacted-topic", 12, 3)
    .with_config("cleanup.policy", "compact")
    .with_config("min.insync.replicas", "2")
    .with_config("retention.ms", "604800000");  // 7 days

admin.create_topics(vec![topic], Duration::from_secs(30), false).await?;
```

### Deleting Topics

```rust,compile
use std::time::Duration;

let results = admin
    .delete_topics(
        vec!["topic-to-delete".to_string()],
        Duration::from_secs(30),
    )
    .await?;

for result in results {
    match result.error {
        None => println!("Deleted: {}", result.name),
        Some(e) => println!("Failed to delete {}: {}", result.name, e),
    }
}
```

### Listing Topics

```rust,compile
let topics = admin.list_topics().await?;
println!("Topics in cluster:");
for topic in topics {
    println!("  - {}", topic);
}
```

### Describing Topics

`describe_topics()` returns a `HashMap<String, TopicInfo>` keyed by topic name for O(1) look-ups:

```rust,compile
let descriptions = admin
    .describe_topics(&["topic1", "topic2"])
    .await?;

for (name, info) in &descriptions {
    println!("Topic: {}", name);
    println!("  Partitions: {}", info.partition_count());
    for partition in info.partitions_iter() {
        println!(
            "    Partition {}: leader={}, replicas={:?}, isr={:?}",
            partition.partition,
            partition.leader,
            partition.replicas,
            partition.isr
        );
    }
}

// Or look up a single topic
if let Some(info) = descriptions.get("topic1") {
    println!("topic1 has {} partitions", info.partition_count());
}
```

For a single-topic look-up, prefer the convenience shortcut:

```rust,compile
if let Some(info) = admin.describe_topic("my-topic").await? {
    println!("Partitions: {}", info.partition_count());
}
```

### Increasing Partition Count

You can increase the number of partitions for an existing topic (but never decrease):

```rust
use std::time::Duration;

let result = admin
    .create_partitions("my-topic", 12, Duration::from_secs(30))
    .await?;

match result.error {
    None => println!("Partitions increased to 12"),
    Some(e) => println!("Failed: {}", e),
}
```

## Configuration Management

### Describing Configuration

`ConfigEntry::config_value()` returns a `ConfigValue` enum that distinguishes
between an explicit value, a sensitive/redacted value, the broker default, and
an unavailable entry:

```rust
use krafka::admin::{DescribeConfigsRequest, ConfigValue};

let configs = admin.describe_configs(DescribeConfigsRequest::for_topic("my-topic")).await?;

println!("Topic configuration:");
for config in configs {
    let flags = format!(
        "{}{}{}",
        if config.read_only { "R" } else { "" },
        if config.is_default { "D" } else { "" },
        if config.is_sensitive { "S" } else { "" }
    );
    match config.config_value() {
        ConfigValue::Value(v) => println!("  {}: {} [{}]", config.name, v, flags),
        ConfigValue::Sensitive   => println!("  {}: <sensitive> [{}]", config.name, flags),
        ConfigValue::Default     => println!("  {}: <default> [{}]", config.name, flags),
        ConfigValue::Unavailable => println!("  {}: <unavailable> [{}]", config.name, flags),
    }
}
```

### Describing Broker Configuration

```rust
let configs = admin.describe_configs(DescribeConfigsRequest::for_broker(0)).await?;

println!("Broker 0 configuration:");
for config in configs.iter().filter(|c| !c.is_default) {
    if let Some(v) = config.config_value().as_str() {
        println!("  {}: {}", config.name, v);
    }
}
```

### Altering Topic Configuration

```rust,compile
use std::collections::HashMap;

let mut configs = HashMap::new();
configs.insert("retention.ms".to_string(), "86400000".to_string());  // 1 day
configs.insert("cleanup.policy".to_string(), "compact".to_string());

let result = admin.alter_topic_config("my-topic", configs).await?;

match result.error {
    None => println!("Configuration updated"),
    Some(e) => println!("Failed: {}", e),
}
```

## Cluster Information

### Describing the Cluster

```rust,compile
let cluster = admin.describe_cluster().await?;

println!("Cluster info:");
println!("  Cluster ID: {}", cluster.cluster_id);
println!("  Controller: {}", cluster.controller_id);
println!("  Brokers:");
for broker in cluster.brokers {
    println!(
        "    - {} at {}:{} (rack: {:?})",
        broker.broker_id, broker.host, broker.port, broker.rack
    );
}
```

### Getting Partition Count

```rust,compile
if let Some(count) = admin.partition_count("my-topic").await? {
    println!("Topic has {} partitions", count);
} else {
    println!("Topic not found");
}
```

## Offsets

### Listing partition offsets

`list_offsets` takes an [`OffsetSpec`] naming *which* offset you want. Five
exist, and three of them answer questions `Latest` cannot:

| Spec | Wire | Needs | Answers |
|---|---|---|---|
| `Earliest` | `-2` | v1 | Log start — the oldest offset still retained anywhere |
| `Latest` | `-1` | v1 | High watermark — where the next record will land |
| `Timestamp(ms)` | `ms` | v1 | First offset at or after a wall-clock time |
| `MaxTimestamp` | `-3` | v7 | Offset of the record with the **largest timestamp** (KIP-734) |
| `EarliestLocal` | `-4` | v8 | Where **local** storage begins; everything below is remote (KIP-405) |
| `LatestTiered` | `-5` | v9 | The tiering frontier — the last offset copied to remote storage (KIP-1005) |

```rust,compile
use krafka::admin::OffsetSpec;

// Is a scan from the log start going to pull from object storage?
let local = admin
    .list_offsets(&[("events", &[0][..])], OffsetSpec::EarliestLocal)
    .await?;
let earliest = admin
    .list_offsets(&[("events", &[0][..])], OffsetSpec::Earliest)
    .await?;

for (local, earliest) in local.iter().zip(&earliest) {
    let remote_records = local.offset - earliest.offset;
    if remote_records > 0 {
        println!(
            "partition {}: {remote_records} records live in remote storage",
            local.partition
        );
    }
}
```

`MaxTimestamp` is not `Latest`. They diverge whenever producers write out of
order — which is any topic whose `CreateTime` timestamps come from application
clocks, or any topic fed by more than one producer. `Latest` tells you where the
log ends; `MaxTimestamp` tells you when it was last genuinely written to, which
is the question a staleness alert is really asking.

**Version enforcement.** The three newer specs are negative timestamps on the
wire. A broker too old to know one does not reject it — it answers as though you
had asked for the first offset at or after a negative timestamp, i.e. the log
start. krafka checks the negotiated version *before* sending and fails with a
message naming the version required, rather than handing back a plausible wrong
number.

### Reading a group's committed offsets

`describe_consumer_group_offsets` takes an [`OffsetVisibility`], because a group
fed by a transactional producer can have an offset that is *written but not yet
committed*:

```rust,compile
use krafka::admin::OffsetVisibility;

// A dashboard wants the freshest number, and can tolerate it moving backwards
// if a transaction aborts.
let live = admin
    .describe_consumer_group_offsets("my-group", None, OffsetVisibility::IncludeUnstable)
    .await?;

// A tool that *acts* on the value must not read an offset an abort can retract.
let settled = admin
    .describe_consumer_group_offsets("my-group", None, OffsetVisibility::StableOnly)
    .await?;
```

With `StableOnly` the broker reports `UNSTABLE_OFFSET_COMMIT` for any partition
staged inside an unresolved transaction, and krafka surfaces that as an error
rather than omitting the partition — an omitted partition is indistinguishable
from one the group never committed to, and that difference is the one that
matters.

On a group with no transactional producer the two are identical, because nothing
can stage an offset.

[`OffsetSpec`]: https://docs.rs/krafka/latest/krafka/admin/enum.OffsetSpec.html
[`OffsetVisibility`]: https://docs.rs/krafka/latest/krafka/admin/enum.OffsetVisibility.html

## Error Handling

```rust
use krafka::admin::{AdminClient, NewTopic};
use krafka::error::KrafkaError;
use std::time::Duration;

async fn create_topic_if_not_exists(
    admin: &AdminClient,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) -> Result<(), KrafkaError> {
    // Check if topic exists
    let topics = admin.list_topics().await?;
    if topics.contains(&name.to_string()) {
        println!("Topic {} already exists", name);
        return Ok(());
    }

    // Create the topic
    let topic = NewTopic::new(name, partitions, replication_factor);
    let results = admin
        .create_topics(vec![topic], Duration::from_secs(30), false)
        .await?;

    for result in results {
        if let Some(error) = result.error {
            return Err(KrafkaError::broker(
                krafka::error::ErrorCode::UnknownServerError,
                error,
            ));
        }
    }

    println!("Created topic: {}", name);
    Ok(())
}
```

## Common Topic Configurations

| Configuration | Type | Default | Description |
|--------------|------|---------|-------------|
| `cleanup.policy` | String | `delete` | `delete` or `compact` |
| `compression.type` | String | `producer` | Compression type |
| `retention.ms` | Long | `-1` | Message retention time (-1 = infinite) |
| `retention.bytes` | Long | `-1` | Max partition size (-1 = infinite) |
| `segment.bytes` | Int | 1GB | Segment file size |
| `min.insync.replicas` | Int | `1` | Min ISR for writes with acks=all |
| `max.message.bytes` | Int | 1MB | Max message size |
| `unclean.leader.election.enable` | Bool | `false` | Allow unclean leader election |

## Best Practices

### Always Check Results

```rust
let results = admin.create_topics(topics, timeout, false).await?;

let mut success = true;
for result in results {
    if let Some(error) = &result.error {
        eprintln!("Failed to create {}: {}", result.name, error);
        success = false;
    }
}

if !success {
    return Err(KrafkaError::invalid_state("Some topics failed to create"));
}
```

### Use Appropriate Timeouts

```rust
use std::time::Duration;

// Short timeout for simple operations
admin.list_topics().await?;  // Uses default timeout

// Longer timeout for operations that may take time
admin.create_topics(topics, Duration::from_secs(60), false).await?;
admin.delete_topics(topics, Duration::from_secs(60)).await?;
```

### Handle Topic Already Exists

```rust
let results = admin.create_topics(vec![topic], timeout, false).await?;

for result in results {
    match &result.error {
        None => println!("Created: {}", result.name),
        Some(e) if e.contains("TOPIC_ALREADY_EXISTS") => {
            println!("Topic {} already exists (OK)", result.name);
        }
        Some(e) => {
            return Err(KrafkaError::broker(
                krafka::error::ErrorCode::UnknownServerError,
                e.clone(),
            ));
        }
    }
}
```

## ACL Management

The AdminClient supports Access Control List (ACL) management for Kafka security.

### Using AclFilter (Recommended)

The `AclFilter` struct provides a cleaner API for ACL queries:

```rust
use krafka::admin::AclFilter;
use krafka::protocol::AclResourceType;

// Filter that matches all ACLs
let all_acls = admin.describe_acls_with_filter(AclFilter::all()).await?;

// Filter for a specific topic
let topic_acls = admin.describe_acls_with_filter(
    AclFilter::for_resource(AclResourceType::Topic, "my-topic")
).await?;

// Filter for a specific principal
let user_acls = admin.describe_acls_with_filter(
    AclFilter::for_principal("User:alice")
).await?;

// Builder pattern for complex filters
let filter = AclFilter::all()
    .resource_type(AclResourceType::Group)
    .resource_name("my-consumer-group")
    .principal("User:bob");

let result = admin.describe_acls_with_filter(filter).await?;
```

### Describe ACLs

Query existing ACLs matching a filter:

```rust
use krafka::protocol::{AclResourceType, AclPatternType, AclOperation, AclPermissionType};

// Find all ACLs for a specific topic
let result = admin.describe_acls(
    AclResourceType::Topic,
    Some("my-topic"),
    AclPatternType::Literal,
    None,  // any principal
    None,  // any host
    AclOperation::Any,
    AclPermissionType::Any,
).await?;

if let Some(error) = result.error {
    println!("Error: {}", error);
} else {
    for binding in result.bindings {
        println!("ACL: {:?} {} {:?} on {}", 
            binding.permission_type, 
            binding.principal, 
            binding.operation,
            binding.resource_name);
    }
}
```

### Create ACLs

Create new access control entries:

```rust,compile
use krafka::protocol::AclBinding;

// Create a simple read ACL
let read_acl = AclBinding::allow_read_topic("my-topic", "User:alice");

// Create a write ACL
let write_acl = AclBinding::allow_write_topic("my-topic", "User:bob");

// Create ACLs
let result = admin.create_acls(vec![read_acl, write_acl]).await?;

for (i, r) in result.results.iter().enumerate() {
    match &r.error {
        None => println!("ACL {} created successfully", i),
        Some(e) => println!("ACL {} failed: {}", i, e),
    }
}
```

### Delete ACLs

Delete ACLs matching a filter:

```rust,compile
use krafka::protocol::{AclBindingFilter, AclResourceType, AclPatternType, AclOperation, AclPermissionType};

// Delete all ACLs for a topic
let filter = AclBindingFilter {
    resource_type: AclResourceType::Topic,
    resource_name: Some("my-topic".to_string()),
    pattern_type: AclPatternType::Literal,
    principal: None,
    host: None,
    operation: AclOperation::Any,
    permission_type: AclPermissionType::Any,
};

let result = admin.delete_acls(vec![filter]).await?;

for (i, fr) in result.filter_results.iter().enumerate() {
    match &fr.error {
        None => println!("Filter {} deleted {} ACLs", i, fr.deleted_count),
        Some(e) => println!("Filter {} failed: {}", i, e),
    }
}
```

## Consumer Group Management

### Describing Consumer Groups

Get detailed information about one or more consumer groups. The method
automatically detects each group's type (classic or KIP-848 consumer protocol)
and dispatches to the appropriate API (Key 15 or Key 69). The request is routed
to each group's coordinator broker via FindCoordinator:

```rust,compile
let descriptions = admin
    .describe_consumer_groups(vec!["my-group".to_string(), "other-group".to_string()])
    .await?;

for group in &descriptions {
    println!("Group: {} (type: {}, state: {})", group.group_id, group.group_type, group.state);
    if let Some(assignor) = &group.assignor {
        println!("  Assignor: {}", assignor);
    }
    if let Some(epoch) = group.group_epoch {
        println!("  Epoch: {}", epoch);
    }
    for member in &group.members {
        println!(
            "    Member: {} (client: {}, host: {}, instance: {:?})",
            member.member_id, member.client_id, member.client_host,
            member.instance_id
        );
    }
    if let Some(error) = &group.error {
        println!("  Error: {}", error);
    }
}
```

> **Note:** Classic-protocol groups return `protocol_type` and `assignor` but
> no epoch or assignment details. KIP-848 groups return `group_epoch`,
> `assignment_epoch`, per-member subscriptions, and topic-UUID-based
> current/target assignments.

### Listing Consumer Groups

```rust,compile
use krafka::admin::GroupListing;

let groups = admin.list_consumer_groups(&GroupListing::all()).await?;

println!("Consumer groups:");
for group in &groups {
    println!("  {} (type: {:?}, protocol: {})", group.group_id, group.group_type, group.protocol_type);
}
```

**Filter on the broker, not in your loop.** A cluster can hold tens of
thousands of consumer groups, and listing all of them to keep the three that
are `Empty` transfers the entire group registry on every call:

```rust,compile
use krafka::admin::GroupListing;

// Candidates for cleanup, without pulling the rest.
let empty = admin
    .list_consumer_groups(&GroupListing::all().in_states(["Empty"]))
    .await?;

// Only groups on the KIP-848 protocol.
let modern = admin
    .list_consumer_groups(&GroupListing::all().of_types(["consumer"]))
    .await?;
```

State names are Kafka's own — `PreparingRebalance`, `CompletingRebalance`,
`Stable`, `Dead`, `Empty` — and types are `classic`, `consumer`, `share`,
`streams`. Both are passed through verbatim, so a value a future broker adds
needs no krafka release.

`states_filter` needs `ListGroups` v4 (KIP-518) and `types_filter` v5
(KIP-848). An older broker ignores the filter and returns more than asked for
rather than failing, so treat the result as a superset when broker versions are
mixed.

> **Note:** `list_consumer_groups()` queries all brokers in the cluster and deduplicates results, since consumer groups are managed by their respective group coordinators.

## Topic Partition Details

### Describing Topic Partitions

Use `describe_topic_partitions()` for paginated, detailed partition information
including ELR (eligible leader replicas) from KIP-966:

```rust,compile
let result = admin
    .describe_topic_partitions(vec!["my-topic".to_string()])
    .await?;

for topic in &result.topics {
    println!(
        "Topic: {} (internal: {}, id: {:?})",
        topic.name.as_deref().unwrap_or("?"),
        topic.is_internal,
        topic.topic_id
    );
    for p in &topic.partitions {
        println!(
            "  Partition {}: leader={}, epoch={}, replicas={:?}, isr={:?}",
            p.partition_index, p.leader_id, p.leader_epoch,
            p.replica_nodes, p.isr_nodes
        );
        if let Some(elr) = &p.eligible_leader_replicas {
            println!("    ELR: {:?}", elr);
        }
        if let Some(last_elr) = &p.last_known_elr {
            println!("    Last known ELR: {:?}", last_elr);
        }
        if !p.offline_replicas.is_empty() {
            println!("    Offline: {:?}", p.offline_replicas);
        }
    }
}
```

> **Note:** The DescribeTopicPartitions API (Key 75) is available on Kafka 4.0+.
> It automatically handles pagination for topics with many partitions (default
> limit 2000 partitions per page). All pages are collected into a single result.

## Record Deletion

### Deleting Records

Delete records from topic partitions before a specified offset. Records with offsets less than the specified offset are marked for deletion (this adjusts the log start offset). Requests are automatically routed to each partition's leader broker:

```rust,compile
use std::collections::HashMap;
use std::time::Duration;

let mut offsets = HashMap::new();
offsets.insert(("my-topic".to_string(), 0), 100i64);  // Delete before offset 100
offsets.insert(("my-topic".to_string(), 1), 250i64);  // Delete before offset 250

let results = admin
    .delete_records(offsets, Duration::from_secs(30))
    .await?;

for result in &results {
    match &result.error {
        None => println!(
            "Deleted records from {}:{}, new low watermark: {}",
            result.topic, result.partition, result.low_watermark
        ),
        Some(e) => println!(
            "Failed to delete from {}:{}: {}",
            result.topic, result.partition, e
        ),
    }
}
```

> **Note:** Deleted records are not immediately removed from disk. The broker adjusts the log start offset, and records before that offset become inaccessible. Physical deletion happens during log segment cleanup.

## Leader Epoch Queries

### OffsetForLeaderEpoch

Query the end offset for a given leader epoch. This is used to detect log truncation after leader changes. Requests are routed to each partition's leader broker:

```rust,compile
// Query the end offset for leader epoch 5 on partition 0 of "my-topic"
let results = admin
    .offset_for_leader_epoch(vec![
        ("my-topic".to_string(), 0, 5),
        ("my-topic".to_string(), 1, 3),
    ])
    .await?;

for result in &results {
    match &result.error {
        None => println!(
            "{}:{} epoch={} end_offset={}",
            result.topic, result.partition,
            result.leader_epoch, result.end_offset
        ),
        Some(e) => println!(
            "{}:{} error: {}",
            result.topic, result.partition, e
        ),
    }
}
```

This API is useful for:
- **Log truncation detection**: After a leader change, check if the log was truncated
- **Consumer offset validation**: Ensure a consumer's saved offset is still valid
- **Replication diagnostics**: Verify epoch boundaries across replicas

## Delegation Tokens

Delegation tokens (KIP-48) allow a principal to delegate authentication to
another principal without sharing credentials. The token HMAC can be used for
SASL/SCRAM authentication.

### Creating a Token

```rust,compile
use std::time::Duration;

// A token for the authenticated caller that "alice" can renew, 24-hour lifetime.
let result = admin
    .create_delegation_token(
        None,
        &[("User", "alice")],
        Some(Duration::from_secs(86_400)),
    )
    .await?;

match result.token {
    Some(token) => println!("Created token: {} (HMAC {} bytes)", token.token_id, token.hmac.len()),
    None => println!("Error: {}", result.error.unwrap()),
}
```

### Tokens on behalf of another principal (KIP-373)

Pass an `owner` to issue a token *for* someone else — how a superuser
provisions a token for a service account that never authenticates
interactively. It needs `CreateDelegationToken` v3+ and `CreateTokens`
authorisation on that principal.

```rust,compile
use std::time::Duration;

let result = admin
    .create_delegation_token(
        Some(("User", "svc-ingest")),          // who the token authenticates as
        &[("User", "platform-admin")],         // who may renew it
        Some(Duration::from_secs(86_400)),
    )
    .await?;

if let Some(token) = result.token {
    // The owner is who the token authenticates as; the requester is who asked
    // for it. That distinction is what an audit trail needs, and it is only
    // present on v3+.
    println!(
        "owner {}:{}, requested by {:?}",
        token.principal_type,
        token.principal_name,
        token.token_requester_principal_name,
    );
}
```

Pass an empty renewers slice to allow only the token owner to renew.
Use `None` for `max_lifetime` to accept the server default (typically 7 days).

### Describing Tokens

```rust,compile
// Describe all tokens visible to the caller
let tokens = admin.describe_delegation_token(None).await?;
for token in &tokens {
    println!(
        "Token {} owned by {}:{}, expires at {}, {} renewer(s)",
        token.token_id,
        token.principal_type,
        token.principal_name,
        token.expiry_timestamp_ms,
        token.renewers.len(),
    );
}

// Describe tokens for a specific owner
let tokens = admin
    .describe_delegation_token(Some(&[("User", "alice")]))
    .await?;
```

### Renewing a Token

```rust,compile
use std::time::Duration;

// Obtain a token (e.g., from a prior create call)
let result = admin
    .create_delegation_token(None, &[("User", "alice")], Some(Duration::from_secs(86_400)))
    .await?;
let token = result.token.expect("token created");

// Extend the token's lifetime by 1 hour
let result = admin
    .renew_delegation_token(&token.hmac, Duration::from_secs(3_600))
    .await?;

match result.error {
    None => println!("New expiry: {}", result.expiry_timestamp_ms),
    Some(e) => println!("Renew failed: {}", e),
}
```

### Expiring a Token

```rust,compile
use std::time::Duration;

// Obtain a token (e.g., from describe)
let tokens = admin.describe_delegation_token(None).await?;
let token = &tokens[0];

// Expire a token immediately
let result = admin.expire_delegation_token(&token.hmac, None).await?;

// Expire a token after a grace period
let result = admin
    .expire_delegation_token(&token.hmac, Some(Duration::from_secs(60)))
    .await?;
```

### Protocol Versions

| API | Versions | Changes |
|-----|----------|---------|
| CreateDelegationToken | v1–v3 | v1 baseline (v0 removed in Kafka 4.0), v2 flexible encoding, v3 owner principal override |
| RenewDelegationToken | v1–v2 | v1 baseline, v2 flexible encoding |
| ExpireDelegationToken | v1–v2 | v1 baseline, v2 flexible encoding |
| DescribeDelegationToken | v1–v3 | v1 baseline, v2 flexible encoding, v3 token requester fields |

## Client Quotas

Client quotas control the resource usage of clients (producer/consumer byte
rates, request percentages, etc.). Use `describe_client_quotas` to query
current quotas and `alter_client_quotas` to change them.

### Describing Quotas

```rust,compile
// Describe all quotas for user "alice" (match_type 0 = exact match)
let result = admin
    .describe_client_quotas(&[("user", 0, Some("alice"))], false)
    .await?;

for entry in &result.entries {
    let entity: Vec<_> = entry.entity.iter().map(|e| {
        format!("{}={}", e.entity_type, e.entity_name.as_deref().unwrap_or("<default>"))
    }).collect();
    println!("Entity: {}", entity.join(", "));
    for v in &entry.values {
        println!("  {} = {}", v.key, v.value);
    }
}
```

Filter match types:
- `0` — exact: match the entity with the given name
- `1` — default: match the default entity for this type
- `2` — any specified: match any entity with a name (non-default)

When `strict` is `true`, only entities that exactly match all given component
types are returned (entities with additional unspecified types are excluded).

### Altering Quotas

```rust,compile
use krafka::admin::QuotaAlteration;

// Set producer byte rate for user "alice"
let results = admin
    .alter_client_quotas(
        &[QuotaAlteration {
            entity: vec![("user", Some("alice"))],
            ops: vec![
                ("producer_byte_rate", Some(1_048_576.0)),  // set to 1 MiB/s
                ("consumer_byte_rate", None),               // remove quota
            ],
        }],
        false,
    )
    .await?;

for result in &results {
    match &result.error {
        None => println!("Quota altered successfully"),
        Some(e) => println!("Error: {}", e),
    }
}

// Dry-run validation (validate_only = true)
let results = admin
    .alter_client_quotas(
        &[QuotaAlteration {
            entity: vec![("user", Some("alice"))],
            ops: vec![("producer_byte_rate", Some(1_048_576.0))],
        }],
        true,
    )
    .await?;
```

## Feature Versioning (KIP-584)

Kafka 2.7+ supports cluster-wide feature flags that control the finalized
version range for features like `metadata.version`. Use `describe_features` to
discover what the cluster supports and `update_features` to upgrade, downgrade,
or delete finalized feature levels.

### Describing Features

```rust,compile
let features = admin.describe_features().await?;
println!("Epoch: {}", features.finalized_features_epoch);
for f in &features.supported_features {
    println!("supported: {} [{}, {}]", f.name, f.min_version, f.max_version);
}
for f in &features.finalized_features {
    println!("finalized: {} [{}, {}]", f.name, f.min_version_level, f.max_version_level);
}
```

### Updating Features

```rust
use krafka::protocol::messages::FeatureUpdateKey;

// Upgrade metadata.version to level 17
let results = admin
    .update_features(
        vec![FeatureUpdateKey::upgrade("metadata.version", 17)],
        false, // validate_only
    )
    .await?;

for r in &results.results {
    match &r.error {
        None => println!("{}: ok", r.feature),
        Some(e) => println!("{}: {}", r.feature, e),
    }
}

// Dry-run validation (validate_only = true, requires v1+)
let results = admin
    .update_features(
        vec![FeatureUpdateKey::upgrade("metadata.version", 17)],
        true,
    )
    .await?;
```

Upgrade types:
- `FeatureUpdateKey::upgrade(name, level)` — raise to a higher level
- `FeatureUpdateKey::safe_downgrade(name, level)` — lower the level safely
- `FeatureUpdateKey::unsafe_downgrade(name, level)` — forceful downgrade (may lose data)
- `FeatureUpdateKey::delete(name)` — remove the finalized feature entirely

When the broker supports `UpdateFeatures` v1+, the request uses the typed
`UpgradeType` field. On older v0 brokers, the client falls back to the boolean
`AllowDowngrade` flag.

`validate_only` is the exception to that graceful fallback, deliberately.
`UpdateFeatures` v0 has no `ValidateOnly` field, so sending the request anyway
would **apply** the change the caller asked to only simulate — and a
`metadata.version` downgrade is data-lossy. Against a v0 broker the call is
therefore refused *before the request is sent*, with an error naming the
required version. Nothing reaches the controller.

`update_features` is controller-only. The client resolves the controller from
cluster metadata, sends there directly, and re-resolves on `NOT_CONTROLLER`
rather than retrying against the same broker.

## Log Directory Inspection

`describe_log_dirs()` queries every broker and returns per-directory information
including partition sizes, offset lag, future-replica status, and (v4+) volume
capacity.

### Describe All Log Directories

```rust,compile
let dirs = admin.describe_log_dirs(None).await?;
for dir in &dirs {
    println!("broker {} — {} (total: {}, usable: {})",
        dir.broker_id, dir.log_dir, dir.total_bytes, dir.usable_bytes);
    if let Some(err) = &dir.error {
        eprintln!("  error: {err}");
    }
    for topic in &dir.topics {
        for p in &topic.partitions {
            println!("  {}-{}: {} bytes, lag {}{}",
                topic.name, p.partition_index, p.partition_size,
                p.offset_lag, if p.is_future_key { " (future)" } else { "" });
        }
    }
}
```

### Describe Specific Topics

```rust,compile
use krafka::protocol::DescribableLogDirTopic;

let filter = vec![DescribableLogDirTopic {
    topic: "my-topic".into(),
    partitions: vec![0, 1, 2],
}];
let dirs = admin.describe_log_dirs(Some(filter)).await?;
```

### Result Fields

| Field | Type | Description |
|-------|------|-------------|
| `broker_id` | `i32` | Broker that owns the directory |
| `log_dir` | `String` | Absolute path on the broker |
| `error` | `Option<String>` | Per-directory error (e.g., `KAFKA_STORAGE_ERROR`) |
| `total_bytes` | `i64` | Volume total bytes (-1 if unknown, v4+) |
| `usable_bytes` | `i64` | Volume free bytes (-1 if unknown, v4+) |
| `topics[].partitions[].partition_size` | `i64` | Log size in bytes |
| `topics[].partitions[].offset_lag` | `i64` | Lag behind high watermark |
| `topics[].partitions[].is_future_key` | `bool` | Future replica (reassignment) |

### Protocol Versions

| Version | Changes |
|---------|--------|
| v1 | Baseline (v0 removed in Kafka 4.0) |
| v2 | Flexible encoding (compact strings + tagged fields) |
| v3 | Top-level `ErrorCode` in response |
| v4 | `TotalBytes` + `UsableBytes` per log directory |

## Leader Election

`elect_leaders()` triggers a leader election for the specified partitions.
Supports preferred election (elect the preferred replica) and unclean election
(elect the first live replica even without in-sync replicas).

### Preferred Election for All Partitions

```rust,compile
use krafka::protocol::ElectionType;

let results = admin
    .elect_leaders(ElectionType::Preferred, None, Duration::from_secs(60))
    .await?;
for topic in &results {
    for p in &topic.partitions {
        if let Some(err) = &p.error {
            eprintln!("{}-{}: {err}", topic.topic, p.partition_id);
        }
    }
}
```

### Unclean Election for Specific Partitions

```rust,compile
use krafka::protocol::{ElectionType, ElectLeadersTopicPartitions};

let results = admin
    .elect_leaders(
        ElectionType::Unclean,
        Some(vec![ElectLeadersTopicPartitions {
            topic: "my-topic".into(),
            partitions: vec![0, 1],
        }]),
        Duration::from_secs(60),
    )
    .await?;
```

### Protocol Versions

| Version | Changes |
|---------|--------|
| v0 | Baseline (preferred election only) |
| v1 | Adds `ElectionType` for preferred/unclean (KIP-460); top-level error code |
| v2 | Flexible encoding (compact strings + tagged fields) |

## Partition Reassignment

`alter_partition_reassignments()` initiates or cancels partition reassignments.
`list_partition_reassignments()` lists all ongoing reassignments.

> **Warning**: Reassigning partitions moves data between brokers and can
> significantly impact cluster load.

### Start a Reassignment

```rust,compile
use krafka::protocol::{ReassignableTopic, ReassignablePartition};

let result = admin.alter_partition_reassignments(
    vec![ReassignableTopic {
        name: "my-topic".into(),
        partitions: vec![ReassignablePartition {
            partition_index: 0,
            replicas: Some(vec![1, 2, 3]),
        }],
    }],
    Duration::from_secs(60),
).await?;

if let Some(err) = &result.error {
    eprintln!("Top-level error: {err}");
}
for topic in &result.topics {
    for p in &topic.partitions {
        if let Some(err) = &p.error {
            eprintln!("{}-{}: {err}", topic.name, p.partition_index);
        }
    }
}
```

### Cancel a Pending Reassignment

```rust,compile
use krafka::protocol::{ReassignableTopic, ReassignablePartition};

// Set replicas to None to cancel
let result = admin.alter_partition_reassignments(
    vec![ReassignableTopic {
        name: "my-topic".into(),
        partitions: vec![ReassignablePartition {
            partition_index: 0,
            replicas: None,  // cancel pending reassignment
        }],
    }],
    Duration::from_secs(60),
).await?;
```

### List Ongoing Reassignments

```rust,compile
let reassignments = admin
    .list_partition_reassignments(None, Duration::from_secs(60))
    .await?;
for topic in &reassignments {
    for p in &topic.partitions {
        println!("{} p{}: replicas={:?} adding={:?} removing={:?}",
            topic.name, p.partition_index, p.replicas,
            p.adding_replicas, p.removing_replicas);
    }
}
```

### AlterPartitionReassignments Protocol Versions

| Version | Changes |
|---------|--------|
| v0 | Baseline (flexible encoding from the start) |

### ListPartitionReassignments Protocol Versions

| Version | Changes |
|---------|--------|
| v0 | Baseline (flexible encoding from the start) |

## SCRAM Credential Management

Manage SASL/SCRAM credentials (KIP-554) for users.

### Describe SCRAM Credentials

```rust,compile
// Describe all users
let result = admin.describe_user_scram_credentials(None).await?;
for user in &result.users {
    println!("{}: {:?}", user.name, user.credential_infos);
}

// Describe specific users
let result = admin
    .describe_user_scram_credentials(Some(vec!["alice".into(), "bob".into()]))
    .await?;
```

### Alter SCRAM Credentials

```rust,compile
use krafka::protocol::{ScramCredentialDeletion, ScramCredentialUpsertion};
use krafka::auth::ScramMechanism;
use zeroize::Zeroizing;

let results = admin.alter_user_scram_credentials(
    vec![ScramCredentialDeletion {
        name: "alice".into(),
        mechanism: ScramMechanism::Sha512,
    }],
    vec![ScramCredentialUpsertion {
        name: "bob".into(),
        mechanism: ScramMechanism::Sha256,
        iterations: 8192,
        salt: Zeroizing::new(vec![1, 2, 3]),
        salted_password: Zeroizing::new(vec![4, 5, 6]),
    }],
).await?;
```

### SCRAM Credential Protocol Versions

| Version | Changes |
|---------|--------|
| DescribeUserScramCredentials v0 | Baseline (KIP-554, flexible from v0) |
| AlterUserScramCredentials v0 | Baseline (KIP-554, flexible from v0) |

## Log Directory Management

### Move Replicas Between Log Directories

```rust,compile
use krafka::protocol::{AlterReplicaLogDir, AlterReplicaLogDirTopic};

let results = admin.alter_replica_log_dirs(vec![
    AlterReplicaLogDir {
        path: "/data/kafka-logs-2".into(),
        topics: vec![AlterReplicaLogDirTopic {
            name: "my-topic".into(),
            partitions: vec![0, 1],
        }],
    },
]).await?;
```

### AlterReplicaLogDirs Protocol Versions

| Version | Changes |
|---------|--------|
| v1 | Baseline (non-flexible encoding) |
| v2 | Flexible encoding |

## Offset Management

### Delete Consumer Group Offsets

```rust
let result = admin.delete_offsets(
    "my-group",
    &[("my-topic", &[0, 1, 2])],
).await?;
if let Some(err) = &result.error {
    eprintln!("Top-level error: {err}");
}
```

### OffsetDelete Protocol Versions

| Version | Changes |
|---------|--------|
| v0 | Baseline (non-flexible encoding) |

## Transaction Debugging

### Describe Producers

Inspect active producers on partitions (useful for debugging stuck transactions).

```rust,compile
let results = admin
    .describe_producers(&[("my-topic", &[0, 1])])
    .await?;
for topic in &results {
    for p in &topic.partitions {
        for pr in &p.active_producers {
            println!("p{}: producer_id={} epoch={} txn_offset={}",
                p.partition_index, pr.producer_id,
                pr.producer_epoch, pr.current_txn_start_offset);
        }
    }
}
```

### Describe Transactions

```rust,compile
let results = admin
    .describe_transactions(&["txn-1", "txn-2"])
    .await?;
for txn in &results {
    println!("{}: state={} producer_id={}", txn.transactional_id, txn.state, txn.producer_id);
}
```

### List Transactions

```rust
// List all ongoing transactions
let result = admin.list_transactions(&["Ongoing"], &[], -1).await?;
for txn in &result.transactions {
    println!("{}: state={} producer_id={}", txn.transactional_id, txn.state, txn.producer_id);
}
```

### Transaction Debug Protocol Versions

| Version | Changes |
|---------|--------|
| DescribeProducers v0 | Baseline (KIP-664, flexible from v0) |
| DescribeTransactions v0 | Baseline (KIP-664, flexible from v0) |
| ListTransactions v0 | Baseline (KIP-664, flexible from v0) |
| ListTransactions v1 | Adds DurationFilter (KIP-994) |

## Client Metrics Resources

### List Client Metrics Subscriptions

```rust,compile
let names = admin.list_client_metrics_resources().await?;
for name in &names {
    println!("subscription: {name}");
}
```

### ListClientMetricsResources Protocol Versions

| Version | Changes |
|---------|--------|
| v0 | Baseline (KIP-714, flexible from v0) |

## Transaction Markers (WriteTxnMarkers)

### Write Transaction Markers

Write COMMIT or ABORT markers for transactions. Primarily useful for aborting
stuck (hanging) transactions via the `abort_transaction` convenience method.

```rust
// Abort a stuck transaction
admin.abort_transaction("my-transactional-id").await?;
```

For low-level control, use `write_txn_markers` directly:

```rust
use krafka::protocol::{WritableTxnMarker, WritableTxnMarkerTopic};

let results = admin
    .write_txn_markers(&[WritableTxnMarker {
        producer_id: 42,
        producer_epoch: 5,
        transaction_result: false, // ABORT
        topics: vec![WritableTxnMarkerTopic {
            name: "my-topic".into(),
            partition_indexes: vec![0, 1],
        }],
        coordinator_epoch: 10,
    }])
    .await?;
```

### WriteTxnMarkers Protocol Versions

| Version | Changes |
|---------|--------|
| WriteTxnMarkers v1 | Baseline (flexible encoding, v0 removed in Kafka 4.0) |
| WriteTxnMarkers v2 | Adds TransactionVersion field (KIP-1228) |

## KRaft Quorum (DescribeQuorum)

### Describe Quorum

Inspect the KRaft quorum for cluster metadata partitions. Returns voter and
observer replicas, leader info, and high watermark.

```rust,compile
let result = admin
    .describe_metadata_quorum(&[("__cluster_metadata", &[0])])
    .await?;
for topic in &result.topics {
    for partition in &topic.partitions {
        println!(
            "partition {} leader={} epoch={} hw={}",
            partition.partition_index,
            partition.leader_id,
            partition.leader_epoch,
            partition.high_watermark
        );
        for voter in &partition.current_voters {
            // `last_fetch_timestamp` / `last_caught_up_timestamp` are leader
            // wall-clock epoch millis (KIP-836, DescribeQuorum v1+), or -1 for
            // the leader's own entry and against a v0 broker. They answer the
            // question `log_end_offset` alone cannot on a low-traffic
            // partition: *how long* has this voter been silent?
            println!(
                "  voter {} log_end_offset={} last_fetch={} last_caught_up={}",
                voter.replica_id,
                voter.log_end_offset,
                voter.last_fetch_timestamp,
                voter.last_caught_up_timestamp,
            );
        }
    }
}
```

### DescribeQuorum Protocol Versions

| Version | Changes |
|---------|--------|
| DescribeQuorum v0 | Baseline (KIP-595, flexible from v0) |
| DescribeQuorum v1 | Adds LastFetchTimestamp + LastCaughtUpTimestamp (KIP-836) |
| DescribeQuorum v2 | Adds Nodes, ErrorMessage, ReplicaDirectoryId (KIP-853) |

krafka negotiates up to **v2**. Two of the v2 additions change what an operator
can actually do:

- **`nodes`** — the listener endpoints of each voter. Below v2, `DescribeQuorum`
  reported replica IDs with no way to reach them, leaving callers to
  cross-reference `DescribeCluster` and hope the two agreed.
- **`replica_directory_id`** — from KIP-853 a KRaft voter is identified by
  `(replica_id, directory_id)`, not by ID alone. A reconfiguration tool that
  removes a voter by ID can otherwise remove a node rebuilt on a fresh disk
  while leaving the original in the quorum.

```rust,compile
let result = admin
    .describe_metadata_quorum(&[("__cluster_metadata", &[0])])
    .await?;
for node in &result.nodes {
    for listener in &node.listeners {
        println!("node {} {} -> {}:{}", node.node_id, listener.name, listener.host, listener.port);
    }
}
```

Both read as their "absent" values (`None` / empty) against a broker that only
speaks v0 or v1.

## Share Group Offset Administration

Share groups (KIP-932) track a *share-partition start offset* rather than a
committed consumer offset. These three operations are how you inspect and
manage it. All require Kafka 4.2+.

### Describing share group offsets

```rust,compile
// Every topic-partition the group holds state for.
let described = admin.describe_share_group_offsets("orders-share", None).await?;
for p in &described.partitions {
    println!(
        "{}-{} start={} epoch={} lag={:?}",
        p.topic, p.partition, p.start_offset, p.leader_epoch, p.lag
    );
}

// Or a specific subset.
let subset = admin
    .describe_share_group_offsets("orders-share", Some(&[("orders", &[0, 1][..])]))
    .await?;
```

`None` and `Some(&[])` mean different things and the wire protocol distinguishes
them: `None` describes **everything**, `Some(&[])` describes **nothing**.

`lag` is `Some(_)` only when the coordinator supports `DescribeShareGroupOffsets`
v1 (KIP-1226, Kafka 4.3+). Against an older broker it is `None` rather than a
misleading `0`.

### Resetting share group offsets

**Destructive.** Moving the start offset backwards re-delivers records the group
already processed; moving it forwards skips records permanently. The group must
be **empty** — a live member draws `NON_EMPTY_GROUP`.

```rust,compile
let results = admin
    .alter_share_group_offsets("orders-share", &[("orders", &[(0, 0), (1, 0)][..])])
    .await?;
for r in &results {
    if let Some(e) = &r.error {
        eprintln!("{}-{} failed: {e}", r.topic, r.partition);
    }
}
```

### Deleting share group offsets

**Destructive.** Drops the group's state for whole topics; the group restarts
them from its configured reset policy. Use it after retiring a topic so the
coordinator stops carrying state for partitions that no longer exist. The group
must be **empty**.

```rust
let results = admin.delete_share_group_offsets("orders-share", &["retired-topic"]).await?;
```

### Share group offset API versions

| API | Key | Versions | Notes |
|-----|-----|----------|-------|
| `DescribeShareGroupOffsets` | 90 | 0–1 | v1 adds `Lag` (KIP-1226) |
| `AlterShareGroupOffsets` | 91 | 0 | Group must be empty |
| `DeleteShareGroupOffsets` | 92 | 0 | Group must be empty |

## Next Steps

- [Interceptors Guide](@/docs/interceptors.md) - Producer and consumer interceptor hooks
- [Configuration Reference](@/docs/configuration.md) - All admin client options
- [Architecture Overview](@/docs/architecture.md) - How admin client works internally

## Streams Groups (KIP-1071)

`describe_streams_groups()` inspects a Kafka Streams application's group:
topology, members, per-member task assignments, and changelog offsets. Requires
Kafka 4.1+.

```rust,compile
let groups = admin.describe_streams_groups(&["my-streams-app"]).await?;

for group in &groups {
    if !group.error_code.is_ok() {
        eprintln!("{}: {:?}", group.group_id, group.error_code);
        continue;
    }

    let topology_epoch = group.topology.as_ref().map_or(-1, |t| t.epoch);
    println!("{} [{}] epoch={}", group.group_id, group.group_state, group.group_epoch);

    for member in &group.members {
        println!(
            "  {} active={} lagging_topology={} rebalancing={}",
            member.member_id,
            member.assignment.active_tasks.len(),
            member.topology_epoch < topology_epoch,
            member.assignment != member.target_assignment,
        );
    }
}
```

krafka cannot *join* a Streams group — that is `StreamsGroupHeartbeat`, whose
request carries the application topology, and krafka has no Streams runtime.
This is the observational half, which is what you need to answer "is this
Streams application healthy?" without running one.

### What to alert on

| Signal | Meaning |
|---|---|
| `member.topology_epoch < topology.epoch` | The member is still running an older topology and has not picked up the new one |
| `member.assignment != member.target_assignment` | The member has not finished rebalancing. Persistently so usually means state restoration is not keeping up — compare `task_offsets` against `task_end_offsets` for the lag that explains it |
| `topology` is `None` | The describe failed, or the group has no topology at all |
| `topology.subtopologies` is `None` | The group is uninitialized, or its source topics are missing or incorrectly partitioned. **Different from an empty list**, and krafka preserves the distinction |

Per-group failures are reported in each group's `error_code` rather than
failing the whole call, so one unknown group does not hide the others. A broker
older than Kafka 4.1 fails the call with `UnknownApiVersion`.
