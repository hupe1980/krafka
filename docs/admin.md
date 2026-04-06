---
layout: default
title: Admin Client
nav_order: 5
description: "Cluster administration - topics, partitions, and configuration"
---

# Admin Client Guide

This guide covers administrative operations using the Krafka AdminClient.

## Overview

The AdminClient provides cluster administration capabilities:

- Topic management (create, delete, describe, list)
- Consumer group management (describe, list)
- Record deletion (delete records before an offset)
- Leader epoch queries (detect log truncation)
- Cluster information
- Partition management
- ACL management
- Delegation token management (create, describe, renew, expire)
- Client quota management (describe, alter)

### API Version Negotiation

The AdminClient automatically negotiates the best API version for each RPC
using the broker's `ApiVersions` response. This ensures forward compatibility
with newer Kafka releases while gracefully falling back to older protocol
versions on legacy brokers. If a broker does not support a required API, the
client returns a clear protocol error.

## Basic Usage

```rust
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

```rust
let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    .sasl_scram_sha256("username", "password")
    .build()
    .await?;
```

### SASL/SCRAM-SHA-512

```rust
let admin = AdminClient::builder()
    .bootstrap_servers("localhost:9092")
    .sasl_scram_sha512("username", "password")
    .build()
    .await?;
```

### Generic AuthConfig

For AWS MSK IAM or advanced configurations:

```rust
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
    .create_topics(vec![topic], Duration::from_secs(30))
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

admin.create_topics(vec![topic], Duration::from_secs(30)).await?;
```

### Deleting Topics

```rust
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

```rust
let topics = admin.list_topics().await?;
println!("Topics in cluster:");
for topic in topics {
    println!("  - {}", topic);
}
```

### Describing Topics

```rust
let descriptions = admin
    .describe_topics(&["topic1".to_string(), "topic2".to_string()])
    .await?;

for topic in descriptions {
    println!("Topic: {}", topic.name);
    println!("  Partitions: {}", topic.partitions.len());
    for partition in &topic.partitions {
        println!(
            "    Partition {}: leader={}, replicas={:?}, isr={:?}",
            partition.partition,
            partition.leader,
            partition.replicas,
            partition.isr
        );
    }
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

### Describing Topic Configuration

```rust
let configs = admin.describe_topic_config("my-topic").await?;

println!("Topic configuration:");
for config in configs {
    let value = config.value.as_deref().unwrap_or("(null)");
    let flags = format!(
        "{}{}{}",
        if config.read_only { "R" } else { "" },
        if config.is_default { "D" } else { "" },
        if config.is_sensitive { "S" } else { "" }
    );
    println!("  {}: {} [{}]", config.name, value, flags);
}
```

### Describing Broker Configuration

```rust
let configs = admin.describe_broker_config(0).await?;

println!("Broker 0 configuration:");
for config in configs.iter().filter(|c| !c.is_default) {
    println!("  {}: {:?}", config.name, config.value);
}
```

### Altering Topic Configuration

```rust
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

```rust
let cluster = admin.describe_cluster().await?;

println!("Cluster info:");
if let Some(controller) = cluster.controller_id {
    println!("  Controller: {}", controller);
}
println!("  Brokers:");
for broker in cluster.brokers {
    println!(
        "    - {} at {}:{} (rack: {:?})",
        broker.id, broker.host(), broker.port(), broker.rack()
    );
}
```

### Getting Partition Count

```rust
if let Some(count) = admin.partition_count("my-topic").await? {
    println!("Topic has {} partitions", count);
} else {
    println!("Topic not found");
}
```

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
        .create_topics(vec![topic], Duration::from_secs(30))
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
let results = admin.create_topics(topics, timeout).await?;

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
admin.create_topics(topics, Duration::from_secs(60)).await?;
admin.delete_topics(topics, Duration::from_secs(60)).await?;
```

### Handle Topic Already Exists

```rust
let results = admin.create_topics(vec![topic], timeout).await?;

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

```rust
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

```rust
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

Get detailed information about one or more consumer groups, including their state, members, and assignment protocol. The request is automatically routed to each group's coordinator broker via FindCoordinator:

```rust
let descriptions = admin
    .describe_groups(vec!["my-group".to_string(), "other-group".to_string()])
    .await?;

for group in &descriptions {
    println!("Group: {} (state: {})", group.group_id, group.state);
    println!("  Protocol: {} / {}", group.protocol_type, group.protocol);
    for member in &group.members {
        println!(
            "    Member: {} (client: {}, host: {}, instance: {:?})",
            member.member_id, member.client_id, member.client_host,
            member.group_instance_id
        );
    }
    if let Some(error) = &group.error {
        println!("  Error: {}", error);
    }
}
```

### Listing Consumer Groups

List all consumer groups across the cluster:

```rust
let groups = admin.list_consumer_groups().await?;

println!("Consumer groups:");
for group in &groups {
    println!("  {} (protocol: {})", group.group_id, group.protocol_type);
}
```

> **Note:** `list_consumer_groups()` queries all brokers in the cluster and deduplicates results, since consumer groups are managed by their respective group coordinators.

## Record Deletion

### Deleting Records

Delete records from topic partitions before a specified offset. Records with offsets less than the specified offset are marked for deletion (this adjusts the log start offset). Requests are automatically routed to each partition's leader broker:

```rust
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

```rust
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

```rust
use std::time::Duration;

// Create a token that "alice" can renew, with a 24-hour lifetime
let result = admin
    .create_delegation_token(
        &[("User", "alice")],
        Some(Duration::from_secs(86_400)),
    )
    .await?;

match result.token {
    Some(token) => println!("Created token: {} (HMAC {} bytes)", token.token_id, token.hmac.len()),
    None => println!("Error: {}", result.error.unwrap()),
}
```

Pass an empty renewers slice to allow only the token owner to renew.
Use `None` for `max_lifetime` to accept the server default (typically 7 days).

### Describing Tokens

```rust
// Describe all tokens visible to the caller
let tokens = admin.describe_delegation_tokens(None).await?;
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
    .describe_delegation_tokens(Some(&[("User", "alice")]))
    .await?;
```

### Renewing a Token

```rust
use std::time::Duration;

// Obtain a token (e.g., from a prior create call)
let result = admin
    .create_delegation_token(&[("User", "alice")], Some(Duration::from_secs(86_400)))
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

```rust
use std::time::Duration;

// Obtain a token (e.g., from describe)
let tokens = admin.describe_delegation_tokens(None).await?;
let token = &tokens[0];

// Expire a token immediately
let result = admin.expire_delegation_token(&token.hmac, None).await?;

// Expire a token after a grace period
let result = admin
    .expire_delegation_token(&token.hmac, Some(Duration::from_secs(60)))
    .await?;
```

## Client Quotas

Client quotas control the resource usage of clients (producer/consumer byte
rates, request percentages, etc.). Use `describe_client_quotas` to query
current quotas and `alter_client_quotas` to change them.

### Describing Quotas

```rust
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

```rust
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

## Next Steps

- [Interceptors Guide](interceptors.md) - Producer and consumer interceptor hooks
- [Configuration Reference](configuration.md) - All admin client options
- [Architecture Overview](architecture.md) - How admin client works internally
