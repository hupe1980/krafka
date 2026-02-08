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
- Cluster information
- Partition management

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
        broker.id, broker.host, broker.port, broker.rack
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

## Next Steps

- [Configuration Reference](configuration.md) - All admin client options
- [Architecture Overview](architecture.md) - How admin client works internally
