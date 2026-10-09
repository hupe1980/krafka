+++
title = "Admin Client"
description = "Topics, partitions, configs, ACLs, quotas, consumer groups, delegation tokens and cluster features."
weight = 60

[extra]
slug_id = "admin"
+++

## Overview

`AdminClient` manages a cluster: topics and partitions, configurations,
ACLs, consumer, share and Streams groups, offsets, transactions, client
quotas, SCRAM credentials, delegation tokens, features, log directories,
leader elections, reassignments and the KRaft quorum.

## Basic Usage

```rust,compile
use krafka::admin::{AdminClient, ListTopicsOptions};
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let admin = krafka::Kafka::builder("localhost:9092")
        .connect()
        .await?
        .admin();

    let topics = admin.list_topics(ListTopicsOptions::default()).await?;
    println!("Topics: {topics:?}");

    admin.close().await?;
    Ok(())
}
```

## The shape of every operation

Every operation is one method. It takes its arguments as krafka types and an
options struct named after it — `CreateTopicsOptions`,
`DescribeConfigsOptions`, `ListOffsetsOptions` — whose `Default` is the usual
choice. Options are set with builder methods:

```rust,compile
use krafka::admin::{CreateTopicsOptions, NewTopic};
use std::time::Duration;

let options = CreateTopicsOptions::default()
    .validate_only(true)
    .timeout(Duration::from_secs(10));
let results = admin
    .create_topics([NewTopic::new("orders", 6, 3)?], options)
    .await?;
```

### One result per item

An operation over several items — topics, partitions, groups, resources,
brokers — returns a `Result` **per item**, keyed by the item. One item's
failure does not fail the others. The outer `Result` fails only for what
applies to the whole call: a closed client or invalid arguments.

Per-item errors keep the broker's code as `KrafkaError::Broker { code, .. }`,
so they can be matched and `is_retriable()` means what it says:

```rust,compile
use krafka::admin::{CreateTopicsOptions, NewTopic};
use krafka::error::{ErrorCode, KrafkaError};

let results = admin
    .create_topics([NewTopic::new("orders", 6, 3)?], CreateTopicsOptions::default())
    .await?;
for (topic, result) in results {
    match result {
        Ok(()) => println!("created {topic}"),
        Err(KrafkaError::Broker { code: ErrorCode::TopicAlreadyExists, .. }) => {
            println!("{topic} already exists");
        }
        Err(e) => return Err(e),
    }
}
```

An operation that asks one node one question — `describe_cluster`,
`describe_acls`, `list_consumer_group_offsets` for one group — returns that
answer, and a broker error for the request as a whole is its `Err`.

### Routing

Each request goes to the node that can answer it:

| Target | Operations |
|---|---|
| Controller | create/delete topics, create partitions, ACL create/delete, incremental alter configs (non-broker resources), client quota alter, SCRAM alter, delegation token create, elect leaders, alter reassignments, update features |
| Group coordinator | describe/delete consumer groups, list/alter/delete group offsets, share-group offsets, Streams groups |
| Transaction coordinator | describe transactions |
| Partition leader | list offsets, delete records, offset for leader epoch, describe producers, the markers of `abort_transaction` |
| A named broker | broker and broker-logger configs, log directories, `list_consumer_groups` and `list_transactions` (every broker), replica log-dir moves |
| Any broker | describe cluster/ACLs/quotas/features/quorum/topics, other configs, token renew/expire/describe, list reassignments, FindCoordinator |

Keys bound for the same node go in one request; requests to different nodes
run concurrently.

### Retries and the deadline

Items whose attempt can succeed elsewhere or later are retried with backoff,
with the node chosen again: a coordinator is looked up again after
`NOT_COORDINATOR`, metadata is fetched again after `NOT_CONTROLLER` or a
leader change, and an any-broker request moves to another broker when the
first one fails.

- **Reads** are retried on any retriable error, including a connection that
  dropped with the request in flight.
- **Writes** are retried only when they were not applied: the connection could
  not be opened, or the broker answered `NOT_CONTROLLER`, `NOT_COORDINATOR`,
  `COORDINATOR_LOAD_IN_PROGRESS`, `COORDINATOR_NOT_AVAILABLE`,
  `NOT_LEADER_OR_FOLLOWER`, `LEADER_NOT_AVAILABLE` or
  `THROTTLING_QUOTA_EXCEEDED`. A write that was sent and never answered fails
  with its `Network` or `Timeout` error, because it may have been applied.

Every call is bounded by its `timeout`, by default the client's
`default_api_timeout` (60 s, as Java's `default.api.timeout.ms`). Lookups,
attempts and backoff all fit inside it. An item still failing at the deadline
reports the broker's last answer, or `KrafkaError::Timeout`.

```rust,compile
use std::time::Duration;

let admin = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .admin()
    .default_api_timeout(Duration::from_secs(30))
    .retry_backoff(Duration::from_millis(200));
admin.close().await?;
```

`retry_backoff` (default 100 ms) is the first backoff; it doubles to a 1 s
ceiling with 20 % jitter. There is no retry count: the deadline is the bound.

### Metrics and telemetry

`metrics()` returns the client's `Metrics` snapshot; an admin client fills in
only the `connections` section. KIP-714 metrics push is **off** for the admin
client, as in Java; `metrics_push(true)` turns it on, and
`client_instance_id(timeout)` then returns the broker-assigned id. See
[Metrics](@/docs/metrics.md).

## Authentication

### SASL/SCRAM

```rust,compile
let admin = krafka::Kafka::builder("localhost:9092")
    .security(krafka::auth::AuthConfig::sasl_scram_sha512("username", "password"))
    .connect()
    .await?
    .admin();
admin.close().await?;
```

### Generic AuthConfig

For AWS MSK IAM or other configurations:

```rust,compile
use krafka::auth::AuthConfig;

let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let admin = krafka::Kafka::builder("msk-broker:9098")
    .security(auth)
    .connect()
    .await?
    .admin();
admin.close().await?;
```

## Topics

### Creating topics

```rust,compile
use krafka::admin::{CreateTopicsOptions, NewTopic};

let topics = [
    NewTopic::new("events", 6, 3)?,
    NewTopic::new("compacted", 12, 3)?
        .with_config("cleanup.policy", "compact")
        .with_config("min.insync.replicas", "2"),
];
let results = admin.create_topics(topics, CreateTopicsOptions::default()).await?;
for (topic, result) in &results {
    if let Err(e) = result {
        eprintln!("{topic}: {e}");
    }
}
```

`NewTopic::with_replica_assignment` places replicas explicitly (partition →
broker IDs, the first being the preferred leader).

### Deleting topics

```rust,compile
use krafka::admin::DeleteTopicsOptions;

let results = admin
    .delete_topics(["old-topic"], DeleteTopicsOptions::default())
    .await?;
```

### Listing and describing topics

`list_topics` fetches metadata and returns the names; internal topics are left
out unless `include_internal(true)`. `describe_topics` returns partitions,
leaders, replicas and ISR — and, on brokers with `DescribeTopicPartitions`
(KIP-966), eligible leader replicas:

```rust,compile
use krafka::admin::DescribeTopicsOptions;

let described = admin
    .describe_topics(["events", "missing"], DescribeTopicsOptions::default())
    .await?;
for (name, result) in &described {
    match result {
        Ok(topic) => {
            println!("{name}: {} partitions", topic.partitions.len());
            for p in &topic.partitions {
                println!(
                    "  {}: leader={:?} isr={:?} elr={:?}",
                    p.partition, p.leader, p.isr, p.eligible_leader_replicas
                );
            }
        }
        Err(e) => eprintln!("{name}: {e}"),
    }
}
```

### Adding partitions

The counts are the new **totals**; partition counts only grow:

```rust,compile
use krafka::admin::CreatePartitionsOptions;

let results = admin
    .create_partitions([("events", 12)], CreatePartitionsOptions::default())
    .await?;
```

## Configurations

A `ConfigResource` names a topic, a broker, a broker's loggers, a group or a
client-metrics subscription. Broker and broker-logger resources are described
and altered **at that broker**.

```rust,compile
use krafka::admin::{ConfigResource, DescribeConfigsOptions};

let topic = ConfigResource::topic("events");
let configs = admin
    .describe_configs(
        [topic.clone(), ConfigResource::broker(1)],
        DescribeConfigsOptions::default().config_names(vec!["retention.ms".into()]),
    )
    .await?;
if let Ok(entries) = &configs[&topic] {
    for entry in entries {
        println!("{} = {:?}", entry.name, entry.config_value());
    }
}
```

`ConfigEntry::config_value()` tells an explicit value from a redacted one, the
broker default, and an unavailable key.

Changes use `IncrementalAlterConfigs`:

```rust,compile
use krafka::admin::{ConfigOp, ConfigResource, IncrementalAlterConfigsOptions};

let results = admin
    .incremental_alter_configs(
        [(
            ConfigResource::topic("events"),
            vec![
                ConfigOp::set("retention.ms", "86400000"),
                ConfigOp::delete("segment.bytes"),
            ],
        )],
        IncrementalAlterConfigsOptions::default(),
    )
    .await?;
```

`list_config_resources` lists the resources the cluster has configuration for
(KIP-1142); before Kafka 4.1 only client-metrics subscriptions can be listed.

## Cluster

```rust,compile
use krafka::admin::DescribeClusterOptions;

let cluster = admin.describe_cluster(DescribeClusterOptions::default()).await?;
println!("cluster {} controller {}", cluster.cluster_id, cluster.controller_id);
for broker in &cluster.brokers {
    println!("  {} at {}:{} fenced={}", broker.id, broker.host, broker.port, broker.is_fenced);
}
```

## Offsets

### Listing partition offsets

Each partition is asked at its leader. `OffsetSpec` covers the earliest and
latest offsets, a timestamp, the largest timestamp (KIP-734), and the
tiered-storage offsets: earliest local (KIP-405, `ListOffsets` v8+), latest
tiered (KIP-1005, v9+) and earliest pending upload (KIP-1023, v11+, the first
offset not yet copied to remote storage). A broker too old for a spec fails
that partition, before anything is sent, rather than answer the wrong question.

```rust,compile
use krafka::admin::{ListOffsetsOptions, OffsetSpec, TopicPartition};

let ends = admin
    .list_offsets(
        (0..3).map(|p| (TopicPartition::new("events", p), OffsetSpec::Latest)),
        ListOffsetsOptions::default(),
    )
    .await?;
for (tp, result) in &ends {
    match result {
        Ok(listed) => println!("{}-{}: {}", tp.topic, tp.partition, listed.offset),
        Err(e) => eprintln!("{}-{}: {e}", tp.topic, tp.partition),
    }
}
```

### Deleting records and leader epochs

`delete_records` moves each partition's log start offset and returns the new
low watermark. `offset_for_leader_epoch` returns where an epoch ends, which is
how log truncation after a leader change is detected.

```rust,compile
use krafka::admin::{DeleteRecordsOptions, TopicPartition};

let low_watermarks = admin
    .delete_records(
        [(TopicPartition::new("events", 0), 1_000)],
        DeleteRecordsOptions::default(),
    )
    .await?;
```

## Consumer groups

### Describing and listing

`describe_consumer_groups` describes KIP-848 groups with ConsumerGroupDescribe
and classic groups with DescribeGroups, decoding classic members' subscriptions
and assignments:

```rust,compile
use krafka::admin::DescribeConsumerGroupsOptions;

let groups = admin
    .describe_consumer_groups(["orders-service"], DescribeConsumerGroupsOptions::default())
    .await?;
for (id, result) in &groups {
    match result {
        Ok(group) => println!(
            "{id}: {} {} with {} member(s)",
            group.group_type,
            group.state,
            group.members.len()
        ),
        Err(e) => eprintln!("{id}: {e}"),
    }
}
```

Every broker knows the groups it coordinates, so `list_consumer_groups` asks
every broker and returns a result **per broker**: a failed broker is an `Err`
beside the others' listings, never silently missing. The state and type
filters are applied by the broker.

```rust,compile
use krafka::admin::ListConsumerGroupsOptions;

let per_broker = admin
    .list_consumer_groups(ListConsumerGroupsOptions::default().states(vec!["Empty".into()]))
    .await?;
for (broker, listing) in &per_broker {
    match listing {
        Ok(groups) => {
            for group in groups {
                println!("{} (broker {broker})", group.group_id);
            }
        }
        Err(e) => eprintln!("broker {broker} did not answer: {e}"),
    }
}
```

`delete_consumer_groups` deletes each group at its coordinator.

### Committed offsets and lag

```rust,compile
use krafka::admin::{ConsumerGroupLagOptions, ListConsumerGroupOffsetsOptions, TopicPartition};

let offsets = admin
    .list_consumer_group_offsets("orders-service", ListConsumerGroupOffsetsOptions::default())
    .await?;
for (tp, offset) in &offsets {
    println!("{}-{}: {:?}", tp.topic, tp.partition, offset.as_ref().map(|o| o.offset));
}

let lag = admin
    .consumer_group_lag("orders-service", ConsumerGroupLagOptions::default())
    .await?;
for (tp, result) in &lag {
    if let Ok(partition) = result {
        println!("{}-{}: lag {:?}", tp.topic, tp.partition, partition.lag);
    }
}

// Reset a partition of an empty group.
let results = admin
    .alter_consumer_group_offsets(
        "orders-service",
        [(TopicPartition::new("orders", 0), 0)],
        Default::default(),
    )
    .await?;
```

`require_stable(true)` reports only offsets no in-flight transaction can
retract (KIP-447). A partition whose end offset could not be fetched has an
`Err` lag, never a lag of zero. `delete_consumer_group_offsets` removes a
group's committed offsets.

## Share groups (KIP-932)

`describe_share_group_offsets`, `alter_share_group_offsets` and
`delete_share_group_offsets` read and reset a share group's share-partition
start offsets at its coordinator. Lag is reported from
`DescribeShareGroupOffsets` v1 (KIP-1226); against older brokers it is `None`.
Altering and deleting require an empty group.

```rust,compile
use krafka::admin::DescribeShareGroupOffsetsOptions;

let offsets = admin
    .describe_share_group_offsets("orders-share", DescribeShareGroupOffsetsOptions::default())
    .await?;
for (tp, result) in &offsets {
    if let Ok(state) = result {
        println!("{}-{} start={} lag={:?}", tp.topic, tp.partition, state.start_offset, state.lag);
    }
}
```

## Streams groups (KIP-1071)

`describe_streams_groups` returns a Streams application's topology, members,
task assignments and offsets (Kafka 4.1+). It only reads: krafka cannot join a
Streams group.

```rust,compile
use krafka::admin::DescribeStreamsGroupsOptions;

let groups = admin
    .describe_streams_groups(["my-streams-app"], DescribeStreamsGroupsOptions::default())
    .await?;
for (id, result) in &groups {
    let Ok(group) = result else { continue };
    let topology_epoch = group.topology.as_ref().map_or(-1, |t| t.epoch);
    for member in &group.members {
        println!(
            "{id}/{} lagging_topology={} rebalancing={}",
            member.member_id,
            member.topology_epoch < topology_epoch,
            member.assignment != member.target_assignment,
        );
    }
}
```

| Signal | Meaning |
|---|---|
| `member.topology_epoch < topology.epoch` | The member runs an older topology |
| `member.assignment != member.target_assignment` | The member has not finished rebalancing |
| `topology.subtopologies` is `None` | The group is uninitialized or its source topics are missing — different from an empty list |

## ACLs

`AclFilter` matches bindings for `describe_acls` and `delete_acls`; unset fields
match anything.

```rust,compile
use krafka::admin::{
    AclBinding, AclFilter, AclResourceType, CreateAclsOptions, DeleteAclsOptions,
    DescribeAclsOptions,
};

let created = admin
    .create_acls(
        vec![AclBinding::allow_read_topic("events", "User:alice")],
        CreateAclsOptions::default(),
    )
    .await?;
for (binding, result) in &created {
    if let Err(e) = result {
        eprintln!("{binding:?}: {e}");
    }
}

let bindings = admin
    .describe_acls(
        AclFilter::for_resource(AclResourceType::Topic, "events"),
        DescribeAclsOptions::default(),
    )
    .await?;
println!("{} binding(s)", bindings.len());

let deleted = admin
    .delete_acls(vec![AclFilter::for_principal("User:alice")], DeleteAclsOptions::default())
    .await?;
for (_, result) in &deleted {
    if let Ok(outcome) = result {
        println!("deleted {}, failed {}", outcome.deleted.len(), outcome.failed.len());
    }
}
```

## Delegation tokens

```rust,compile
use krafka::admin::{
    CreateDelegationTokenOptions, DelegationTokenPrincipal, DescribeDelegationTokenOptions,
    ExpireDelegationTokenOptions, RenewDelegationTokenOptions,
};
use std::time::Duration;

let token = admin
    .create_delegation_token(
        CreateDelegationTokenOptions::default()
            .renewers(vec![DelegationTokenPrincipal::user("ops")])
            .max_lifetime(Duration::from_secs(24 * 3600)),
    )
    .await?;
println!("token {} expires at {}", token.token_id, token.expiry_timestamp_ms);

let expiry = admin
    .renew_delegation_token(&token.hmac, RenewDelegationTokenOptions::default())
    .await?;
let tokens = admin
    .describe_delegation_token(DescribeDelegationTokenOptions::default())
    .await?;
admin
    .expire_delegation_token(&token.hmac, ExpireDelegationTokenOptions::default())
    .await?;
```

`CreateDelegationTokenOptions::owner` creates a token on behalf of another
principal (KIP-373); `DelegationToken::requester` then names who asked. The
HMAC is redacted from `Debug` output.

## Client quotas

```rust,compile
use krafka::admin::{
    AlterClientQuotasOptions, ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter,
    DescribeClientQuotasOptions, QuotaMatch,
};

let alice: ClientQuotaEntity = [("user", Some("alice"))].into_iter().collect();
let results = admin
    .alter_client_quotas(
        vec![ClientQuotaAlteration::new(
            alice.clone(),
            vec![("producer_byte_rate".into(), Some(1_048_576.0))],
        )],
        AlterClientQuotasOptions::default(),
    )
    .await?;

let quotas = admin
    .describe_client_quotas(
        ClientQuotaFilter::all().component("user", QuotaMatch::Exact("alice".into())),
        DescribeClientQuotasOptions::default(),
    )
    .await?;
println!("{:?}", quotas.get(&alice));
```

A value of `None` in an alteration removes that quota.

## Features (KIP-584)

```rust,compile
use krafka::admin::{DescribeFeaturesOptions, FeatureUpdateKey, UpdateFeaturesOptions};

let features = admin.describe_features(DescribeFeaturesOptions::default()).await?;
for f in &features.finalized {
    println!("{} = {}", f.name, f.max_version_level);
}

// A dry run first: validate without applying.
let results = admin
    .update_features(
        vec![FeatureUpdateKey::upgrade("metadata.version", 17)],
        UpdateFeaturesOptions::default().validate_only(true),
    )
    .await?;
```

Supported features come from one broker's `ApiVersions` answer, and differ
between brokers during a rolling upgrade. Name the broker to ask with
`node_id` (KIP-1160); an id that is not in the cluster metadata fails the call
instead of falling back to another broker:

```rust,compile
use krafka::admin::DescribeFeaturesOptions;

let on_broker_2 = admin
    .describe_features(DescribeFeaturesOptions::default().node_id(2))
    .await?;
println!("broker 2 supports {} features", on_broker_2.supported.len());
```

Downgrades can lose data. A Kafka 4.0+ controller answers for the request as a
whole; `validate_only` against a controller without `UpdateFeatures` v1 is
refused before anything is sent.

## Replicas and leaders

### Log directories

```rust,compile
use krafka::admin::{AlterReplicaLogDirsOptions, DescribeLogDirsOptions, TopicPartitionReplica};

let per_broker = admin.describe_log_dirs([1, 2, 3], DescribeLogDirsOptions::default()).await?;
for (broker, dirs) in &per_broker {
    if let Ok(dirs) = dirs {
        for (path, dir) in dirs {
            if let Ok(dir) = dir {
                println!(
                    "broker {broker} {path}: {} replicas, usable {:?}",
                    dir.replicas.len(),
                    dir.usable_bytes
                );
            }
        }
    }
}

// Move one replica to another directory on the broker that holds it.
let moved = admin
    .alter_replica_log_dirs(
        [(TopicPartitionReplica::new("events", 0, 1), "/data/kafka-2".to_string())],
        AlterReplicaLogDirsOptions::default(),
    )
    .await?;
```

### Leader election

A partition whose preferred leader already leads (`ELECTION_NOT_NEEDED`) is
`Ok`. `None` elects for every partition.

```rust,compile
use krafka::admin::{ElectLeadersOptions, ElectionType, TopicPartition};

let results = admin
    .elect_leaders(
        ElectionType::Preferred,
        Some(vec![TopicPartition::new("events", 0)]),
        ElectLeadersOptions::default(),
    )
    .await?;
```

### Reassignments

A target of `None` cancels the partition's pending reassignment.
`allow_replication_factor_change(false)` makes the controller reject a target
of a different size (Kafka 4.1+); against an older controller the call fails
rather than send a request that would permit the change.

```rust,compile
use krafka::admin::{
    AlterPartitionReassignmentsOptions, ListPartitionReassignmentsOptions, TopicPartition,
};

let results = admin
    .alter_partition_reassignments(
        [(TopicPartition::new("events", 0), Some(vec![1, 2, 3]))],
        AlterPartitionReassignmentsOptions::default().allow_replication_factor_change(false),
    )
    .await?;

let ongoing = admin
    .list_partition_reassignments(ListPartitionReassignmentsOptions::default())
    .await?;
for (tp, r) in &ongoing {
    println!("{}-{}: adding {:?} removing {:?}", tp.topic, tp.partition, r.adding_replicas, r.removing_replicas);
}
```

## SCRAM credentials

`describe_user_scram_credentials` returns each user's mechanisms and iteration
counts. `alter_user_scram_credentials` takes deletions and upsertions
(precomputed salt and salted password) and returns a result per user.

## Transactions

```rust,compile
use krafka::admin::{
    AbortTransactionOptions, DescribeProducersOptions, DescribeTransactionsOptions,
    ListTransactionsOptions, TopicPartition,
};

let producers = admin
    .describe_producers([TopicPartition::new("events", 0)], DescribeProducersOptions::default())
    .await?;

let transactions = admin
    .describe_transactions(["payments-tx"], DescribeTransactionsOptions::default())
    .await?;
if let Ok(tx) = &transactions["payments-tx"] {
    println!("{} on {} partition(s)", tx.state, tx.partitions.len());
}

let per_broker = admin
    .list_transactions(ListTransactionsOptions::default().states(vec!["Ongoing".into()]))
    .await?;

// Abort a hanging transaction.
let aborted = admin
    .abort_transaction("payments-tx", AbortTransactionOptions::default())
    .await?;
```

`abort_transaction` writes ABORT markers to each partition's leader. The
coordinator epoch is read from the producer state on those partitions unless
`AbortTransactionOptions::coordinator_epoch` gives it; a leader with no cached
epoch accepts any epoch, so krafka does not send a guessed one. Partitions that disagree
on the epoch are a retriable `CONCURRENT_TRANSACTIONS`: the transaction is
mid-transition.

`list_transactions` asks every broker and returns a result per broker;
`transactional_id_pattern` needs Kafka 4.1+ (KIP-1152).

## KRaft quorum

```rust,compile
use krafka::admin::DescribeMetadataQuorumOptions;

let quorum = admin
    .describe_metadata_quorum(DescribeMetadataQuorumOptions::default())
    .await?;
println!("leader {} epoch {} hw {}", quorum.leader_id, quorum.leader_epoch, quorum.high_watermark);
for voter in &quorum.voters {
    println!("  voter {} at {} (last fetch {:?})", voter.replica_id, voter.log_end_offset, voter.last_fetch_timestamp);
}
```

From `DescribeQuorum` v2 (KIP-853) voters carry a directory ID and `nodes`
lists each node's listeners.

## Common topic configurations

| Configuration | Type | Default | Description |
|--------------|------|---------|-------------|
| `cleanup.policy` | String | `delete` | `delete` or `compact` |
| `compression.type` | String | `producer` | Compression type |
| `retention.ms` | Long | `604800000` | Message retention time (-1 = infinite) |
| `retention.bytes` | Long | `-1` | Max partition size (-1 = infinite) |
| `segment.bytes` | Int | 1GB | Segment file size |
| `min.insync.replicas` | Int | `1` | Min ISR for writes with acks=all |
| `max.message.bytes` | Int | `1048588` | Max record batch size |
| `unclean.leader.election.enable` | Bool | `false` | Allow unclean leader election |

## Next Steps

- [Configuration Reference](@/docs/configuration.md) - All admin client options
- [Architecture Overview](@/docs/architecture.md) - How the clients work internally
- [Error Handling](@/docs/errors.md) - Error kinds and retriability
