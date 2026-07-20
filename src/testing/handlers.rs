//! Default request handlers.
//!
//! These are deliberately minimal: enough to carry a real `krafka` client
//! through a handshake, a metadata refresh, a produce/consume cycle and a
//! consumer-group session, and no further. Anything a test wants to be
//! different it drives through a control hook or the cluster-manipulation API,
//! not by extending the defaults.
//!
//! Every handler is *routing-aware*: it checks whether the broker it is running
//! on is actually the leader, the coordinator or the controller for the request
//! it received, and returns the corresponding Kafka error if not. That is what
//! makes leader and coordinator moves observable to the client rather than
//! silently absorbed.

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{ErrorCode, Result};
use crate::protocol::ApiKey;
use crate::protocol::{Encode, KafkaString, TryEncode};

use super::state::{ClusterState, CommittedOffset, GroupMember};
use super::wire::*;

/// The single API version the fake broker speaks for each supported API.
///
/// Pinning `min == max` forces the client's negotiation onto exactly the
/// version each codec in [`super::wire`] was written against. Non-flexible
/// versions are chosen wherever the client still accepts them, so there are no
/// compact lengths or tagged fields to mis-handle.
pub(crate) fn supported_versions() -> Vec<(ApiKey, i16)> {
    vec![
        (ApiKey::ApiVersions, 0),
        (ApiKey::Metadata, 8),
        // v10 is the lowest Produce version carrying the KIP-951 leader hint,
        // which a client test needs to observe a failover without a metadata
        // refresh. It is flexible, hence the tagged-field handling in `wire`.
        (ApiKey::Produce, 10),
        (ApiKey::Fetch, 11),
        (ApiKey::ListOffsets, 5),
        (ApiKey::FindCoordinator, 2),
        (ApiKey::JoinGroup, 5),
        (ApiKey::SyncGroup, 3),
        (ApiKey::Heartbeat, 3),
        (ApiKey::LeaveGroup, 3),
        (ApiKey::OffsetCommit, 7),
        (ApiKey::OffsetFetch, 5),
        (ApiKey::InitProducerId, 1),
        (ApiKey::CreateTopics, 4),
        (ApiKey::DeleteTopics, 3),
    ]
}

/// Serve one request, writing the response body (no header) into `out`.
///
/// `node_id` is the broker the request arrived at, which is what lets the
/// handlers detect misrouted requests.
pub(crate) fn dispatch(
    api_key: ApiKey,
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    match api_key {
        ApiKey::ApiVersions => api_versions(out),
        ApiKey::Metadata => metadata(body, state, out),
        ApiKey::Produce => produce(body, node_id, state, out),
        ApiKey::Fetch => fetch(body, node_id, state, out),
        ApiKey::ListOffsets => list_offsets(body, node_id, state, out),
        ApiKey::FindCoordinator => find_coordinator(body, state, out),
        ApiKey::JoinGroup => join_group(body, node_id, state, out),
        ApiKey::SyncGroup => sync_group(body, node_id, state, out),
        ApiKey::Heartbeat => heartbeat(body, node_id, state, out),
        ApiKey::LeaveGroup => leave_group(body, node_id, state, out),
        ApiKey::OffsetCommit => offset_commit(body, node_id, state, out),
        ApiKey::OffsetFetch => offset_fetch(body, node_id, state, out),
        ApiKey::InitProducerId => init_producer_id(body, state, out),
        ApiKey::CreateTopics => create_topics(body, node_id, state, out),
        ApiKey::DeleteTopics => delete_topics(body, node_id, state, out),
        other => Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!("fake broker has no handler for {other:?}"),
        )),
    }
}

/// Serve one request as a forced failure with `code`.
///
/// The response is structurally valid for the API — the error is placed in
/// whatever top-level or per-partition field the format actually has — so the
/// client's normal error handling runs, rather than its "malformed frame" path.
pub(crate) fn dispatch_error(
    api_key: ApiKey,
    body: &mut Bytes,
    code: ErrorCode,
    out: &mut BytesMut,
) -> Result<()> {
    match api_key {
        ApiKey::ApiVersions => {
            write_error(out, code);
            write_array_len(out, 0)
        }
        ApiKey::Metadata => {
            let req = MetadataReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, 0)?; // brokers
            write_nullable_string(out, None)?; // cluster_id
            out.put_i32(-1); // controller_id
            let names = req.topics.unwrap_or_default();
            write_array_len(out, names.len())?;
            for name in &names {
                write_error(out, code);
                write_string(out, name)?;
                out.put_u8(0);
                write_array_len(out, 0)?;
                out.put_i32(i32::MIN);
            }
            out.put_i32(i32::MIN);
            Ok(())
        }
        ApiKey::Produce => {
            let req = ProduceReq::read(body)?;
            write_compact_array_len(out, req.topics.len())?;
            for topic in &req.topics {
                KafkaString::new(&topic.name).try_encode_compact(out)?;
                write_compact_array_len(out, topic.partitions.len())?;
                for partition in &topic.partitions {
                    // No `CurrentLeader`: an injected error stands in for a
                    // broker that reports a problem without naming a
                    // replacement, which is the case that still needs a
                    // metadata refresh.
                    write_produce_partition(out, partition.index, code, -1, -1, None)?;
                }
                write_empty_tagged_fields(out)?;
            }
            out.put_i32(0);
            write_empty_tagged_fields(out)
        }
        ApiKey::Fetch => {
            let req = FetchReq::read(body)?;
            out.put_i32(0);
            write_error(out, ErrorCode::None);
            out.put_i32(req.session_id);
            write_array_len(out, req.topics.len())?;
            for topic in &req.topics {
                write_string(out, &topic.topic)?;
                write_array_len(out, topic.partitions.len())?;
                for partition in &topic.partitions {
                    write_fetch_partition(out, partition.partition, code, 0, 0, None)?;
                }
            }
            Ok(())
        }
        ApiKey::ListOffsets => {
            let req = ListOffsetsReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, req.topics.len())?;
            for topic in &req.topics {
                write_string(out, &topic.name)?;
                write_array_len(out, topic.partitions.len())?;
                for partition in &topic.partitions {
                    out.put_i32(partition.partition_index);
                    write_error(out, code);
                    out.put_i64(-1);
                    out.put_i64(-1);
                    out.put_i32(-1);
                }
            }
            Ok(())
        }
        ApiKey::FindCoordinator => {
            let _ = FindCoordinatorReq::read(body)?;
            write_find_coordinator(out, code, -1, "", -1)
        }
        ApiKey::JoinGroup => {
            let req = JoinGroupReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            out.put_i32(-1);
            write_nullable_string(out, None)?;
            write_string(out, "")?;
            write_string(out, &req.member_id)?;
            write_array_len(out, 0)
        }
        ApiKey::SyncGroup => {
            let _ = SyncGroupReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            write_nullable_bytes(out, Some(&Bytes::new()))
        }
        ApiKey::Heartbeat => {
            let _ = HeartbeatReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            Ok(())
        }
        ApiKey::LeaveGroup => {
            let _ = LeaveGroupReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            write_array_len(out, 0)
        }
        ApiKey::OffsetCommit => {
            let req = OffsetCommitReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, req.topics.len())?;
            for topic in &req.topics {
                write_string(out, &topic.name)?;
                write_array_len(out, topic.partitions.len())?;
                for partition in &topic.partitions {
                    out.put_i32(partition.partition_index);
                    write_error(out, code);
                }
            }
            Ok(())
        }
        ApiKey::OffsetFetch => {
            let _ = OffsetFetchReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, 0)?;
            write_error(out, code);
            Ok(())
        }
        ApiKey::InitProducerId => {
            let _ = InitProducerIdReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            out.put_i64(-1);
            out.put_i16(-1);
            Ok(())
        }
        ApiKey::CreateTopics => {
            let req = CreateTopicsReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, req.topics.len())?;
            for topic in &req.topics {
                write_string(out, &topic.name)?;
                write_error(out, code);
                write_nullable_string(out, Some("injected by the fake broker"))?;
            }
            Ok(())
        }
        ApiKey::DeleteTopics => {
            let req = DeleteTopicsReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, req.topic_names.len())?;
            for name in &req.topic_names {
                write_nullable_string(out, Some(name))?;
                write_error(out, code);
            }
            Ok(())
        }
        other => Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!("fake broker cannot synthesize an error for {other:?}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// ApiVersions
// ---------------------------------------------------------------------------

fn api_versions(out: &mut BytesMut) -> Result<()> {
    let versions = supported_versions();
    write_error(out, ErrorCode::None);
    write_array_len(out, versions.len())?;
    for (api_key, version) in versions {
        api_key.to_i16().encode(out);
        version.encode(out);
        version.encode(out);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

fn metadata(body: &mut Bytes, state: &mut ClusterState, out: &mut BytesMut) -> Result<()> {
    let req = MetadataReq::read(body)?;

    // Requested topics that do not exist are created when the cluster is in
    // auto-create mode and the client asked for it, mirroring a broker with
    // `auto.create.topics.enable=true`.
    let requested: Vec<String> = match &req.topics {
        Some(names) => {
            for name in names {
                if !state.topics.contains_key(name)
                    && state.auto_create_topics
                    && req.allow_auto_topic_creation
                {
                    let partitions = state.default_partitions;
                    state.create_topic(name, partitions);
                }
            }
            names.clone()
        }
        None => {
            let mut all: Vec<String> = state.topics.keys().cloned().collect();
            // Sorted so that "all topics" responses are byte-identical across runs.
            all.sort();
            all
        }
    };

    out.put_i32(0); // throttle_time_ms

    write_array_len(out, state.brokers.len())?;
    for broker in &state.brokers {
        out.put_i32(broker.node_id);
        write_string(out, &broker.host)?;
        out.put_i32(broker.port);
        write_nullable_string(out, broker.rack.as_deref())?;
    }

    write_nullable_string(out, Some(&state.cluster_id))?;
    out.put_i32(state.controller_id);

    write_array_len(out, requested.len())?;
    for name in &requested {
        match state.topics.get(name) {
            None => {
                write_error(out, ErrorCode::UnknownTopicOrPartition);
                write_string(out, name)?;
                out.put_u8(0);
                write_array_len(out, 0)?;
                out.put_i32(i32::MIN);
            }
            Some(topic) => {
                write_error(out, ErrorCode::None);
                write_string(out, name)?;
                out.put_u8(0); // is_internal
                write_array_len(out, topic.partitions.len())?;
                for (index, partition) in topic.partitions.iter().enumerate() {
                    write_error(out, ErrorCode::None);
                    out.put_i32(index as i32);
                    out.put_i32(partition.leader);
                    out.put_i32(partition.leader_epoch);
                    write_i32_array(out, &partition.replicas)?;
                    write_i32_array(out, &partition.isr)?;
                    write_i32_array(out, &[])?; // offline_replicas
                }
                out.put_i32(i32::MIN); // topic_authorized_operations, not requested
            }
        }
    }

    out.put_i32(i32::MIN); // cluster_authorized_operations, not requested
    Ok(())
}

fn write_i32_array(out: &mut BytesMut, values: &[i32]) -> Result<()> {
    write_array_len(out, values.len())?;
    for value in values {
        out.put_i32(*value);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Produce
// ---------------------------------------------------------------------------

fn produce(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ProduceReq::read(body)?;

    // Leaders named by a `CurrentLeader` field in this response. Their
    // addresses have to be repeated at the top level as `NodeEndpoints`, so
    // they are collected while the partitions are written (KIP-951).
    let mut hinted_leaders: Vec<i32> = Vec::new();

    write_compact_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        KafkaString::new(&topic.name).try_encode_compact(out)?;
        write_compact_array_len(out, topic.partitions.len())?;
        for partition in &topic.partitions {
            let leader = state
                .partition(&topic.name, partition.index)
                .map(|p| (p.leader, p.leader_epoch));
            match leader {
                None => write_produce_partition(
                    out,
                    partition.index,
                    ErrorCode::UnknownTopicOrPartition,
                    -1,
                    -1,
                    None,
                )?,
                // The client sent to a broker that no longer leads this
                // partition. A real broker names the new leader alongside the
                // error so the client can retry there directly; without that
                // the client must fall back to a metadata refresh.
                Some((leader_id, leader_epoch)) if leader_id != node_id => {
                    if !hinted_leaders.contains(&leader_id) {
                        hinted_leaders.push(leader_id);
                    }
                    write_produce_partition(
                        out,
                        partition.index,
                        ErrorCode::NotLeaderForPartition,
                        -1,
                        -1,
                        Some((leader_id, leader_epoch)),
                    )?;
                }
                Some(_) => {
                    let (base_offset, log_start_offset) = match (
                        &partition.records,
                        state.partition_mut(&topic.name, partition.index),
                    ) {
                        (Some(records), Some(p)) => (p.append(records), p.log_start_offset),
                        (None, Some(p)) => (p.next_offset, p.log_start_offset),
                        _ => (-1, -1),
                    };
                    write_produce_partition(
                        out,
                        partition.index,
                        ErrorCode::None,
                        base_offset,
                        log_start_offset,
                        None,
                    )?;
                }
            }
        }
        write_empty_tagged_fields(out)?; // topic tagged fields
    }
    out.put_i32(0); // throttle_time_ms
    write_produce_node_endpoints(out, &hinted_leaders, state)
}

/// Write the top-level `NodeEndpoints` tagged field for every leader this
/// response named, or an empty tagged-field section when it named none.
fn write_produce_node_endpoints(
    out: &mut BytesMut,
    leaders: &[i32],
    state: &ClusterState,
) -> Result<()> {
    if leaders.is_empty() {
        return write_empty_tagged_fields(out);
    }
    let endpoints: Vec<(i32, &str, i32)> = leaders
        .iter()
        .filter_map(|id| {
            state
                .brokers
                .iter()
                .find(|b| b.node_id == *id)
                .map(|b| (b.node_id, b.host.as_str(), b.port))
        })
        .collect();
    if endpoints.is_empty() {
        return write_empty_tagged_fields(out);
    }
    write_tagged_fields(out, vec![node_endpoints_field(&endpoints)?])
}

/// Write one partition entry of a Produce v10 response.
///
/// `current_leader` attaches the KIP-951 `CurrentLeader` tagged field naming
/// the node that should have received this write.
fn write_produce_partition(
    out: &mut BytesMut,
    index: i32,
    code: ErrorCode,
    base_offset: i64,
    log_start_offset: i64,
    current_leader: Option<(i32, i32)>,
) -> Result<()> {
    out.put_i32(index);
    write_error(out, code);
    out.put_i64(base_offset);
    out.put_i64(-1); // log_append_time_ms
    out.put_i64(log_start_offset);
    write_compact_array_len(out, 0)?; // record_errors
    write_compact_nullable_string(out, None)?; // error_message
    match current_leader {
        Some((leader_id, leader_epoch)) => {
            write_tagged_fields(out, vec![current_leader_field(leader_id, leader_epoch)])
        }
        None => write_empty_tagged_fields(out),
    }
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

fn fetch(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = FetchReq::read(body)?;

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    out.put_i32(req.session_id);

    write_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        write_string(out, &topic.topic)?;
        write_array_len(out, topic.partitions.len())?;
        for partition in &topic.partitions {
            match state.partition(&topic.topic, partition.partition) {
                None => write_fetch_partition(
                    out,
                    partition.partition,
                    ErrorCode::UnknownTopicOrPartition,
                    0,
                    0,
                    None,
                )?,
                Some(p) if p.leader != node_id => write_fetch_partition(
                    out,
                    partition.partition,
                    ErrorCode::NotLeaderForPartition,
                    p.next_offset,
                    p.log_start_offset,
                    None,
                )?,
                // A client whose leader epoch is behind the broker's has missed a
                // leadership change; a client ahead of the broker is talking to a
                // stale replica. Both are reported so the truncation-detection
                // path in the consumer is reachable without a real cluster.
                Some(p)
                    if partition.current_leader_epoch >= 0
                        && partition.current_leader_epoch != p.leader_epoch =>
                {
                    let code = if partition.current_leader_epoch < p.leader_epoch {
                        ErrorCode::FencedLeaderEpoch
                    } else {
                        ErrorCode::UnknownLeaderEpoch
                    };
                    write_fetch_partition(
                        out,
                        partition.partition,
                        code,
                        p.next_offset,
                        p.log_start_offset,
                        None,
                    )?;
                }
                Some(p) if partition.fetch_offset > p.next_offset => write_fetch_partition(
                    out,
                    partition.partition,
                    ErrorCode::OffsetOutOfRange,
                    p.next_offset,
                    p.log_start_offset,
                    None,
                )?,
                Some(p) => {
                    let records = p.read_from(partition.fetch_offset);
                    write_fetch_partition(
                        out,
                        partition.partition,
                        ErrorCode::None,
                        p.next_offset,
                        p.log_start_offset,
                        Some(&records),
                    )?;
                }
            }
        }
    }
    Ok(())
}

fn write_fetch_partition(
    out: &mut BytesMut,
    partition: i32,
    code: ErrorCode,
    high_watermark: i64,
    log_start_offset: i64,
    records: Option<&Bytes>,
) -> Result<()> {
    out.put_i32(partition);
    write_error(out, code);
    out.put_i64(high_watermark);
    out.put_i64(high_watermark); // last_stable_offset
    out.put_i64(log_start_offset);
    write_array_len(out, 0)?; // aborted_transactions
    out.put_i32(-1); // preferred_read_replica
    write_nullable_bytes(out, records)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ListOffsets
// ---------------------------------------------------------------------------

/// Sentinel timestamp meaning "the earliest retained offset".
const TIMESTAMP_EARLIEST: i64 = -2;
/// Sentinel timestamp meaning "the next offset to be written".
const TIMESTAMP_LATEST: i64 = -1;

fn list_offsets(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ListOffsetsReq::read(body)?;

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        write_string(out, &topic.name)?;
        write_array_len(out, topic.partitions.len())?;
        for partition in &topic.partitions {
            out.put_i32(partition.partition_index);
            match state.partition(&topic.name, partition.partition_index) {
                None => {
                    write_error(out, ErrorCode::UnknownTopicOrPartition);
                    out.put_i64(-1);
                    out.put_i64(-1);
                    out.put_i32(-1);
                }
                Some(p) if p.leader != node_id => {
                    write_error(out, ErrorCode::NotLeaderForPartition);
                    out.put_i64(-1);
                    out.put_i64(-1);
                    out.put_i32(-1);
                }
                Some(p) => {
                    // The fake log has no per-record timestamps to search, so a
                    // timestamp lookup resolves to the earliest retained offset.
                    let offset = match partition.timestamp {
                        TIMESTAMP_LATEST => p.next_offset,
                        TIMESTAMP_EARLIEST => p.log_start_offset,
                        _ => p.log_start_offset,
                    };
                    write_error(out, ErrorCode::None);
                    out.put_i64(-1); // timestamp
                    out.put_i64(offset);
                    out.put_i32(p.leader_epoch);
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// FindCoordinator
// ---------------------------------------------------------------------------

/// `key_type` value for a transaction coordinator lookup.
const COORDINATOR_TYPE_TRANSACTION: i8 = 1;

fn find_coordinator(body: &mut Bytes, state: &mut ClusterState, out: &mut BytesMut) -> Result<()> {
    let req = FindCoordinatorReq::read(body)?;

    let node_id = if req.key_type == COORDINATOR_TYPE_TRANSACTION {
        state.txn_coordinator(&req.key)
    } else {
        state.group_coordinator(&req.key)
    };

    match state.broker(node_id).filter(|b| b.online).cloned() {
        Some(broker) => write_find_coordinator(
            out,
            ErrorCode::None,
            broker.node_id,
            &broker.host,
            broker.port,
        ),
        None => write_find_coordinator(out, ErrorCode::CoordinatorNotAvailable, -1, "", -1),
    }
}

fn write_find_coordinator(
    out: &mut BytesMut,
    code: ErrorCode,
    node_id: i32,
    host: &str,
    port: i32,
) -> Result<()> {
    out.put_i32(0); // throttle_time_ms
    write_error(out, code);
    write_nullable_string(out, None)?; // error_message
    out.put_i32(node_id);
    write_string(out, host)?;
    out.put_i32(port);
    Ok(())
}

/// Reject a group request that arrived at a broker which is not the group's
/// coordinator.
fn coordinator_check(state: &ClusterState, group_id: &str, node_id: i32) -> Option<ErrorCode> {
    let coordinator = state.group_coordinator(group_id);
    if coordinator == node_id {
        None
    } else if state.broker(coordinator).map(|b| b.online) == Some(true) {
        Some(ErrorCode::NotCoordinator)
    } else {
        Some(ErrorCode::CoordinatorNotAvailable)
    }
}

// ---------------------------------------------------------------------------
// Consumer group
// ---------------------------------------------------------------------------

fn join_group(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = JoinGroupReq::read(body)?;

    if let Some(code) = coordinator_check(state, &req.group_id, node_id) {
        out.put_i32(0);
        write_error(out, code);
        out.put_i32(-1);
        write_nullable_string(out, None)?;
        write_string(out, "")?;
        write_string(out, &req.member_id)?;
        return write_array_len(out, 0);
    }

    let member_id = if req.member_id.is_empty() {
        state.next_member_id(&req.group_id)
    } else {
        req.member_id.clone()
    };

    let protocol_name = req.protocols.first().map(|p| p.name.clone());
    let metadata = req
        .protocols
        .first()
        .map(|p| p.metadata.clone())
        .unwrap_or_default();

    let group = state.groups.entry(req.group_id.clone()).or_default();
    group.protocol_type = req.protocol_type.clone();
    group.protocol_name = protocol_name.clone();
    group.generation_id += 1;
    // A single member is enough for the scenarios this harness targets, so each
    // join replaces the membership rather than accumulating members. That also
    // makes the joining member always the leader, which is what drives the
    // client's own assignor.
    group.members = vec![GroupMember {
        member_id: member_id.clone(),
        group_instance_id: req.group_instance_id.clone(),
        metadata: metadata.clone(),
    }];
    group.leader = member_id.clone();
    group.assignments.clear();

    let generation_id = group.generation_id;
    let members = group.members.clone();

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    out.put_i32(generation_id);
    write_nullable_string(out, protocol_name.as_deref())?;
    write_string(out, &member_id)?; // leader
    write_string(out, &member_id)?;
    write_array_len(out, members.len())?;
    for member in &members {
        write_string(out, &member.member_id)?;
        write_nullable_string(out, member.group_instance_id.as_deref())?;
        write_nullable_bytes(out, Some(&member.metadata))?;
    }
    Ok(())
}

fn sync_group(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = SyncGroupReq::read(body)?;

    if let Some(code) = coordinator_check(state, &req.group_id, node_id) {
        out.put_i32(0);
        write_error(out, code);
        return write_nullable_bytes(out, Some(&Bytes::new()));
    }

    let group = state.groups.entry(req.group_id.clone()).or_default();
    if group.generation_id != req.generation_id {
        out.put_i32(0);
        write_error(out, ErrorCode::IllegalGeneration);
        return write_nullable_bytes(out, Some(&Bytes::new()));
    }

    for assignment in &req.assignments {
        group
            .assignments
            .insert(assignment.member_id.clone(), assignment.assignment.clone());
    }
    let assignment = group
        .assignments
        .get(&req.member_id)
        .cloned()
        .unwrap_or_default();

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    write_nullable_bytes(out, Some(&assignment))
}

fn heartbeat(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = HeartbeatReq::read(body)?;

    let code = match coordinator_check(state, &req.group_id, node_id) {
        Some(code) => code,
        None => match state.groups.get(&req.group_id) {
            Some(group) if group.generation_id != req.generation_id => ErrorCode::IllegalGeneration,
            Some(group) if !group.members.iter().any(|m| m.member_id == req.member_id) => {
                ErrorCode::UnknownMemberId
            }
            _ => ErrorCode::None,
        },
    };

    out.put_i32(0); // throttle_time_ms
    write_error(out, code);
    Ok(())
}

fn leave_group(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = LeaveGroupReq::read(body)?;

    if let Some(code) = coordinator_check(state, &req.group_id, node_id) {
        out.put_i32(0);
        write_error(out, code);
        return write_array_len(out, 0);
    }

    if let Some(group) = state.groups.get_mut(&req.group_id) {
        group
            .members
            .retain(|m| !req.members.iter().any(|(id, _)| *id == m.member_id));
    }

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    write_array_len(out, req.members.len())?;
    for (member_id, instance) in &req.members {
        write_string(out, member_id)?;
        write_nullable_string(out, instance.as_deref())?;
        write_error(out, ErrorCode::None);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Offsets
// ---------------------------------------------------------------------------

fn offset_commit(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = OffsetCommitReq::read(body)?;

    // A commit is rejected if it is misrouted, or if the member has been
    // rebalanced out from under it. Both make the client re-join rather than
    // silently committing against a stale generation.
    let rejection = coordinator_check(state, &req.group_id, node_id).or_else(|| {
        let group = state.groups.get(&req.group_id)?;
        // `generation_id == -1` is how a consumer with no group commits.
        if req.generation_id >= 0 && group.generation_id != req.generation_id {
            Some(ErrorCode::IllegalGeneration)
        } else if !req.member_id.is_empty()
            && !group.members.iter().any(|m| m.member_id == req.member_id)
        {
            Some(ErrorCode::UnknownMemberId)
        } else {
            None
        }
    });

    if rejection.is_none() {
        let group = state.groups.entry(req.group_id.clone()).or_default();
        for topic in &req.topics {
            for partition in &topic.partitions {
                group.offsets.insert(
                    (topic.name.clone(), partition.partition_index),
                    CommittedOffset {
                        offset: partition.committed_offset,
                        leader_epoch: partition.committed_leader_epoch,
                        metadata: partition.committed_metadata.clone(),
                    },
                );
            }
        }
    }

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        write_string(out, &topic.name)?;
        write_array_len(out, topic.partitions.len())?;
        for partition in &topic.partitions {
            out.put_i32(partition.partition_index);
            write_error(out, rejection.unwrap_or(ErrorCode::None));
        }
    }
    Ok(())
}

/// Offset returned for a partition the group has never committed.
const NO_COMMITTED_OFFSET: i64 = -1;

fn offset_fetch(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = OffsetFetchReq::read(body)?;

    if let Some(code) = coordinator_check(state, &req.group_id, node_id) {
        out.put_i32(0);
        write_array_len(out, 0)?;
        write_error(out, code);
        return Ok(());
    }

    let group = state.groups.entry(req.group_id.clone()).or_default();

    // A null topics array means "everything this group has committed".
    let requested: Vec<(String, Vec<i32>)> = match req.topics {
        Some(topics) => topics,
        None => {
            let mut by_topic: std::collections::BTreeMap<String, Vec<i32>> = Default::default();
            for (topic, partition) in group.offsets.keys() {
                by_topic.entry(topic.clone()).or_default().push(*partition);
            }
            by_topic
                .into_iter()
                .map(|(topic, mut parts)| {
                    parts.sort_unstable();
                    (topic, parts)
                })
                .collect()
        }
    };

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, requested.len())?;
    for (name, partitions) in &requested {
        write_string(out, name)?;
        write_array_len(out, partitions.len())?;
        for partition in partitions {
            out.put_i32(*partition);
            match group.offsets.get(&(name.clone(), *partition)) {
                Some(committed) => {
                    out.put_i64(committed.offset);
                    out.put_i32(committed.leader_epoch);
                    write_nullable_string(out, committed.metadata.as_deref())?;
                    write_error(out, ErrorCode::None);
                }
                None => {
                    out.put_i64(NO_COMMITTED_OFFSET);
                    out.put_i32(-1);
                    write_nullable_string(out, None)?;
                    write_error(out, ErrorCode::None);
                }
            }
        }
    }
    write_error(out, ErrorCode::None); // top-level error_code
    Ok(())
}

// ---------------------------------------------------------------------------
// Producer IDs
// ---------------------------------------------------------------------------

fn init_producer_id(body: &mut Bytes, state: &mut ClusterState, out: &mut BytesMut) -> Result<()> {
    let _ = InitProducerIdReq::read(body)?;
    let (producer_id, producer_epoch) = state.allocate_producer_id();

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    out.put_i64(producer_id);
    out.put_i16(producer_epoch);
    Ok(())
}

// ---------------------------------------------------------------------------
// Topic administration
// ---------------------------------------------------------------------------

fn create_topics(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = CreateTopicsReq::read(body)?;

    // CreateTopics is controller-only. Answering NOT_CONTROLLER when it lands
    // anywhere else is what exercises the admin client's controller
    // re-resolution path.
    if state.controller_id != node_id {
        out.put_i32(0);
        write_array_len(out, req.topics.len())?;
        for topic in &req.topics {
            write_string(out, &topic.name)?;
            write_error(out, ErrorCode::NotController);
            write_nullable_string(out, Some("this broker is not the controller"))?;
        }
        return Ok(());
    }

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        let partitions = if topic.num_partitions > 0 {
            topic.num_partitions
        } else {
            state.default_partitions
        };
        let (code, message) = if state.topics.contains_key(&topic.name) {
            (ErrorCode::TopicAlreadyExists, Some("topic already exists"))
        } else {
            if !req.validate_only {
                state.create_topic(&topic.name, partitions);
            }
            (ErrorCode::None, None)
        };
        write_string(out, &topic.name)?;
        write_error(out, code);
        write_nullable_string(out, message)?;
    }
    Ok(())
}

fn delete_topics(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = DeleteTopicsReq::read(body)?;

    if state.controller_id != node_id {
        out.put_i32(0);
        write_array_len(out, req.topic_names.len())?;
        for name in &req.topic_names {
            write_nullable_string(out, Some(name))?;
            write_error(out, ErrorCode::NotController);
        }
        return Ok(());
    }

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, req.topic_names.len())?;
    for name in &req.topic_names {
        let code = if state.topics.remove(name).is_some() {
            ErrorCode::None
        } else {
            ErrorCode::UnknownTopicOrPartition
        };
        write_nullable_string(out, Some(name))?;
        write_error(out, code);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::{MetadataResponse, VersionedDecode};
    use bytes::Buf;

    /// Advertising the same API twice would let the client negotiate a version
    /// no codec here was written against, so the list must be a clean mapping.
    #[test]
    fn each_api_is_advertised_exactly_once() {
        let versions = supported_versions();
        let mut keys: Vec<i16> = versions.iter().map(|(k, _)| k.to_i16()).collect();
        keys.sort_unstable();
        let unique = {
            let mut u = keys.clone();
            u.dedup();
            u
        };
        assert_eq!(keys, unique, "an API is advertised more than once");

        assert!(
            versions.iter().any(|(k, _)| *k == ApiKey::ApiVersions),
            "ApiVersions must be advertised or no client can complete a handshake"
        );
    }

    /// The Metadata writer here and the client's Metadata v8 reader must agree.
    /// Round-tripping through the real decoder is the check that keeps them in
    /// step as either side changes.
    #[test]
    fn metadata_response_round_trips_through_the_client_decoder() {
        let mut state = ClusterState::new(2);
        state.brokers[0].port = 9092;
        state.brokers[1].port = 9093;
        state.controller_id = 1;
        state.create_topic("orders", 2);

        // A v8 request body for a single topic, as the client would encode it.
        let mut body = BytesMut::new();
        body.put_i32(1);
        write_string(&mut body, "orders").unwrap();
        body.put_u8(0); // allow_auto_topic_creation
        body.put_u8(0);
        body.put_u8(0);
        let mut body = body.freeze();

        let mut out = BytesMut::new();
        metadata(&mut body, &mut state, &mut out).unwrap();

        let mut encoded = out.freeze();
        let decoded = MetadataResponse::decode_versioned(8, &mut encoded).unwrap();
        assert_eq!(encoded.remaining(), 0, "writer emitted trailing bytes");

        assert_eq!(decoded.controller_id, 1);
        assert_eq!(decoded.brokers.len(), 2);
        assert_eq!(decoded.cluster_id.as_deref(), Some("krafka-fake-cluster"));
        let topic = decoded.find_topic("orders").unwrap();
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(topic.error_code, ErrorCode::None);
    }

    /// A synthesized error must still be a structurally valid response.
    #[test]
    fn synthesized_metadata_error_still_decodes() {
        let mut body = BytesMut::new();
        body.put_i32(1);
        write_string(&mut body, "orders").unwrap();
        body.put_u8(0);
        body.put_u8(0);
        body.put_u8(0);
        let mut body = body.freeze();

        let mut out = BytesMut::new();
        dispatch_error(
            ApiKey::Metadata,
            &mut body,
            ErrorCode::NotController,
            &mut out,
        )
        .unwrap();

        let mut encoded = out.freeze();
        let decoded = MetadataResponse::decode_versioned(8, &mut encoded).unwrap();
        assert_eq!(encoded.remaining(), 0);
        assert_eq!(decoded.topics[0].error_code, ErrorCode::NotController);
    }
}
