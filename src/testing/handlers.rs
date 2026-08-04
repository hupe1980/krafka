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

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{ErrorCode, Result};
use crate::protocol::ApiKey;
use crate::protocol::{Encode, KafkaString, TaggedField, TryEncode};

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
        // The value here is ignored for ApiVersions: it is advertised as the
        // range in `API_VERSIONS_RANGE` instead. See that constant.
        (ApiKey::ApiVersions, 0),
        // v12 is the lowest version carrying topic UUIDs in a form KIP-848
        // can use (v10 forces an all-zero UUID in the *request*, v12 is where
        // the client may look topics up by ID). Serving it also means the
        // flexible Metadata codec is exercised by every test here, which v8
        // never reached.
        (ApiKey::Metadata, 12),
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
        // KIP-848. v1 is the only version krafka negotiates, and the only one
        // carrying the client-generated member ID (KIP-1082).
        (ApiKey::ConsumerGroupHeartbeat, 1),
        (ApiKey::InitProducerId, 1),
        (ApiKey::CreateTopics, 4),
        (ApiKey::DeleteTopics, 3),
        // KIP-932 share groups. v1 is the stable version; v2 (KIP-1206
        // ShareAcquireMode, KIP-1222 renew-ack) is deliberately not advertised
        // because neither an acquire mode nor a lock timer is modelled here,
        // and advertising a version whose semantics the fake broker does not
        // implement would make tests pass for the wrong reason.
        (ApiKey::ShareGroupHeartbeat, 1),
        (ApiKey::ShareFetch, 1),
        (ApiKey::ShareAcknowledge, 1),
        // KIP-584. v2 is the Kafka 4.0 version that dropped the per-feature
        // `Results` array; overriding this down to v0 is how a test reaches
        // the client's "validate_only needs v1+" refusal.
        (ApiKey::UpdateFeatures, 2),
        // KIP-1071, describe half only — see `streams_group_describe`.
        (ApiKey::StreamsGroupDescribe, 0),
    ]
}

/// Serve one request, writing the response body (no header) into `out`.
///
/// `node_id` is the broker the request arrived at, which is what lets the
/// handlers detect misrouted requests.
pub(crate) fn dispatch(
    api_key: ApiKey,
    api_version: i16,
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    match api_key {
        ApiKey::ApiVersions => api_versions(api_version, state, out),
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
        ApiKey::ConsumerGroupHeartbeat => consumer_group_heartbeat(body, node_id, state, out),
        ApiKey::InitProducerId => init_producer_id(body, state, out),
        ApiKey::CreateTopics => create_topics(body, node_id, state, out),
        ApiKey::DeleteTopics => delete_topics(body, node_id, state, out),
        ApiKey::ShareGroupHeartbeat => share_group_heartbeat(body, node_id, state, out),
        ApiKey::ShareFetch => share_fetch(body, api_version, node_id, state, out),
        ApiKey::ShareAcknowledge => share_acknowledge(body, api_version, node_id, state, out),
        ApiKey::UpdateFeatures => update_features(body, api_version, node_id, state, out),
        ApiKey::StreamsGroupDescribe => streams_group_describe(body, node_id, state, out),
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
    api_version: i16,
    body: &mut Bytes,
    code: ErrorCode,
    out: &mut BytesMut,
) -> Result<()> {
    match api_key {
        ApiKey::ApiVersions => {
            // An injected ApiVersions error still has to be *shaped* like a
            // real broker's, or the client reports a malformed frame instead
            // of the error under test.
            //
            // UNSUPPORTED_VERSION is the special case: a broker answering it
            // always uses the **v0** body layout, whatever version was asked
            // for — that is what makes the reply parseable by a client that
            // guessed too high — and names the range it does support so the
            // retry is directed rather than a blind walk down.
            if code == ErrorCode::UnsupportedVersion {
                let (min_version, max_version) = API_VERSIONS_RANGE;
                write_error(out, code);
                write_array_len(out, 1)?;
                ApiKey::ApiVersions.to_i16().encode(out);
                min_version.encode(out);
                max_version.encode(out);
                return Ok(());
            }

            write_error(out, code);
            if api_version >= 3 {
                write_compact_array_len(out, 0)?;
                out.put_i32(0); // throttle_time_ms
                write_empty_tagged_fields(out)
            } else {
                write_array_len(out, 0)?;
                if api_version >= 1 {
                    out.put_i32(0); // throttle_time_ms
                }
                Ok(())
            }
        }
        ApiKey::ConsumerGroupHeartbeat => {
            // Every KIP-848 error is top-level; the member epoch echoed back is
            // what a fenced member is expected to reset to.
            let _req = ConsumerGroupHeartbeatReq::read(body)?;
            out.put_i32(0); // throttle_time_ms
            write_error(out, code);
            write_compact_nullable_string(out, Some(&format!("injected {code:?}")))?;
            write_compact_nullable_string(out, None)?; // member_id
            out.put_i32(0); // member_epoch
            out.put_i32(HEARTBEAT_INTERVAL_MS);
            write_heartbeat_assignment(out, None)?;
            write_empty_tagged_fields(out)
        }
        ApiKey::Metadata => {
            let req = MetadataReq::read_v12(body)?;
            out.put_i32(0); // throttle_time_ms
            write_compact_array_len(out, 0)?; // brokers
            write_compact_nullable_string(out, None)?; // cluster_id
            out.put_i32(-1); // controller_id
            let names = req.topics.unwrap_or_default();
            write_compact_array_len(out, names.len())?;
            for name in &names {
                write_error(out, code);
                write_compact_nullable_string(out, Some(name))?;
                out.put_slice(&[0u8; 16]); // topic_id
                out.put_u8(0); // is_internal
                write_compact_array_len(out, 0)?; // partitions
                out.put_i32(i32::MIN); // topic_authorized_operations
                write_empty_tagged_fields(out)?;
            }
            // v12 drops cluster_authorized_operations.
            write_empty_tagged_fields(out)
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

/// Range of `ApiVersions` versions the fake broker itself speaks.
///
/// Every other entry in [`supported_versions`] pins `min == max`, because the
/// client negotiates those against this response. `ApiVersions` cannot work
/// that way — it *is* the negotiation — so the client probes with its ceiling
/// and falls back on `UNSUPPORTED_VERSION`. Advertising a genuine range here is
/// what lets the fake broker exercise both outcomes.
///
/// The ceiling of 4 is deliberately the highest version a *released* Kafka
/// supports, so the fake broker rejects exactly what a real one would.
pub(crate) const API_VERSIONS_RANGE: (i16, i16) = (0, 4);

fn api_versions(request_version: i16, state: &ClusterState, out: &mut BytesMut) -> Result<()> {
    let (min_version, max_version) = API_VERSIONS_RANGE;

    if request_version < min_version || request_version > max_version {
        // A real broker answers an out-of-range ApiVersions request with a
        // **v0-format** body — that is mandated precisely so a client that
        // guessed too high can still parse the reply — carrying
        // UNSUPPORTED_VERSION and the range it does support.
        write_error(out, ErrorCode::UnsupportedVersion);
        write_array_len(out, 1)?;
        ApiKey::ApiVersions.to_i16().encode(out);
        min_version.encode(out);
        max_version.encode(out);
        return Ok(());
    }

    let flexible = request_version >= 3;
    let versions = supported_versions();

    write_error(out, ErrorCode::None);
    if flexible {
        write_compact_array_len(out, versions.len())?;
    } else {
        write_array_len(out, versions.len())?;
    }
    for (api_key, version) in versions {
        let (lo, hi) = if let Some(&range) = state.api_version_overrides.get(&api_key) {
            range
        } else if api_key == ApiKey::ApiVersions {
            (min_version, max_version)
        } else {
            (version, version)
        };
        api_key.to_i16().encode(out);
        lo.encode(out);
        hi.encode(out);
        if flexible {
            write_empty_tagged_fields(out)?;
        }
    }
    // throttle_time_ms exists from v1 onward.
    if request_version >= 1 {
        out.put_i32(0);
    }
    if flexible {
        write_feature_tagged_fields(state, out)?;
    }
    Ok(())
}

/// Write the KIP-584 feature tagged fields of an `ApiVersions` v3+ response.
///
/// A cluster with no finalized features writes an empty section, which is the
/// case a client must tolerate and the one this broker used to model
/// unconditionally. Once `UpdateFeatures` has finalized something, the fields
/// are emitted — which is what lets `AdminClient::describe_features()` be
/// tested against what `update_features()` actually applied, rather than each
/// being asserted in isolation.
fn write_feature_tagged_fields(state: &ClusterState, out: &mut BytesMut) -> Result<()> {
    if state.finalized_features.is_empty() {
        return write_empty_tagged_fields(out);
    }

    let mut features: Vec<(&String, &i16)> = state.finalized_features.iter().collect();
    features.sort_by_key(|(name, _)| (*name).clone());

    // Tag 0 — SupportedFeatures: what this broker *can* run. The fake broker
    // supports every finalized feature from 1 up to its finalized level, which
    // is the only relationship a real cluster guarantees.
    let mut supported = BytesMut::new();
    write_compact_array_len(&mut supported, features.len())?;
    for (name, level) in &features {
        write_compact_string(&mut supported, name)?;
        supported.put_i16(1); // min_version
        supported.put_i16(**level); // max_version
        write_empty_tagged_fields(&mut supported)?;
    }

    // Tag 1 — FinalizedFeaturesEpoch, as a bare i64. A client that reads a
    // negative epoch must ignore tag 2 entirely, so this has to be >= 0 for
    // the finalized features to be visible at all.
    let mut epoch = BytesMut::new();
    epoch.put_i64(state.finalized_features_epoch);

    // Tag 2 — FinalizedFeatures. Note the field order: max level precedes min
    // level here, the reverse of SupportedFeatures. Getting that backwards
    // produces a response that decodes without error and means the wrong
    // thing.
    let mut finalized = BytesMut::new();
    write_compact_array_len(&mut finalized, features.len())?;
    for (name, level) in &features {
        write_compact_string(&mut finalized, name)?;
        finalized.put_i16(**level); // max_version_level
        finalized.put_i16(1); // min_version_level
        write_empty_tagged_fields(&mut finalized)?;
    }

    write_tagged_fields(
        out,
        vec![
            TaggedField {
                tag: 0,
                data: supported.freeze(),
            },
            TaggedField {
                tag: 1,
                data: epoch.freeze(),
            },
            TaggedField {
                tag: 2,
                data: finalized.freeze(),
            },
        ],
    )
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

fn metadata(body: &mut Bytes, state: &mut ClusterState, out: &mut BytesMut) -> Result<()> {
    let req = MetadataReq::read_v12(body)?;

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

    write_compact_array_len(out, state.brokers.len())?;
    for broker in &state.brokers {
        out.put_i32(broker.node_id);
        write_compact_nullable_string(out, Some(&broker.host))?;
        out.put_i32(broker.port);
        write_compact_nullable_string(out, broker.rack.as_deref())?;
        write_empty_tagged_fields(out)?;
    }

    write_compact_nullable_string(out, Some(&state.cluster_id))?;
    out.put_i32(state.controller_id);

    write_compact_array_len(out, requested.len())?;
    for name in &requested {
        match state.topics.get(name) {
            None => {
                write_error(out, ErrorCode::UnknownTopicOrPartition);
                write_compact_nullable_string(out, Some(name))?;
                out.put_slice(&[0u8; 16]); // topic_id: unknown topic has none
                out.put_u8(0); // is_internal
                write_compact_array_len(out, 0)?;
                out.put_i32(i32::MIN); // topic_authorized_operations
                write_empty_tagged_fields(out)?;
            }
            Some(topic) => {
                write_error(out, ErrorCode::None);
                write_compact_nullable_string(out, Some(name))?;
                // The UUID is what makes KIP-848 assignments resolvable: the
                // coordinator names topics by ID, and the client maps them
                // back through this field.
                out.put_slice(&topic.topic_id);
                out.put_u8(0); // is_internal
                write_compact_array_len(out, topic.partitions.len())?;
                for (index, partition) in topic.partitions.iter().enumerate() {
                    write_error(out, ErrorCode::None);
                    out.put_i32(index as i32);
                    out.put_i32(partition.leader);
                    out.put_i32(partition.leader_epoch);
                    write_compact_i32_array(out, &partition.replicas)?;
                    write_compact_i32_array(out, &partition.isr)?;
                    write_compact_i32_array(out, &[])?; // offline_replicas
                    write_empty_tagged_fields(out)?;
                }
                out.put_i32(i32::MIN); // topic_authorized_operations, not requested
                write_empty_tagged_fields(out)?;
            }
        }
    }

    // v12 drops cluster_authorized_operations (it existed only in v8-v10).
    write_empty_tagged_fields(out)
}

fn write_compact_i32_array(out: &mut BytesMut, values: &[i32]) -> Result<()> {
    write_compact_array_len(out, values.len())?;
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

/// Serve a `Fetch` whose record bytes are corrupt, so the batch fails CRC.
///
/// Everything else about the response is well-formed; the damage is confined
/// to the inside of the record batch, which is the only way to exercise the
/// client's batch-decode failure path rather than its malformed-frame path.
pub(crate) fn dispatch_corrupt(
    api_key: ApiKey,
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    match api_key {
        ApiKey::Fetch => fetch_inner(body, node_id, state, out, true),
        other => Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!(
                "fake broker models record corruption only for Fetch, not {other:?}; \
                 asserting on Control::CorruptRecords here would prove nothing"
            ),
        )),
    }
}

/// Flip one byte inside the CRC-covered region of a record batch.
///
/// The v2 batch header is `base_offset(8) | batch_length(4) |
/// partition_leader_epoch(4) | magic(1) | crc(4)`, so the CRC covers
/// everything from byte 21 on. Mutating a byte there — and only there — leaves
/// `batch_length` and the magic byte valid, so the batch still *frames*
/// correctly and the client reaches the CRC check rather than bailing out
/// earlier on a structural error.
fn corrupt_record_bytes(records: &Bytes) -> Bytes {
    const CRC_REGION_START: usize = 21;
    if records.len() <= CRC_REGION_START {
        // Nothing to corrupt; hand the bytes back unchanged rather than
        // fabricating a differently-shaped failure.
        return records.clone();
    }
    let mut bytes = records.to_vec();
    bytes[CRC_REGION_START] ^= 0xFF;
    Bytes::from(bytes)
}

fn fetch(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    fetch_inner(body, node_id, state, out, false)
}

fn fetch_inner(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
    corrupt: bool,
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
                    let records = if corrupt {
                        corrupt_record_bytes(&records)
                    } else {
                        records
                    };
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
// ConsumerGroupHeartbeat (KIP-848)
// ---------------------------------------------------------------------------

/// Heartbeat interval the fake coordinator advertises.
///
/// Short enough that a test does not wait long for the background heartbeat
/// task to tick, long enough not to saturate the loopback listener.
pub(crate) const HEARTBEAT_INTERVAL_MS: i32 = 1_000;

/// Acquisition-lock timeout reported in `ShareFetch` responses.
///
/// The fake broker never expires a lock — a record stays acquired until the
/// client acknowledges it. This value is what a client would *see*, so a test
/// can observe the field being carried; it is not a timer.
pub(crate) const ACQUISITION_LOCK_TIMEOUT_MS: i32 = 30_000;

/// Serve a KIP-848 `ConsumerGroupHeartbeat`.
///
/// This models the parts of the coordinator a *client* has to get right, and
/// deliberately not the parts it does not observe:
///
/// - **Epoch ownership is the coordinator's.** A member heartbeating with an
///   epoch other than the one the coordinator holds is fenced with
///   `FENCED_MEMBER_EPOCH`, exactly as KIP-848 specifies. That is what makes
///   the client's "give up all partitions and rejoin at epoch 0" path
///   reachable without a real cluster.
/// - **Assignment is server-side.** The member sends no assignment; the
///   coordinator computes one and the member reconciles to it. Here every
///   partition of every subscribed topic goes to the single member, which is
///   the correct answer for a one-member group and keeps the test surface
///   about the *protocol* rather than about assignor arithmetic.
/// - **Leaving is an epoch, not an API.** `-1` (and `-2` for a static
///   member's temporary leave) are heartbeats, not a separate request.
///
/// Multi-member reconciliation — the genuinely hard half of KIP-848, where the
/// coordinator drives members through revoke/epoch-bump/assign in lockstep —
/// is *not* modelled. Tests here must not be read as validating it.
fn consumer_group_heartbeat(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ConsumerGroupHeartbeatReq::read(body)?;

    // Route check: a heartbeat must reach the group's coordinator.
    if state.group_coordinator(&req.group_id) != node_id {
        return write_heartbeat_error(out, ErrorCode::NotCoordinator, None, 0);
    }

    // Leave (-1) and static temporary leave (-2) are heartbeats, not an API.
    if req.member_epoch < 0 {
        if let Some(group) = state.groups.get_mut(&req.group_id) {
            group.consumer_members.remove(&req.member_id);
            group.group_epoch += 1;
        }
        out.put_i32(0); // throttle_time_ms
        write_error(out, ErrorCode::None);
        write_compact_nullable_string(out, None)?; // error_message
        write_compact_nullable_string(out, Some(&req.member_id))?;
        out.put_i32(req.member_epoch); // echo the leave epoch back
        out.put_i32(HEARTBEAT_INTERVAL_MS);
        write_heartbeat_assignment(out, None)?;
        return write_empty_tagged_fields(out);
    }

    // Snapshot the topic layout before taking a mutable borrow of the group.
    let partition_counts: HashMap<String, i32> = state
        .topics
        .iter()
        .map(|(name, t)| (name.clone(), t.partitions.len() as i32))
        .collect();
    let topic_ids: HashMap<String, [u8; 16]> = state
        .topics
        .iter()
        .map(|(name, t)| (name.clone(), t.topic_id))
        .collect();

    let group = state.groups.entry(req.group_id.clone()).or_default();
    let known = group.consumer_members.get(&req.member_id).cloned();

    // Epoch validation. A joining member (epoch 0) is always accepted: that is
    // how a fenced member re-registers. An established member must present the
    // epoch the coordinator last handed it.
    if let Some(existing) = &known
        && req.member_epoch != 0
        && req.member_epoch != existing.member_epoch
    {
        return write_heartbeat_error(out, ErrorCode::FencedMemberEpoch, Some(&req.member_id), 0);
    }

    // A member the coordinator has never seen, heartbeating at a non-zero
    // epoch, is a stale member from a previous incarnation of the group.
    if known.is_none() && req.member_epoch != 0 {
        return write_heartbeat_error(out, ErrorCode::UnknownMemberId, Some(&req.member_id), 0);
    }

    // `None` means "unchanged since my last heartbeat".
    let subscribed = req
        .subscribed_topic_names
        .clone()
        .or_else(|| known.as_ref().map(|m| m.subscribed_topics.clone()))
        .unwrap_or_default();

    // The member reports what it *currently owns*. This is the acknowledgement
    // half of reconciliation: until it arrives, the coordinator must assume the
    // member is still holding whatever it held before, and must not hand those
    // partitions to anyone else.
    let reported_owned: Option<HashMap<String, Vec<i32>>> =
        req.topic_partitions.as_ref().map(|tps| {
            let mut owned: HashMap<String, Vec<i32>> = HashMap::new();
            for tp in tps {
                if let Some((name, _)) = topic_ids.iter().find(|(_, id)| **id == tp.topic_id) {
                    owned.insert(name.clone(), tp.partitions.clone());
                }
            }
            owned
        });

    let is_new = known.is_none();
    let rejoining = known.is_some() && req.member_epoch == 0;
    let subscription_changed = known
        .as_ref()
        .is_some_and(|m| m.subscribed_topics != subscribed);
    if is_new || rejoining || subscription_changed {
        group.group_epoch += 1;
    }
    let group_epoch = group.group_epoch;

    {
        let member = group
            .consumer_members
            .entry(req.member_id.clone())
            .or_default();
        member.instance_id = req.instance_id.clone();
        member.subscribed_topics = subscribed.clone();
        if let Some(owned) = reported_owned {
            member.owned = owned;
        }
        if is_new || rejoining {
            // A (re-)joining member owns nothing until the coordinator grants
            // it something.
            member.owned.clear();
            member.assignment.clear();
        }
    }

    // ── Target assignment ────────────────────────────────────────────────
    //
    // Every partition of every subscribed topic, distributed round-robin over
    // the members that subscribe to it, in a deterministic order. Assignor
    // sophistication is not the point here; *reconciliation* is.
    let targets = compute_target_assignment(group, &partition_counts);

    // ── Reconciliation ───────────────────────────────────────────────────
    //
    // KIP-848 revokes before it assigns, in two steps separated by a
    // heartbeat:
    //
    //  1. If the member owns partitions that are not in its target, send it
    //     only `owned ∩ target`. Its epoch does **not** advance; the
    //     coordinator waits for the member to report the reduced set back.
    //  2. Once the member owns nothing outside its target, grant the target —
    //     but only the partitions no *other* member still owns.
    //
    // Step 2's restriction is the whole safety property: a partition moves to
    // its new owner strictly after the previous owner has confirmed releasing
    // it, so no two members ever believe they own it at once.
    let empty_target: HashMap<String, Vec<i32>> = HashMap::new();
    let target = targets.get(&req.member_id).unwrap_or(&empty_target);
    let member_owned = group
        .consumer_members
        .get(&req.member_id)
        .map(|m| m.owned.clone())
        .unwrap_or_default();

    let owns_beyond_target = member_owned.iter().any(|(topic, partitions)| {
        let keep = target.get(topic);
        partitions
            .iter()
            .any(|p| !keep.is_some_and(|k| k.contains(p)))
    });

    let held_elsewhere: HashMap<String, Vec<i32>> = {
        let mut held: HashMap<String, Vec<i32>> = HashMap::new();
        for (id, m) in &group.consumer_members {
            if *id == req.member_id {
                continue;
            }
            for (topic, partitions) in &m.owned {
                held.entry(topic.clone()).or_default().extend(partitions);
            }
        }
        held
    };

    let (granted, advance_epoch) = if owns_beyond_target {
        // Step 1: revoke. Hand back only what the member keeps.
        let mut keep: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partitions) in &member_owned {
            if let Some(target_partitions) = target.get(topic) {
                let retained: Vec<i32> = partitions
                    .iter()
                    .copied()
                    .filter(|p| target_partitions.contains(p))
                    .collect();
                if !retained.is_empty() {
                    keep.insert(topic.clone(), retained);
                }
            }
        }
        (keep, false)
    } else {
        // Step 2: assign, minus anything a peer has not released yet.
        let mut grant: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partitions) in target {
            let blocked = held_elsewhere.get(topic);
            let available: Vec<i32> = partitions
                .iter()
                .copied()
                .filter(|p| !blocked.is_some_and(|b| b.contains(p)))
                .collect();
            if !available.is_empty() {
                grant.insert(topic.clone(), available);
            }
        }
        let complete = grant == *target;
        (grant, complete)
    };

    // One mutable borrow for the whole state update. `or_default()` rather than
    // an `expect`: the entry was inserted above, but this file is compiled as
    // library code under the `test-broker` feature, where the crate denies
    // panicking constructs — and a fake broker that panics takes the client's
    // test process with it instead of failing an assertion.
    let (member_epoch, send_assignment) = {
        let member = group
            .consumer_members
            .entry(req.member_id.clone())
            .or_default();
        if advance_epoch || member.member_epoch == 0 {
            // A joining member has to leave epoch 0 or it would look like a
            // rejoin on every heartbeat and never converge.
            member.member_epoch = group_epoch;
        }
        let changed = member.assignment != granted;
        member.assignment = granted.clone();
        if changed {
            member.assignment_dirty = true;
        }

        // What the coordinator believes the member holds only ever *grows*
        // here; it shrinks solely when the member reports a smaller set.
        //
        // That asymmetry is the point. Granting a partition means the member
        // will start consuming it, so the coordinator must count it as held
        // immediately or it would hand the same partition to a second member.
        // Revocation is the opposite: the coordinator has *asked* the member
        // to let go, but until the member says it has, assuming so would
        // release the partition to its new owner while the old one is still
        // reading it — exactly the split-brain reconciliation exists to
        // prevent.
        for (topic, partitions) in &granted {
            let held = member.owned.entry(topic.clone()).or_default();
            for p in partitions {
                if !held.contains(p) {
                    held.push(*p);
                }
            }
            held.sort_unstable();
        }

        // The assignment field is null when nothing changed — that is how the
        // coordinator says "keep what you have", and a client that treats null
        // as "revoke everything" would break against a real broker.
        let dirty = member.assignment_dirty || is_new || rejoining || subscription_changed;
        member.assignment_dirty = false;

        (member.member_epoch, dirty)
    };

    let wire_assignment: Vec<HeartbeatTopicPartitions> = granted
        .iter()
        .filter_map(|(topic, partitions)| {
            topic_ids.get(topic).map(|id| HeartbeatTopicPartitions {
                topic_id: *id,
                partitions: partitions.clone(),
            })
        })
        .collect();

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    write_compact_nullable_string(out, None)?; // error_message
    write_compact_nullable_string(out, Some(&req.member_id))?;
    out.put_i32(member_epoch);
    out.put_i32(HEARTBEAT_INTERVAL_MS);
    write_heartbeat_assignment(
        out,
        if send_assignment {
            Some(&wire_assignment)
        } else {
            None
        },
    )?;
    write_empty_tagged_fields(out)
}

/// Distribute every partition of every subscribed topic across the members that
/// subscribe to it, round-robin in member-ID order.
///
/// Deterministic on purpose: a test that asserts on a specific split needs the
/// same answer every run.
fn compute_target_assignment(
    group: &super::state::GroupState,
    partition_counts: &HashMap<String, i32>,
) -> HashMap<String, HashMap<String, Vec<i32>>> {
    let mut member_ids: Vec<&String> = group.consumer_members.keys().collect();
    member_ids.sort();

    let mut targets: HashMap<String, HashMap<String, Vec<i32>>> = member_ids
        .iter()
        .map(|id| ((*id).clone(), HashMap::new()))
        .collect();

    // Every topic any member subscribes to, in a stable order.
    let mut topics: Vec<&String> = group
        .consumer_members
        .values()
        .flat_map(|m| m.subscribed_topics.iter())
        .collect();
    topics.sort();
    topics.dedup();

    for topic in topics {
        let subscribers: Vec<&String> = member_ids
            .iter()
            .copied()
            .filter(|id| {
                group
                    .consumer_members
                    .get(*id)
                    .is_some_and(|m| m.subscribed_topics.contains(topic))
            })
            .collect();
        if subscribers.is_empty() {
            continue;
        }
        let count = partition_counts.get(topic).copied().unwrap_or(0);
        for partition in 0..count {
            let owner = subscribers[(partition as usize) % subscribers.len()];
            targets
                .entry(owner.clone())
                .or_default()
                .entry(topic.clone())
                .or_default()
                .push(partition);
        }
    }

    targets
}

/// Write a `ConsumerGroupHeartbeat` error response.
fn write_heartbeat_error(
    out: &mut BytesMut,
    code: ErrorCode,
    member_id: Option<&str>,
    member_epoch: i32,
) -> Result<()> {
    out.put_i32(0); // throttle_time_ms
    write_error(out, code);
    write_compact_nullable_string(out, Some(&format!("{code:?}")))?;
    write_compact_nullable_string(out, member_id)?;
    out.put_i32(member_epoch);
    out.put_i32(HEARTBEAT_INTERVAL_MS);
    write_heartbeat_assignment(out, None)?;
    write_empty_tagged_fields(out)
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
                // Same epoch fencing the Fetch handler applies: a client whose
                // leader epoch disagrees with the broker's is working from a
                // stale view of leadership, and must not be handed an offset
                // from a log it cannot vouch for (KIP-320).
                Some(p)
                    if partition.current_leader_epoch >= 0
                        && partition.current_leader_epoch != p.leader_epoch =>
                {
                    let code = if partition.current_leader_epoch < p.leader_epoch {
                        ErrorCode::FencedLeaderEpoch
                    } else {
                        ErrorCode::UnknownLeaderEpoch
                    };
                    write_error(out, code);
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

// ── Share groups (KIP-932) ───────────────────────────────────────────────

/// Serve a `ShareGroupHeartbeat` (API key 76, v1).
///
/// # How this differs from `consumer_group_heartbeat`
///
/// A share group has no exclusive partition ownership, so it has no
/// reconciliation: the coordinator computes an assignment and the member is
/// on it from that heartbeat onward. There is no revoke step, no
/// "owned ∩ target" intermediate, and no waiting for a peer to release a
/// partition — all of which `consumer_group_heartbeat` above must model, and
/// none of which exists here. That is the protocol difference, not a
/// simplification.
///
/// Everything else carries over: `-1` is a leave, epoch `0` is a join, a
/// mismatched epoch is `FENCED_MEMBER_EPOCH`, an unknown member at a non-zero
/// epoch is `UNKNOWN_MEMBER_ID`, and a null assignment means "keep what you
/// have".
fn share_group_heartbeat(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ShareGroupHeartbeatReq::read(body)?;

    if state.group_coordinator(&req.group_id) != node_id {
        return write_heartbeat_error(out, ErrorCode::NotCoordinator, None, 0);
    }

    if req.member_epoch < 0 {
        if let Some(group) = state.share_groups.get_mut(&req.group_id) {
            group.members.remove(&req.member_id);
            group.group_epoch += 1;
            // Whatever the departing member was holding is nobody's now.
            if group.members.is_empty() {
                group.release_in_flight();
            }
        }
        return write_share_heartbeat(out, &req.member_id, req.member_epoch, None);
    }

    let partition_counts: HashMap<String, i32> = state
        .topics
        .iter()
        .map(|(name, t)| (name.clone(), t.partitions.len() as i32))
        .collect();
    let topic_ids: HashMap<String, [u8; 16]> = state
        .topics
        .iter()
        .map(|(name, t)| (name.clone(), t.topic_id))
        .collect();

    let group = state.share_groups.entry(req.group_id.clone()).or_default();
    let known = group.members.get(&req.member_id).cloned();

    if let Some(existing) = &known
        && req.member_epoch != 0
        && req.member_epoch != existing.member_epoch
    {
        return write_heartbeat_error(out, ErrorCode::FencedMemberEpoch, Some(&req.member_id), 0);
    }
    if known.is_none() && req.member_epoch != 0 {
        return write_heartbeat_error(out, ErrorCode::UnknownMemberId, Some(&req.member_id), 0);
    }

    let subscribed = req
        .subscribed_topic_names
        .clone()
        .or_else(|| known.as_ref().map(|m| m.subscribed_topics.clone()))
        .unwrap_or_default();

    let is_new = known.is_none();
    let rejoining = known.is_some() && req.member_epoch == 0;
    let subscription_changed = known
        .as_ref()
        .is_some_and(|m| m.subscribed_topics != subscribed);
    if is_new || rejoining || subscription_changed {
        group.group_epoch += 1;
    }
    let group_epoch = group.group_epoch;

    {
        let member = group.members.entry(req.member_id.clone()).or_default();
        member.subscribed_topics = subscribed.clone();
    }

    // Round-robin every partition of every subscribed topic over the members
    // that subscribe to it. A share group *may* hand the same partition to
    // several members; distributing them is the simpler behaviour and is what
    // the reference `SimpleShareAssignor` does while members ≤ partitions.
    let mut member_ids: Vec<String> = group.members.keys().cloned().collect();
    member_ids.sort();
    let mut targets: HashMap<String, HashMap<String, Vec<i32>>> = HashMap::new();
    let mut topics: Vec<&String> = partition_counts.keys().collect();
    topics.sort();
    for topic in topics {
        let subscribers: Vec<&String> = member_ids
            .iter()
            .filter(|id| {
                group
                    .members
                    .get(*id)
                    .is_some_and(|m| m.subscribed_topics.contains(topic))
            })
            .collect();
        if subscribers.is_empty() {
            continue;
        }
        let count = partition_counts.get(topic).copied().unwrap_or(0);
        for partition in 0..count {
            let owner = subscribers[(partition as usize) % subscribers.len()];
            targets
                .entry(owner.clone())
                .or_default()
                .entry(topic.clone())
                .or_default()
                .push(partition);
        }
    }

    let granted = targets.remove(&req.member_id).unwrap_or_default();

    let (member_epoch, send_assignment) = {
        let member = group.members.entry(req.member_id.clone()).or_default();
        if member.assignment != granted {
            member.assignment = granted.clone();
            member.assignment_dirty = true;
        }
        if is_new || rejoining || subscription_changed || member.member_epoch == 0 {
            member.member_epoch = group_epoch;
        }
        let dirty = member.assignment_dirty;
        member.assignment_dirty = false;
        (member.member_epoch, dirty)
    };

    let wire_assignment: Vec<HeartbeatTopicPartitions> = granted
        .iter()
        .filter_map(|(topic, partitions)| {
            topic_ids.get(topic).map(|id| HeartbeatTopicPartitions {
                topic_id: *id,
                partitions: partitions.clone(),
            })
        })
        .collect();

    write_share_heartbeat(
        out,
        &req.member_id,
        member_epoch,
        if send_assignment {
            Some(&wire_assignment)
        } else {
            None
        },
    )
}

/// Write a successful `ShareGroupHeartbeat` response.
///
/// The wire shape is identical to `ConsumerGroupHeartbeat`'s, including the
/// nullable-struct presence byte in front of the assignment, so
/// [`write_heartbeat_assignment`] serves both.
fn write_share_heartbeat(
    out: &mut BytesMut,
    member_id: &str,
    member_epoch: i32,
    assignment: Option<&[HeartbeatTopicPartitions]>,
) -> Result<()> {
    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    write_compact_nullable_string(out, None)?; // error_message
    write_compact_nullable_string(out, Some(member_id))?;
    out.put_i32(member_epoch);
    out.put_i32(HEARTBEAT_INTERVAL_MS);
    write_heartbeat_assignment(out, assignment)?;
    write_empty_tagged_fields(out)
}

/// Resolve a topic UUID back to its name.
fn topic_name_for_id(state: &ClusterState, topic_id: [u8; 16]) -> Option<String> {
    state
        .topics
        .iter()
        .find(|(_, t)| t.topic_id == topic_id)
        .map(|(name, _)| name.clone())
}

/// Apply every acknowledgement batch a share request piggybacked onto one
/// partition, returning the error to report in the acknowledge-error field.
///
/// A batch whose `acknowledge_types` array has one entry applies that type to
/// the whole range; otherwise there must be exactly one type per offset, which
/// is what the KIP-932 format specifies. Anything else is `INVALID_REQUEST` —
/// the same answer a real broker gives, and worth modelling because the
/// client builds these arrays itself.
fn apply_share_acks(
    state: &mut ClusterState,
    group_id: &str,
    topic: &str,
    partition: i32,
    batches: &[ShareAckBatch],
) -> ErrorCode {
    for batch in batches {
        if batch.last_offset < batch.first_offset {
            return ErrorCode::InvalidRequest;
        }
        let span = batch.last_offset - batch.first_offset + 1;
        let types: Vec<i8> = match batch.acknowledge_types.len() {
            1 => vec![batch.acknowledge_types[0]; span as usize],
            n if n as i64 == span => batch.acknowledge_types.clone(),
            _ => return ErrorCode::InvalidRequest,
        };
        let share_partition = state
            .share_groups
            .entry(group_id.to_string())
            .or_default()
            .partitions
            .entry((topic.to_string(), partition))
            .or_default();
        for (i, &ack_type) in types.iter().enumerate() {
            let offset = batch.first_offset + i as i64;
            share_partition.acknowledge(offset, offset, ack_type);
        }
    }
    ErrorCode::None
}

/// Both share data APIs carry the group and member ID as *nullable* compact
/// strings, because the same request type is reused where the fields do not
/// apply. On `ShareFetch` and `ShareAcknowledge` they are mandatory: the
/// broker resolves share-partition state by group and attributes the
/// acquisition to a member. Returning the records regardless would let a
/// client that forgot to set them pass every test here and fail against a
/// real broker.
///
/// Returns the group ID when both are present and non-empty.
fn required_share_identity(
    group_id: &Option<String>,
    member_id: &Option<String>,
) -> Option<String> {
    let group = group_id.as_deref().filter(|g| !g.is_empty())?;
    member_id.as_deref().filter(|m| !m.is_empty())?;
    Some(group.to_string())
}

/// Serve a `ShareFetch` (API key 78, v1).
///
/// Acknowledgements piggybacked on the request are applied *before* records
/// are acquired, which is the ordering a real broker uses and the reason a
/// client can accept a batch and fetch the next one in a single round trip.
fn share_fetch(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ShareFetchReq::read(body, api_version)?;
    let Some(group_id) = required_share_identity(&req.group_id, &req.member_id) else {
        out.put_i32(0); // throttle_time_ms
        write_error(out, ErrorCode::InvalidRequest);
        write_compact_nullable_string(out, Some("ShareFetch requires a group ID and member ID"))?;
        out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);
        write_compact_array_len(out, 0)?; // responses
        write_compact_array_len(out, 0)?; // node_endpoints
        return write_empty_tagged_fields(out);
    };

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    write_compact_nullable_string(out, None)?; // error_message
    out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);

    // Group the flat (topic_id, partition) list back into topics, preserving
    // first-seen order so the response mirrors the request.
    let mut order: Vec<[u8; 16]> = Vec::new();
    let mut grouped: HashMap<[u8; 16], Vec<&ShareTopicPartitionAcks>> = HashMap::new();
    for tp in &req.topics {
        if !grouped.contains_key(&tp.topic_id) {
            order.push(tp.topic_id);
        }
        grouped.entry(tp.topic_id).or_default().push(tp);
    }

    write_compact_array_len(out, order.len())?;
    for topic_id in &order {
        out.put_slice(topic_id);
        let entries = grouped.get(topic_id).map_or(&[][..], Vec::as_slice);
        write_compact_array_len(out, entries.len())?;
        for entry in entries {
            let Some(topic) = topic_name_for_id(state, *topic_id) else {
                write_share_fetch_partition(
                    out,
                    entry.partition_index,
                    ErrorCode::UnknownTopicId,
                    ErrorCode::None,
                    -1,
                    -1,
                    None,
                    &[],
                )?;
                continue;
            };

            let ack_error = apply_share_acks(
                state,
                &group_id,
                &topic,
                entry.partition_index,
                &entry.acknowledgement_batches,
            );

            let Some(p) = state.partition(&topic, entry.partition_index) else {
                write_share_fetch_partition(
                    out,
                    entry.partition_index,
                    ErrorCode::UnknownTopicOrPartition,
                    ack_error,
                    -1,
                    -1,
                    None,
                    &[],
                )?;
                continue;
            };
            if p.leader != node_id {
                let (leader, epoch) = (p.leader, p.leader_epoch);
                write_share_fetch_partition(
                    out,
                    entry.partition_index,
                    ErrorCode::NotLeaderForPartition,
                    ack_error,
                    leader,
                    epoch,
                    None,
                    &[],
                )?;
                continue;
            }

            // Snapshot what the log holds before borrowing the share state.
            let (leader, leader_epoch, log, next_offset) =
                (p.leader, p.leader_epoch, p.log.clone(), p.next_offset);

            let share_partition = state
                .share_groups
                .entry(group_id.clone())
                .or_default()
                .partitions
                .entry((topic.clone(), entry.partition_index))
                .or_default();
            let cursor = share_partition
                .next_acquire
                .max(share_partition.start_offset);

            // Acquire whole batches, stopping once `max_records` is reached.
            // Batch granularity is what a real broker uses too: it never
            // splits a batch to honour the cap exactly.
            let mut records = Vec::new();
            let mut acquired_first = i64::MAX;
            let mut acquired_last = -1i64;
            let mut taken = 0i64;
            for batch in &log {
                let base = batch_base_offset(batch).unwrap_or(0);
                let count = batch_record_count(batch).unwrap_or(0);
                if base + count <= cursor {
                    continue;
                }
                if req.max_records > 0 && taken >= i64::from(req.max_records) {
                    break;
                }
                records.extend_from_slice(batch);
                acquired_first = acquired_first.min(base.max(cursor));
                acquired_last = acquired_last.max(base + count - 1);
                taken += count;
            }

            let acquired = if acquired_last >= acquired_first {
                let delivery_count = share_partition.acquire(acquired_first, acquired_last);
                vec![(acquired_first, acquired_last, delivery_count)]
            } else {
                Vec::new()
            };
            debug_assert!(
                acquired.is_empty() || acquired_last < next_offset,
                "acquired past the high watermark"
            );

            let records = Bytes::from(records);
            write_share_fetch_partition(
                out,
                entry.partition_index,
                ErrorCode::None,
                ack_error,
                leader,
                leader_epoch,
                if records.is_empty() {
                    None
                } else {
                    Some(&records)
                },
                &acquired,
            )?;
        }
        write_empty_tagged_fields(out)?; // topic tagged fields
    }

    write_compact_array_len(out, 0)?; // node_endpoints
    write_empty_tagged_fields(out)
}

/// Write one partition of a `ShareFetch` response.
#[allow(clippy::too_many_arguments)]
fn write_share_fetch_partition(
    out: &mut BytesMut,
    partition: i32,
    error: ErrorCode,
    ack_error: ErrorCode,
    leader_id: i32,
    leader_epoch: i32,
    records: Option<&Bytes>,
    acquired: &[(i64, i64, i16)],
) -> Result<()> {
    out.put_i32(partition);
    write_error(out, error);
    write_compact_nullable_string(out, None)?; // error_message
    write_error(out, ack_error);
    write_compact_nullable_string(out, None)?; // acknowledge_error_message
    out.put_i32(leader_id);
    out.put_i32(leader_epoch);
    write_empty_tagged_fields(out)?; // CurrentLeader tagged section
    write_compact_nullable_bytes(out, records)?;
    write_compact_array_len(out, acquired.len())?;
    for &(first, last, delivery_count) in acquired {
        out.put_i64(first);
        out.put_i64(last);
        out.put_i16(delivery_count);
        write_empty_tagged_fields(out)?;
    }
    write_empty_tagged_fields(out) // partition tagged fields
}

/// Serve a `ShareAcknowledge` (API key 79, v1).
fn share_acknowledge(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ShareAcknowledgeReq::read(body, api_version)?;
    let Some(group_id) = required_share_identity(&req.group_id, &req.member_id) else {
        out.put_i32(0); // throttle_time_ms
        write_error(out, ErrorCode::InvalidRequest);
        write_compact_nullable_string(
            out,
            Some("ShareAcknowledge requires a group ID and member ID"),
        )?;
        write_compact_array_len(out, 0)?; // responses
        write_compact_array_len(out, 0)?; // node_endpoints
        return write_empty_tagged_fields(out);
    };

    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    write_compact_nullable_string(out, None)?; // error_message

    let mut order: Vec<[u8; 16]> = Vec::new();
    let mut grouped: HashMap<[u8; 16], Vec<&ShareTopicPartitionAcks>> = HashMap::new();
    for tp in &req.topics {
        if !grouped.contains_key(&tp.topic_id) {
            order.push(tp.topic_id);
        }
        grouped.entry(tp.topic_id).or_default().push(tp);
    }

    write_compact_array_len(out, order.len())?;
    for topic_id in &order {
        out.put_slice(topic_id);
        let entries = grouped.get(topic_id).map_or(&[][..], Vec::as_slice);
        write_compact_array_len(out, entries.len())?;
        for entry in entries {
            let (error, leader, epoch) = match topic_name_for_id(state, *topic_id) {
                None => (ErrorCode::UnknownTopicId, -1, -1),
                Some(topic) => match state.partition(&topic, entry.partition_index) {
                    None => (ErrorCode::UnknownTopicOrPartition, -1, -1),
                    Some(p) if p.leader != node_id => {
                        (ErrorCode::NotLeaderForPartition, p.leader, p.leader_epoch)
                    }
                    Some(p) => {
                        let (leader, epoch) = (p.leader, p.leader_epoch);
                        let code = apply_share_acks(
                            state,
                            &group_id,
                            &topic,
                            entry.partition_index,
                            &entry.acknowledgement_batches,
                        );
                        (code, leader, epoch)
                    }
                },
            };
            out.put_i32(entry.partition_index);
            write_error(out, error);
            write_compact_nullable_string(out, None)?; // error_message
            out.put_i32(leader);
            out.put_i32(epoch);
            write_empty_tagged_fields(out)?; // CurrentLeader tagged section
            write_empty_tagged_fields(out)?; // partition tagged fields
        }
        write_empty_tagged_fields(out)?; // topic tagged fields
    }

    write_compact_array_len(out, 0)?; // node_endpoints
    write_empty_tagged_fields(out)
}

// ── UpdateFeatures (KIP-584) ─────────────────────────────────────────────

/// Serve an `UpdateFeatures` (API key 57).
///
/// The controller-only routing is the point: sending this to an arbitrary
/// broker is what used to surface a controller failover as a blanket-retriable
/// protocol error, so the fake broker answers `NOT_CONTROLLER` from anywhere
/// else exactly as a real one does.
///
/// The updates are recorded on the cluster so a test can assert what the
/// controller was actually asked to do — including, when `validate_only` is
/// set, that it was asked to do nothing.
fn update_features(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = UpdateFeaturesReq::read(body, api_version)?;

    let write_response = |out: &mut BytesMut, code: ErrorCode, results: &[(String, ErrorCode)]| {
        out.put_i32(0); // throttle_time_ms
        write_error(out, code);
        write_compact_nullable_string(out, None)?; // error_message
        // v2 (KIP-1014) dropped the per-feature array; v0/v1 still carry it.
        if api_version < 2 {
            write_compact_array_len(out, results.len())?;
            for (feature, result) in results {
                write_compact_string(out, feature)?;
                write_error(out, *result);
                write_compact_nullable_string(out, None)?;
                write_empty_tagged_fields(out)?;
            }
        }
        write_empty_tagged_fields(out)
    };

    if state.controller_id != node_id {
        return write_response(out, ErrorCode::NotController, &[]);
    }

    let results: Vec<(String, ErrorCode)> = req
        .feature_updates
        .iter()
        .map(|u| (u.feature.clone(), ErrorCode::None))
        .collect();

    if !req.validate_only && !req.feature_updates.is_empty() {
        for update in &req.feature_updates {
            if update.max_version_level == 0 {
                state.finalized_features.remove(&update.feature);
            } else {
                state
                    .finalized_features
                    .insert(update.feature.clone(), update.max_version_level);
            }
        }
        // KIP-584 requires the epoch to advance whenever the finalized set
        // changes; a client is entitled to treat an unchanged epoch as an
        // unchanged set and skip re-reading it.
        state.finalized_features_epoch += 1;
    }

    write_response(out, ErrorCode::None, &results)
}

// ── StreamsGroupDescribe (KIP-1071) ──────────────────────────────────────

/// Serve a `StreamsGroupDescribe` (API key 89, v0).
///
/// Group state comes from [`ClusterState::streams_groups`], which a test
/// populates directly — krafka cannot join a Streams group, so there is
/// nothing for the broker to derive it from.
///
/// The point of serving it at all is the *decoder*: this response exercises
/// two nullable structs behind presence bytes (`Topology`, `UserEndpoint`), a
/// nullable array nested inside one of them (`Subtopologies`), and a `uint16`
/// port. Each is a shape the client gets exactly one chance to read correctly.
fn streams_group_describe(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = StreamsGroupDescribeReq::read(body)?;

    out.put_i32(0); // throttle_time_ms
    write_compact_array_len(out, req.group_ids.len())?;

    for group_id in &req.group_ids {
        // Route check: like every group API, this belongs to the coordinator.
        if state.group_coordinator(group_id) != node_id {
            write_error(out, ErrorCode::NotCoordinator);
            write_compact_nullable_string(out, None)?;
            write_compact_string(out, group_id)?;
            write_compact_string(out, "")?; // group_state
            out.put_i32(0); // group_epoch
            out.put_i32(0); // assignment_epoch
            write_presence(out, false); // topology
            write_compact_array_len(out, 0)?; // members
            out.put_i32(i32::MIN); // authorized_operations
            write_empty_tagged_fields(out)?;
            continue;
        }

        let Some(group) = state.streams_groups.get(group_id) else {
            write_error(out, ErrorCode::GroupIdNotFound);
            write_compact_nullable_string(out, Some("group not found"))?;
            write_compact_string(out, group_id)?;
            write_compact_string(out, "")?;
            out.put_i32(0);
            out.put_i32(0);
            write_presence(out, false);
            write_compact_array_len(out, 0)?;
            out.put_i32(i32::MIN);
            write_empty_tagged_fields(out)?;
            continue;
        };

        write_error(out, ErrorCode::None);
        write_compact_nullable_string(out, None)?; // error_message
        write_compact_string(out, group_id)?;
        write_compact_string(out, &group.group_state)?;
        out.put_i32(group.group_epoch);
        out.put_i32(group.assignment_epoch);

        // Topology: nullable struct.
        match group.topology_epoch {
            None => write_presence(out, false),
            Some(epoch) => {
                write_presence(out, true);
                out.put_i32(epoch);
                // Subtopologies: nullable *array* — raw varint 0 is null,
                // which the format distinguishes from an empty array.
                match &group.subtopologies {
                    None => crate::util::varint::encode_unsigned_varint(0, out),
                    Some(subs) => {
                        write_compact_array_len(out, subs.len())?;
                        for id in subs {
                            write_compact_string(out, id)?;
                            write_compact_array_len(out, 1)?; // source_topics
                            write_compact_string(out, "source-topic")?;
                            write_compact_array_len(out, 0)?; // repartition_sink_topics
                            write_compact_array_len(out, 0)?; // state_changelog_topics
                            write_compact_array_len(out, 0)?; // repartition_source_topics
                            write_empty_tagged_fields(out)?;
                        }
                    }
                }
                write_empty_tagged_fields(out)?; // topology tagged fields
            }
        }

        write_compact_array_len(out, group.members.len())?;
        for member in &group.members {
            write_compact_string(out, &member.member_id)?;
            out.put_i32(member.member_epoch);
            write_compact_nullable_string(out, None)?; // instance_id
            write_compact_nullable_string(out, None)?; // rack_id
            write_compact_string(out, "krafka-test")?; // client_id
            write_compact_string(out, "127.0.0.1")?; // client_host
            out.put_i32(member.topology_epoch);
            write_compact_string(out, &member.process_id)?;

            // UserEndpoint: nullable struct with a `uint16` port.
            match &member.user_endpoint {
                None => write_presence(out, false),
                Some((host, port)) => {
                    write_presence(out, true);
                    write_compact_string(out, host)?;
                    out.put_u16(*port);
                    write_empty_tagged_fields(out)?;
                }
            }

            write_compact_array_len(out, 0)?; // client_tags
            write_compact_array_len(out, 0)?; // task_offsets
            write_compact_array_len(out, 0)?; // task_end_offsets
            write_streams_assignment(out, &member.active_tasks)?;
            write_streams_assignment(out, &member.target_active_tasks)?;
            out.put_u8(0); // is_classic
            write_empty_tagged_fields(out)?;
        }

        out.put_i32(if req.include_authorized_operations {
            0
        } else {
            i32::MIN
        });
        write_empty_tagged_fields(out)?;
    }

    write_empty_tagged_fields(out)
}

/// Write an `Assignment` struct: active, standby and warm-up task lists.
///
/// Only active tasks are modelled; standby and warm-up are written empty. A
/// test that needs them needs a real Streams runtime to produce them.
fn write_streams_assignment(out: &mut BytesMut, active: &[(String, Vec<i32>)]) -> Result<()> {
    write_compact_array_len(out, active.len())?;
    for (subtopology_id, partitions) in active {
        write_compact_string(out, subtopology_id)?;
        write_compact_array_len(out, partitions.len())?;
        for p in partitions {
            out.put_i32(*p);
        }
        write_empty_tagged_fields(out)?;
    }
    write_compact_array_len(out, 0)?; // standby_tasks
    write_compact_array_len(out, 0)?; // warmup_tasks
    write_empty_tagged_fields(out) // assignment tagged fields
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

        // A v12 request body for a single topic, encoded exactly as the client
        // does: compact array, 16-byte topic id, compact name, tagged fields.
        let mut body = BytesMut::new();
        write_compact_array_len(&mut body, 1).unwrap();
        body.put_slice(&[0u8; 16]); // topic_id: looking up by name
        write_compact_nullable_string(&mut body, Some("orders")).unwrap();
        write_empty_tagged_fields(&mut body).unwrap();
        body.put_u8(0); // allow_auto_topic_creation
        body.put_u8(0); // include_topic_authorized_operations
        write_empty_tagged_fields(&mut body).unwrap();
        let mut body = body.freeze();

        let mut out = BytesMut::new();
        metadata(&mut body, &mut state, &mut out).unwrap();

        let mut encoded = out.freeze();
        let decoded = MetadataResponse::decode_versioned(12, &mut encoded).unwrap();
        assert_eq!(encoded.remaining(), 0, "writer emitted trailing bytes");

        assert_eq!(decoded.controller_id, 1);
        assert_eq!(decoded.brokers.len(), 2);
        assert_eq!(decoded.cluster_id.as_deref(), Some("krafka-fake-cluster"));
        let topic = decoded.find_topic("orders").unwrap();
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(topic.error_code, ErrorCode::None);
        // The UUID must survive the round trip: KIP-848 assignments name
        // topics by ID, and an all-zero id would make them unresolvable.
        assert!(
            topic.topic_id.is_some_and(|id| id != [0u8; 16]),
            "v12 must carry a real topic UUID"
        );
    }

    /// A synthesized error must still be a structurally valid response.
    #[test]
    fn synthesized_metadata_error_still_decodes() {
        let mut body = BytesMut::new();
        write_compact_array_len(&mut body, 1).unwrap();
        body.put_slice(&[0u8; 16]);
        write_compact_nullable_string(&mut body, Some("orders")).unwrap();
        write_empty_tagged_fields(&mut body).unwrap();
        body.put_u8(0);
        body.put_u8(0);
        write_empty_tagged_fields(&mut body).unwrap();
        let mut body = body.freeze();

        let mut out = BytesMut::new();
        dispatch_error(
            ApiKey::Metadata,
            12,
            &mut body,
            ErrorCode::NotController,
            &mut out,
        )
        .unwrap();

        let mut encoded = out.freeze();
        let decoded = MetadataResponse::decode_versioned(12, &mut encoded).unwrap();
        assert_eq!(encoded.remaining(), 0);
        assert_eq!(decoded.topics[0].error_code, ErrorCode::NotController);
    }
}
