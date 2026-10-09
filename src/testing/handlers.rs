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
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{ErrorCode, Result};
use crate::protocol::ApiKey;
use crate::protocol::{Decode, Encode, KafkaString, TaggedField, TryEncode};

use super::state::{
    BrokerTransaction, ClassicGroupState, ClusterState, CommittedOffset,
    ConsumerGroupHeartbeatSeen, GroupMember, LeaveGroupMemberSeen, ListOffsetsLookup,
    SequenceCheck, ShareSession, ShareSessionClose, TxnStatus,
};
use super::wire::*;

/// The API versions the fake broker serves, as `(api, min, max)`.
///
/// Most APIs are pinned to one version (`min == max`), forcing the client's
/// negotiation onto exactly the version each codec in [`super::wire`] was
/// written against. Ranges are served where the version itself carries
/// semantics a test needs to reach: the `Produce`, `InitProducerId` and
/// `EndTxn` versions select between the TV1 and TV2 transaction protocols and
/// KIP-360 recovery, and the share APIs' v2 adds KIP-1206 and KIP-1222.
pub(crate) fn supported_versions() -> Vec<(ApiKey, i16, i16)> {
    vec![
        // The range here is ignored for ApiVersions: it is advertised as
        // `API_VERSIONS_RANGE` instead. See that constant.
        (ApiKey::ApiVersions, 0, 0),
        // v12 is the lowest version carrying topic UUIDs in a form KIP-848
        // can use (v10 forces an all-zero UUID in the *request*, v12 is where
        // the client may look topics up by ID).
        (ApiKey::Metadata, 12, 12),
        // v10 is the lowest Produce version carrying the KIP-951 leader hint.
        // v10–v12 share one layout in both directions. A transactional write
        // at v12 joins its partition to the transaction (KIP-890 TV2); at v11
        // and below the partition must have been added with
        // `AddPartitionsToTxn` first (TV1).
        (ApiKey::Produce, 10, 12),
        (ApiKey::Fetch, 11, 11),
        // v11 understands every offset sentinel down to -6 (KIP-1023); the
        // per-sentinel minimum versions are reached by overriding the range.
        (ApiKey::ListOffsets, 5, 11),
        (ApiKey::FindCoordinator, 2, 2),
        (ApiKey::JoinGroup, 5, 5),
        (ApiKey::SyncGroup, 3, 3),
        (ApiKey::Heartbeat, 3, 3),
        (ApiKey::LeaveGroup, 3, 3),
        // v8 is flexible; from v9 a KIP-848 member commits with its member
        // epoch in the generation field.
        (ApiKey::OffsetCommit, 7, 9),
        (ApiKey::OffsetFetch, 5, 5),
        // KIP-848. v1 is the only version krafka negotiates, and the only one
        // carrying the client-generated member ID (KIP-1082).
        (ApiKey::ConsumerGroupHeartbeat, 1, 1),
        // v3 adds the KIP-360 producer ID and epoch, v4 PRODUCER_FENCED. v6
        // (KIP-939) is served when a test advertises it.
        (ApiKey::InitProducerId, 0, 5),
        // Transactions. v0 for the two TV1-only APIs, because the client still
        // speaks the non-flexible format there.
        (ApiKey::AddPartitionsToTxn, 0, 0),
        (ApiKey::AddOffsetsToTxn, 0, 0),
        (ApiKey::TxnOffsetCommit, 5, 5),
        // v3–v5 share a request layout. The coordinator reads the protocol
        // from the version, as Kafka does: v5 is TV2 and bumps the epoch at
        // every completion, v4 and below is TV1 and does not.
        (ApiKey::EndTxn, 3, 5),
        (ApiKey::CreateTopics, 4, 4),
        (ApiKey::DeleteTopics, 3, 3),
        // KIP-932 share groups. ShareFetch and ShareAcknowledge v2 add
        // ShareAcquireMode (KIP-1206) and IsRenewAck (KIP-1222).
        (ApiKey::ShareGroupHeartbeat, 1, 1),
        (ApiKey::ShareFetch, 1, 2),
        (ApiKey::ShareAcknowledge, 1, 2),
        // KIP-584. v2 is the Kafka 4.0 version that dropped the per-feature
        // `Results` array; overriding this down to v0 is how a test reaches
        // the client's "validate_only needs v1+" refusal.
        (ApiKey::UpdateFeatures, 2, 2),
        // KIP-1071, describe half only — see `streams_group_describe`.
        (ApiKey::StreamsGroupDescribe, 0, 0),
        // v4 is the newest non-flexible DescribeGroups, and the newest one
        // carrying `group_instance_id`.
        (ApiKey::DescribeGroups, 4, 4),
    ]
}

/// Long-poll state carried across the attempts of one `Fetch` or
/// `ShareFetch`.
#[derive(Debug, Default)]
pub(crate) struct LongPoll {
    /// The wait is over: answer with whatever is there.
    pub expired: bool,
    /// Acknowledgement results of a `ShareFetch` whose session check and
    /// acknowledgements ran on an earlier attempt, keyed by
    /// `(topic_id, partition)`.
    pub share_acks: Option<HashMap<([u8; 16], i32), ErrorCode>>,
}

/// What [`dispatch`] did with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Served {
    /// The response is in `out`.
    Done,
    /// Nothing to return yet: run the request again when data arrives, or
    /// with [`LongPoll::expired`] set once this long has passed.
    Wait(Duration),
}

/// Serve one request, writing the response body (no header) into `out`.
///
/// `node_id` is the broker the request arrived at, which is what lets the
/// handlers detect misrouted requests. `client_id` is the one from the request
/// header, which the group APIs record and report back.
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch(
    api_key: ApiKey,
    api_version: i16,
    body: &mut Bytes,
    node_id: i32,
    client_id: Option<&str>,
    state: &mut ClusterState,
    poll: &mut LongPoll,
    out: &mut BytesMut,
) -> Result<Served> {
    match api_key {
        ApiKey::Fetch => return fetch_inner(body, node_id, state, poll, out, false),
        ApiKey::ShareFetch => return share_fetch(body, api_version, node_id, state, poll, out),
        _ => {}
    }
    match api_key {
        ApiKey::ApiVersions => api_versions(api_version, state, out),
        ApiKey::Metadata => metadata(body, state, out),
        ApiKey::Produce => produce(body, api_version, node_id, state, out),
        ApiKey::ListOffsets => list_offsets(body, api_version, node_id, state, out),
        ApiKey::FindCoordinator => find_coordinator(body, state, out),
        ApiKey::JoinGroup => join_group(body, node_id, client_id, state, out),
        ApiKey::SyncGroup => sync_group(body, node_id, state, out),
        ApiKey::Heartbeat => heartbeat(body, node_id, state, out),
        ApiKey::LeaveGroup => leave_group(body, node_id, state, out),
        ApiKey::OffsetCommit => offset_commit(body, api_version, node_id, state, out),
        ApiKey::OffsetFetch => offset_fetch(body, node_id, state, out),
        ApiKey::ConsumerGroupHeartbeat => consumer_group_heartbeat(body, node_id, state, out),
        ApiKey::InitProducerId => init_producer_id(body, api_version, node_id, state, out),
        ApiKey::AddPartitionsToTxn => add_partitions_to_txn(body, api_version, node_id, state, out),
        ApiKey::AddOffsetsToTxn => add_offsets_to_txn(body, api_version, node_id, state, out),
        ApiKey::TxnOffsetCommit => txn_offset_commit(body, node_id, state, out),
        ApiKey::EndTxn => end_txn(body, api_version, node_id, state, out),
        ApiKey::CreateTopics => create_topics(body, node_id, state, out),
        ApiKey::DeleteTopics => delete_topics(body, node_id, state, out),
        ApiKey::ShareGroupHeartbeat => share_group_heartbeat(body, node_id, state, out),
        ApiKey::ShareAcknowledge => share_acknowledge(body, api_version, node_id, state, out),
        ApiKey::UpdateFeatures => update_features(body, api_version, node_id, state, out),
        ApiKey::StreamsGroupDescribe => streams_group_describe(body, node_id, state, out),
        ApiKey::DescribeGroups => describe_groups(body, node_id, state, out),
        ApiKey::GetTelemetrySubscriptions => get_telemetry_subscriptions(body, state, out),
        ApiKey::PushTelemetry => push_telemetry(body, state, out),
        other => Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!("fake broker has no handler for {other:?}"),
        )),
    }?;
    Ok(Served::Done)
}

/// `SaslHandshake` v1: accept PLAIN.
pub(crate) fn sasl_handshake(body: &mut Bytes, out: &mut BytesMut) -> Result<()> {
    let mechanism = KafkaString::decode(body)?.0.unwrap_or_default();
    if mechanism == "PLAIN" {
        write_error(out, ErrorCode::None);
    } else {
        write_error(out, ErrorCode::UnsupportedSaslMechanism);
    }
    write_array_len(out, 1)?;
    write_string(out, "PLAIN")?;
    Ok(())
}

/// `SaslAuthenticate` v1 for PLAIN: `\0username\0password` against
/// `credentials`. Returns whether the connection is now authenticated.
pub(crate) fn sasl_authenticate(
    body: &mut Bytes,
    credentials: &(String, String),
    out: &mut BytesMut,
) -> Result<bool> {
    let auth = crate::protocol::KafkaBytes::decode(body)?
        .0
        .unwrap_or_default();
    let expected = format!("\0{}\0{}", credentials.0, credentials.1);
    let ok = auth.as_ref() == expected.as_bytes();
    if ok {
        write_error(out, ErrorCode::None);
        (-1i16).encode(out); // null error message
    } else {
        write_error(out, ErrorCode::SaslAuthenticationFailed);
        write_string(out, "Authentication failed: invalid credentials")?;
    }
    0i32.encode(out); // empty auth bytes
    0i64.encode(out); // session_lifetime_ms
    Ok(ok)
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
        ApiKey::ShareGroupHeartbeat => {
            // Same response shape as the KIP-848 heartbeat; every error is
            // top-level.
            let req = ShareGroupHeartbeatReq::read(body)?;
            write_heartbeat_error(out, code, Some(&req.member_id), 0)
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
            let req = ListOffsetsReq::read(body, api_version)?;
            let topics: Vec<(String, Vec<ListOffsetsAnswer>)> = req
                .topics
                .iter()
                .map(|t| {
                    let answers = t
                        .partitions
                        .iter()
                        .map(|p| ListOffsetsAnswer::error(p.partition_index, code))
                        .collect();
                    (t.name.clone(), answers)
                })
                .collect();
            write_list_offsets_response(out, api_version, &topics)
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
            let req = OffsetCommitReq::read(body, api_version)?;
            write_offset_commit_response(out, api_version, &req, code)
        }
        ApiKey::OffsetFetch => {
            let _ = OffsetFetchReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, 0)?;
            write_error(out, code);
            Ok(())
        }
        ApiKey::InitProducerId => {
            let _ = InitProducerIdReq::read(body, api_version)?;
            write_init_producer_id(out, api_version, 0, code, -1, -1, (-1, -1))
        }
        ApiKey::AddPartitionsToTxn => {
            let req = AddPartitionsToTxnReq::read(body)?;
            let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
            for (topic, partition) in req.partitions {
                match by_topic.iter_mut().find(|(name, _)| *name == topic) {
                    Some((_, partitions)) => partitions.push(partition),
                    None => by_topic.push((topic, vec![partition])),
                }
            }
            out.put_i32(0);
            write_array_len(out, by_topic.len())?;
            for (topic, partitions) in &by_topic {
                write_string(out, topic)?;
                write_array_len(out, partitions.len())?;
                for partition in partitions {
                    out.put_i32(*partition);
                    write_error(out, code);
                }
            }
            Ok(())
        }
        ApiKey::AddOffsetsToTxn => {
            let _ = AddOffsetsToTxnReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            Ok(())
        }
        ApiKey::TxnOffsetCommit => {
            let req = TxnOffsetCommitReq::read(body)?;
            let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
            for offset in req.offsets {
                match by_topic.iter_mut().find(|(name, _)| *name == offset.topic) {
                    Some((_, partitions)) => partitions.push(offset.partition),
                    None => by_topic.push((offset.topic, vec![offset.partition])),
                }
            }
            out.put_i32(0);
            write_compact_array_len(out, by_topic.len())?;
            for (topic, partitions) in &by_topic {
                write_compact_string(out, topic)?;
                write_compact_array_len(out, partitions.len())?;
                for partition in partitions {
                    out.put_i32(*partition);
                    write_error(out, code);
                    write_empty_tagged_fields(out)?;
                }
                write_empty_tagged_fields(out)?;
            }
            write_empty_tagged_fields(out)
        }
        ApiKey::EndTxn => {
            let _ = EndTxnReq::read(body)?;
            out.put_i32(0);
            write_error(out, code);
            if api_version >= 5 {
                out.put_i64(-1);
                out.put_i16(-1);
            }
            write_empty_tagged_fields(out)
        }
        // Session and request errors are top-level; partition and
        // acknowledgement errors go on every requested partition.
        ApiKey::ShareFetch => {
            let req = ShareFetchReq::read(body, api_version)?;
            if !share_partition_scoped(code) {
                return write_share_fetch_error(out, code, "injected by the fake broker");
            }
            let (error, ack_error) = if code == ErrorCode::InvalidRecordState {
                (ErrorCode::None, code)
            } else {
                (code, ErrorCode::None)
            };
            out.put_i32(0);
            write_error(out, ErrorCode::None);
            write_compact_nullable_string(out, None)?;
            out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);
            write_share_partitions(out, &req.topics, |out, partition| {
                write_share_fetch_partition(out, partition, error, ack_error, -1, -1, None, &[])
            })?;
            write_compact_array_len(out, 0)?;
            write_empty_tagged_fields(out)
        }
        ApiKey::ShareAcknowledge => {
            let req = ShareAcknowledgeReq::read(body, api_version)?;
            if !share_partition_scoped(code) {
                return write_share_acknowledge_error(
                    out,
                    api_version,
                    code,
                    "injected by the fake broker",
                );
            }
            out.put_i32(0);
            write_error(out, ErrorCode::None);
            write_compact_nullable_string(out, None)?;
            if api_version >= 2 {
                out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);
            }
            write_share_partitions(out, &req.topics, |out, partition| {
                out.put_i32(partition);
                write_error(out, code);
                write_compact_nullable_string(out, None)?;
                out.put_i32(-1);
                out.put_i32(-1);
                write_empty_tagged_fields(out)?;
                write_empty_tagged_fields(out)
            })?;
            write_compact_array_len(out, 0)?;
            write_empty_tagged_fields(out)
        }
        ApiKey::UpdateFeatures => {
            let req = UpdateFeaturesReq::read(body, api_version)?;
            out.put_i32(0);
            write_error(out, code);
            write_compact_nullable_string(out, Some("injected by the fake broker"))?;
            if api_version < 2 {
                write_compact_array_len(out, req.feature_updates.len())?;
                for update in &req.feature_updates {
                    write_compact_string(out, &update.feature)?;
                    write_error(out, code);
                    write_compact_nullable_string(out, None)?;
                    write_empty_tagged_fields(out)?;
                }
            }
            write_empty_tagged_fields(out)
        }
        ApiKey::StreamsGroupDescribe => {
            let req = StreamsGroupDescribeReq::read(body)?;
            out.put_i32(0);
            write_compact_array_len(out, req.group_ids.len())?;
            for group_id in &req.group_ids {
                write_error(out, code);
                write_compact_nullable_string(out, None)?;
                write_compact_string(out, group_id)?;
                write_compact_string(out, "")?;
                out.put_i32(0);
                out.put_i32(0);
                write_presence(out, false);
                write_compact_array_len(out, 0)?;
                out.put_i32(i32::MIN);
                write_empty_tagged_fields(out)?;
            }
            write_empty_tagged_fields(out)
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
        ApiKey::DescribeGroups => {
            let req = DescribeGroupsReq::read(body)?;
            out.put_i32(0);
            write_array_len(out, req.groups.len())?;
            for group_id in &req.groups {
                write_error(out, code);
                write_string(out, group_id)?;
                write_string(out, "")?; // group_state
                write_string(out, "")?; // protocol_type
                write_string(out, "")?; // protocol_data
                write_array_len(out, 0)?; // members
                out.put_i32(i32::MIN); // authorized_operations
            }
            Ok(())
        }
        ApiKey::GetTelemetrySubscriptions => {
            out.put_i32(0); // throttle_time_ms
            write_error(out, code);
            out.put_slice(&[0; 16]); // client_instance_id
            out.put_i32(0); // subscription_id
            write_compact_array_len(out, 0)?; // accepted_compression_types
            out.put_i32(0); // push_interval_ms
            out.put_i32(0); // telemetry_max_bytes
            out.put_u8(0); // delta_temporality
            write_compact_array_len(out, 0)?; // requested_metrics
            write_empty_tagged_fields(out)
        }
        ApiKey::PushTelemetry => {
            out.put_i32(0); // throttle_time_ms
            write_error(out, code);
            write_empty_tagged_fields(out)
        }
        other => Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!("fake broker cannot synthesize an error for {other:?}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// KIP-714 client telemetry
// ---------------------------------------------------------------------------

fn read_uuid(body: &mut Bytes) -> Result<[u8; 16]> {
    if body.len() < 16 {
        return Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::TruncatedFrame,
            "uuid",
        ));
    }
    let mut id = [0; 16];
    id.copy_from_slice(&body.split_to(16));
    Ok(id)
}

/// `GetTelemetrySubscriptions` v0: hand out the cluster's subscription,
/// assigning the configured instance id to a client that asks with zero.
fn get_telemetry_subscriptions(
    body: &mut Bytes,
    state: &ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let requested_id = read_uuid(body)?;
    skip_tagged_fields(body)?;
    let Some(subscription) = &state.telemetry else {
        return dispatch_error(
            ApiKey::GetTelemetrySubscriptions,
            0,
            body,
            ErrorCode::UnsupportedVersion,
            out,
        );
    };
    out.put_i32(0); // throttle_time_ms
    write_error(out, ErrorCode::None);
    if requested_id == [0; 16] {
        out.put_slice(&subscription.client_instance_id);
    } else {
        out.put_slice(&requested_id);
    }
    out.put_i32(subscription.subscription_id);
    write_compact_array_len(out, subscription.accepted_compression_types.len())?;
    for codec in &subscription.accepted_compression_types {
        codec.encode(out);
    }
    out.put_i32(i32::try_from(subscription.push_interval.as_millis()).unwrap_or(i32::MAX));
    out.put_i32(1024 * 1024); // telemetry_max_bytes
    out.put_u8(u8::from(subscription.delta_temporality));
    write_compact_array_len(out, subscription.requested_metrics.len())?;
    for name in &subscription.requested_metrics {
        write_compact_string(out, name)?;
    }
    write_empty_tagged_fields(out)
}

/// `PushTelemetry` v0: record the push; refuse one naming another
/// subscription.
fn push_telemetry(body: &mut Bytes, state: &mut ClusterState, out: &mut BytesMut) -> Result<()> {
    let client_instance_id = read_uuid(body)?;
    let subscription_id = i32::decode(body)?;
    let terminating = bool::decode(body)?;
    let compression_type = i8::decode(body)?;
    let metrics = read_compact_nullable_bytes(body)?.unwrap_or_default();
    skip_tagged_fields(body)?;
    let code = match &state.telemetry {
        Some(subscription) if subscription.subscription_id == subscription_id => {
            state.telemetry_pushes.push(super::TelemetryPush {
                client_instance_id,
                subscription_id,
                terminating,
                compression_type,
                metrics,
            });
            ErrorCode::None
        }
        _ => ErrorCode::UnknownSubscriptionId,
    };
    out.put_i32(0); // throttle_time_ms
    write_error(out, code);
    write_empty_tagged_fields(out)
}

// ---------------------------------------------------------------------------
// ApiVersions
// ---------------------------------------------------------------------------

/// Range of `ApiVersions` versions the fake broker itself speaks.
///
/// Every other entry in [`supported_versions`] is negotiated against this
/// response. `ApiVersions` cannot work that way — it *is* the negotiation —
/// so the client probes with its ceiling
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
    let mut versions = supported_versions();
    if state.telemetry.is_some() {
        versions.push((ApiKey::GetTelemetrySubscriptions, 0, 0));
        versions.push((ApiKey::PushTelemetry, 0, 0));
    }
    if state.sasl_plain.is_some() {
        versions.push((ApiKey::SaslHandshake, 1, 1));
        versions.push((ApiKey::SaslAuthenticate, 1, 1));
    }
    // An override for an API with no handler is still advertised, so a test
    // can observe where the client routes a request this broker cannot serve.
    for (&api_key, &(lo, hi)) in &state.api_version_overrides {
        if !versions.iter().any(|(k, _, _)| *k == api_key) {
            versions.push((api_key, lo, hi));
        }
    }

    write_error(out, ErrorCode::None);
    if flexible {
        write_compact_array_len(out, versions.len())?;
    } else {
        write_array_len(out, versions.len())?;
    }
    for (api_key, min, max) in versions {
        let (lo, hi) = if let Some(&range) = state.api_version_overrides.get(&api_key) {
            range
        } else if api_key == ApiKey::ApiVersions {
            (min_version, max_version)
        } else {
            (min, max)
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
/// A cluster with no finalized features writes an empty section. Once
/// `UpdateFeatures` has finalized something, the fields are emitted, so
/// `AdminClient::describe_features()` can be tested against what
/// `update_features()` applied.
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
    api_version: i16,
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
                    let (code, base_offset, log_start_offset) = append_produce_partition(
                        state,
                        api_version,
                        req.transactional_id.as_deref(),
                        &topic.name,
                        partition.index,
                        partition.records.as_ref(),
                    );
                    write_produce_partition(
                        out,
                        partition.index,
                        code,
                        base_offset,
                        log_start_offset,
                        None,
                    )?;
                }
            }
        }
        write_empty_tagged_fields(out)?; // topic tagged fields
    }
    out.put_i32(state.throttle(ApiKey::Produce));
    write_produce_node_endpoints(out, &hinted_leaders, state)
}

/// Append one partition's batch on its leader, returning
/// `(error, base_offset, log_start_offset)`.
///
/// A batch from a producer with an ID runs the checks a Kafka leader runs
/// before it writes (`ProducerAppendInfo` and the transaction verification of
/// KIP-890):
///
/// 1. A transactional batch must come from the producer ID and epoch the
///    transaction coordinator holds (`INVALID_PRODUCER_ID_MAPPING`,
///    `INVALID_PRODUCER_EPOCH`), and its partition must be in the
///    transaction: at `Produce` v12 (TV2) the write adds it, below v12 (TV1)
///    `AddPartitionsToTxn` must have (`INVALID_TXN_STATE`).
/// 2. A non-transactional batch from a producer with an open transaction on
///    this partition is `INVALID_TXN_STATE`.
/// 3. The sequence and epoch checks of [`PartitionState::check_sequence`]: a
///    duplicate is acknowledged at the offset it was first written at,
///    without writing it again.
///
/// [`PartitionState::check_sequence`]: super::state::PartitionState
fn append_produce_partition(
    state: &mut ClusterState,
    api_version: i16,
    transactional_id: Option<&str>,
    topic: &str,
    partition: i32,
    records: Option<&Bytes>,
) -> (ErrorCode, i64, i64) {
    let Some(records) = records else {
        let (next, start) = state
            .partition(topic, partition)
            .map_or((-1, -1), |p| (p.next_offset, p.log_start_offset));
        return (ErrorCode::None, next, start);
    };
    let producer = batch_producer(records).filter(|b| b.producer_id >= 0);

    // Without producer state checks, a transactional write still joins its
    // transaction by transactional ID.
    let Some(producer) = producer.filter(|_| state.idempotence) else {
        let txn_id = transactional_id
            .filter(|id| state.transactions.contains_key(*id))
            .map(str::to_string);
        let Some(p) = state.partition_mut(topic, partition) else {
            return (ErrorCode::UnknownTopicOrPartition, -1, -1);
        };
        let base_offset = p.append(records);
        if txn_id.is_some() {
            let producer_id = producer.map_or(-1, |b| b.producer_id);
            p.open_transactions
                .entry(producer_id)
                .or_insert(base_offset);
        }
        let log_start_offset = p.log_start_offset;
        if let Some(id) = txn_id
            && let Some(txn) = state.transactions.get_mut(&id)
        {
            txn.begin();
            let entry = (topic.to_string(), partition);
            if !txn.partitions.contains(&entry) {
                txn.partitions.push(entry);
            }
        }
        return (ErrorCode::None, base_offset, log_start_offset);
    };

    // A rejection still reports the log start, as a Kafka leader does: it is
    // how a producer tells retention from loss on UNKNOWN_PRODUCER_ID.
    let log_start_offset = state
        .partition(topic, partition)
        .map_or(-1, |p| p.log_start_offset);
    let reject = |code: ErrorCode| (code, -1, log_start_offset);

    // 1. The coordinator's view of a transactional write.
    let mut joins_transaction: Option<String> = None;
    if producer.transactional {
        let txn_id = transactional_id
            .map(str::to_string)
            .or_else(|| state.transaction_for_producer(producer.producer_id));
        let Some((txn_id, txn)) = txn_id.and_then(|id| {
            let txn = state.transactions.get(&id)?.clone();
            Some((id, txn))
        }) else {
            return reject(ErrorCode::InvalidProducerIdMapping);
        };
        if txn.producer_id != producer.producer_id {
            return reject(ErrorCode::InvalidProducerIdMapping);
        }
        if txn.producer_epoch != producer.producer_epoch {
            return reject(ErrorCode::InvalidProducerEpoch);
        }
        if matches!(
            txn.status,
            TxnStatus::PrepareCommit | TxnStatus::PrepareAbort
        ) {
            return reject(ErrorCode::ConcurrentTransactions);
        }
        let registered = txn.is_open()
            && txn
                .partitions
                .iter()
                .any(|(t, p)| t == topic && *p == partition);
        if !registered {
            if api_version < 12 {
                return reject(ErrorCode::InvalidTxnState);
            }
            joins_transaction = Some(txn_id);
        }
    }

    let pre_kip360 = state.pre_kip360();
    let Some(p) = state.partition_mut(topic, partition) else {
        return reject(ErrorCode::UnknownTopicOrPartition);
    };

    // 2. An idempotent write cannot interleave with the same producer's open
    // transaction.
    if !producer.transactional && p.open_transactions.contains_key(&producer.producer_id) {
        return reject(ErrorCode::InvalidTxnState);
    }

    // 3. Producer state.
    match p.check_sequence(
        producer.producer_id,
        producer.producer_epoch,
        producer.base_sequence,
        producer.record_count,
        pre_kip360,
    ) {
        SequenceCheck::Reject(code) => return reject(code),
        SequenceCheck::Duplicate(base_offset) => {
            return (ErrorCode::None, base_offset, p.log_start_offset);
        }
        SequenceCheck::Append => {}
    }

    let base_offset = p.append(records);
    p.record_batch(
        producer.producer_id,
        producer.producer_epoch,
        producer.base_sequence,
        producer.record_count,
        base_offset,
    );
    if producer.transactional {
        p.open_transactions
            .entry(producer.producer_id)
            .or_insert(base_offset);
    }
    let log_start_offset = p.log_start_offset;

    if let Some(txn_id) = joins_transaction
        && let Some(txn) = state.transactions.get_mut(&txn_id)
    {
        txn.begin();
        let entry = (topic.to_string(), partition);
        if !txn.partitions.contains(&entry) {
            txn.partitions.push(entry);
        }
    }
    (ErrorCode::None, base_offset, log_start_offset)
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
        ApiKey::Fetch => {
            let mut poll = LongPoll {
                expired: true,
                ..LongPoll::default()
            };
            fetch_inner(body, node_id, state, &mut poll, out, true).map(|_| ())
        }
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

/// Serve a `Fetch` v11.
///
/// Long-polls as a broker does: with no partition in error and fewer than
/// `min_bytes` (at least one byte) of records to return, the request is held
/// for up to `max_wait_ms` and answered as soon as an append brings enough
/// data, or empty when the wait runs out.
fn fetch_inner(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    poll: &mut LongPoll,
    out: &mut BytesMut,
    corrupt: bool,
) -> Result<Served> {
    let req = FetchReq::read(body)?;

    out.put_i32(state.throttle(ApiKey::Fetch));
    write_error(out, ErrorCode::None);
    out.put_i32(req.session_id);

    let mut record_bytes = 0usize;
    let mut any_error = false;
    write_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        write_string(out, &topic.topic)?;
        write_array_len(out, topic.partitions.len())?;
        for partition in &topic.partitions {
            let p = state.partition(&topic.topic, partition.partition);
            let error = match p {
                None => Some(ErrorCode::UnknownTopicOrPartition),
                Some(p) if p.leader != node_id => Some(ErrorCode::NotLeaderForPartition),
                // A client whose leader epoch is behind the broker's has
                // missed a leadership change; a client ahead of the broker is
                // talking to a stale replica. Both are reported so the
                // truncation-detection path in the consumer is reachable
                // without a real cluster.
                Some(p)
                    if partition.current_leader_epoch >= 0
                        && partition.current_leader_epoch != p.leader_epoch =>
                {
                    Some(if partition.current_leader_epoch < p.leader_epoch {
                        ErrorCode::FencedLeaderEpoch
                    } else {
                        ErrorCode::UnknownLeaderEpoch
                    })
                }
                Some(p) if partition.fetch_offset > p.next_offset => {
                    Some(ErrorCode::OffsetOutOfRange)
                }
                Some(_) => None,
            };
            let Some(p) = p.filter(|_| error.is_none()) else {
                any_error = true;
                let (high_watermark, log_start_offset) =
                    p.map_or((0, 0), |p| (p.next_offset, p.log_start_offset));
                write_fetch_partition(
                    out,
                    partition.partition,
                    error.unwrap_or(ErrorCode::UnknownServerError),
                    high_watermark,
                    log_start_offset,
                    None,
                )?;
                continue;
            };
            // `read_committed` (isolation_level 1) stops at the last stable
            // offset and reports the aborted transactions the client must
            // filter. `read_uncommitted` sees everything, including records
            // inside an open transaction.
            let read_committed = req.isolation_level == 1;
            let records = if read_committed {
                p.read_range(partition.fetch_offset, p.last_stable_offset())
            } else {
                p.read_from(partition.fetch_offset)
            };
            record_bytes += records.len();
            let records = if corrupt {
                corrupt_record_bytes(&records)
            } else {
                records
            };
            let aborted = if read_committed {
                p.aborted_transactions_from(partition.fetch_offset)
            } else {
                Vec::new()
            };
            write_fetch_partition_with_aborted(
                out,
                partition.partition,
                ErrorCode::None,
                p.next_offset,
                p.last_stable_offset(),
                p.log_start_offset,
                &aborted,
                Some(&records),
            )?;
        }
    }

    let min_bytes = usize::try_from(req.min_bytes.max(1)).unwrap_or(1);
    if !poll.expired && !any_error && req.max_wait_ms > 0 && record_bytes < min_bytes {
        return Ok(Served::Wait(Duration::from_millis(req.max_wait_ms as u64)));
    }
    Ok(Served::Done)
}

fn write_fetch_partition(
    out: &mut BytesMut,
    partition: i32,
    code: ErrorCode,
    high_watermark: i64,
    log_start_offset: i64,
    records: Option<&Bytes>,
) -> Result<()> {
    // With no transaction in flight the last stable offset *is* the high
    // watermark, which is what every error path here reports.
    write_fetch_partition_with_aborted(
        out,
        partition,
        code,
        high_watermark,
        high_watermark,
        log_start_offset,
        &[],
        records,
    )
}

/// As [`write_fetch_partition`], but carrying a distinct last stable offset
/// and the aborted-transaction list a `read_committed` fetch needs.
#[allow(clippy::too_many_arguments)]
fn write_fetch_partition_with_aborted(
    out: &mut BytesMut,
    partition: i32,
    code: ErrorCode,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    aborted: &[(i64, i64)],
    records: Option<&Bytes>,
) -> Result<()> {
    out.put_i32(partition);
    write_error(out, code);
    out.put_i64(high_watermark);
    out.put_i64(last_stable_offset);
    out.put_i64(log_start_offset);
    write_array_len(out, aborted.len())?;
    for (producer_id, first_offset) in aborted {
        out.put_i64(*producer_id);
        out.put_i64(*first_offset);
    }
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

/// How long a KIP-848 member may go without a heartbeat before the
/// coordinator removes it (Kafka's `group.consumer.session.timeout.ms`
/// default).
const CONSUMER_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

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
    state
        .consumer_group_heartbeats
        .push(ConsumerGroupHeartbeatSeen {
            group_id: req.group_id.clone(),
            member_id: req.member_id.clone(),
            member_epoch: req.member_epoch,
            instance_id: req.instance_id.clone(),
            server_assignor: req.server_assignor.clone(),
            full: req.subscribed_topic_names.is_some(),
        });

    // Members whose session expired leave the group, and their partitions
    // become assignable. Expiry is applied when a heartbeat reaches the group.
    let now = tokio::time::Instant::now();
    if let Some(group) = state.groups.get_mut(&req.group_id) {
        let before = group.consumer_members.len();
        group.consumer_members.retain(|id, member| {
            *id == req.member_id
                || member
                    .last_heartbeat
                    .is_none_or(|at| now.duration_since(at) < CONSUMER_SESSION_TIMEOUT)
        });
        if group.consumer_members.len() != before {
            group.group_epoch += 1;
        }
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

    // The assignors an Apache Kafka coordinator ships with.
    if let Some(assignor) = req.server_assignor.as_deref()
        && !matches!(assignor, "uniform" | "range")
    {
        return write_heartbeat_error(out, ErrorCode::UnsupportedAssignor, Some(&req.member_id), 0);
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
        member.last_heartbeat = Some(now);
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
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ListOffsetsReq::read(body, api_version)?;

    let mut topics: Vec<(String, Vec<ListOffsetsAnswer>)> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let mut answers = Vec::with_capacity(topic.partitions.len());
        for partition in &topic.partitions {
            state.list_offsets_lookups.push(ListOffsetsLookup {
                node_id,
                api_version,
                topic: topic.name.clone(),
                partition: partition.partition_index,
                timestamp: partition.timestamp,
            });
            let index = partition.partition_index;
            answers.push(match state.partition(&topic.name, index) {
                None => ListOffsetsAnswer::error(index, ErrorCode::UnknownTopicOrPartition),
                Some(p) if p.leader != node_id => {
                    ListOffsetsAnswer::error(index, ErrorCode::NotLeaderForPartition)
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
                    ListOffsetsAnswer::error(index, code)
                }
                Some(p) => {
                    // "Latest" is the high watermark for `read_uncommitted`
                    // and the last stable offset for `read_committed`. A
                    // timestamp resolves to the first record at or after it,
                    // or to offset -1 when there is none, as Kafka answers.
                    // The other sentinels (tiered storage, max timestamp)
                    // resolve to the log start: the fake log has no remote
                    // tier.
                    let (timestamp, offset) = match partition.timestamp {
                        TIMESTAMP_LATEST if req.isolation_level == 1 => {
                            (-1, p.last_stable_offset())
                        }
                        TIMESTAMP_LATEST => (-1, p.next_offset),
                        TIMESTAMP_EARLIEST => (-1, p.log_start_offset),
                        ts if ts >= 0 => first_at_or_after(p, ts)?.unwrap_or((-1, -1)),
                        _ => (-1, p.log_start_offset),
                    };
                    ListOffsetsAnswer {
                        partition_index: index,
                        error_code: ErrorCode::None,
                        timestamp,
                        offset,
                        leader_epoch: p.leader_epoch,
                    }
                }
            });
        }
        topics.push((topic.name.clone(), answers));
    }
    write_list_offsets_response(out, api_version, &topics)
}

/// The `(timestamp, offset)` of the first data record at or after
/// `timestamp` in a partition's retained log.
fn first_at_or_after(
    partition: &super::state::PartitionState,
    timestamp: i64,
) -> Result<Option<(i64, i64)>> {
    for stored in &partition.log {
        let mut buf = stored.clone();
        let batch = crate::protocol::RecordBatch::decode(&mut buf)?;
        if batch.attributes.is_control_batch || batch.max_timestamp < timestamp {
            continue;
        }
        for record in &batch.records {
            let offset = batch
                .base_offset
                .saturating_add(i64::from(record.offset_delta));
            let record_timestamp = batch.base_timestamp.saturating_add(record.timestamp_delta);
            if offset >= partition.log_start_offset && record_timestamp >= timestamp {
                return Ok(Some((record_timestamp, offset)));
            }
        }
    }
    Ok(None)
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
    client_id: Option<&str>,
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
        client_id: client_id.unwrap_or_default().to_string(),
        client_host: "/127.0.0.1".to_string(),
    }];
    group.leader = member_id.clone();
    group.assignments.clear();
    group.state = ClassicGroupState::CompletingRebalance;

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
    if !group.members.is_empty() {
        group.state = ClassicGroupState::Stable;
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

/// DescribeGroups (Key 15), v4.
///
/// Reports each member's subscription and assignment exactly as the coordinator
/// holds them: the opaque blobs the members and the group leader wrote. The
/// harness never parses them, which is the point — a client that decodes them
/// is decoding bytes a real broker would have handed back unchanged.
fn describe_groups(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = DescribeGroupsReq::read(body)?;

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, req.groups.len())?;

    for group_id in &req.groups {
        // Like every group API, this one belongs to the coordinator.
        if let Some(code) = coordinator_check(state, group_id, node_id) {
            write_error(out, code);
            write_string(out, group_id)?;
            write_string(out, "")?; // group_state
            write_string(out, "")?; // protocol_type
            write_string(out, "")?; // protocol_data
            write_array_len(out, 0)?;
            out.put_i32(i32::MIN); // authorized_operations
            continue;
        }

        let Some(group) = state.groups.get(group_id) else {
            // A group that was never created is reported as Dead with no
            // members, which is what a real coordinator does — not an error.
            write_error(out, ErrorCode::None);
            write_string(out, group_id)?;
            write_string(out, "Dead")?;
            write_string(out, "")?;
            write_string(out, "")?;
            write_array_len(out, 0)?;
            out.put_i32(i32::MIN);
            continue;
        };

        write_error(out, ErrorCode::None);
        write_string(out, group_id)?;
        write_string(out, group.state.as_str())?;
        write_string(out, &group.protocol_type)?;
        write_string(out, group.protocol_name.as_deref().unwrap_or(""))?;

        write_array_len(out, group.members.len())?;
        for member in &group.members {
            write_string(out, &member.member_id)?;
            write_nullable_string(out, member.group_instance_id.as_deref())?;
            write_string(out, &member.client_id)?;
            write_string(out, &member.client_host)?;
            write_nullable_bytes(out, Some(&member.metadata))?;
            let assignment = group.assignments.get(&member.member_id);
            write_nullable_bytes(out, Some(assignment.unwrap_or(&EMPTY_ASSIGNMENT)))?;
        }

        out.put_i32(if req.include_authorized_operations {
            // Every operation this harness models is permitted.
            i32::MAX
        } else {
            i32::MIN
        });
    }

    Ok(())
}

/// What the coordinator stores for a member that has joined but not yet been
/// given an assignment.
const EMPTY_ASSIGNMENT: Bytes = Bytes::from_static(&[]);

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

    for (member_id, instance) in &req.members {
        state.leave_group_members.push(LeaveGroupMemberSeen {
            group_id: req.group_id.clone(),
            member_id: member_id.clone(),
            group_instance_id: instance.clone(),
        });
    }
    if let Some(group) = state.groups.get_mut(&req.group_id) {
        group
            .members
            .retain(|m| !req.members.iter().any(|(id, _)| *id == m.member_id));
        if group.members.is_empty() {
            group.state = ClassicGroupState::Empty;
            group.assignments.clear();
        }
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

/// Serve `OffsetCommit` v7–v9.
///
/// A group with KIP-848 members validates the commit as Kafka's consumer
/// group does: a commit without a member and epoch passes only while the
/// group is empty, the member must be known (`UNKNOWN_MEMBER_ID`), must commit
/// at v9 or later (`UNSUPPORTED_VERSION`), and must send its current member
/// epoch (`STALE_MEMBER_EPOCH` below it, `FENCED_MEMBER_EPOCH` above it).
/// Any other group validates the classic generation and member ID.
fn offset_commit(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = OffsetCommitReq::read(body, api_version)?;

    // A commit is rejected if it is misrouted, or if the member has been
    // rebalanced out from under it. Both make the client re-join rather than
    // silently committing against a stale generation.
    let rejection = coordinator_check(state, &req.group_id, node_id).or_else(|| {
        let group = state.groups.get(&req.group_id)?;
        if !group.consumer_members.is_empty() {
            let Some(member) = group.consumer_members.get(&req.member_id) else {
                return Some(ErrorCode::UnknownMemberId);
            };
            return if api_version < 9 {
                Some(ErrorCode::UnsupportedVersion)
            } else if req.generation_id < member.member_epoch {
                Some(ErrorCode::StaleMemberEpoch)
            } else if req.generation_id > member.member_epoch {
                Some(ErrorCode::FencedMemberEpoch)
            } else {
                None
            };
        }
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

    write_offset_commit_response(out, api_version, &req, rejection.unwrap_or(ErrorCode::None))
}

/// One `code` for every partition of `req`, in the v7 or the flexible v8–v9
/// layout.
fn write_offset_commit_response(
    out: &mut BytesMut,
    api_version: i16,
    req: &OffsetCommitReq,
    code: ErrorCode,
) -> Result<()> {
    out.put_i32(0); // throttle_time_ms
    if api_version < 8 {
        write_array_len(out, req.topics.len())?;
        for topic in &req.topics {
            write_string(out, &topic.name)?;
            write_array_len(out, topic.partitions.len())?;
            for partition in &topic.partitions {
                out.put_i32(partition.partition_index);
                write_error(out, code);
            }
        }
        return Ok(());
    }
    write_compact_array_len(out, req.topics.len())?;
    for topic in &req.topics {
        write_compact_string(out, &topic.name)?;
        write_compact_array_len(out, topic.partitions.len())?;
        for partition in &topic.partitions {
            out.put_i32(partition.partition_index);
            write_error(out, code);
            write_empty_tagged_fields(out)?;
        }
        write_empty_tagged_fields(out)?;
    }
    write_empty_tagged_fields(out)
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

/// Serve `InitProducerId` v0–v6.
///
/// A plain idempotent producer always gets a fresh producer ID at epoch 0.
/// A transactional ID runs the coordinator's rules (KIP-98, KIP-360):
///
/// - A new transactional ID gets a new producer ID at epoch 0; naming a
///   producer ID for one is `INVALID_PRODUCER_ID_MAPPING`.
/// - A known one keeps its producer ID and gets the next epoch, which fences
///   the previous incarnation. When the request names the producer ID and
///   epoch it holds (v3+), the epoch must be the current one; the previous
///   one is a retry and gets the current epoch back; anything else is
///   `PRODUCER_FENCED` (`INVALID_PRODUCER_EPOCH` below v4).
/// - An open transaction is aborted first, with its markers written at a
///   bumped epoch, and the request is answered `CONCURRENT_TRANSACTIONS`; the
///   retry gets the next epoch. With `keep_prepared_txn` (v6, KIP-939) the
///   transaction is kept and reported instead.
/// - While markers are pending the answer is `CONCURRENT_TRANSACTIONS`.
fn init_producer_id(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = InitProducerIdReq::read(body, api_version)?;

    let mut ongoing = (-1, -1);
    let outcome = match &req.transactional_id {
        None => Ok(state.allocate_producer_id()),
        Some(transactional_id) => {
            init_transactional_producer(state, transactional_id, &req, api_version, node_id).map(
                |(identity, kept)| {
                    if let Some(kept) = kept {
                        ongoing = kept;
                    }
                    identity
                },
            )
        }
    };
    let (code, (producer_id, producer_epoch)) = match outcome {
        Ok(identity) => (ErrorCode::None, identity),
        Err(code) => (code, (-1, -1)),
    };
    write_init_producer_id(
        out,
        api_version,
        state.throttle(ApiKey::InitProducerId),
        code,
        producer_id,
        producer_epoch,
        ongoing,
    )
}

/// The coordinator half of [`init_producer_id`]: the identity to return,
/// and the open transaction kept by `keep_prepared_txn`, if any.
#[allow(clippy::type_complexity)]
fn init_transactional_producer(
    state: &mut ClusterState,
    transactional_id: &str,
    req: &InitProducerIdReq,
    api_version: i16,
    node_id: i32,
) -> std::result::Result<((i64, i16), Option<(i64, i16)>), ErrorCode> {
    if let Some(code) = txn_coordinator_check(state, transactional_id, node_id) {
        return Err(code);
    }
    if req.transaction_timeout_ms <= 0
        || req.transaction_timeout_ms > state.transaction_max_timeout_ms
    {
        return Err(ErrorCode::InvalidTransactionTimeout);
    }
    let fenced = fenced_code(ApiKey::InitProducerId, api_version);
    let expected = (req.producer_id >= 0 && req.producer_epoch >= 0)
        .then_some((req.producer_id, req.producer_epoch));

    let Some(txn) = state.transactions.get(transactional_id) else {
        if expected.is_some() {
            return Err(ErrorCode::InvalidProducerIdMapping);
        }
        let (producer_id, producer_epoch) = state.allocate_producer_id();
        state.transactions.insert(
            transactional_id.to_string(),
            BrokerTransaction {
                producer_id,
                producer_epoch,
                last_producer_epoch: -1,
                transaction_timeout_ms: req.transaction_timeout_ms,
                ..BrokerTransaction::default()
            },
        );
        return Ok(((producer_id, producer_epoch), None));
    };

    if let Some((producer_id, _)) = expected
        && producer_id != txn.producer_id
    {
        return Err(ErrorCode::InvalidProducerIdMapping);
    }
    match txn.status {
        TxnStatus::PrepareCommit | TxnStatus::PrepareAbort => {
            return Err(ErrorCode::ConcurrentTransactions);
        }
        TxnStatus::Ongoing if req.keep_prepared_txn => {
            let identity = (txn.producer_id, txn.producer_epoch);
            return Ok((identity, Some(identity)));
        }
        TxnStatus::Ongoing => {
            state.fence_transaction(transactional_id);
            return Err(ErrorCode::ConcurrentTransactions);
        }
        _ => {}
    }

    let Some(txn) = state.transactions.get_mut(transactional_id) else {
        return Err(ErrorCode::InvalidProducerIdMapping);
    };
    match expected {
        Some((_, epoch)) if epoch == txn.producer_epoch => txn.bump_epoch(),
        // The bump this producer asked for already happened; its response was
        // lost.
        Some((_, epoch)) if epoch == txn.last_producer_epoch => {}
        Some(_) => return Err(fenced),
        None => txn.bump_epoch(),
    }
    txn.transaction_timeout_ms = req.transaction_timeout_ms;
    Ok(((txn.producer_id, txn.producer_epoch), None))
}

/// Write an `InitProducerId` response for any version from 0 to 6.
fn write_init_producer_id(
    out: &mut BytesMut,
    api_version: i16,
    throttle_time_ms: i32,
    code: ErrorCode,
    producer_id: i64,
    producer_epoch: i16,
    ongoing: (i64, i16),
) -> Result<()> {
    out.put_i32(throttle_time_ms);
    write_error(out, code);
    out.put_i64(producer_id);
    out.put_i16(producer_epoch);
    if api_version >= 6 {
        out.put_i64(ongoing.0);
        out.put_i16(ongoing.1);
    }
    if api_version >= 2 {
        write_empty_tagged_fields(out)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Transactions (KIP-98, KIP-360, KIP-447, KIP-890)
// ---------------------------------------------------------------------------

/// The error a transaction coordinator answers a fenced producer with:
/// `PRODUCER_FENCED` from the versions that know it, `INVALID_PRODUCER_EPOCH`
/// before them.
fn fenced_code(api_key: ApiKey, api_version: i16) -> ErrorCode {
    let first = match api_key {
        ApiKey::InitProducerId => 4,
        ApiKey::AddPartitionsToTxn | ApiKey::AddOffsetsToTxn | ApiKey::EndTxn => 2,
        _ => i16::MAX,
    };
    if api_version >= first {
        ErrorCode::ProducerFenced
    } else {
        ErrorCode::InvalidProducerEpoch
    }
}

/// Reject a request that reached a broker which does not coordinate this
/// transactional ID.
fn txn_coordinator_check(
    state: &ClusterState,
    transactional_id: &str,
    node_id: i32,
) -> Option<ErrorCode> {
    let coordinator = state.txn_coordinator(transactional_id);
    if coordinator == node_id {
        None
    } else if state.broker(coordinator).map(|b| b.online) == Some(true) {
        Some(ErrorCode::NotCoordinator)
    } else {
        Some(ErrorCode::CoordinatorNotAvailable)
    }
}

/// Reject a request that adds to a transaction, in the order a real
/// coordinator checks:
///
/// 1. **Misrouted** — the request reached a broker that does not coordinate
///    this transactional ID.
/// 2. **Unknown producer** — the producer ID is not the one this coordinator
///    assigned (`INVALID_PRODUCER_ID_MAPPING`).
/// 3. **Fenced** — the epoch is not the current one, so this producer is a
///    zombie and every write it attempts must fail.
/// 4. **Markers in flight** — the previous transaction is still completing
///    (`CONCURRENT_TRANSACTIONS`, retriable).
fn txn_check(
    state: &ClusterState,
    api_key: ApiKey,
    api_version: i16,
    transactional_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    node_id: i32,
) -> Option<ErrorCode> {
    if let Some(code) = txn_coordinator_check(state, transactional_id, node_id) {
        return Some(code);
    }
    match state.transactions.get(transactional_id) {
        None => Some(ErrorCode::InvalidProducerIdMapping),
        Some(txn) if txn.producer_id != producer_id => Some(ErrorCode::InvalidProducerIdMapping),
        Some(txn) if txn.producer_epoch != producer_epoch => {
            Some(fenced_code(api_key, api_version))
        }
        Some(txn)
            if matches!(
                txn.status,
                TxnStatus::PrepareCommit | TxnStatus::PrepareAbort
            ) =>
        {
            Some(ErrorCode::ConcurrentTransactions)
        }
        Some(_) => None,
    }
}

/// Serve `AddPartitionsToTxn` v0 — TV1 only.
///
/// Under TV2 the client never sends this, and a test can prove that by
/// asserting the request count is zero after a transaction.
fn add_partitions_to_txn(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = AddPartitionsToTxnReq::read(body)?;
    let rejection = txn_check(
        state,
        ApiKey::AddPartitionsToTxn,
        api_version,
        &req.transactional_id,
        req.producer_id,
        req.producer_epoch,
        node_id,
    );

    if rejection.is_none()
        && let Some(txn) = state.transactions.get_mut(&req.transactional_id)
    {
        txn.begin();
        for entry in &req.partitions {
            if !txn.partitions.contains(entry) {
                txn.partitions.push(entry.clone());
            }
        }
    }

    // Response v0: throttle_time_ms, then results grouped by topic.
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for (topic, partition) in &req.partitions {
        match by_topic.iter_mut().find(|(name, _)| name == topic) {
            Some((_, partitions)) => partitions.push(*partition),
            None => by_topic.push((topic.clone(), vec![*partition])),
        }
    }

    out.put_i32(0); // throttle_time_ms
    write_array_len(out, by_topic.len())?;
    for (topic, partitions) in &by_topic {
        write_string(out, topic)?;
        write_array_len(out, partitions.len())?;
        for partition in partitions {
            out.put_i32(*partition);
            write_error(out, rejection.unwrap_or(ErrorCode::None));
        }
    }
    Ok(())
}

/// Serve `AddOffsetsToTxn` v0 — TV1 only.
fn add_offsets_to_txn(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = AddOffsetsToTxnReq::read(body)?;
    let rejection = txn_check(
        state,
        ApiKey::AddOffsetsToTxn,
        api_version,
        &req.transactional_id,
        req.producer_id,
        req.producer_epoch,
        node_id,
    );

    if rejection.is_none()
        && let Some(txn) = state.transactions.get_mut(&req.transactional_id)
    {
        txn.begin();
        txn.staged_offsets.entry(req.group_id).or_default();
    }

    out.put_i32(0); // throttle_time_ms
    write_error(out, rejection.unwrap_or(ErrorCode::None));
    Ok(())
}

/// Serve `TxnOffsetCommit` v3–v5.
///
/// The offsets are **staged**, not committed: they become visible to
/// `OffsetFetch` only when `EndTxn(commit)` applies them. An aborted
/// transaction must leave the group's committed offsets exactly as it found
/// them — which is the half of exactly-once a produce-only test never reaches.
fn txn_offset_commit(
    body: &mut Bytes,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = TxnOffsetCommitReq::read(body)?;

    // Routed to the **group** coordinator, not the transaction coordinator.
    let rejection = coordinator_check(state, &req.group_id, node_id).or_else(|| {
        match state.transactions.get(&req.transactional_id) {
            None => Some(ErrorCode::InvalidProducerIdMapping),
            Some(txn) if txn.producer_id != req.producer_id => {
                Some(ErrorCode::InvalidProducerIdMapping)
            }
            Some(txn) if txn.producer_epoch != req.producer_epoch => {
                Some(ErrorCode::InvalidProducerEpoch)
            }
            // KIP-447: a committer whose group generation is stale, or whose
            // member ID the group no longer knows, has been rebalanced out.
            // Its offsets would resurrect a position the new owner has already
            // moved past, so both halves of the fencing triple are checked —
            // a coordinator that reads only the generation lets a member
            // evicted within one generation keep committing.
            Some(_) => state.groups.get(&req.group_id).and_then(|group| {
                if req.generation_id >= 0 && group.generation_id != req.generation_id {
                    Some(ErrorCode::IllegalGeneration)
                } else if !req.member_id.is_empty()
                    && !group.members.is_empty()
                    && !group.members.iter().any(|m| {
                        m.member_id == req.member_id
                            || (req.group_instance_id.is_some()
                                && m.group_instance_id == req.group_instance_id)
                    })
                {
                    Some(ErrorCode::UnknownMemberId)
                } else {
                    None
                }
            }),
        }
    });

    if rejection.is_none()
        && let Some(txn) = state.transactions.get_mut(&req.transactional_id)
    {
        txn.begin();
        let staged = txn.staged_offsets.entry(req.group_id.clone()).or_default();
        for offset in &req.offsets {
            staged.insert(
                (offset.topic.clone(), offset.partition),
                CommittedOffset {
                    offset: offset.committed_offset,
                    leader_epoch: offset.committed_leader_epoch,
                    metadata: offset.metadata.clone(),
                },
            );
        }
    }

    // Response v3+: flexible, results grouped by topic.
    let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
    for offset in &req.offsets {
        match by_topic.iter_mut().find(|(name, _)| *name == offset.topic) {
            Some((_, partitions)) => partitions.push(offset.partition),
            None => by_topic.push((offset.topic.clone(), vec![offset.partition])),
        }
    }

    out.put_i32(0); // throttle_time_ms
    write_compact_array_len(out, by_topic.len())?;
    for (topic, partitions) in &by_topic {
        KafkaString::new(topic).try_encode_compact(out)?;
        write_compact_array_len(out, partitions.len())?;
        for partition in partitions {
            out.put_i32(*partition);
            write_error(out, rejection.unwrap_or(ErrorCode::None));
            write_empty_tagged_fields(out)?;
        }
        write_empty_tagged_fields(out)?;
    }
    write_empty_tagged_fields(out)
}

/// Serve `EndTxn` v3–v5.
///
/// The protocol is read from the request version, as Kafka's coordinator
/// reads it: v5 is TV2 (KIP-890), v4 and below TV1.
///
/// Ending an `Ongoing` transaction writes its markers (see
/// [`ClusterState::write_transaction_markers`]) — under TV2 at a bumped
/// epoch, which the v5 response returns. Everything else follows the
/// coordinator's state table:
///
/// | State | Same epoch, same outcome | Same epoch, other outcome |
/// |---|---|---|
/// | `PrepareCommit` / `PrepareAbort` | `CONCURRENT_TRANSACTIONS` | `INVALID_TXN_STATE` |
/// | `CompleteCommit` / `CompleteAbort` | `NONE` (a retry) | `INVALID_TXN_STATE` |
/// | `Empty` | — | `INVALID_TXN_STATE` |
///
/// Under TV2 "same epoch" for a retry is the epoch before the bump, and an
/// abort with no transaction open bumps the epoch and succeeds. Any other
/// epoch is `PRODUCER_FENCED`.
fn end_txn(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = EndTxnReq::read(body)?;
    let outcome = end_txn_outcome(state, &req, api_version, node_id);

    let (producer_id, producer_epoch) =
        match (&outcome, state.transactions.get(&req.transactional_id)) {
            (None, Some(txn)) => (txn.producer_id, txn.producer_epoch),
            _ => (-1, -1),
        };
    out.put_i32(state.throttle(ApiKey::EndTxn));
    write_error(out, outcome.unwrap_or(ErrorCode::None));
    if api_version >= 5 {
        out.put_i64(producer_id);
        out.put_i16(producer_epoch);
    }
    write_empty_tagged_fields(out)
}

/// Apply an `EndTxn` to the coordinator's state, returning the error to
/// answer with, if any.
fn end_txn_outcome(
    state: &mut ClusterState,
    req: &EndTxnReq,
    api_version: i16,
    node_id: i32,
) -> Option<ErrorCode> {
    if let Some(code) = txn_coordinator_check(state, &req.transactional_id, node_id) {
        return Some(code);
    }
    let Some(txn) = state.transactions.get_mut(&req.transactional_id) else {
        return Some(ErrorCode::InvalidProducerIdMapping);
    };
    if txn.producer_id != req.producer_id {
        return Some(ErrorCode::InvalidProducerIdMapping);
    }
    let tv2 = api_version >= 5;
    let current = req.producer_epoch == txn.producer_epoch;
    let retry =
        tv2 && txn.last_producer_epoch >= 0 && req.producer_epoch == txn.last_producer_epoch;
    let same_outcome = |committed: bool| committed == req.committed;
    let status = txn.status;

    match status {
        TxnStatus::Ongoing if current => {
            state.end_transaction(&req.transactional_id, req.committed, tv2);
            None
        }
        TxnStatus::PrepareCommit | TxnStatus::PrepareAbort if (current && !tv2) || retry => {
            if same_outcome(status == TxnStatus::PrepareCommit) {
                Some(ErrorCode::ConcurrentTransactions)
            } else {
                Some(ErrorCode::InvalidTxnState)
            }
        }
        TxnStatus::CompleteCommit | TxnStatus::CompleteAbort if (current && !tv2) || retry => {
            if same_outcome(status == TxnStatus::CompleteCommit) {
                None
            } else {
                Some(ErrorCode::InvalidTxnState)
            }
        }
        TxnStatus::Empty | TxnStatus::CompleteCommit | TxnStatus::CompleteAbort
            if current && tv2 && !req.committed =>
        {
            if let Some(txn) = state.transactions.get_mut(&req.transactional_id) {
                txn.bump_epoch();
                txn.status = TxnStatus::CompleteAbort;
            }
            None
        }
        _ if current => Some(ErrorCode::InvalidTxnState),
        _ => Some(fenced_code(ApiKey::EndTxn, api_version)),
    }
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
        let code = if state.delete_topic(name) {
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

    if req.member_id.is_empty() {
        return write_heartbeat_error(out, ErrorCode::InvalidRequest, None, 0);
    }

    if req.member_epoch < 0 {
        if let Some(group) = state.share_groups.get_mut(&req.group_id) {
            group.members.remove(&req.member_id);
            group.group_epoch += 1;
            // Whatever the departing member was holding goes back to the pool.
            group.release_member(&req.member_id);
        }
        return write_share_heartbeat(out, &req.member_id, req.member_epoch, None);
    }

    // A member joins by naming what it subscribes to; a null subscription
    // means "unchanged", which a joining member has nothing to be unchanged
    // from.
    if req.member_epoch == 0 && req.subscribed_topic_names.is_none() {
        return write_heartbeat_error(out, ErrorCode::InvalidRequest, Some(&req.member_id), 0);
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
        // A (re)joining member always receives its assignment.
        let dirty = member.assignment_dirty || is_new || rejoining;
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

/// Validate and apply every acknowledgement batch a share request carried for
/// one partition, returning the error to report for it.
///
/// A batch whose `acknowledge_types` array has one entry applies that type to
/// the whole range; otherwise there must be exactly one type per offset, which
/// is what the KIP-932 format specifies. Anything else, an unknown type, or
/// `RENEW` below v2 is `INVALID_REQUEST`. Every acknowledged record must be
/// held by `member_id`, or the whole partition's acknowledgements are refused
/// with `INVALID_RECORD_STATE` and nothing changes, as on a broker.
#[allow(clippy::too_many_arguments)]
fn apply_share_acks(
    state: &mut ClusterState,
    api_version: i16,
    group_id: &str,
    member_id: &str,
    topic: &str,
    partition: i32,
    batches: &[ShareAckBatch],
) -> ErrorCode {
    let mut plan: Vec<(i64, i8)> = Vec::new();
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
        for (i, &ack_type) in types.iter().enumerate() {
            if !(0..=4).contains(&ack_type) || (ack_type == 4 && api_version < 2) {
                return ErrorCode::InvalidRequest;
            }
            plan.push((batch.first_offset + i as i64, ack_type));
        }
    }
    if plan.is_empty() {
        return ErrorCode::None;
    }
    let share_partition = state
        .share_groups
        .entry(group_id.to_string())
        .or_default()
        .partitions
        .entry((topic.to_string(), partition))
        .or_default();
    if !plan
        .iter()
        .all(|&(offset, _)| share_partition.held_by(offset, offset, member_id))
    {
        return ErrorCode::InvalidRecordState;
    }
    for (offset, ack_type) in plan {
        share_partition.acknowledge(offset, ack_type);
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
/// Returns `(group_id, member_id)` when both are present and non-empty.
fn required_share_identity(
    group_id: &Option<String>,
    member_id: &Option<String>,
) -> Option<(String, String)> {
    let group = group_id.as_deref().filter(|g| !g.is_empty())?;
    let member = member_id.as_deref().filter(|m| !m.is_empty())?;
    Some((group.to_string(), member.to_string()))
}

/// Record a request that closes its share session (epoch `-1`).
fn record_share_session_close(
    state: &mut ClusterState,
    api_key: ApiKey,
    node_id: i32,
    group_id: &str,
    member_id: &str,
    share_session_epoch: i32,
) {
    if share_session_epoch == -1 {
        state.share_session_closes.push(ShareSessionClose {
            api_key,
            node_id,
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
        });
    }
}

/// Apply the share-session rules to one request, returning the top-level
/// error to answer with, if any (KIP-932):
///
/// - Epoch `0` opens a session (replacing any the member had on this broker).
///   A `ShareFetch` opening one must not carry acknowledgements
///   (`INVALID_REQUEST`); a `ShareAcknowledge` cannot open one
///   (`INVALID_SHARE_SESSION_EPOCH`).
/// - Any other epoch needs an open session (`SHARE_SESSION_NOT_FOUND`); a
///   positive one must be the session's next epoch
///   (`INVALID_SHARE_SESSION_EPOCH`), which then advances. Epoch `-1` closes
///   the session.
#[allow(clippy::too_many_arguments)]
fn check_share_session(
    state: &mut ClusterState,
    api_key: ApiKey,
    node_id: i32,
    group_id: &str,
    member_id: &str,
    epoch: i32,
    has_acks: bool,
    requested: &[(String, i32)],
    forgotten: &[(String, i32)],
) -> Option<ErrorCode> {
    let key = (node_id, group_id.to_string(), member_id.to_string());
    if epoch == 0 {
        if api_key == ApiKey::ShareAcknowledge {
            return Some(ErrorCode::InvalidShareSessionEpoch);
        }
        if has_acks {
            return Some(ErrorCode::InvalidRequest);
        }
        state.share_sessions.insert(
            key,
            ShareSession {
                epoch: 1,
                partitions: requested.iter().cloned().collect(),
            },
        );
        return None;
    }
    let Some(session) = state.share_sessions.get_mut(&key) else {
        return Some(ErrorCode::ShareSessionNotFound);
    };
    if epoch == -1 {
        state.share_sessions.remove(&key);
        return None;
    }
    if epoch != session.epoch {
        return Some(ErrorCode::InvalidShareSessionEpoch);
    }
    session.advance();
    session.partitions.extend(requested.iter().cloned());
    for entry in forgotten {
        session.partitions.remove(entry);
    }
    None
}

/// Write a `ShareFetch` response carrying only a top-level error.
fn write_share_fetch_error(out: &mut BytesMut, code: ErrorCode, message: &str) -> Result<()> {
    out.put_i32(0); // throttle_time_ms
    write_error(out, code);
    write_compact_nullable_string(out, Some(message))?;
    out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);
    write_compact_array_len(out, 0)?; // responses
    write_compact_array_len(out, 0)?; // node_endpoints
    write_empty_tagged_fields(out)
}

/// Write a `ShareAcknowledge` response carrying only a top-level error.
fn write_share_acknowledge_error(
    out: &mut BytesMut,
    api_version: i16,
    code: ErrorCode,
    message: &str,
) -> Result<()> {
    out.put_i32(0); // throttle_time_ms
    write_error(out, code);
    write_compact_nullable_string(out, Some(message))?;
    if api_version >= 2 {
        out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);
    }
    write_compact_array_len(out, 0)?; // responses
    write_compact_array_len(out, 0)?; // node_endpoints
    write_empty_tagged_fields(out)
}

/// Resolve `(topic_id, partition)` pairs to names, dropping unknown IDs.
fn share_partition_names(state: &ClusterState, ids: &[([u8; 16], i32)]) -> Vec<(String, i32)> {
    ids.iter()
        .filter_map(|(topic_id, partition)| {
            topic_name_for_id(state, *topic_id).map(|name| (name, *partition))
        })
        .collect()
}

/// Acquire records of one share partition for `member_id`, returning the
/// batches to send and the acquired ranges as
/// `(first_offset, last_offset, delivery_count)`.
///
/// Whole batches are returned, as a broker returns them; only the records in
/// the acquired ranges belong to the member. `budget` is the request's
/// remaining `max_records`: in batch-optimized mode (KIP-1206 mode 0) it is
/// checked between batches and may be overshot to finish one, in
/// record-limit mode (1) it is exact.
fn acquire_share_records(
    state: &mut ClusterState,
    group_id: &str,
    member_id: &str,
    topic: &str,
    partition: i32,
    budget: &mut i64,
    record_limit: bool,
) -> (Bytes, Vec<(i64, i64, i16)>) {
    let Some(log) = state.partition(topic, partition).map(|p| p.log.clone()) else {
        return (Bytes::new(), Vec::new());
    };
    let share_partition = state
        .share_groups
        .entry(group_id.to_string())
        .or_default()
        .partitions
        .entry((topic.to_string(), partition))
        .or_default();

    let mut records = Vec::new();
    let mut acquired: Vec<(i64, i16)> = Vec::new();
    for batch in &log {
        if *budget <= 0 {
            break;
        }
        let base = batch_base_offset(batch).unwrap_or(0);
        let count = batch_record_count(batch).unwrap_or(0);
        let mut taken = 0;
        for offset in base..base + count {
            if record_limit && taken >= *budget {
                break;
            }
            if share_partition.is_available(offset) {
                acquired.push((offset, share_partition.acquire(offset, member_id)));
                taken += 1;
            }
        }
        if taken > 0 {
            records.extend_from_slice(batch);
            *budget -= taken;
        }
    }

    let mut ranges: Vec<(i64, i64, i16)> = Vec::new();
    for (offset, delivery_count) in acquired {
        match ranges.last_mut() {
            Some((_, last, count)) if *last + 1 == offset && *count == delivery_count => {
                *last = offset;
            }
            _ => ranges.push((offset, offset, delivery_count)),
        }
    }
    (Bytes::from(records), ranges)
}

/// Serve a `ShareFetch` (API key 78, v1–v2).
///
/// The session check and the piggybacked acknowledgements run first, once,
/// which is the ordering a real broker uses and the reason a client can
/// accept a batch and fetch the next one in a single round trip. Then records
/// are acquired for the member across the requested partitions and any other
/// partition in its session, up to `max_records`. With nothing to acquire the
/// request is held for up to `max_wait_ms`. A closing request (epoch `-1`)
/// and a KIP-1222 renew request (`IsRenewAck`) acquire nothing and do not
/// wait; closing releases every record the member still holds.
fn share_fetch(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    poll: &mut LongPoll,
    out: &mut BytesMut,
) -> Result<Served> {
    let req = ShareFetchReq::read(body, api_version)?;
    let Some((group_id, member_id)) = required_share_identity(&req.group_id, &req.member_id) else {
        write_share_fetch_error(
            out,
            ErrorCode::InvalidRequest,
            "ShareFetch requires a group ID and member ID",
        )?;
        return Ok(Served::Done);
    };

    let ack_errors = match poll.share_acks.take() {
        Some(acks) => acks,
        None => {
            record_share_session_close(
                state,
                ApiKey::ShareFetch,
                node_id,
                &group_id,
                &member_id,
                req.share_session_epoch,
            );
            let requested: Vec<([u8; 16], i32)> = req
                .topics
                .iter()
                .map(|tp| (tp.topic_id, tp.partition_index))
                .collect();
            let requested = share_partition_names(state, &requested);
            let forgotten = share_partition_names(state, &req.forgotten);
            let has_acks = req
                .topics
                .iter()
                .any(|tp| !tp.acknowledgement_batches.is_empty());
            if let Some(code) = check_share_session(
                state,
                ApiKey::ShareFetch,
                node_id,
                &group_id,
                &member_id,
                req.share_session_epoch,
                has_acks,
                &requested,
                &forgotten,
            ) {
                write_share_fetch_error(out, code, &format!("{code:?}"))?;
                return Ok(Served::Done);
            }
            let mut acks = HashMap::new();
            for entry in &req.topics {
                let Some(topic) = topic_name_for_id(state, entry.topic_id) else {
                    continue;
                };
                if state
                    .partition(&topic, entry.partition_index)
                    .is_none_or(|p| p.leader != node_id)
                {
                    continue;
                }
                let code = apply_share_acks(
                    state,
                    api_version,
                    &group_id,
                    &member_id,
                    &topic,
                    entry.partition_index,
                    &entry.acknowledgement_batches,
                );
                acks.insert((entry.topic_id, entry.partition_index), code);
            }
            if req.share_session_epoch == -1
                && let Some(group) = state.share_groups.get_mut(&group_id)
            {
                group.release_member(&member_id);
            }
            acks
        }
    };

    let fetching = req.share_session_epoch != -1 && !req.is_renew_ack;

    // The partitions to answer for: the requested ones, then any other
    // partition of the session.
    let mut targets: Vec<([u8; 16], i32, bool)> = req
        .topics
        .iter()
        .map(|tp| (tp.topic_id, tp.partition_index, true))
        .collect();
    let session_key = (node_id, group_id.clone(), member_id.clone());
    if fetching && let Some(session) = state.share_sessions.get(&session_key) {
        for (topic, partition) in &session.partitions {
            let Some(topic_id) = state.topics.get(topic).map(|t| t.topic_id) else {
                continue;
            };
            if !targets
                .iter()
                .any(|(id, p, _)| *id == topic_id && p == partition)
            {
                targets.push((topic_id, *partition, false));
            }
        }
    }

    let mut budget = if req.max_records > 0 {
        i64::from(req.max_records)
    } else {
        i64::MAX
    };
    let record_limit = req.share_acquire_mode == 1;
    let mut any_error = false;
    let mut any_acquired = false;
    // (topic_id, partition, error, ack_error, leader, epoch, records, acquired)
    type ShareFetchEntry = (
        [u8; 16],
        i32,
        ErrorCode,
        ErrorCode,
        i32,
        i32,
        Bytes,
        Vec<(i64, i64, i16)>,
    );
    let mut entries: Vec<ShareFetchEntry> = Vec::new();
    for (topic_id, partition, requested) in targets {
        let ack_error = ack_errors
            .get(&(topic_id, partition))
            .copied()
            .unwrap_or(ErrorCode::None);
        let Some(topic) = topic_name_for_id(state, topic_id) else {
            any_error = true;
            entries.push((
                topic_id,
                partition,
                ErrorCode::UnknownTopicId,
                ack_error,
                -1,
                -1,
                Bytes::new(),
                Vec::new(),
            ));
            continue;
        };
        let Some((leader, leader_epoch)) = state
            .partition(&topic, partition)
            .map(|p| (p.leader, p.leader_epoch))
        else {
            any_error = true;
            entries.push((
                topic_id,
                partition,
                ErrorCode::UnknownTopicOrPartition,
                ack_error,
                -1,
                -1,
                Bytes::new(),
                Vec::new(),
            ));
            continue;
        };
        if leader != node_id {
            any_error = true;
            entries.push((
                topic_id,
                partition,
                ErrorCode::NotLeaderForPartition,
                ack_error,
                leader,
                leader_epoch,
                Bytes::new(),
                Vec::new(),
            ));
            continue;
        }
        let (records, acquired) = if fetching {
            acquire_share_records(
                state,
                &group_id,
                &member_id,
                &topic,
                partition,
                &mut budget,
                record_limit,
            )
        } else {
            (Bytes::new(), Vec::new())
        };
        any_acquired |= !acquired.is_empty();
        if requested || !acquired.is_empty() {
            entries.push((
                topic_id,
                partition,
                ErrorCode::None,
                ack_error,
                leader,
                leader_epoch,
                records,
                acquired,
            ));
        }
    }

    if fetching && !poll.expired && !any_error && !any_acquired && req.max_wait_ms > 0 {
        poll.share_acks = Some(ack_errors);
        return Ok(Served::Wait(Duration::from_millis(req.max_wait_ms as u64)));
    }

    out.put_i32(state.throttle(ApiKey::ShareFetch));
    write_error(out, ErrorCode::None);
    write_compact_nullable_string(out, None)?; // error_message
    out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);

    // Group the entries back into topics, preserving first-seen order so the
    // response mirrors the request.
    let mut order: Vec<[u8; 16]> = Vec::new();
    for entry in &entries {
        if !order.contains(&entry.0) {
            order.push(entry.0);
        }
    }
    write_compact_array_len(out, order.len())?;
    for topic_id in &order {
        out.put_slice(topic_id);
        let topic_entries: Vec<&ShareFetchEntry> =
            entries.iter().filter(|e| e.0 == *topic_id).collect();
        write_compact_array_len(out, topic_entries.len())?;
        for (_, partition, error, ack_error, leader, epoch, records, acquired) in topic_entries {
            write_share_fetch_partition(
                out,
                *partition,
                *error,
                *ack_error,
                *leader,
                *epoch,
                if records.is_empty() {
                    None
                } else {
                    Some(records)
                },
                acquired,
            )?;
        }
        write_empty_tagged_fields(out)?; // topic tagged fields
    }

    write_compact_array_len(out, 0)?; // node_endpoints
    write_empty_tagged_fields(out)?;
    Ok(Served::Done)
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

/// Serve a `ShareAcknowledge` (API key 79, v1–v2), under the same session
/// rules as `ShareFetch`. Closing the session (epoch `-1`) releases every
/// record the member still holds after its acknowledgements apply.
fn share_acknowledge(
    body: &mut Bytes,
    api_version: i16,
    node_id: i32,
    state: &mut ClusterState,
    out: &mut BytesMut,
) -> Result<()> {
    let req = ShareAcknowledgeReq::read(body, api_version)?;
    let Some((group_id, member_id)) = required_share_identity(&req.group_id, &req.member_id) else {
        return write_share_acknowledge_error(
            out,
            api_version,
            ErrorCode::InvalidRequest,
            "ShareAcknowledge requires a group ID and member ID",
        );
    };
    record_share_session_close(
        state,
        ApiKey::ShareAcknowledge,
        node_id,
        &group_id,
        &member_id,
        req.share_session_epoch,
    );
    if let Some(code) = check_share_session(
        state,
        ApiKey::ShareAcknowledge,
        node_id,
        &group_id,
        &member_id,
        req.share_session_epoch,
        true,
        &[],
        &[],
    ) {
        return write_share_acknowledge_error(out, api_version, code, &format!("{code:?}"));
    }

    out.put_i32(state.throttle(ApiKey::ShareAcknowledge));
    write_error(out, ErrorCode::None);
    write_compact_nullable_string(out, None)?; // error_message
    if api_version >= 2 {
        out.put_i32(ACQUISITION_LOCK_TIMEOUT_MS);
    }

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
                            api_version,
                            &group_id,
                            &member_id,
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

    if req.share_session_epoch == -1
        && let Some(group) = state.share_groups.get_mut(&group_id)
    {
        group.release_member(&member_id);
    }

    write_compact_array_len(out, 0)?; // node_endpoints
    write_empty_tagged_fields(out)
}

/// Whether an injected share-API error belongs on each partition rather than
/// at the top level of the response.
fn share_partition_scoped(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::NotLeaderForPartition
            | ErrorCode::UnknownTopicOrPartition
            | ErrorCode::UnknownTopicId
            | ErrorCode::FencedLeaderEpoch
            | ErrorCode::UnknownLeaderEpoch
            | ErrorCode::InvalidRecordState
    )
}

/// Write the topics array of a share response with one entry per requested
/// partition, grouped by topic in request order.
fn write_share_partitions(
    out: &mut BytesMut,
    topics: &[ShareTopicPartitionAcks],
    mut write_partition: impl FnMut(&mut BytesMut, i32) -> Result<()>,
) -> Result<()> {
    let mut order: Vec<[u8; 16]> = Vec::new();
    for tp in topics {
        if !order.contains(&tp.topic_id) {
            order.push(tp.topic_id);
        }
    }
    write_compact_array_len(out, order.len())?;
    for topic_id in &order {
        out.put_slice(topic_id);
        let partitions: Vec<i32> = topics
            .iter()
            .filter(|tp| tp.topic_id == *topic_id)
            .map(|tp| tp.partition_index)
            .collect();
        write_compact_array_len(out, partitions.len())?;
        for partition in partitions {
            write_partition(out, partition)?;
        }
        write_empty_tagged_fields(out)?;
    }
    Ok(())
}

// ── UpdateFeatures (KIP-584) ─────────────────────────────────────────────

/// Serve an `UpdateFeatures` (API key 57).
///
/// Controller-only: the fake broker answers `NOT_CONTROLLER` from any other
/// node, exactly as a real one does.
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
        let mut keys: Vec<i16> = versions.iter().map(|(k, _, _)| k.to_i16()).collect();
        keys.sort_unstable();
        let unique = {
            let mut u = keys.clone();
            u.dedup();
            u
        };
        assert_eq!(keys, unique, "an API is advertised more than once");

        assert!(
            versions.iter().any(|(k, _, _)| *k == ApiKey::ApiVersions),
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
