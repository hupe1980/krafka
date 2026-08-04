//! `StreamsGroupDescribe` (API key 89) — KIP-1071.
//!
//! # Why only the describe half of KIP-1071
//!
//! KIP-1071 adds two APIs. `StreamsGroupHeartbeat` (key 88) is a *runtime*
//! API: a member sends its topology — subtopologies, repartition topics,
//! changelog topics — and the coordinator assigns it tasks. Sending it
//! requires being a Streams runtime, which krafka is not, and a client that
//! sent a fabricated topology would corrupt the group's shared topology
//! metadata for every real member.
//!
//! `StreamsGroupDescribe` is purely observational and is what an operator
//! actually needs: which members exist, what tasks they hold, and how far
//! behind their changelogs are. It is implemented here; key 88 remains a
//! documented gap in `xtask/protocol_parity.py`.
//!
//! # Wire notes
//!
//! Flexible from v0, so every string and array is compact and every struct is
//! followed by a tagged-field section. Three details are easy to get wrong:
//!
//! * `Topology` and `UserEndpoint` are **nullable structs**. A non-tagged
//!   nullable struct in a flexible version is preceded by a single presence
//!   byte — `-1` for null, `1` for present — not by an array length.
//! * `Subtopologies` is a **nullable array** nested inside `Topology`, so a
//!   present topology can still carry no subtopologies (the group is
//!   uninitialised, or its source topics are missing).
//! * `Endpoint.Port` is `uint16`, not `int16`. Decoding it signed turns any
//!   port above 32767 negative.

use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};

/// Request to describe Streams groups (KIP-1071).
#[derive(Debug, Clone)]
pub struct StreamsGroupDescribeRequest {
    /// The IDs of the groups to describe.
    pub group_ids: Vec<String>,
    /// Whether to include the authorized-operations bitfield.
    pub include_authorized_operations: bool,
}

impl StreamsGroupDescribeRequest {
    /// Create a request for the given group IDs.
    #[must_use]
    pub fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            include_authorized_operations: false,
        }
    }

    /// Request the authorized-operations bitfield alongside each group.
    #[must_use]
    pub fn with_authorized_operations(mut self, include: bool) -> Self {
        self.include_authorized_operations = include;
        self
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.group_ids.len(), buf)?;
        for id in &self.group_ids {
            KafkaString::new(id).try_encode_compact(buf)?;
        }
        buf.put_u8(u8::from(self.include_authorized_operations));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// A user-defined endpoint for Interactive Queries.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsEndpoint {
    /// Endpoint host.
    pub host: String,
    /// Endpoint port. Decoded as unsigned — the wire type is `uint16`.
    pub port: u16,
}

/// A key/value pair, used for client tags and topic configs.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsKeyValue {
    /// Key.
    pub key: String,
    /// Value.
    pub value: String,
}

/// An internally-created topic associated with a subtopology.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsTopicInfo {
    /// Topic name.
    pub name: String,
    /// Partition count, or `0` when no specific count is enforced. Always `0`
    /// for changelog topics.
    pub partitions: i32,
    /// Replication factor, or `0` to use the cluster default.
    pub replication_factor: i16,
    /// Topic-level configuration overrides.
    pub topic_configs: Vec<StreamsKeyValue>,
}

/// One subtopology of a Streams application.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsSubtopology {
    /// Identifier, unique within the topology.
    pub subtopology_id: String,
    /// Topics this subtopology reads from.
    pub source_topics: Vec<String>,
    /// Repartition topics this subtopology writes to.
    pub repartition_sink_topics: Vec<String>,
    /// State changelog topics, created automatically.
    pub state_changelog_topics: Vec<StreamsTopicInfo>,
    /// Source topics that are internally-created repartition topics.
    pub repartition_source_topics: Vec<StreamsTopicInfo>,
}

/// The topology currently initialized for a Streams group.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsTopology {
    /// Epoch of the initialized topology.
    pub epoch: i32,
    /// Subtopologies, or `None` when the group is uninitialized or its source
    /// topics are missing or incorrectly partitioned.
    ///
    /// `None` and `Some(vec![])` are different states and the wire format
    /// distinguishes them, so this is an `Option<Vec<_>>` rather than a `Vec`
    /// that is empty in both cases.
    pub subtopologies: Option<Vec<StreamsSubtopology>>,
}

/// A set of task IDs within one subtopology.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsTaskIds {
    /// Subtopology identifier.
    pub subtopology_id: String,
    /// Partitions of the input topics processed by this member.
    pub partitions: Vec<i32>,
}

/// A member's task assignment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsAssignment {
    /// Active tasks.
    pub active_tasks: Vec<StreamsTaskIds>,
    /// Standby tasks.
    pub standby_tasks: Vec<StreamsTaskIds>,
    /// Warm-up tasks.
    pub warmup_tasks: Vec<StreamsTaskIds>,
}

/// A cumulative changelog offset for one task.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamsTaskOffset {
    /// Subtopology identifier.
    pub subtopology_id: String,
    /// Partition.
    pub partition: i32,
    /// Offset.
    pub offset: i64,
}

/// A member of a Streams group.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamsGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Member epoch.
    pub member_epoch: i32,
    /// Static membership instance ID, if any.
    pub instance_id: Option<String>,
    /// Rack ID, if any.
    pub rack_id: Option<String>,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Epoch of the topology held by this client.
    ///
    /// A value below the group's [`StreamsTopology::epoch`] means this member
    /// is still running an older topology and has not caught up.
    pub topology_epoch: i32,
    /// Identity of the Streams instance, which may host multiple clients.
    pub process_id: String,
    /// User-defined Interactive Queries endpoint, if configured.
    pub user_endpoint: Option<StreamsEndpoint>,
    /// Client tags used by the rack-aware assignor.
    pub client_tags: Vec<StreamsKeyValue>,
    /// Cumulative changelog offsets per task.
    pub task_offsets: Vec<StreamsTaskOffset>,
    /// Cumulative changelog end offsets per task.
    pub task_end_offsets: Vec<StreamsTaskOffset>,
    /// The assignment this member currently holds.
    pub assignment: StreamsAssignment,
    /// The assignment the coordinator wants it to reach.
    ///
    /// A difference from [`Self::assignment`] means a rebalance is in
    /// progress: the member has not yet finished moving to its target.
    pub target_assignment: StreamsAssignment,
    /// True for a classic-protocol member that has not been upgraded.
    pub is_classic: bool,
}

/// One described Streams group.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DescribedStreamsGroup {
    /// Per-group error, or `None` on success.
    pub error_code: ErrorCode,
    /// Per-group error message, if the broker supplied one.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Group state, or the empty string.
    pub group_state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// The initialized topology, or `None` when the describe failed or the
    /// group has none.
    pub topology: Option<StreamsTopology>,
    /// Members of the group.
    pub members: Vec<StreamsGroupMember>,
    /// Authorized-operations bitfield.
    ///
    /// `i32::MIN` is the broker's "not requested" sentinel, which is what you
    /// get unless the request set `include_authorized_operations`.
    pub authorized_operations: i32,
}

/// Response to a [`StreamsGroupDescribeRequest`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct StreamsGroupDescribeResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described groups, in request order.
    pub groups: Vec<DescribedStreamsGroup>,
}

/// Read a non-tagged nullable struct's presence byte.
///
/// Returns `false` for null. A flexible-version nullable struct is preceded by
/// `-1` (null) or `1` (present) — anything else is malformed rather than
/// something to guess at, because guessing desynchronises the whole frame.
fn read_presence(buf: &mut impl Buf, what: &'static str) -> Result<bool> {
    if buf.remaining() < 1 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::TruncatedFrame,
            format!("not enough bytes for {what} presence tag"),
        ));
    }
    let presence = buf.get_i8();
    if presence < 0 {
        return Ok(false);
    }
    if presence != 1 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("invalid {what} presence tag {presence}: expected -1 (null) or 1 (present)"),
        ));
    }
    Ok(true)
}

fn read_compact_string(buf: &mut impl Buf, field: &'static str) -> Result<String> {
    super::non_nullable_string(field, KafkaString::decode_compact(buf)?.0)
}

fn read_compact_string_array(buf: &mut impl Buf, field: &'static str) -> Result<Vec<String>> {
    let count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
    let mut out = Vec::with_capacity(decode_capacity(count, buf.remaining()));
    for _ in 0..count {
        out.push(read_compact_string(buf, field)?);
    }
    Ok(out)
}

fn read_key_values(buf: &mut impl Buf) -> Result<Vec<StreamsKeyValue>> {
    let count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
    let mut out = Vec::with_capacity(decode_capacity(count, buf.remaining()));
    for _ in 0..count {
        let key = read_compact_string(buf, "KeyValue.Key")?;
        let value = read_compact_string(buf, "KeyValue.Value")?;
        let _ = TaggedFields::decode(buf)?;
        out.push(StreamsKeyValue { key, value });
    }
    Ok(out)
}

fn read_topic_infos(buf: &mut impl Buf) -> Result<Vec<StreamsTopicInfo>> {
    let count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
    let mut out = Vec::with_capacity(decode_capacity(count, buf.remaining()));
    for _ in 0..count {
        let name = read_compact_string(buf, "TopicInfo.Name")?;
        let partitions = i32::decode(buf)?;
        let replication_factor = i16::decode(buf)?;
        let topic_configs = read_key_values(buf)?;
        let _ = TaggedFields::decode(buf)?;
        out.push(StreamsTopicInfo {
            name,
            partitions,
            replication_factor,
            topic_configs,
        });
    }
    Ok(out)
}

fn read_task_ids(buf: &mut impl Buf) -> Result<Vec<StreamsTaskIds>> {
    let count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
    let mut out = Vec::with_capacity(decode_capacity(count, buf.remaining()));
    for _ in 0..count {
        let subtopology_id = read_compact_string(buf, "TaskIds.SubtopologyId")?;
        let p_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut partitions = Vec::with_capacity(decode_capacity(p_count, buf.remaining()));
        for _ in 0..p_count {
            partitions.push(i32::decode(buf)?);
        }
        let _ = TaggedFields::decode(buf)?;
        out.push(StreamsTaskIds {
            subtopology_id,
            partitions,
        });
    }
    Ok(out)
}

fn read_assignment(buf: &mut impl Buf) -> Result<StreamsAssignment> {
    let active_tasks = read_task_ids(buf)?;
    let standby_tasks = read_task_ids(buf)?;
    let warmup_tasks = read_task_ids(buf)?;
    let _ = TaggedFields::decode(buf)?;
    Ok(StreamsAssignment {
        active_tasks,
        standby_tasks,
        warmup_tasks,
    })
}

fn read_task_offsets(buf: &mut impl Buf) -> Result<Vec<StreamsTaskOffset>> {
    let count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
    let mut out = Vec::with_capacity(decode_capacity(count, buf.remaining()));
    for _ in 0..count {
        let subtopology_id = read_compact_string(buf, "TaskOffset.SubtopologyId")?;
        let partition = i32::decode(buf)?;
        let offset = i64::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        out.push(StreamsTaskOffset {
            subtopology_id,
            partition,
            offset,
        });
    }
    Ok(out)
}

fn read_topology(buf: &mut impl Buf) -> Result<Option<StreamsTopology>> {
    if !read_presence(buf, "Topology")? {
        return Ok(None);
    }
    let epoch = i32::decode(buf)?;

    // Nullable *array*: raw 0 means null, which is a different state from an
    // empty array and means "uninitialized, or source topics missing".
    let raw = crate::util::varint::decode_unsigned_varint(buf)?;
    let subtopologies = if raw == 0 {
        None
    } else {
        let count = check_compact_array_len(raw)?;
        let mut out = Vec::with_capacity(decode_capacity(count, buf.remaining()));
        for _ in 0..count {
            let subtopology_id = read_compact_string(buf, "Subtopology.SubtopologyId")?;
            let source_topics = read_compact_string_array(buf, "Subtopology.SourceTopics")?;
            let repartition_sink_topics =
                read_compact_string_array(buf, "Subtopology.RepartitionSinkTopics")?;
            let state_changelog_topics = read_topic_infos(buf)?;
            let repartition_source_topics = read_topic_infos(buf)?;
            let _ = TaggedFields::decode(buf)?;
            out.push(StreamsSubtopology {
                subtopology_id,
                source_topics,
                repartition_sink_topics,
                state_changelog_topics,
                repartition_source_topics,
            });
        }
        Some(out)
    };

    let _ = TaggedFields::decode(buf)?;
    Ok(Some(StreamsTopology {
        epoch,
        subtopologies,
    }))
}

fn read_member(buf: &mut impl Buf) -> Result<StreamsGroupMember> {
    let member_id = read_compact_string(buf, "Member.MemberId")?;
    let member_epoch = i32::decode(buf)?;
    let instance_id = KafkaString::decode_compact(buf)?.0;
    let rack_id = KafkaString::decode_compact(buf)?.0;
    let client_id = read_compact_string(buf, "Member.ClientId")?;
    let client_host = read_compact_string(buf, "Member.ClientHost")?;
    let topology_epoch = i32::decode(buf)?;
    let process_id = read_compact_string(buf, "Member.ProcessId")?;

    let user_endpoint = if read_presence(buf, "UserEndpoint")? {
        let host = read_compact_string(buf, "Endpoint.Host")?;
        if buf.remaining() < 2 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for Endpoint.Port",
            ));
        }
        // `uint16` on the wire. Decoding this as i16 turns every port above
        // 32767 negative.
        let port = buf.get_u16();
        let _ = TaggedFields::decode(buf)?;
        Some(StreamsEndpoint { host, port })
    } else {
        None
    };

    let client_tags = read_key_values(buf)?;
    let task_offsets = read_task_offsets(buf)?;
    let task_end_offsets = read_task_offsets(buf)?;
    let assignment = read_assignment(buf)?;
    let target_assignment = read_assignment(buf)?;

    if buf.remaining() < 1 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::TruncatedFrame,
            "not enough bytes for Member.IsClassic",
        ));
    }
    let is_classic = buf.get_u8() != 0;
    let _ = TaggedFields::decode(buf)?;

    Ok(StreamsGroupMember {
        member_id,
        member_epoch,
        instance_id,
        rack_id,
        client_id,
        client_host,
        topology_epoch,
        process_id,
        user_endpoint,
        client_tags,
        task_offsets,
        task_end_offsets,
        assignment,
        target_assignment,
        is_classic,
    })
}

impl StreamsGroupDescribeResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = read_compact_string(buf, "DescribedGroup.GroupId")?;
            let group_state = read_compact_string(buf, "DescribedGroup.GroupState")?;
            let group_epoch = i32::decode(buf)?;
            let assignment_epoch = i32::decode(buf)?;
            let topology = read_topology(buf)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(decode_capacity(member_count, buf.remaining()));
            for _ in 0..member_count {
                members.push(read_member(buf)?);
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(DescribedStreamsGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                group_epoch,
                assignment_epoch,
                topology,
                members,
                authorized_operations,
            });
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl VersionedEncode for StreamsGroupDescribeRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("StreamsGroupDescribeRequest", version),
        }
    }
}

impl VersionedDecode for StreamsGroupDescribeResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("StreamsGroupDescribeResponse", version),
        }
    }
}
