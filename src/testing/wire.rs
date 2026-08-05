//! Broker-side wire helpers: request readers and response writers.
//!
//! The crate ships encoders for *requests* and decoders for *responses*,
//! because that is the only direction a client needs. A broker needs the
//! mirror image. Rather than duplicating the whole message layer, this module
//! reuses the crate's primitives ([`KafkaString`], [`Decode`], [`TryEncode`],
//! [`TaggedFields`]) and adds only the thin field-order glue for the specific
//! API versions the fake broker speaks.
//!
//! The approach follows `src/protocol/proptests.rs`, which writes a test-local
//! reader for the same reason. Each reader here is the exact mirror of the
//! corresponding `encode_vN` in `src/protocol/messages/`, and each writer the
//! exact mirror of the corresponding `decode_vN`, so a mismatch shows up
//! immediately as a decode failure in a client-driven test.
//!
//! # Version pinning
//!
//! The broker advertises `min == max` for every API it implements, which
//! forces the client's version negotiation onto exactly the version each
//! reader/writer pair here was written against. Adding a version means
//! changing both the advertised range and the codec together. Non-flexible
//! versions are preferred where the client still supports them: they need no
//! tagged-field or compact-length handling, so there is less to get wrong.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedField, TaggedFields, TryEncode,
};

/// Upper bound on any array length read off the wire.
///
/// The fake broker only ever talks to a cooperating in-process client, but a
/// malformed frame must still fail cleanly rather than trying to reserve a
/// multi-gigabyte `Vec`.
const MAX_ARRAY_LEN: usize = 100_000;

/// Read a non-null protocol string.
pub(crate) fn read_string(buf: &mut impl Buf) -> Result<String> {
    KafkaString::decode(buf)?.0.ok_or_else(|| {
        KrafkaError::protocol_kind(ProtocolErrorKind::Malformed, "unexpected null string")
    })
}

/// Read a nullable protocol string.
pub(crate) fn read_nullable_string(buf: &mut impl Buf) -> Result<Option<String>> {
    Ok(KafkaString::decode(buf)?.0)
}

/// Read a nullable protocol byte array.
pub(crate) fn read_nullable_bytes(buf: &mut impl Buf) -> Result<Option<Bytes>> {
    Ok(KafkaBytes::decode(buf)?.0)
}

/// Read a non-null protocol byte array.
pub(crate) fn read_bytes(buf: &mut impl Buf) -> Result<Bytes> {
    read_nullable_bytes(buf)?.ok_or_else(|| {
        KrafkaError::protocol_kind(ProtocolErrorKind::Malformed, "unexpected null bytes")
    })
}

/// Read a non-nullable `i32` array length, rejecting null and absurd sizes.
pub(crate) fn read_array_len(buf: &mut impl Buf) -> Result<usize> {
    let len = i32::decode(buf)?;
    if len < 0 {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("null array length {len} where a non-nullable array was expected"),
        ));
    }
    check_len(len as usize)
}

/// Read a nullable `i32` array length. `-1` maps to `None`.
pub(crate) fn read_nullable_array_len(buf: &mut impl Buf) -> Result<Option<usize>> {
    let len = i32::decode(buf)?;
    if len < 0 {
        return Ok(None);
    }
    Ok(Some(check_len(len as usize)?))
}

fn check_len(len: usize) -> Result<usize> {
    if len > MAX_ARRAY_LEN {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!("array length {len} exceeds the fake broker's safety limit {MAX_ARRAY_LEN}"),
        ));
    }
    Ok(len)
}

/// Write a non-null protocol string.
pub(crate) fn write_string(buf: &mut impl BufMut, value: &str) -> Result<()> {
    KafkaString::new(value).try_encode(buf)
}

/// Write a nullable protocol string.
pub(crate) fn write_nullable_string(buf: &mut impl BufMut, value: Option<&str>) -> Result<()> {
    match value {
        Some(v) => KafkaString::new(v).try_encode(buf),
        None => KafkaString::null().try_encode(buf),
    }
}

/// Write a protocol byte array, or the null marker when `value` is `None`.
pub(crate) fn write_nullable_bytes(buf: &mut impl BufMut, value: Option<&Bytes>) -> Result<()> {
    match value {
        Some(v) => KafkaBytes::new(v.clone()).try_encode(buf),
        None => KafkaBytes::null().try_encode(buf),
    }
}

/// Write an `i32` array length prefix.
pub(crate) fn write_array_len(buf: &mut impl BufMut, len: usize) -> Result<()> {
    let len = i32::try_from(len).map_err(|_| {
        KrafkaError::protocol_kind(ProtocolErrorKind::InvalidLength, "array length exceeds i32")
    })?;
    buf.put_i32(len);
    Ok(())
}

/// Write an error code as its `i16` wire value.
pub(crate) fn write_error(buf: &mut impl BufMut, code: ErrorCode) {
    code.to_i16().encode(buf);
}

// --- flexible (KIP-482) helpers --------------------------------------------
//
// Only the APIs the broker serves on a flexible version need these. Produce is
// one: `CurrentLeader` and `NodeEndpoints` (KIP-951) are tagged fields, and
// tagged fields exist only in the flexible encoding.

/// Read a compact array length (`len + 1` as an unsigned varint).
pub(crate) fn read_compact_array_len(buf: &mut impl Buf) -> Result<usize> {
    let raw = crate::util::varint::decode_unsigned_varint(buf)?;
    if raw == 0 {
        return Ok(0); // null array
    }
    check_len((raw - 1) as usize)
}

/// Write a compact array length.
pub(crate) fn write_compact_array_len(buf: &mut impl BufMut, len: usize) -> Result<()> {
    let len = u32::try_from(len.saturating_add(1)).map_err(|_| {
        KrafkaError::protocol_kind(ProtocolErrorKind::InvalidLength, "array length exceeds u32")
    })?;
    crate::util::varint::encode_unsigned_varint(len, buf);
    Ok(())
}

/// Read a non-null compact string.
pub(crate) fn read_compact_string(buf: &mut impl Buf) -> Result<String> {
    KafkaString::decode_compact(buf)?.0.ok_or_else(|| {
        KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "unexpected null compact string",
        )
    })
}

/// Write a non-nullable compact string.
pub(crate) fn write_compact_string(buf: &mut impl BufMut, value: &str) -> Result<()> {
    KafkaString::new(value).try_encode_compact(buf)
}

/// Write a nullable compact string.
pub(crate) fn write_compact_nullable_string(
    buf: &mut impl BufMut,
    value: Option<&str>,
) -> Result<()> {
    match value {
        Some(v) => KafkaString::new(v).try_encode_compact(buf),
        None => KafkaString::null().try_encode_compact(buf),
    }
}

/// Read compact nullable bytes.
pub(crate) fn read_compact_nullable_bytes(buf: &mut impl Buf) -> Result<Option<Bytes>> {
    Ok(KafkaBytes::decode_compact(buf)?.0)
}

/// Write nullable compact bytes.
pub(crate) fn write_compact_nullable_bytes(
    buf: &mut impl BufMut,
    value: Option<&Bytes>,
) -> Result<()> {
    match value {
        Some(v) => KafkaBytes(Some(v.clone())).try_encode_compact(buf),
        None => KafkaBytes(None).try_encode_compact(buf),
    }
}

/// Write an empty tagged-field section: a single zero varint.
pub(crate) fn write_empty_tagged_fields(buf: &mut impl BufMut) -> Result<()> {
    TaggedFields::default().try_encode(buf)
}

/// Write a tagged-field section containing exactly the given fields.
pub(crate) fn write_tagged_fields(buf: &mut impl BufMut, fields: Vec<TaggedField>) -> Result<()> {
    TaggedFields(fields).try_encode(buf)
}

/// Skip a tagged-field section.
pub(crate) fn skip_tagged_fields(buf: &mut impl Buf) -> Result<()> {
    TaggedFields::decode(buf)?;
    Ok(())
}

/// Build the `CurrentLeader` tagged field (tag 0) a broker attaches to a
/// partition it no longer leads (KIP-951).
pub(crate) fn current_leader_field(leader_id: i32, leader_epoch: i32) -> TaggedField {
    let mut data = BytesMut::new();
    data.put_i32(leader_id);
    data.put_i32(leader_epoch);
    data.put_u8(0); // the struct's own (empty) tagged fields
    TaggedField {
        tag: 0,
        data: data.freeze(),
    }
}

/// Build the top-level `NodeEndpoints` tagged field (tag 0) that carries the
/// addresses of the leaders named by `CurrentLeader` (KIP-951).
pub(crate) fn node_endpoints_field(endpoints: &[(i32, &str, i32)]) -> Result<TaggedField> {
    let mut data = BytesMut::new();
    write_compact_array_len(&mut data, endpoints.len())?;
    for (node_id, host, port) in endpoints {
        data.put_i32(*node_id);
        KafkaString::new(*host).try_encode_compact(&mut data)?;
        data.put_i32(*port);
        KafkaString::null().try_encode_compact(&mut data)?; // rack
        write_empty_tagged_fields(&mut data)?;
    }
    Ok(TaggedField {
        tag: 0,
        data: data.freeze(),
    })
}

// ===========================================================================
// Requests
// ===========================================================================

/// Metadata request, v8 wire format.
///
/// Mirrors `MetadataRequest::encode_v8`.
#[derive(Debug, Clone)]
pub(crate) struct MetadataReq {
    /// Requested topics; `None` means "every topic in the cluster".
    pub topics: Option<Vec<String>>,
    /// Whether the client permits the broker to create missing topics.
    pub allow_auto_topic_creation: bool,
}

impl MetadataReq {
    /// Read a v12 (flexible) Metadata request.
    ///
    /// v12 differs from v8 in three ways that matter here: compact
    /// encodings throughout, a 16-byte `TopicId` before each topic name, and
    /// the removal of `IncludeClusterAuthorizedOperations`.
    ///
    /// A topic entry may carry a UUID instead of a name (that is the point of
    /// v12), but krafka only ever *requests* by name, so a null name is
    /// reported rather than silently resolved — the fake broker should not
    /// invent behaviour the client does not exercise.
    pub(crate) fn read_v12(buf: &mut impl Buf) -> Result<Self> {
        let topics = match read_compact_nullable_array_len(buf)? {
            None => None,
            Some(count) => {
                let mut names = Vec::with_capacity(count);
                for _ in 0..count {
                    if buf.remaining() < 16 {
                        return Err(crate::error::KrafkaError::protocol_kind(
                            crate::error::ProtocolErrorKind::TruncatedFrame,
                            "fake broker: truncated topic id in Metadata v12 request",
                        ));
                    }
                    let mut topic_id = [0u8; 16];
                    buf.copy_to_slice(&mut topic_id);
                    let name = read_compact_nullable_string(buf)?.ok_or_else(|| {
                        crate::error::KrafkaError::protocol_kind(
                            crate::error::ProtocolErrorKind::InvalidValue,
                            "fake broker: Metadata v12 topic lookup by UUID is not modelled; \
                             krafka always requests by name",
                        )
                    })?;
                    skip_tagged_fields(buf)?;
                    names.push(name);
                }
                Some(names)
            }
        };
        let allow_auto_topic_creation = bool::decode(buf)?;
        // include_topic_authorized_operations — the client always sends false
        // and ignores the result, so it is read only to keep the cursor aligned.
        let _ = bool::decode(buf)?;
        skip_tagged_fields(buf)?;
        Ok(Self {
            topics,
            allow_auto_topic_creation,
        })
    }
}

/// One partition's records inside a Produce request.
#[derive(Debug, Clone)]
pub(crate) struct ProduceReqPartition {
    /// Partition index.
    pub index: i32,
    /// The record batch exactly as the producer framed it.
    pub records: Option<Bytes>,
}

/// One topic inside a Produce request.
#[derive(Debug, Clone)]
pub(crate) struct ProduceReqTopic {
    /// Topic name.
    pub name: String,
    /// Per-partition record batches.
    pub partitions: Vec<ProduceReqPartition>,
}

/// Produce request, v12 wire format.
///
/// Mirrors `ProduceRequest::encode_v9`, which covers v9–v12: the request layout
/// is identical across them, as is the response decoder, so serving v12 rather
/// than v10 costs nothing. v10 is the lowest version carrying the KIP-951
/// `CurrentLeader` and `NodeEndpoints` tagged fields — which is why Produce,
/// unlike the other APIs here, is served on a flexible version rather than the
/// simpler v8 — and v12 is the floor KIP-890 (TV2) requires.
#[derive(Debug, Clone)]
pub(crate) struct ProduceReq {
    /// Transactional ID, when the write belongs to a transaction.
    ///
    /// Read rather than discarded because it is the whole of the KIP-890 (TV2)
    /// contract: the coordinator learns which partitions a transaction touched
    /// from the `Produce` request itself, with no `AddPartitionsToTxn` round
    /// trip. A broker that ignores it silently drops those partitions from the
    /// commit marker.
    pub transactional_id: Option<String>,
    /// Topics being produced to.
    pub topics: Vec<ProduceReqTopic>,
}

impl ProduceReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let transactional_id = KafkaString::decode_compact(buf)?
            .0
            .filter(|s| !s.is_empty());
        let _acks = i16::decode(buf)?;
        let _timeout_ms = i32::decode(buf)?;
        let topic_count = read_compact_array_len(buf)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_compact_string(buf)?;
            let partition_count = read_compact_array_len(buf)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let index = i32::decode(buf)?;
                let records = read_compact_nullable_bytes(buf)?;
                skip_tagged_fields(buf)?;
                partitions.push(ProduceReqPartition { index, records });
            }
            skip_tagged_fields(buf)?;
            topics.push(ProduceReqTopic { name, partitions });
        }
        skip_tagged_fields(buf)?;
        Ok(Self {
            transactional_id,
            topics,
        })
    }
}

/// One partition inside a Fetch request.
#[derive(Debug, Clone)]
pub(crate) struct FetchReqPartition {
    /// Partition index.
    pub partition: i32,
    /// Leader epoch the client believes is current, or `-1`.
    pub current_leader_epoch: i32,
    /// Offset the client wants to read from.
    pub fetch_offset: i64,
}

/// One topic inside a Fetch request.
#[derive(Debug, Clone)]
pub(crate) struct FetchReqTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions being fetched.
    pub partitions: Vec<FetchReqPartition>,
}

/// Fetch request, v11 wire format.
///
/// Mirrors `FetchRequest::encode_v11`.
#[derive(Debug, Clone)]
pub(crate) struct FetchReq {
    /// Fetch session ID, echoed back in the response so the client's session
    /// bookkeeping stays consistent.
    pub session_id: i32,
    /// `0` = `read_uncommitted`, `1` = `read_committed`.
    ///
    /// Read rather than discarded because it selects whether the fetch stops
    /// at the last stable offset and reports aborted transactions. A broker
    /// that ignores it hands a `read_committed` consumer records from open and
    /// aborted transactions, which is the one guarantee the isolation level
    /// exists to provide.
    pub isolation_level: i8,
    /// Topics being fetched.
    pub topics: Vec<FetchReqTopic>,
}

impl FetchReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let _replica_id = i32::decode(buf)?;
        let _max_wait_ms = i32::decode(buf)?;
        let _min_bytes = i32::decode(buf)?;
        let _max_bytes = i32::decode(buf)?;
        let isolation_level = i8::decode(buf)?;
        let session_id = i32::decode(buf)?;
        let _session_epoch = i32::decode(buf)?;

        let topic_count = read_array_len(buf)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = read_string(buf)?;
            let partition_count = read_array_len(buf)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let current_leader_epoch = i32::decode(buf)?;
                let fetch_offset = i64::decode(buf)?;
                let _log_start_offset = i64::decode(buf)?;
                let _partition_max_bytes = i32::decode(buf)?;
                partitions.push(FetchReqPartition {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                });
            }
            topics.push(FetchReqTopic { topic, partitions });
        }

        // Forgotten topics (v7+) — the fake broker keeps no fetch-session state,
        // so these are read and discarded.
        let forgotten_count = read_array_len(buf)?;
        for _ in 0..forgotten_count {
            let _ = read_string(buf)?;
            let partition_count = read_array_len(buf)?;
            for _ in 0..partition_count {
                let _ = i32::decode(buf)?;
            }
        }
        // rack_id (v11+).
        let _ = read_nullable_string(buf)?;

        Ok(Self {
            session_id,
            isolation_level,
            topics,
        })
    }
}

/// Read a compact nullable string (flexible encoding).
pub(crate) fn read_compact_nullable_string(buf: &mut impl Buf) -> Result<Option<String>> {
    Ok(KafkaString::decode_compact(buf)?.0)
}

/// Read a compact nullable array length, returning `None` for a null array.
///
/// Distinguishing null from empty matters for `ConsumerGroupHeartbeat`: a null
/// `SubscribedTopicNames` means "unchanged since my last heartbeat", while an
/// empty one means "I am subscribed to nothing".
pub(crate) fn read_compact_nullable_array_len(buf: &mut impl Buf) -> Result<Option<usize>> {
    let raw = crate::util::varint::decode_unsigned_varint(buf)?;
    if raw == 0 {
        return Ok(None);
    }
    Ok(Some(crate::protocol::check_compact_array_len(raw)?))
}

/// One member's owned partitions inside a `ConsumerGroupHeartbeat` request.
#[derive(Debug, Clone)]
pub(crate) struct HeartbeatTopicPartitions {
    /// Topic UUID.
    pub topic_id: [u8; 16],
    /// Partitions the member currently owns for that topic.
    pub partitions: Vec<i32>,
}

/// `ConsumerGroupHeartbeat` request, v1 wire format (KIP-848 + KIP-1082).
///
/// Mirrors `ConsumerGroupHeartbeatRequest::encode_v1`.
#[derive(Debug, Clone)]
pub(crate) struct ConsumerGroupHeartbeatReq {
    /// Group being joined or maintained.
    pub group_id: String,
    /// Client-generated member ID (KIP-1082).
    pub member_id: String,
    /// `0` to join, `-1` to leave, `-2` for a static member's temporary leave.
    pub member_epoch: i32,
    /// Static membership ID, if any.
    pub instance_id: Option<String>,
    /// Client rack, for rack-aware assignment.
    #[allow(dead_code)]
    pub rack_id: Option<String>,
    /// Rebalance timeout, or `-1` when unchanged.
    #[allow(dead_code)]
    pub rebalance_timeout_ms: i32,
    /// Subscribed topics, or `None` when unchanged since the last heartbeat.
    pub subscribed_topic_names: Option<Vec<String>>,
    /// Regex subscription, or `None`.
    #[allow(dead_code)]
    pub subscribed_topic_regex: Option<String>,
    /// Requested server-side assignor, or `None`.
    #[allow(dead_code)]
    pub server_assignor: Option<String>,
    /// Partitions the member currently owns, or `None` when unchanged.
    #[allow(dead_code)]
    pub topic_partitions: Option<Vec<HeartbeatTopicPartitions>>,
}

impl ConsumerGroupHeartbeatReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_compact_string(buf)?;
        let member_id = read_compact_string(buf)?;
        let member_epoch = i32::decode(buf)?;
        let instance_id = read_compact_nullable_string(buf)?;
        let rack_id = read_compact_nullable_string(buf)?;
        let rebalance_timeout_ms = i32::decode(buf)?;

        let subscribed_topic_names = match read_compact_nullable_array_len(buf)? {
            None => None,
            Some(count) => {
                let mut names = Vec::with_capacity(count);
                for _ in 0..count {
                    names.push(read_compact_string(buf)?);
                }
                Some(names)
            }
        };

        let subscribed_topic_regex = read_compact_nullable_string(buf)?;
        let server_assignor = read_compact_nullable_string(buf)?;

        let topic_partitions = match read_compact_nullable_array_len(buf)? {
            None => None,
            Some(count) => {
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let mut topic_id = [0u8; 16];
                    if buf.remaining() < 16 {
                        return Err(crate::error::KrafkaError::protocol_kind(
                            crate::error::ProtocolErrorKind::TruncatedFrame,
                            "fake broker: truncated topic id in ConsumerGroupHeartbeat",
                        ));
                    }
                    buf.copy_to_slice(&mut topic_id);
                    let partition_count = read_compact_array_len(buf)?;
                    let mut partitions = Vec::with_capacity(partition_count);
                    for _ in 0..partition_count {
                        partitions.push(i32::decode(buf)?);
                    }
                    skip_tagged_fields(buf)?;
                    entries.push(HeartbeatTopicPartitions {
                        topic_id,
                        partitions,
                    });
                }
                Some(entries)
            }
        };

        skip_tagged_fields(buf)?;

        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            instance_id,
            rack_id,
            rebalance_timeout_ms,
            subscribed_topic_names,
            subscribed_topic_regex,
            server_assignor,
            topic_partitions,
        })
    }
}

/// Write a `ConsumerGroupHeartbeat` response assignment.
///
/// Non-tagged nullable structs in flexible versions use a single signed byte
/// as the presence marker: negative means null, `1` means the fields follow.
/// This mirrors `ConsumerGroupHeartbeatResponse::decode_assignment`.
pub(crate) fn write_heartbeat_assignment(
    out: &mut BytesMut,
    assignment: Option<&[HeartbeatTopicPartitions]>,
) -> Result<()> {
    match assignment {
        None => {
            out.put_i8(-1);
            Ok(())
        }
        Some(entries) => {
            out.put_i8(1);
            write_compact_array_len(out, entries.len())?;
            for entry in entries {
                out.put_slice(&entry.topic_id);
                write_compact_array_len(out, entry.partitions.len())?;
                for partition in &entry.partitions {
                    out.put_i32(*partition);
                }
                write_empty_tagged_fields(out)?;
            }
            write_empty_tagged_fields(out)
        }
    }
}

/// One partition inside a ListOffsets request.
#[derive(Debug, Clone)]
pub(crate) struct ListOffsetsReqPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The client's view of the partition's leader epoch, or `-1` when it has
    /// none. Retained so the handler can fence a stale client the way a real
    /// broker does (KIP-320); dropping it made that path untestable.
    pub current_leader_epoch: i32,
    /// `-2` for earliest, `-1` for latest, otherwise a millisecond timestamp.
    pub timestamp: i64,
}

/// One topic inside a ListOffsets request.
#[derive(Debug, Clone)]
pub(crate) struct ListOffsetsReqTopic {
    /// Topic name.
    pub name: String,
    /// Partitions being queried.
    pub partitions: Vec<ListOffsetsReqPartition>,
}

/// ListOffsets request, v5 wire format.
///
/// Mirrors `ListOffsetsRequest::encode_v4` (which covers v4–v5).
#[derive(Debug, Clone)]
pub(crate) struct ListOffsetsReq {
    /// Topics being queried.
    pub topics: Vec<ListOffsetsReqTopic>,
}

impl ListOffsetsReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let _replica_id = i32::decode(buf)?;
        let _isolation_level = i8::decode(buf)?;
        let topic_count = read_array_len(buf)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_string(buf)?;
            let partition_count = read_array_len(buf)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let current_leader_epoch = i32::decode(buf)?;
                let timestamp = i64::decode(buf)?;
                partitions.push(ListOffsetsReqPartition {
                    partition_index,
                    current_leader_epoch,
                    timestamp,
                });
            }
            topics.push(ListOffsetsReqTopic { name, partitions });
        }
        Ok(Self { topics })
    }
}

/// FindCoordinator request, v2 wire format.
///
/// Mirrors `FindCoordinatorRequest::encode_v1` (which covers v1–v2).
#[derive(Debug, Clone)]
pub(crate) struct FindCoordinatorReq {
    /// Group ID or transactional ID being resolved.
    pub key: String,
    /// `0` for a consumer group, `1` for a transaction coordinator.
    pub key_type: i8,
}

impl FindCoordinatorReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let key = read_string(buf)?;
        let key_type = i8::decode(buf)?;
        Ok(Self { key, key_type })
    }
}

/// One protocol a member supports, inside a JoinGroup request.
#[derive(Debug, Clone)]
pub(crate) struct JoinGroupReqProtocol {
    /// Assignor name, e.g. `range`.
    pub name: String,
    /// Opaque subscription metadata the leader will read back.
    pub metadata: Bytes,
}

/// JoinGroup request, v5 wire format.
///
/// Mirrors `JoinGroupRequest::encode_v5`.
#[derive(Debug, Clone)]
pub(crate) struct JoinGroupReq {
    /// Group being joined.
    pub group_id: String,
    /// Member ID, empty on a first join.
    pub member_id: String,
    /// Static membership ID (KIP-345), if any.
    pub group_instance_id: Option<String>,
    /// Protocol type, e.g. `consumer`.
    pub protocol_type: String,
    /// Protocols this member supports, in preference order.
    pub protocols: Vec<JoinGroupReqProtocol>,
}

impl JoinGroupReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_string(buf)?;
        let _session_timeout_ms = i32::decode(buf)?;
        let _rebalance_timeout_ms = i32::decode(buf)?;
        let member_id = read_string(buf)?;
        let group_instance_id = read_nullable_string(buf)?;
        let protocol_type = read_string(buf)?;
        let protocol_count = read_array_len(buf)?;
        let mut protocols = Vec::with_capacity(protocol_count);
        for _ in 0..protocol_count {
            let name = read_string(buf)?;
            let metadata = read_bytes(buf)?;
            protocols.push(JoinGroupReqProtocol { name, metadata });
        }
        Ok(Self {
            group_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
        })
    }
}

/// One member's assignment inside a SyncGroup request.
#[derive(Debug, Clone)]
pub(crate) struct SyncGroupReqAssignment {
    /// Member the assignment is for.
    pub member_id: String,
    /// Opaque assignment bytes computed by the group leader.
    pub assignment: Bytes,
}

/// SyncGroup request, v3 wire format.
///
/// Mirrors `SyncGroupRequest::encode_v3`.
#[derive(Debug, Clone)]
pub(crate) struct SyncGroupReq {
    /// Group being synced.
    pub group_id: String,
    /// Generation the member believes it is in.
    pub generation_id: i32,
    /// Member sending the request.
    pub member_id: String,
    /// Assignments, non-empty only when sent by the group leader.
    pub assignments: Vec<SyncGroupReqAssignment>,
}

impl SyncGroupReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_string(buf)?;
        let generation_id = i32::decode(buf)?;
        let member_id = read_string(buf)?;
        let _group_instance_id = read_nullable_string(buf)?;
        let count = read_array_len(buf)?;
        let mut assignments = Vec::with_capacity(count);
        for _ in 0..count {
            let member_id = read_string(buf)?;
            let assignment = read_bytes(buf)?;
            assignments.push(SyncGroupReqAssignment {
                member_id,
                assignment,
            });
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            assignments,
        })
    }
}

/// Heartbeat request, v3 wire format.
///
/// Mirrors `HeartbeatRequest::encode_v3`.
#[derive(Debug, Clone)]
pub(crate) struct HeartbeatReq {
    /// Group being heartbeated.
    pub group_id: String,
    /// Generation the member believes it is in.
    pub generation_id: i32,
    /// Member sending the heartbeat.
    pub member_id: String,
}

impl HeartbeatReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_string(buf)?;
        let generation_id = i32::decode(buf)?;
        let member_id = read_string(buf)?;
        let _group_instance_id = read_nullable_string(buf)?;
        Ok(Self {
            group_id,
            generation_id,
            member_id,
        })
    }
}

/// LeaveGroup request, v3 wire format.
///
/// Mirrors `LeaveGroupRequest::encode_v3`.
#[derive(Debug, Clone)]
pub(crate) struct LeaveGroupReq {
    /// Group being left.
    pub group_id: String,
    /// Members leaving, as `(member_id, group_instance_id)`.
    pub members: Vec<(String, Option<String>)>,
}

impl LeaveGroupReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_string(buf)?;
        let count = read_array_len(buf)?;
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            let member_id = read_string(buf)?;
            let instance = read_nullable_string(buf)?;
            members.push((member_id, instance));
        }
        Ok(Self { group_id, members })
    }
}

/// One partition's committed offset inside an OffsetCommit request.
#[derive(Debug, Clone)]
pub(crate) struct OffsetCommitReqPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Offset being committed.
    pub committed_offset: i64,
    /// Leader epoch at the committed offset, or `-1`.
    pub committed_leader_epoch: i32,
    /// Opaque metadata attached to the commit.
    pub committed_metadata: Option<String>,
}

/// One topic inside an OffsetCommit request.
#[derive(Debug, Clone)]
pub(crate) struct OffsetCommitReqTopic {
    /// Topic name.
    pub name: String,
    /// Partitions being committed.
    pub partitions: Vec<OffsetCommitReqPartition>,
}

/// OffsetCommit request, v7 wire format.
///
/// Mirrors `OffsetCommitRequest::encode_v7`.
#[derive(Debug, Clone)]
pub(crate) struct OffsetCommitReq {
    /// Group whose offsets are being committed.
    pub group_id: String,
    /// Generation the member believes it is in.
    pub generation_id: i32,
    /// Member sending the commit.
    pub member_id: String,
    /// Topics being committed.
    pub topics: Vec<OffsetCommitReqTopic>,
}

impl OffsetCommitReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_string(buf)?;
        let generation_id = i32::decode(buf)?;
        let member_id = read_string(buf)?;
        let _group_instance_id = read_nullable_string(buf)?;
        let topic_count = read_array_len(buf)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_string(buf)?;
            let partition_count = read_array_len(buf)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let committed_leader_epoch = i32::decode(buf)?;
                let committed_metadata = read_nullable_string(buf)?;
                partitions.push(OffsetCommitReqPartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    committed_metadata,
                });
            }
            topics.push(OffsetCommitReqTopic { name, partitions });
        }
        Ok(Self {
            group_id,
            generation_id,
            member_id,
            topics,
        })
    }
}

/// OffsetFetch request, v5 wire format.
///
/// Mirrors `OffsetFetchRequest::encode_v1` (which covers v1–v5).
#[derive(Debug, Clone)]
pub(crate) struct OffsetFetchReq {
    /// Group whose offsets are being read.
    pub group_id: String,
    /// Requested topics and partitions; `None` means "everything committed".
    pub topics: Option<Vec<(String, Vec<i32>)>>,
}

impl OffsetFetchReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_string(buf)?;
        let topics = match read_nullable_array_len(buf)? {
            None => None,
            Some(count) => {
                let mut topics = Vec::with_capacity(count);
                for _ in 0..count {
                    let name = read_string(buf)?;
                    let partition_count = read_array_len(buf)?;
                    let mut partitions = Vec::with_capacity(partition_count);
                    for _ in 0..partition_count {
                        partitions.push(i32::decode(buf)?);
                    }
                    topics.push((name, partitions));
                }
                Some(topics)
            }
        };
        Ok(Self { group_id, topics })
    }
}

/// InitProducerId request, v1 wire format.
///
/// Mirrors `InitProducerIdRequest::encode_v0` (which covers v0–v1).
#[derive(Debug, Clone)]
pub(crate) struct InitProducerIdReq {
    /// Transactional ID, or `None` for a plain idempotent producer.
    ///
    /// This is the fencing key: a known transactional ID must get its existing
    /// producer ID back with a **higher** epoch, so the previous incarnation's
    /// writes are rejected (KIP-360). Discarding it, as this reader used to,
    /// makes every `InitProducerId` mint a fresh identity and fences nothing.
    pub transactional_id: Option<String>,
}

impl InitProducerIdReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let transactional_id = read_nullable_string(buf)?.filter(|s| !s.is_empty());
        let _transaction_timeout_ms = i32::decode(buf)?;
        Ok(Self { transactional_id })
    }
}

/// `AddPartitionsToTxn` request, v0 wire format (KIP-98, TV1 only).
///
/// Mirrors `AddPartitionsToTxnRequest::encode_v0`.
#[derive(Debug, Clone)]
pub(crate) struct AddPartitionsToTxnReq {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID the client believes it holds.
    pub producer_id: i64,
    /// Producer epoch the client believes it holds.
    pub producer_epoch: i16,
    /// Partitions to enrol, as `(topic, partition)`.
    pub partitions: Vec<(String, i32)>,
}

impl AddPartitionsToTxnReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let transactional_id = read_string(buf)?;
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let topic_count = read_array_len(buf)?;
        let mut partitions = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_string(buf)?;
            let partition_count = read_array_len(buf)?;
            for _ in 0..partition_count {
                partitions.push((name.clone(), i32::decode(buf)?));
            }
        }
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            partitions,
        })
    }
}

/// `AddOffsetsToTxn` request, v0 wire format (KIP-98, TV1 only).
///
/// Mirrors `AddOffsetsToTxnRequest::encode_v0`.
#[derive(Debug, Clone)]
pub(crate) struct AddOffsetsToTxnReq {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID the client believes it holds.
    pub producer_id: i64,
    /// Producer epoch the client believes it holds.
    pub producer_epoch: i16,
    /// Consumer group whose offsets join the transaction.
    pub group_id: String,
}

impl AddOffsetsToTxnReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self {
            transactional_id: read_string(buf)?,
            producer_id: i64::decode(buf)?,
            producer_epoch: i16::decode(buf)?,
            group_id: read_string(buf)?,
        })
    }
}

/// `EndTxn` request, v3–v5 wire format (flexible).
///
/// Mirrors `EndTxnRequest::encode_v3`, which covers v3–v5 — the request layout
/// is identical across them; only the *response* grows the KIP-890 bumped
/// identity at v5.
#[derive(Debug, Clone)]
pub(crate) struct EndTxnReq {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID the client believes it holds.
    pub producer_id: i64,
    /// Producer epoch the client believes it holds.
    pub producer_epoch: i16,
    /// `true` to commit, `false` to abort.
    pub committed: bool,
}

impl EndTxnReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let transactional_id = read_compact_string(buf)?;
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let committed = i8::decode(buf)? != 0;
        skip_tagged_fields(buf)?;
        Ok(Self {
            transactional_id,
            producer_id,
            producer_epoch,
            committed,
        })
    }
}

/// One staged offset inside a `TxnOffsetCommit` request.
#[derive(Debug, Clone)]
pub(crate) struct TxnOffsetCommitReqPartition {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Offset the group will resume from.
    pub committed_offset: i64,
    /// Leader epoch the offset was read at, or `-1`.
    pub committed_leader_epoch: i32,
    /// Opaque metadata.
    pub metadata: Option<String>,
}

/// `TxnOffsetCommit` request, v3–v5 wire format (flexible).
///
/// Mirrors `TxnOffsetCommitRequest::encode_v3`, which covers v3–v5.
#[derive(Debug, Clone)]
pub(crate) struct TxnOffsetCommitReq {
    /// Transactional ID.
    pub transactional_id: String,
    /// Consumer group receiving the offsets.
    pub group_id: String,
    /// Producer ID the client believes it holds.
    pub producer_id: i64,
    /// Producer epoch the client believes it holds.
    pub producer_epoch: i16,
    /// KIP-447 fencing triple: the group generation the committer belongs to.
    pub generation_id: i32,
    /// KIP-447 fencing triple: the committer's member ID.
    pub member_id: String,
    /// KIP-447 fencing triple: the committer's static-membership ID, if any.
    pub group_instance_id: Option<String>,
    /// Offsets being staged.
    pub offsets: Vec<TxnOffsetCommitReqPartition>,
}

impl TxnOffsetCommitReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let transactional_id = read_compact_string(buf)?;
        let group_id = read_compact_string(buf)?;
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let generation_id = i32::decode(buf)?;
        let member_id = read_compact_string(buf)?;
        let group_instance_id = read_compact_nullable_string(buf)?;
        let topic_count = read_compact_array_len(buf)?;
        let mut offsets = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic = read_compact_string(buf)?;
            let partition_count = read_compact_array_len(buf)?;
            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let committed_leader_epoch = i32::decode(buf)?;
                let metadata = read_compact_nullable_string(buf)?;
                skip_tagged_fields(buf)?;
                offsets.push(TxnOffsetCommitReqPartition {
                    topic: topic.clone(),
                    partition,
                    committed_offset,
                    committed_leader_epoch,
                    metadata,
                });
            }
            skip_tagged_fields(buf)?;
        }
        skip_tagged_fields(buf)?;
        Ok(Self {
            transactional_id,
            group_id,
            producer_id,
            producer_epoch,
            generation_id,
            member_id,
            group_instance_id,
            offsets,
        })
    }
}

/// One topic inside a CreateTopics request.
#[derive(Debug, Clone)]
pub(crate) struct CreateTopicsReqTopic {
    /// Topic name.
    pub name: String,
    /// Requested partition count, or `-1` for the broker default.
    pub num_partitions: i32,
}

/// CreateTopics request, v4 wire format.
///
/// Mirrors `CreateTopicsRequest::encode_v2` (which covers v2–v4).
#[derive(Debug, Clone)]
pub(crate) struct CreateTopicsReq {
    /// Topics to create.
    pub topics: Vec<CreateTopicsReqTopic>,
    /// Whether the request is a dry run.
    pub validate_only: bool,
}

impl CreateTopicsReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = read_array_len(buf)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_string(buf)?;
            let num_partitions = i32::decode(buf)?;
            let _replication_factor = i16::decode(buf)?;
            let assignment_count = read_array_len(buf)?;
            for _ in 0..assignment_count {
                let _ = i32::decode(buf)?;
                let broker_count = read_array_len(buf)?;
                for _ in 0..broker_count {
                    let _ = i32::decode(buf)?;
                }
            }
            let config_count = read_array_len(buf)?;
            for _ in 0..config_count {
                let _ = read_string(buf)?;
                let _ = read_nullable_string(buf)?;
            }
            topics.push(CreateTopicsReqTopic {
                name,
                num_partitions,
            });
        }
        let _timeout_ms = i32::decode(buf)?;
        let validate_only = bool::decode(buf)?;
        Ok(Self {
            topics,
            validate_only,
        })
    }
}

/// DeleteTopics request, v3 wire format.
///
/// Mirrors `DeleteTopicsRequest`'s v1–v3 encoder: a plain string array
/// followed by the timeout.
#[derive(Debug, Clone)]
pub(crate) struct DeleteTopicsReq {
    /// Topic names to delete.
    pub topic_names: Vec<String>,
}

impl DeleteTopicsReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let count = read_array_len(buf)?;
        let mut topic_names = Vec::with_capacity(count);
        for _ in 0..count {
            topic_names.push(read_string(buf)?);
        }
        let _timeout_ms = i32::decode(buf)?;
        Ok(Self { topic_names })
    }
}

// ===========================================================================
// Record batch patching
// ===========================================================================

/// Byte offset of `base_offset` within a v2 record batch.
const BATCH_BASE_OFFSET_POS: usize = 0;
/// Byte offset of `partition_leader_epoch` within a v2 record batch.
const BATCH_LEADER_EPOCH_POS: usize = 12;
/// Byte offset of `records_count` within a v2 record batch.
///
/// Layout up to this point: `base_offset`(8) `batch_length`(4)
/// `partition_leader_epoch`(4) `magic`(1) `crc`(4) `attributes`(2)
/// `last_offset_delta`(4) `base_timestamp`(8) `max_timestamp`(8)
/// `producer_id`(8) `producer_epoch`(2) `base_sequence`(4).
///
/// `records_count` is preferred over `last_offset_delta` because the encoder
/// always derives it from the records actually written, whereas
/// `last_offset_delta` is a caller-set field that may be left at zero.
const BATCH_RECORDS_COUNT_POS: usize = 57;
/// Smallest byte length that can hold a complete v2 record batch header.
const BATCH_HEADER_LEN: usize = 61;

/// Number of records in a v2 record batch.
///
/// Returns `None` when the buffer is too short to be a valid batch, and clamps
/// a header claiming a negative count to zero.
pub(crate) fn batch_record_count(batch: &[u8]) -> Option<i64> {
    if batch.len() < BATCH_HEADER_LEN {
        return None;
    }
    let bytes = batch.get(BATCH_RECORDS_COUNT_POS..BATCH_RECORDS_COUNT_POS + 4)?;
    let count = i32::from_be_bytes([
        *bytes.first()?,
        *bytes.get(1)?,
        *bytes.get(2)?,
        *bytes.get(3)?,
    ]);
    Some(i64::from(count).max(0))
}

/// Stamp a batch with the offset and leader epoch the broker assigned it.
///
/// Both fields sit *before* the CRC field in the v2 batch layout, and the CRC
/// only covers the bytes after itself, so rewriting them in place keeps the
/// batch checksum valid. That is what lets the fake broker store the producer's
/// bytes verbatim and still serve them back at broker-assigned offsets.
pub(crate) fn stamp_batch(batch: &Bytes, base_offset: i64, leader_epoch: i32) -> Bytes {
    if batch.len() < BATCH_HEADER_LEN {
        return batch.clone();
    }
    let mut out = BytesMut::from(&batch[..]);
    out[BATCH_BASE_OFFSET_POS..BATCH_BASE_OFFSET_POS + 8]
        .copy_from_slice(&base_offset.to_be_bytes());
    out[BATCH_LEADER_EPOCH_POS..BATCH_LEADER_EPOCH_POS + 4]
        .copy_from_slice(&leader_epoch.to_be_bytes());
    out.freeze()
}

/// Read the `base_offset` stamped into a batch.
pub(crate) fn batch_base_offset(batch: &[u8]) -> Option<i64> {
    if batch.len() < BATCH_HEADER_LEN {
        return None;
    }
    let bytes = batch.get(BATCH_BASE_OFFSET_POS..BATCH_BASE_OFFSET_POS + 8)?;
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Some(i64::from_be_bytes(arr))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::{Record, RecordBatch};

    /// Stamping must leave the batch decodable: the CRC covers only the bytes
    /// after it, so rewriting `base_offset` and `partition_leader_epoch` in
    /// place cannot invalidate it.
    #[test]
    fn stamping_a_batch_preserves_its_crc() {
        let mut batch = RecordBatch::new();
        batch.records = vec![
            Record::new(None, Some(Bytes::from_static(b"a"))).with_offset_delta(0),
            Record::new(None, Some(Bytes::from_static(b"b"))).with_offset_delta(1),
        ];
        let encoded = batch.encode().unwrap();

        assert_eq!(batch_record_count(&encoded), Some(2));

        let stamped = stamp_batch(&encoded, 41, 7);
        assert_eq!(batch_base_offset(&stamped), Some(41));

        let mut cursor = stamped.clone();
        let decoded = RecordBatch::decode(&mut cursor).expect("stamped batch must still decode");
        assert_eq!(decoded.base_offset, 41);
        assert_eq!(decoded.partition_leader_epoch, 7);
        assert_eq!(decoded.records.len(), 2);
    }

    #[test]
    fn short_buffers_are_rejected_rather_than_indexed() {
        assert_eq!(batch_record_count(&[0u8; 4]), None);
        assert_eq!(batch_base_offset(&[0u8; 4]), None);
        let short = Bytes::from_static(&[0u8; 4]);
        assert_eq!(stamp_batch(&short, 1, 1), short);
    }

    #[test]
    fn array_lengths_beyond_the_safety_limit_are_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i32(MAX_ARRAY_LEN as i32 + 1);
        assert!(read_array_len(&mut buf.freeze()).is_err());
    }

    #[test]
    fn null_array_length_is_rejected_for_non_nullable_arrays() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        assert!(read_array_len(&mut buf.freeze()).is_err());
    }
}

// ── Share groups (KIP-932) ───────────────────────────────────────────────

/// A `ShareGroupHeartbeat` v1 request.
#[derive(Debug)]
pub(crate) struct ShareGroupHeartbeatReq {
    /// Share group ID.
    pub group_id: String,
    /// Client-generated member ID (KIP-932 share groups always use one).
    pub member_id: String,
    /// Member epoch; `-1` signals a leave.
    pub member_epoch: i32,
    /// Subscribed topics, or `None` when unchanged since the last heartbeat.
    pub subscribed_topic_names: Option<Vec<String>>,
}

impl ShareGroupHeartbeatReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let group_id = read_compact_string(buf)?;
        let member_id = read_compact_string(buf)?;
        let member_epoch = i32::decode(buf)?;
        let _rack_id = read_compact_nullable_string(buf)?;
        let subscribed_topic_names = match read_compact_nullable_array_len(buf)? {
            None => None,
            Some(n) => {
                let mut topics = Vec::with_capacity(n);
                for _ in 0..n {
                    topics.push(read_compact_string(buf)?);
                }
                Some(topics)
            }
        };
        skip_tagged_fields(buf)?;
        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            subscribed_topic_names,
        })
    }
}

/// One acknowledgement batch inside a `ShareFetch` / `ShareAcknowledge`.
#[derive(Debug)]
pub(crate) struct ShareAckBatch {
    /// First offset of the range.
    pub first_offset: i64,
    /// Last offset of the range, inclusive.
    pub last_offset: i64,
    /// One acknowledge type per offset in the range.
    pub acknowledge_types: Vec<i8>,
}

/// A `(topic_id, partition, acks)` triple from a share request.
#[derive(Debug)]
pub(crate) struct ShareTopicPartitionAcks {
    /// Topic UUID.
    pub topic_id: [u8; 16],
    /// Partition index.
    pub partition_index: i32,
    /// Acknowledgement batches piggybacked on this partition.
    pub acknowledgement_batches: Vec<ShareAckBatch>,
}

/// Read the `Topics -> Partitions -> AcknowledgementBatches` nesting shared by
/// `ShareFetch` and `ShareAcknowledge`, then the trailing forgotten-topics
/// array where present.
fn read_share_topics(buf: &mut impl Buf) -> Result<Vec<ShareTopicPartitionAcks>> {
    let topic_count = read_compact_array_len(buf)?;
    let mut out = Vec::new();
    for _ in 0..topic_count {
        let mut topic_id = [0u8; 16];
        if buf.remaining() < 16 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for share topic_id",
            ));
        }
        buf.copy_to_slice(&mut topic_id);
        let part_count = read_compact_array_len(buf)?;
        for _ in 0..part_count {
            let partition_index = i32::decode(buf)?;
            let batch_count = read_compact_array_len(buf)?;
            let mut batches = Vec::with_capacity(batch_count);
            for _ in 0..batch_count {
                let first_offset = i64::decode(buf)?;
                let last_offset = i64::decode(buf)?;
                let type_count = read_compact_array_len(buf)?;
                let mut acknowledge_types = Vec::with_capacity(type_count);
                for _ in 0..type_count {
                    acknowledge_types.push(i8::decode(buf)?);
                }
                skip_tagged_fields(buf)?;
                batches.push(ShareAckBatch {
                    first_offset,
                    last_offset,
                    acknowledge_types,
                });
            }
            skip_tagged_fields(buf)?;
            out.push(ShareTopicPartitionAcks {
                topic_id,
                partition_index,
                acknowledgement_batches: batches,
            });
        }
        skip_tagged_fields(buf)?;
    }
    Ok(out)
}

/// A `ShareFetch` v1/v2 request.
#[derive(Debug)]
pub(crate) struct ShareFetchReq {
    /// Share group ID.
    pub group_id: Option<String>,
    /// Member ID.
    pub member_id: Option<String>,
    /// Maximum records the broker may acquire for this request.
    pub max_records: i32,
    /// Requested topic-partitions, with any piggybacked acknowledgements.
    pub topics: Vec<ShareTopicPartitionAcks>,
}

impl ShareFetchReq {
    pub(crate) fn read(buf: &mut impl Buf, version: i16) -> Result<Self> {
        let group_id = read_compact_nullable_string(buf)?;
        let member_id = read_compact_nullable_string(buf)?;
        let _share_session_epoch = i32::decode(buf)?;
        let _max_wait_ms = i32::decode(buf)?;
        let _min_bytes = i32::decode(buf)?;
        let _max_bytes = i32::decode(buf)?;
        let max_records = i32::decode(buf)?;
        let _batch_size = i32::decode(buf)?;
        if version >= 2 {
            let _share_acquire_mode = i8::decode(buf)?;
            let _is_renew_ack = i8::decode(buf)?;
        }
        let topics = read_share_topics(buf)?;
        // ForgottenTopicsData: topic_id + partition list, no ack batches.
        let forgotten_count = read_compact_array_len(buf)?;
        for _ in 0..forgotten_count {
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::TruncatedFrame,
                    "not enough bytes for forgotten topic_id",
                ));
            }
            buf.advance(16);
            let n = read_compact_array_len(buf)?;
            for _ in 0..n {
                let _ = i32::decode(buf)?;
            }
            skip_tagged_fields(buf)?;
        }
        skip_tagged_fields(buf)?;
        Ok(Self {
            group_id,
            member_id,
            max_records,
            topics,
        })
    }
}

/// A `ShareAcknowledge` v1/v2 request.
#[derive(Debug)]
pub(crate) struct ShareAcknowledgeReq {
    /// Share group ID.
    pub group_id: Option<String>,
    /// Member ID.
    pub member_id: Option<String>,
    /// Acknowledged topic-partitions.
    pub topics: Vec<ShareTopicPartitionAcks>,
}

impl ShareAcknowledgeReq {
    pub(crate) fn read(buf: &mut impl Buf, version: i16) -> Result<Self> {
        let group_id = read_compact_nullable_string(buf)?;
        let member_id = read_compact_nullable_string(buf)?;
        let _share_session_epoch = i32::decode(buf)?;
        if version >= 2 {
            let _is_renew_ack = i8::decode(buf)?;
        }
        let topics = read_share_topics(buf)?;
        skip_tagged_fields(buf)?;
        Ok(Self {
            group_id,
            member_id,
            topics,
        })
    }
}

// ── UpdateFeatures (KIP-584) ─────────────────────────────────────────────

/// One feature update inside an `UpdateFeatures` request.
#[derive(Debug)]
pub(crate) struct FeatureUpdate {
    /// Feature name, e.g. `metadata.version`.
    pub feature: String,
    /// Requested version level; `0` deletes the feature.
    pub max_version_level: i16,
}

/// An `UpdateFeatures` v0–v2 request.
#[derive(Debug)]
pub(crate) struct UpdateFeaturesReq {
    /// Requested updates.
    pub feature_updates: Vec<FeatureUpdate>,
    /// Whether the controller should simulate rather than apply.
    ///
    /// Absent before v1, which is the whole reason this handler cares about
    /// the version: a v0 controller silently *applies* what the caller asked
    /// to simulate.
    pub validate_only: bool,
}

impl UpdateFeaturesReq {
    pub(crate) fn read(buf: &mut impl Buf, version: i16) -> Result<Self> {
        let _timeout_ms = i32::decode(buf)?;
        let count = read_compact_array_len(buf)?;
        let mut feature_updates = Vec::with_capacity(count);
        for _ in 0..count {
            let feature = read_compact_string(buf)?;
            let max_version_level = i16::decode(buf)?;
            // v0 has AllowDowngrade (bool), v1+ has UpgradeType (i8). Both are
            // one byte and neither changes what this handler does.
            let _ = i8::decode(buf)?;
            skip_tagged_fields(buf)?;
            feature_updates.push(FeatureUpdate {
                feature,
                max_version_level,
            });
        }
        let validate_only = if version >= 1 {
            i8::decode(buf)? != 0
        } else {
            false
        };
        skip_tagged_fields(buf)?;
        Ok(Self {
            feature_updates,
            validate_only,
        })
    }
}

// ── StreamsGroupDescribe (KIP-1071) ──────────────────────────────────────

/// A `StreamsGroupDescribe` v0 request.
#[derive(Debug)]
pub(crate) struct StreamsGroupDescribeReq {
    /// Groups to describe.
    pub group_ids: Vec<String>,
    /// Whether the caller asked for the authorized-operations bitfield.
    pub include_authorized_operations: bool,
}

impl StreamsGroupDescribeReq {
    pub(crate) fn read(buf: &mut impl Buf) -> Result<Self> {
        let count = read_compact_array_len(buf)?;
        let mut group_ids = Vec::with_capacity(count);
        for _ in 0..count {
            group_ids.push(read_compact_string(buf)?);
        }
        let include_authorized_operations = i8::decode(buf)? != 0;
        skip_tagged_fields(buf)?;
        Ok(Self {
            group_ids,
            include_authorized_operations,
        })
    }
}

/// Write a non-tagged nullable struct's presence byte.
pub(crate) fn write_presence(buf: &mut impl BufMut, present: bool) {
    buf.put_i8(if present { 1 } else { -1 });
}
