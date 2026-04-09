use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::check_compact_array_len;
use crate::protocol::primitives::{Decode, KafkaArray, KafkaString, TaggedFields, TryEncode};
macro_rules! unsupported_encode {
    ($type:expr, $version:expr) => {
        Err(KrafkaError::protocol(format!(
            "unsupported {} encode version {}",
            $type, $version
        )))
    };
}

macro_rules! unsupported_decode {
    ($type:expr, $version:expr) => {
        Err(KrafkaError::protocol(format!(
            "unsupported {} decode version {}",
            $type, $version
        )))
    };
}

/// Metadata request.
#[derive(Debug, Clone, Default)]
pub struct MetadataRequest {
    /// Topics to fetch metadata for. Null means all topics.
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// Whether to allow auto topic creation (v4+).
    pub allow_auto_topic_creation: bool,
}

/// Topic in metadata request.
#[derive(Debug, Clone)]
pub struct MetadataRequestTopic {
    /// Topic ID (v10+).
    pub topic_id: Option<[u8; 16]>,
    /// Topic name.
    pub name: Option<String>,
}

impl MetadataRequest {
    /// Create a request for all topics.
    ///
    /// `topics: None`. `encode_v0` converts this to an empty array (v0 is
    /// non-nullable); `encode_v1`+ emits a null array.
    pub fn all_topics() -> Self {
        Self {
            topics: None,
            ..Default::default()
        }
    }

    /// Create a request for specific topics.
    pub fn for_topics(topics: Vec<&str>) -> Self {
        Self {
            topics: Some(
                topics
                    .into_iter()
                    .map(|name| MetadataRequestTopic {
                        topic_id: None,
                        name: Some(name.to_string()),
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Metadata
    }

    /// Extract topic names as `KafkaString`s for wire encoding (v0-v8).
    ///
    /// Used only by pre-flexible encode paths. Returns an error if any
    /// entry has `name: None`; flexible encoders (v9+) handle topic IDs
    /// directly via [`encode_topic_entries_flexible`].
    fn topic_names(topics: &[MetadataRequestTopic]) -> Result<Vec<KafkaString>> {
        topics
            .iter()
            .map(|t| {
                t.name.as_ref().map(KafkaString::new).ok_or_else(|| {
                    crate::error::KrafkaError::protocol(
                        "MetadataRequestTopic.name is required for v0-v8",
                    )
                })
            })
            .collect()
    }

    /// Encode for version 1-3 (topics is nullable; `None` → null array = "all topics").
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.topics {
            None => KafkaArray::<KafkaString>::null().try_encode(buf)?,
            Some(topics) => KafkaArray::new(Self::topic_names(topics)?).try_encode(buf)?,
        }
        Ok(())
    }

    /// Encode for version 4-7.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_v1(buf)?;
        buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
        Ok(())
    }

    /// Encode for version 8.
    ///
    /// Authorized-operations flags are always encoded as `false` because
    /// `MetadataResponse` does not yet surface the results.
    pub fn encode_v8(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_v4(buf)?;
        buf.put_u8(0); // include_cluster_authorized_operations — not yet surfaced
        buf.put_u8(0); // include_topic_authorized_operations — not yet surfaced
        Ok(())
    }

    /// Encode for version 9 (flexible).
    ///
    /// v9: first flexible version. Each topic entry is a struct with a compact
    /// name string and its own tagged-fields section. `IncludeClusterAuthorizedOperations`
    /// and `IncludeTopicAuthorizedOperations` are still present.
    pub fn encode_v9(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_topic_entries_flexible(buf, TopicIdMode::Omit)?;
        buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
        buf.put_u8(0); // include_cluster_authorized_operations (v8-v10)
        buf.put_u8(0); // include_topic_authorized_operations  (v8+)
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 10 (flexible).
    ///
    /// v10 adds a 16-byte `TopicId` (UUID) per topic entry, but v10
    /// must NOT populate the field — the all-zero UUID is always written.
    /// `IncludeClusterAuthorizedOperations` is still present on the wire.
    pub fn encode_v10(&self, buf: &mut impl BufMut) -> Result<()> {
        // v10: TopicId field present on wire but must be all zeros.
        self.encode_topic_entries_flexible(buf, TopicIdMode::ForceZero)?;
        buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
        buf.put_u8(0); // include_cluster_authorized_operations (v8-v10)
        buf.put_u8(0); // include_topic_authorized_operations  (v8+)
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 11 (flexible).
    ///
    /// v11 deprecates `IncludeClusterAuthorizedOperations` (removed from wire).
    /// TopicId is still forced to zeros (see v10 note).
    pub fn encode_v11(&self, buf: &mut impl BufMut) -> Result<()> {
        // v11: TopicId field present on wire but must be all zeros.
        self.encode_topic_entries_flexible(buf, TopicIdMode::ForceZero)?;
        buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
        buf.put_u8(0); // include_topic_authorized_operations (v8+)
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 12-13 (flexible).
    ///
    /// v12+ supports real `TopicId` lookups. We still default to the all-zero
    /// UUID (name-based lookup) when `topic_id` is `None`.
    pub fn encode_v12(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_topic_entries_flexible(buf, TopicIdMode::UseField)?;
        buf.put_u8(if self.allow_auto_topic_creation { 1 } else { 0 });
        buf.put_u8(0); // include_topic_authorized_operations (v8+)
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode topic entries for flexible versions (v9+).
    ///
    /// Each entry is a struct: `[TopicId(v10+)] Name TaggedFields`.
    fn encode_topic_entries_flexible(
        &self,
        buf: &mut impl BufMut,
        topic_id_mode: TopicIdMode,
    ) -> Result<()> {
        match &self.topics {
            None => {
                // null compact array: varint 0
                KafkaArray::<KafkaString>::null().try_encode_compact(buf)?;
            }
            Some(topics) => {
                let len_plus_one = u32::try_from(topics.len().saturating_add(1)).map_err(|_| {
                    crate::error::KrafkaError::protocol(format!(
                        "topics array length {} exceeds u32 limit",
                        topics.len()
                    ))
                })?;
                crate::util::varint::encode_unsigned_varint(len_plus_one, buf);
                for t in topics {
                    match topic_id_mode {
                        TopicIdMode::Omit | TopicIdMode::ForceZero => {
                            // v9-v11: TopicId is absent or zero — name is required.
                            if t.name.is_none() {
                                return Err(crate::error::KrafkaError::protocol(
                                    "MetadataRequest topic name must be non-null \
                                     when TopicId is absent or zero",
                                ));
                            }
                            if matches!(topic_id_mode, TopicIdMode::ForceZero) {
                                buf.put_slice(&[0u8; 16]);
                            }
                        }
                        TopicIdMode::UseField => {
                            // v12+: at least one of topic_id/name must be set.
                            if t.topic_id.is_none() && t.name.is_none() {
                                return Err(crate::error::KrafkaError::protocol(
                                    "MetadataRequest topic must have at least one \
                                     of topic_id or name set",
                                ));
                            }
                            buf.put_slice(&t.topic_id.unwrap_or([0u8; 16]));
                        }
                    }
                    // Name — compact string
                    match &t.name {
                        Some(name) => KafkaString::new(name).try_encode_compact(buf)?,
                        None => KafkaString::null().try_encode_compact(buf)?,
                    }
                    // Tagged fields for the struct
                    TaggedFields::default().try_encode(buf)?;
                }
            }
        }
        Ok(())
    }
}

/// Controls how `TopicId` is written in MetadataRequest flexible encoding.
enum TopicIdMode {
    /// No TopicId field on wire (v9 and earlier flexible versions).
    Omit,
    /// TopicId present on wire but forced to all-zero UUID (v10-v11).
    ForceZero,
    /// TopicId taken from the struct field, defaulting to zeros (v12+).
    UseField,
}

/// Metadata response.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MetadataResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<MetadataBroker>,
    /// Cluster ID.
    pub cluster_id: Option<String>,
    /// Controller broker ID.
    pub controller_id: i32,
    /// Topic metadata.
    pub topics: Vec<MetadataTopicResponse>,
    /// Top-level error code (v13+).
    pub error_code: ErrorCode,
}

/// Broker info in metadata response.
#[derive(Debug, Clone)]
pub struct MetadataBroker {
    /// Broker ID.
    pub node_id: i32,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Broker rack.
    pub rack: Option<String>,
}

/// Topic metadata in response.
#[derive(Debug, Clone)]
pub struct MetadataTopicResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Topic name.
    pub name: Option<String>,
    /// Topic ID (v10+).
    pub topic_id: Option<[u8; 16]>,
    /// Is internal topic.
    pub is_internal: bool,
    /// Partition metadata.
    pub partitions: Vec<MetadataPartitionResponse>,
}

/// Partition metadata in response.
#[derive(Debug, Clone)]
pub struct MetadataPartitionResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Partition ID.
    pub partition_index: i32,
    /// Leader broker ID.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Replica broker IDs.
    pub replica_nodes: Vec<i32>,
    /// In-sync replica broker IDs.
    pub isr_nodes: Vec<i32>,
    /// Offline replica broker IDs (v5+).
    pub offline_replicas: Vec<i32>,
}

impl MetadataResponse {
    /// Decode from version 1.
    ///
    /// v1 adds broker rack, controller_id, and topic is_internal.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let brokers = decode_array::<MetadataBrokerV1, _>(buf)?;
        let controller_id = i32::decode(buf)?;
        let topics = decode_array::<MetadataTopicResponseV1, _>(buf)?;
        Ok(Self {
            throttle_time_ms: 0,
            brokers,
            cluster_id: None,
            controller_id,
            topics,
            error_code: ErrorCode::None,
        })
    }

    /// Decode from version 2.
    ///
    /// v2 adds cluster_id.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let brokers = decode_array::<MetadataBrokerV1, _>(buf)?;
        let cluster_id = KafkaString::decode(buf)?.0;
        let controller_id = i32::decode(buf)?;
        let topics = decode_array::<MetadataTopicResponseV1, _>(buf)?;
        Ok(Self {
            throttle_time_ms: 0,
            brokers,
            cluster_id,
            controller_id,
            topics,
            error_code: ErrorCode::None,
        })
    }

    /// Decode from version 3-4.
    ///
    /// v3 adds throttle_time_ms. v4 only changes the request (allow_auto_topic_creation).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v3_plus::<MetadataTopicResponseV1>(buf)
    }

    /// Decode from version 5-6.
    ///
    /// v5 adds partition offline_replicas. v6 has no wire changes.
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v3_plus::<MetadataTopicResponseV5>(buf)
    }

    /// Decode from version 7.
    ///
    /// v7 adds partition leader_epoch.
    pub fn decode_v7(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v3_plus::<MetadataTopicResponseV7>(buf)
    }

    /// Decode from version 8.
    ///
    /// v8 adds topic_authorized_operations and cluster_authorized_operations.
    /// Both are read and discarded — the encoder always sends `false` for the
    /// include flags, so brokers return the "not requested" sentinel (`i32::MIN`).
    /// When authorized-operations support is added, plumb these into the response.
    pub fn decode_v8(buf: &mut impl Buf) -> Result<Self> {
        let resp = Self::decode_v3_plus::<MetadataTopicResponseV8>(buf)?;
        // cluster_authorized_operations — read and discard
        let _cluster_authorized_operations = i32::decode(buf)?;
        Ok(resp)
    }

    /// Decode from version 9 (flexible).
    ///
    /// v9 switches to flexible encoding (compact strings, compact arrays,
    /// tagged fields) but has no new fields compared to v8.
    pub fn decode_v9(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v9_plus::<MetadataTopicResponseV9>(buf, true)
    }

    /// Decode from version 10 (flexible).
    ///
    /// v10 adds a 16-byte topic_id (UUID) to each topic entry. The UUID
    /// is required for KIP-848 consumer protocol assignment resolution.
    /// `ClusterAuthorizedOperations` is still present in v10.
    pub fn decode_v10(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v9_plus::<MetadataTopicResponseV10>(buf, true)
    }

    /// Decode from version 11-12 (flexible).
    ///
    /// v11 deprecates `ClusterAuthorizedOperations` (removed from wire).
    /// Topic entries still include topic_id (UUID).
    pub fn decode_v11(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v9_plus::<MetadataTopicResponseV10>(buf, false)
    }

    /// Shared decoder for Metadata v3-v8 wire format.
    ///
    /// Layout: throttle_time_ms, brokers (v1 format), cluster_id,
    /// controller_id, topics (parametrized by `T`).
    fn decode_v3_plus<T: Decode + Into<MetadataTopicResponse>>(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let brokers = decode_array::<MetadataBrokerV1, _>(buf)?;
        let cluster_id = KafkaString::decode(buf)?.0;
        let controller_id = i32::decode(buf)?;
        let topics = decode_array::<T, _>(buf)?;
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            error_code: ErrorCode::None,
        })
    }

    /// Shared decoder for Metadata v9+ flexible wire format.
    ///
    /// Layout: throttle_time_ms, brokers (compact), cluster_id (compact),
    /// controller_id, topics (compact, parametrized by `T`).
    ///
    /// `include_cluster_auth_ops`: `true` for v9-v10 (field present),
    /// `false` for v11-v12 (field removed from wire).
    fn decode_v9_plus<T: Decode + Into<MetadataTopicResponse>>(
        buf: &mut impl Buf,
        include_cluster_auth_ops: bool,
    ) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let brokers = decode_compact_array::<MetadataBrokerV9, _>(buf)?;
        let cluster_id = KafkaString::decode_compact(buf)?.0;
        let controller_id = i32::decode(buf)?;
        let topics = decode_compact_array::<T, _>(buf)?;
        if include_cluster_auth_ops {
            // cluster_authorized_operations — read and discard (v8-v10)
            let _cluster_authorized_operations = i32::decode(buf)?;
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            error_code: ErrorCode::None,
        })
    }

    /// Decode from version 13 (flexible, top-level error code).
    ///
    /// v13 adds a top-level `ErrorCode` after the topics array.
    pub fn decode_v13(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let brokers = decode_compact_array::<MetadataBrokerV9, _>(buf)?;
        let cluster_id = KafkaString::decode_compact(buf)?.0;
        let controller_id = i32::decode(buf)?;
        let topics = decode_compact_array::<MetadataTopicResponseV10, _>(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
            error_code,
        })
    }

    /// Find broker by ID.
    pub fn find_broker(&self, node_id: i32) -> Option<&MetadataBroker> {
        self.brokers.iter().find(|b| b.node_id == node_id)
    }

    /// Find topic by name.
    pub fn find_topic(&self, name: &str) -> Option<&MetadataTopicResponse> {
        self.topics.iter().find(|t| t.name.as_deref() == Some(name))
    }
}

/// Decode a non-nullable `KafkaArray` (pre-flexible versions) of newtype wrappers
/// and unwrap each element's inner value.
///
/// Returns an error if the wire value is null (length `-1`), since non-nullable
/// arrays must have `length >= 0`. Use this only for fields that are
/// defined as non-nullable in the Kafka protocol schema.
fn decode_array<W: Decode + Into<T>, T>(buf: &mut impl Buf) -> Result<Vec<T>> {
    let items = non_nullable_array(KafkaArray::<W>::decode(buf)?.0)?;
    Ok(items.into_iter().map(Into::into).collect())
}

/// Decode a non-nullable compact `KafkaArray` (flexible versions) of newtype wrappers
/// and unwrap each.
///
/// Returns an error if the wire value is null (raw varint 0), since non-nullable
/// compact arrays must have `raw >= 1`. Use this only for fields that are
/// defined as non-nullable in the Kafka protocol schema.
fn decode_compact_array<W: Decode + Into<T>, T>(buf: &mut impl Buf) -> Result<Vec<T>> {
    let items = KafkaArray::<W>::decode_compact(buf)?.0.ok_or_else(|| {
        crate::error::KrafkaError::protocol(
            "compact array raw value 0 (null) is invalid for a non-nullable field",
        )
    })?;
    Ok(items.into_iter().map(Into::into).collect())
}

/// Reject null for a non-nullable array field.
///
/// In the Kafka wire format, a length of `-1` encodes a null array.
/// Non-nullable fields must never be null — this helper turns `None` into
/// a protocol error instead of silently defaulting to an empty `Vec`.
fn non_nullable_array<T>(opt: Option<Vec<T>>) -> Result<Vec<T>> {
    opt.ok_or_else(|| {
        crate::error::KrafkaError::protocol(
            "array length -1 (null) is invalid for a non-nullable field",
        )
    })
}

// ── Metadata decode helper newtypes ─────────────────────────────────
//
// Each newtype decodes a specific wire format version and converts into the
// public response type via `From`. Only the fields that differ between
// versions need a separate newtype; higher versions reuse lower partition
// decoders when the partition layout hasn't changed.

/// v0 broker decoder: node_id, host, port (no rack).
struct MetadataBrokerV0(MetadataBroker);

impl Decode for MetadataBrokerV0 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?.0.ok_or_else(|| {
            KrafkaError::protocol("metadata broker host must be a non-null string")
        })?;
        let port = i32::decode(buf)?;
        Ok(Self(MetadataBroker {
            node_id,
            host,
            port,
            rack: None,
        }))
    }
}

impl From<MetadataBrokerV0> for MetadataBroker {
    fn from(w: MetadataBrokerV0) -> Self {
        w.0
    }
}

/// v1+ broker decoder: adds rack (nullable string).
struct MetadataBrokerV1(MetadataBroker);

impl Decode for MetadataBrokerV1 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?.0.ok_or_else(|| {
            KrafkaError::protocol("metadata broker host must be a non-null string")
        })?;
        let port = i32::decode(buf)?;
        let rack = KafkaString::decode(buf)?.0;
        Ok(Self(MetadataBroker {
            node_id,
            host,
            port,
            rack,
        }))
    }
}

impl From<MetadataBrokerV1> for MetadataBroker {
    fn from(w: MetadataBrokerV1) -> Self {
        w.0
    }
}

/// v9+ broker decoder (flexible): compact strings + tagged fields.
struct MetadataBrokerV9(MetadataBroker);

impl Decode for MetadataBrokerV9 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode_compact(buf)?.0.ok_or_else(|| {
            KrafkaError::protocol("metadata broker host must be a non-null compact string")
        })?;
        let port = i32::decode(buf)?;
        let rack = KafkaString::decode_compact(buf)?.0;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self(MetadataBroker {
            node_id,
            host,
            port,
            rack,
        }))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf)
    }
}

impl From<MetadataBrokerV9> for MetadataBroker {
    fn from(w: MetadataBrokerV9) -> Self {
        w.0
    }
}

/// v0 partition decoder: no offline_replicas, no leader_epoch.
struct MetadataPartitionResponseV0(MetadataPartitionResponse);

impl Decode for MetadataPartitionResponseV0 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let replica_nodes = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        let isr_nodes = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        Ok(Self(MetadataPartitionResponse {
            error_code,
            partition_index,
            leader_id,
            leader_epoch: -1,
            replica_nodes,
            isr_nodes,
            offline_replicas: Vec::new(),
        }))
    }
}

impl From<MetadataPartitionResponseV0> for MetadataPartitionResponse {
    fn from(w: MetadataPartitionResponseV0) -> Self {
        w.0
    }
}

/// v5+ partition decoder: adds offline_replicas.
struct MetadataPartitionResponseV5(MetadataPartitionResponse);

impl Decode for MetadataPartitionResponseV5 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let replica_nodes = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        let isr_nodes = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        let offline_replicas = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        Ok(Self(MetadataPartitionResponse {
            error_code,
            partition_index,
            leader_id,
            leader_epoch: -1,
            replica_nodes,
            isr_nodes,
            offline_replicas,
        }))
    }
}

impl From<MetadataPartitionResponseV5> for MetadataPartitionResponse {
    fn from(w: MetadataPartitionResponseV5) -> Self {
        w.0
    }
}

/// v7+ partition decoder: adds leader_epoch (offline_replicas since v5).
struct MetadataPartitionResponseV7(MetadataPartitionResponse);

impl Decode for MetadataPartitionResponseV7 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let leader_epoch = i32::decode(buf)?;
        let replica_nodes = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        let isr_nodes = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        let offline_replicas = non_nullable_array(KafkaArray::<i32>::decode(buf)?.0)?;
        Ok(Self(MetadataPartitionResponse {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
        }))
    }
}

impl From<MetadataPartitionResponseV7> for MetadataPartitionResponse {
    fn from(w: MetadataPartitionResponseV7) -> Self {
        w.0
    }
}

/// v9+ partition decoder (flexible): compact arrays + tagged fields.
struct MetadataPartitionResponseV9(MetadataPartitionResponse);

impl Decode for MetadataPartitionResponseV9 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let leader_epoch = i32::decode(buf)?;
        // replica_nodes, isr_nodes, offline_replicas are non-nullable in v9+.
        // Use check_compact_array_len (rejects varint 0 → null) instead of
        // decode_compact().unwrap_or_default() which silently coerces null.
        let replica_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut replica_nodes = Vec::with_capacity(replica_count);
        for _ in 0..replica_count {
            replica_nodes.push(i32::decode(buf)?);
        }
        let isr_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut isr_nodes = Vec::with_capacity(isr_count);
        for _ in 0..isr_count {
            isr_nodes.push(i32::decode(buf)?);
        }
        let offline_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut offline_replicas = Vec::with_capacity(offline_count);
        for _ in 0..offline_count {
            offline_replicas.push(i32::decode(buf)?);
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self(MetadataPartitionResponse {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
        }))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf)
    }
}

impl From<MetadataPartitionResponseV9> for MetadataPartitionResponse {
    fn from(w: MetadataPartitionResponseV9) -> Self {
        w.0
    }
}

/// v0 topic decoder: no is_internal, v0 partitions.
struct MetadataTopicResponseV0(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV0 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode(buf)?.0;
        let partitions = decode_array::<MetadataPartitionResponseV0, _>(buf)?;
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: None,
            is_internal: false,
            partitions,
        }))
    }
}

impl From<MetadataTopicResponseV0> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV0) -> Self {
        w.0
    }
}

/// v1-v4 topic decoder: adds is_internal, v0 partitions.
struct MetadataTopicResponseV1(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV1 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode(buf)?.0;
        let is_internal = bool::decode(buf)?;
        let partitions = decode_array::<MetadataPartitionResponseV0, _>(buf)?;
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: None,
            is_internal,
            partitions,
        }))
    }
}

impl From<MetadataTopicResponseV1> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV1) -> Self {
        w.0
    }
}

/// v5-v6 topic decoder: is_internal + v5 partitions (offline_replicas).
struct MetadataTopicResponseV5(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV5 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode(buf)?.0;
        let is_internal = bool::decode(buf)?;
        let partitions = decode_array::<MetadataPartitionResponseV5, _>(buf)?;
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: None,
            is_internal,
            partitions,
        }))
    }
}

impl From<MetadataTopicResponseV5> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV5) -> Self {
        w.0
    }
}

/// v7 topic decoder: is_internal + v7 partitions (leader_epoch + offline_replicas).
struct MetadataTopicResponseV7(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV7 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode(buf)?.0;
        let is_internal = bool::decode(buf)?;
        let partitions = decode_array::<MetadataPartitionResponseV7, _>(buf)?;
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: None,
            is_internal,
            partitions,
        }))
    }
}

impl From<MetadataTopicResponseV7> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV7) -> Self {
        w.0
    }
}

/// v8 topic decoder: v7 partitions + topic_authorized_operations (discarded).
struct MetadataTopicResponseV8(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV8 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode(buf)?.0;
        let is_internal = bool::decode(buf)?;
        let partitions = decode_array::<MetadataPartitionResponseV7, _>(buf)?;
        // topic_authorized_operations — read and discard
        let _topic_authorized_operations = i32::decode(buf)?;
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: None,
            is_internal,
            partitions,
        }))
    }
}

impl From<MetadataTopicResponseV8> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV8) -> Self {
        w.0
    }
}

/// v9 topic decoder (flexible): compact strings/arrays + tagged fields, no topic_id yet.
struct MetadataTopicResponseV9(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV9 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode_compact(buf)?.0;
        let is_internal = bool::decode(buf)?;
        let partitions = decode_compact_array::<MetadataPartitionResponseV9, _>(buf)?;
        // topic_authorized_operations — read and discard
        let _topic_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: None,
            is_internal,
            partitions,
        }))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf)
    }
}

impl From<MetadataTopicResponseV9> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV9) -> Self {
        w.0
    }
}

/// v10+ topic decoder (flexible): adds topic_id (UUID).
struct MetadataTopicResponseV10(MetadataTopicResponse);

impl Decode for MetadataTopicResponseV10 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let name = KafkaString::decode_compact(buf)?.0;
        // topic_id: 16-byte UUID
        let mut topic_id = [0u8; 16];
        if buf.remaining() < 16 {
            return Err(crate::error::KrafkaError::protocol(
                "not enough bytes for topic_id UUID",
            ));
        }
        buf.copy_to_slice(&mut topic_id);
        let is_internal = bool::decode(buf)?;
        let partitions = decode_compact_array::<MetadataPartitionResponseV9, _>(buf)?;
        // topic_authorized_operations — read and discard
        let _topic_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        // Treat all-zero UUID as absent.
        let topic_id_opt = if topic_id == [0u8; 16] {
            None
        } else {
            Some(topic_id)
        };
        Ok(Self(MetadataTopicResponse {
            error_code,
            name,
            topic_id: topic_id_opt,
            is_internal,
            partitions,
        }))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf)
    }
}

impl From<MetadataTopicResponseV10> for MetadataTopicResponse {
    fn from(w: MetadataTopicResponseV10) -> Self {
        w.0
    }
}

// Produce request/response

impl VersionedEncode for MetadataRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1..=3 => self.encode_v1(buf)?,
            4..=7 => self.encode_v4(buf)?,
            8 => self.encode_v8(buf)?,
            9 => self.encode_v9(buf)?,
            10 => self.encode_v10(buf)?,
            11 => self.encode_v11(buf)?,
            12..=13 => self.encode_v12(buf)?,
            _ => return unsupported_encode!("MetadataRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for MetadataResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            3..=4 => Self::decode_v3(buf),
            5..=6 => Self::decode_v5(buf),
            7 => Self::decode_v7(buf),
            8 => Self::decode_v8(buf),
            9 => Self::decode_v9(buf),
            10 => Self::decode_v10(buf),
            11..=12 => Self::decode_v11(buf),
            13 => Self::decode_v13(buf),
            _ => unsupported_decode!("MetadataResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    /// Helper to write a v9 flexible metadata response.
    fn build_metadata_response_v9_bytes() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        // brokers compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(1); // node_id
        let host = b"broker-1";
        varint::encode_unsigned_varint(host.len() as u32 + 1, &mut buf);
        buf.put_slice(host);
        buf.put_i32(9092); // port
        // rack null compact string
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf); // broker tagged fields
        // cluster_id nullable compact string
        let cluster = b"cluster-abc";
        varint::encode_unsigned_varint(cluster.len() as u32 + 1, &mut buf);
        buf.put_slice(cluster);
        buf.put_i32(1); // controller_id
        // topics compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i16(0); // topic error_code
        // topic name compact string
        let topic = b"test-topic";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        buf.put_u8(0); // is_internal = false
        // partitions compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i16(0); // partition error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(5); // leader_epoch
        // replica_nodes compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(1);
        // isr_nodes compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(1);
        // offline_replicas compact array: empty (1 = 0 + 1)
        varint::encode_unsigned_varint(1, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged fields
        buf.put_i32(0x7FFF_FFFF); // topic_authorized_operations
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        buf.put_i32(0x7FFF_FFFF); // cluster_authorized_operations
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields
        buf
    }

    #[test]
    fn test_metadata_request_all_topics() {
        let request = MetadataRequest::all_topics();
        // Null array = "all topics" for Metadata v1+.
        assert!(request.topics.is_none());
    }

    /// v1+: topics is nullable — `None` encodes as null array (length -1).
    #[test]
    fn test_metadata_request_all_topics_encode_v1() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // v1+: null array (-1 length) means "all topics"
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), -1);
    }

    #[test]
    fn test_metadata_request_specific_topics() {
        let request = MetadataRequest::for_topics(vec!["topic1", "topic2"]);
        assert_eq!(request.topics.as_ref().unwrap().len(), 2);
    }

    // ── VersionedEncode / VersionedDecode tests ─────────────────────

    #[test]
    fn test_versioned_encode_metadata_request_v0_unsupported() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
    }

    #[test]
    fn test_versioned_encode_metadata_request_v1() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_versioned(1, &mut buf).unwrap();
        let mut expected = BytesMut::new();
        request.encode_v1(&mut expected).unwrap();
        assert_eq!(buf, expected);
    }

    /// v1 and v4 encode differently for all_topics(): null array vs nullable array with allow_auto_topic_creation.
    #[test]
    fn test_versioned_encode_metadata_v1_vs_v4_all_topics() {
        let request = MetadataRequest::all_topics();
        let mut buf_v1 = BytesMut::new();
        request.encode_versioned(1, &mut buf_v1).unwrap();
        let mut buf_v4 = BytesMut::new();
        request.encode_versioned(4, &mut buf_v4).unwrap();
        // v1 and v4 should produce different output (v4 adds allow_auto_topic_creation)
        assert_ne!(buf_v1, buf_v4);
    }

    #[test]
    fn test_versioned_encode_metadata_request_v4() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_versioned(4, &mut buf).unwrap();
        let mut expected = BytesMut::new();
        request.encode_v4(&mut expected).unwrap();
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_versioned_encode_metadata_request_v8() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_versioned(8, &mut buf).unwrap();
        let mut expected = BytesMut::new();
        request.encode_v8(&mut expected).unwrap();
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_versioned_encode_metadata_request_rejects_v14() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        let result = request.encode_versioned(14, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    #[test]
    fn test_versioned_decode_metadata_rejects_v14() {
        let mut buf = bytes::Bytes::new();
        let result = MetadataResponse::decode_versioned(14, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    /// v0 is below METADATA_MIN and should be rejected by decode_versioned.
    #[test]
    fn test_metadata_response_decode_v0_unsupported() {
        let mut buf = BytesMut::new();
        // minimal v0 payload — just enough to prove it rejects before parsing
        buf.put_i32(0); // 0 brokers
        buf.put_i32(0); // 0 topics

        assert!(MetadataResponse::decode_versioned(0, &mut buf.freeze()).is_err());
    }

    /// Build a v1 Metadata response on the wire:
    /// brokers (with rack) + controller_id + topics (with is_internal).
    #[test]
    fn test_metadata_response_decode_v1() {
        let mut buf = BytesMut::new();
        // 1 broker
        buf.put_i32(1);
        buf.put_i32(1); // node_id
        let host = b"broker1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(9092); // port
        let rack = b"us-east-1a";
        buf.put_i16(rack.len() as i16);
        buf.put_slice(rack); // rack
        // controller_id
        buf.put_i32(1);
        // 1 topic
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        let topic = b"test";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_u8(0); // is_internal = false
        // 1 partition
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(1); // replicas count
        buf.put_i32(1); // replica
        buf.put_i32(1); // isr count
        buf.put_i32(1); // isr

        let resp = MetadataResponse::decode_versioned(1, &mut buf.freeze()).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].rack.as_deref(), Some("us-east-1a"));
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.cluster_id, None);
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, -1);
    }

    /// v2 adds cluster_id.
    #[test]
    fn test_metadata_response_decode_v2() {
        let mut buf = BytesMut::new();
        // 1 broker
        buf.put_i32(1);
        buf.put_i32(1);
        let host = b"broker1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(9092);
        let rack = b"rack-a";
        buf.put_i16(rack.len() as i16);
        buf.put_slice(rack);
        // cluster_id
        let cid = b"abc-cluster";
        buf.put_i16(cid.len() as i16);
        buf.put_slice(cid);
        // controller_id
        buf.put_i32(1);
        // 0 topics
        buf.put_i32(0);

        let resp = MetadataResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(resp.cluster_id.as_deref(), Some("abc-cluster"));
        assert_eq!(resp.brokers[0].rack.as_deref(), Some("rack-a"));
    }

    /// v3 adds throttle_time_ms.
    #[test]
    fn test_metadata_response_decode_v3() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        // 0 brokers
        buf.put_i32(0);
        // cluster_id = null
        buf.put_i16(-1);
        // controller_id
        buf.put_i32(-1);
        // 0 topics
        buf.put_i32(0);

        let resp = MetadataResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.cluster_id, None);
    }

    /// v4 response is same wire format as v3.
    #[test]
    fn test_metadata_response_decode_v4() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(0); // 0 brokers
        let cid = b"kraft-cluster-1";
        buf.put_i16(cid.len() as i16);
        buf.put_slice(cid);
        buf.put_i32(2); // controller_id
        buf.put_i32(0); // 0 topics

        let resp = MetadataResponse::decode_versioned(4, &mut buf.freeze()).unwrap();
        assert_eq!(resp.cluster_id.as_deref(), Some("kraft-cluster-1"));
        assert_eq!(resp.controller_id, 2);
    }

    /// v5 adds partition offline_replicas.
    #[test]
    fn test_metadata_response_decode_v5() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(0); // 0 brokers
        buf.put_i16(-1); // cluster_id null
        buf.put_i32(-1); // controller_id
        // 1 topic
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        let topic = b"t1";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_u8(0); // is_internal
        // 1 partition
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(2); // replicas count
        buf.put_i32(1);
        buf.put_i32(2);
        buf.put_i32(2); // isr count
        buf.put_i32(1);
        buf.put_i32(2);
        buf.put_i32(1); // offline_replicas count
        buf.put_i32(2); // offline replica

        let resp = MetadataResponse::decode_versioned(5, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].partitions[0].offline_replicas, vec![2]);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, -1); // not in v5
    }

    /// v6 has the same wire format as v5 — verify dispatch routes through decode_v5.
    #[test]
    fn test_metadata_response_decode_v6() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(0); // 0 brokers
        buf.put_i16(-1); // cluster_id null
        buf.put_i32(-1); // controller_id
        // 1 topic
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        let topic = b"t2";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_u8(1); // is_internal = true
        // 1 partition
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(1); // replicas count
        buf.put_i32(1);
        buf.put_i32(1); // isr count
        buf.put_i32(1);
        buf.put_i32(0); // offline_replicas count

        let resp = MetadataResponse::decode_versioned(6, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name.as_deref(), Some("t2"));
        assert!(resp.topics[0].is_internal);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, -1); // not in v6
    }

    /// v7 adds partition leader_epoch.
    #[test]
    fn test_metadata_response_decode_v7() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        // 1 broker with rack
        buf.put_i32(1);
        buf.put_i32(1);
        let host = b"broker1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(9092);
        let rack = b"az-1";
        buf.put_i16(rack.len() as i16);
        buf.put_slice(rack);
        // cluster_id
        let cid = b"kraft-id";
        buf.put_i16(cid.len() as i16);
        buf.put_slice(cid);
        buf.put_i32(1); // controller_id
        // 1 topic
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        let topic = b"events";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_u8(0); // is_internal
        // 1 partition
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(42); // leader_epoch (new in v7)
        buf.put_i32(1); // replicas count
        buf.put_i32(1);
        buf.put_i32(1); // isr count
        buf.put_i32(1);
        buf.put_i32(0); // offline_replicas count

        let resp = MetadataResponse::decode_versioned(7, &mut buf.freeze()).unwrap();
        assert_eq!(resp.cluster_id.as_deref(), Some("kraft-id"));
        assert_eq!(resp.brokers[0].rack.as_deref(), Some("az-1"));
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 42);
        assert!(resp.topics[0].partitions[0].offline_replicas.is_empty());
    }

    /// v8 adds topic_authorized_operations and cluster_authorized_operations.
    #[test]
    fn test_metadata_response_decode_v8() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i32(0); // 0 brokers
        let cid = b"kraft-8";
        buf.put_i16(cid.len() as i16);
        buf.put_slice(cid);
        buf.put_i32(0); // controller_id
        // 1 topic
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        let topic = b"orders";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_u8(0); // is_internal
        // 0 partitions
        buf.put_i32(0);
        buf.put_i32(-2147483648_i32); // topic_authorized_operations (not requested)
        // cluster_authorized_operations
        buf.put_i32(-2147483648_i32);

        let resp = MetadataResponse::decode_versioned(8, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.cluster_id.as_deref(), Some("kraft-8"));
        assert_eq!(resp.topics[0].name.as_deref(), Some("orders"));
    }

    /// v9: flexible encoding, no topic_id.
    #[test]
    fn test_metadata_response_decode_v9() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms

        // 1 broker (compact array: length + 1 as unsigned varint)
        buf.put_u8(2); // compact array count = 1 + 1
        buf.put_i32(1); // node_id
        // compact string "b1" (length + 1 as unsigned varint)
        buf.put_u8(3); // len 2 + 1
        buf.put_slice(b"b1");
        buf.put_i32(9092); // port
        buf.put_u8(1); // rack = null compact string (0 means null, 1 means empty)
        buf.put_u8(0); // tagged fields (empty)

        // cluster_id compact nullable string
        buf.put_u8(6); // len 5 + 1
        buf.put_slice(b"cls-9");
        buf.put_i32(1); // controller_id

        // 1 topic (compact array)
        buf.put_u8(2); // 1 + 1
        buf.put_i16(0); // error_code
        // topic name compact string
        buf.put_u8(5); // len 4 + 1
        buf.put_slice(b"my-t");
        buf.put_u8(0); // is_internal = false
        buf.put_u8(1); // 0 partitions (compact array 0 + 1)
        buf.put_i32(-2147483648_i32); // topic_authorized_operations
        buf.put_u8(0); // tagged fields

        // cluster_authorized_operations
        buf.put_i32(-2147483648_i32);
        buf.put_u8(0); // tagged fields (top-level)

        let resp = MetadataResponse::decode_versioned(9, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.cluster_id.as_deref(), Some("cls-9"));
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].host, "b1");
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name.as_deref(), Some("my-t"));
        assert!(resp.topics[0].topic_id.is_none());
    }

    /// v9: exercises the partition-level decode path (replica_nodes, isr_nodes, offline_replicas)
    /// through `check_compact_array_len` + varint, ensuring non-nullable compact arrays are
    /// correctly decoded.
    #[test]
    fn test_metadata_response_decode_v9_with_partitions() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms

        // 0 brokers
        buf.put_u8(1); // compact array 0+1

        // cluster_id = null
        buf.put_u8(0);
        buf.put_i32(-1); // controller_id

        // 1 topic
        buf.put_u8(2); // 1+1
        buf.put_i16(0); // error_code
        buf.put_u8(3); // compact string "t1" (len 2+1)
        buf.put_slice(b"t1");
        buf.put_u8(0); // is_internal = false

        // 1 partition
        buf.put_u8(2); // compact array 1+1
        buf.put_i16(0); // partition error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(5); // leader_epoch

        // replica_nodes: [1, 2, 3]
        buf.put_u8(4); // compact array 3+1
        buf.put_i32(1);
        buf.put_i32(2);
        buf.put_i32(3);
        // isr_nodes: [1, 2]
        buf.put_u8(3); // compact array 2+1
        buf.put_i32(1);
        buf.put_i32(2);
        // offline_replicas: [] (empty, not null)
        buf.put_u8(1); // compact array 0+1
        buf.put_u8(0); // tagged fields (partition)

        buf.put_i32(-2147483648_i32); // topic_authorized_operations
        buf.put_u8(0); // tagged fields (topic)

        buf.put_i32(-2147483648_i32); // cluster_authorized_operations
        buf.put_u8(0); // tagged fields (top-level)

        let resp = MetadataResponse::decode_versioned(9, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        let topic = &resp.topics[0];
        assert_eq!(topic.name.as_deref(), Some("t1"));
        assert_eq!(topic.partitions.len(), 1);
        let part = &topic.partitions[0];
        assert_eq!(part.partition_index, 0);
        assert_eq!(part.leader_id, 1);
        assert_eq!(part.leader_epoch, 5);
        assert_eq!(part.replica_nodes, vec![1, 2, 3]);
        assert_eq!(part.isr_nodes, vec![1, 2]);
        assert!(part.offline_replicas.is_empty());
    }

    /// v9: null non-nullable compact array (varint 0) in partition must be rejected.
    #[test]
    fn test_metadata_response_decode_v9_null_replica_array_rejected() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(1); // 0 brokers
        buf.put_u8(0); // cluster_id = null
        buf.put_i32(-1); // controller_id

        // 1 topic, 1 partition
        buf.put_u8(2); // 1 topic
        buf.put_i16(0); // error_code
        buf.put_u8(3); // compact string "t1"
        buf.put_slice(b"t1");
        buf.put_u8(0); // is_internal = false
        buf.put_u8(2); // 1 partition
        buf.put_i16(0); // partition error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(0); // leader_epoch
        // replica_nodes: null (varint 0 — invalid for non-nullable field)
        buf.put_u8(0);

        let err = MetadataResponse::decode_versioned(9, &mut buf.freeze()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("null") || msg.contains("0"),
            "expected null/0 rejection error, got: {msg}"
        );
    }

    /// v10: flexible encoding + topic_id UUID.
    #[test]
    fn test_metadata_response_decode_v10() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms

        // 0 brokers
        buf.put_u8(1); // compact array count = 0 + 1

        // cluster_id = null
        buf.put_u8(0);
        buf.put_i32(-1); // controller_id

        // 1 topic
        buf.put_u8(2); // 1 + 1
        buf.put_i16(0); // error_code
        // topic name compact string "events"
        buf.put_u8(7); // len 6 + 1
        buf.put_slice(b"events");
        // topic_id: 16-byte UUID
        let topic_uuid: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        buf.put_slice(&topic_uuid);
        buf.put_u8(0); // is_internal = false
        buf.put_u8(1); // 0 partitions
        buf.put_i32(-2147483648_i32); // topic_authorized_operations
        buf.put_u8(0); // tagged fields

        buf.put_i32(-2147483648_i32); // cluster_authorized_operations
        buf.put_u8(0); // tagged fields

        let resp = MetadataResponse::decode_versioned(10, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name.as_deref(), Some("events"));
        assert_eq!(resp.topics[0].topic_id, Some(topic_uuid));
    }

    /// v10: all-zero topic_id is treated as absent.
    #[test]
    fn test_metadata_response_decode_v10_zero_uuid() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(1); // 0 brokers
        buf.put_u8(0); // cluster_id = null
        buf.put_i32(-1); // controller_id

        buf.put_u8(2); // 1 topic
        buf.put_i16(0); // error_code
        buf.put_u8(4); // compact string "foo"
        buf.put_slice(b"foo");
        buf.put_slice(&[0u8; 16]); // all-zero UUID
        buf.put_u8(0); // is_internal = false
        buf.put_u8(1); // 0 partitions
        buf.put_i32(-2147483648_i32); // topic_authorized_operations
        buf.put_u8(0); // tagged fields

        buf.put_i32(-2147483648_i32); // cluster_authorized_operations
        buf.put_u8(0); // tagged fields

        let resp = MetadataResponse::decode_versioned(10, &mut buf.freeze()).unwrap();
        assert!(resp.topics[0].topic_id.is_none());
    }

    /// v9 encode is flexible (compact arrays + tagged fields).
    #[test]
    fn test_metadata_request_encode_v9() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_v9(&mut buf).unwrap();
        // Compact null array (0 varint) + allow_auto_topic_creation + 2 authorized ops + tagged fields
        assert!(!buf.is_empty());
        // Verify VersionedEncode dispatches correctly
        let mut buf2 = BytesMut::new();
        request.encode_versioned(9, &mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    /// v12 body matches v11 for all_topics() (no per-topic entries to differ).
    #[test]
    fn test_metadata_versioned_v12_dispatches() {
        let request = MetadataRequest::all_topics();
        let mut buf_v11 = BytesMut::new();
        request.encode_v11(&mut buf_v11).unwrap();
        let mut buf_v12 = BytesMut::new();
        request.encode_versioned(12, &mut buf_v12).unwrap();
        assert_eq!(buf_v11, buf_v12);
    }

    /// Encode v4 adds allow_auto_topic_creation byte on top of v1.
    #[test]
    fn test_metadata_request_encode_v4_adds_auto_create() {
        let request = MetadataRequest::all_topics();
        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();
        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        // v4 = v1 + 1 byte (allow_auto_topic_creation)
        assert_eq!(buf_v4.len(), buf_v1.len() + 1);
    }

    /// v10 encode adds 16-byte topic_id per entry vs v9.
    #[test]
    fn test_metadata_request_encode_v10_adds_topic_id() {
        let request = MetadataRequest::for_topics(vec!["my-test"]);
        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();
        let mut buf_v10 = BytesMut::new();
        request.encode_v10(&mut buf_v10).unwrap();
        // v10 adds 16-byte topic_id per topic entry
        assert_eq!(buf_v10.len(), buf_v9.len() + 16);
    }

    /// v11 encode omits IncludeClusterAuthorizedOperations (1 byte shorter than v10).
    #[test]
    fn test_metadata_request_encode_v11_no_cluster_auth_ops() {
        let request = MetadataRequest::all_topics();
        let mut buf_v10 = BytesMut::new();
        request.encode_v10(&mut buf_v10).unwrap();
        let mut buf_v11 = BytesMut::new();
        request.encode_v11(&mut buf_v11).unwrap();
        // v11 omits include_cluster_authorized_operations (1 byte less)
        assert_eq!(buf_v11.len(), buf_v10.len() - 1);
    }

    /// v11 decode: no ClusterAuthorizedOperations field on wire.
    #[test]
    fn test_metadata_response_decode_v11() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(1); // 0 brokers
        buf.put_u8(0); // cluster_id = null
        buf.put_i32(-1); // controller_id

        // 1 topic
        buf.put_u8(2); // 1 + 1
        buf.put_i16(0); // error_code
        buf.put_u8(4); // compact string "foo"
        buf.put_slice(b"foo");
        buf.put_slice(&[0xAB; 16]); // topic_id UUID
        buf.put_u8(0); // is_internal = false
        buf.put_u8(1); // 0 partitions
        buf.put_i32(-2147483648_i32); // topic_authorized_operations
        buf.put_u8(0); // tagged fields

        // NO cluster_authorized_operations for v11+
        buf.put_u8(0); // tagged fields (top-level)

        let resp = MetadataResponse::decode_versioned(11, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name.as_deref(), Some("foo"));
        assert_eq!(resp.topics[0].topic_id, Some([0xAB; 16]));
    }

    /// v9 encode with specific topics encodes each as a struct with tagged fields.
    #[test]
    fn test_metadata_request_encode_v9_with_topics() {
        let request = MetadataRequest::for_topics(vec!["test-topic"]);
        let mut buf = BytesMut::new();
        request.encode_v9(&mut buf).unwrap();
        // Should encode: compact array len(1+1=2), compact string("test-topic", len+1=11),
        // tagged fields(0), allow_auto_topic_creation(1), 2x auth ops, top-level tagged fields
        assert!(!buf.is_empty());

        // Verify round-trip via VersionedEncode
        let mut buf2 = BytesMut::new();
        request.encode_versioned(9, &mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    /// Encode v8 adds two more boolean bytes.
    #[test]
    fn test_metadata_request_encode_v8_adds_authorized_ops() {
        let request = MetadataRequest::all_topics();
        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        let mut buf_v8 = BytesMut::new();
        request.encode_v8(&mut buf_v8).unwrap();
        // v8 = v4 + 2 bytes (include_cluster/topic_authorized_operations)
        assert_eq!(buf_v8.len(), buf_v4.len() + 2);
    }

    #[test]
    fn test_encode_oversized_string_returns_error_not_panic() {
        // A string exceeding i16::MAX bytes must produce an Err, not a panic.
        let oversized = "x".repeat(i16::MAX as usize + 1);
        let request = FindCoordinatorRequest {
            key: oversized,
            key_type: 0,
        };
        let mut buf = BytesMut::new();
        let result = request.encode_v1(&mut buf);
        assert!(result.is_err(), "expected Err for oversized string");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("exceeds"),
            "error should mention size limit: {msg}"
        );
    }

    #[test]
    fn test_encode_oversized_topic_name_returns_error_not_panic() {
        // KafkaString uses an i16 length prefix, so > 32767 bytes triggers error.
        // We can't allocate i32::MAX bytes, so we validate the fallible path
        // via the smaller KafkaString limit instead.
        let oversized_topic = "x".repeat(i16::MAX as usize + 1);
        let request = ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 30000,
            topic_data: vec![ProduceTopicData {
                name: oversized_topic,
                topic_id: None,
                partition_data: vec![],
            }],
        };
        let mut buf = BytesMut::new();
        let result = request.encode_v3(&mut buf);
        assert!(result.is_err(), "expected Err for oversized topic name");
    }

    #[test]
    fn test_encode_versioned_oversized_returns_error() {
        // End-to-end: VersionedEncode must propagate encoding errors.
        let oversized = "x".repeat(i16::MAX as usize + 1);
        let request = FindCoordinatorRequest {
            key: oversized,
            key_type: 0,
        };
        let mut buf = BytesMut::new();
        let result = request.encode_versioned(1, &mut buf);
        assert!(
            result.is_err(),
            "VersionedEncode must propagate encoding errors"
        );
    }

    // ---- Story 1.6: Metadata ----

    #[test]
    fn test_metadata_request_v1_encodes_null_topics() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_versioned(1, &mut buf).unwrap();
        let mut r = buf.freeze();
        // v1: null array = -1 (all topics)
        assert_eq!(i32::decode(&mut r).unwrap(), -1);
    }

    #[test]
    fn test_metadata_request_v9_flexible_encoding() {
        let request = MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                topic_id: None,
                name: Some("my-topic".to_string()),
            }]),
            allow_auto_topic_creation: true,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(9, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    // ---- Version floor enforcement (rstest parameterized) ----
    //
    // Pattern: use #[rstest] with #[case] to test that encode_versioned rejects
    // every version below MIN for each API. Each case is a (request_name, version)
    // pair. Add new cases when raising MIN or adding APIs.

    #[rstest]
    // Metadata MIN=1
    #[case::metadata_v0(0)]
    fn test_metadata_encode_below_min(#[case] version: i16) {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ===================================================================
    // Story 1.6: Metadata v9–v13 Wire-Format Tests
    // ===================================================================

    #[test]
    fn test_metadata_request_encode_v9_flexible() {
        let request = MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                topic_id: None,
                name: Some("test-topic".to_string()),
            }]),
            allow_auto_topic_creation: false,
        };
        let mut buf_v8 = BytesMut::new();
        request.encode_versioned(8, &mut buf_v8).unwrap();
        let mut buf_v9 = BytesMut::new();
        request.encode_versioned(9, &mut buf_v9).unwrap();
        assert_ne!(
            buf_v8.as_ref(),
            buf_v9.as_ref(),
            "v9 flexible should differ from v8"
        );
    }

    #[rstest]
    #[case::v10(10)]
    #[case::v11(11)]
    fn test_metadata_request_encode_v10_v11(#[case] version: i16) {
        let request = MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                topic_id: Some([0xAB; 16]),
                name: Some("t".to_string()),
            }]),
            allow_auto_topic_creation: true,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[rstest]
    #[case::v12(12)]
    #[case::v13(13)]
    fn test_metadata_request_encode_v12_v13_topic_id(#[case] version: i16) {
        let request = MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                topic_id: Some([0xAB; 16]),
                name: Some("t".to_string()),
            }]),
            allow_auto_topic_creation: false,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        // v12-v13 both use encode_v12.
        let mut buf2 = BytesMut::new();
        request.encode_v12(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_metadata_response_decode_v9_flexible() {
        let buf = build_metadata_response_v9_bytes();
        let resp = MetadataResponse::decode_versioned(9, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].host, "broker-1");
        assert_eq!(resp.brokers[0].port, 9092);
        assert_eq!(resp.cluster_id.as_deref(), Some("cluster-abc"));
        assert_eq!(resp.controller_id, 1);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name.as_deref(), Some("test-topic"));
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].leader_id, 1);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
    }

    #[test]
    fn test_metadata_response_decode_v10_topic_id() {
        // v10 adds topic_id (UUID) to each topic.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        // brokers: empty
        varint::encode_unsigned_varint(1, &mut buf);
        // cluster_id null
        varint::encode_unsigned_varint(0, &mut buf);
        buf.put_i32(-1); // controller_id
        // topics: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i16(0); // error_code
        // topic name
        let topic = b"t1";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        // topic_id (UUID) — new in v10
        let uuid = [0xAB_u8; 16];
        buf.put_slice(&uuid);
        buf.put_u8(0); // is_internal
        // partitions: empty
        varint::encode_unsigned_varint(1, &mut buf);
        buf.put_i32(0); // topic_authorized_operations
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        buf.put_i32(0); // cluster_authorized_operations
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = MetadataResponse::decode_versioned(10, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name.as_deref(), Some("t1"));
        assert_eq!(resp.topics[0].topic_id, Some([0xAB; 16]));
    }

    #[test]
    fn test_metadata_response_decode_v11_no_cluster_auth_ops() {
        // v11+ removes cluster_authorized_operations.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(1, &mut buf); // empty brokers
        varint::encode_unsigned_varint(0, &mut buf); // null cluster_id
        buf.put_i32(-1); // controller_id
        // topics: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i16(0); // error_code
        let topic = b"t";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        buf.put_slice(&[0u8; 16]); // topic_id (zero UUID)
        buf.put_u8(0); // is_internal
        varint::encode_unsigned_varint(1, &mut buf); // empty partitions
        buf.put_i32(0); // topic_authorized_operations
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        // Note: no cluster_authorized_operations in v11+
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = MetadataResponse::decode_versioned(11, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name.as_deref(), Some("t"));
    }

    #[test]
    fn test_metadata_response_decode_v13_top_level_error_code() {
        // v13 adds top-level error_code.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(1, &mut buf); // empty brokers
        varint::encode_unsigned_varint(0, &mut buf); // null cluster_id
        buf.put_i32(-1); // controller_id
        varint::encode_unsigned_varint(1, &mut buf); // empty topics
        buf.put_i16(0); // top-level error_code (new in v13)
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields

        let resp = MetadataResponse::decode_versioned(13, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_metadata_response_decode_v12_uses_v11_decoder() {
        // v12 shares decoder with v11, verify dispatch.
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        varint::encode_unsigned_varint(1, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        buf.put_i32(-1);
        varint::encode_unsigned_varint(1, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);

        let resp = MetadataResponse::decode_versioned(12, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.topics.is_empty());
    }

    // ===================================================================
    // Story 18.4: ApiVersions v5 Wire-Format Test
    // ===================================================================

    #[test]
    fn test_api_versions_request_v5_fields() {
        use crate::protocol::api::ApiVersionsRequest;
        use crate::protocol::primitives::KafkaString;
        let req = ApiVersionsRequest {
            client_software_name: Some(KafkaString::new("krafka")),
            client_software_version: Some(KafkaString::new("0.4.0")),
            cluster_id: Some("my-cluster".to_string()),
            node_id: 42,
        };
        let mut buf = BytesMut::new();
        req.encode_v5(&mut buf).unwrap();
        assert!(!buf.is_empty());
        // v5 should be longer than v3 due to ClusterId + NodeId.
        let mut buf_v3 = BytesMut::new();
        req.encode_v3(&mut buf_v3).unwrap();
        assert!(
            buf.len() > buf_v3.len(),
            "v5 should be longer than v3 (cluster_id + node_id)"
        );
    }
}
