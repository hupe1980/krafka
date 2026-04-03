//! Kafka protocol message types.
//!
//! This module defines the request and response types for all Kafka APIs.
//! Each request/response type implements [`VersionedEncode`] and/or
//! [`VersionedDecode`] for version-dispatched encoding and decoding.

use bytes::{Buf, BufMut, Bytes};

use super::api::ApiKey;
use super::array_len_i32;
use super::primitives::{Decode, Encode, KafkaArray, KafkaBytes, KafkaString, TryEncode};
use super::{check_decode_array_len, check_decode_nullable_array_len};
use crate::error::{ErrorCode, KrafkaError, Result};

/// Trait for encoding a request/response at a specific protocol version.
///
/// Implementors dispatch to the appropriate `encode_vN` method based on
/// the version number, returning an error for unsupported versions.
/// All encoding is fallible — oversized inputs return an error instead of
/// panicking.
pub trait VersionedEncode {
    /// Encode this message for the given protocol version.
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()>;
}

/// Trait for decoding a response/request from a specific protocol version.
///
/// Implementors dispatch to the appropriate `decode_vN` method based on
/// the version number, returning an error for unsupported versions.
pub trait VersionedDecode: Sized {
    /// Decode this message from the given protocol version.
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self>;
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

    /// Extract topic names as `KafkaString`s for wire encoding.
    ///
    /// Returns an error if any entry has `name: None` — topic IDs are only
    /// supported from v10+, and we cap at v8.
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

    /// Encode for version 0 (topics is non-nullable; `None` → empty array = "all topics").
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.topics {
            None => KafkaArray::<KafkaString>::new(vec![]).try_encode(buf)?,
            Some(topics) => KafkaArray::new(Self::topic_names(topics)?).try_encode(buf)?,
        }
        Ok(())
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
}

/// Metadata response.
#[derive(Debug, Clone, Default)]
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
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let brokers = decode_array::<MetadataBrokerV0, _>(buf)?;
        let topics = decode_array::<MetadataTopicResponseV0, _>(buf)?;
        Ok(Self {
            throttle_time_ms: 0,
            brokers,
            cluster_id: None,
            controller_id: -1,
            topics,
        })
    }

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

/// Decode a `KafkaArray` of newtype wrappers and unwrap each element's inner value.
fn decode_array<W: Decode + Into<T>, T>(buf: &mut impl Buf) -> Result<Vec<T>> {
    Ok(KafkaArray::<W>::decode(buf)?
        .0
        .unwrap_or_default()
        .into_iter()
        .map(Into::into)
        .collect())
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
        let host = KafkaString::decode(buf)?.0.unwrap_or_default();
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
        let host = KafkaString::decode(buf)?.0.unwrap_or_default();
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

/// v0 partition decoder: no offline_replicas, no leader_epoch.
struct MetadataPartitionResponseV0(MetadataPartitionResponse);

impl Decode for MetadataPartitionResponseV0 {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let replica_nodes = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
        let isr_nodes = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
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
        let replica_nodes = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
        let isr_nodes = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
        let offline_replicas = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
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
        let replica_nodes = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
        let isr_nodes = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
        let offline_replicas = KafkaArray::<i32>::decode(buf)?.0.unwrap_or_default();
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

// Produce request/response

/// Produce request.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    /// Transactional ID (v3+).
    pub transactional_id: Option<String>,
    /// Required acks (-1, 0, 1).
    pub acks: i16,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Topic data.
    pub topic_data: Vec<ProduceTopicData>,
}

/// Topic data in produce request.
#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    /// Topic name.
    pub name: String,
    /// Partition data.
    pub partition_data: Vec<ProducePartitionData>,
}

/// Partition data in produce request.
#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    /// Partition index.
    pub index: i32,
    /// Record batch data.
    pub records: Bytes,
}

impl ProduceRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Produce
    }

    /// Encode for version 0-2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        self.acks.encode(buf);
        self.timeout_ms.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topic_data.len())?);
        for topic in &self.topic_data {
            KafkaString::new(&topic.name).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partition_data.len())?);
            for partition in &topic.partition_data {
                partition.index.encode(buf);
                KafkaBytes::new(partition.records.clone()).try_encode(buf)?;
            }
        }
        Ok(())
    }

    /// Encode for version 3+.
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.transactional_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        self.encode_v0(buf)?;
        Ok(())
    }
}

/// Produce response.
#[derive(Debug, Clone, Default)]
pub struct ProduceResponse {
    /// Topic responses.
    pub responses: Vec<ProduceTopicResponse>,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

/// Topic response in produce response.
#[derive(Debug, Clone)]
pub struct ProduceTopicResponse {
    /// Topic name.
    pub name: String,
    /// Partition responses.
    pub partition_responses: Vec<ProducePartitionResponse>,
}

/// Partition response in produce response.
#[derive(Debug, Clone)]
pub struct ProducePartitionResponse {
    /// Partition index.
    pub index: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Base offset.
    pub base_offset: i64,
    /// Log append time.
    pub log_append_time_ms: i64,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
}

impl ProduceResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let mut responses = Vec::new();
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partition_responses = Vec::new();

            for _ in 0..partition_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms: -1,
                    log_start_offset: -1,
                });
            }

            responses.push(ProduceTopicResponse {
                name,
                partition_responses,
            });
        }

        Ok(Self {
            responses,
            throttle_time_ms: 0,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let mut response = Self::decode_v0(buf)?;
        response.throttle_time_ms = i32::decode(buf)?;
        Ok(response)
    }

    /// Decode from version 2+.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let mut responses = Vec::new();
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partition_responses = Vec::new();

            for _ in 0..partition_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;
                let log_append_time_ms = i64::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset: -1,
                });
            }

            responses.push(ProduceTopicResponse {
                name,
                partition_responses,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}

// Fetch request/response

/// Fetch request.
///
/// This struct is `#[non_exhaustive]`; use [`Default::default()`] and then
/// set the fields you need.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FetchRequest {
    /// Replica ID (-1 for consumers).
    pub replica_id: i32,
    /// Max wait time in milliseconds.
    pub max_wait_ms: i32,
    /// Min bytes to return.
    pub min_bytes: i32,
    /// Max bytes to return (v3+).
    pub max_bytes: i32,
    /// Isolation level (v4+).
    pub isolation_level: i8,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Session epoch (v7+).
    pub session_epoch: i32,
    /// Topic data.
    pub topics: Vec<FetchTopicRequest>,
    /// Forgotten topics/partitions to remove from the session (v7+).
    pub forgotten_topics: Vec<FetchForgottenTopic>,
    /// Consumer rack ID for closest-replica routing (v11+, KIP-392).
    pub rack_id: String,
}

impl Default for FetchRequest {
    fn default() -> Self {
        Self {
            replica_id: -1,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: Vec::new(),
            forgotten_topics: Vec::new(),
            rack_id: String::new(),
        }
    }
}

/// Topic-partitions to forget from a fetch session (v7+).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FetchForgottenTopic {
    /// Topic name.
    pub topic: String,
    /// Partition IDs to forget.
    pub partitions: Vec<i32>,
}

/// Topic in fetch request.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchTopicRequest {
    /// Topic name.
    pub topic: String,
    /// Partition data.
    pub partitions: Vec<FetchPartitionRequest>,
}

/// Partition in fetch request.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FetchPartitionRequest {
    /// Partition ID.
    pub partition: i32,
    /// Current leader epoch (v9+).
    pub current_leader_epoch: i32,
    /// Fetch offset.
    pub fetch_offset: i64,
    /// Last fetched epoch (v12+).
    pub last_fetched_epoch: i32,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Partition max bytes.
    pub partition_max_bytes: i32,
}

impl Default for FetchPartitionRequest {
    fn default() -> Self {
        Self {
            partition: 0,
            current_leader_epoch: -1,
            fetch_offset: 0,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 0,
        }
    }
}

impl FetchRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Fetch
    }

    /// Encode for version 0-2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.fetch_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 3.
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.fetch_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 4.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.fetch_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 7 (fetch sessions: session_id, session_epoch, forgotten_topics).
    pub fn encode_v7(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner_v7(buf, false)
    }

    /// Encode for version 9 (v7 + `current_leader_epoch` per partition, KIP-320).
    pub fn encode_v9(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner_v7(buf, true)
    }

    /// Shared encoder for v7–v10. When `include_leader_epoch` is true, emits
    /// `current_leader_epoch` between partition id and fetch_offset (v9+, KIP-320).
    fn encode_inner_v7(&self, buf: &mut impl BufMut, include_leader_epoch: bool) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);
        self.session_id.encode(buf);
        self.session_epoch.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                if include_leader_epoch {
                    // current_leader_epoch introduced in v9 (KIP-320)
                    partition.current_leader_epoch.encode(buf);
                }
                partition.fetch_offset.encode(buf);
                // log_start_offset introduced in v5
                partition.log_start_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
            }
        }

        // Forgotten topics array (v7+)
        buf.put_i32(array_len_i32(self.forgotten_topics.len())?);
        for forgotten in &self.forgotten_topics {
            KafkaString::new(&forgotten.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(forgotten.partitions.len())?);
            for &partition in &forgotten.partitions {
                partition.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 11 (v9 + rack_id for closest-replica routing, KIP-392).
    pub fn encode_v11(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_v9(buf)?;
        KafkaString::new(&self.rack_id).try_encode(buf)?;
        Ok(())
    }
}

/// Fetch response.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code (v7+).
    pub error_code: ErrorCode,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Topic responses.
    pub responses: Vec<FetchTopicResponse>,
}

/// Topic in fetch response.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchTopicResponse {
    /// Topic name.
    pub topic: String,
    /// Partition responses.
    pub partitions: Vec<FetchPartitionResponse>,
}

/// Partition in fetch response.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FetchPartitionResponse {
    /// Partition ID.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// High watermark.
    pub high_watermark: i64,
    /// Last stable offset (v4+).
    pub last_stable_offset: i64,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Aborted transactions (v4+).
    pub aborted_transactions: Vec<AbortedTransaction>,
    /// Preferred read replica ID for closest-replica routing (v11+, KIP-392).
    ///
    /// When >= 0, the consumer should preferentially fetch from this replica
    /// (reduces cross-rack traffic). When -1, no preference; use the leader.
    /// v7-v10 responses always set this to -1.
    pub preferred_read_replica: i32,
    /// Record batches.
    pub records: Option<Bytes>,
}

impl Default for FetchPartitionResponse {
    fn default() -> Self {
        Self {
            partition: 0,
            error_code: ErrorCode::None,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            aborted_transactions: Vec::new(),
            preferred_read_replica: -1,
            records: None,
        }
    }
}

/// Aborted transaction info.
#[derive(Debug, Clone)]
pub struct AbortedTransaction {
    /// Producer ID.
    pub producer_id: i64,
    /// First offset.
    pub first_offset: i64,
}

impl FetchResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let mut responses = Vec::new();
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::new();

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let records = KafkaBytes::decode(buf)?.0;

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code,
                    high_watermark,
                    last_stable_offset: -1,
                    log_start_offset: -1,
                    aborted_transactions: Vec::new(),
                    preferred_read_replica: -1,
                    records,
                });
            }

            responses.push(FetchTopicResponse { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id: 0,
            responses,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let mut response = Self::decode_inner_v0(buf)?;
        response.throttle_time_ms = throttle_time_ms;
        Ok(response)
    }

    /// Decode from version 4 (includes last_stable_offset and aborted_transactions).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let mut responses = Vec::new();
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::new();

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let last_stable_offset = i64::decode(buf)?;
                let aborted_tx_count = check_decode_nullable_array_len(i32::decode(buf)?)?;
                let mut aborted_transactions = Vec::new();
                for _ in 0..aborted_tx_count {
                    aborted_transactions.push(AbortedTransaction {
                        producer_id: i64::decode(buf)?,
                        first_offset: i64::decode(buf)?,
                    });
                }
                let records = KafkaBytes::decode(buf)?.0;

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset: -1,
                    aborted_transactions,
                    preferred_read_replica: -1,
                    records,
                });
            }

            responses.push(FetchTopicResponse { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            error_code: ErrorCode::None,
            session_id: 0,
            responses,
        })
    }

    /// Decode from version 7 (includes error_code, session_id, log_start_offset).
    pub fn decode_v7(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner_v7(buf, false)
    }

    /// Decode from version 11 (v7 + preferred_read_replica per partition, KIP-392).
    pub fn decode_v11(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner_v7(buf, true)
    }

    /// Shared decoder for v7–v11. When `has_preferred_read_replica` is true,
    /// reads `preferred_read_replica` between aborted_transactions and records
    /// (v11+, KIP-392); otherwise defaults to `-1`.
    fn decode_inner_v7(buf: &mut impl Buf, has_preferred_read_replica: bool) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let session_id = i32::decode(buf)?;
        let mut responses = Vec::new();
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::new();

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let partition_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let last_stable_offset = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                let aborted_tx_count = check_decode_nullable_array_len(i32::decode(buf)?)?;
                let mut aborted_transactions = Vec::new();
                for _ in 0..aborted_tx_count {
                    aborted_transactions.push(AbortedTransaction {
                        producer_id: i64::decode(buf)?,
                        first_offset: i64::decode(buf)?,
                    });
                }
                let preferred_read_replica = if has_preferred_read_replica {
                    i32::decode(buf)?
                } else {
                    -1
                };
                let records = KafkaBytes::decode(buf)?.0;

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code: partition_error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset,
                    aborted_transactions,
                    preferred_read_replica,
                    records,
                });
            }

            responses.push(FetchTopicResponse { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
        })
    }

    fn decode_inner_v0(buf: &mut impl Buf) -> Result<Self> {
        let mut responses = Vec::new();
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::new();

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let records = KafkaBytes::decode(buf)?.0;

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code,
                    high_watermark,
                    last_stable_offset: -1,
                    log_start_offset: -1,
                    aborted_transactions: Vec::new(),
                    preferred_read_replica: -1,
                    records,
                });
            }

            responses.push(FetchTopicResponse { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            session_id: 0,
            responses,
        })
    }
}

// FindCoordinator request/response

/// Find coordinator request.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    /// Key (group ID or transactional ID).
    pub key: String,
    /// Key type (0 = group, 1 = txn).
    pub key_type: i8,
}

impl FindCoordinatorRequest {
    /// Create a request for a consumer group.
    pub fn for_group(group_id: &str) -> Self {
        Self {
            key: group_id.to_string(),
            key_type: 0,
        }
    }

    /// Create a request for a transaction.
    pub fn for_transaction(transactional_id: &str) -> Self {
        Self {
            key: transactional_id.to_string(),
            key_type: 1,
        }
    }

    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::FindCoordinator
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.key).try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 1+.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.key).try_encode(buf)?;
        self.key_type.encode(buf);
        Ok(())
    }
}

/// Find coordinator response.
#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Coordinator node ID.
    pub node_id: i32,
    /// Coordinator host.
    pub host: String,
    /// Coordinator port.
    pub port: i32,
}

impl FindCoordinatorResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?.0.unwrap_or_default();
        let port = i32::decode(buf)?;

        Ok(Self {
            throttle_time_ms: 0,
            error_code,
            error_message: None,
            node_id,
            host,
            port,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?.0.unwrap_or_default();
        let port = i32::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }
}

// ============================================================================
// JoinGroup request/response
// ============================================================================

/// JoinGroup request protocol.
#[derive(Debug, Clone)]
pub struct JoinGroupRequestProtocol {
    /// Protocol name.
    pub name: String,
    /// Protocol metadata.
    pub metadata: Bytes,
}

/// JoinGroup request.
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Session timeout.
    pub session_timeout_ms: i32,
    /// Rebalance timeout (v1+).
    pub rebalance_timeout_ms: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v5+).
    pub group_instance_id: Option<String>,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Supported protocols.
    pub protocols: Vec<JoinGroupRequestProtocol>,
}

impl JoinGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::JoinGroup
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.session_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        KafkaString::new(&self.protocol_type).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.protocols.len())?);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 1+.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        KafkaString::new(&self.protocol_type).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.protocols.len())?);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 5+ (adds group_instance_id for KIP-345 static membership).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        KafkaString::new(&self.protocol_type).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.protocols.len())?);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode(buf)?;
        }
        Ok(())
    }
}

/// Member in JoinGroup response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponseMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Member metadata.
    pub metadata: Bytes,
}

/// JoinGroup response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Generation ID.
    pub generation_id: i32,
    /// Protocol type.
    pub protocol_type: Option<String>,
    /// Selected protocol name.
    pub protocol_name: Option<String>,
    /// Leader member ID.
    pub leader: String,
    /// This member's ID.
    pub member_id: String,
    /// Members (only for leader).
    pub members: Vec<JoinGroupResponseMember>,
}

impl JoinGroupResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode(buf)?.0;
        let leader = KafkaString::decode(buf)?.0.unwrap_or_default();
        let member_id = KafkaString::decode(buf)?.0.unwrap_or_default();

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let metadata = KafkaBytes::decode(buf)?.0.unwrap_or_default();
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id: None,
                metadata,
            });
        }

        Ok(Self {
            throttle_time_ms: 0,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }

    /// Decode from version 2+.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode(buf)?.0;
        let leader = KafkaString::decode(buf)?.0.unwrap_or_default();
        let member_id = KafkaString::decode(buf)?.0.unwrap_or_default();

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let metadata = KafkaBytes::decode(buf)?.0.unwrap_or_default();
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id: None,
                metadata,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }

    /// Decode from version 5+ (adds group_instance_id per member).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode(buf)?.0;
        let leader = KafkaString::decode(buf)?.0.unwrap_or_default();
        let member_id = KafkaString::decode(buf)?.0.unwrap_or_default();

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let group_instance_id = KafkaString::decode(buf)?.0;
            let metadata = KafkaBytes::decode(buf)?.0.unwrap_or_default();
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            member_id,
            members,
        })
    }

    /// Check if this member is the leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.member_id == self.leader
    }
}

// ============================================================================
// SyncGroup request/response
// ============================================================================

/// Assignment for a member in SyncGroup.
#[derive(Debug, Clone)]
pub struct SyncGroupRequestAssignment {
    /// Member ID.
    pub member_id: String,
    /// Assignment data.
    pub assignment: Bytes,
}

/// SyncGroup request.
#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v3+).
    pub group_instance_id: Option<String>,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignments (only from leader).
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

impl SyncGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::SyncGroup
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.assignments.len())?);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 3+ (KIP-345: includes group_instance_id).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }

        buf.put_i32(array_len_i32(self.assignments.len())?);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode(buf)?;
        }
        Ok(())
    }
}

/// SyncGroup response.
#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignment for this member.
    pub assignment: Bytes,
}

impl SyncGroupResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let assignment = KafkaBytes::decode(buf)?.0.unwrap_or_default();

        Ok(Self {
            throttle_time_ms: 0,
            error_code,
            protocol_type: None,
            protocol_name: None,
            assignment,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let assignment = KafkaBytes::decode(buf)?.0.unwrap_or_default();

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type: None,
            protocol_name: None,
            assignment,
        })
    }
}

// ============================================================================
// Heartbeat request/response
// ============================================================================

/// Heartbeat request.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v3+).
    pub group_instance_id: Option<String>,
}

impl HeartbeatRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Heartbeat
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3+ (adds group_instance_id for KIP-345 static membership).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        Ok(())
    }
}

/// Heartbeat response.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl HeartbeatResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms: 0,
            error_code,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

// ============================================================================
// LeaveGroup request/response
// ============================================================================

/// Member leaving in LeaveGroup (v3+).
#[derive(Debug, Clone)]
pub struct LeaveGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
}

/// LeaveGroup request.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Member ID (v0-v2).
    pub member_id: String,
    /// Members (v3+).
    pub members: Vec<LeaveGroupMember>,
}

impl LeaveGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::LeaveGroup
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        KafkaString::new(&self.member_id).try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3+.
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        buf.put_i32(array_len_i32(self.members.len())?);
        for member in &self.members {
            KafkaString::new(&member.member_id).try_encode(buf)?;
            match &member.group_instance_id {
                Some(id) => KafkaString::new(id).try_encode(buf)?,
                None => KafkaString::null().try_encode(buf)?,
            }
        }
        Ok(())
    }
}

/// Member result in LeaveGroup response (v3+).
#[derive(Debug, Clone)]
pub struct LeaveGroupResponseMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Per-member error code.
    pub error_code: ErrorCode,
}

/// LeaveGroup response.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Per-member results (v3+ only, empty for earlier versions).
    pub members: Vec<LeaveGroupResponseMember>,
}

impl LeaveGroupResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms: 0,
            error_code,
            members: vec![],
        })
    }

    /// Decode from version 1-2.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
            members: vec![],
        })
    }

    /// Decode from version 3+ (KIP-345 batch leave with per-member results).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::new();
        for _ in 0..member_count {
            let member_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let group_instance_id = KafkaString::decode(buf)?.0;
            let member_error_code = ErrorCode::from_i16(i16::decode(buf)?);
            members.push(LeaveGroupResponseMember {
                member_id,
                group_instance_id,
                error_code: member_error_code,
            });
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }
}

// ============================================================================
// OffsetCommit request/response
// ============================================================================

/// Partition in OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Committed offset.
    pub committed_offset: i64,
    /// Committed leader epoch (v6+).
    pub committed_leader_epoch: i32,
    /// Commit timestamp (v1; deprecated in v2+).
    pub commit_timestamp: i64,
    /// Metadata.
    pub committed_metadata: Option<String>,
}

/// Topic in OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

/// OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID (v1+).
    pub generation_id: i32,
    /// Member ID (v1+).
    pub member_id: String,
    /// Group instance ID (v7+).
    pub group_instance_id: Option<String>,
    /// Retention time (v2-v4; deprecated).
    pub retention_time_ms: i64,
    /// Topics.
    pub topics: Vec<OffsetCommitRequestTopic>,
}

impl OffsetCommitRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetCommit
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }

    /// Encode for version 1.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                partition.commit_timestamp.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }

    /// Encode for version 2+.
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        self.retention_time_ms.encode(buf);

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }
}

/// Partition in OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Topic in OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

/// OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetCommitResponseTopic>,
}

impl OffsetCommitResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(OffsetCommitResponsePartition {
                    partition_index,
                    error_code,
                });
            }

            topics.push(OffsetCommitResponseTopic { name, partitions });
        }

        Ok(Self {
            throttle_time_ms: 0,
            topics,
        })
    }

    /// Decode from version 3+.
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(OffsetCommitResponsePartition {
                    partition_index,
                    error_code,
                });
            }

            topics.push(OffsetCommitResponseTopic { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Check if all partitions succeeded.
    pub fn all_success(&self) -> bool {
        self.topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .all(|p| p.error_code.is_ok())
    }
}

// ============================================================================
// ListOffsets request/response
// ============================================================================

/// Partition in ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The current leader epoch (use -1 if not available).
    pub current_leader_epoch: i32,
    /// The target timestamp (-1 = latest, -2 = earliest).
    pub timestamp: i64,
}

/// Topic in ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<ListOffsetsRequestPartition>,
}

/// ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    /// Broker ID of the requester (-1 for a consumer).
    pub replica_id: i32,
    /// Isolation level (0 = read_uncommitted, 1 = read_committed).
    pub isolation_level: i8,
    /// Topics.
    pub topics: Vec<ListOffsetsRequestTopic>,
}

impl ListOffsetsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListOffsets
    }

    /// Encode for version 1 (single offset response per partition).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i64(partition.timestamp);
            }
        }
        Ok(())
    }

    /// Encode for version 2+ (includes `isolation_level`).
    ///
    /// Version 2 adds `isolation_level` after `replica_id`, which controls
    /// whether transactional (uncommitted) records are visible.
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i8(self.isolation_level);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i64(partition.timestamp);
            }
        }
        Ok(())
    }
}

/// Partition in ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// The result timestamp (-1 if not set).
    pub timestamp: i64,
    /// The result offset.
    pub offset: i64,
}

/// Topic in ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

/// ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    /// Topics.
    pub topics: Vec<ListOffsetsResponseTopic>,
}

impl ListOffsetsResponse {
    /// Decode version 1 response.
    ///
    /// Uses checked reads to avoid panics on truncated data and guards
    /// negative counts to prevent OOM allocations.
    ///
    /// The v1 response format has the topics array directly (no throttle_time_ms).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(crate::error::KrafkaError::protocol(
                "ListOffsetsResponse: truncated (no topic count)",
            ));
        }
        let topic_count = check_decode_array_len(buf.get_i32())?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            if buf.remaining() < 4 {
                return Err(crate::error::KrafkaError::protocol(
                    "ListOffsetsResponse: truncated (no partition count)",
                ));
            }
            let partition_count = check_decode_array_len(buf.get_i32())?;
            let mut partitions = Vec::with_capacity(partition_count);
            // Each partition needs 4 + 2 + 8 + 8 = 22 bytes
            for _ in 0..partition_count {
                if buf.remaining() < 22 {
                    return Err(crate::error::KrafkaError::protocol(
                        "ListOffsetsResponse: truncated partition data",
                    ));
                }
                let partition_index = buf.get_i32();
                let error_code = ErrorCode::from(buf.get_i16());
                let timestamp = buf.get_i64();
                let offset = buf.get_i64();
                partitions.push(ListOffsetsResponsePartition {
                    partition_index,
                    error_code,
                    timestamp,
                    offset,
                });
            }
            topics.push(ListOffsetsResponseTopic { name, partitions });
        }
        Ok(Self { topics })
    }

    /// Decode version 2+ response (includes `throttle_time_ms`).
    ///
    /// The v2+ response format starts with `throttle_time_ms` (INT32)
    /// followed by the same topics array as v1.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(crate::error::KrafkaError::protocol(
                "ListOffsetsResponse v2: truncated (no throttle_time_ms)",
            ));
        }
        let _throttle_time_ms = buf.get_i32();

        // Remainder is identical to v1
        Self::decode_v1(buf)
    }
}

// ============================================================================
// OffsetFetch request/response
// ============================================================================

/// Topic in OffsetFetch request.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partition indices.
    pub partition_indexes: Vec<i32>,
}

/// OffsetFetch request.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    /// Group ID.
    pub group_id: String,
    /// Topics (null = all topics in group).
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// Require stable offsets (v7+).
    pub require_stable: bool,
}

impl OffsetFetchRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetFetch
    }

    /// Encode for version 0-1.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;

        match &self.topics {
            Some(topics) => {
                buf.put_i32(array_len_i32(topics.len())?);
                for topic in topics {
                    KafkaString::new(&topic.name).try_encode(buf)?;
                    buf.put_i32(array_len_i32(topic.partition_indexes.len())?);
                    for partition in &topic.partition_indexes {
                        partition.encode(buf);
                    }
                }
            }
            None => {
                buf.put_i32(-1);
            }
        }
        Ok(())
    }
}

/// Partition in OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Committed offset.
    pub committed_offset: i64,
    /// Committed leader epoch (v5+).
    pub committed_leader_epoch: i32,
    /// Metadata.
    pub metadata: Option<String>,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Topic in OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

/// OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// Error code (v2+).
    pub error_code: ErrorCode,
}

impl OffsetFetchResponse {
    /// Decode from version 0-1.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let metadata = KafkaString::decode(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: -1,
                    metadata,
                    error_code,
                });
            }

            topics.push(OffsetFetchResponseTopic { name, partitions });
        }

        Ok(Self {
            throttle_time_ms: 0,
            topics,
            error_code: ErrorCode::None,
        })
    }

    /// Decode from version 2.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let metadata = KafkaString::decode(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: -1,
                    metadata,
                    error_code,
                });
            }

            topics.push(OffsetFetchResponseTopic { name, partitions });
        }

        let error_code = ErrorCode::from_i16(i16::decode(buf)?);

        Ok(Self {
            throttle_time_ms: 0,
            topics,
            error_code,
        })
    }

    /// Decode from version 3+.
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let metadata = KafkaString::decode(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: -1,
                    metadata,
                    error_code,
                });
            }

            topics.push(OffsetFetchResponseTopic { name, partitions });
        }

        let error_code = ErrorCode::from_i16(i16::decode(buf)?);

        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }

    /// Get the offset for a specific topic-partition.
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.topics.iter().find(|t| t.name == topic).and_then(|t| {
            t.partitions
                .iter()
                .find(|p| p.partition_index == partition)
                .map(|p| p.committed_offset)
        })
    }
}

// ============================================================================
// CreateTopics request/response
// ============================================================================

/// Topic configuration for CreateTopics.
#[derive(Debug, Clone)]
pub struct CreatableTopicConfig {
    /// Config name.
    pub name: String,
    /// Config value.
    pub value: Option<String>,
}

/// Topic to create.
#[derive(Debug, Clone)]
pub struct CreatableTopic {
    /// Topic name.
    pub name: String,
    /// Number of partitions (-1 = default).
    pub num_partitions: i32,
    /// Replication factor (-1 = default).
    pub replication_factor: i16,
    /// Manual replica assignments.
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// Topic configs.
    pub configs: Vec<CreatableTopicConfig>,
}

/// Replica assignment.
#[derive(Debug, Clone)]
pub struct CreatableReplicaAssignment {
    /// Partition index.
    pub partition_index: i32,
    /// Broker IDs.
    pub broker_ids: Vec<i32>,
}

/// CreateTopics request.
#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    /// Topics to create.
    pub topics: Vec<CreatableTopic>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Validate only (v1+).
    pub validate_only: bool,
}

impl CreateTopicsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::CreateTopics
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            topic.num_partitions.encode(buf);
            topic.replication_factor.encode(buf);

            buf.put_i32(array_len_i32(topic.assignments.len())?);
            for assignment in &topic.assignments {
                assignment.partition_index.encode(buf);
                buf.put_i32(array_len_i32(assignment.broker_ids.len())?);
                for broker in &assignment.broker_ids {
                    broker.encode(buf);
                }
            }

            buf.put_i32(array_len_i32(topic.configs.len())?);
            for config in &topic.configs {
                KafkaString::new(&config.name).try_encode(buf)?;
                match &config.value {
                    Some(v) => KafkaString::new(v).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        self.timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 1+.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            topic.num_partitions.encode(buf);
            topic.replication_factor.encode(buf);

            buf.put_i32(array_len_i32(topic.assignments.len())?);
            for assignment in &topic.assignments {
                assignment.partition_index.encode(buf);
                buf.put_i32(array_len_i32(assignment.broker_ids.len())?);
                for broker in &assignment.broker_ids {
                    broker.encode(buf);
                }
            }

            buf.put_i32(array_len_i32(topic.configs.len())?);
            for config in &topic.configs {
                KafkaString::new(&config.name).try_encode(buf)?;
                match &config.value {
                    Some(v) => KafkaString::new(v).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        self.timeout_ms.encode(buf);
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        Ok(())
    }
}

/// Result for a created topic.
#[derive(Debug, Clone)]
pub struct CreatableTopicResult {
    /// Topic name.
    pub name: String,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (v1+).
    pub error_message: Option<String>,
    /// Number of partitions (v5+).
    pub num_partitions: i32,
    /// Replication factor (v5+).
    pub replication_factor: i16,
}

/// CreateTopics response.
#[derive(Debug, Clone)]
pub struct CreateTopicsResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<CreatableTopicResult>,
}

impl CreateTopicsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);

            topics.push(CreatableTopicResult {
                name,
                error_code,
                error_message: None,
                num_partitions: -1,
                replication_factor: -1,
            });
        }

        Ok(Self {
            throttle_time_ms: 0,
            topics,
        })
    }

    /// Decode from version 1.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            topics.push(CreatableTopicResult {
                name,
                error_code,
                error_message,
                num_partitions: -1,
                replication_factor: -1,
            });
        }

        Ok(Self {
            throttle_time_ms: 0,
            topics,
        })
    }

    /// Decode from version 2+.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            topics.push(CreatableTopicResult {
                name,
                error_code,
                error_message,
                num_partitions: -1,
                replication_factor: -1,
            });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// ============================================================================
// DeleteTopics request/response
// ============================================================================

/// DeleteTopics request.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    /// Topic names.
    pub topic_names: Vec<String>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DeleteTopics
    }

    /// Encode for version 0+.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topic_names.len())?);
        for name in &self.topic_names {
            KafkaString::new(name).try_encode(buf)?;
        }
        self.timeout_ms.encode(buf);
        Ok(())
    }
}

/// Result for a deleted topic.
#[derive(Debug, Clone)]
pub struct DeletableTopicResult {
    /// Topic name.
    pub name: Option<String>,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (v5+).
    pub error_message: Option<String>,
}

/// DeleteTopics response.
#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Responses.
    pub responses: Vec<DeletableTopicResult>,
}

impl DeleteTopicsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let response_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(response_count);

        for _ in 0..response_count {
            let name = KafkaString::decode(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);

            responses.push(DeletableTopicResult {
                name,
                error_code,
                error_message: None,
            });
        }

        Ok(Self {
            throttle_time_ms: 0,
            responses,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let response_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(response_count);

        for _ in 0..response_count {
            let name = KafkaString::decode(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);

            responses.push(DeletableTopicResult {
                name,
                error_code,
                error_message: None,
            });
        }

        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }
}

// ============================================================================
// CreatePartitions API (Key 37)
// ============================================================================

/// CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsRequest {
    /// Topics to create partitions for.
    pub topics: Vec<CreatePartitionsTopic>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// If true, validate the request without actually creating partitions.
    pub validate_only: bool,
}

/// Topic in CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsTopic {
    /// Topic name.
    pub name: String,
    /// New total partition count.
    pub count: i32,
    /// Assignment of new partitions to brokers.
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
}

/// Partition assignment in CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsAssignment {
    /// Broker IDs to assign the partition replicas to.
    pub broker_ids: Vec<i32>,
}

impl CreatePartitionsRequest {
    /// Create a simple partition increase request.
    pub fn new(topic: impl Into<String>, count: i32, timeout: std::time::Duration) -> Self {
        Self {
            topics: vec![CreatePartitionsTopic {
                name: topic.into(),
                count,
                assignments: None,
            }],
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
            validate_only: false,
        }
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        // Array of topics
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            topic.count.encode(buf);

            // Assignments (nullable array)
            match &topic.assignments {
                None => (-1i32).encode(buf),
                Some(assignments) => {
                    array_len_i32(assignments.len())?.encode(buf);
                    for assignment in assignments {
                        array_len_i32(assignment.broker_ids.len())?.encode(buf);
                        for &broker_id in &assignment.broker_ids {
                            broker_id.encode(buf);
                        }
                    }
                }
            }
        }
        self.timeout_ms.encode(buf);
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        Ok(())
    }
}

/// CreatePartitions response.
#[derive(Debug, Clone)]
pub struct CreatePartitionsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results per topic.
    pub results: Vec<CreatePartitionsTopicResult>,
}

/// Result for a topic in CreatePartitions response.
#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicResult {
    /// Topic name.
    pub name: String,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
}

impl CreatePartitionsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            results.push(CreatePartitionsTopicResult {
                name,
                error_code,
                error_message,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

// ============================================================================
// DescribeConfigs API (Key 32)
// ============================================================================

/// Resource type for config operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigResourceType {
    /// Unknown resource type.
    Unknown = 0,
    /// Topic resource.
    Topic = 2,
    /// Broker resource.
    Broker = 4,
    /// Broker logger resource.
    BrokerLogger = 8,
}

impl ConfigResourceType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// DescribeConfigs request.
#[derive(Debug, Clone)]
pub struct DescribeConfigsRequest {
    /// Resources to describe.
    pub resources: Vec<DescribeConfigsResource>,
    /// Include synonyms in response.
    pub include_synonyms: bool,
    /// Include documentation in response.
    pub include_documentation: bool,
}

/// Resource in DescribeConfigs request.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResource {
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name (topic name or broker ID as string).
    pub resource_name: String,
    /// Config names to describe (null for all).
    pub config_names: Option<Vec<String>>,
}

impl DescribeConfigsRequest {
    /// Create a request to describe topic configs.
    pub fn for_topic(topic: impl Into<String>) -> Self {
        Self {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: topic.into(),
                config_names: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        }
    }

    /// Create a request to describe broker configs.
    pub fn for_broker(broker_id: i32) -> Self {
        Self {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Broker,
                resource_name: broker_id.to_string(),
                config_names: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        }
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.resources.len())?.encode(buf);
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode(buf)?;

            match &resource.config_names {
                None => (-1i32).encode(buf),
                Some(names) => {
                    array_len_i32(names.len())?.encode(buf);
                    for name in names {
                        KafkaString::new(name).try_encode(buf)?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// DescribeConfigs response.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results per resource.
    pub results: Vec<DescribeConfigsResult>,
}

/// Result for a resource in DescribeConfigs response.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Configuration entries.
    pub configs: Vec<DescribeConfigsEntry>,
}

/// Configuration entry in DescribeConfigs response.
#[derive(Debug, Clone)]
pub struct DescribeConfigsEntry {
    /// Config name.
    pub name: String,
    /// Config value.
    pub value: Option<String>,
    /// Whether the config is read-only.
    pub read_only: bool,
    /// Whether the config is the default value.
    pub is_default: bool,
    /// Whether the config is sensitive.
    pub is_sensitive: bool,
}

impl DescribeConfigsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = KafkaString::decode(buf)?.0.unwrap_or_default();

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(config_count);

            for _ in 0..config_count {
                let name = KafkaString::decode(buf)?.0.unwrap_or_default();
                let value = KafkaString::decode(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let is_default = i8::decode(buf)? != 0;
                let is_sensitive = i8::decode(buf)? != 0;

                configs.push(DescribeConfigsEntry {
                    name,
                    value,
                    read_only,
                    is_default,
                    is_sensitive,
                });
            }

            results.push(DescribeConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

// ============================================================================
// AlterConfigs API (Key 33)
// ============================================================================

/// AlterConfigs request.
#[derive(Debug, Clone)]
pub struct AlterConfigsRequest {
    /// Resources to alter.
    pub resources: Vec<AlterConfigsResource>,
    /// If true, validate without actually changing configs.
    pub validate_only: bool,
}

/// Resource in AlterConfigs request.
#[derive(Debug, Clone)]
pub struct AlterConfigsResource {
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Configurations to set.
    pub configs: Vec<AlterConfigsEntry>,
}

/// Configuration entry in AlterConfigs request.
#[derive(Debug, Clone)]
pub struct AlterConfigsEntry {
    /// Config name.
    pub name: String,
    /// Config value.
    pub value: Option<String>,
}

impl AlterConfigsRequest {
    /// Create a request to alter topic configs.
    pub fn for_topic(topic: impl Into<String>, configs: Vec<(String, String)>) -> Self {
        Self {
            resources: vec![AlterConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: topic.into(),
                configs: configs
                    .into_iter()
                    .map(|(name, value)| AlterConfigsEntry {
                        name,
                        value: Some(value),
                    })
                    .collect(),
            }],
            validate_only: false,
        }
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.resources.len())?.encode(buf);
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode(buf)?;

            array_len_i32(resource.configs.len())?.encode(buf);
            for config in &resource.configs {
                KafkaString::new(&config.name).try_encode(buf)?;
                match &config.value {
                    Some(v) => KafkaString::new(v).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        Ok(())
    }
}

/// AlterConfigs response.
#[derive(Debug, Clone)]
pub struct AlterConfigsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results per resource.
    pub results: Vec<AlterConfigsResult>,
}

/// Result for a resource in AlterConfigs response.
#[derive(Debug, Clone)]
pub struct AlterConfigsResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name.
    pub resource_name: String,
}

impl AlterConfigsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = KafkaString::decode(buf)?.0.unwrap_or_default();

            results.push(AlterConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

// ============================================================================
// InitProducerId (API Key 22) - Idempotent Producer Support
// ============================================================================

/// Request to initialize producer ID for idempotent/transactional production.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    /// Transactional ID (null for non-transactional producers).
    pub transactional_id: Option<String>,
    /// Transaction timeout in milliseconds (-1 for non-transactional).
    pub transaction_timeout_ms: i32,
    /// Producer ID to use (for recovery; -1 for new producer).
    pub producer_id: i64,
    /// Producer epoch to use (for recovery; -1 for new producer).
    pub producer_epoch: i16,
}

impl InitProducerIdRequest {
    /// Create a request for a non-transactional idempotent producer.
    #[inline]
    pub fn idempotent() -> Self {
        Self {
            transactional_id: None,
            transaction_timeout_ms: -1,
            producer_id: -1,
            producer_epoch: -1,
        }
    }

    /// Create a request for a transactional producer.
    #[inline]
    pub fn transactional(transactional_id: &str, timeout_ms: i32) -> Self {
        Self {
            transactional_id: Some(transactional_id.to_string()),
            transaction_timeout_ms: timeout_ms,
            producer_id: -1,
            producer_epoch: -1,
        }
    }

    /// Encode as version 0 (non-transactional idempotent only).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode(buf)?;
        self.transaction_timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode as version 2 (with producer_id and epoch for recovery).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode(buf)?;
        self.transaction_timeout_ms.encode(buf);
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        Ok(())
    }
}

/// Response from InitProducerId.
#[derive(Debug, Clone)]
pub struct InitProducerIdResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Producer ID assigned by the broker.
    pub producer_id: i64,
    /// Producer epoch assigned by the broker.
    pub producer_epoch: i16,
}

impl InitProducerIdResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

// ============================================================================
// SaslHandshake (API Key 17) - SASL Mechanism Negotiation
// ============================================================================

/// Request to negotiate SASL mechanism.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequest {
    /// SASL mechanism name (e.g., "PLAIN", "SCRAM-SHA-256").
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    /// Create a new SASL handshake request.
    #[inline]
    pub fn new(mechanism: impl Into<String>) -> Self {
        Self {
            mechanism: mechanism.into(),
        }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.mechanism.clone())).try_encode(buf)?;
        Ok(())
    }

    /// Encode as version 1.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        // Same as v0
        self.encode_v0(buf)?;
        Ok(())
    }
}

/// Response from SASL handshake.
#[derive(Debug, Clone)]
pub struct SaslHandshakeResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// List of mechanisms enabled on the broker.
    pub enabled_mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let count = check_decode_array_len(i32::decode(buf)?)?;
        let mut enabled_mechanisms = Vec::with_capacity(count);

        for _ in 0..count {
            if let Some(mech) = KafkaString::decode(buf)?.0 {
                enabled_mechanisms.push(mech);
            }
        }

        Ok(Self {
            error_code,
            enabled_mechanisms,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

// ============================================================================
// SaslAuthenticate (API Key 36) - SASL Authentication
// ============================================================================

/// Request to authenticate via SASL.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateRequest {
    /// SASL authentication bytes.
    pub auth_bytes: Vec<u8>,
}

impl SaslAuthenticateRequest {
    /// Create a new SASL authenticate request.
    #[inline]
    pub fn new(auth_bytes: Vec<u8>) -> Self {
        Self { auth_bytes }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaBytes(Some(bytes::Bytes::from(self.auth_bytes.clone()))).try_encode(buf)?;
        Ok(())
    }

    /// Encode as version 1.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        // Same as v0
        self.encode_v0(buf)?;
        Ok(())
    }
}

/// Response from SASL authentication.
#[derive(Debug, Clone)]
pub struct SaslAuthenticateResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (if any).
    pub error_message: Option<String>,
    /// Authentication response bytes.
    pub auth_bytes: Vec<u8>,
    /// Session lifetime in milliseconds (v1+).
    pub session_lifetime_ms: i64,
}

impl SaslAuthenticateResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;
        let auth_bytes = KafkaBytes::decode(buf)?
            .0
            .map(|b| b.to_vec())
            .unwrap_or_default();

        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms: 0,
        })
    }

    /// Decode from version 1.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;
        let auth_bytes = KafkaBytes::decode(buf)?
            .0
            .map(|b| b.to_vec())
            .unwrap_or_default();
        let session_lifetime_ms = i64::decode(buf)?;

        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }

    /// Check if authentication is complete.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.error_code.is_ok() && self.auth_bytes.is_empty()
    }
}

// ============================================================================
// ACL Management (API Keys 29, 30, 31)
// ============================================================================

/// ACL resource type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclResourceType {
    /// Unknown resource type.
    Unknown = 0,
    /// Any resource type (for filtering).
    #[default]
    Any = 1,
    /// Topic resource.
    Topic = 2,
    /// Group resource (consumer groups).
    Group = 3,
    /// Cluster resource.
    Cluster = 4,
    /// Transactional ID resource.
    TransactionalId = 5,
    /// Delegation token resource.
    DelegationToken = 6,
}

impl AclResourceType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Topic,
            3 => Self::Group,
            4 => Self::Cluster,
            5 => Self::TransactionalId,
            6 => Self::DelegationToken,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL pattern type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclPatternType {
    /// Unknown pattern type.
    Unknown = 0,
    /// Any pattern (for filtering).
    #[default]
    Any = 1,
    /// Exact match pattern.
    Literal = 2,
    /// Prefix match pattern.
    Prefixed = 3,
}

impl AclPatternType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Literal,
            3 => Self::Prefixed,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclOperation {
    /// Unknown operation.
    Unknown = 0,
    /// Any operation (for filtering).
    #[default]
    Any = 1,
    /// All operations.
    All = 2,
    /// Read operation.
    Read = 3,
    /// Write operation.
    Write = 4,
    /// Create operation.
    Create = 5,
    /// Delete operation.
    Delete = 6,
    /// Alter operation.
    Alter = 7,
    /// Describe operation.
    Describe = 8,
    /// Cluster action.
    ClusterAction = 9,
    /// Describe configs.
    DescribeConfigs = 10,
    /// Alter configs.
    AlterConfigs = 11,
    /// Idempotent write.
    IdempotentWrite = 12,
}

impl AclOperation {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::All,
            3 => Self::Read,
            4 => Self::Write,
            5 => Self::Create,
            6 => Self::Delete,
            7 => Self::Alter,
            8 => Self::Describe,
            9 => Self::ClusterAction,
            10 => Self::DescribeConfigs,
            11 => Self::AlterConfigs,
            12 => Self::IdempotentWrite,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL permission type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AclPermissionType {
    /// Unknown permission.
    Unknown = 0,
    /// Any permission (for filtering).
    #[default]
    Any = 1,
    /// Deny permission.
    Deny = 2,
    /// Allow permission.
    Allow = 3,
}

impl AclPermissionType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Any,
            2 => Self::Deny,
            3 => Self::Allow,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// ACL binding for creation/description.
#[derive(Debug, Clone)]
pub struct AclBinding {
    /// Resource type.
    pub resource_type: AclResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Resource pattern type.
    pub pattern_type: AclPatternType,
    /// Principal (e.g., "User:alice").
    pub principal: String,
    /// Host (e.g., "*" for any host).
    pub host: String,
    /// Operation.
    pub operation: AclOperation,
    /// Permission type.
    pub permission_type: AclPermissionType,
}

impl AclBinding {
    /// Create a new ACL binding.
    pub fn new(
        resource_type: AclResourceType,
        resource_name: impl Into<String>,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type: AclPatternType::Literal,
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }

    /// Set the pattern type.
    pub fn with_pattern_type(mut self, pattern_type: AclPatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }

    /// Create an allow read ACL for a topic.
    pub fn allow_read_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::new(
            AclResourceType::Topic,
            topic,
            principal,
            "*",
            AclOperation::Read,
            AclPermissionType::Allow,
        )
    }

    /// Create an allow write ACL for a topic.
    pub fn allow_write_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::new(
            AclResourceType::Topic,
            topic,
            principal,
            "*",
            AclOperation::Write,
            AclPermissionType::Allow,
        )
    }

    /// Create an allow all ACL for a topic.
    pub fn allow_all_topic(topic: impl Into<String>, principal: impl Into<String>) -> Self {
        Self::new(
            AclResourceType::Topic,
            topic,
            principal,
            "*",
            AclOperation::All,
            AclPermissionType::Allow,
        )
    }
}

/// DescribeAcls request (API Key 29).
#[derive(Debug, Clone)]
pub struct DescribeAclsRequest {
    /// Resource type filter.
    pub resource_type: AclResourceType,
    /// Resource name filter (null for any).
    pub resource_name: Option<String>,
    /// Pattern type filter.
    pub pattern_type: AclPatternType,
    /// Principal filter (null for any).
    pub principal: Option<String>,
    /// Host filter (null for any).
    pub host: Option<String>,
    /// Operation filter.
    pub operation: AclOperation,
    /// Permission type filter.
    pub permission_type: AclPermissionType,
}

impl DescribeAclsRequest {
    /// Create a request to describe all ACLs.
    pub fn all() -> Self {
        Self {
            resource_type: AclResourceType::Any,
            resource_name: None,
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }

    /// Create a request to describe ACLs for a topic.
    pub fn for_topic(topic: impl Into<String>) -> Self {
        Self {
            resource_type: AclResourceType::Topic,
            resource_name: Some(topic.into()),
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.resource_type.to_i8()).encode(buf);
        KafkaString(self.resource_name.clone()).try_encode(buf)?;
        KafkaString(self.principal.clone()).try_encode(buf)?;
        KafkaString(self.host.clone()).try_encode(buf)?;
        (self.operation.to_i8()).encode(buf);
        (self.permission_type.to_i8()).encode(buf);
        Ok(())
    }

    /// Encode as version 1 (with pattern type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.resource_type.to_i8()).encode(buf);
        KafkaString(self.resource_name.clone()).try_encode(buf)?;
        (self.pattern_type.to_i8()).encode(buf);
        KafkaString(self.principal.clone()).try_encode(buf)?;
        KafkaString(self.host.clone()).try_encode(buf)?;
        (self.operation.to_i8()).encode(buf);
        (self.permission_type.to_i8()).encode(buf);
        Ok(())
    }
}

/// DescribeAcls response.
#[derive(Debug, Clone)]
pub struct DescribeAclsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// List of ACL resources.
    pub resources: Vec<DescribeAclsResource>,
}

/// ACL resource in describe response.
#[derive(Debug, Clone)]
pub struct DescribeAclsResource {
    /// Resource type.
    pub resource_type: AclResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Pattern type.
    pub pattern_type: AclPatternType,
    /// ACLs for this resource.
    pub acls: Vec<AclDescription>,
}

/// Individual ACL description.
#[derive(Debug, Clone)]
pub struct AclDescription {
    /// Principal.
    pub principal: String,
    /// Host.
    pub host: String,
    /// Operation.
    pub operation: AclOperation,
    /// Permission type.
    pub permission_type: AclPermissionType,
}

impl DescribeAclsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;

        let resource_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut resources = Vec::with_capacity(resource_count);

        for _ in 0..resource_count {
            let resource_type = AclResourceType::from_i8(i8::decode(buf)?);
            let resource_name = KafkaString::decode(buf)?.0.unwrap_or_default();

            let acl_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut acls = Vec::with_capacity(acl_count);

            for _ in 0..acl_count {
                let principal = KafkaString::decode(buf)?.0.unwrap_or_default();
                let host = KafkaString::decode(buf)?.0.unwrap_or_default();
                let operation = AclOperation::from_i8(i8::decode(buf)?);
                let permission_type = AclPermissionType::from_i8(i8::decode(buf)?);

                acls.push(AclDescription {
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            resources.push(DescribeAclsResource {
                resource_type,
                resource_name,
                pattern_type: AclPatternType::Literal, // v0 doesn't have pattern type
                acls,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            resources,
        })
    }
}

/// CreateAcls request (API Key 30).
#[derive(Debug, Clone)]
pub struct CreateAclsRequest {
    /// ACL bindings to create.
    pub creations: Vec<AclBinding>,
}

impl CreateAclsRequest {
    /// Create a new request.
    pub fn new(creations: Vec<AclBinding>) -> Self {
        Self { creations }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.creations.len())?.encode(buf);
        for acl in &self.creations {
            (acl.resource_type.to_i8()).encode(buf);
            KafkaString(Some(acl.resource_name.clone())).try_encode(buf)?;
            KafkaString(Some(acl.principal.clone())).try_encode(buf)?;
            KafkaString(Some(acl.host.clone())).try_encode(buf)?;
            (acl.operation.to_i8()).encode(buf);
            (acl.permission_type.to_i8()).encode(buf);
        }
        Ok(())
    }

    /// Encode as version 1 (with pattern type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.creations.len())?.encode(buf);
        for acl in &self.creations {
            (acl.resource_type.to_i8()).encode(buf);
            KafkaString(Some(acl.resource_name.clone())).try_encode(buf)?;
            (acl.pattern_type.to_i8()).encode(buf);
            KafkaString(Some(acl.principal.clone())).try_encode(buf)?;
            KafkaString(Some(acl.host.clone())).try_encode(buf)?;
            (acl.operation.to_i8()).encode(buf);
            (acl.permission_type.to_i8()).encode(buf);
        }
        Ok(())
    }
}

/// CreateAcls response.
#[derive(Debug, Clone)]
pub struct CreateAclsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results for each ACL creation.
    pub results: Vec<CreateAclsResult>,
}

/// Result of a single ACL creation.
#[derive(Debug, Clone)]
pub struct CreateAclsResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
}

impl CreateAclsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            results.push(CreateAclsResult {
                error_code,
                error_message,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Check if all ACLs were created successfully.
    pub fn is_ok(&self) -> bool {
        self.results.iter().all(|r| r.error_code.is_ok())
    }
}

/// DeleteAcls request (API Key 31).
#[derive(Debug, Clone)]
pub struct DeleteAclsRequest {
    /// ACL filters for deletion.
    pub filters: Vec<AclBindingFilter>,
}

/// Filter for deleting ACLs.
#[derive(Debug, Clone)]
pub struct AclBindingFilter {
    /// Resource type filter.
    pub resource_type: AclResourceType,
    /// Resource name filter (null for any).
    pub resource_name: Option<String>,
    /// Pattern type filter.
    pub pattern_type: AclPatternType,
    /// Principal filter (null for any).
    pub principal: Option<String>,
    /// Host filter (null for any).
    pub host: Option<String>,
    /// Operation filter.
    pub operation: AclOperation,
    /// Permission type filter.
    pub permission_type: AclPermissionType,
}

impl AclBindingFilter {
    /// Create a filter that matches a specific ACL binding.
    pub fn matching(binding: &AclBinding) -> Self {
        Self {
            resource_type: binding.resource_type,
            resource_name: Some(binding.resource_name.clone()),
            pattern_type: binding.pattern_type,
            principal: Some(binding.principal.clone()),
            host: Some(binding.host.clone()),
            operation: binding.operation,
            permission_type: binding.permission_type,
        }
    }

    /// Create a filter for all ACLs on a topic.
    pub fn for_topic(topic: impl Into<String>) -> Self {
        Self {
            resource_type: AclResourceType::Topic,
            resource_name: Some(topic.into()),
            pattern_type: AclPatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission_type: AclPermissionType::Any,
        }
    }
}

impl DeleteAclsRequest {
    /// Create a new request.
    pub fn new(filters: Vec<AclBindingFilter>) -> Self {
        Self { filters }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.filters.len())?.encode(buf);
        for filter in &self.filters {
            (filter.resource_type.to_i8()).encode(buf);
            KafkaString(filter.resource_name.clone()).try_encode(buf)?;
            KafkaString(filter.principal.clone()).try_encode(buf)?;
            KafkaString(filter.host.clone()).try_encode(buf)?;
            (filter.operation.to_i8()).encode(buf);
            (filter.permission_type.to_i8()).encode(buf);
        }
        Ok(())
    }

    /// Encode as version 1 (with pattern type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.filters.len())?.encode(buf);
        for filter in &self.filters {
            (filter.resource_type.to_i8()).encode(buf);
            KafkaString(filter.resource_name.clone()).try_encode(buf)?;
            (filter.pattern_type.to_i8()).encode(buf);
            KafkaString(filter.principal.clone()).try_encode(buf)?;
            KafkaString(filter.host.clone()).try_encode(buf)?;
            (filter.operation.to_i8()).encode(buf);
            (filter.permission_type.to_i8()).encode(buf);
        }
        Ok(())
    }
}

/// DeleteAcls response.
#[derive(Debug, Clone)]
pub struct DeleteAclsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results for each filter.
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

/// Result of a single filter.
#[derive(Debug, Clone)]
pub struct DeleteAclsFilterResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Matching ACLs that were deleted.
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}

/// ACL that matched the deletion filter.
#[derive(Debug, Clone)]
pub struct DeleteAclsMatchingAcl {
    /// Error code for this specific ACL.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Resource type.
    pub resource_type: AclResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Pattern type.
    pub pattern_type: AclPatternType,
    /// Principal.
    pub principal: String,
    /// Host.
    pub host: String,
    /// Operation.
    pub operation: AclOperation,
    /// Permission type.
    pub permission_type: AclPermissionType,
}

impl DeleteAclsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let filter_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut filter_results = Vec::with_capacity(filter_count);

        for _ in 0..filter_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            let matching_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut matching_acls = Vec::with_capacity(matching_count);

            for _ in 0..matching_count {
                let acl_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let acl_error_message = KafkaString::decode(buf)?.0;
                let resource_type = AclResourceType::from_i8(i8::decode(buf)?);
                let resource_name = KafkaString::decode(buf)?.0.unwrap_or_default();
                let principal = KafkaString::decode(buf)?.0.unwrap_or_default();
                let host = KafkaString::decode(buf)?.0.unwrap_or_default();
                let operation = AclOperation::from_i8(i8::decode(buf)?);
                let permission_type = AclPermissionType::from_i8(i8::decode(buf)?);

                matching_acls.push(DeleteAclsMatchingAcl {
                    error_code: acl_error_code,
                    error_message: acl_error_message,
                    resource_type,
                    resource_name,
                    pattern_type: AclPatternType::Literal,
                    principal,
                    host,
                    operation,
                    permission_type,
                });
            }

            filter_results.push(DeleteAclsFilterResult {
                error_code,
                error_message,
                matching_acls,
            });
        }

        Ok(Self {
            throttle_time_ms,
            filter_results,
        })
    }
}

// ============================================================================
// Transaction Messages (API Keys 24-28)
// ============================================================================

/// Transaction result for partition operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionResult {
    /// Commit the transaction.
    Commit,
    /// Abort the transaction.
    Abort,
}

impl TransactionResult {
    /// Convert to boolean for wire format.
    #[inline]
    pub fn to_bool(self) -> bool {
        match self {
            TransactionResult::Commit => true,
            TransactionResult::Abort => false,
        }
    }

    /// Create from boolean.
    #[inline]
    pub fn from_bool(committed: bool) -> Self {
        if committed {
            TransactionResult::Commit
        } else {
            TransactionResult::Abort
        }
    }
}

/// AddPartitionsToTxn request (API Key 24).
///
/// Adds partitions to an ongoing transaction.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Topics to add.
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

/// Topic in AddPartitionsToTxn request.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnTopic {
    /// Topic name.
    pub name: String,
    /// Partition indices.
    pub partitions: Vec<i32>,
}

impl AddPartitionsToTxnRequest {
    /// Create a new request.
    pub fn new(transactional_id: impl Into<String>, producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            topics: Vec::new(),
        }
    }

    /// Add a topic-partition.
    pub fn add_partition(mut self, topic: impl Into<String>, partition: i32) -> Self {
        let topic_name = topic.into();
        if let Some(t) = self.topics.iter_mut().find(|t| t.name == topic_name) {
            if !t.partitions.contains(&partition) {
                t.partitions.push(partition);
            }
        } else {
            self.topics.push(AddPartitionsToTxnTopic {
                name: topic_name,
                partitions: vec![partition],
            });
        }
        self
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode(buf)?;
            array_len_i32(topic.partitions.len())?.encode(buf);
            for partition in &topic.partitions {
                partition.encode(buf);
            }
        }
        Ok(())
    }
}

/// AddPartitionsToTxn response.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results by topic.
    pub results: Vec<AddPartitionsToTxnTopicResult>,
}

/// Result for a topic in AddPartitionsToTxn.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition results.
    pub partitions: Vec<AddPartitionsToTxnPartitionResult>,
}

/// Result for a partition in AddPartitionsToTxn.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl AddPartitionsToTxnResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(AddPartitionsToTxnPartitionResult {
                    partition,
                    error_code,
                });
            }

            results.push(AddPartitionsToTxnTopicResult { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Check if all partitions were added successfully.
    pub fn is_ok(&self) -> bool {
        self.results
            .iter()
            .all(|t| t.partitions.iter().all(|p| p.error_code.is_ok()))
    }
}

/// AddOffsetsToTxn request (API Key 25).
///
/// Adds consumer group offsets to a transaction.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Consumer group ID.
    pub group_id: String,
}

impl AddOffsetsToTxnRequest {
    /// Create a new request.
    pub fn new(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            group_id: group_id.into(),
        }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        Ok(())
    }
}

/// AddOffsetsToTxn response.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl AddOffsetsToTxnResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Check if successful.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

/// EndTxn request (API Key 26).
///
/// Commits or aborts a transaction.
#[derive(Debug, Clone)]
pub struct EndTxnRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Whether to commit (true) or abort (false).
    pub committed: bool,
}

impl EndTxnRequest {
    /// Create a commit request.
    pub fn commit(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            committed: true,
        }
    }

    /// Create an abort request.
    pub fn abort(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            committed: false,
        }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.committed.encode(buf);
        Ok(())
    }
}

/// EndTxn response.
#[derive(Debug, Clone)]
pub struct EndTxnResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl EndTxnResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Check if successful.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

/// TxnOffsetCommit request (API Key 28).
///
/// Commits offsets as part of a transaction.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Consumer group ID.
    pub group_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Offsets to commit by topic.
    pub topics: Vec<TxnOffsetCommitTopic>,
}

/// Topic in TxnOffsetCommit request.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with offsets.
    pub partitions: Vec<TxnOffsetCommitPartition>,
}

/// Partition offset in TxnOffsetCommit request.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitPartition {
    /// Partition index.
    pub partition: i32,
    /// Offset to commit.
    pub committed_offset: i64,
    /// Leader epoch (optional, -1 if not used).
    pub committed_leader_epoch: i32,
    /// Metadata.
    pub metadata: Option<String>,
}

impl TxnOffsetCommitRequest {
    /// Create a new request.
    pub fn new(
        transactional_id: impl Into<String>,
        group_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            group_id: group_id.into(),
            producer_id,
            producer_epoch,
            topics: Vec::new(),
        }
    }

    /// Add an offset to commit.
    pub fn add_offset(
        mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
    ) -> Self {
        let topic_name = topic.into();
        let partition_data = TxnOffsetCommitPartition {
            partition,
            committed_offset: offset,
            committed_leader_epoch: -1,
            metadata,
        };

        if let Some(t) = self.topics.iter_mut().find(|t| t.name == topic_name) {
            if let Some(p) = t.partitions.iter_mut().find(|p| p.partition == partition) {
                *p = partition_data;
            } else {
                t.partitions.push(partition_data);
            }
        } else {
            self.topics.push(TxnOffsetCommitTopic {
                name: topic_name,
                partitions: vec![partition_data],
            });
        }
        self
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode(buf)?;
            array_len_i32(topic.partitions.len())?.encode(buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.committed_offset.encode(buf);
                KafkaString(partition.metadata.clone()).try_encode(buf)?;
            }
        }
        Ok(())
    }

    /// Encode as version 2 (with leader epoch).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode(buf)?;
            array_len_i32(topic.partitions.len())?.encode(buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                KafkaString(partition.metadata.clone()).try_encode(buf)?;
            }
        }
        Ok(())
    }
}

/// TxnOffsetCommit response.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results by topic.
    pub topics: Vec<TxnOffsetCommitTopicResult>,
}

/// Result for a topic in TxnOffsetCommit.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition results.
    pub partitions: Vec<TxnOffsetCommitPartitionResult>,
}

/// Result for a partition in TxnOffsetCommit.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl TxnOffsetCommitResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(TxnOffsetCommitPartitionResult {
                    partition,
                    error_code,
                });
            }

            topics.push(TxnOffsetCommitTopicResult { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Check if all offsets were committed successfully.
    pub fn is_ok(&self) -> bool {
        self.topics
            .iter()
            .all(|t| t.partitions.iter().all(|p| p.error_code.is_ok()))
    }
}

// ============================================================================
// DescribeGroups API (Key 15)
// ============================================================================

/// DescribeGroups request.
#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    /// Group IDs to describe.
    pub groups: Vec<String>,
}

impl DescribeGroupsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeGroups
    }

    /// Encode for version 0+.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.groups.len())?);
        for group in &self.groups {
            KafkaString::new(group).try_encode(buf)?;
        }
        Ok(())
    }
}

/// A member in a described group.
#[derive(Debug, Clone)]
pub struct DescribeGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (static membership).
    pub group_instance_id: Option<String>,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Member metadata.
    pub member_metadata: Bytes,
    /// Member assignment.
    pub member_assignment: Bytes,
}

/// A described group.
#[derive(Debug, Clone)]
pub struct DescribedGroup {
    /// Error code.
    pub error_code: ErrorCode,
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g., "Stable", "Empty", "Dead").
    pub group_state: String,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Protocol data (assignor name).
    pub protocol_data: String,
    /// Group members.
    pub members: Vec<DescribeGroupMember>,
}

/// DescribeGroups response.
#[derive(Debug, Clone)]
pub struct DescribeGroupsResponse {
    /// Throttle time (v1+).
    pub throttle_time_ms: i32,
    /// Described groups.
    pub groups: Vec<DescribedGroup>,
}

impl DescribeGroupsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let group_state = KafkaString::decode(buf)?.0.unwrap_or_default();
            let protocol_type = KafkaString::decode(buf)?.0.unwrap_or_default();
            let protocol_data = KafkaString::decode(buf)?.0.unwrap_or_default();

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(member_count);

            for _ in 0..member_count {
                let member_id = KafkaString::decode(buf)?.0.unwrap_or_default();
                let client_id = KafkaString::decode(buf)?.0.unwrap_or_default();
                let client_host = KafkaString::decode(buf)?.0.unwrap_or_default();
                let member_metadata = KafkaBytes::decode(buf)?.0.unwrap_or_default();
                let member_assignment = KafkaBytes::decode(buf)?.0.unwrap_or_default();

                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id: None,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            groups.push(DescribedGroup {
                error_code,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
            });
        }

        Ok(Self {
            throttle_time_ms: 0,
            groups,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let group_state = KafkaString::decode(buf)?.0.unwrap_or_default();
            let protocol_type = KafkaString::decode(buf)?.0.unwrap_or_default();
            let protocol_data = KafkaString::decode(buf)?.0.unwrap_or_default();

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(member_count);

            for _ in 0..member_count {
                let member_id = KafkaString::decode(buf)?.0.unwrap_or_default();
                let client_id = KafkaString::decode(buf)?.0.unwrap_or_default();
                let client_host = KafkaString::decode(buf)?.0.unwrap_or_default();
                let member_metadata = KafkaBytes::decode(buf)?.0.unwrap_or_default();
                let member_assignment = KafkaBytes::decode(buf)?.0.unwrap_or_default();

                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id: None,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            groups.push(DescribedGroup {
                error_code,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

// ============================================================================
// ListGroups API (Key 16)
// ============================================================================

/// ListGroups request.
#[derive(Debug, Clone)]
pub struct ListGroupsRequest;

impl ListGroupsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListGroups
    }

    /// Encode for version 0+.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        // No request body for v0
        let _ = buf;
        Ok(())
    }
}

/// A listed group.
#[derive(Debug, Clone)]
pub struct ListedGroup {
    /// Group ID.
    pub group_id: String,
    /// Protocol type.
    pub protocol_type: String,
}

/// ListGroups response.
#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    /// Throttle time (v1+).
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Listed groups.
    pub groups: Vec<ListedGroup>,
}

impl ListGroupsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let group_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let protocol_type = KafkaString::decode(buf)?.0.unwrap_or_default();
            groups.push(ListedGroup {
                group_id,
                protocol_type,
            });
        }

        Ok(Self {
            throttle_time_ms: 0,
            error_code,
            groups,
        })
    }

    /// Decode from version 1+.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let group_id = KafkaString::decode(buf)?.0.unwrap_or_default();
            let protocol_type = KafkaString::decode(buf)?.0.unwrap_or_default();
            groups.push(ListedGroup {
                group_id,
                protocol_type,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }
}

// ============================================================================
// DeleteRecords API (Key 21)
// ============================================================================

/// Partition data for DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The offset before which records should be deleted.
    /// Records with offsets less than this value will be deleted.
    pub offset: i64,
}

/// Topic data for DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<DeleteRecordsPartition>,
}

/// DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsRequest {
    /// Topics to delete records from.
    pub topics: Vec<DeleteRecordsTopic>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

impl DeleteRecordsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DeleteRecords
    }

    /// Encode for version 0+.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.offset.encode(buf);
            }
        }
        self.timeout_ms.encode(buf);
        Ok(())
    }
}

/// Partition result for DeleteRecords response.
#[derive(Debug, Clone)]
pub struct DeleteRecordsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Low watermark after deletion.
    pub low_watermark: i64,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Topic result for DeleteRecords response.
#[derive(Debug, Clone)]
pub struct DeleteRecordsTopicResult {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<DeleteRecordsPartitionResult>,
}

/// DeleteRecords response.
#[derive(Debug, Clone)]
pub struct DeleteRecordsResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topic results.
    pub topics: Vec<DeleteRecordsTopicResult>,
}

impl DeleteRecordsResponse {
    /// Decode from version 0+.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let low_watermark = i64::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(DeleteRecordsPartitionResult {
                    partition_index,
                    low_watermark,
                    error_code,
                });
            }

            topics.push(DeleteRecordsTopicResult { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// ============================================================================
// OffsetForLeaderEpoch API (Key 23)
// ============================================================================

/// Partition in OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v2+, for fencing).
    pub current_leader_epoch: i32,
    /// Requested leader epoch.
    pub leader_epoch: i32,
}

/// Topic in OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions.
    pub partitions: Vec<OffsetForLeaderEpochPartition>,
}

/// OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequest {
    /// Replica ID (-1 for consumers, broker ID for followers).
    pub replica_id: i32,
    /// Topics.
    pub topics: Vec<OffsetForLeaderEpochTopic>,
}

impl OffsetForLeaderEpochRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetForLeaderEpoch
    }

    /// Encode for version 0–1 (no current_leader_epoch field).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 2 (adds current_leader_epoch per partition for fencing).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 3+ (adds replica_id field).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }
}

/// Partition in OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// End offset for the requested leader epoch.
    pub end_offset: i64,
}

/// Topic in OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partitions.
    pub partitions: Vec<OffsetForLeaderEpochPartitionResult>,
}

/// OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponse {
    /// Throttle time (v2+).
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetForLeaderEpochTopicResult>,
}

impl OffsetForLeaderEpochResponse {
    /// Decode from version 0+.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch: -1,
                    end_offset,
                });
            }

            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms: 0,
            topics,
        })
    }

    /// Decode from version 1+ (adds leader_epoch to response).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch,
                    end_offset,
                });
            }

            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms: 0,
            topics,
        })
    }

    /// Decode from version 2+ (adds throttle_time_ms header).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = KafkaString::decode(buf)?.0.unwrap_or_default();
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch,
                    end_offset,
                });
            }

            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// ---------------------------------------------------------------------------
// VersionedEncode / VersionedDecode implementations
// ---------------------------------------------------------------------------

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

impl VersionedEncode for MetadataRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1..=3 => self.encode_v1(buf)?,
            4..=7 => self.encode_v4(buf)?,
            8 => self.encode_v8(buf)?,
            _ => return unsupported_encode!("MetadataRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for MetadataResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            3..=4 => Self::decode_v3(buf),
            5..=6 => Self::decode_v5(buf),
            7 => Self::decode_v7(buf),
            8 => Self::decode_v8(buf),
            _ => unsupported_decode!("MetadataResponse", version),
        }
    }
}

impl VersionedEncode for ProduceRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("ProduceRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ProduceResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2..=3 => Self::decode_v2(buf),
            _ => unsupported_decode!("ProduceResponse", version),
        }
    }
}

impl VersionedEncode for FetchRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            // v5-v6 add fields (log_start_offset) that encode_v4 doesn't produce
            5 | 6 => return unsupported_encode!("FetchRequest", version),
            // v7-v8 share the same wire format (fetch sessions, KIP-227)
            7 | 8 => self.encode_v7(buf)?,
            // v9 adds current_leader_epoch per partition (KIP-320)
            9 | 10 => self.encode_v9(buf)?,
            // v11 adds rack_id for closest-replica routing (KIP-392)
            11 => self.encode_v11(buf)?,
            _ => return unsupported_encode!("FetchRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for FetchResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1..=3 => Self::decode_v1(buf),
            4 => Self::decode_v4(buf),
            // v5-v6 add fields (log_start_offset) that decode_v4 doesn't consume
            5 | 6 => unsupported_decode!("FetchResponse", version),
            // v7-v10 share the same wire format
            7..=10 => Self::decode_v7(buf),
            // v11 adds preferred_read_replica per partition (KIP-392)
            11 => Self::decode_v11(buf),
            _ => unsupported_decode!("FetchResponse", version),
        }
    }
}

impl VersionedEncode for FindCoordinatorRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("FindCoordinatorRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for FindCoordinatorResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("FindCoordinatorResponse", version),
        }
    }
}

impl VersionedEncode for JoinGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1..=4 => self.encode_v1(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("JoinGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for JoinGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            2..=4 => Self::decode_v2(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("JoinGroupResponse", version),
        }
    }
}

impl VersionedEncode for SyncGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("SyncGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SyncGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1..=3 => Self::decode_v1(buf),
            _ => unsupported_decode!("SyncGroupResponse", version),
        }
    }
}

impl VersionedEncode for HeartbeatRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("HeartbeatRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for HeartbeatResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1..=3 => Self::decode_v1(buf),
            _ => unsupported_decode!("HeartbeatResponse", version),
        }
    }
}

impl VersionedEncode for LeaveGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("LeaveGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for LeaveGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1..=2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            _ => unsupported_decode!("LeaveGroupResponse", version),
        }
    }
}

impl VersionedEncode for OffsetCommitRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("OffsetCommitRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetCommitResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            _ => unsupported_decode!("OffsetCommitResponse", version),
        }
    }
}

impl VersionedEncode for ListOffsetsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("ListOffsetsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListOffsetsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("ListOffsetsResponse", version),
        }
    }
}

impl VersionedEncode for OffsetFetchRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("OffsetFetchRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetFetchResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("OffsetFetchResponse", version),
        }
    }
}

impl VersionedEncode for CreateTopicsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1..=2 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("CreateTopicsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for CreateTopicsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("CreateTopicsResponse", version),
        }
    }
}

impl VersionedEncode for DeleteTopicsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("DeleteTopicsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteTopicsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("DeleteTopicsResponse", version),
        }
    }
}

impl VersionedEncode for CreatePartitionsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("CreatePartitionsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for CreatePartitionsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("CreatePartitionsResponse", version),
        }
    }
}

impl VersionedEncode for DescribeConfigsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("DescribeConfigsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeConfigsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeConfigsResponse", version),
        }
    }
}

impl VersionedEncode for AlterConfigsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("AlterConfigsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AlterConfigsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("AlterConfigsResponse", version),
        }
    }
}

impl VersionedEncode for InitProducerIdRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("InitProducerIdRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for InitProducerIdResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("InitProducerIdResponse", version),
        }
    }
}

impl VersionedEncode for SaslHandshakeRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("SaslHandshakeRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SaslHandshakeResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("SaslHandshakeResponse", version),
        }
    }
}

impl VersionedEncode for SaslAuthenticateRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("SaslAuthenticateRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SaslAuthenticateResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("SaslAuthenticateResponse", version),
        }
    }
}

impl VersionedEncode for DescribeAclsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("DescribeAclsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeAclsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeAclsResponse", version),
        }
    }
}

impl VersionedEncode for CreateAclsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("CreateAclsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for CreateAclsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("CreateAclsResponse", version),
        }
    }
}

impl VersionedEncode for DeleteAclsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("DeleteAclsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteAclsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("DeleteAclsResponse", version),
        }
    }
}

impl VersionedEncode for AddPartitionsToTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("AddPartitionsToTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AddPartitionsToTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("AddPartitionsToTxnResponse", version),
        }
    }
}

impl VersionedEncode for AddOffsetsToTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("AddOffsetsToTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AddOffsetsToTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("AddOffsetsToTxnResponse", version),
        }
    }
}

impl VersionedEncode for EndTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("EndTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for EndTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("EndTxnResponse", version),
        }
    }
}

impl VersionedEncode for TxnOffsetCommitRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("TxnOffsetCommitRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for TxnOffsetCommitResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("TxnOffsetCommitResponse", version),
        }
    }
}

impl VersionedEncode for DescribeGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("DescribeGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("DescribeGroupsResponse", version),
        }
    }
}

impl VersionedEncode for ListGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("ListGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("ListGroupsResponse", version),
        }
    }
}

impl VersionedEncode for DeleteRecordsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("DeleteRecordsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteRecordsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DeleteRecordsResponse", version),
        }
    }
}

impl VersionedEncode for OffsetForLeaderEpochRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            3 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("OffsetForLeaderEpochRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetForLeaderEpochResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2..=3 => Self::decode_v2(buf),
            _ => unsupported_decode!("OffsetForLeaderEpochResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_metadata_request_all_topics() {
        let request = MetadataRequest::all_topics();
        // Null array = "all topics" for Metadata v1+.
        assert!(request.topics.is_none());
    }

    /// v0: topics is non-nullable — `None` encodes as empty array (length 0).
    #[test]
    fn test_metadata_request_all_topics_encode_v0() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        // v0: empty array (length 0) means "all topics"
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), 0);
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

    #[test]
    fn test_find_coordinator_request() {
        let request = FindCoordinatorRequest::for_group("my-group");
        assert_eq!(request.key, "my-group");
        assert_eq!(request.key_type, 0);

        let request = FindCoordinatorRequest::for_transaction("my-txn");
        assert_eq!(request.key, "my-txn");
        assert_eq!(request.key_type, 1);
    }

    #[test]
    fn test_sasl_handshake_request() {
        let request = SaslHandshakeRequest::new("PLAIN");
        assert_eq!(request.mechanism, "PLAIN");

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_sasl_authenticate_request() {
        let auth_bytes = vec![0, b'u', b's', b'e', b'r', 0, b'p', b'a', b's', b's'];
        let request = SaslAuthenticateRequest::new(auth_bytes.clone());
        assert_eq!(request.auth_bytes, auth_bytes);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_init_producer_id_request() {
        let request = InitProducerIdRequest::idempotent();
        assert!(request.transactional_id.is_none());
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);

        let request = InitProducerIdRequest::transactional("my-txn", 60000);
        assert_eq!(request.transactional_id.as_deref(), Some("my-txn"));
        assert_eq!(request.transaction_timeout_ms, 60000);
    }

    #[test]
    fn test_acl_resource_type() {
        assert_eq!(AclResourceType::Topic.to_i8(), 2);
        assert_eq!(AclResourceType::Group.to_i8(), 3);
        assert_eq!(AclResourceType::Cluster.to_i8(), 4);
        assert_eq!(AclResourceType::from_i8(2), AclResourceType::Topic);
        assert_eq!(AclResourceType::from_i8(99), AclResourceType::Unknown);
    }

    #[test]
    fn test_acl_operation() {
        assert_eq!(AclOperation::Read.to_i8(), 3);
        assert_eq!(AclOperation::Write.to_i8(), 4);
        assert_eq!(AclOperation::from_i8(3), AclOperation::Read);
        assert_eq!(AclOperation::from_i8(99), AclOperation::Unknown);
    }

    #[test]
    fn test_acl_permission_type() {
        assert_eq!(AclPermissionType::Allow.to_i8(), 3);
        assert_eq!(AclPermissionType::Deny.to_i8(), 2);
        assert_eq!(AclPermissionType::from_i8(3), AclPermissionType::Allow);
    }

    #[test]
    fn test_acl_binding() {
        let binding = AclBinding::allow_read_topic("my-topic", "User:alice");
        assert_eq!(binding.resource_type, AclResourceType::Topic);
        assert_eq!(binding.resource_name, "my-topic");
        assert_eq!(binding.principal, "User:alice");
        assert_eq!(binding.host, "*");
        assert_eq!(binding.operation, AclOperation::Read);
        assert_eq!(binding.permission_type, AclPermissionType::Allow);
    }

    #[test]
    fn test_describe_acls_request() {
        let request = DescribeAclsRequest::all();
        assert_eq!(request.resource_type, AclResourceType::Any);
        assert!(request.resource_name.is_none());

        let request = DescribeAclsRequest::for_topic("my-topic");
        assert_eq!(request.resource_type, AclResourceType::Topic);
        assert_eq!(request.resource_name.as_deref(), Some("my-topic"));
    }

    #[test]
    fn test_create_acls_request() {
        let bindings = vec![
            AclBinding::allow_read_topic("topic1", "User:alice"),
            AclBinding::allow_write_topic("topic2", "User:bob"),
        ];
        let request = CreateAclsRequest::new(bindings);
        assert_eq!(request.creations.len(), 2);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_acls_filter() {
        let binding = AclBinding::allow_read_topic("my-topic", "User:alice");
        let filter = AclBindingFilter::matching(&binding);

        assert_eq!(filter.resource_name.as_deref(), Some("my-topic"));
        assert_eq!(filter.principal.as_deref(), Some("User:alice"));
    }

    #[test]
    fn test_transaction_result() {
        assert!(TransactionResult::Commit.to_bool());
        assert!(!TransactionResult::Abort.to_bool());
        assert_eq!(
            TransactionResult::from_bool(true),
            TransactionResult::Commit
        );
        assert_eq!(
            TransactionResult::from_bool(false),
            TransactionResult::Abort
        );
    }

    #[test]
    fn test_add_partitions_to_txn_request() {
        let request = AddPartitionsToTxnRequest::new("my-txn", 12345, 0)
            .add_partition("topic1", 0)
            .add_partition("topic1", 1)
            .add_partition("topic2", 0);

        assert_eq!(request.transactional_id, "my-txn");
        assert_eq!(request.producer_id, 12345);
        assert_eq!(request.topics.len(), 2);

        let topic1 = request.topics.iter().find(|t| t.name == "topic1").unwrap();
        assert_eq!(topic1.partitions, vec![0, 1]);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_add_offsets_to_txn_request() {
        let request = AddOffsetsToTxnRequest::new("my-txn", 12345, 0, "my-group");

        assert_eq!(request.transactional_id, "my-txn");
        assert_eq!(request.group_id, "my-group");

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_end_txn_request() {
        let commit = EndTxnRequest::commit("my-txn", 12345, 0);
        assert!(commit.committed);

        let abort = EndTxnRequest::abort("my-txn", 12345, 0);
        assert!(!abort.committed);

        let mut buf = BytesMut::new();
        commit.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_txn_offset_commit_request() {
        let request = TxnOffsetCommitRequest::new("my-txn", "my-group", 12345, 0)
            .add_offset("topic1", 0, 100, Some("metadata".to_string()))
            .add_offset("topic1", 1, 200, None);

        assert_eq!(request.transactional_id, "my-txn");
        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_groups_request() {
        let request = DescribeGroupsRequest {
            groups: vec!["group-1".to_string(), "group-2".to_string()],
        };
        assert_eq!(request.groups.len(), 2);
        assert_eq!(DescribeGroupsRequest::api_key(), ApiKey::DescribeGroups);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_groups_response_decode_v0() {
        // Build a minimal v0 response: throttle_time_ms=0, 1 group with 0 members
        let mut buf = BytesMut::new();
        // group count
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // group_id
        let group_id = "test-group";
        buf.put_i16(group_id.len() as i16);
        buf.put_slice(group_id.as_bytes());
        // group_state
        let state = "Stable";
        buf.put_i16(state.len() as i16);
        buf.put_slice(state.as_bytes());
        // protocol_type
        let ptype = "consumer";
        buf.put_i16(ptype.len() as i16);
        buf.put_slice(ptype.as_bytes());
        // protocol_data
        let pdata = "range";
        buf.put_i16(pdata.len() as i16);
        buf.put_slice(pdata.as_bytes());
        // members count
        buf.put_i32(0);

        let response = DescribeGroupsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].group_id, "test-group");
        assert_eq!(response.groups[0].group_state, "Stable");
        assert_eq!(response.groups[0].protocol_type, "consumer");
        assert_eq!(response.groups[0].protocol_data, "range");
        assert!(response.groups[0].error_code.is_ok());
        assert!(response.groups[0].members.is_empty());
    }

    #[test]
    fn test_list_groups_request() {
        let request = ListGroupsRequest;
        assert_eq!(ListGroupsRequest::api_key(), ApiKey::ListGroups);
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        // ListGroups v0 has an empty body
        assert!(buf.is_empty());
    }

    #[test]
    fn test_list_groups_response_decode_v0() {
        let mut buf = BytesMut::new();
        // error_code
        buf.put_i16(0);
        // groups count
        buf.put_i32(2);
        // group 1
        let g1 = "group-a";
        buf.put_i16(g1.len() as i16);
        buf.put_slice(g1.as_bytes());
        let pt1 = "consumer";
        buf.put_i16(pt1.len() as i16);
        buf.put_slice(pt1.as_bytes());
        // group 2
        let g2 = "group-b";
        buf.put_i16(g2.len() as i16);
        buf.put_slice(g2.as_bytes());
        let pt2 = "consumer";
        buf.put_i16(pt2.len() as i16);
        buf.put_slice(pt2.as_bytes());

        let response = ListGroupsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(response.error_code.is_ok());
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].group_id, "group-a");
        assert_eq!(response.groups[1].group_id, "group-b");
    }

    #[test]
    fn test_delete_records_request() {
        let request = DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: "my-topic".to_string(),
                partitions: vec![
                    DeleteRecordsPartition {
                        partition_index: 0,
                        offset: 100,
                    },
                    DeleteRecordsPartition {
                        partition_index: 1,
                        offset: 200,
                    },
                ],
            }],
            timeout_ms: 30000,
        };
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(DeleteRecordsRequest::api_key(), ApiKey::DeleteRecords);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_records_response_decode_v0() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // topics count
        buf.put_i32(1);
        // topic name
        let topic = "my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // partitions count
        buf.put_i32(1);
        // partition_index
        buf.put_i32(0);
        // low_watermark
        buf.put_i64(100);
        // error_code
        buf.put_i16(0);

        let response = DeleteRecordsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "my-topic");
        assert_eq!(response.topics[0].partitions.len(), 1);
        assert_eq!(response.topics[0].partitions[0].partition_index, 0);
        assert_eq!(response.topics[0].partitions[0].low_watermark, 100);
        assert!(response.topics[0].partitions[0].error_code.is_ok());
    }

    #[test]
    fn test_offset_for_leader_epoch_request() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: "my-topic".to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition: 0,
                    current_leader_epoch: 5,
                    leader_epoch: 4,
                }],
            }],
        };
        assert_eq!(
            OffsetForLeaderEpochRequest::api_key(),
            ApiKey::OffsetForLeaderEpoch
        );

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());

        let mut buf3 = BytesMut::new();
        request.encode_v3(&mut buf3).unwrap();
        // v3 includes replica_id prefix, so it should be longer
        assert!(buf3.len() > buf.len());
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v0() {
        let mut buf = BytesMut::new();
        // topics count
        buf.put_i32(1);
        // topic name
        let topic = "my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // partitions count
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // end_offset
        buf.put_i64(500);

        let response = OffsetForLeaderEpochResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].topic, "my-topic");
        assert_eq!(response.topics[0].partitions.len(), 1);
        assert!(response.topics[0].partitions[0].error_code.is_ok());
        assert_eq!(response.topics[0].partitions[0].partition, 0);
        assert_eq!(response.topics[0].partitions[0].end_offset, 500);
        assert_eq!(response.topics[0].partitions[0].leader_epoch, -1); // v0 doesn't have leader_epoch
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v1() {
        let mut buf = BytesMut::new();
        // topics count
        buf.put_i32(1);
        // topic name
        let topic = "my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // partitions count
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // leader_epoch
        buf.put_i32(5);
        // end_offset
        buf.put_i64(500);

        let response = OffsetForLeaderEpochResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions[0].leader_epoch, 5);
        assert_eq!(response.topics[0].partitions[0].end_offset, 500);
    }

    #[test]
    fn test_join_group_request_encode_v5() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
        };

        let mut buf_v0 = BytesMut::new();
        request.encode_v0(&mut buf_v0).unwrap();

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        // v5 should include group_instance_id, so it should be larger
        assert!(buf_v5.len() > buf_v0.len());
    }

    #[test]
    fn test_heartbeat_request_encode_v3() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
        };

        let mut buf_v0 = BytesMut::new();
        request.encode_v0(&mut buf_v0).unwrap();

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 should include group_instance_id, so it should be larger
        assert!(buf_v3.len() > buf_v0.len());
    }

    #[test]
    fn test_heartbeat_request_encode_v3_null_instance_id() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
        };

        let mut buf_v0 = BytesMut::new();
        request.encode_v0(&mut buf_v0).unwrap();

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 with null instance_id should be slightly larger (null marker)
        assert!(buf_v3.len() >= buf_v0.len());
    }

    #[test]
    fn test_join_group_response_decode_v5() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(100);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(3);
        // protocol_name
        let proto = b"range";
        buf.put_i16(proto.len() as i16);
        buf.put_slice(proto);
        // leader
        let leader = b"member-1";
        buf.put_i16(leader.len() as i16);
        buf.put_slice(leader);
        // member_id
        let member = b"member-1";
        buf.put_i16(member.len() as i16);
        buf.put_slice(member);
        // member_count = 2
        buf.put_i32(2);
        // member 1: member_id, group_instance_id, metadata
        let m1 = b"member-1";
        buf.put_i16(m1.len() as i16);
        buf.put_slice(m1);
        let inst1 = b"instance-1";
        buf.put_i16(inst1.len() as i16);
        buf.put_slice(inst1);
        let meta1 = b"meta1";
        buf.put_i32(meta1.len() as i32);
        buf.put_slice(meta1);
        // member 2: member_id, null group_instance_id, metadata
        let m2 = b"member-2";
        buf.put_i16(m2.len() as i16);
        buf.put_slice(m2);
        buf.put_i16(-1); // null instance id
        let meta2 = b"meta2";
        buf.put_i32(meta2.len() as i32);
        buf.put_slice(meta2);

        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v5(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 3);
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert_eq!(resp.leader, "member-1");
        assert_eq!(resp.member_id, "member-1");
        assert!(resp.is_leader());
        assert_eq!(resp.members.len(), 2);
        assert_eq!(resp.members[0].member_id, "member-1");
        assert_eq!(
            resp.members[0].group_instance_id,
            Some("instance-1".to_string())
        );
        assert_eq!(resp.members[1].member_id, "member-2");
        assert_eq!(resp.members[1].group_instance_id, None);
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v2() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms (v2 adds this)
        buf.put_i32(50);
        // topic_count = 1
        buf.put_i32(1);
        // topic name
        let topic = b"my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partition_count = 1
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // leader_epoch
        buf.put_i32(5);
        // end_offset
        buf.put_i64(1000);

        let mut data = buf.freeze();
        let resp = OffsetForLeaderEpochResponse::decode_v2(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "my-topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        assert_eq!(resp.topics[0].partitions[0].partition, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
        assert_eq!(resp.topics[0].partitions[0].end_offset, 1000);
    }

    // SyncGroupRequest::encode_v3 includes group_instance_id.
    #[test]
    fn test_sync_group_request_encode_v3_includes_group_instance_id() {
        use bytes::BytesMut;

        let request = SyncGroupRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![],
        };

        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();

        // Verify the buffer contains the group_instance_id
        let data = buf.freeze();
        let data_str = String::from_utf8_lossy(&data);
        assert!(
            data_str.contains("instance-1"),
            "v3 encoding should include group_instance_id"
        );
    }

    // SyncGroupRequest::encode_v0 does NOT include group_instance_id.
    #[test]
    fn test_sync_group_request_encode_v0_omits_group_instance_id() {
        use bytes::BytesMut;

        let request = SyncGroupRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![],
        };

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();

        let data = buf.freeze();
        let data_str = String::from_utf8_lossy(&data);
        assert!(
            !data_str.contains("instance-1"),
            "v0 encoding should NOT include group_instance_id"
        );
    }

    // LeaveGroupResponse decode_v0 and decode_v1 roundtrip.
    #[test]
    fn test_leave_group_response_decode_v0() {
        let mut buf = BytesMut::new();
        // error_code = 0 (NONE)
        buf.put_i16(0);

        let mut data = buf.freeze();
        let resp = LeaveGroupResponse::decode_v0(&mut data).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.members.is_empty());
    }

    #[test]
    fn test_leave_group_response_decode_v1_with_error() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(100);
        // error_code = 16 (NOT_COORDINATOR)
        buf.put_i16(16);

        let mut data = buf.freeze();
        let resp = LeaveGroupResponse::decode_v1(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert!(!resp.error_code.is_ok());
        assert!(resp.members.is_empty());
    }

    #[test]
    fn test_leave_group_response_decode_v3_with_members() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(50);
        // top-level error_code = 0 (NONE)
        buf.put_i16(0);
        // members array length = 2
        buf.put_i32(2);

        // member 1: member_id = "m-1", group_instance_id = "i-1", error_code = 0
        let m1 = b"m-1";
        buf.put_i16(m1.len() as i16);
        buf.put_slice(m1);
        let i1 = b"i-1";
        buf.put_i16(i1.len() as i16);
        buf.put_slice(i1);
        buf.put_i16(0);

        // member 2: member_id = "m-2", group_instance_id = null, error_code = 79 (FENCED_INSTANCE_ID)
        let m2 = b"m-2";
        buf.put_i16(m2.len() as i16);
        buf.put_slice(m2);
        buf.put_i16(-1); // null group_instance_id
        buf.put_i16(79);

        let mut data = buf.freeze();
        let resp = LeaveGroupResponse::decode_v3(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.members.len(), 2);

        assert_eq!(resp.members[0].member_id, "m-1");
        assert_eq!(resp.members[0].group_instance_id, Some("i-1".to_string()));
        assert!(resp.members[0].error_code.is_ok());

        assert_eq!(resp.members[1].member_id, "m-2");
        assert_eq!(resp.members[1].group_instance_id, None);
        assert!(!resp.members[1].error_code.is_ok());
    }

    // SyncGroupResponse decode_v1 roundtrip.
    #[test]
    fn test_sync_group_response_decode_v1() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(50);
        // error_code = 0 (NONE)
        buf.put_i16(0);
        // assignment (empty bytes: length = 0)
        buf.put_i32(0);

        let mut data = buf.freeze();
        let resp = SyncGroupResponse::decode_v1(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert!(resp.assignment.is_empty());
    }

    // ── R14: ListOffsetsResponse decode safety ──

    #[test]
    fn test_list_offsets_response_decode_v1_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // 0 topics
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v1(&mut data).unwrap();
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_truncated_no_topic_count() {
        let mut buf = BytesMut::new();
        buf.put_i8(0); // only 1 byte — not enough for i32
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_negative_topic_count() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // negative count
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_negative_partition_count() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        // topic name (short string)
        buf.put_i16(4);
        buf.put_slice(b"test");
        buf.put_i32(-1); // negative partition count
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_truncated_partition() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        buf.put_i16(4);
        buf.put_slice(b"test");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index only (missing error_code, timestamp, offset)
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_valid() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        buf.put_i16(5);
        buf.put_slice(b"topic");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1234567890); // timestamp
        buf.put_i64(42); // offset
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v1(&mut data).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].offset, 42);
        assert_eq!(resp.topics[0].partitions[0].timestamp, 1234567890);
    }

    // ── ListOffsetsResponse decode_v2 (with throttle_time_ms) ──

    #[test]
    fn test_list_offsets_response_decode_v2_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(0); // 0 topics
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v2(&mut data).unwrap();
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_offsets_response_decode_v2_valid() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        buf.put_i32(1); // 1 topic
        buf.put_i16(5);
        buf.put_slice(b"topic");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1234567890); // timestamp
        buf.put_i64(42); // offset
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v2(&mut data).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].offset, 42);
    }

    #[test]
    fn test_list_offsets_response_decode_v2_truncated() {
        let mut buf = BytesMut::new();
        buf.put_i8(0); // only 1 byte — not enough for throttle_time_ms
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v2(&mut data);
        assert!(result.is_err());
    }

    // ── R14: ListOffsetsRequest encode_v2 with isolation_level ──

    #[test]
    fn test_list_offsets_request_encode_v2_includes_isolation_level() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1, // read_committed
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    timestamp: -1,
                }],
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();

        // replica_id (4 bytes) + isolation_level (1 byte) + topics
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), -1);
        assert_eq!(buf[4], 1); // isolation_level = read_committed
    }

    #[test]
    fn test_list_offsets_request_encode_v1_no_isolation_level() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![],
        };

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();

        let mut buf_v2 = BytesMut::new();
        request.encode_v2(&mut buf_v2).unwrap();

        // v2 should be 1 byte longer (isolation_level)
        assert_eq!(buf_v2.len(), buf_v1.len() + 1);
    }

    #[test]
    fn test_fetch_request_encode_v7_includes_session_fields() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 42,
            session_epoch: 3,
            topics: vec![FetchTopicRequest {
                topic: "test-topic".to_string(),
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1048576,
                }],
            }],
            forgotten_topics: vec![FetchForgottenTopic {
                topic: "old-topic".to_string(),
                partitions: vec![1, 2],
            }],
            rack_id: String::new(),
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();

        let mut buf_v7 = BytesMut::new();
        request.encode_v7(&mut buf_v7).unwrap();

        // v7 adds session_id(4) + session_epoch(4) + log_start_offset per partition(8)
        // + forgotten_topics array (4 + topic string + partitions)
        assert!(buf_v7.len() > buf_v4.len());

        // Verify session_id and session_epoch at expected offsets:
        // replica_id(4) + max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) = 17
        let session_id_bytes = &buf_v7[17..21];
        assert_eq!(i32::from_be_bytes(session_id_bytes.try_into().unwrap()), 42);
        let session_epoch_bytes = &buf_v7[21..25];
        assert_eq!(
            i32::from_be_bytes(session_epoch_bytes.try_into().unwrap()),
            3
        );
    }

    #[test]
    fn test_fetch_request_encode_v7_empty_forgotten_topics() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf = BytesMut::new();
        request.encode_v7(&mut buf).unwrap();

        // Should still encode: header fields + empty topics array(4) + empty forgotten array(4)
        // replica_id(4) + max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1)
        // + session_id(4) + session_epoch(4) + topics_count(4) + forgotten_count(4) = 33
        assert_eq!(buf.len(), 33);
    }

    #[test]
    fn test_fetch_response_decode_v7_round_trip() {
        // Build a v7 response manually: throttle(4) + error_code(2) + session_id(4) + topics
        let mut raw = BytesMut::new();
        raw.put_i32(100); // throttle_time_ms
        raw.put_i16(0); // error_code (None)
        raw.put_i32(42); // session_id
        raw.put_i32(1); // 1 topic
        // topic name
        raw.put_i16(5);
        raw.put_slice(b"topic");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(1000); // high_watermark
        raw.put_i64(999); // last_stable_offset
        raw.put_i64(0); // log_start_offset
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(-1); // records (null/-1 length)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v7(&mut buf).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.session_id, 42);
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].topic, "topic");
        assert_eq!(resp.responses[0].partitions.len(), 1);
        assert_eq!(resp.responses[0].partitions[0].partition, 0);
        assert_eq!(resp.responses[0].partitions[0].high_watermark, 1000);
        assert_eq!(resp.responses[0].partitions[0].last_stable_offset, 999);
        assert_eq!(resp.responses[0].partitions[0].log_start_offset, 0);
    }

    #[test]
    fn test_fetch_response_decode_v7_session_error() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i16(70); // FetchSessionIdNotFound
        raw.put_i32(0); // session_id
        raw.put_i32(0); // 0 topics

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v7(&mut buf).unwrap();

        assert_eq!(resp.error_code, ErrorCode::FetchSessionIdNotFound);
        assert_eq!(resp.session_id, 0);
        assert!(resp.responses.is_empty());
    }

    #[test]
    fn test_fetch_response_decode_v7_vs_v4_extra_fields() {
        // v4 response: no error_code, no session_id
        let mut raw_v4 = BytesMut::new();
        raw_v4.put_i32(50); // throttle_time_ms
        raw_v4.put_i32(0); // 0 topics

        let mut buf_v4 = raw_v4.freeze();
        let resp_v4 = FetchResponse::decode_v4(&mut buf_v4).unwrap();
        assert_eq!(resp_v4.throttle_time_ms, 50);
        assert_eq!(resp_v4.error_code, ErrorCode::None); // default
        assert_eq!(resp_v4.session_id, 0); // default

        // v7 response: has error_code + session_id
        let mut raw_v7 = BytesMut::new();
        raw_v7.put_i32(50); // throttle_time_ms
        raw_v7.put_i16(0); // error_code
        raw_v7.put_i32(99); // session_id
        raw_v7.put_i32(0); // 0 topics

        let mut buf_v7 = raw_v7.freeze();
        let resp_v7 = FetchResponse::decode_v7(&mut buf_v7).unwrap();
        assert_eq!(resp_v7.throttle_time_ms, 50);
        assert_eq!(resp_v7.session_id, 99);
    }

    // ── VersionedEncode / VersionedDecode tests ─────────────────────

    #[test]
    fn test_versioned_encode_metadata_request_v0() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        request.encode_versioned(0, &mut buf).unwrap();
        // Should produce the same bytes as encode_v0
        let mut expected = BytesMut::new();
        request.encode_v0(&mut expected).unwrap();
        assert_eq!(buf, expected);
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

    /// v0 and v1 encode differently for all_topics(): empty array vs null array.
    #[test]
    fn test_versioned_encode_metadata_v0_vs_v1_all_topics() {
        let request = MetadataRequest::all_topics();
        let mut buf_v0 = BytesMut::new();
        request.encode_versioned(0, &mut buf_v0).unwrap();
        let mut buf_v1 = BytesMut::new();
        request.encode_versioned(1, &mut buf_v1).unwrap();
        // v0: 0x00000000 (empty array), v1: 0xFFFFFFFF (null array)
        assert_ne!(buf_v0, buf_v1);
        assert_eq!(
            i32::from_be_bytes([buf_v0[0], buf_v0[1], buf_v0[2], buf_v0[3]]),
            0
        );
        assert_eq!(
            i32::from_be_bytes([buf_v1[0], buf_v1[1], buf_v1[2], buf_v1[3]]),
            -1
        );
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
    fn test_versioned_encode_metadata_request_rejects_v9() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        let result = request.encode_versioned(9, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    #[test]
    fn test_versioned_encode_rejects_negative_version() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        let result = request.encode_versioned(-1, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    #[test]
    fn test_versioned_decode_rejects_negative_version() {
        let mut buf = bytes::Bytes::new();
        let result = MetadataResponse::decode_versioned(-1, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    #[test]
    fn test_versioned_decode_metadata_rejects_v9() {
        let mut buf = bytes::Bytes::new();
        let result = MetadataResponse::decode_versioned(9, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    /// v0: brokers (no rack) + topics (no is_internal), no controller_id/cluster_id/throttle.
    #[test]
    fn test_metadata_response_decode_v0() {
        let mut buf = BytesMut::new();
        // 1 broker
        buf.put_i32(1);
        buf.put_i32(1); // node_id
        let host = b"broker1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(9092); // port
        // 1 topic
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        let topic = b"test";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // 1 partition
        buf.put_i32(1);
        buf.put_i16(0); // error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(1); // leader_id
        buf.put_i32(1); // replicas count
        buf.put_i32(1); // replica
        buf.put_i32(1); // isr count
        buf.put_i32(1); // isr

        let resp = MetadataResponse::decode_versioned(0, &mut buf.freeze()).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].rack, None); // no rack in v0
        assert_eq!(resp.controller_id, -1); // no controller in v0
        assert_eq!(resp.cluster_id, None);
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(!resp.topics[0].is_internal); // defaults to false in v0
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, -1);
        assert!(resp.topics[0].partitions[0].offline_replicas.is_empty());
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
    fn test_versioned_encode_decode_roundtrip_sasl_handshake() {
        let request = SaslHandshakeRequest::new("SCRAM-SHA-256");
        let mut buf = BytesMut::new();
        request.encode_versioned(0, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // High version still works (dispatches to latest encoder)
        let mut buf2 = BytesMut::new();
        request.encode_versioned(1, &mut buf2).unwrap();
        assert!(!buf2.is_empty());
    }

    #[test]
    fn test_versioned_encode_fetch_request_dispatches_correctly() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        // v0 and v7 should use different encoders and produce different output
        let mut buf_v0 = BytesMut::new();
        request.encode_versioned(0, &mut buf_v0).unwrap();
        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();
        // v7 encodes extra fields (session_id, session_epoch) so should be longer
        assert!(buf_v7.len() > buf_v0.len());
    }

    #[test]
    fn test_fetch_request_encode_v11_appends_rack_id() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: 5,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 0,
                    partition_max_bytes: 1048576,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: "us-east-1a".to_string(),
        };

        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();

        let mut buf_v11 = BytesMut::new();
        request.encode_v11(&mut buf_v11).unwrap();

        // v11 is v9 + rack_id string (2 bytes length + 10 bytes "us-east-1a")
        assert_eq!(buf_v11.len(), buf_v9.len() + 2 + 10);

        // The v11 buffer should start with the same bytes as v9
        assert_eq!(&buf_v11[..buf_v9.len()], &buf_v9[..]);
    }

    #[test]
    fn test_fetch_request_encode_v11_empty_rack_id() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();

        let mut buf_v11 = BytesMut::new();
        request.encode_v11(&mut buf_v11).unwrap();

        // Empty rack_id: 2-byte length prefix (0) only
        assert_eq!(buf_v11.len(), buf_v9.len() + 2);
    }

    #[test]
    fn test_fetch_response_decode_v11_preferred_read_replica() {
        // Build a v11 response: same as v7 but with preferred_read_replica per partition
        let mut raw = BytesMut::new();
        raw.put_i32(100); // throttle_time_ms
        raw.put_i16(0); // error_code (None)
        raw.put_i32(42); // session_id
        raw.put_i32(1); // 1 topic
        // topic name
        raw.put_i16(5);
        raw.put_slice(b"topic");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(1000); // high_watermark
        raw.put_i64(999); // last_stable_offset
        raw.put_i64(0); // log_start_offset
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(3); // preferred_read_replica = broker 3
        raw.put_i32(-1); // records (null/-1 length)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v11(&mut buf).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.session_id, 42);
        assert_eq!(resp.responses.len(), 1);
        let part = &resp.responses[0].partitions[0];
        assert_eq!(part.partition, 0);
        assert_eq!(part.high_watermark, 1000);
        assert_eq!(part.preferred_read_replica, 3);
    }

    #[test]
    fn test_fetch_response_decode_v11_no_preferred_replica() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i16(0); // error_code
        raw.put_i32(0); // session_id
        raw.put_i32(1); // 1 topic
        raw.put_i16(1);
        raw.put_slice(b"t");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(500); // high_watermark
        raw.put_i64(499); // last_stable_offset
        raw.put_i64(0); // log_start_offset
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(-1); // preferred_read_replica = -1 (no preference)
        raw.put_i32(-1); // records (null)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v11(&mut buf).unwrap();

        assert_eq!(resp.responses[0].partitions[0].preferred_read_replica, -1);
    }

    #[test]
    fn test_versioned_encode_fetch_v11_dispatches_to_encode_v11() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: "rack-a".to_string(),
        };

        let mut buf_direct = BytesMut::new();
        request.encode_v11(&mut buf_direct).unwrap();

        let mut buf_versioned = BytesMut::new();
        request.encode_versioned(11, &mut buf_versioned).unwrap();

        assert_eq!(buf_direct, buf_versioned);
    }

    #[test]
    fn test_versioned_decode_fetch_v11_dispatches_to_decode_v11() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i16(0); // error_code
        raw.put_i32(0); // session_id
        raw.put_i32(0); // 0 topics

        let data = raw.freeze();
        let resp = FetchResponse::decode_versioned(11, &mut data.clone()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.responses.is_empty());
    }

    #[test]
    fn test_versioned_encode_fetch_v8_dispatches_to_v7() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 0,
                    partition_max_bytes: 1048576,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();

        let mut buf_v8 = BytesMut::new();
        request.encode_versioned(8, &mut buf_v8).unwrap();
        assert_eq!(buf_v8, buf_v7, "v8 should produce same bytes as v7");
    }

    #[test]
    fn test_versioned_encode_fetch_v9_v10_dispatches_to_v9() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: 5,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 0,
                    partition_max_bytes: 1048576,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();

        for version in 9..=10 {
            let mut buf = BytesMut::new();
            request.encode_versioned(version, &mut buf).unwrap();
            assert_eq!(buf, buf_v9, "v{version} should produce same bytes as v9");
        }

        // v9+ includes current_leader_epoch (4 bytes per partition) that v7 omits
        let mut buf_v7 = BytesMut::new();
        request.encode_v7(&mut buf_v7).unwrap();
        assert_eq!(
            buf_v9.len(),
            buf_v7.len() + 4,
            "v9 should be 4 bytes longer than v7 (current_leader_epoch per partition)"
        );
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
        let result = request.encode_v0(&mut buf);
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
                partition_data: vec![],
            }],
        };
        let mut buf = BytesMut::new();
        let result = request.encode_v0(&mut buf);
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
        let result = request.encode_versioned(0, &mut buf);
        assert!(
            result.is_err(),
            "VersionedEncode must propagate encoding errors"
        );
    }
}
