use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};
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

// ============================================================================
// DeleteGroups API (Key 42)
// ============================================================================

/// DeleteGroups request (API Key 42).
#[derive(Debug, Clone)]
pub struct DeleteGroupsRequest {
    /// Group names to delete.
    pub group_names: Vec<String>,
}

impl DeleteGroupsRequest {
    /// Create a new request.
    pub fn new(group_names: Vec<String>) -> Self {
        Self { group_names }
    }

    /// Encode for version 0–1 (non-flexible).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.group_names.len())?.encode(buf);
        for name in &self.group_names {
            KafkaString::new(name).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 2 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.group_names.len(), buf)?;
        for name in &self.group_names {
            KafkaString::new(name).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DeleteGroups response (API Key 42).
#[derive(Debug, Clone)]
pub struct DeleteGroupsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Deletion results.
    pub results: Vec<DeletableGroupResult>,
}

/// Result for a single group deletion.
#[derive(Debug, Clone)]
pub struct DeletableGroupResult {
    /// Group ID.
    pub group_id: String,
    /// Error code.
    pub error_code: ErrorCode,
}

impl DeleteGroupsResponse {
    /// Decode from version 0–1 (non-flexible).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            results.push(DeletableGroupResult {
                group_id,
                error_code,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 2 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let _ = TaggedFields::decode(buf)?;
            results.push(DeletableGroupResult {
                group_id,
                error_code,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

// ============================================================================
// DescribeCluster API (Key 60)
// ============================================================================

/// DescribeCluster request (API Key 60). Flexible from v0.
#[derive(Debug, Clone)]
pub struct DescribeClusterRequest {
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// Endpoint type to describe (v1+). 1=brokers, 2=controllers.
    pub endpoint_type: i8,
    /// Whether to include fenced brokers (v2+).
    pub include_fenced_brokers: bool,
}

impl Default for DescribeClusterRequest {
    fn default() -> Self {
        Self {
            include_cluster_authorized_operations: false,
            endpoint_type: 1,
            include_fenced_brokers: false,
        }
    }
}

impl DescribeClusterRequest {
    /// Encode for version 0 (flexible from v0).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(u8::from(self.include_cluster_authorized_operations));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 1 (adds endpoint_type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(u8::from(self.include_cluster_authorized_operations));
        self.endpoint_type.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 2 (adds include_fenced_brokers).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(u8::from(self.include_cluster_authorized_operations));
        self.endpoint_type.encode(buf);
        buf.put_u8(u8::from(self.include_fenced_brokers));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DescribeCluster response (API Key 60). Flexible from v0.
#[derive(Debug, Clone)]
pub struct DescribeClusterResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message.
    pub error_message: Option<String>,
    /// Endpoint type (v1+). 1=brokers, 2=controllers.
    pub endpoint_type: i8,
    /// Cluster ID.
    pub cluster_id: String,
    /// Controller broker ID.
    pub controller_id: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<DescribeClusterBroker>,
    /// Cluster authorized operations (bitfield).
    pub cluster_authorized_operations: i32,
}

/// Broker info in DescribeCluster response.
#[derive(Debug, Clone)]
pub struct DescribeClusterBroker {
    /// Broker ID.
    pub broker_id: i32,
    /// Broker hostname.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Rack (if assigned).
    pub rack: Option<String>,
    /// Whether the broker is fenced (v2+).
    pub is_fenced: bool,
}

impl DescribeClusterResponse {
    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let cluster_id = non_nullable_string("cluster_id", KafkaString::decode_compact(buf)?.0)?;
        let controller_id = i32::decode(buf)?;

        let broker_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = i32::decode(buf)?;
            let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
            let port = i32::decode(buf)?;
            let rack = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
                is_fenced: false,
            });
        }

        let cluster_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type: 1,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }

    /// Decode from version 1 (adds endpoint_type).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let endpoint_type = i8::decode(buf)?;
        let cluster_id = non_nullable_string("cluster_id", KafkaString::decode_compact(buf)?.0)?;
        let controller_id = i32::decode(buf)?;

        let broker_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = i32::decode(buf)?;
            let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
            let port = i32::decode(buf)?;
            let rack = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
                is_fenced: false,
            });
        }

        let cluster_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }

    /// Decode from version 2 (adds is_fenced per broker).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let endpoint_type = i8::decode(buf)?;
        let cluster_id = non_nullable_string("cluster_id", KafkaString::decode_compact(buf)?.0)?;
        let controller_id = i32::decode(buf)?;

        let broker_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = i32::decode(buf)?;
            let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
            let port = i32::decode(buf)?;
            let rack = KafkaString::decode_compact(buf)?.0;
            let is_fenced = i8::decode(buf)? != 0;
            let _ = TaggedFields::decode(buf)?;
            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
                is_fenced,
            });
        }

        let cluster_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }
}

// ============================================================================
// ConsumerGroupDescribe API (Key 69)
// ============================================================================

/// ConsumerGroupDescribe request (API Key 69). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeRequest {
    /// Group IDs to describe.
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
}

impl ConsumerGroupDescribeRequest {
    /// Create a new request.
    pub fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            include_authorized_operations: false,
        }
    }

    /// Encode for version 0–1 (flexible from v0; v1 request is same wire format as v0).
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

/// Topic-partition assignment used in ConsumerGroupDescribe response.
#[derive(Debug, Clone)]
pub struct DescribeGroupTopicPartition {
    /// Topic ID.
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Partition indices.
    pub partitions: Vec<i32>,
}

/// Assignment (target or current) for a consumer group member.
#[derive(Debug, Clone)]
pub struct DescribeGroupAssignment {
    /// Topic-partition assignments.
    pub topic_partitions: Vec<DescribeGroupTopicPartition>,
}

/// A member in ConsumerGroupDescribe response.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeMember {
    /// Member ID.
    pub member_id: String,
    /// Instance ID (static membership).
    pub instance_id: Option<String>,
    /// Rack ID.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Subscribed topic regex.
    pub subscribed_topic_regex: Option<String>,
    /// Current assignment.
    pub assignment: DescribeGroupAssignment,
    /// Target assignment.
    pub target_assignment: DescribeGroupAssignment,
    /// Member type (v1+). -1=unknown, 0=classic, 1=consumer.
    pub member_type: i8,
}

/// A described group in ConsumerGroupDescribe response.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeGroup {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Group state.
    pub group_state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Assignor name.
    pub assignor_name: String,
    /// Members.
    pub members: Vec<ConsumerGroupDescribeMember>,
    /// Authorized operations bitfield.
    pub authorized_operations: i32,
}

/// ConsumerGroupDescribe response (API Key 69). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described groups.
    pub groups: Vec<ConsumerGroupDescribeGroup>,
}

impl ConsumerGroupDescribeResponse {
    /// Decode assignment (shared between current and target).
    fn decode_assignment(buf: &mut impl Buf) -> Result<DescribeGroupAssignment> {
        let tp_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topic_partitions = Vec::with_capacity(tp_count);
        for _ in 0..tp_count {
            let mut topic_id = [0u8; 16];
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("short buf for topic_id"));
            }
            buf.copy_to_slice(&mut topic_id);
            let topic_name =
                non_nullable_string("topic_name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                partitions.push(i32::decode(buf)?);
            }
            let _ = TaggedFields::decode(buf)?;
            topic_partitions.push(DescribeGroupTopicPartition {
                topic_id,
                topic_name,
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(DescribeGroupAssignment { topic_partitions })
    }

    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let group_epoch = i32::decode(buf)?;
            let assignment_epoch = i32::decode(buf)?;
            let assignor_name =
                non_nullable_string("assignor_name", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let instance_id = KafkaString::decode_compact(buf)?.0;
                let rack_id = KafkaString::decode_compact(buf)?.0;
                let member_epoch = i32::decode(buf)?;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;

                let sub_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut subscribed_topic_names = Vec::with_capacity(sub_count);
                for _ in 0..sub_count {
                    subscribed_topic_names.push(non_nullable_string(
                        "subscribed_topic",
                        KafkaString::decode_compact(buf)?.0,
                    )?);
                }
                let subscribed_topic_regex = KafkaString::decode_compact(buf)?.0;

                let assignment = Self::decode_assignment(buf)?;
                let target_assignment = Self::decode_assignment(buf)?;

                let _ = TaggedFields::decode(buf)?;
                members.push(ConsumerGroupDescribeMember {
                    member_id,
                    instance_id,
                    rack_id,
                    member_epoch,
                    client_id,
                    client_host,
                    subscribed_topic_names,
                    subscribed_topic_regex,
                    assignment,
                    target_assignment,
                    member_type: -1,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(ConsumerGroupDescribeGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                group_epoch,
                assignment_epoch,
                assignor_name,
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

    /// Decode from version 1 (adds member_type per member; KIP-1099).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let group_epoch = i32::decode(buf)?;
            let assignment_epoch = i32::decode(buf)?;
            let assignor_name =
                non_nullable_string("assignor_name", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let instance_id = KafkaString::decode_compact(buf)?.0;
                let rack_id = KafkaString::decode_compact(buf)?.0;
                let member_epoch = i32::decode(buf)?;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;

                let sub_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut subscribed_topic_names = Vec::with_capacity(sub_count);
                for _ in 0..sub_count {
                    subscribed_topic_names.push(non_nullable_string(
                        "subscribed_topic",
                        KafkaString::decode_compact(buf)?.0,
                    )?);
                }
                let subscribed_topic_regex = KafkaString::decode_compact(buf)?.0;

                let assignment = Self::decode_assignment(buf)?;
                let target_assignment = Self::decode_assignment(buf)?;
                let member_type = i8::decode(buf)?;

                let _ = TaggedFields::decode(buf)?;
                members.push(ConsumerGroupDescribeMember {
                    member_id,
                    instance_id,
                    rack_id,
                    member_epoch,
                    client_id,
                    client_host,
                    subscribed_topic_names,
                    subscribed_topic_regex,
                    assignment,
                    target_assignment,
                    member_type,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(ConsumerGroupDescribeGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                group_epoch,
                assignment_epoch,
                assignor_name,
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

// ============================================================================
// ListClientMetricsResources API (Key 74)
// ============================================================================

/// ListClientMetricsResources request (API Key 74). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ListClientMetricsResourcesRequest;

impl ListClientMetricsResourcesRequest {
    /// Encode for version 0 (flexible from v0, empty body).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// A client metrics resource name.
#[derive(Debug, Clone)]
pub struct ClientMetricsResource {
    /// Resource name.
    pub name: String,
}

/// ListClientMetricsResources response (API Key 74). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ListClientMetricsResourcesResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Client metrics resource names.
    pub client_metrics_resources: Vec<ClientMetricsResource>,
}

impl ListClientMetricsResourcesResponse {
    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let resource_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut client_metrics_resources = Vec::with_capacity(resource_count);
        for _ in 0..resource_count {
            let name =
                non_nullable_string("metric resource name", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            client_metrics_resources.push(ClientMetricsResource { name });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            client_metrics_resources,
        })
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
    /// Whether to include authorized operations (v3+).
    pub include_authorized_operations: bool,
}

impl DescribeGroupsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeGroups
    }

    /// Encode for version 1–2 (groups array only, no authorized ops).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.groups.len())?);
        for group in &self.groups {
            KafkaString::new(group).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 3–4 (adds include_authorized_operations).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.groups.len())?);
        for group in &self.groups {
            KafkaString::new(group).try_encode(buf)?;
        }
        self.include_authorized_operations.encode(buf);
        Ok(())
    }

    /// Encode for version 5–6 (flexible: compact strings + tagged fields).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = u32::try_from(self.groups.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("groups array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for group in &self.groups {
            KafkaString::new(group).try_encode_compact(buf)?;
        }
        self.include_authorized_operations.encode(buf);
        TaggedFields::default().try_encode(buf)?;
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
    /// Error message (v6+, KIP-1043).
    pub error_message: Option<String>,
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
    /// Authorized operations bitfield (v3+, -2147483648 when not requested).
    pub authorized_operations: i32,
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
    /// Decode from version 1–2 (non-flexible, no authorized_operations, no instance_id).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let group_state = non_nullable_string("group_state", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            let protocol_data = non_nullable_string("protocol_data", KafkaString::decode(buf)?.0)?;

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
                let client_id = non_nullable_string("client_id", KafkaString::decode(buf)?.0)?;
                let client_host = non_nullable_string("client_host", KafkaString::decode(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode(buf)?.0)?;
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
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations: i32::MIN,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 3 (adds authorized_operations per group).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let group_state = non_nullable_string("group_state", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            let protocol_data = non_nullable_string("protocol_data", KafkaString::decode(buf)?.0)?;

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
                let client_id = non_nullable_string("client_id", KafkaString::decode(buf)?.0)?;
                let client_host = non_nullable_string("client_host", KafkaString::decode(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode(buf)?.0)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id: None,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 4 (adds group_instance_id per member).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let group_state = non_nullable_string("group_state", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            let protocol_data = non_nullable_string("protocol_data", KafkaString::decode(buf)?.0)?;

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
                let group_instance_id = KafkaString::decode(buf)?.0;
                let client_id = non_nullable_string("client_id", KafkaString::decode(buf)?.0)?;
                let client_host = non_nullable_string("client_host", KafkaString::decode(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode(buf)?.0)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 5 (flexible: compact strings, tagged fields, group_instance_id).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let protocol_data =
                non_nullable_string("protocol_data", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let group_instance_id = KafkaString::decode_compact(buf)?.0;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode_compact(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode_compact(buf)?.0)?;
                let _ = TaggedFields::decode(buf)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
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

    /// Decode from version 6 (adds error_message per group, KIP-1043).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let protocol_data =
                non_nullable_string("protocol_data", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let group_instance_id = KafkaString::decode_compact(buf)?.0;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode_compact(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode_compact(buf)?.0)?;
                let _ = TaggedFields::decode(buf)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
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

// ============================================================================
// ListGroups API (Key 16)
// ============================================================================

/// ListGroups request.
#[derive(Debug, Clone)]
pub struct ListGroupsRequest {
    /// State filter (v4+, KIP-518). Empty means all states.
    pub states_filter: Vec<String>,
    /// Type filter (v5+, KIP-848). Empty means all types.
    pub types_filter: Vec<String>,
}

impl ListGroupsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListGroups
    }

    /// Encode for version 1–2 (empty body).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        let _ = buf;
        Ok(())
    }

    /// Encode for version 3 (flexible, empty body + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 4 (flexible, adds states_filter).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = u32::try_from(self.states_filter.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("states_filter array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for state in &self.states_filter {
            KafkaString::new(state).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (flexible, adds types_filter).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        let states_len = u32::try_from(self.states_filter.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("states_filter array too large"))?;
        crate::util::varint::encode_unsigned_varint(states_len, buf);
        for state in &self.states_filter {
            KafkaString::new(state).try_encode_compact(buf)?;
        }
        let types_len = u32::try_from(self.types_filter.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("types_filter array too large"))?;
        crate::util::varint::encode_unsigned_varint(types_len, buf);
        for t in &self.types_filter {
            KafkaString::new(t).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
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
    /// Group state (v4+, KIP-518).
    pub group_state: Option<String>,
    /// Group type (v5+, KIP-848).
    pub group_type: Option<String>,
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
    /// Decode from version 1–2 (non-flexible).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: None,
                group_type: None,
            });
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    /// Decode from version 3 (flexible, no new fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: None,
                group_type: None,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    /// Decode from version 4 (adds group_state per group, KIP-518).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: Some(group_state),
                group_type: None,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    /// Decode from version 5 (adds group_type per group, KIP-848).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let group_type =
                non_nullable_string("group_type", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: Some(group_state),
                group_type: Some(group_type),
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }
}

impl VersionedEncode for DeleteGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DeleteGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("DeleteGroupsResponse", version),
        }
    }
}

impl VersionedEncode for DescribeClusterRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DescribeClusterRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeClusterResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("DescribeClusterResponse", version),
        }
    }
}

impl VersionedEncode for ConsumerGroupDescribeRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("ConsumerGroupDescribeRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ConsumerGroupDescribeResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("ConsumerGroupDescribeResponse", version),
        }
    }
}

impl VersionedEncode for ListClientMetricsResourcesRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("ListClientMetricsResourcesRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListClientMetricsResourcesResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("ListClientMetricsResourcesResponse", version),
        }
    }
}

impl VersionedEncode for DescribeGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 | 2 => self.encode_v1(buf)?,
            3 | 4 => self.encode_v3(buf)?,
            5 | 6 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("DescribeGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 | 2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            6 => Self::decode_v6(buf),
            _ => unsupported_decode!("DescribeGroupsResponse", version),
        }
    }
}

impl VersionedEncode for ListGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 | 2 => self.encode_v1(buf)?,
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("ListGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 | 2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("ListGroupsResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::varint;
    use bytes::BytesMut;

    // -----------------------------------------------------------------------
    // ConsumerGroupHeartbeat (API key 68, KIP-848)
    // -----------------------------------------------------------------------

    /// Helper: encode a compact string into `buf`.
    /// Non-null string: varint(len + 1) then bytes.
    /// Null string: varint(0).
    fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(val) => {
                // len + 1 fits in one byte for small strings
                buf.put_u8((val.len() + 1) as u8);
                buf.put_slice(val.as_bytes());
            }
            None => buf.put_u8(0),
        }
    }

    /// Helper: write empty tagged fields (varint 0).
    fn put_tagged_fields(buf: &mut BytesMut) {
        buf.put_u8(0);
    }

    #[test]
    fn test_describe_groups_request() {
        let request = DescribeGroupsRequest {
            groups: vec!["group-1".to_string(), "group-2".to_string()],
            include_authorized_operations: false,
        };
        assert_eq!(request.groups.len(), 2);
        assert_eq!(DescribeGroupsRequest::api_key(), ApiKey::DescribeGroups);

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_groups_response_decode_v1() {
        // Build a minimal v1 response: throttle_time_ms + 1 group with 0 members
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
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

        let response = DescribeGroupsResponse::decode_v1(&mut buf.freeze()).unwrap();
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
    fn test_describe_groups_request_encode_v3_authorized_ops() {
        let request = DescribeGroupsRequest {
            groups: vec!["my-group".to_string()],
            include_authorized_operations: true,
        };

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 adds the include_authorized_operations bool (1 byte)
        assert_eq!(buf_v3.len(), buf_v1.len() + 1);
    }

    #[test]
    fn test_describe_groups_request_encode_v5_flexible() {
        let request = DescribeGroupsRequest {
            groups: vec!["my-group".to_string()],
            include_authorized_operations: true,
        };

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        // v5 uses compact encoding (varint lengths instead of i32/i16)
        assert!(!buf_v5.is_empty());
        assert_ne!(buf_v3.len(), buf_v5.len());
    }

    #[test]
    fn test_describe_groups_response_decode_v3_authorized_ops() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(10); // throttle_time_ms
        buf.put_i32(1); // group count

        buf.put_i16(0); // error_code
        let gid = b"test-group";
        buf.put_i16(gid.len() as i16);
        buf.put_slice(gid);
        let state = b"Stable";
        buf.put_i16(state.len() as i16);
        buf.put_slice(state);
        let ptype = b"consumer";
        buf.put_i16(ptype.len() as i16);
        buf.put_slice(ptype);
        let pdata = b"range";
        buf.put_i16(pdata.len() as i16);
        buf.put_slice(pdata);
        buf.put_i32(0); // member count
        buf.put_i32(0x0000_001F); // authorized_operations

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v3(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, "test-group");
        assert_eq!(resp.groups[0].authorized_operations, 0x0000_001F);
        assert_eq!(resp.groups[0].error_message, None);
    }

    #[test]
    fn test_describe_groups_response_decode_v4_instance_id() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // group count

        buf.put_i16(0); // error_code
        let gid = b"grp";
        buf.put_i16(gid.len() as i16);
        buf.put_slice(gid);
        let state = b"Stable";
        buf.put_i16(state.len() as i16);
        buf.put_slice(state);
        let ptype = b"consumer";
        buf.put_i16(ptype.len() as i16);
        buf.put_slice(ptype);
        let pdata = b"range";
        buf.put_i16(pdata.len() as i16);
        buf.put_slice(pdata);

        // 1 member
        buf.put_i32(1);
        let mid = b"member-1";
        buf.put_i16(mid.len() as i16);
        buf.put_slice(mid);
        // group_instance_id (v4+): "inst-1"
        let inst = b"inst-1";
        buf.put_i16(inst.len() as i16);
        buf.put_slice(inst);
        let cid = b"client-1";
        buf.put_i16(cid.len() as i16);
        buf.put_slice(cid);
        let host = b"/10.0.0.1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(0); // member_metadata (empty bytes)
        buf.put_i32(0); // member_assignment (empty bytes)

        buf.put_i32(i32::MIN); // authorized_operations (not requested)

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v4(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].members.len(), 1);
        assert_eq!(
            resp.groups[0].members[0].group_instance_id,
            Some("inst-1".to_string())
        );
        assert_eq!(resp.groups[0].members[0].client_id, "client-1");
        assert_eq!(resp.groups[0].members[0].client_host, "/10.0.0.1");
        assert_eq!(resp.groups[0].authorized_operations, i32::MIN);
    }

    #[test]
    fn test_describe_groups_response_decode_v5_flexible() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(5); // throttle_time_ms

        // groups compact array: 1 group → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            buf.put_i16(0); // error_code

            // group_id (compact): "grp"
            crate::util::varint::encode_unsigned_varint(4, &mut buf);
            buf.put_slice(b"grp");
            // group_state (compact): "Empty"
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"Empty");
            // protocol_type (compact): "consumer"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            // protocol_data (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            // members compact array: 0 → varint(1)
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            buf.put_i32(0x0F); // authorized_operations
            // group tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v5(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, "grp");
        assert_eq!(resp.groups[0].group_state, "Empty");
        assert_eq!(resp.groups[0].error_message, None);
        assert_eq!(resp.groups[0].authorized_operations, 0x0F);
        assert!(resp.groups[0].members.is_empty());
    }

    #[test]
    fn test_describe_groups_response_decode_v6_error_message() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms

        // groups compact array: 1 group → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            buf.put_i16(69); // error_code (GROUP_ID_NOT_FOUND)

            // error_message (compact nullable): "group not found"
            let msg = b"group not found";
            crate::util::varint::encode_unsigned_varint((msg.len() + 1) as u32, &mut buf);
            buf.put_slice(msg);

            // group_id (compact): "missing-grp"
            let gid = b"missing-grp";
            crate::util::varint::encode_unsigned_varint((gid.len() + 1) as u32, &mut buf);
            buf.put_slice(gid);
            // group_state (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);
            // protocol_type (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);
            // protocol_data (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            // members compact array: 0 → varint(1)
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            buf.put_i32(i32::MIN); // authorized_operations
            // group tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v6(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 1);
        assert!(!resp.groups[0].error_code.is_ok());
        assert_eq!(
            resp.groups[0].error_message,
            Some("group not found".to_string())
        );
        assert_eq!(resp.groups[0].group_id, "missing-grp");
    }

    #[test]
    fn test_list_groups_request() {
        let request = ListGroupsRequest {
            states_filter: Vec::new(),
            types_filter: Vec::new(),
        };
        assert_eq!(ListGroupsRequest::api_key(), ApiKey::ListGroups);
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // ListGroups v1 has an empty body
        assert!(buf.is_empty());
    }

    #[test]
    fn test_list_groups_response_decode_v1() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
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

        let response = ListGroupsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert!(response.error_code.is_ok());
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].group_id, "group-a");
        assert_eq!(response.groups[1].group_id, "group-b");
        assert_eq!(response.groups[0].group_state, None);
        assert_eq!(response.groups[0].group_type, None);
    }

    #[test]
    fn test_list_groups_request_encode_v3_flexible() {
        let request = ListGroupsRequest {
            states_filter: Vec::new(),
            types_filter: Vec::new(),
        };

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();
        assert!(buf_v1.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 adds tagged fields (just 0x00 for empty)
        assert_eq!(buf_v3.len(), 1);
        assert_eq!(buf_v3[0], 0x00);
    }

    #[test]
    fn test_list_groups_request_encode_v4_states_filter() {
        let request = ListGroupsRequest {
            states_filter: vec!["Stable".to_string(), "Empty".to_string()],
            types_filter: Vec::new(),
        };

        let mut buf = BytesMut::new();
        request.encode_v4(&mut buf).unwrap();

        // Should contain the state filter strings
        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("Stable"));
        assert!(data.contains("Empty"));
    }

    #[test]
    fn test_list_groups_request_encode_v5_types_filter() {
        let request = ListGroupsRequest {
            states_filter: vec!["Stable".to_string()],
            types_filter: vec!["classic".to_string(), "consumer".to_string()],
        };

        let mut buf = BytesMut::new();
        request.encode_v5(&mut buf).unwrap();

        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("Stable"));
        assert!(data.contains("classic"));
        assert!(data.contains("consumer"));
    }

    #[test]
    fn test_list_groups_response_decode_v3_flexible() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code

        // groups compact array: 1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // group_id: "grp-1"
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-1");
            // protocol_type: "consumer"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = ListGroupsResponse::decode_v3(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, "grp-1");
        assert_eq!(resp.groups[0].protocol_type, "consumer");
        assert_eq!(resp.groups[0].group_state, None);
        assert_eq!(resp.groups[0].group_type, None);
    }

    #[test]
    fn test_list_groups_response_decode_v4_group_state() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code

        // groups compact array: 1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // group_id: "grp-1"
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-1");
            // protocol_type: "consumer"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            // group_state: "Stable"
            crate::util::varint::encode_unsigned_varint(7, &mut buf);
            buf.put_slice(b"Stable");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = ListGroupsResponse::decode_v4(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_state, Some("Stable".to_string()));
        assert_eq!(resp.groups[0].group_type, None);
    }

    #[test]
    fn test_list_groups_response_decode_v5_group_type() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code

        // groups compact array: 2 → varint(3)
        crate::util::varint::encode_unsigned_varint(3, &mut buf);
        {
            // group 1
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-1");
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            crate::util::varint::encode_unsigned_varint(7, &mut buf);
            buf.put_slice(b"Stable");
            crate::util::varint::encode_unsigned_varint(8, &mut buf);
            buf.put_slice(b"classic");
            crate::util::varint::encode_unsigned_varint(0, &mut buf);

            // group 2
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-2");
            crate::util::varint::encode_unsigned_varint(1, &mut buf); // empty protocol_type
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"Empty");
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = ListGroupsResponse::decode_v5(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 2);
        assert_eq!(resp.groups[0].group_id, "grp-1");
        assert_eq!(resp.groups[0].group_state, Some("Stable".to_string()));
        assert_eq!(resp.groups[0].group_type, Some("classic".to_string()));
        assert_eq!(resp.groups[1].group_id, "grp-2");
        assert_eq!(resp.groups[1].group_state, Some("Empty".to_string()));
        assert_eq!(resp.groups[1].group_type, Some("consumer".to_string()));
    }

    // ── DeleteGroups v0 (non-flexible) / v2 (flexible) ──

    #[test]
    fn test_delete_groups_request_encode_v0_round_trip() {
        let req = DeleteGroupsRequest::new(vec!["g1".to_string(), "g2".to_string()]);
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), 2); // 2 groups
        assert_eq!(cur.get_i16(), 2);
        let mut g = vec![0u8; 2];
        cur.copy_to_slice(&mut g);
        assert_eq!(g, b"g1");
        assert_eq!(cur.get_i16(), 2);
        cur.copy_to_slice(&mut g);
        assert_eq!(g, b"g2");
        assert!(cur.is_empty());
    }

    #[test]
    fn test_delete_groups_request_encode_v2_flexible() {
        let req = DeleteGroupsRequest::new(vec!["grp".to_string()]);
        let mut buf = BytesMut::new();
        req.encode_v2(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr, 2); // 1 + 1
        let name_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_v, 4); // len("grp") + 1
        let mut g = vec![0u8; 3];
        cur.copy_to_slice(&mut g);
        assert_eq!(g, b"grp");
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_delete_groups_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(20); // throttle_time_ms
        buf.put_i32(2); // 2 results
        buf.put_i16(3);
        buf.put_slice(b"ga1");
        buf.put_i16(0); // NONE
        buf.put_i16(2);
        buf.put_slice(b"gb");
        buf.put_i16(69); // NON_EMPTY_GROUP

        let resp = DeleteGroupsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 20);
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.results[0].group_id, "ga1");
        assert!(resp.results[0].error_code.is_ok());
        assert_eq!(resp.results[1].group_id, "gb");
        assert!(!resp.results[1].error_code.is_ok());
    }

    #[test]
    fn test_delete_groups_response_decode_v2_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 result
        put_compact_string(&mut buf, Some("mygroup"));
        buf.put_i16(0); // NONE
        put_tagged_fields(&mut buf); // result tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DeleteGroupsResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].group_id, "mygroup");
        assert!(resp.results[0].error_code.is_ok());
    }

    // ── DescribeCluster v0 / v1 / v2 ──

    #[test]
    fn test_describe_cluster_request_encode_v0() {
        let req = DescribeClusterRequest {
            include_cluster_authorized_operations: true,
            endpoint_type: 1,
            include_fenced_brokers: false,
        };
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 1); // include_cluster_authorized_operations
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_cluster_request_encode_v1_endpoint_type() {
        let req = DescribeClusterRequest {
            include_cluster_authorized_operations: false,
            endpoint_type: 2,
            include_fenced_brokers: false,
        };
        let mut buf = BytesMut::new();
        req.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 0); // include_cluster_authorized_operations
        assert_eq!(cur.get_i8(), 2); // endpoint_type
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_cluster_request_encode_v2_fenced() {
        let req = DescribeClusterRequest {
            include_cluster_authorized_operations: true,
            endpoint_type: 1,
            include_fenced_brokers: true,
        };
        let mut buf = BytesMut::new();
        req.encode_v2(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 1);
        assert_eq!(cur.get_i8(), 1);
        assert_eq!(cur.get_u8(), 1); // include_fenced_brokers
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_cluster_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message null
        put_compact_string(&mut buf, Some("cluster-1")); // cluster_id
        buf.put_i32(0); // controller_id
        varint::encode_unsigned_varint(2, &mut buf); // 1 broker
        buf.put_i32(0); // broker_id
        put_compact_string(&mut buf, Some("host-0")); // host
        buf.put_i32(9092); // port
        put_compact_string(&mut buf, Some("rack-a")); // rack
        put_tagged_fields(&mut buf); // broker tagged fields
        buf.put_i32(-2_147_483_648); // cluster_authorized_operations
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeClusterResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.cluster_id, "cluster-1");
        assert_eq!(resp.controller_id, 0);
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].broker_id, 0);
        assert_eq!(resp.brokers[0].host, "host-0");
        assert_eq!(resp.brokers[0].port, 9092);
        assert_eq!(resp.brokers[0].rack.as_deref(), Some("rack-a"));
        assert!(!resp.brokers[0].is_fenced); // default false in v0
        assert_eq!(resp.endpoint_type, 1); // default for v0
    }

    #[test]
    fn test_describe_cluster_response_decode_v1_endpoint_type() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // null error_message
        buf.put_i8(2); // endpoint_type = 2 (controllers)
        put_compact_string(&mut buf, Some("c")); // cluster_id
        buf.put_i32(1); // controller_id
        varint::encode_unsigned_varint(1, &mut buf); // 0 brokers
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeClusterResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.endpoint_type, 2);
        assert_eq!(resp.cluster_id, "c");
        assert!(resp.brokers.is_empty());
    }

    #[test]
    fn test_describe_cluster_response_decode_v2_is_fenced() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // null error_message
        buf.put_i8(1); // endpoint_type
        put_compact_string(&mut buf, Some("c")); // cluster_id
        buf.put_i32(0); // controller_id
        varint::encode_unsigned_varint(3, &mut buf); // 2 brokers
        // broker 0: not fenced
        buf.put_i32(0);
        put_compact_string(&mut buf, Some("h0"));
        buf.put_i32(9092);
        put_compact_string(&mut buf, None); // rack null
        buf.put_i8(0); // is_fenced = false
        put_tagged_fields(&mut buf);
        // broker 1: fenced
        buf.put_i32(1);
        put_compact_string(&mut buf, Some("h1"));
        buf.put_i32(9093);
        put_compact_string(&mut buf, None);
        buf.put_i8(1); // is_fenced = true
        put_tagged_fields(&mut buf);
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf);

        let resp = DescribeClusterResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.brokers.len(), 2);
        assert!(!resp.brokers[0].is_fenced);
        assert!(resp.brokers[1].is_fenced);
    }

    // ── ConsumerGroupDescribe v0 / v1 ──

    #[test]
    fn test_consumer_group_describe_request_encode_v0() {
        let req = ConsumerGroupDescribeRequest::new(vec!["g1".to_string()]);
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr, 2); // 1 + 1
        let id_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(id_len, 3); // len("g1") + 1
        let mut id_bytes = vec![0u8; 2];
        cur.copy_to_slice(&mut id_bytes);
        assert_eq!(id_bytes, b"g1");
        assert_eq!(cur.get_u8(), 0); // include_authorized_operations = false
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_consumer_group_describe_response_decode_v0_empty_group() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message null
        put_compact_string(&mut buf, Some("g1")); // group_id
        put_compact_string(&mut buf, Some("Stable")); // group_state
        buf.put_i32(1); // group_epoch
        buf.put_i32(1); // assignment_epoch
        put_compact_string(&mut buf, Some("range")); // assignor_name
        varint::encode_unsigned_varint(1, &mut buf); // 0 members
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf); // group tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ConsumerGroupDescribeResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.groups.len(), 1);
        let g = &resp.groups[0];
        assert!(g.error_code.is_ok());
        assert_eq!(g.group_id, "g1");
        assert_eq!(g.group_state, "Stable");
        assert_eq!(g.group_epoch, 1);
        assert_eq!(g.assignor_name, "range");
        assert!(g.members.is_empty());
    }

    #[test]
    fn test_consumer_group_describe_response_decode_v0_with_member() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message
        put_compact_string(&mut buf, Some("g")); // group_id
        put_compact_string(&mut buf, Some("S")); // group_state
        buf.put_i32(2); // group_epoch
        buf.put_i32(2); // assignment_epoch
        put_compact_string(&mut buf, Some("a")); // assignor_name
        varint::encode_unsigned_varint(2, &mut buf); // 1 member
        // member fields
        put_compact_string(&mut buf, Some("m1")); // member_id
        put_compact_string(&mut buf, None); // instance_id null
        put_compact_string(&mut buf, Some("rack0")); // rack_id
        buf.put_i32(3); // member_epoch
        put_compact_string(&mut buf, Some("client")); // client_id
        put_compact_string(&mut buf, Some("/127.0.0.1")); // client_host
        varint::encode_unsigned_varint(2, &mut buf); // 1 subscribed_topic_name
        put_compact_string(&mut buf, Some("tp1"));
        put_compact_string(&mut buf, None); // subscribed_topic_regex null
        // assignment (current): 1 topic_partition
        varint::encode_unsigned_varint(2, &mut buf); // 1 tp
        buf.put_slice(&[0u8; 16]); // topic_id
        put_compact_string(&mut buf, Some("tp1")); // topic_name
        varint::encode_unsigned_varint(3, &mut buf); // 2 partitions
        buf.put_i32(0);
        buf.put_i32(1);
        put_tagged_fields(&mut buf); // tp tagged fields
        put_tagged_fields(&mut buf); // assignment tagged fields
        // target_assignment: empty
        varint::encode_unsigned_varint(1, &mut buf); // 0 tp
        put_tagged_fields(&mut buf); // target assignment tagged fields
        put_tagged_fields(&mut buf); // member tagged fields
        buf.put_i32(-2_147_483_648); // authorized_operations
        put_tagged_fields(&mut buf); // group tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ConsumerGroupDescribeResponse::decode_v0(&mut buf.freeze()).unwrap();
        let g = &resp.groups[0];
        assert_eq!(g.members.len(), 1);
        let m = &g.members[0];
        assert_eq!(m.member_id, "m1");
        assert!(m.instance_id.is_none());
        assert_eq!(m.rack_id.as_deref(), Some("rack0"));
        assert_eq!(m.member_epoch, 3);
        assert_eq!(m.subscribed_topic_names, vec!["tp1"]);
        assert_eq!(m.assignment.topic_partitions.len(), 1);
        assert_eq!(m.assignment.topic_partitions[0].partitions, vec![0, 1]);
        assert!(m.target_assignment.topic_partitions.is_empty());
        assert_eq!(m.member_type, -1); // v0 default
    }

    #[test]
    fn test_consumer_group_describe_response_decode_v1_member_type() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message
        put_compact_string(&mut buf, Some("g")); // group_id
        put_compact_string(&mut buf, Some("S")); // state
        buf.put_i32(1);
        buf.put_i32(1); // epochs
        put_compact_string(&mut buf, Some("a")); // assignor
        varint::encode_unsigned_varint(2, &mut buf); // 1 member
        put_compact_string(&mut buf, Some("m")); // member_id
        put_compact_string(&mut buf, None); // instance_id
        put_compact_string(&mut buf, None); // rack_id
        buf.put_i32(1); // member_epoch
        put_compact_string(&mut buf, Some("c")); // client_id
        put_compact_string(&mut buf, Some("h")); // client_host
        varint::encode_unsigned_varint(1, &mut buf); // 0 subscribed topics
        put_compact_string(&mut buf, None); // subscribed_topic_regex
        // current assignment: empty
        varint::encode_unsigned_varint(1, &mut buf);
        put_tagged_fields(&mut buf);
        // target assignment: empty
        varint::encode_unsigned_varint(1, &mut buf);
        put_tagged_fields(&mut buf);
        buf.put_i8(1); // member_type = consumer (v1 field)
        put_tagged_fields(&mut buf); // member tagged fields
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf); // group tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ConsumerGroupDescribeResponse::decode_v1(&mut buf.freeze()).unwrap();
        let m = &resp.groups[0].members[0];
        assert_eq!(m.member_type, 1); // consumer
    }

    // ── ListClientMetricsResources v0 ──

    #[test]
    fn test_list_client_metrics_resources_request_encode_v0() {
        let req = ListClientMetricsResourcesRequest;
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 0); // tagged fields only
        assert!(cur.is_empty());
    }

    #[test]
    fn test_list_client_metrics_resources_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(3, &mut buf); // 2 resources
        put_compact_string(&mut buf, Some("metric-a"));
        put_tagged_fields(&mut buf); // resource tagged fields
        put_compact_string(&mut buf, Some("metric-b"));
        put_tagged_fields(&mut buf); // resource tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ListClientMetricsResourcesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.client_metrics_resources.len(), 2);
        assert_eq!(resp.client_metrics_resources[0].name, "metric-a");
        assert_eq!(resp.client_metrics_resources[1].name, "metric-b");
    }

    #[test]
    fn test_list_client_metrics_resources_response_decode_v0_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(1, &mut buf); // 0 resources
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ListClientMetricsResourcesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.client_metrics_resources.is_empty());
    }
}
