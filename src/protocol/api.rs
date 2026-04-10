//! Kafka API keys and version negotiation.
//!
//! This module defines all Kafka API keys and provides version negotiation support.

use bytes::{Buf, BufMut};

use super::primitives::{Decode, Encode, KafkaArray, KafkaString, TaggedFields, TryEncode};
use crate::error::Result;

/// Maximum number of supported features we'll accept from a broker.
const MAX_SUPPORTED_FEATURES: usize = 256;

/// Kafka API keys.
///
/// Each API key corresponds to a specific request/response pair in the Kafka protocol.
/// Forward compatibility is provided by the `Unknown(i16)` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
pub enum ApiKey {
    /// Produce messages to topics.
    Produce = 0,
    /// Fetch messages from topics.
    Fetch = 1,
    /// List offsets for partitions.
    ListOffsets = 2,
    /// Get cluster metadata.
    Metadata = 3,
    /// Leader and ISR updates (internal).
    LeaderAndIsr = 4,
    /// Stop replica (internal).
    StopReplica = 5,
    /// Update metadata (internal).
    UpdateMetadata = 6,
    /// Controlled shutdown (internal).
    ControlledShutdown = 7,
    /// Commit offsets.
    OffsetCommit = 8,
    /// Fetch committed offsets.
    OffsetFetch = 9,
    /// Find group coordinator.
    FindCoordinator = 10,
    /// Join consumer group.
    JoinGroup = 11,
    /// Send heartbeat.
    Heartbeat = 12,
    /// Leave consumer group.
    LeaveGroup = 13,
    /// Sync consumer group.
    SyncGroup = 14,
    /// Describe groups.
    DescribeGroups = 15,
    /// List groups.
    ListGroups = 16,
    /// SASL handshake.
    SaslHandshake = 17,
    /// Get API versions.
    ApiVersions = 18,
    /// Create topics.
    CreateTopics = 19,
    /// Delete topics.
    DeleteTopics = 20,
    /// Delete records.
    DeleteRecords = 21,
    /// Init producer ID.
    InitProducerId = 22,
    /// Offset for leader epoch.
    OffsetForLeaderEpoch = 23,
    /// Add partitions to txn.
    AddPartitionsToTxn = 24,
    /// Add offsets to txn.
    AddOffsetsToTxn = 25,
    /// End txn.
    EndTxn = 26,
    /// Write txn markers.
    WriteTxnMarkers = 27,
    /// Describe txn coordinator.
    TxnOffsetCommit = 28,
    /// Describe ACLs.
    DescribeAcls = 29,
    /// Create ACLs.
    CreateAcls = 30,
    /// Delete ACLs.
    DeleteAcls = 31,
    /// Describe configs.
    DescribeConfigs = 32,
    /// Alter configs.
    AlterConfigs = 33,
    /// Alter replica log dirs.
    AlterReplicaLogDirs = 34,
    /// Describe log dirs.
    DescribeLogDirs = 35,
    /// SASL authenticate.
    SaslAuthenticate = 36,
    /// Create partitions.
    CreatePartitions = 37,
    /// Create delegation token.
    CreateDelegationToken = 38,
    /// Renew delegation token.
    RenewDelegationToken = 39,
    /// Expire delegation token.
    ExpireDelegationToken = 40,
    /// Describe delegation token.
    DescribeDelegationToken = 41,
    /// Delete groups.
    DeleteGroups = 42,
    /// Elect leaders.
    ElectLeaders = 43,
    /// Incremental alter configs.
    IncrementalAlterConfigs = 44,
    /// Alter partition reassignments.
    AlterPartitionReassignments = 45,
    /// List partition reassignments.
    ListPartitionReassignments = 46,
    /// Offset delete.
    OffsetDelete = 47,
    /// Describe client quotas.
    DescribeClientQuotas = 48,
    /// Alter client quotas.
    AlterClientQuotas = 49,
    /// Describe user SCRAM credentials.
    DescribeUserScramCredentials = 50,
    /// Alter user SCRAM credentials.
    AlterUserScramCredentials = 51,
    /// Vote (KRaft).
    Vote = 52,
    /// Begin quorum epoch (KRaft).
    BeginQuorumEpoch = 53,
    /// End quorum epoch (KRaft).
    EndQuorumEpoch = 54,
    /// Describe quorum (KRaft).
    DescribeQuorum = 55,
    /// Alter partition.
    AlterPartition = 56,
    /// Update features.
    UpdateFeatures = 57,
    /// Envelope (internal).
    Envelope = 58,
    /// Fetch snapshot (KRaft).
    FetchSnapshot = 59,
    /// Describe cluster.
    DescribeCluster = 60,
    /// Describe producers.
    DescribeProducers = 61,
    /// Broker registration (KRaft).
    BrokerRegistration = 62,
    /// Broker heartbeat (KRaft).
    BrokerHeartbeat = 63,
    /// Unregister broker (KRaft).
    UnregisterBroker = 64,
    /// Describe transactions.
    DescribeTransactions = 65,
    /// List transactions.
    ListTransactions = 66,
    /// Allocate producer IDs.
    AllocateProducerIds = 67,
    /// Consumer group heartbeat.
    ConsumerGroupHeartbeat = 68,
    /// Consumer group describe (KIP-848).
    ConsumerGroupDescribe = 69,
    /// Get telemetry subscriptions (KIP-714).
    GetTelemetrySubscriptions = 71,
    /// Push telemetry (KIP-714).
    PushTelemetry = 72,
    /// List client metrics resources (KIP-714).
    ListClientMetricsResources = 74,
    /// Describe topic partitions (KIP-966).
    DescribeTopicPartitions = 75,
    /// Share group heartbeat (KIP-932).
    ShareGroupHeartbeat = 76,
    /// Share group describe (KIP-932).
    ShareGroupDescribe = 77,
    /// Share fetch (KIP-932).
    ShareFetch = 78,
    /// Share acknowledge (KIP-932).
    ShareAcknowledge = 79,
    /// Unknown API key.
    Unknown(i16),
}

impl ApiKey {
    /// Create an ApiKey from a raw i16 value.
    #[inline]
    pub fn from_i16(key: i16) -> Self {
        match key {
            0 => Self::Produce,
            1 => Self::Fetch,
            2 => Self::ListOffsets,
            3 => Self::Metadata,
            4 => Self::LeaderAndIsr,
            5 => Self::StopReplica,
            6 => Self::UpdateMetadata,
            7 => Self::ControlledShutdown,
            8 => Self::OffsetCommit,
            9 => Self::OffsetFetch,
            10 => Self::FindCoordinator,
            11 => Self::JoinGroup,
            12 => Self::Heartbeat,
            13 => Self::LeaveGroup,
            14 => Self::SyncGroup,
            15 => Self::DescribeGroups,
            16 => Self::ListGroups,
            17 => Self::SaslHandshake,
            18 => Self::ApiVersions,
            19 => Self::CreateTopics,
            20 => Self::DeleteTopics,
            21 => Self::DeleteRecords,
            22 => Self::InitProducerId,
            23 => Self::OffsetForLeaderEpoch,
            24 => Self::AddPartitionsToTxn,
            25 => Self::AddOffsetsToTxn,
            26 => Self::EndTxn,
            27 => Self::WriteTxnMarkers,
            28 => Self::TxnOffsetCommit,
            29 => Self::DescribeAcls,
            30 => Self::CreateAcls,
            31 => Self::DeleteAcls,
            32 => Self::DescribeConfigs,
            33 => Self::AlterConfigs,
            34 => Self::AlterReplicaLogDirs,
            35 => Self::DescribeLogDirs,
            36 => Self::SaslAuthenticate,
            37 => Self::CreatePartitions,
            38 => Self::CreateDelegationToken,
            39 => Self::RenewDelegationToken,
            40 => Self::ExpireDelegationToken,
            41 => Self::DescribeDelegationToken,
            42 => Self::DeleteGroups,
            43 => Self::ElectLeaders,
            44 => Self::IncrementalAlterConfigs,
            45 => Self::AlterPartitionReassignments,
            46 => Self::ListPartitionReassignments,
            47 => Self::OffsetDelete,
            48 => Self::DescribeClientQuotas,
            49 => Self::AlterClientQuotas,
            50 => Self::DescribeUserScramCredentials,
            51 => Self::AlterUserScramCredentials,
            52 => Self::Vote,
            53 => Self::BeginQuorumEpoch,
            54 => Self::EndQuorumEpoch,
            55 => Self::DescribeQuorum,
            56 => Self::AlterPartition,
            57 => Self::UpdateFeatures,
            58 => Self::Envelope,
            59 => Self::FetchSnapshot,
            60 => Self::DescribeCluster,
            61 => Self::DescribeProducers,
            62 => Self::BrokerRegistration,
            63 => Self::BrokerHeartbeat,
            64 => Self::UnregisterBroker,
            65 => Self::DescribeTransactions,
            66 => Self::ListTransactions,
            67 => Self::AllocateProducerIds,
            68 => Self::ConsumerGroupHeartbeat,
            69 => Self::ConsumerGroupDescribe,
            71 => Self::GetTelemetrySubscriptions,
            72 => Self::PushTelemetry,
            74 => Self::ListClientMetricsResources,
            75 => Self::DescribeTopicPartitions,
            76 => Self::ShareGroupHeartbeat,
            77 => Self::ShareGroupDescribe,
            78 => Self::ShareFetch,
            79 => Self::ShareAcknowledge,
            other => Self::Unknown(other),
        }
    }

    /// Convert the ApiKey to its raw i16 value.
    #[inline]
    pub fn to_i16(self) -> i16 {
        match self {
            Self::Produce => 0,
            Self::Fetch => 1,
            Self::ListOffsets => 2,
            Self::Metadata => 3,
            Self::LeaderAndIsr => 4,
            Self::StopReplica => 5,
            Self::UpdateMetadata => 6,
            Self::ControlledShutdown => 7,
            Self::OffsetCommit => 8,
            Self::OffsetFetch => 9,
            Self::FindCoordinator => 10,
            Self::JoinGroup => 11,
            Self::Heartbeat => 12,
            Self::LeaveGroup => 13,
            Self::SyncGroup => 14,
            Self::DescribeGroups => 15,
            Self::ListGroups => 16,
            Self::SaslHandshake => 17,
            Self::ApiVersions => 18,
            Self::CreateTopics => 19,
            Self::DeleteTopics => 20,
            Self::DeleteRecords => 21,
            Self::InitProducerId => 22,
            Self::OffsetForLeaderEpoch => 23,
            Self::AddPartitionsToTxn => 24,
            Self::AddOffsetsToTxn => 25,
            Self::EndTxn => 26,
            Self::WriteTxnMarkers => 27,
            Self::TxnOffsetCommit => 28,
            Self::DescribeAcls => 29,
            Self::CreateAcls => 30,
            Self::DeleteAcls => 31,
            Self::DescribeConfigs => 32,
            Self::AlterConfigs => 33,
            Self::AlterReplicaLogDirs => 34,
            Self::DescribeLogDirs => 35,
            Self::SaslAuthenticate => 36,
            Self::CreatePartitions => 37,
            Self::CreateDelegationToken => 38,
            Self::RenewDelegationToken => 39,
            Self::ExpireDelegationToken => 40,
            Self::DescribeDelegationToken => 41,
            Self::DeleteGroups => 42,
            Self::ElectLeaders => 43,
            Self::IncrementalAlterConfigs => 44,
            Self::AlterPartitionReassignments => 45,
            Self::ListPartitionReassignments => 46,
            Self::OffsetDelete => 47,
            Self::DescribeClientQuotas => 48,
            Self::AlterClientQuotas => 49,
            Self::DescribeUserScramCredentials => 50,
            Self::AlterUserScramCredentials => 51,
            Self::Vote => 52,
            Self::BeginQuorumEpoch => 53,
            Self::EndQuorumEpoch => 54,
            Self::DescribeQuorum => 55,
            Self::AlterPartition => 56,
            Self::UpdateFeatures => 57,
            Self::Envelope => 58,
            Self::FetchSnapshot => 59,
            Self::DescribeCluster => 60,
            Self::DescribeProducers => 61,
            Self::BrokerRegistration => 62,
            Self::BrokerHeartbeat => 63,
            Self::UnregisterBroker => 64,
            Self::DescribeTransactions => 65,
            Self::ListTransactions => 66,
            Self::AllocateProducerIds => 67,
            Self::ConsumerGroupHeartbeat => 68,
            Self::ConsumerGroupDescribe => 69,
            Self::GetTelemetrySubscriptions => 71,
            Self::PushTelemetry => 72,
            Self::ListClientMetricsResources => 74,
            Self::DescribeTopicPartitions => 75,
            Self::ShareGroupHeartbeat => 76,
            Self::ShareGroupDescribe => 77,
            Self::ShareFetch => 78,
            Self::ShareAcknowledge => 79,
            Self::Unknown(key) => key,
        }
    }

    /// Return the minimum API version at which this API key uses flexible
    /// encoding (compact strings + tagged fields in headers and payloads).
    ///
    /// Values sourced from the Apache Kafka protocol JSON schemas.
    /// Returns `i16::MAX` when the API never uses flexible encoding.
    pub fn flexible_version(self) -> i16 {
        match self {
            Self::Produce => 9,
            Self::Fetch => 12,
            Self::ListOffsets => 6,
            Self::Metadata => 9,
            Self::LeaderAndIsr => 4,
            Self::StopReplica => 2,
            Self::UpdateMetadata => 6,
            Self::ControlledShutdown => 3,
            Self::OffsetCommit => 8,
            Self::OffsetFetch => 6,
            Self::FindCoordinator => 3,
            Self::JoinGroup => 6,
            Self::Heartbeat => 4,
            Self::LeaveGroup => 4,
            Self::SyncGroup => 4,
            Self::DescribeGroups => 5,
            Self::ListGroups => 3,
            Self::SaslHandshake => i16::MAX,
            Self::ApiVersions => 3,
            Self::CreateTopics => 5,
            Self::DeleteTopics => 4,
            Self::DeleteRecords => 2,
            Self::InitProducerId => 2,
            Self::OffsetForLeaderEpoch => 4,
            Self::AddPartitionsToTxn => 4,
            Self::AddOffsetsToTxn => 3,
            Self::EndTxn => 3,
            Self::WriteTxnMarkers => 1,
            Self::TxnOffsetCommit => 3,
            Self::DescribeAcls => 2,
            Self::CreateAcls => 2,
            Self::DeleteAcls => 2,
            Self::DescribeConfigs => 4,
            Self::AlterConfigs => 2,
            Self::AlterReplicaLogDirs => 2,
            Self::DescribeLogDirs => 2,
            Self::SaslAuthenticate => 2,
            Self::CreatePartitions => 2,
            Self::CreateDelegationToken => 2,
            Self::RenewDelegationToken => 2,
            Self::ExpireDelegationToken => 2,
            Self::DescribeDelegationToken => 2,
            Self::DeleteGroups => 2,
            Self::ElectLeaders => 2,
            Self::IncrementalAlterConfigs => 1,
            Self::AlterPartitionReassignments => 0,
            Self::ListPartitionReassignments => 0,
            Self::OffsetDelete => i16::MAX,
            Self::DescribeClientQuotas => 1,
            Self::AlterClientQuotas => 1,
            Self::DescribeUserScramCredentials => 0,
            Self::AlterUserScramCredentials => 0,
            Self::Vote => 0,
            Self::BeginQuorumEpoch => 0,
            Self::EndQuorumEpoch => 0,
            Self::DescribeQuorum => 0,
            Self::AlterPartition => 0,
            Self::UpdateFeatures => 0,
            Self::Envelope => 0,
            Self::FetchSnapshot => 0,
            Self::DescribeCluster => 0,
            Self::DescribeProducers => 0,
            Self::BrokerRegistration => 0,
            Self::BrokerHeartbeat => 0,
            Self::UnregisterBroker => 0,
            Self::DescribeTransactions => 0,
            Self::ListTransactions => 0,
            Self::AllocateProducerIds => 0,
            Self::ConsumerGroupHeartbeat => 0,
            Self::ConsumerGroupDescribe => 0,
            Self::GetTelemetrySubscriptions => 0,
            Self::PushTelemetry => 0,
            Self::ListClientMetricsResources => 0,
            Self::DescribeTopicPartitions => 0,
            Self::ShareGroupHeartbeat => 0,
            Self::ShareGroupDescribe => 0,
            Self::ShareFetch => 0,
            Self::ShareAcknowledge => 0,
            // Unknown APIs: assume never flexible (safest default).
            Self::Unknown(_) => i16::MAX,
        }
    }
}

impl From<i16> for ApiKey {
    fn from(key: i16) -> Self {
        Self::from_i16(key)
    }
}

impl std::fmt::Display for ApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown(key) => write!(f, "Unknown({key})"),
            other => std::fmt::Debug::fmt(other, f),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        key.to_i16()
    }
}

impl Encode for ApiKey {
    fn encode(&self, buf: &mut impl BufMut) {
        self.to_i16().encode(buf);
    }
}

impl TryEncode for ApiKey {
    #[inline]
    fn try_encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode(buf);
        Ok(())
    }
}

impl Decode for ApiKey {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self::from_i16(i16::decode(buf)?))
    }
}

/// API version range for a specific API key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    /// The API key.
    pub api_key: ApiKey,
    /// Minimum supported version.
    pub min_version: i16,
    /// Maximum supported version.
    pub max_version: i16,
}

impl ApiVersionRange {
    /// Create a new API version range.
    pub fn new(api_key: ApiKey, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
        }
    }

    /// Check if a specific version is supported.
    pub fn supports(&self, version: i16) -> bool {
        version >= self.min_version && version <= self.max_version
    }

    /// Negotiate the best version between client and broker.
    ///
    /// Returns the highest mutually supported version, or None if no overlap.
    ///
    /// # Arguments
    ///
    /// * `client_max` - The maximum version the client supports
    /// * `client_min` - The minimum version the client supports (default 0)
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::protocol::{ApiKey, ApiVersionRange};
    ///
    /// let broker_range = ApiVersionRange::new(ApiKey::Fetch, 0, 12);
    /// // Client supports v4-v11
    /// let negotiated = broker_range.negotiate(11, 4);
    /// assert_eq!(negotiated, Some(11));
    /// ```
    #[inline]
    pub fn negotiate(&self, client_max: i16, client_min: i16) -> Option<i16> {
        // Find the highest mutually supported version
        let max_supported = self.max_version.min(client_max);
        let min_supported = self.min_version.max(client_min);

        if max_supported >= min_supported {
            Some(max_supported)
        } else {
            None
        }
    }

    /// Negotiate with default client minimum of 0.
    #[inline]
    pub fn negotiate_max(&self, client_max: i16) -> Option<i16> {
        self.negotiate(client_max, 0)
    }
}

impl Encode for ApiVersionRange {
    fn encode(&self, buf: &mut impl BufMut) {
        self.api_key.encode(buf);
        self.min_version.encode(buf);
        self.max_version.encode(buf);
    }

    fn encode_compact(&self, buf: &mut impl BufMut) {
        self.encode(buf);
        // Empty tagged fields for flexible versions.
        TaggedFields::default().encode(buf);
    }
}

impl TryEncode for ApiVersionRange {
    fn try_encode(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode(buf);
        Ok(())
    }

    fn try_encode_compact(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl Decode for ApiVersionRange {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self {
            api_key: ApiKey::decode(buf)?,
            min_version: i16::decode(buf)?,
            max_version: i16::decode(buf)?,
        })
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        let result = Self {
            api_key: ApiKey::decode(buf)?,
            min_version: i16::decode(buf)?,
            max_version: i16::decode(buf)?,
        };
        // Skip tagged fields
        let _ = TaggedFields::decode(buf)?;
        Ok(result)
    }
}

/// A feature supported by the broker, returned in ApiVersions v3+ tagged fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedFeature {
    /// Feature name.
    pub name: String,
    /// Minimum supported version for the feature.
    pub min_version: i16,
    /// Maximum supported version for the feature.
    pub max_version: i16,
}

/// Request for API versions (ApiVersions API key = 18).
#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    /// Client software name (v3+).
    pub client_software_name: Option<KafkaString>,
    /// Client software version (v3+).
    pub client_software_version: Option<KafkaString>,
    /// Cluster ID the client intends to connect to (v5+, KIP-1242).
    pub cluster_id: Option<String>,
    /// Node ID the client intends to connect to (v5+, KIP-1242). -1 if unknown.
    pub node_id: i32,
}

impl Default for ApiVersionsRequest {
    fn default() -> Self {
        Self {
            client_software_name: None,
            client_software_version: None,
            cluster_id: None,
            node_id: -1,
        }
    }
}

impl ApiVersionsRequest {
    /// Create a new ApiVersionsRequest.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set client software name.
    pub fn with_client_software(mut self, name: &str, version: &str) -> Self {
        self.client_software_name = Some(KafkaString::new(name));
        self.client_software_version = Some(KafkaString::new(version));
        self
    }

    /// Get the API key for this request.
    pub fn api_key() -> ApiKey {
        ApiKey::ApiVersions
    }

    /// Encode for version 0-2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        // Empty request body for v0-2
        let _ = buf;
        Ok(())
    }

    /// Encode for version 3+ (flexible).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        if let Some(ref name) = self.client_software_name {
            name.try_encode_compact(buf)?;
        } else {
            KafkaString::null().try_encode_compact(buf)?;
        }
        if let Some(ref version) = self.client_software_version {
            version.try_encode_compact(buf)?;
        } else {
            KafkaString::null().try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (KIP-1242: ClusterId + NodeId).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        if let Some(ref name) = self.client_software_name {
            name.try_encode_compact(buf)?;
        } else {
            KafkaString::null().try_encode_compact(buf)?;
        }
        if let Some(ref version) = self.client_software_version {
            version.try_encode_compact(buf)?;
        } else {
            KafkaString::null().try_encode_compact(buf)?;
        }
        KafkaString(self.cluster_id.clone()).try_encode_compact(buf)?;
        self.node_id.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Response for API versions.
#[derive(Debug, Clone, Default)]
pub struct ApiVersionsResponse {
    /// Error code.
    pub error_code: i16,
    /// Supported API versions.
    pub api_keys: Vec<ApiVersionRange>,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Features supported by the broker (v3+ tagged field, tag 0).
    pub supported_features: Vec<SupportedFeature>,
}

impl ApiVersionsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let api_keys = KafkaArray::<ApiVersionRange>::decode(buf)?
            .0
            .unwrap_or_default();
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms: 0,
            supported_features: Vec::new(),
        })
    }

    /// Decode from version 1-2.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let api_keys = KafkaArray::<ApiVersionRange>::decode(buf)?
            .0
            .unwrap_or_default();
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features: Vec::new(),
        })
    }

    /// Decode from version 3–5 (flexible).
    ///
    /// v4 (KAFKA-17011) fixes SupportedFeatures.MinVersion so it can be 0;
    /// v5 (KIP-1242) adds ClusterId/NodeId to the *request* and
    /// REBOOTSTRAP_REQUIRED to the error codes; the response wire format is
    /// identical to v3.
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let error_code = i16::decode(buf)?;
        let api_keys = KafkaArray::<ApiVersionRange>::decode_compact(buf)?
            .0
            .unwrap_or_default();
        let throttle_time_ms = i32::decode(buf)?;
        let tagged = TaggedFields::decode(buf)?;
        let supported_features = Self::parse_supported_features(&tagged)?;
        Ok(Self {
            error_code,
            api_keys,
            throttle_time_ms,
            supported_features,
        })
    }

    /// Parse SupportedFeatures from tagged field tag 0.
    ///
    /// Wire format: compact-array of \[compact-string Name, i16 MinVersion, i16 MaxVersion\],
    /// each entry followed by its own empty tagged fields.
    fn parse_supported_features(tagged: &TaggedFields) -> Result<Vec<SupportedFeature>> {
        let Some(field) = tagged.0.iter().find(|f| f.tag == 0) else {
            return Ok(Vec::new());
        };
        let mut buf = &field.data[..];
        let raw_count = crate::util::varint::decode_unsigned_varint(&mut buf)? as usize;
        if raw_count == 0 {
            return Ok(Vec::new());
        }
        // compact array length is count + 1; actual items = count - 1
        let items = raw_count.saturating_sub(1);
        if items > MAX_SUPPORTED_FEATURES {
            return Err(crate::error::KrafkaError::protocol(format!(
                "SupportedFeatures array too large: {items}"
            )));
        }
        let mut features = Vec::with_capacity(items);
        for _ in 0..items {
            let name = super::non_nullable_string(
                "feature name",
                KafkaString::decode_compact(&mut buf)?.0,
            )?;
            let min_version = i16::decode(&mut buf)?;
            let max_version = i16::decode(&mut buf)?;
            // skip per-entry tagged fields
            let _ = TaggedFields::decode(&mut buf)?;
            features.push(SupportedFeature {
                name,
                min_version,
                max_version,
            });
        }
        if buf.has_remaining() {
            return Err(crate::error::KrafkaError::protocol(format!(
                "SupportedFeatures: {} trailing bytes after parsing {items} entries",
                buf.remaining()
            )));
        }
        Ok(features)
    }

    /// Get the version range for a specific API key.
    pub fn get_api_version(&self, api_key: ApiKey) -> Option<&ApiVersionRange> {
        self.api_keys.iter().find(|v| v.api_key == api_key)
    }

    /// Get a supported feature by name.
    pub fn get_supported_feature(&self, name: &str) -> Option<&SupportedFeature> {
        self.supported_features.iter().find(|f| f.name == name)
    }

    /// Check if an API is supported.
    pub fn supports(&self, api_key: ApiKey, version: i16) -> bool {
        self.get_api_version(api_key)
            .is_some_and(|v| v.supports(version))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_api_key_roundtrip() {
        let keys = [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::Metadata,
            ApiKey::ApiVersions,
        ];

        for key in keys {
            let mut buf = BytesMut::new();
            key.encode(&mut buf);
            let decoded = ApiKey::decode(&mut buf.freeze()).unwrap();
            assert_eq!(decoded, key);
        }
    }

    #[test]
    fn test_api_version_range() {
        let range = ApiVersionRange::new(ApiKey::Produce, 0, 9);
        assert!(range.supports(0));
        assert!(range.supports(5));
        assert!(range.supports(9));
        assert!(!range.supports(-1));
        assert!(!range.supports(10));
    }

    #[test]
    fn test_api_version_range_encode_decode() {
        let range = ApiVersionRange::new(ApiKey::Fetch, 0, 13);
        let mut buf = BytesMut::new();
        range.encode(&mut buf);

        let decoded = ApiVersionRange::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.api_key, ApiKey::Fetch);
        assert_eq!(decoded.min_version, 0);
        assert_eq!(decoded.max_version, 13);
    }

    #[test]
    fn test_api_versions_request() {
        let request =
            ApiVersionsRequest::new().with_client_software("krafka", env!("CARGO_PKG_VERSION"));
        assert_eq!(
            request.client_software_name.as_ref().unwrap().as_str(),
            Some("krafka")
        );
        assert_eq!(
            request.client_software_version.as_ref().unwrap().as_str(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn test_api_versions_response() {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersionRange::new(ApiKey::Produce, 0, 9),
                ApiVersionRange::new(ApiKey::Fetch, 0, 13),
            ],
            throttle_time_ms: 0,
            supported_features: Vec::new(),
        };

        assert!(response.supports(ApiKey::Produce, 5));
        assert!(!response.supports(ApiKey::Produce, 10));
        assert!(response.supports(ApiKey::Fetch, 13));
        assert!(!response.supports(ApiKey::Metadata, 0));
    }

    #[test]
    fn test_negotiate_version() {
        // Broker supports v0-v12, client supports v4-v11 -> use v11
        let range = ApiVersionRange::new(ApiKey::Fetch, 0, 12);
        assert_eq!(range.negotiate(11, 4), Some(11));

        // Broker supports v4-v8, client supports v0-v6 -> use v6
        let range = ApiVersionRange::new(ApiKey::Produce, 4, 8);
        assert_eq!(range.negotiate(6, 0), Some(6));

        // No overlap: broker v0-v3, client v5-v10 -> None
        let range = ApiVersionRange::new(ApiKey::Metadata, 0, 3);
        assert_eq!(range.negotiate(10, 5), None);

        // Exact match
        let range = ApiVersionRange::new(ApiKey::Heartbeat, 2, 2);
        assert_eq!(range.negotiate(2, 2), Some(2));

        // negotiate_max helper
        let range = ApiVersionRange::new(ApiKey::Fetch, 0, 12);
        assert_eq!(range.negotiate_max(8), Some(8));
        assert_eq!(range.negotiate_max(15), Some(12)); // capped to broker max
    }

    #[test]
    fn test_api_key_display() {
        assert_eq!(ApiKey::Produce.to_string(), "Produce");
        assert_eq!(ApiKey::Fetch.to_string(), "Fetch");
        assert_eq!(ApiKey::Unknown(999).to_string(), "Unknown(999)");
    }

    // ── ApiVersions v3/v4 round-trip and SupportedFeatures parsing ──

    #[test]
    fn test_api_versions_request_v3_round_trip() {
        let request = ApiVersionsRequest::new().with_client_software("krafka", "0.4.0");
        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();
        // v3 and v4 share the same wire format; a second encode must produce identical bytes
        let mut buf2 = BytesMut::new();
        request.encode_v3(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_api_versions_response_decode_v3_no_tagged_features() {
        use crate::util::varint;
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        // compact api_keys array: varint(count+1) = 1 means 0 items
        varint::encode_unsigned_varint(1, &mut buf);
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0); // empty tagged fields
        let mut data = buf.freeze();
        let resp = ApiVersionsResponse::decode_v3(&mut data).unwrap();
        assert_eq!(resp.error_code, 0);
        assert!(resp.api_keys.is_empty());
        assert!(resp.supported_features.is_empty());
    }

    #[test]
    fn test_api_versions_response_decode_v3_with_supported_features() {
        use crate::util::varint;
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        // compact api_keys array: 1 item (varint(2))
        varint::encode_unsigned_varint(2, &mut buf);
        // ApiVersionRange: api_key(i16) + min_version(i16) + max_version(i16)
        buf.put_i16(0); // api_key = Produce
        buf.put_i16(0); // min_version
        buf.put_i16(9); // max_version
        // per-entry tagged fields for compact api key
        buf.put_u8(0);
        buf.put_i32(0); // throttle_time_ms

        // Tagged fields: 1 field, tag 0 = SupportedFeatures
        let mut tag_data = BytesMut::new();
        // compact array of features: 2 items (varint(3) = 2+1)
        varint::encode_unsigned_varint(3, &mut tag_data);
        // Feature 1: "metadata.version" min=1 max=20
        let name1 = b"metadata.version";
        varint::encode_unsigned_varint(name1.len() as u32 + 1, &mut tag_data);
        tag_data.put_slice(name1);
        tag_data.put_i16(1); // min_version
        tag_data.put_i16(20); // max_version
        tag_data.put_u8(0); // per-entry tagged fields
        // Feature 2: "kraft.version" min=0 max=1
        let name2 = b"kraft.version";
        varint::encode_unsigned_varint(name2.len() as u32 + 1, &mut tag_data);
        tag_data.put_slice(name2);
        tag_data.put_i16(0); // min_version
        tag_data.put_i16(1); // max_version
        tag_data.put_u8(0); // per-entry tagged fields

        // Emit top-level tagged fields: 1 field
        varint::encode_unsigned_varint(1, &mut buf); // 1 tagged field
        varint::encode_unsigned_varint(0, &mut buf); // tag = 0
        varint::encode_unsigned_varint(tag_data.len() as u32, &mut buf);
        buf.extend_from_slice(&tag_data);

        let mut data = buf.freeze();
        let resp = ApiVersionsResponse::decode_v3(&mut data).unwrap();
        assert_eq!(resp.supported_features.len(), 2);
        assert_eq!(resp.supported_features[0].name, "metadata.version");
        assert_eq!(resp.supported_features[0].min_version, 1);
        assert_eq!(resp.supported_features[0].max_version, 20);
        assert_eq!(resp.supported_features[1].name, "kraft.version");
        assert_eq!(resp.supported_features[1].min_version, 0);
        assert_eq!(resp.supported_features[1].max_version, 1);

        // Test feature lookup
        let feat = resp.get_supported_feature("kraft.version").unwrap();
        assert_eq!(feat.min_version, 0);
        assert_eq!(feat.max_version, 1);
        assert!(resp.get_supported_feature("nonexistent").is_none());
    }

    #[test]
    fn test_api_versions_response_decode_v0_no_features() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_i32(0); // api_keys count = 0
        let mut data = buf.freeze();
        let resp = ApiVersionsResponse::decode_v0(&mut data).unwrap();
        assert!(resp.supported_features.is_empty());
        assert_eq!(resp.throttle_time_ms, 0);
    }
}
