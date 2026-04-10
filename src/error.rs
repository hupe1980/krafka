//! Error types for Krafka.
//!
//! This module provides structured error types for all Krafka operations.

use std::io;

use thiserror::Error;

/// The main error type for Krafka operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum KrafkaError {
    /// Network-related errors (connection, I/O).
    #[error("network error: {0}")]
    Network(#[from] io::Error),

    /// Protocol encoding/decoding errors.
    #[error("protocol error: {message}")]
    Protocol {
        /// Error message describing the protocol error.
        message: String,
    },

    /// Authentication errors.
    #[error("authentication error: {message}")]
    Auth {
        /// Error message describing the authentication failure.
        message: String,
    },

    /// Timeout errors.
    #[error("operation timed out: {operation}")]
    Timeout {
        /// The operation that timed out.
        operation: String,
    },

    /// Broker errors returned by Kafka.
    #[error("broker error: {code:?} - {message}")]
    Broker {
        /// The Kafka error code.
        code: ErrorCode,
        /// Human-readable error message.
        message: String,
    },

    /// Configuration errors.
    #[error("configuration error: {message}")]
    Config {
        /// Error message describing the configuration problem.
        message: String,
    },

    /// Compression errors.
    #[error("compression error: {message}")]
    Compression {
        /// Error message describing the compression failure.
        message: String,
    },

    /// Invalid state errors.
    #[error("invalid state: {message}")]
    InvalidState {
        /// Error message describing the invalid state.
        message: String,
    },

    /// Serialization errors.
    #[error("serialization error: {message}")]
    Serialization {
        /// Error message describing the serialization failure.
        message: String,
    },

    /// Schema registry errors.
    #[error("schema registry error: {message}")]
    SchemaRegistry {
        /// Human-readable error message.
        message: String,
    },
}

impl KrafkaError {
    /// Create a new protocol error.
    #[cold]
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    /// Create a new authentication error.
    #[cold]
    pub fn auth(message: impl Into<String>) -> Self {
        Self::Auth {
            message: message.into(),
        }
    }

    /// Create a new timeout error.
    #[cold]
    pub fn timeout(operation: impl Into<String>) -> Self {
        Self::Timeout {
            operation: operation.into(),
        }
    }

    /// Create a new broker error.
    #[cold]
    pub fn broker(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Broker {
            code,
            message: message.into(),
        }
    }

    /// Create a new configuration error.
    #[cold]
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            message: message.into(),
        }
    }

    /// Create a new compression error.
    #[cold]
    pub fn compression(message: impl Into<String>) -> Self {
        Self::Compression {
            message: message.into(),
        }
    }

    /// Create a new invalid state error.
    #[cold]
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState {
            message: message.into(),
        }
    }

    /// Create a new serialization error.
    #[cold]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization {
            message: message.into(),
        }
    }

    /// Create a new schema registry error.
    #[cold]
    pub fn schema_registry(message: impl Into<String>) -> Self {
        Self::SchemaRegistry {
            message: message.into(),
        }
    }

    /// Returns true if this is a retriable error.
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Network(_) => true,
            Self::Timeout { .. } => true,
            Self::Broker { code, .. } => code.is_retriable(),
            _ => false,
        }
    }
}

/// Kafka protocol error codes.
///
/// These are the standard error codes defined in the Kafka protocol.
/// Forward compatibility is provided by the `Unknown(i16)` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(i16)]
pub enum ErrorCode {
    /// No error.
    #[default]
    None = 0,
    /// Unknown server error.
    UnknownServerError = -1,
    /// The requested offset is outside the range of offsets maintained by the server.
    OffsetOutOfRange = 1,
    /// Message contents does not match its CRC.
    CorruptMessage = 2,
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3,
    /// Invalid message size.
    InvalidMessageSize = 4,
    /// Leader not available.
    LeaderNotAvailable = 5,
    /// Not leader for partition.
    NotLeaderForPartition = 6,
    /// Request timed out.
    RequestTimedOut = 7,
    /// Broker not available.
    BrokerNotAvailable = 8,
    /// Replica not available.
    ReplicaNotAvailable = 9,
    /// Message too large.
    MessageTooLarge = 10,
    /// Stale controller epoch.
    StaleControllerEpoch = 11,
    /// Offset metadata too large.
    OffsetMetadataTooLarge = 12,
    /// Network exception.
    NetworkException = 13,
    /// Coordinator load in progress.
    CoordinatorLoadInProgress = 14,
    /// Coordinator not available.
    CoordinatorNotAvailable = 15,
    /// Not coordinator.
    NotCoordinator = 16,
    /// Invalid topic.
    InvalidTopic = 17,
    /// Record list too large.
    RecordListTooLarge = 18,
    /// Not enough replicas.
    NotEnoughReplicas = 19,
    /// Not enough replicas after append.
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks.
    InvalidRequiredAcks = 21,
    /// Illegal generation.
    IllegalGeneration = 22,
    /// Inconsistent group protocol.
    InconsistentGroupProtocol = 23,
    /// Invalid group ID.
    InvalidGroupId = 24,
    /// Unknown member ID.
    UnknownMemberId = 25,
    /// Invalid session timeout.
    InvalidSessionTimeout = 26,
    /// Rebalance in progress.
    RebalanceInProgress = 27,
    /// Invalid commit offset size.
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Group authorization failed.
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
    /// Invalid timestamp.
    InvalidTimestamp = 32,
    /// Unsupported SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Illegal SASL state.
    IllegalSaslState = 34,
    /// Unsupported version.
    UnsupportedVersion = 35,
    /// Topic already exists.
    TopicAlreadyExists = 36,
    /// Invalid partitions.
    InvalidPartitions = 37,
    /// Invalid replication factor.
    InvalidReplicationFactor = 38,
    /// Invalid replica assignment.
    InvalidReplicaAssignment = 39,
    /// Invalid config.
    InvalidConfig = 40,
    /// Not controller.
    NotController = 41,
    /// Invalid request.
    InvalidRequest = 42,
    /// Unsupported for message format.
    UnsupportedForMessageFormat = 43,
    /// Policy violation.
    PolicyViolation = 44,
    /// Out of order sequence number.
    OutOfOrderSequenceNumber = 45,
    /// Duplicate sequence number.
    DuplicateSequenceNumber = 46,
    /// Invalid producer epoch.
    InvalidProducerEpoch = 47,
    /// Invalid txn state.
    InvalidTxnState = 48,
    /// Invalid producer ID mapping.
    InvalidProducerIdMapping = 49,
    /// Invalid transaction timeout.
    InvalidTransactionTimeout = 50,
    /// Concurrent transactions.
    ConcurrentTransactions = 51,
    /// Transaction coordinator fenced.
    TransactionCoordinatorFenced = 52,
    /// Transactional ID authorization failed.
    TransactionalIdAuthorizationFailed = 53,
    /// Security disabled.
    SecurityDisabled = 54,
    /// Operation not attempted.
    OperationNotAttempted = 55,
    /// Kafka storage exception.
    KafkaStorageException = 56,
    /// Log directory not found.
    LogDirNotFound = 57,
    /// SASL authentication failed.
    SaslAuthenticationFailed = 58,
    /// Unknown producer ID.
    UnknownProducerId = 59,
    /// Reassignment in progress.
    ReassignmentInProgress = 60,
    /// Delegation token auth disabled.
    DelegationTokenAuthDisabled = 61,
    /// Delegation token not found.
    DelegationTokenNotFound = 62,
    /// Delegation token owner mismatch.
    DelegationTokenOwnerMismatch = 63,
    /// Delegation token request not allowed.
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation token authorization failed.
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation token expired.
    DelegationTokenExpired = 66,
    /// Invalid principal type.
    InvalidPrincipalType = 67,
    /// Non-empty group.
    NonEmptyGroup = 68,
    /// Group ID not found.
    GroupIdNotFound = 69,
    /// Fetch session ID not found.
    FetchSessionIdNotFound = 70,
    /// Invalid fetch session epoch.
    InvalidFetchSessionEpoch = 71,
    /// Listener not found.
    ListenerNotFound = 72,
    /// Topic deletion disabled.
    TopicDeletionDisabled = 73,
    /// Fenced leader epoch.
    FencedLeaderEpoch = 74,
    /// Unknown leader epoch.
    UnknownLeaderEpoch = 75,
    /// Unsupported compression type.
    UnsupportedCompressionType = 76,
    /// Stale broker epoch.
    StaleBrokerEpoch = 77,
    /// Offset not available.
    OffsetNotAvailable = 78,
    /// Member ID required.
    MemberIdRequired = 79,
    /// Preferred leader not available.
    PreferredLeaderNotAvailable = 80,
    /// Group max size reached.
    GroupMaxSizeReached = 81,
    /// Fenced instance ID.
    FencedInstanceId = 82,
    /// Eligible leaders not available.
    EligibleLeadersNotAvailable = 83,
    /// Election not needed.
    ElectionNotNeeded = 84,
    /// No reassignment in progress.
    NoReassignmentInProgress = 85,
    /// Group subscribed to topic.
    GroupSubscribedToTopic = 86,
    /// Invalid record.
    InvalidRecord = 87,
    /// Unstable offset commit.
    UnstableOffsetCommit = 88,
    /// The throttling quota has been exceeded.
    ThrottlingQuotaExceeded = 89,
    /// There is a newer producer with the same transactionalId which fences the current one.
    ProducerFenced = 90,
    /// A request illegally referred to a resource that does not exist.
    ResourceNotFound = 91,
    /// A request illegally referred to the same resource twice.
    DuplicateResource = 92,
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential = 93,
    /// Either the sender or recipient of a voter-only request is not one of the expected voters.
    InconsistentVoterSet = 94,
    /// The given update version was invalid.
    InvalidUpdateVersion = 95,
    /// Unable to update finalized features due to an unexpected server error.
    FeatureUpdateFailed = 96,
    /// Request principal deserialization failed during forwarding.
    PrincipalDeserializationFailure = 97,
    /// Requested snapshot was not found.
    SnapshotNotFound = 98,
    /// Requested position is not greater than or equal to zero, and less than the size of the snapshot.
    PositionOutOfRange = 99,
    /// This server does not host this topic ID.
    UnknownTopicId = 100,
    /// This broker ID is already in use.
    DuplicateBrokerRegistration = 101,
    /// The given broker ID was not registered.
    BrokerIdNotRegistered = 102,
    /// The log's topic ID did not match the topic ID in the request.
    InconsistentTopicId = 103,
    /// The clusterId in the request does not match that found on the server.
    InconsistentClusterId = 104,
    /// The transactionalId could not be found.
    TransactionalIdNotFound = 105,
    /// The fetch session encountered inconsistent topic ID usage.
    FetchSessionTopicIdError = 106,
    /// The new ISR contains at least one ineligible replica.
    IneligibleReplica = 107,
    /// The AlterPartition request successfully updated the partition state but the leader has changed.
    NewLeaderElected = 108,
    /// The requested offset is moved to tiered storage.
    OffsetMovedToTieredStorage = 109,
    /// Fenced member epoch (KIP-848).
    FencedMemberEpoch = 110,
    /// The instance ID is still used by another member in the consumer group (KIP-848).
    UnreleasedInstanceId = 111,
    /// The assignor or its version range is not supported by the consumer group (KIP-848).
    UnsupportedAssignor = 112,
    /// The member epoch is stale; retry after receiving an updated epoch via ConsumerGroupHeartbeat (KIP-848).
    StaleMemberEpoch = 113,
    /// The request was sent to an endpoint of the wrong type.
    MismatchedEndpointType = 114,
    /// This endpoint type is not supported yet.
    UnsupportedEndpointType = 115,
    /// This controller ID is not known.
    UnknownControllerId = 116,
    /// Client sent a push telemetry request with an invalid or outdated subscription ID.
    UnknownSubscriptionId = 117,
    /// Client sent a push telemetry request larger than the maximum size the broker will accept.
    TelemetryTooLarge = 118,
    /// The controller has considered the broker registration to be invalid.
    InvalidRegistration = 119,
    /// The server encountered an error with the transaction. The client can abort the transaction to continue (KIP-890).
    TransactionAbortable = 120,
    /// The regular expression is not valid (KIP-848 v1+).
    InvalidRegularExpression = 128,
    /// Unknown error code.
    Unknown(i16),
}

impl ErrorCode {
    /// Create an ErrorCode from a raw i16 value.
    #[inline]
    pub fn from_i16(code: i16) -> Self {
        match code {
            0 => Self::None,
            -1 => Self::UnknownServerError,
            1 => Self::OffsetOutOfRange,
            2 => Self::CorruptMessage,
            3 => Self::UnknownTopicOrPartition,
            4 => Self::InvalidMessageSize,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderForPartition,
            7 => Self::RequestTimedOut,
            8 => Self::BrokerNotAvailable,
            9 => Self::ReplicaNotAvailable,
            10 => Self::MessageTooLarge,
            11 => Self::StaleControllerEpoch,
            12 => Self::OffsetMetadataTooLarge,
            13 => Self::NetworkException,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopic,
            18 => Self::RecordListTooLarge,
            19 => Self::NotEnoughReplicas,
            20 => Self::NotEnoughReplicasAfterAppend,
            21 => Self::InvalidRequiredAcks,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            24 => Self::InvalidGroupId,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            28 => Self::InvalidCommitOffsetSize,
            29 => Self::TopicAuthorizationFailed,
            30 => Self::GroupAuthorizationFailed,
            31 => Self::ClusterAuthorizationFailed,
            32 => Self::InvalidTimestamp,
            33 => Self::UnsupportedSaslMechanism,
            34 => Self::IllegalSaslState,
            35 => Self::UnsupportedVersion,
            36 => Self::TopicAlreadyExists,
            37 => Self::InvalidPartitions,
            38 => Self::InvalidReplicationFactor,
            39 => Self::InvalidReplicaAssignment,
            40 => Self::InvalidConfig,
            41 => Self::NotController,
            42 => Self::InvalidRequest,
            43 => Self::UnsupportedForMessageFormat,
            44 => Self::PolicyViolation,
            45 => Self::OutOfOrderSequenceNumber,
            46 => Self::DuplicateSequenceNumber,
            47 => Self::InvalidProducerEpoch,
            48 => Self::InvalidTxnState,
            49 => Self::InvalidProducerIdMapping,
            50 => Self::InvalidTransactionTimeout,
            51 => Self::ConcurrentTransactions,
            52 => Self::TransactionCoordinatorFenced,
            53 => Self::TransactionalIdAuthorizationFailed,
            54 => Self::SecurityDisabled,
            55 => Self::OperationNotAttempted,
            56 => Self::KafkaStorageException,
            57 => Self::LogDirNotFound,
            58 => Self::SaslAuthenticationFailed,
            59 => Self::UnknownProducerId,
            60 => Self::ReassignmentInProgress,
            61 => Self::DelegationTokenAuthDisabled,
            62 => Self::DelegationTokenNotFound,
            63 => Self::DelegationTokenOwnerMismatch,
            64 => Self::DelegationTokenRequestNotAllowed,
            65 => Self::DelegationTokenAuthorizationFailed,
            66 => Self::DelegationTokenExpired,
            67 => Self::InvalidPrincipalType,
            68 => Self::NonEmptyGroup,
            69 => Self::GroupIdNotFound,
            70 => Self::FetchSessionIdNotFound,
            71 => Self::InvalidFetchSessionEpoch,
            72 => Self::ListenerNotFound,
            73 => Self::TopicDeletionDisabled,
            74 => Self::FencedLeaderEpoch,
            75 => Self::UnknownLeaderEpoch,
            76 => Self::UnsupportedCompressionType,
            77 => Self::StaleBrokerEpoch,
            78 => Self::OffsetNotAvailable,
            79 => Self::MemberIdRequired,
            80 => Self::PreferredLeaderNotAvailable,
            81 => Self::GroupMaxSizeReached,
            82 => Self::FencedInstanceId,
            83 => Self::EligibleLeadersNotAvailable,
            84 => Self::ElectionNotNeeded,
            85 => Self::NoReassignmentInProgress,
            86 => Self::GroupSubscribedToTopic,
            87 => Self::InvalidRecord,
            88 => Self::UnstableOffsetCommit,
            89 => Self::ThrottlingQuotaExceeded,
            90 => Self::ProducerFenced,
            91 => Self::ResourceNotFound,
            92 => Self::DuplicateResource,
            93 => Self::UnacceptableCredential,
            94 => Self::InconsistentVoterSet,
            95 => Self::InvalidUpdateVersion,
            96 => Self::FeatureUpdateFailed,
            97 => Self::PrincipalDeserializationFailure,
            98 => Self::SnapshotNotFound,
            99 => Self::PositionOutOfRange,
            100 => Self::UnknownTopicId,
            101 => Self::DuplicateBrokerRegistration,
            102 => Self::BrokerIdNotRegistered,
            103 => Self::InconsistentTopicId,
            104 => Self::InconsistentClusterId,
            105 => Self::TransactionalIdNotFound,
            106 => Self::FetchSessionTopicIdError,
            107 => Self::IneligibleReplica,
            108 => Self::NewLeaderElected,
            109 => Self::OffsetMovedToTieredStorage,
            110 => Self::FencedMemberEpoch,
            111 => Self::UnreleasedInstanceId,
            112 => Self::UnsupportedAssignor,
            113 => Self::StaleMemberEpoch,
            114 => Self::MismatchedEndpointType,
            115 => Self::UnsupportedEndpointType,
            116 => Self::UnknownControllerId,
            117 => Self::UnknownSubscriptionId,
            118 => Self::TelemetryTooLarge,
            119 => Self::InvalidRegistration,
            120 => Self::TransactionAbortable,
            128 => Self::InvalidRegularExpression,
            other => Self::Unknown(other),
        }
    }

    /// Convert the ErrorCode to its raw i16 value.
    #[inline]
    pub fn to_i16(self) -> i16 {
        match self {
            Self::None => 0,
            Self::UnknownServerError => -1,
            Self::OffsetOutOfRange => 1,
            Self::CorruptMessage => 2,
            Self::UnknownTopicOrPartition => 3,
            Self::InvalidMessageSize => 4,
            Self::LeaderNotAvailable => 5,
            Self::NotLeaderForPartition => 6,
            Self::RequestTimedOut => 7,
            Self::BrokerNotAvailable => 8,
            Self::ReplicaNotAvailable => 9,
            Self::MessageTooLarge => 10,
            Self::StaleControllerEpoch => 11,
            Self::OffsetMetadataTooLarge => 12,
            Self::NetworkException => 13,
            Self::CoordinatorLoadInProgress => 14,
            Self::CoordinatorNotAvailable => 15,
            Self::NotCoordinator => 16,
            Self::InvalidTopic => 17,
            Self::RecordListTooLarge => 18,
            Self::NotEnoughReplicas => 19,
            Self::NotEnoughReplicasAfterAppend => 20,
            Self::InvalidRequiredAcks => 21,
            Self::IllegalGeneration => 22,
            Self::InconsistentGroupProtocol => 23,
            Self::InvalidGroupId => 24,
            Self::UnknownMemberId => 25,
            Self::InvalidSessionTimeout => 26,
            Self::RebalanceInProgress => 27,
            Self::InvalidCommitOffsetSize => 28,
            Self::TopicAuthorizationFailed => 29,
            Self::GroupAuthorizationFailed => 30,
            Self::ClusterAuthorizationFailed => 31,
            Self::InvalidTimestamp => 32,
            Self::UnsupportedSaslMechanism => 33,
            Self::IllegalSaslState => 34,
            Self::UnsupportedVersion => 35,
            Self::TopicAlreadyExists => 36,
            Self::InvalidPartitions => 37,
            Self::InvalidReplicationFactor => 38,
            Self::InvalidReplicaAssignment => 39,
            Self::InvalidConfig => 40,
            Self::NotController => 41,
            Self::InvalidRequest => 42,
            Self::UnsupportedForMessageFormat => 43,
            Self::PolicyViolation => 44,
            Self::OutOfOrderSequenceNumber => 45,
            Self::DuplicateSequenceNumber => 46,
            Self::InvalidProducerEpoch => 47,
            Self::InvalidTxnState => 48,
            Self::InvalidProducerIdMapping => 49,
            Self::InvalidTransactionTimeout => 50,
            Self::ConcurrentTransactions => 51,
            Self::TransactionCoordinatorFenced => 52,
            Self::TransactionalIdAuthorizationFailed => 53,
            Self::SecurityDisabled => 54,
            Self::OperationNotAttempted => 55,
            Self::KafkaStorageException => 56,
            Self::LogDirNotFound => 57,
            Self::SaslAuthenticationFailed => 58,
            Self::UnknownProducerId => 59,
            Self::ReassignmentInProgress => 60,
            Self::DelegationTokenAuthDisabled => 61,
            Self::DelegationTokenNotFound => 62,
            Self::DelegationTokenOwnerMismatch => 63,
            Self::DelegationTokenRequestNotAllowed => 64,
            Self::DelegationTokenAuthorizationFailed => 65,
            Self::DelegationTokenExpired => 66,
            Self::InvalidPrincipalType => 67,
            Self::NonEmptyGroup => 68,
            Self::GroupIdNotFound => 69,
            Self::FetchSessionIdNotFound => 70,
            Self::InvalidFetchSessionEpoch => 71,
            Self::ListenerNotFound => 72,
            Self::TopicDeletionDisabled => 73,
            Self::FencedLeaderEpoch => 74,
            Self::UnknownLeaderEpoch => 75,
            Self::UnsupportedCompressionType => 76,
            Self::StaleBrokerEpoch => 77,
            Self::OffsetNotAvailable => 78,
            Self::MemberIdRequired => 79,
            Self::PreferredLeaderNotAvailable => 80,
            Self::GroupMaxSizeReached => 81,
            Self::FencedInstanceId => 82,
            Self::EligibleLeadersNotAvailable => 83,
            Self::ElectionNotNeeded => 84,
            Self::NoReassignmentInProgress => 85,
            Self::GroupSubscribedToTopic => 86,
            Self::InvalidRecord => 87,
            Self::UnstableOffsetCommit => 88,
            Self::ThrottlingQuotaExceeded => 89,
            Self::ProducerFenced => 90,
            Self::ResourceNotFound => 91,
            Self::DuplicateResource => 92,
            Self::UnacceptableCredential => 93,
            Self::InconsistentVoterSet => 94,
            Self::InvalidUpdateVersion => 95,
            Self::FeatureUpdateFailed => 96,
            Self::PrincipalDeserializationFailure => 97,
            Self::SnapshotNotFound => 98,
            Self::PositionOutOfRange => 99,
            Self::UnknownTopicId => 100,
            Self::DuplicateBrokerRegistration => 101,
            Self::BrokerIdNotRegistered => 102,
            Self::InconsistentTopicId => 103,
            Self::InconsistentClusterId => 104,
            Self::TransactionalIdNotFound => 105,
            Self::FetchSessionTopicIdError => 106,
            Self::IneligibleReplica => 107,
            Self::NewLeaderElected => 108,
            Self::OffsetMovedToTieredStorage => 109,
            Self::FencedMemberEpoch => 110,
            Self::UnreleasedInstanceId => 111,
            Self::UnsupportedAssignor => 112,
            Self::StaleMemberEpoch => 113,
            Self::MismatchedEndpointType => 114,
            Self::UnsupportedEndpointType => 115,
            Self::UnknownControllerId => 116,
            Self::UnknownSubscriptionId => 117,
            Self::TelemetryTooLarge => 118,
            Self::InvalidRegistration => 119,
            Self::TransactionAbortable => 120,
            Self::InvalidRegularExpression => 128,
            Self::Unknown(code) => code,
        }
    }

    /// Returns true if this error is retriable.
    #[inline]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::CorruptMessage
                | Self::UnknownTopicOrPartition
                | Self::LeaderNotAvailable
                | Self::NotLeaderForPartition
                | Self::RequestTimedOut
                | Self::BrokerNotAvailable
                | Self::ReplicaNotAvailable
                | Self::NetworkException
                | Self::CoordinatorLoadInProgress
                | Self::CoordinatorNotAvailable
                | Self::NotCoordinator
                | Self::NotEnoughReplicas
                | Self::NotEnoughReplicasAfterAppend
                | Self::OutOfOrderSequenceNumber
                | Self::ConcurrentTransactions
                | Self::OperationNotAttempted
                | Self::KafkaStorageException
                | Self::UnknownProducerId
                | Self::FetchSessionIdNotFound
                | Self::InvalidFetchSessionEpoch
                | Self::FencedLeaderEpoch
                | Self::UnknownLeaderEpoch
                | Self::OffsetNotAvailable
                | Self::PreferredLeaderNotAvailable
                | Self::EligibleLeadersNotAvailable
                | Self::UnstableOffsetCommit
                | Self::ThrottlingQuotaExceeded
                | Self::NewLeaderElected
        )
    }

    /// Returns true if this error code indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl From<i16> for ErrorCode {
    #[inline]
    fn from(code: i16) -> Self {
        Self::from_i16(code)
    }
}

impl From<ErrorCode> for i16 {
    #[inline]
    fn from(code: ErrorCode) -> Self {
        code.to_i16()
    }
}

/// A specialized Result type for Krafka operations.
pub type Result<T> = std::result::Result<T, KrafkaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_from_i16() {
        assert_eq!(ErrorCode::from_i16(0), ErrorCode::None);
        assert_eq!(ErrorCode::from_i16(-1), ErrorCode::UnknownServerError);
        assert_eq!(ErrorCode::from_i16(3), ErrorCode::UnknownTopicOrPartition);
        assert_eq!(ErrorCode::from_i16(999), ErrorCode::Unknown(999));
    }

    #[test]
    fn test_error_code_to_i16() {
        assert_eq!(ErrorCode::None.to_i16(), 0);
        assert_eq!(ErrorCode::UnknownServerError.to_i16(), -1);
        assert_eq!(ErrorCode::UnknownTopicOrPartition.to_i16(), 3);
        assert_eq!(ErrorCode::Unknown(999).to_i16(), 999);
    }

    #[test]
    fn test_error_code_is_retriable() {
        assert!(ErrorCode::LeaderNotAvailable.is_retriable());
        assert!(ErrorCode::RequestTimedOut.is_retriable());
        assert!(ErrorCode::CorruptMessage.is_retriable());
        assert!(ErrorCode::UnknownTopicOrPartition.is_retriable());
        assert!(ErrorCode::OutOfOrderSequenceNumber.is_retriable());
        assert!(ErrorCode::ConcurrentTransactions.is_retriable());
        assert!(ErrorCode::OperationNotAttempted.is_retriable());
        assert!(ErrorCode::BrokerNotAvailable.is_retriable());
        assert!(ErrorCode::NetworkException.is_retriable());
        assert!(!ErrorCode::None.is_retriable());
        assert!(!ErrorCode::InvalidTopic.is_retriable());
    }

    #[test]
    fn test_error_code_is_ok() {
        assert!(ErrorCode::None.is_ok());
        assert!(!ErrorCode::UnknownServerError.is_ok());
    }

    #[test]
    fn test_krafka_error_is_retriable() {
        assert!(KrafkaError::timeout("test").is_retriable());
        assert!(KrafkaError::broker(ErrorCode::LeaderNotAvailable, "test").is_retriable());
        assert!(!KrafkaError::config("test").is_retriable());
    }

    // ── R9.10: ErrorCode 56–88 from_i16 / to_i16 round-trip ──

    #[test]
    fn test_error_code_from_i16_codes_56_to_88() {
        assert_eq!(ErrorCode::from_i16(56), ErrorCode::KafkaStorageException);
        assert_eq!(ErrorCode::from_i16(57), ErrorCode::LogDirNotFound);
        assert_eq!(ErrorCode::from_i16(58), ErrorCode::SaslAuthenticationFailed);
        assert_eq!(ErrorCode::from_i16(59), ErrorCode::UnknownProducerId);
        assert_eq!(ErrorCode::from_i16(60), ErrorCode::ReassignmentInProgress);
        assert_eq!(
            ErrorCode::from_i16(61),
            ErrorCode::DelegationTokenAuthDisabled
        );
        assert_eq!(ErrorCode::from_i16(62), ErrorCode::DelegationTokenNotFound);
        assert_eq!(
            ErrorCode::from_i16(63),
            ErrorCode::DelegationTokenOwnerMismatch
        );
        assert_eq!(
            ErrorCode::from_i16(64),
            ErrorCode::DelegationTokenRequestNotAllowed
        );
        assert_eq!(
            ErrorCode::from_i16(65),
            ErrorCode::DelegationTokenAuthorizationFailed
        );
        assert_eq!(ErrorCode::from_i16(66), ErrorCode::DelegationTokenExpired);
        assert_eq!(ErrorCode::from_i16(67), ErrorCode::InvalidPrincipalType);
        assert_eq!(ErrorCode::from_i16(68), ErrorCode::NonEmptyGroup);
        assert_eq!(ErrorCode::from_i16(69), ErrorCode::GroupIdNotFound);
        assert_eq!(ErrorCode::from_i16(70), ErrorCode::FetchSessionIdNotFound);
        assert_eq!(ErrorCode::from_i16(71), ErrorCode::InvalidFetchSessionEpoch);
        assert_eq!(ErrorCode::from_i16(72), ErrorCode::ListenerNotFound);
        assert_eq!(ErrorCode::from_i16(73), ErrorCode::TopicDeletionDisabled);
        assert_eq!(ErrorCode::from_i16(74), ErrorCode::FencedLeaderEpoch);
        assert_eq!(ErrorCode::from_i16(75), ErrorCode::UnknownLeaderEpoch);
        assert_eq!(
            ErrorCode::from_i16(76),
            ErrorCode::UnsupportedCompressionType
        );
        assert_eq!(ErrorCode::from_i16(77), ErrorCode::StaleBrokerEpoch);
        assert_eq!(ErrorCode::from_i16(78), ErrorCode::OffsetNotAvailable);
        assert_eq!(ErrorCode::from_i16(79), ErrorCode::MemberIdRequired);
        assert_eq!(
            ErrorCode::from_i16(80),
            ErrorCode::PreferredLeaderNotAvailable
        );
        assert_eq!(ErrorCode::from_i16(81), ErrorCode::GroupMaxSizeReached);
        assert_eq!(ErrorCode::from_i16(82), ErrorCode::FencedInstanceId);
        assert_eq!(
            ErrorCode::from_i16(83),
            ErrorCode::EligibleLeadersNotAvailable
        );
        assert_eq!(ErrorCode::from_i16(84), ErrorCode::ElectionNotNeeded);
        assert_eq!(ErrorCode::from_i16(85), ErrorCode::NoReassignmentInProgress);
        assert_eq!(ErrorCode::from_i16(86), ErrorCode::GroupSubscribedToTopic);
        assert_eq!(ErrorCode::from_i16(87), ErrorCode::InvalidRecord);
        assert_eq!(ErrorCode::from_i16(88), ErrorCode::UnstableOffsetCommit);
    }

    #[test]
    fn test_error_code_to_i16_codes_56_to_88() {
        assert_eq!(ErrorCode::KafkaStorageException.to_i16(), 56);
        assert_eq!(ErrorCode::LogDirNotFound.to_i16(), 57);
        assert_eq!(ErrorCode::SaslAuthenticationFailed.to_i16(), 58);
        assert_eq!(ErrorCode::UnknownProducerId.to_i16(), 59);
        assert_eq!(ErrorCode::ReassignmentInProgress.to_i16(), 60);
        assert_eq!(ErrorCode::DelegationTokenAuthDisabled.to_i16(), 61);
        assert_eq!(ErrorCode::DelegationTokenNotFound.to_i16(), 62);
        assert_eq!(ErrorCode::DelegationTokenOwnerMismatch.to_i16(), 63);
        assert_eq!(ErrorCode::DelegationTokenRequestNotAllowed.to_i16(), 64);
        assert_eq!(ErrorCode::DelegationTokenAuthorizationFailed.to_i16(), 65);
        assert_eq!(ErrorCode::DelegationTokenExpired.to_i16(), 66);
        assert_eq!(ErrorCode::InvalidPrincipalType.to_i16(), 67);
        assert_eq!(ErrorCode::NonEmptyGroup.to_i16(), 68);
        assert_eq!(ErrorCode::GroupIdNotFound.to_i16(), 69);
        assert_eq!(ErrorCode::FetchSessionIdNotFound.to_i16(), 70);
        assert_eq!(ErrorCode::InvalidFetchSessionEpoch.to_i16(), 71);
        assert_eq!(ErrorCode::ListenerNotFound.to_i16(), 72);
        assert_eq!(ErrorCode::TopicDeletionDisabled.to_i16(), 73);
        assert_eq!(ErrorCode::FencedLeaderEpoch.to_i16(), 74);
        assert_eq!(ErrorCode::UnknownLeaderEpoch.to_i16(), 75);
        assert_eq!(ErrorCode::UnsupportedCompressionType.to_i16(), 76);
        assert_eq!(ErrorCode::StaleBrokerEpoch.to_i16(), 77);
        assert_eq!(ErrorCode::OffsetNotAvailable.to_i16(), 78);
        assert_eq!(ErrorCode::MemberIdRequired.to_i16(), 79);
        assert_eq!(ErrorCode::PreferredLeaderNotAvailable.to_i16(), 80);
        assert_eq!(ErrorCode::GroupMaxSizeReached.to_i16(), 81);
        assert_eq!(ErrorCode::FencedInstanceId.to_i16(), 82);
        assert_eq!(ErrorCode::EligibleLeadersNotAvailable.to_i16(), 83);
        assert_eq!(ErrorCode::ElectionNotNeeded.to_i16(), 84);
        assert_eq!(ErrorCode::NoReassignmentInProgress.to_i16(), 85);
        assert_eq!(ErrorCode::GroupSubscribedToTopic.to_i16(), 86);
        assert_eq!(ErrorCode::InvalidRecord.to_i16(), 87);
        assert_eq!(ErrorCode::UnstableOffsetCommit.to_i16(), 88);
    }

    // ── R9.10: new retriable codes ──

    #[test]
    fn test_r9_10_new_retriable_error_codes() {
        // These new codes from 56–88 should be retriable
        assert!(ErrorCode::KafkaStorageException.is_retriable());
        assert!(ErrorCode::UnknownProducerId.is_retriable());
        assert!(ErrorCode::FetchSessionIdNotFound.is_retriable());
        assert!(ErrorCode::InvalidFetchSessionEpoch.is_retriable());
        assert!(ErrorCode::FencedLeaderEpoch.is_retriable());
        assert!(ErrorCode::UnknownLeaderEpoch.is_retriable());
        assert!(ErrorCode::OffsetNotAvailable.is_retriable());
        assert!(ErrorCode::PreferredLeaderNotAvailable.is_retriable());
        assert!(ErrorCode::EligibleLeadersNotAvailable.is_retriable());
        assert!(ErrorCode::UnstableOffsetCommit.is_retriable());
    }

    #[test]
    fn test_r9_10_non_retriable_error_codes_56_to_88() {
        // These new codes from 56–88 should NOT be retriable
        assert!(!ErrorCode::LogDirNotFound.is_retriable());
        assert!(!ErrorCode::SaslAuthenticationFailed.is_retriable());
        assert!(!ErrorCode::ReassignmentInProgress.is_retriable());
        assert!(!ErrorCode::DelegationTokenAuthDisabled.is_retriable());
        assert!(!ErrorCode::DelegationTokenNotFound.is_retriable());
        assert!(!ErrorCode::DelegationTokenOwnerMismatch.is_retriable());
        assert!(!ErrorCode::DelegationTokenRequestNotAllowed.is_retriable());
        assert!(!ErrorCode::DelegationTokenAuthorizationFailed.is_retriable());
        assert!(!ErrorCode::DelegationTokenExpired.is_retriable());
        assert!(!ErrorCode::InvalidPrincipalType.is_retriable());
        assert!(!ErrorCode::NonEmptyGroup.is_retriable());
        assert!(!ErrorCode::GroupIdNotFound.is_retriable());
        assert!(!ErrorCode::ListenerNotFound.is_retriable());
        assert!(!ErrorCode::TopicDeletionDisabled.is_retriable());
        assert!(!ErrorCode::UnsupportedCompressionType.is_retriable());
        assert!(!ErrorCode::StaleBrokerEpoch.is_retriable());
        assert!(!ErrorCode::MemberIdRequired.is_retriable());
        assert!(!ErrorCode::GroupMaxSizeReached.is_retriable());
        assert!(!ErrorCode::FencedInstanceId.is_retriable());
        assert!(!ErrorCode::ElectionNotNeeded.is_retriable());
        assert!(!ErrorCode::NoReassignmentInProgress.is_retriable());
        assert!(!ErrorCode::GroupSubscribedToTopic.is_retriable());
        assert!(!ErrorCode::InvalidRecord.is_retriable());
    }

    // ── R9.10: round-trip through From<i16> / From<ErrorCode> traits ──

    #[test]
    fn test_error_code_round_trip_56_to_88() {
        for code in 56i16..=88 {
            let ec = ErrorCode::from(code);
            let back: i16 = ec.into();
            assert_eq!(back, code, "round-trip failed for code {}", code);
        }
    }
}
