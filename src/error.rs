//! Error types for Krafka.
//!
//! This module provides structured error types for all Krafka operations.

use std::io;

use thiserror::Error;

/// The main error type for Krafka operations.
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
            Self::Unknown(code) => code,
        }
    }

    /// Returns true if this error is retriable.
    #[inline]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::LeaderNotAvailable
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
}
