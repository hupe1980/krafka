use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
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
    /// Enable two-phase commit for transactions (v6+, KIP-939).
    pub enable_2pc: bool,
    /// Keep ongoing prepared transaction instead of aborting (v6+, KIP-939).
    pub keep_prepared_txn: bool,
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
            enable_2pc: false,
            keep_prepared_txn: false,
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
            enable_2pc: false,
            keep_prepared_txn: false,
        }
    }

    /// Encode as version 0–1.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode(buf)?;
        self.transaction_timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible: compact strings + tagged fields).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode_compact(buf)?;
        self.transaction_timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3–5 (flexible + ProducerId/ProducerEpoch for epoch recovery).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode_compact(buf)?;
        self.transaction_timeout_ms.encode(buf);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 6 (KIP-939: two-phase commit).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode_compact(buf)?;
        self.transaction_timeout_ms.encode(buf);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_u8(u8::from(self.enable_2pc));
        buf.put_u8(u8::from(self.keep_prepared_txn));
        TaggedFields::default().try_encode(buf)?;
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
    /// Producer ID for ongoing transaction when KeepPreparedTxn is used (v6+, KIP-939).
    pub ongoing_txn_producer_id: i64,
    /// Producer epoch for ongoing transaction when KeepPreparedTxn is used (v6+, KIP-939).
    pub ongoing_txn_producer_epoch: i16,
}

impl InitProducerIdResponse {
    /// Decode from version 0–1.
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
            ongoing_txn_producer_id: -1,
            ongoing_txn_producer_epoch: -1,
        })
    }

    /// Decode from version 2–5 (flexible: tagged fields appended).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id: -1,
            ongoing_txn_producer_epoch: -1,
        })
    }

    /// Decode from version 6 (KIP-939: two-phase commit).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let ongoing_txn_producer_id = i64::decode(buf)?;
        let ongoing_txn_producer_epoch = i16::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id,
            ongoing_txn_producer_epoch,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

// ============================================================================
// Transaction Messages (API Keys 24-28)
// ============================================================================

/// Transaction result for partition operations.
#[non_exhaustive]
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

    /// Encode as version 0–2.
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

    /// Encode for version 3 (flexible: compact strings, varint arrays, tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 4–5 (batched transactions, broker-compatible).
    ///
    /// v4+ wraps the transaction in a `Transactions` compact array with
    /// `VerifyOnly = false`. The client always sends a single transaction.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        // Transactions compact array: 1 + 1 = varint(2)
        crate::util::varint::encode_unsigned_varint(2, buf);
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_u8(0); // VerifyOnly = false
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
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
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
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

    /// Decode from version 3 (flexible: compact strings, varint arrays, tagged fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(AddPartitionsToTxnPartitionResult {
                    partition,
                    error_code,
                });
            }

            let _ = TaggedFields::decode(buf)?;
            results.push(AddPartitionsToTxnTopicResult { name, partitions });
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 4–5 (batched: `ResultsByTransaction` compact array).
    ///
    /// Extracts results from the first transaction in the batched response.
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let _error_code = i16::decode(buf)?; // top-level error code (v4+)

        let txn_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::new();

        for i in 0..txn_count {
            let _transactional_id = KafkaString::decode_compact(buf)?.0;
            let topic_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;

            for _ in 0..topic_count {
                let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
                let partition_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut partitions = Vec::with_capacity(partition_count);

                for _ in 0..partition_count {
                    let partition = i32::decode(buf)?;
                    let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                    let _ = TaggedFields::decode(buf)?;
                    partitions.push(AddPartitionsToTxnPartitionResult {
                        partition,
                        error_code,
                    });
                }

                let _ = TaggedFields::decode(buf)?;
                if i == 0 {
                    results.push(AddPartitionsToTxnTopicResult { name, partitions });
                }
            }

            let _ = TaggedFields::decode(buf)?;
        }

        let _ = TaggedFields::decode(buf)?;
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

    /// Encode as version 0–2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3–4 (flexible: compact strings + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        KafkaString(Some(self.group_id.clone())).try_encode_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
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
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Decode from version 3–4 (flexible: tagged fields appended).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
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

    /// Encode as version 0–2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.committed.encode(buf);
        Ok(())
    }

    /// Encode for version 3–5 (flexible: compact strings + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.committed.encode(buf);
        TaggedFields::default().try_encode(buf)?;
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
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Decode from version 3–5 (flexible: tagged fields appended).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
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
    /// Generation ID of the consumer (v3+, default -1).
    pub generation_id: i32,
    /// Member ID assigned by the group coordinator (v3+, default "").
    pub member_id: String,
    /// Unique consumer instance ID provided by the end user (v3+, default None).
    pub group_instance_id: Option<String>,
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
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
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

    /// Encode as version 0–1.
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

    /// Encode as version 2 (adds committed leader epoch).
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

    /// Encode as version 3–5 (flexible, adds generation_id, member_id, group_instance_id).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        KafkaString(Some(self.group_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.generation_id.encode(buf);
        KafkaString(Some(self.member_id.clone())).try_encode_compact(buf)?;
        KafkaString(self.group_instance_id.clone()).try_encode_compact(buf)?;
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                KafkaString(partition.metadata.clone()).try_encode_compact(buf)?;
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
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
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
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

    /// Decode from version 3–5 (flexible: compact strings, varint arrays, tagged fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(TxnOffsetCommitPartitionResult {
                    partition,
                    error_code,
                });
            }

            let _ = TaggedFields::decode(buf)?;
            topics.push(TxnOffsetCommitTopicResult { name, partitions });
        }

        let _ = TaggedFields::decode(buf)?;
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

impl VersionedEncode for InitProducerIdRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            3..=5 => self.encode_v3(buf)?,
            6 => self.encode_v6(buf)?,
            _ => return unsupported_encode!("InitProducerIdRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for InitProducerIdResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            2..=5 => Self::decode_v2(buf),
            6 => Self::decode_v6(buf),
            _ => unsupported_decode!("InitProducerIdResponse", version),
        }
    }
}

impl VersionedEncode for AddPartitionsToTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            4 | 5 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("AddPartitionsToTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AddPartitionsToTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3 => Self::decode_v3(buf),
            4 | 5 => Self::decode_v4(buf),
            _ => unsupported_decode!("AddPartitionsToTxnResponse", version),
        }
    }
}

impl VersionedEncode for AddOffsetsToTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 | 4 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("AddOffsetsToTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AddOffsetsToTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3 | 4 => Self::decode_v3(buf),
            _ => unsupported_decode!("AddOffsetsToTxnResponse", version),
        }
    }
}

impl VersionedEncode for EndTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3..=5 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("EndTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for EndTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3..=5 => Self::decode_v3(buf),
            _ => unsupported_decode!("EndTxnResponse", version),
        }
    }
}

impl VersionedEncode for TxnOffsetCommitRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            3..=5 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("TxnOffsetCommitRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for TxnOffsetCommitResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3..=5 => Self::decode_v3(buf),
            _ => unsupported_decode!("TxnOffsetCommitResponse", version),
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

    // ── InitProducerId wire-format tests ──

    #[test]
    fn test_init_producer_id_request_v0_wire_format() {
        let request = InitProducerIdRequest::transactional("txn-1", 30000);
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut cur = &buf[..];
        // nullable string: len=5 "txn-1"
        assert_eq!(cur.get_i16(), 5);
        let mut name = vec![0u8; 5];
        cur.copy_to_slice(&mut name);
        assert_eq!(name, b"txn-1");
        assert_eq!(cur.get_i32(), 30000);
        assert!(cur.is_empty());
    }

    #[test]
    fn test_init_producer_id_request_v1_same_as_v0() {
        let request = InitProducerIdRequest::transactional("t", 1000);
        let mut buf_v0 = BytesMut::new();
        request.encode_versioned(0, &mut buf_v0).unwrap();
        let mut buf_v1 = BytesMut::new();
        request.encode_versioned(1, &mut buf_v1).unwrap();
        assert_eq!(buf_v0, buf_v1);
    }

    #[test]
    fn test_init_producer_id_request_v2_flexible() {
        let request = InitProducerIdRequest::idempotent();
        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        let mut cur = &buf[..];
        // compact nullable string: varint(0) = null
        let len_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(len_varint, 0); // null transactional_id
        assert_eq!(cur.get_i32(), -1); // transaction_timeout_ms
        assert_eq!(cur.get_u8(), 0); // empty tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_init_producer_id_request_v3_includes_pid_epoch() {
        let mut request = InitProducerIdRequest::transactional("txn", 5000);
        request.producer_id = 42;
        request.producer_epoch = 3;
        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();
        let mut cur = &buf[..];
        // compact string: varint(4) then 3 bytes
        let name_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_varint, 4); // len+1=3+1
        let mut name = vec![0u8; 3];
        cur.copy_to_slice(&mut name);
        assert_eq!(name, b"txn");
        assert_eq!(cur.get_i32(), 5000);
        assert_eq!(cur.get_i64(), 42); // producer_id
        assert_eq!(cur.get_i16(), 3); // producer_epoch
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_init_producer_id_request_v3_v5_same_wire(#[case] version: i16) {
        let request = InitProducerIdRequest::transactional("t", 1000);
        let mut buf_v3 = BytesMut::new();
        request.encode_versioned(3, &mut buf_v3).unwrap();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert_eq!(buf, buf_v3, "v{version} encode should equal v3");
    }

    #[test]
    fn test_init_producer_id_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1000); // producer_id
        buf.put_i16(5); // producer_epoch
        let mut data = buf.freeze();
        let resp = InitProducerIdResponse::decode_v0(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, 1000);
        assert_eq!(resp.producer_epoch, 5);
    }

    #[test]
    fn test_init_producer_id_response_decode_v2_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i64(42); // producer_id
        buf.put_i16(1); // producer_epoch
        buf.put_u8(0); // tagged fields
        let mut data = buf.freeze();
        let resp = InitProducerIdResponse::decode_v2(&mut data).unwrap();
        assert_eq!(resp.producer_id, 42);
        assert_eq!(resp.producer_epoch, 1);
    }

    #[rstest]
    #[case::v2(2)]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_init_producer_id_response_v2_v5_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i64(99); // producer_id
        buf.put_i16(7); // producer_epoch
        buf.put_u8(0); // tagged fields
        let mut data = buf.freeze();
        let resp = InitProducerIdResponse::decode_versioned(version, &mut data).unwrap();
        assert_eq!(resp.producer_id, 99);
        assert_eq!(resp.producer_epoch, 7);
    }

    // ===================================================================
    // Epic 10: Transaction API Version Upgrades
    // ===================================================================

    // ── AddPartitionsToTxn (Story 10.1) ──────────────────────────────────

    #[test]
    fn test_add_partitions_to_txn_v0_wire_format() {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5)
            .add_partition("topic1", 0)
            .add_partition("topic1", 1);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut data = buf.freeze();

        // transactional_id (2-byte len + 5 bytes "txn-1")
        let txn_id = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(txn_id, "txn-1");
        assert_eq!(i64::decode(&mut data).unwrap(), 100); // producer_id
        assert_eq!(i16::decode(&mut data).unwrap(), 5); // producer_epoch
        assert_eq!(i32::decode(&mut data).unwrap(), 1); // topics count
        let name = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(name, "topic1");
        assert_eq!(i32::decode(&mut data).unwrap(), 2); // partitions count
        assert_eq!(i32::decode(&mut data).unwrap(), 0); // partition 0
        assert_eq!(i32::decode(&mut data).unwrap(), 1); // partition 1
        assert!(!data.has_remaining());
    }

    #[test]
    fn test_add_partitions_to_txn_v3_flexible() {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5).add_partition("t1", 2);

        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();

        // v3 uses compact strings (varint length) instead of i16 length
        // but also has tagged fields => different size
        assert_ne!(v0.len(), v3.len());

        // Round-trip via dispatch
        let mut buf = BytesMut::new();
        request.encode_versioned(3, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_add_partitions_to_txn_v4_batched() {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5).add_partition("t1", 0);

        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();
        let mut v4 = BytesMut::new();
        request.encode_v4(&mut v4).unwrap();

        // v4 wraps in Transactions array + VerifyOnly bool => larger
        assert!(v4.len() > v3.len());

        // Dispatch routes v4 and v5 to encode_v4
        let mut buf5 = BytesMut::new();
        request.encode_versioned(5, &mut buf5).unwrap();
        assert_eq!(v4.freeze(), buf5.freeze());
    }

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_add_partitions_to_txn_v1_v2_same_as_v0(#[case] version: i16) {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5).add_partition("t1", 0);
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v0.freeze(), vn.freeze());
    }

    #[test]
    fn test_add_partitions_to_txn_response_v0_wire() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i32(1); // topics count
        let name = b"t1";
        buf.put_i16(name.len() as i16);
        buf.put_slice(name);
        buf.put_i32(1); // partitions count
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code (NONE)

        let resp = AddPartitionsToTxnResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].name, "t1");
        assert_eq!(resp.results[0].partitions[0].partition, 0);
        assert!(resp.results[0].partitions[0].error_code.is_ok());
    }

    #[test]
    fn test_add_partitions_to_txn_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // topics: 1 + 1
        // topic name compact string: len+1 as varint
        let name = b"t1";
        crate::util::varint::encode_unsigned_varint(name.len() as u32 + 1, &mut buf);
        buf.put_slice(name);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // partitions: 1 + 1
        buf.put_i32(3); // partition
        buf.put_i16(0); // error_code
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let resp = AddPartitionsToTxnResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.results[0].name, "t1");
        assert_eq!(resp.results[0].partitions[0].partition, 3);
    }

    #[test]
    fn test_add_partitions_to_txn_response_v4_batched() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // top-level error code (v4+)
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // txn count: 1 + 1
        // transactional_id compact string
        let txn = b"txn-1";
        crate::util::varint::encode_unsigned_varint(txn.len() as u32 + 1, &mut buf);
        buf.put_slice(txn);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // topics: 1 + 1
        let topic = b"t1";
        crate::util::varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // partitions: 1 + 1
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // txn tagged fields
        buf.put_u8(0); // top-level tagged fields

        let resp = AddPartitionsToTxnResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].name, "t1");
    }

    // ── AddOffsetsToTxn (Story 10.2) ────────────────────────────────────

    #[test]
    fn test_add_offsets_to_txn_v0_wire_format() {
        let request = AddOffsetsToTxnRequest::new("txn-1", 100, 5, "grp-1");

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut data = buf.freeze();

        let txn_id = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(txn_id, "txn-1");
        assert_eq!(i64::decode(&mut data).unwrap(), 100);
        assert_eq!(i16::decode(&mut data).unwrap(), 5);
        let group = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(group, "grp-1");
        assert!(!data.has_remaining());
    }

    #[test]
    fn test_add_offsets_to_txn_v3_flexible() {
        let request = AddOffsetsToTxnRequest::new("txn-1", 100, 5, "grp-1");

        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();
        assert_ne!(v0.len(), v3.len());

        // v4 uses same wire format as v3
        let mut v4 = BytesMut::new();
        request.encode_versioned(4, &mut v4).unwrap();
        assert_eq!(v3.freeze(), v4.freeze());
    }

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_add_offsets_to_txn_v1_v2_same_as_v0(#[case] version: i16) {
        let request = AddOffsetsToTxnRequest::new("txn-1", 100, 5, "grp-1");
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v0.freeze(), vn.freeze());
    }

    #[test]
    fn test_add_offsets_to_txn_response_v0_wire() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code

        let resp = AddOffsetsToTxnResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_add_offsets_to_txn_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(0); // tagged fields

        let resp = AddOffsetsToTxnResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert!(resp.error_code.is_ok());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    fn test_add_offsets_to_txn_response_v3_v4_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_u8(0);
        let resp = AddOffsetsToTxnResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
    }

    // ── EndTxn (Story 10.3) ─────────────────────────────────────────────

    #[test]
    fn test_end_txn_v0_wire_format() {
        let request = EndTxnRequest::commit("txn-1", 100, 5);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut data = buf.freeze();

        let txn_id = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(txn_id, "txn-1");
        assert_eq!(i64::decode(&mut data).unwrap(), 100);
        assert_eq!(i16::decode(&mut data).unwrap(), 5);
        assert_eq!(u8::from(bool::decode(&mut data).unwrap()), 1); // committed=true
        assert!(!data.has_remaining());
    }

    #[test]
    fn test_end_txn_v3_flexible() {
        let request = EndTxnRequest::commit("txn-1", 100, 5);

        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();

        // v3 appends tagged fields byte even if empty
        // Verify v3 encodes without error and both produce valid output
        assert!(!v3.is_empty());
        assert!(!v0.is_empty());
    }

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_end_txn_v1_v2_same_as_v0(#[case] version: i16) {
        let request = EndTxnRequest::abort("txn-1", 100, 5);
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v0.freeze(), vn.freeze());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_end_txn_v3_v5_same_wire(#[case] version: i16) {
        let request = EndTxnRequest::commit("txn-1", 100, 5);
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v3.freeze(), vn.freeze());
    }

    #[test]
    fn test_end_txn_response_v0_wire() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code

        let resp = EndTxnResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_end_txn_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(0); // tagged fields

        let resp = EndTxnResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert!(resp.error_code.is_ok());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_end_txn_response_v3_v5_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_u8(0);
        let resp = EndTxnResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
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

    // ===================================================================
    // Story 18.5: InitProducerId v6 Round-Trip Test
    // ===================================================================

    #[test]
    fn test_init_producer_id_v6_round_trip() {
        let req = InitProducerIdRequest::idempotent();
        let mut buf = BytesMut::new();
        req.encode_versioned(6, &mut buf).unwrap();
        assert!(!buf.is_empty());

        // Build a v6 response manually.
        let mut resp_buf = BytesMut::new();
        resp_buf.put_i32(5); // throttle_time_ms
        resp_buf.put_i16(0); // error_code (None)
        resp_buf.put_i64(100); // producer_id
        resp_buf.put_i16(1); // producer_epoch
        resp_buf.put_i64(200); // ongoing_txn_producer_id
        resp_buf.put_i16(2); // ongoing_txn_producer_epoch
        varint::encode_unsigned_varint(0, &mut resp_buf); // tagged fields

        let resp = InitProducerIdResponse::decode_versioned(6, &mut resp_buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.producer_id, 100);
        assert_eq!(resp.producer_epoch, 1);
        assert_eq!(resp.ongoing_txn_producer_id, 200);
        assert_eq!(resp.ongoing_txn_producer_epoch, 2);
    }

    #[test]
    fn test_init_producer_id_v2_sets_new_fields_defaults() {
        // v2 decode should set ongoing_txn fields to -1.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i64(42); // producer_id
        buf.put_i16(0); // producer_epoch
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields

        let resp = InitProducerIdResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(resp.ongoing_txn_producer_id, -1);
        assert_eq!(resp.ongoing_txn_producer_epoch, -1);
    }
}
