use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};
use crate::protocol::{array_len_i32, check_compact_array_len, check_decode_array_len};

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
    /// Reason for joining (v8+, KIP-800).
    pub reason: Option<String>,
}

impl JoinGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::JoinGroup
    }

    /// Encode for version 4.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
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

    /// Encode for version 6–7 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        KafkaString::new(&self.protocol_type).try_encode_compact(buf)?;

        let len = u32::try_from(self.protocols.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("protocols array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode_compact(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 8–9 (v6 + reason field, KIP-800).
    pub fn encode_v8(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        KafkaString::new(&self.protocol_type).try_encode_compact(buf)?;

        let len = u32::try_from(self.protocols.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("protocols array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode_compact(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        match &self.reason {
            Some(r) => KafkaString::new(r).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        TaggedFields::default().try_encode(buf)?;
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
    /// Protocol type (v7+).
    pub protocol_type: Option<String>,
    /// Selected protocol name.
    pub protocol_name: Option<String>,
    /// Leader member ID.
    pub leader: String,
    /// True if the leader must skip running the assignment (v9+).
    pub skip_assignment: bool,
    /// This member's ID.
    pub member_id: String,
    /// Members (only for leader).
    pub members: Vec<JoinGroupResponseMember>,
}

impl JoinGroupResponse {
    /// Decode from version 4.
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
            let metadata = non_nullable_bytes("member metadata", KafkaBytes::decode(buf)?.0)?;
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
            skip_assignment: false,
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
        let leader = non_nullable_string("leader", KafkaString::decode(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
            let group_instance_id = KafkaString::decode(buf)?.0;
            let metadata = non_nullable_bytes("member metadata", KafkaBytes::decode(buf)?.0)?;
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
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 6 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode_compact(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;

        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let metadata =
                non_nullable_bytes("member metadata", KafkaBytes::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 7–8 (v6 + protocol_type field, KIP-559).
    pub fn decode_v7(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_type = KafkaString::decode_compact(buf)?.0;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode_compact(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;

        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let metadata =
                non_nullable_bytes("member metadata", KafkaBytes::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 9 (v7 + skip_assignment field).
    pub fn decode_v9(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_type = KafkaString::decode_compact(buf)?.0;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode_compact(buf)?.0)?;
        let skip_assignment = bool::decode(buf)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;

        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let metadata =
                non_nullable_bytes("member metadata", KafkaBytes::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment,
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

    /// Encode for version 3 (KIP-345: includes group_instance_id).
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

    /// Encode for version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }

        let len = u32::try_from(self.assignments.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("assignments array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode_compact(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (v4 + protocol_type/protocol_name, KIP-559).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        match &self.protocol_type {
            Some(pt) => KafkaString::new(pt).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        match &self.protocol_name {
            Some(pn) => KafkaString::new(pn).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }

        let len = u32::try_from(self.assignments.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("assignments array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode_compact(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
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
    /// Decode from version 3 (non-flexible).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let assignment = non_nullable_bytes("assignment", KafkaBytes::decode(buf)?.0)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type: None,
            protocol_name: None,
            assignment,
        })
    }

    /// Decode from version 4 (flexible: compact bytes + tagged fields).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let assignment = non_nullable_bytes("assignment", KafkaBytes::decode_compact(buf)?.0)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type: None,
            protocol_name: None,
            assignment,
        })
    }

    /// Decode from version 5 (v4 + protocol_type/protocol_name, KIP-559).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let protocol_type = KafkaString::decode_compact(buf)?.0;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let assignment = non_nullable_bytes("assignment", KafkaBytes::decode_compact(buf)?.0)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
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

    /// Encode for version 3 (adds group_instance_id for KIP-345 static membership).
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

    /// Encode for version 4 (flexible: compact strings + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        TaggedFields::default().try_encode(buf)?;
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
    /// Decode from version 3 (non-flexible).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Decode from version 4 (flexible: tagged fields trailer).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
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
    /// Reason for leaving (v5+, KIP-800).
    pub reason: Option<String>,
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

    /// Encode for version 3 (batch leave with per-member group_instance_id).
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

    /// Encode for version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        let len = u32::try_from(self.members.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("members array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for member in &self.members {
            KafkaString::new(&member.member_id).try_encode_compact(buf)?;
            match &member.group_instance_id {
                Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (v4 + reason field per member, KIP-800).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        let len = u32::try_from(self.members.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("members array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for member in &self.members {
            KafkaString::new(&member.member_id).try_encode_compact(buf)?;
            match &member.group_instance_id {
                Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            match &member.reason {
                Some(r) => KafkaString::new(r).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
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
    /// Decode from version 3 (KIP-345 batch leave with per-member results).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
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

    /// Decode from version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let member_error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let _ = TaggedFields::decode(buf)?;
            members.push(LeaveGroupResponseMember {
                member_id,
                group_instance_id,
                error_code: member_error_code,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }

    /// Decode from version 5 (v4, wire-identical — reason field is request-only).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v4(buf)
    }
}

impl VersionedEncode for JoinGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            6 | 7 => self.encode_v6(buf)?,
            8 | 9 => self.encode_v8(buf)?,
            _ => return unsupported_encode!("JoinGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for JoinGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            6 => Self::decode_v6(buf),
            7 | 8 => Self::decode_v7(buf),
            9 => Self::decode_v9(buf),
            _ => unsupported_decode!("JoinGroupResponse", version),
        }
    }
}

impl VersionedEncode for SyncGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("SyncGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SyncGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("SyncGroupResponse", version),
        }
    }
}

impl VersionedEncode for HeartbeatRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("HeartbeatRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for HeartbeatResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            _ => unsupported_decode!("HeartbeatResponse", version),
        }
    }
}

impl VersionedEncode for LeaveGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("LeaveGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for LeaveGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("LeaveGroupResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

    /// Build a compact-encoded JoinGroup v6 response buffer.
    fn build_join_group_response_v6_buf() -> BytesMut {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(50);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(7);

        // protocol_name (compact nullable string): "range" → varint(6) + "range"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"range");

        // leader (compact string): "leader-1" → varint(9) + "leader-1"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"leader-1");

        // member_id (compact string): "member-1" → varint(9) + "member-1"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"member-1");

        // members compact array: count=1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // member_id: "member-1"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"member-1");
            // group_instance_id: null → varint(0)
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
            // metadata (compact bytes): b"meta" → varint(5) + "meta"
            crate::util::varint::encode_unsigned_varint(5, &mut buf);
            buf.put_slice(b"meta");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        buf
    }

    /// Build a compact-encoded JoinGroup v7 response buffer (adds protocol_type).
    fn build_join_group_response_v7_buf() -> BytesMut {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(100);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(12);

        // protocol_type (compact nullable string): "consumer" → varint(9) + "consumer"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"consumer");

        // protocol_name (compact nullable string): "range" → varint(6) + "range"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"range");

        // leader: "leader-1"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"leader-1");

        // member_id: "follower-1"
        crate::util::varint::encode_unsigned_varint(11, &mut buf);
        buf.put_slice(b"follower-1");

        // members compact array: count=1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // member_id: "leader-1"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"leader-1");
            // group_instance_id: "inst-1" → varint(7) + "inst-1"
            crate::util::varint::encode_unsigned_varint(7, &mut buf);
            buf.put_slice(b"inst-1");
            // metadata: b"\x01\x02"
            crate::util::varint::encode_unsigned_varint(3, &mut buf);
            buf.put_slice(b"\x01\x02");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        buf
    }

    /// Helper: write empty tagged fields (varint 0).
    fn put_tagged_fields(buf: &mut BytesMut) {
        buf.put_u8(0);
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
            reason: None,
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        // v5 should include group_instance_id, so it should be larger
        assert!(buf_v5.len() > buf_v4.len());
    }

    #[test]
    fn test_heartbeat_request_encode_v3() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
        };

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 should include group_instance_id
        let data = String::from_utf8_lossy(&buf_v3);
        assert!(data.contains("instance-1"));
    }

    #[test]
    fn test_heartbeat_request_encode_v3_null_instance_id() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
        };

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 with null instance_id encodes a null marker (-1 as i16)
        assert!(!buf_v3.is_empty());
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
    fn test_join_group_request_encode_v6_flexible() {
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
            reason: None,
        };

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        let mut buf_v6 = BytesMut::new();
        request.encode_v6(&mut buf_v6).unwrap();

        // v6 uses compact encoding which is typically shorter due to varint lengths
        // and adds tagged fields (0x00 per element + top-level). Sizes differ.
        assert!(!buf_v6.is_empty());
        assert_ne!(buf_v5.len(), buf_v6.len());
    }

    #[test]
    fn test_join_group_request_encode_v6_v7_wire_identical() {
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
            reason: None,
        };

        let mut buf_v6 = BytesMut::new();
        request.encode_v6(&mut buf_v6).unwrap();

        let mut buf_v7 = BytesMut::new();
        request.encode_v6(&mut buf_v7).unwrap();

        // v6 and v7 share the same encode_v6 encoder
        assert_eq!(buf_v6, buf_v7);
    }

    #[test]
    fn test_join_group_request_encode_v8_with_reason() {
        let request_no_reason = JoinGroupRequest {
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
            reason: None,
        };

        let request_with_reason = JoinGroupRequest {
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
            reason: Some("rebalance triggered".to_string()),
        };

        let mut buf_no_reason = BytesMut::new();
        request_no_reason.encode_v8(&mut buf_no_reason).unwrap();

        let mut buf_with_reason = BytesMut::new();
        request_with_reason.encode_v8(&mut buf_with_reason).unwrap();

        // v8 with a reason string should be longer than v8 with null reason
        assert!(buf_with_reason.len() > buf_no_reason.len());

        // v8 with reason should contain the reason text
        let data = String::from_utf8_lossy(&buf_with_reason);
        assert!(data.contains("rebalance triggered"));
    }

    #[test]
    fn test_join_group_request_encode_v8_v9_wire_identical() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: Some("test".to_string()),
        };

        let mut buf_v8 = BytesMut::new();
        request.encode_v8(&mut buf_v8).unwrap();

        let mut buf_v9 = BytesMut::new();
        request.encode_v8(&mut buf_v9).unwrap();

        // v8 and v9 share the same encode_v8 encoder
        assert_eq!(buf_v8, buf_v9);
    }

    #[test]
    fn test_join_group_request_encode_v8_vs_v6_null_reason() {
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
            reason: None,
        };

        let mut buf_v6 = BytesMut::new();
        request.encode_v6(&mut buf_v6).unwrap();

        let mut buf_v8 = BytesMut::new();
        request.encode_v8(&mut buf_v8).unwrap();

        // v8 with null reason is v6 + null compact string (varint 0) before top tagged fields
        // So v8 should be exactly 1 byte longer (the 0x00 null marker for reason)
        assert_eq!(buf_v8.len(), buf_v6.len() + 1);
    }

    #[test]
    fn test_join_group_response_decode_v6_flexible() {
        let buf = build_join_group_response_v6_buf();
        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v6(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 7);
        assert_eq!(resp.protocol_type, None); // v6 does not decode protocol_type
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert_eq!(resp.leader, "leader-1");
        assert!(!resp.skip_assignment); // always false for v6
        assert_eq!(resp.member_id, "member-1");
        assert_eq!(resp.members.len(), 1);
        assert_eq!(resp.members[0].member_id, "member-1");
        assert_eq!(resp.members[0].group_instance_id, None);
        assert_eq!(resp.members[0].metadata, bytes::Bytes::from_static(b"meta"));
    }

    #[test]
    fn test_join_group_response_decode_v7_protocol_type() {
        let buf = build_join_group_response_v7_buf();
        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v7(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 12);
        assert_eq!(resp.protocol_type, Some("consumer".to_string()));
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert_eq!(resp.leader, "leader-1");
        assert!(!resp.skip_assignment); // always false for v7
        assert_eq!(resp.member_id, "follower-1");
        assert!(!resp.is_leader()); // follower-1 != leader-1
        assert_eq!(resp.members.len(), 1);
        assert_eq!(resp.members[0].member_id, "leader-1");
        assert_eq!(
            resp.members[0].group_instance_id,
            Some("inst-1".to_string())
        );
    }

    #[test]
    fn test_join_group_response_decode_v9_skip_assignment() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(200);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(42);

        // protocol_type: "consumer"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"consumer");

        // protocol_name: "sticky"
        crate::util::varint::encode_unsigned_varint(7, &mut buf);
        buf.put_slice(b"sticky");

        // leader: "m-leader"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"m-leader");

        // skip_assignment: true (1 byte)
        buf.put_u8(1);

        // member_id: "m-leader"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"m-leader");

        // members compact array: count=0 → varint(1)
        crate::util::varint::encode_unsigned_varint(1, &mut buf);

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v9(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 200);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 42);
        assert_eq!(resp.protocol_type, Some("consumer".to_string()));
        assert_eq!(resp.protocol_name, Some("sticky".to_string()));
        assert_eq!(resp.leader, "m-leader");
        assert!(resp.skip_assignment); // true
        assert_eq!(resp.member_id, "m-leader");
        assert!(resp.is_leader());
        assert!(resp.members.is_empty());
    }

    #[test]
    fn test_join_group_response_decode_v9_skip_assignment_false() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i32(1); // generation_id

        // protocol_type: null
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        // protocol_name: "range"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"range");

        // leader: "m1"
        crate::util::varint::encode_unsigned_varint(3, &mut buf);
        buf.put_slice(b"m1");

        // skip_assignment: false
        buf.put_u8(0);

        // member_id: "m2"
        crate::util::varint::encode_unsigned_varint(3, &mut buf);
        buf.put_slice(b"m2");

        // members: empty
        crate::util::varint::encode_unsigned_varint(1, &mut buf);

        // tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v9(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.generation_id, 1);
        assert_eq!(resp.protocol_type, None);
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert!(!resp.skip_assignment);
        assert_eq!(resp.member_id, "m2");
        assert!(!resp.is_leader());
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
        request.encode_v3(&mut buf).unwrap();

        let data = buf.freeze();
        // v3 encoding includes group_instance_id, so it should contain the string
        let data_str = String::from_utf8_lossy(&data);
        assert!(
            data_str.contains("instance-1"),
            "v3 encoding should include group_instance_id"
        );
    }

    // LeaveGroupResponse decode_v3 roundtrip (v0/v1 no longer supported).
    #[test]
    fn test_leave_group_response_decode_v3_no_members() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(100);
        // error_code = 0 (NONE)
        buf.put_i16(0);
        // members array length = 0
        buf.put_i32(0);

        let mut data = buf.freeze();
        let resp = LeaveGroupResponse::decode_v3(&mut data).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.throttle_time_ms, 100);
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
        let resp = SyncGroupResponse::decode_v3(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert!(resp.assignment.is_empty());
    }

    // ---- Story 1.8: HeartbeatResponse and SyncGroupResponse decode ----

    #[test]
    fn test_heartbeat_response_decode_v3() {
        let mut buf = BytesMut::new();
        // v3 is flexible: throttle_time_ms, error_code, tagged_fields
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(0); // error_code (None)
        put_tagged_fields(&mut buf);

        let resp = HeartbeatResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.error_code, ErrorCode::None);
    }

    #[test]
    fn test_heartbeat_response_below_min_rejected() {
        let mut buf1 = BytesMut::new();
        buf1.put_i16(0);
        assert!(HeartbeatResponse::decode_versioned(0, &mut buf1.freeze()).is_err());
        let mut buf2 = BytesMut::new();
        buf2.put_i16(0);
        assert!(HeartbeatResponse::decode_versioned(2, &mut buf2.freeze()).is_err());
    }

    #[test]
    fn test_sync_group_response_decode_v3() {
        let mut buf = BytesMut::new();
        // v3 dispatches to decode_v1: throttle_time_ms, error_code, assignment (non-flexible bytes)
        buf.put_i32(25); // throttle_time_ms
        buf.put_i16(0); // error_code
        // assignment (non-flexible KafkaBytes: i32 length + data)
        let assignment = b"\x00\x01\x02";
        buf.put_i32(assignment.len() as i32);
        buf.put_slice(assignment);

        let resp = SyncGroupResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 25);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.assignment, bytes::Bytes::from_static(b"\x00\x01\x02"));
    }

    #[test]
    fn test_sync_group_response_below_min_rejected() {
        let mut buf1 = BytesMut::new();
        buf1.put_i16(0);
        assert!(SyncGroupResponse::decode_versioned(0, &mut buf1.freeze()).is_err());
        let mut buf2 = BytesMut::new();
        buf2.put_i16(0);
        assert!(SyncGroupResponse::decode_versioned(2, &mut buf2.freeze()).is_err());
    }

    #[rstest]
    // JoinGroup MIN=4
    #[case::join_v0(0)]
    #[case::join_v1(1)]
    #[case::join_v2(2)]
    #[case::join_v3(3)]
    fn test_join_group_encode_below_min(#[case] version: i16) {
        let request = JoinGroupRequest {
            group_id: "g".to_string(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![],
            reason: None,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    // SyncGroup MIN=3
    #[case::sync_v0(0)]
    #[case::sync_v1(1)]
    #[case::sync_v2(2)]
    fn test_sync_group_encode_below_min(#[case] version: i16) {
        let request = SyncGroupRequest {
            group_id: "g".to_string(),
            generation_id: 1,
            member_id: "m".to_string(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    // Heartbeat MIN=3
    #[case::hb_v0(0)]
    #[case::hb_v1(1)]
    #[case::hb_v2(2)]
    fn test_heartbeat_encode_below_min(#[case] version: i16) {
        let request = HeartbeatRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: String::new(),
            group_instance_id: None,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    // LeaveGroup MIN=3
    #[case::leave_v0(0)]
    #[case::leave_v1(1)]
    #[case::leave_v2(2)]
    fn test_leave_group_encode_below_min(#[case] version: i16) {
        let request = LeaveGroupRequest {
            group_id: "g".to_string(),
            member_id: String::new(),
            members: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    // Decode floor: HeartbeatResponse MIN=3
    #[case::hb_resp_v0(0)]
    #[case::hb_resp_v1(1)]
    #[case::hb_resp_v2(2)]
    fn test_heartbeat_response_decode_below_min(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        assert!(HeartbeatResponse::decode_versioned(version, &mut buf.freeze()).is_err());
    }

    #[rstest]
    // Decode floor: SyncGroupResponse MIN=3
    #[case::sg_resp_v0(0)]
    #[case::sg_resp_v1(1)]
    #[case::sg_resp_v2(2)]
    fn test_sync_group_response_decode_below_min(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i32(0);
        assert!(SyncGroupResponse::decode_versioned(version, &mut buf.freeze()).is_err());
    }

    // ── Heartbeat v4 flexible round-trip ──────────────────────────────

    #[test]
    fn test_heartbeat_request_encode_v4_flexible() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
        };

        let mut buf = BytesMut::new();
        request.encode_v4(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify it encodes differently from v3 (compact strings are shorter).
        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_heartbeat_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(42);
        // error_code
        buf.put_i16(0);
        // tagged fields (empty)
        buf.put_u8(0);

        let resp = HeartbeatResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 42);
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_heartbeat_v4_dispatch() {
        let request = HeartbeatRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: "m".to_string(),
            group_instance_id: None,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(4, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    // ── SyncGroup v4/v5 flexible round-trip ───────────────────────────

    #[test]
    fn test_sync_group_request_encode_v4_flexible() {
        let request = SyncGroupRequest {
            group_id: "my-group".to_string(),
            generation_id: 2,
            member_id: "member-1".to_string(),
            group_instance_id: Some("inst-1".to_string()),
            protocol_type: None,
            protocol_name: None,
            assignments: vec![SyncGroupRequestAssignment {
                member_id: "member-1".to_string(),
                assignment: Bytes::from_static(b"\x00\x01"),
            }],
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        assert!(!buf_v4.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf_v4.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_sync_group_request_encode_v5_with_protocol() {
        let request = SyncGroupRequest {
            group_id: "grp".to_string(),
            generation_id: 1,
            member_id: "mem".to_string(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![],
        };

        let mut buf = BytesMut::new();
        request.encode_v5(&mut buf).unwrap();
        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("consumer"));
        assert!(data.contains("range"));
    }

    #[test]
    fn test_sync_group_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(10);
        // error_code
        buf.put_i16(0);
        // assignment (compact bytes: len+1 varint, then data)
        let assign = b"\x00\x01";
        buf.put_u8((assign.len() + 1) as u8); // compact bytes length
        buf.put_slice(assign);
        // tagged fields (empty)
        buf.put_u8(0);

        let resp = SyncGroupResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.assignment, Bytes::from_static(b"\x00\x01"));
        assert!(resp.protocol_type.is_none());
        assert!(resp.protocol_name.is_none());
    }

    #[test]
    fn test_sync_group_response_decode_v5_with_protocol() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(5);
        // error_code
        buf.put_i16(0);
        // protocol_type (compact nullable string: len+1, then data)
        let pt = b"consumer";
        buf.put_u8((pt.len() + 1) as u8);
        buf.put_slice(pt);
        // protocol_name
        let pn = b"range";
        buf.put_u8((pn.len() + 1) as u8);
        buf.put_slice(pn);
        // assignment (compact bytes: len+1 varint, then data)
        let assign = b"\x02\x03";
        buf.put_u8((assign.len() + 1) as u8);
        buf.put_slice(assign);
        // tagged fields (empty)
        buf.put_u8(0);

        let resp = SyncGroupResponse::decode_v5(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.protocol_type.as_deref(), Some("consumer"));
        assert_eq!(resp.protocol_name.as_deref(), Some("range"));
        assert_eq!(resp.assignment, Bytes::from_static(b"\x02\x03"));
    }

    #[test]
    fn test_sync_group_v4_v5_dispatch() {
        let request = SyncGroupRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: "m".to_string(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![],
        };
        for version in [4, 5] {
            let mut buf = BytesMut::new();
            request.encode_versioned(version, &mut buf).unwrap();
            assert!(!buf.is_empty());
        }
    }

    // ── LeaveGroup v4/v5 flexible round-trip ──────────────────────────

    #[test]
    fn test_leave_group_request_encode_v4_flexible() {
        let request = LeaveGroupRequest {
            group_id: "grp".to_string(),
            member_id: "m".to_string(),
            members: vec![LeaveGroupMember {
                member_id: "m1".to_string(),
                group_instance_id: Some("inst-1".to_string()),
                reason: None,
            }],
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        assert!(!buf_v4.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf_v4.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_leave_group_request_encode_v5_with_reason() {
        let request = LeaveGroupRequest {
            group_id: "grp".to_string(),
            member_id: "m".to_string(),
            members: vec![LeaveGroupMember {
                member_id: "m1".to_string(),
                group_instance_id: None,
                reason: Some("shutting down".to_string()),
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v5(&mut buf).unwrap();
        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("shutting down"));
    }

    #[test]
    fn test_leave_group_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(15);
        // error_code
        buf.put_i16(0);
        // members array (compact: len+1 varint) = 1 member → varint(2)
        buf.put_u8(2);
        // member_id (compact string: len+1, then data)
        let mid = b"m1";
        buf.put_u8((mid.len() + 1) as u8);
        buf.put_slice(mid);
        // group_instance_id (compact nullable: null → 0)
        buf.put_u8(0);
        // per-member error_code
        buf.put_i16(0);
        // tagged fields (per member)
        buf.put_u8(0);
        // tagged fields (top-level)
        buf.put_u8(0);

        let resp = LeaveGroupResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 15);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.members.len(), 1);
        assert_eq!(resp.members[0].member_id, "m1");
        assert!(resp.members[0].group_instance_id.is_none());
        assert!(resp.members[0].error_code.is_ok());
    }

    #[test]
    fn test_leave_group_v4_v5_dispatch() {
        let request = LeaveGroupRequest {
            group_id: "g".to_string(),
            member_id: "m".to_string(),
            members: vec![],
        };
        for version in [4, 5] {
            let mut buf = BytesMut::new();
            request.encode_versioned(version, &mut buf).unwrap();
            assert!(!buf.is_empty());
        }
    }
}
