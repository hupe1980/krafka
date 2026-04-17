use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// DeleteTopics request/response
// ============================================================================

/// DeleteTopics request.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    /// Topic names (v1–v5).
    pub topic_names: Vec<String>,
    /// Topics with optional topic_id for v6+ deletion.
    pub topics: Vec<DeleteTopicState>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

/// Topic entry for v6 DeleteTopics (supports deletion by name or UUID).
#[derive(Debug, Clone)]
pub struct DeleteTopicState {
    /// Topic name (nullable in v6+).
    pub name: Option<String>,
    /// Topic UUID (v6+).
    pub topic_id: [u8; 16],
}

impl DeleteTopicsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DeleteTopics
    }

    /// Encode for version 1–3 (non-flexible).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topic_names.len())?);
        for name in &self.topic_names {
            KafkaString::new(name).try_encode(buf)?;
        }
        self.timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 4–5 (flexible encoding, still TopicNames).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topic_names.len(), buf)?;
        for name in &self.topic_names {
            KafkaString::new(name).try_encode_compact(buf)?;
        }
        self.timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 6 (flexible, Topics array with Name + TopicId).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            match &topic.name {
                Some(n) => KafkaString::new(n).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            buf.put_slice(&topic.topic_id);
            TaggedFields::default().try_encode(buf)?; // per-topic tagged fields
        }
        self.timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Result for a deleted topic.
#[derive(Debug, Clone)]
pub struct DeletableTopicResult {
    /// Topic name (nullable in v6+).
    pub name: Option<String>,
    /// Topic UUID (v6+).
    pub topic_id: Option<[u8; 16]>,
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
    /// Decode from version 1–3 (non-flexible).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses: Self::decode_responses_v1(buf)?,
        })
    }

    /// Decode from version 4 (flexible, no error_message, no topic_id).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let count = check_compact_array_len(raw)?;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaString::decode_compact(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            TaggedFields::decode(buf)?;

            responses.push(DeletableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message: None,
            });
        }

        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    /// Decode from version 5 (flexible, adds error_message).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let count = check_compact_array_len(raw)?;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaString::decode_compact(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            TaggedFields::decode(buf)?;

            responses.push(DeletableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message,
            });
        }

        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    /// Decode from version 6 (flexible, adds topic_id).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let count = check_compact_array_len(raw)?;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaString::decode_compact(buf)?.0;

            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
            }
            let mut id = [0u8; 16];
            buf.copy_to_slice(&mut id);
            let topic_id = if id == [0u8; 16] { None } else { Some(id) };

            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            TaggedFields::decode(buf)?;

            responses.push(DeletableTopicResult {
                name,
                topic_id,
                error_code,
                error_message,
            });
        }

        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    /// Shared responses array decoder for v1–v3 (non-flexible).
    fn decode_responses_v1(buf: &mut impl Buf) -> Result<Vec<DeletableTopicResult>> {
        let response_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(response_count);

        for _ in 0..response_count {
            let name = KafkaString::decode(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);

            responses.push(DeletableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message: None,
            });
        }

        Ok(responses)
    }
}

impl VersionedEncode for DeleteTopicsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1..=3 => self.encode_v1(buf)?,
            4 | 5 => self.encode_v4(buf)?,
            6 => self.encode_v6(buf)?,
            _ => return unsupported_encode!("DeleteTopicsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteTopicsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1..=3 => Self::decode_v1(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            6 => Self::decode_v6(buf),
            _ => unsupported_decode!("DeleteTopicsResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_delete_topics_v2_v3_same_wire_as_v1() {
        let request = DeleteTopicsRequest {
            topic_names: vec!["test".to_string()],
            topics: vec![],
            timeout_ms: 30_000,
        };
        let mut v1 = BytesMut::new();
        request.encode_versioned(1, &mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_versioned(2, &mut v2).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v1, v2);
        assert_eq!(v1, v3);
    }

    #[test]
    fn test_delete_topics_v4_flexible() {
        let request = DeleteTopicsRequest {
            topic_names: vec!["test".to_string()],
            topics: vec![],
            timeout_ms: 30_000,
        };
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v4 = BytesMut::new();
        request.encode_v4(&mut v4).unwrap();
        assert_ne!(v1.len(), v4.len());
        // v4 and v5 share wire format
        let mut v5 = BytesMut::new();
        request.encode_versioned(5, &mut v5).unwrap();
        assert_eq!(v4, v5);
    }

    #[test]
    fn test_delete_topics_v6_with_topic_id() {
        let topic_uuid = [0xAA; 16];
        let request = DeleteTopicsRequest {
            topic_names: vec![],
            topics: vec![DeleteTopicState {
                name: None,
                topic_id: topic_uuid,
            }],
            timeout_ms: 30_000,
        };
        let mut buf = BytesMut::new();
        request.encode_v6(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_topics_response_v4_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // responses: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_i16(0); // error_code
        buf.put_u8(0); // per-response tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteTopicsResponse::decode_v4(&mut frozen).unwrap();
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].name.as_deref(), Some("test"));
        assert!(resp.responses[0].topic_id.is_none());
        assert!(resp.responses[0].error_message.is_none());
    }

    #[test]
    fn test_delete_topics_response_v5_with_error_message() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // responses: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // per-response tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteTopicsResponse::decode_v5(&mut frozen).unwrap();
        assert_eq!(resp.responses[0].name.as_deref(), Some("test"));
        assert!(resp.responses[0].error_message.is_none());
    }

    #[test]
    fn test_delete_topics_response_v6_with_topic_id() {
        let topic_uuid = [0xBB; 16];
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // responses: compact array count=1+1=2
        buf.put_u8(0); // name: compact null (deleted by UUID)
        buf.put_slice(&topic_uuid); // topic_id: 16 bytes
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // per-response tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteTopicsResponse::decode_v6(&mut frozen).unwrap();
        assert!(resp.responses[0].name.is_none());
        assert_eq!(resp.responses[0].topic_id, Some(topic_uuid));
    }
}
