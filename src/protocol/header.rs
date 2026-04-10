//! Kafka protocol request and response headers.
//!
//! All Kafka requests and responses are framed with headers that contain
//! metadata like API key, version, correlation ID, etc.

use bytes::{Buf, BufMut};

use super::api::ApiKey;
use super::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::error::{KrafkaError, Result};

/// Request header for Kafka protocol.
///
/// The header format varies based on the header version:
/// - v0: api_key, api_version, correlation_id
/// - v1: api_key, api_version, correlation_id, client_id
/// - v2: api_key, api_version, correlation_id, client_id, tagged_fields (flexible)
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RequestHeader {
    /// The API key for the request.
    pub api_key: ApiKey,
    /// The API version for the request.
    pub api_version: i16,
    /// The correlation ID for request/response matching.
    pub correlation_id: i32,
    /// The client ID.
    pub client_id: Option<KafkaString>,
}

impl RequestHeader {
    /// Create a new request header.
    pub fn new(api_key: ApiKey, api_version: i16, correlation_id: i32) -> Self {
        Self {
            api_key,
            api_version,
            correlation_id,
            client_id: None,
        }
    }

    /// Set the client ID.
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(KafkaString::new(client_id));
        self
    }

    /// Encode the header for header version 1.
    #[inline]
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
        match &self.client_id {
            Some(client_id) => client_id.try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        Ok(())
    }

    /// Encode the header for header version 2 (flexible).
    ///
    /// Per the Kafka protocol spec, `ClientId` has `flexibleVersions: "none"`:
    /// it is always serialized with the old-style two-byte length prefix, even
    /// in header v2, so that older brokers can still parse ApiVersionsRequest
    /// headers from newer clients.
    #[inline]
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
        // ClientId uses standard (non-compact) encoding — see doc comment.
        match &self.client_id {
            Some(client_id) => client_id.try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Determine the request header version based on the API key and version.
    ///
    /// Each Kafka API transitions to flexible encoding (compact strings +
    /// tagged fields) at a version defined by `ApiKey::flexible_version()`.
    /// Below that threshold → header v1 (standard strings);
    /// at or above → header v2 (flexible).
    pub fn header_version(api_key: ApiKey, api_version: i16) -> i16 {
        if api_version >= api_key.flexible_version() {
            2
        } else {
            1
        }
    }

    /// Encode the header using the appropriate version.
    ///
    /// `header_version()` returns 1 (non-flexible) or 2 (flexible);
    /// v0 is unused because all APIs use client_id in requests.
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let header_version = Self::header_version(self.api_key, self.api_version);
        match header_version {
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            v => {
                return Err(KrafkaError::protocol(format!(
                    "unsupported request header version {v}"
                )));
            }
        }
        Ok(())
    }
}

/// Response header for Kafka protocol.
///
/// The header format varies based on the header version:
/// - v0: correlation_id
/// - v1: correlation_id, tagged_fields (flexible)
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    /// The correlation ID matching the request.
    pub correlation_id: i32,
}

impl ResponseHeader {
    /// Create a new response header.
    pub fn new(correlation_id: i32) -> Self {
        Self { correlation_id }
    }

    /// Decode the header for header version 0.
    #[inline]
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self {
            correlation_id: i32::decode(buf)?,
        })
    }

    /// Decode the header for header version 1 (flexible).
    #[inline]
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let correlation_id = i32::decode(buf)?;
        // Skip tagged fields
        let _ = TaggedFields::decode(buf)?;
        Ok(Self { correlation_id })
    }

    /// Determine the response header version based on the API key and version.
    ///
    /// Below `ApiKey::flexible_version()` → header v0 (correlation_id only);
    /// at or above → header v1 (correlation_id + tagged fields).
    ///
    /// **Exception:** ApiVersions always uses response header v0 regardless
    /// of the API version (needed for protocol bootstrapping).
    pub fn header_version(api_key: ApiKey, api_version: i16) -> i16 {
        if api_key == ApiKey::ApiVersions {
            return 0;
        }
        if api_version >= api_key.flexible_version() {
            1
        } else {
            0
        }
    }

    /// Decode the header using the appropriate version.
    pub fn decode(buf: &mut impl Buf, api_key: ApiKey, api_version: i16) -> Result<Self> {
        let header_version = Self::header_version(api_key, api_version);
        match header_version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            v => Err(KrafkaError::protocol(format!(
                "unsupported response header version {v}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_request_header_v1_without_client_id() {
        // Header v1 without client_id encodes api_key, api_version,
        // correlation_id, then a null string (2-byte length = -1).
        let header = RequestHeader::new(ApiKey::ApiVersions, 0, 1);
        let mut buf = BytesMut::new();
        header.encode_v1(&mut buf).unwrap();

        let mut buf = buf.freeze();
        assert_eq!(i16::decode(&mut buf).unwrap(), 18); // ApiVersions = 18
        assert_eq!(i16::decode(&mut buf).unwrap(), 0); // version
        assert_eq!(i32::decode(&mut buf).unwrap(), 1); // correlation_id
        let client_id = KafkaString::decode(&mut buf).unwrap();
        assert!(client_id.is_null());
    }

    #[test]
    fn test_request_header_v1() {
        let header = RequestHeader::new(ApiKey::Metadata, 0, 42).with_client_id("test-client");
        let mut buf = BytesMut::new();
        header.encode_v1(&mut buf).unwrap();

        let mut buf = buf.freeze();
        assert_eq!(i16::decode(&mut buf).unwrap(), 3); // Metadata = 3
        assert_eq!(i16::decode(&mut buf).unwrap(), 0); // version
        assert_eq!(i32::decode(&mut buf).unwrap(), 42); // correlation_id
        let client_id = KafkaString::decode(&mut buf).unwrap();
        assert_eq!(client_id.as_str(), Some("test-client"));
    }

    /// Header v2 must use standard (2-byte i16) encoding for ClientId,
    /// NOT compact (varint), because `flexibleVersions: "none"` in the spec.
    #[test]
    fn test_request_header_v2_client_id_uses_standard_encoding() {
        let header = RequestHeader::new(ApiKey::Metadata, 12, 99).with_client_id("krafka-client");
        let mut buf = BytesMut::new();
        header.encode_v2(&mut buf).unwrap();

        let mut buf = buf.freeze();
        assert_eq!(i16::decode(&mut buf).unwrap(), 3); // Metadata = 3
        assert_eq!(i16::decode(&mut buf).unwrap(), 12); // version
        assert_eq!(i32::decode(&mut buf).unwrap(), 99); // correlation_id
        // ClientId: standard 2-byte i16 length prefix, NOT compact varint.
        let client_id = KafkaString::decode(&mut buf).unwrap();
        assert_eq!(client_id.as_str(), Some("krafka-client"));
        // Trailing tagged fields (empty = single 0x00 byte).
        let tf = TaggedFields::decode(&mut buf).unwrap();
        assert!(tf.0.is_empty());
        assert!(!buf.has_remaining(), "no trailing bytes");
    }

    #[test]
    fn test_request_header_v2_without_client_id() {
        let header = RequestHeader::new(ApiKey::Metadata, 12, 1);
        let mut buf = BytesMut::new();
        header.encode_v2(&mut buf).unwrap();

        let mut buf = buf.freeze();
        assert_eq!(i16::decode(&mut buf).unwrap(), 3);
        assert_eq!(i16::decode(&mut buf).unwrap(), 12);
        assert_eq!(i32::decode(&mut buf).unwrap(), 1);
        // Null ClientId: standard encoding = i16(-1).
        let client_id = KafkaString::decode(&mut buf).unwrap();
        assert!(client_id.is_null());
        let _ = TaggedFields::decode(&mut buf).unwrap();
        assert!(!buf.has_remaining());
    }

    #[test]
    fn test_response_header_v0() {
        let mut buf = BytesMut::new();
        42i32.encode(&mut buf);

        let header = ResponseHeader::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(header.correlation_id, 42);
    }

    #[test]
    fn test_response_header_v1() {
        let mut buf = BytesMut::new();
        42i32.encode(&mut buf);
        TaggedFields::default().try_encode(&mut buf).unwrap();

        let header = ResponseHeader::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(header.correlation_id, 42);
    }

    #[test]
    fn test_header_version_api_versions() {
        // ApiVersions v0-2 uses header v1
        assert_eq!(RequestHeader::header_version(ApiKey::ApiVersions, 0), 1);
        assert_eq!(RequestHeader::header_version(ApiKey::ApiVersions, 2), 1);
        // ApiVersions v3+ uses header v2
        assert_eq!(RequestHeader::header_version(ApiKey::ApiVersions, 3), 2);

        // ApiVersions always uses response header v0
        assert_eq!(ResponseHeader::header_version(ApiKey::ApiVersions, 0), 0);
        assert_eq!(ResponseHeader::header_version(ApiKey::ApiVersions, 3), 0);
    }

    #[test]
    fn test_header_version_fetch() {
        // Fetch becomes flexible at v12. Versions 0-11 must use non-flexible headers.
        for v in 0..12 {
            assert_eq!(
                RequestHeader::header_version(ApiKey::Fetch, v),
                1,
                "Fetch v{v} request header should be v1 (non-flexible)"
            );
            assert_eq!(
                ResponseHeader::header_version(ApiKey::Fetch, v),
                0,
                "Fetch v{v} response header should be v0 (non-flexible)"
            );
        }
        // v12+ uses flexible headers
        assert_eq!(RequestHeader::header_version(ApiKey::Fetch, 12), 2);
        assert_eq!(ResponseHeader::header_version(ApiKey::Fetch, 12), 1);
    }

    /// Verify header versions at the flexible boundary for every API we use.
    #[test]
    fn test_header_version_flexible_boundaries() {
        // APIs krafka sends requests for; the flexible boundary is derived
        // from `ApiKey::flexible_version()` to avoid duplicating that mapping.
        let apis: &[ApiKey] = &[
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::SyncGroup,
            ApiKey::DescribeGroups,
            ApiKey::ListGroups,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
            ApiKey::DeleteRecords,
            ApiKey::InitProducerId,
            ApiKey::OffsetForLeaderEpoch,
            ApiKey::AddPartitionsToTxn,
            ApiKey::AddOffsetsToTxn,
            ApiKey::EndTxn,
            ApiKey::TxnOffsetCommit,
            ApiKey::DescribeAcls,
            ApiKey::CreateAcls,
            ApiKey::DeleteAcls,
            ApiKey::DescribeConfigs,
            ApiKey::AlterConfigs,
            ApiKey::CreatePartitions,
            ApiKey::ApiVersions,
        ];

        for &api in apis {
            let flex = api.flexible_version();

            // One version below the boundary: non-flexible headers.
            if flex > 0 {
                let before = flex - 1;
                assert_eq!(
                    RequestHeader::header_version(api, before),
                    1,
                    "{api:?} v{before} request header should be v1"
                );
                assert_eq!(
                    ResponseHeader::header_version(api, before),
                    0,
                    "{api:?} v{before} response header should be v0"
                );
            }

            // At the boundary: flexible headers.
            assert_eq!(
                RequestHeader::header_version(api, flex),
                2,
                "{api:?} v{flex} request header should be v2"
            );
            // ApiVersions response is special-cased to always return v0.
            let expected_resp = if api == ApiKey::ApiVersions { 0 } else { 1 };
            assert_eq!(
                ResponseHeader::header_version(api, flex),
                expected_resp,
                "{api:?} v{flex} response header mismatch"
            );
        }
    }
}
