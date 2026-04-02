//! Kafka protocol request and response headers.
//!
//! All Kafka requests and responses are framed with headers that contain
//! metadata like API key, version, correlation ID, etc.

use bytes::{Buf, BufMut};

use super::api::ApiKey;
use super::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::error::Result;

/// Request header for Kafka protocol.
///
/// The header format varies based on the header version:
/// - v0: api_key, api_version, correlation_id
/// - v1: api_key, api_version, correlation_id, client_id
/// - v2: api_key, api_version, correlation_id, client_id, tagged_fields (flexible)
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

    /// Encode the header for header version 0.
    #[inline]
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
        Ok(())
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
    #[inline]
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
        match &self.client_id {
            Some(client_id) => client_id.try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
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
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let header_version = Self::header_version(self.api_key, self.api_version);
        match header_version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => self.encode_v2(buf)?,
        }
        Ok(())
    }
}

/// Response header for Kafka protocol.
///
/// The header format varies based on the header version:
/// - v0: correlation_id
/// - v1: correlation_id, tagged_fields (flexible)
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
            _ => Self::decode_v1(buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_request_header_v0() {
        let header = RequestHeader::new(ApiKey::ApiVersions, 0, 1);
        let mut buf = BytesMut::new();
        header.encode_v0(&mut buf).unwrap();

        let mut buf = buf.freeze();
        assert_eq!(i16::decode(&mut buf).unwrap(), 18); // ApiVersions = 18
        assert_eq!(i16::decode(&mut buf).unwrap(), 0); // version
        assert_eq!(i32::decode(&mut buf).unwrap(), 1); // correlation_id
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
        // (api_key, flexible_version) for all APIs krafka sends requests for.
        let apis: &[(ApiKey, i16)] = &[
            (ApiKey::Produce, 9),
            (ApiKey::Fetch, 12),
            (ApiKey::ListOffsets, 6),
            (ApiKey::Metadata, 9),
            (ApiKey::OffsetCommit, 8),
            (ApiKey::OffsetFetch, 6),
            (ApiKey::FindCoordinator, 3),
            (ApiKey::JoinGroup, 6),
            (ApiKey::Heartbeat, 4),
            (ApiKey::LeaveGroup, 4),
            (ApiKey::SyncGroup, 4),
            (ApiKey::DescribeGroups, 5),
            (ApiKey::ListGroups, 3),
            (ApiKey::CreateTopics, 5),
            (ApiKey::DeleteTopics, 4),
            (ApiKey::DeleteRecords, 2),
            (ApiKey::InitProducerId, 2),
            (ApiKey::OffsetForLeaderEpoch, 4),
            (ApiKey::AddPartitionsToTxn, 4),
            (ApiKey::AddOffsetsToTxn, 3),
            (ApiKey::EndTxn, 3),
            (ApiKey::TxnOffsetCommit, 3),
            (ApiKey::DescribeAcls, 2),
            (ApiKey::CreateAcls, 2),
            (ApiKey::DeleteAcls, 2),
            (ApiKey::DescribeConfigs, 4),
            (ApiKey::AlterConfigs, 2),
            (ApiKey::CreatePartitions, 2),
        ];

        for &(api, flex) in apis {
            assert_eq!(
                api.flexible_version(),
                flex,
                "{api:?} flexible_version mismatch"
            );

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
