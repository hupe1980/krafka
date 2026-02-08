//! Kafka protocol request and response headers.
//!
//! All Kafka requests and responses are framed with headers that contain
//! metadata like API key, version, correlation ID, etc.

use bytes::{Buf, BufMut};

use super::api::ApiKey;
use super::primitives::{Decode, Encode, KafkaString, TaggedFields};
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
    pub fn encode_v0(&self, buf: &mut impl BufMut) {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
    }

    /// Encode the header for header version 1.
    #[inline]
    pub fn encode_v1(&self, buf: &mut impl BufMut) {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
        match &self.client_id {
            Some(client_id) => client_id.encode(buf),
            None => KafkaString::null().encode(buf),
        }
    }

    /// Encode the header for header version 2 (flexible).
    #[inline]
    pub fn encode_v2(&self, buf: &mut impl BufMut) {
        self.api_key.encode(buf);
        self.api_version.encode(buf);
        self.correlation_id.encode(buf);
        match &self.client_id {
            Some(client_id) => client_id.encode_compact(buf),
            None => KafkaString::null().encode_compact(buf),
        }
        TaggedFields::default().encode(buf);
    }

    /// Determine the header version to use based on the API key and version.
    pub fn header_version(api_key: ApiKey, api_version: i16) -> i16 {
        // Most APIs use header v2 for flexible versions
        // This is a simplified version - in practice, each API has specific rules
        match api_key {
            ApiKey::ApiVersions => {
                // ApiVersions uses header v0 for v0-2, v2 for v3+
                if api_version >= 3 { 2 } else { 1 }
            }
            // For most other APIs, use header v2 for newer versions
            _ => {
                if api_version >= 9 {
                    2
                } else {
                    1
                }
            }
        }
    }

    /// Encode the header using the appropriate version.
    pub fn encode(&self, buf: &mut impl BufMut) {
        let header_version = Self::header_version(self.api_key, self.api_version);
        match header_version {
            0 => self.encode_v0(buf),
            1 => self.encode_v1(buf),
            _ => self.encode_v2(buf),
        }
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

    /// Determine the header version to use based on the API key and version.
    pub fn header_version(api_key: ApiKey, api_version: i16) -> i16 {
        match api_key {
            ApiKey::ApiVersions => {
                // ApiVersions always uses response header v0
                0
            }
            // For most other APIs, use header v1 for flexible versions
            _ => {
                if api_version >= 9 {
                    1
                } else {
                    0
                }
            }
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
        header.encode_v0(&mut buf);

        let mut buf = buf.freeze();
        assert_eq!(i16::decode(&mut buf).unwrap(), 18); // ApiVersions = 18
        assert_eq!(i16::decode(&mut buf).unwrap(), 0); // version
        assert_eq!(i32::decode(&mut buf).unwrap(), 1); // correlation_id
    }

    #[test]
    fn test_request_header_v1() {
        let header = RequestHeader::new(ApiKey::Metadata, 0, 42).with_client_id("test-client");
        let mut buf = BytesMut::new();
        header.encode_v1(&mut buf);

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
        TaggedFields::default().encode(&mut buf);

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
}
