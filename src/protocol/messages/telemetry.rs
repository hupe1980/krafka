use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::check_compact_array_len;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

// ============================================================================
// GetTelemetrySubscriptions (API Key 71) — KIP-714
// ============================================================================

/// Request to get telemetry subscriptions from the broker (KIP-714).
#[derive(Debug, Clone)]
pub struct GetTelemetrySubscriptionsRequest {
    /// Client instance ID. Must be set to zero UUID on the first request.
    pub client_instance_id: [u8; 16],
}

impl GetTelemetrySubscriptionsRequest {
    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_slice(&self.client_instance_id);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Response from GetTelemetrySubscriptions (KIP-714).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetTelemetrySubscriptionsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Assigned client instance ID (if the request had zero UUID).
    pub client_instance_id: [u8; 16],
    /// Unique identifier for the current subscription set.
    pub subscription_id: i32,
    /// Compression types accepted by the broker for PushTelemetry.
    pub accepted_compression_types: Vec<i8>,
    /// Configured push interval in milliseconds.
    pub push_interval_ms: i32,
    /// Maximum bytes of binary data the broker accepts.
    pub telemetry_max_bytes: i32,
    /// Whether monotonic/counter metrics should be emitted as deltas.
    pub delta_temporality: bool,
    /// Requested metrics prefix string match patterns.
    pub requested_metrics: Vec<String>,
}

impl GetTelemetrySubscriptionsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let mut client_instance_id = [0u8; 16];
        if buf.remaining() < 16 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for client_instance_id UUID",
            ));
        }
        buf.copy_to_slice(&mut client_instance_id);
        let subscription_id = i32::decode(buf)?;

        // AcceptedCompressionTypes — compact array of i8
        let ct_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut accepted_compression_types = Vec::with_capacity(ct_count);
        for _ in 0..ct_count {
            accepted_compression_types.push(i8::decode(buf)?);
        }

        let push_interval_ms = i32::decode(buf)?;
        let telemetry_max_bytes = i32::decode(buf)?;
        let delta_temporality = {
            if buf.remaining() < 1 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::TruncatedFrame,
                    "not enough bytes for delta_temporality",
                ));
            }
            buf.get_u8() != 0
        };

        // RequestedMetrics — compact array of compact strings
        let rm_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut requested_metrics = Vec::with_capacity(rm_count);
        for _ in 0..rm_count {
            let s = super::non_nullable_string(
                "requested metric",
                KafkaString::decode_compact(buf)?.0,
            )?;
            requested_metrics.push(s);
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            client_instance_id,
            subscription_id,
            accepted_compression_types,
            push_interval_ms,
            telemetry_max_bytes,
            delta_temporality,
            requested_metrics,
        })
    }
}

// ============================================================================
// PushTelemetry (API Key 72) — KIP-714
// ============================================================================

/// Request to push telemetry data to the broker (KIP-714).
#[derive(Debug, Clone)]
pub struct PushTelemetryRequest {
    /// Client instance ID.
    pub client_instance_id: [u8; 16],
    /// Current subscription ID.
    pub subscription_id: i32,
    /// Whether the client is terminating.
    pub terminating: bool,
    /// Compression codec for the metrics payload: 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd.
    pub compression_type: i8,
    /// Metrics encoded in OpenTelemetry MetricsData v1 protobuf format.
    pub metrics: Bytes,
}

impl PushTelemetryRequest {
    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_slice(&self.client_instance_id);
        self.subscription_id.encode(buf);
        buf.put_u8(u8::from(self.terminating));
        self.compression_type.encode(buf);
        // Metrics as compact bytes (varint length + 1, then data)
        let metrics_len = u32::try_from(self.metrics.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "metrics payload too large",
            )
        })?;
        crate::util::varint::encode_unsigned_varint(metrics_len, buf);
        buf.put_slice(&self.metrics);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Response from PushTelemetry (KIP-714).
#[derive(Debug, Clone)]
pub struct PushTelemetryResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl PushTelemetryResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

impl VersionedEncode for GetTelemetrySubscriptionsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("GetTelemetrySubscriptionsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for GetTelemetrySubscriptionsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("GetTelemetrySubscriptionsResponse", version),
        }
    }
}

impl VersionedEncode for PushTelemetryRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("PushTelemetryRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for PushTelemetryResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("PushTelemetryResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::util::varint;
    use bytes::BytesMut;

    // ===================================================================
    // Story 18.3: Telemetry Wire-Format Tests (KIP-714)
    // ===================================================================

    #[test]
    fn test_get_telemetry_subscriptions_encode_v0() {
        let req = GetTelemetrySubscriptionsRequest {
            client_instance_id: [0u8; 16],
        };
        let mut buf = BytesMut::new();
        req.encode_versioned(0, &mut buf).unwrap();
        // 16 bytes UUID + 1 byte tagged fields (varint 0)
        assert_eq!(buf.len(), 17);
    }

    #[test]
    fn test_get_telemetry_subscriptions_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_slice(&[7u8; 16]); // client_instance_id
        buf.put_i32(42); // subscription_id
        // accepted_compression_types: compact array [0, 1]
        varint::encode_unsigned_varint(3, &mut buf); // len + 1 = 3
        buf.put_i8(0);
        buf.put_i8(1);
        buf.put_i32(30_000); // push_interval_ms
        buf.put_i32(1_048_576); // telemetry_max_bytes
        buf.put_u8(1); // delta_temporality = true
        // requested_metrics: compact array ["cpu.usage"]
        varint::encode_unsigned_varint(2, &mut buf); // len + 1 = 2
        let metric = b"cpu.usage";
        varint::encode_unsigned_varint((metric.len() + 1) as u32, &mut buf);
        buf.put_slice(metric);
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields

        let resp =
            GetTelemetrySubscriptionsResponse::decode_versioned(0, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.client_instance_id, [7u8; 16]);
        assert_eq!(resp.subscription_id, 42);
        assert_eq!(resp.accepted_compression_types, vec![0, 1]);
        assert_eq!(resp.push_interval_ms, 30_000);
        assert_eq!(resp.telemetry_max_bytes, 1_048_576);
        assert!(resp.delta_temporality);
        assert_eq!(resp.requested_metrics, vec!["cpu.usage"]);
    }

    #[test]
    fn test_push_telemetry_encode_v0() {
        let req = PushTelemetryRequest {
            client_instance_id: [5u8; 16],
            subscription_id: 99,
            terminating: true,
            compression_type: 1,
            metrics: Bytes::from_static(b"binary-data"),
        };
        let mut buf = BytesMut::new();
        req.encode_versioned(0, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // Verify the binary data appears in the output.
        let encoded = buf.freeze();
        assert!(
            encoded.windows(11).any(|w| w == b"binary-data"),
            "expected metrics payload in output"
        );
    }

    #[test]
    fn test_push_telemetry_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields

        let resp = PushTelemetryResponse::decode_versioned(0, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.error_code, ErrorCode::None);
    }
}
