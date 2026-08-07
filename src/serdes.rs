//! Pluggable serialization applied on the way to and from the wire.
//!
//! A [`Serializer`] runs on every record the producer sends, after the
//! interceptors and before partitioning. A [`Deserializer`] runs on every
//! record the consumer delivers, after the interceptors and immediately before
//! `poll()` returns. Attach them with `key_serializer` / `value_serializer` on
//! either producer builder and `key_deserializer` / `value_deserializer` on the
//! consumer builder.
//!
//! This is the same hook the Java client exposes as `key.serializer` /
//! `value.serializer`, and it exists for the same reason: the Kafka client
//! should own the *place* the transformation happens, and the ecosystem should
//! own the transformations.
//!
//! # What krafka deliberately does not ship
//!
//! There is no schema-registry client here, and no Avro, Protobuf or JSON
//! codec. Every comparable client draws the line in the same place — Java's
//! `kafka-clients` has no registry support (`kafka-avro-serializer` is a
//! separate artifact), librdkafka has none (`libschemaregistry` is a separate
//! library), and franz-go keeps `pkg/sr` out of `kgo`. A schema registry is a
//! different service, with a different protocol, auth model and release
//! cadence; coupling it to the Kafka protocol client means a registry API
//! change forces a Kafka client release.
//!
//! krafka carried a Confluent + AWS Glue registry client until 0.18. It now
//! lives in [`schemreg`](https://crates.io/crates/schemreg), which also has
//! native Apicurio support and real Avro / Protobuf / JSON codecs that krafka
//! never had. Pair the two with a small adapter — see the
//! [Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/#use-a-schema-registry).
//!
//! # Beyond schemas
//!
//! Because the traits are plain `Bytes -> Bytes`, they are not limited to
//! schema framing. Envelope encryption, an application-level compression
//! scheme, or a bare `serde_json` round-trip all fit the same hook.
//!
//! # Errors
//!
//! An error from either trait fails the operation: the producer's `send`
//! returns it, and `poll` returns it rather than delivering a record it could
//! not decode. Neither is invoked for an absent key or value.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;

use crate::error::Result;

/// Transforms a record's key or value on its way to the broker.
///
/// Implementations must be cheap to share: one instance handles every record on
/// the producer, so keep per-call work off the hot path and cache anything
/// derived from the topic.
///
/// # Example
///
/// A serializer that prefixes a version byte.
///
/// ```rust
/// use std::future::Future;
/// use std::pin::Pin;
///
/// use bytes::{BufMut, Bytes, BytesMut};
/// use krafka::serdes::Serializer;
///
/// #[derive(Debug)]
/// struct VersionTagged(u8);
///
/// impl Serializer for VersionTagged {
///     fn serialize(
///         &self,
///         payload: Bytes,
///         _topic: &str,
///         _record_name: Option<&str>,
///         _is_key: bool,
///     ) -> Pin<Box<dyn Future<Output = krafka::Result<Bytes>> + Send + '_>> {
///         let version = self.0;
///         Box::pin(async move {
///             let mut out = BytesMut::with_capacity(1 + payload.len());
///             out.put_u8(version);
///             out.put_slice(&payload);
///             Ok(out.freeze())
///         })
///     }
/// }
/// ```
pub trait Serializer: Send + Sync {
    /// Transform `payload` before it is written to the record batch.
    ///
    /// `topic` is the target topic. `record_name` is
    /// [`ProducerRecord::record_name`](crate::producer::ProducerRecord::record_name),
    /// carried through for implementations that derive a subject or type name
    /// from it; it is `None` unless the caller set one. `is_key` distinguishes
    /// the key from the value, since the two usually map to different schemas.
    ///
    /// Takes `Bytes` rather than `&[u8]` so an implementation can move the
    /// buffer into the returned future without copying.
    fn serialize(
        &self,
        payload: Bytes,
        topic: &str,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}

/// Transforms a record's key or value on its way to the application.
///
/// The inverse of [`Serializer`], applied by the consumer immediately before
/// `poll()` returns.
pub trait Deserializer: Send + Sync {
    /// Transform `payload` before it is handed to the application.
    ///
    /// `topic` is the source topic and `is_key` distinguishes key from value,
    /// for the same reason as on [`Serializer::serialize`]. There is no
    /// `record_name`: on the read path the framing itself identifies the
    /// schema, which is why registry decoders need no hint.
    ///
    /// Takes `Bytes` so an implementation can return a sub-slice of its input
    /// without allocating — stripping a fixed-size header is a slice, not a
    /// copy.
    fn deserialize(
        &self,
        payload: Bytes,
        topic: &str,
        is_key: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>>;
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Debug)]
    struct Prefix(&'static [u8]);

    impl Serializer for Prefix {
        fn serialize(
            &self,
            payload: Bytes,
            _topic: &str,
            _record_name: Option<&str>,
            _is_key: bool,
        ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>> {
            Box::pin(async move {
                let mut out = Vec::with_capacity(self.0.len() + payload.len());
                out.extend_from_slice(self.0);
                out.extend_from_slice(&payload);
                Ok(Bytes::from(out))
            })
        }
    }

    impl Deserializer for Prefix {
        fn deserialize(
            &self,
            payload: Bytes,
            _topic: &str,
            _is_key: bool,
        ) -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send + '_>> {
            let len = self.0.len();
            Box::pin(async move { Ok(payload.slice(len..)) })
        }
    }

    /// Both traits must be usable as `Arc<dyn _>`, which is how the builders
    /// store them — a non-object-safe signature would only fail at the call
    /// site, far from the definition.
    #[tokio::test]
    async fn traits_are_object_safe_and_round_trip() {
        let ser: Arc<dyn Serializer> = Arc::new(Prefix(b"\x00\x01"));
        let de: Arc<dyn Deserializer> = Arc::new(Prefix(b"\x00\x01"));

        let framed = ser
            .serialize(Bytes::from_static(b"payload"), "orders", None, false)
            .await
            .unwrap();
        assert_eq!(&framed[..], b"\x00\x01payload");

        let plain = de.deserialize(framed, "orders", false).await.unwrap();
        assert_eq!(&plain[..], b"payload");
    }

    /// Deserializing must be able to return a slice of its input rather than a
    /// fresh allocation; that is the reason the signature takes `Bytes`.
    #[tokio::test]
    async fn deserialize_can_be_zero_copy() {
        let de = Prefix(b"\x00\x01");
        let input = Bytes::from_static(b"\x00\x01payload");
        let out = de
            .deserialize(input.clone(), "orders", false)
            .await
            .unwrap();
        assert_eq!(out.as_ptr(), input[2..].as_ptr(), "expected a sub-slice");
    }
}
