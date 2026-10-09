//! Typed serialization for the producer, pluggable byte transforms for the
//! consumer.
//!
//! A [`Serializer<T>`] turns an application value into the bytes of a
//! record's key or value. [`TypedProducer`](crate::producer::TypedProducer)
//! runs one for the key and one for the value before the record enters the
//! producer, so the interceptors see the encoded record. A serializer may write
//! headers too, which is where a schema registry puts a schema id.
//!
//! A [`Deserializer`] runs on every record the consumer delivers, after the
//! interceptors and immediately before `poll()` returns. Attach it with
//! `key_deserializer` / `value_deserializer` on the consumer builder.
//!
//! # What krafka deliberately does not ship
//!
//! There is no schema-registry client here, and no Avro, Protobuf or JSON
//! codec. Every comparable client draws the line in the same place — Java's
//! `kafka-clients` has no registry support (`kafka-avro-serializer` is a
//! separate artifact), librdkafka has none (`libschemaregistry` is a separate
//! library), and franz-go keeps `pkg/sr` out of `kgo`. Pair krafka with a
//! registry crate such as [`schemreg`](https://crates.io/crates/schemreg)
//! through a small adapter — see the
//! [Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/#use-a-schema-registry).
//!
//! # Errors
//!
//! An error from a serializer fails the send before anything is reserved or
//! queued. A deserializer error makes `poll` return it rather than deliver a
//! record it could not decode. Neither is invoked for an absent key or value.

use std::sync::Arc;

use bytes::Bytes;

use crate::Headers;
use crate::error::{KrafkaError, Result};

/// Encodes an application value as the bytes of a record key or value.
///
/// Synchronous: registration with a schema registry belongs before the send,
/// in the registry client's own cache, so the per-record call only encodes.
/// One instance serves every record, so it must be cheap to share.
///
/// `headers` are the record's headers; a serializer may add to them (a
/// header-based schema id, a content type).
///
/// krafka ships [`BytesSerializer`] (pass-through for `Bytes`, `Vec<u8>` and
/// `[u8]`) and [`StringSerializer`] (UTF-8 for `String` and `str`).
///
/// # Example
///
/// ```rust
/// use bytes::Bytes;
/// use krafka::Headers;
/// use krafka::serdes::Serializer;
///
/// /// Big-endian `u64`, tagged with a content-type header.
/// struct BigEndian;
///
/// impl Serializer<u64> for BigEndian {
///     fn serialize(
///         &self,
///         _topic: &str,
///         headers: &mut Headers,
///         value: &u64,
///     ) -> krafka::Result<Bytes> {
///         headers.push(("content-type".into(), Some(Bytes::from_static(b"u64-be"))));
///         Ok(Bytes::copy_from_slice(&value.to_be_bytes()))
///     }
/// }
/// ```
pub trait Serializer<T: ?Sized>: Send + Sync {
    /// Encode `value` for `topic`, optionally adding `headers`.
    fn serialize(&self, topic: &str, headers: &mut Headers, value: &T) -> Result<Bytes>;
}

/// Passes byte values through unchanged: `Bytes`, `Vec<u8>` and `[u8]`.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytesSerializer;

impl Serializer<Bytes> for BytesSerializer {
    fn serialize(&self, _topic: &str, _headers: &mut Headers, value: &Bytes) -> Result<Bytes> {
        Ok(value.clone())
    }
}

impl Serializer<Vec<u8>> for BytesSerializer {
    fn serialize(&self, _topic: &str, _headers: &mut Headers, value: &Vec<u8>) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(value))
    }
}

impl Serializer<[u8]> for BytesSerializer {
    fn serialize(&self, _topic: &str, _headers: &mut Headers, value: &[u8]) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(value))
    }
}

/// Encodes strings as UTF-8: `String` and `str`.
#[derive(Debug, Clone, Copy, Default)]
pub struct StringSerializer;

impl Serializer<String> for StringSerializer {
    fn serialize(&self, _topic: &str, _headers: &mut Headers, value: &String) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(value.as_bytes()))
    }
}

impl Serializer<str> for StringSerializer {
    fn serialize(&self, _topic: &str, _headers: &mut Headers, value: &str) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(value.as_bytes()))
    }
}

/// A serializer that refuses every value, for a key type a producer never
/// sends. Its `serialize` is never called for a `None` key.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoKey;

impl<T: ?Sized> Serializer<T> for NoKey {
    fn serialize(&self, topic: &str, _headers: &mut Headers, _value: &T) -> Result<Bytes> {
        Err(KrafkaError::serialization(format!(
            "this producer sends no keys, but a key was given for topic {topic}"
        )))
    }
}

/// Transforms a record's key or value on its way to the application.
///
/// The inverse of [`Serializer`], applied by the consumer and the share
/// consumer to every record before `poll()`/`recv()` returns it.
/// Synchronous: it runs inside the cancel-safe receive path. A panic
/// propagates out of `poll()`/`recv()`.
///
/// ```rust
/// use bytes::Bytes;
/// use krafka::Headers;
/// use krafka::serdes::Deserializer;
///
/// /// Strips a 5-byte schema-registry prefix.
/// struct StripPrefix;
///
/// impl Deserializer for StripPrefix {
///     fn deserialize(
///         &self,
///         _topic: &str,
///         _headers: &Headers,
///         payload: Bytes,
///         _is_key: bool,
///     ) -> krafka::Result<Bytes> {
///         Ok(payload.slice(5.min(payload.len())..))
///     }
/// }
/// ```
pub trait Deserializer: Send + Sync {
    /// Transform `payload` before it is handed to the application.
    ///
    /// `topic` is the source topic, `headers` the record's headers, and
    /// `is_key` distinguishes key from value. Takes `Bytes` so an
    /// implementation can return a sub-slice of its input without
    /// allocating.
    ///
    /// # Errors
    ///
    /// The consumer reports an error as
    /// [`RecordDeserialization`](KrafkaError::RecordDeserialization).
    fn deserialize(
        &self,
        topic: &str,
        headers: &Headers,
        payload: Bytes,
        is_key: bool,
    ) -> Result<Bytes>;
}

impl<T: Deserializer + ?Sized> Deserializer for Arc<T> {
    fn deserialize(
        &self,
        topic: &str,
        headers: &Headers,
        payload: Bytes,
        is_key: bool,
    ) -> Result<Bytes> {
        (**self).deserialize(topic, headers, payload, is_key)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Prefix(&'static [u8]);

    impl Deserializer for Prefix {
        fn deserialize(
            &self,
            _topic: &str,
            _headers: &Headers,
            payload: Bytes,
            _is_key: bool,
        ) -> Result<Bytes> {
            Ok(payload.slice(self.0.len()..))
        }
    }

    /// `Deserializer` must be usable as `Arc<dyn _>`, which is how the
    /// consumer builder stores it.
    #[test]
    fn deserializer_is_object_safe() {
        let de: Arc<dyn Deserializer> = Arc::new(Prefix(b"\x00\x01"));
        let plain = de
            .deserialize(
                "orders",
                &Headers::new(),
                Bytes::from_static(b"\x00\x01payload"),
                false,
            )
            .unwrap();
        assert_eq!(&plain[..], b"payload");
    }

    /// Deserializing must be able to return a slice of its input rather than a
    /// fresh allocation; that is the reason the signature takes `Bytes`.
    #[test]
    fn deserialize_can_be_zero_copy() {
        let de = Prefix(b"\x00\x01");
        let input = Bytes::from_static(b"\x00\x01payload");
        let out = de
            .deserialize("orders", &Headers::new(), input.clone(), false)
            .unwrap();
        assert_eq!(out.as_ptr(), input[2..].as_ptr(), "expected a sub-slice");
    }

    #[test]
    fn byte_and_string_serializers_pass_values_through() {
        let mut headers = Vec::new();
        let bytes = Bytes::from_static(b"raw");
        assert_eq!(
            BytesSerializer
                .serialize("t", &mut headers, &bytes)
                .unwrap(),
            bytes
        );
        assert_eq!(
            BytesSerializer
                .serialize("t", &mut headers, &b"raw".to_vec())
                .unwrap(),
            bytes
        );
        assert_eq!(
            BytesSerializer
                .serialize("t", &mut headers, &b"raw"[..])
                .unwrap(),
            bytes
        );
        assert_eq!(
            StringSerializer
                .serialize("t", &mut headers, "héllo")
                .unwrap(),
            Bytes::from("héllo")
        );
        assert_eq!(
            StringSerializer
                .serialize("t", &mut headers, &"héllo".to_string())
                .unwrap(),
            Bytes::from("héllo")
        );
        assert!(
            headers.is_empty(),
            "pass-through serializers add no headers"
        );
    }

    #[test]
    fn no_key_refuses_a_key() {
        let mut headers = Vec::new();
        assert!(NoKey.serialize("t", &mut headers, "k").is_err());
    }
}
