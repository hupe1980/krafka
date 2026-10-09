//! A producer of typed keys and values.

use std::sync::Arc;

use super::{DeliveryHandle, Producer, Record, RecordMetadata};
use crate::error::Result;
use crate::serdes::Serializer;

/// A [`Producer`] that serializes typed keys and values.
///
/// Each send serializes the key, then the value — either may add headers —
/// then hands the encoded record to the producer, whose interceptors see the
/// bytes. A `None` key or value is not serialized: a `None` value is a
/// tombstone. A serializer error fails the send before anything is reserved
/// or queued.
///
/// # Example
///
/// ```rust,no_run
/// use krafka::Kafka;
/// use krafka::producer::TypedProducer;
/// use krafka::serdes::{BytesSerializer, StringSerializer};
///
/// # async fn example() -> krafka::Result<()> {
/// let kafka = Kafka::builder("localhost:9092").connect().await?;
/// let producer = kafka.producer().build().await?;
/// let typed: TypedProducer<str, [u8]> =
///     TypedProducer::new(producer, StringSerializer, BytesSerializer);
/// typed.send("events", Some("user-42"), Some(&b"signed-up"[..])).await?;
/// typed.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct TypedProducer<K: ?Sized, V: ?Sized> {
    producer: Producer,
    key: Arc<dyn Serializer<K>>,
    value: Arc<dyn Serializer<V>>,
}

impl<K: ?Sized, V: ?Sized> std::fmt::Debug for TypedProducer<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedProducer")
            .field("producer", &self.producer)
            .finish_non_exhaustive()
    }
}

impl<K: ?Sized, V: ?Sized> TypedProducer<K, V> {
    /// Wrap `producer` with a key and a value serializer.
    pub fn new(
        producer: Producer,
        key: impl Serializer<K> + 'static,
        value: impl Serializer<V> + 'static,
    ) -> Self {
        Self {
            producer,
            key: Arc::new(key),
            value: Arc::new(value),
        }
    }

    /// Serialize and send a record, waiting for the broker's acknowledgement.
    ///
    /// Not cancel safe, as [`Producer::send`]: dropped after the record is
    /// queued, it is still delivered.
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&K>,
        value: Option<&V>,
    ) -> Result<RecordMetadata> {
        self.enqueue(topic, key, value).await?.await
    }

    /// Serialize and queue a record; see [`Producer::enqueue`].
    pub async fn enqueue(
        &self,
        topic: &str,
        key: Option<&K>,
        value: Option<&V>,
    ) -> Result<DeliveryHandle> {
        let record = self.serialize(topic, key, value)?;
        self.producer.enqueue(record).await
    }

    fn serialize(&self, topic: &str, key: Option<&K>, value: Option<&V>) -> Result<Record> {
        let mut record = Record::new(topic, bytes::Bytes::new());
        record.key = key
            .map(|key| self.key.serialize(topic, &mut record.headers, key))
            .transpose()?;
        record.value = value
            .map(|value| self.value.serialize(topic, &mut record.headers, value))
            .transpose()?;
        Ok(record)
    }

    /// The producer underneath, for flush, metrics and untyped sends.
    pub fn producer(&self) -> &Producer {
        &self.producer
    }

    /// Unwrap the producer.
    pub fn into_inner(self) -> Producer {
        self.producer
    }

    /// Close the producer underneath; see [`Producer::close`].
    pub async fn close(&self) -> Result<()> {
        self.producer.close().await
    }
}
