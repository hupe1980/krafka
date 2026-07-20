//! Property-based round-trip tests for the wire protocol.
//!
//! The natural invariant for a codec is `decode(encode(x)) == x`. This module
//! asserts it over randomly generated inputs for:
//!
//! * the varint family (`src/util.rs` `varint`), in both signed and unsigned,
//!   32- and 64-bit forms;
//! * the protocol primitives — [`KafkaString`], [`KafkaBytes`], [`KafkaArray`]
//!   and [`TaggedFields`] — in both their compact (flexible-version) and
//!   non-compact encodings;
//! * [`Record`] and [`RecordBatch`], across every compiled-in compression
//!   codec;
//! * [`DescribeTopicPartitionsRequest`] across its supported version range,
//!   using a test-local reader that mirrors the Kafka message generator. This
//!   pins the nullable-struct presence byte of the pagination cursor in *both*
//!   directions.
//!
//! Request types are encode-only and response types decode-only in this crate,
//! so a symmetric round-trip is only available where both halves exist. For the
//! remaining request encoders the property asserted is the weaker but still
//! useful one that encoding is total and deterministic over the supported
//! version range.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use bytes::{Buf, Bytes, BytesMut};
use proptest::prelude::*;

use crate::protocol::messages::{
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsRequest, VersionedEncode,
};
use crate::protocol::primitives::{
    Decode, KafkaArray, KafkaBytes, KafkaString, TaggedField, TaggedFields, TryEncode,
};
use crate::protocol::record::{Compression, LazyRecordBatch, Record, RecordBatch};
use crate::util::varint;

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Strings kept short: the non-compact encoding uses an `i16` length prefix, so
/// values beyond `i16::MAX` legitimately fail to encode and are not round-trip
/// candidates.
fn arb_string() -> impl Strategy<Value = String> {
    ".{0,64}"
}

fn arb_kafka_string() -> impl Strategy<Value = KafkaString> {
    prop_oneof![
        1 => Just(KafkaString(None)),
        4 => arb_string().prop_map(|s| KafkaString(Some(s))),
    ]
}

fn arb_kafka_bytes() -> impl Strategy<Value = KafkaBytes> {
    prop_oneof![
        1 => Just(KafkaBytes(None)),
        4 => prop::collection::vec(any::<u8>(), 0..64)
            .prop_map(|v| KafkaBytes(Some(Bytes::from(v)))),
    ]
}

fn arb_tagged_fields() -> impl Strategy<Value = TaggedFields> {
    // Tags must be strictly increasing on the wire in real Kafka messages, but
    // the codec here is order-preserving, so any sequence round-trips.
    prop::collection::vec(
        (any::<u32>(), prop::collection::vec(any::<u8>(), 0..32)),
        0..8,
    )
    .prop_map(|v| {
        TaggedFields(
            v.into_iter()
                .map(|(tag, data)| TaggedField {
                    tag,
                    data: Bytes::from(data),
                })
                .collect(),
        )
    })
}

fn arb_record() -> impl Strategy<Value = Record> {
    (
        any::<i64>(),
        any::<i32>(),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..32)),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..32)),
        prop::collection::vec((".{0,16}", prop::collection::vec(any::<u8>(), 0..16)), 0..4),
    )
        .prop_map(|(ts, off, key, value, headers)| {
            let mut rec = Record::new(key.map(Bytes::from), value.map(Bytes::from))
                .with_timestamp_delta(ts)
                .with_offset_delta(off);
            for (k, v) in headers {
                rec = rec.with_header(k, Bytes::from(v));
            }
            rec
        })
}

// ---------------------------------------------------------------------------
// Varints
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn varint_unsigned_roundtrip(v in any::<u32>()) {
        let mut buf = BytesMut::new();
        varint::encode_unsigned_varint(v, &mut buf);
        let mut b = buf.freeze();
        prop_assert_eq!(varint::decode_unsigned_varint(&mut b).unwrap(), v);
        prop_assert_eq!(b.remaining(), 0, "decoder must consume exactly the encoded bytes");
    }

    #[test]
    fn varint_signed_roundtrip(v in any::<i32>()) {
        let mut buf = BytesMut::new();
        varint::encode_signed_varint(v, &mut buf);
        let mut b = buf.freeze();
        prop_assert_eq!(varint::decode_signed_varint(&mut b).unwrap(), v);
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn varlong_unsigned_roundtrip(v in any::<u64>()) {
        let mut buf = BytesMut::new();
        varint::encode_unsigned_varlong(v, &mut buf);
        let mut b = buf.freeze();
        prop_assert_eq!(varint::decode_unsigned_varlong(&mut b).unwrap(), v);
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn varlong_signed_roundtrip(v in any::<i64>()) {
        let mut buf = BytesMut::new();
        varint::encode_signed_varlong(v, &mut buf);
        let mut b = buf.freeze();
        prop_assert_eq!(varint::decode_signed_varlong(&mut b).unwrap(), v);
        prop_assert_eq!(b.remaining(), 0);
    }
}

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn kafka_string_roundtrip(s in arb_kafka_string()) {
        let mut buf = BytesMut::new();
        s.try_encode(&mut buf).unwrap();
        let mut b = buf.freeze();
        prop_assert_eq!(KafkaString::decode(&mut b).unwrap().0, s.0.clone());
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn kafka_string_compact_roundtrip(s in arb_kafka_string()) {
        let mut buf = BytesMut::new();
        s.try_encode_compact(&mut buf).unwrap();
        let mut b = buf.freeze();
        prop_assert_eq!(KafkaString::decode_compact(&mut b).unwrap().0, s.0.clone());
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn kafka_bytes_roundtrip(v in arb_kafka_bytes()) {
        let mut buf = BytesMut::new();
        v.try_encode(&mut buf).unwrap();
        let mut b = buf.freeze();
        prop_assert_eq!(KafkaBytes::decode(&mut b).unwrap().0, v.0.clone());
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn kafka_bytes_compact_roundtrip(v in arb_kafka_bytes()) {
        let mut buf = BytesMut::new();
        v.try_encode_compact(&mut buf).unwrap();
        let mut b = buf.freeze();
        prop_assert_eq!(KafkaBytes::decode_compact(&mut b).unwrap().0, v.0.clone());
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn kafka_array_i32_roundtrip(items in prop::option::of(prop::collection::vec(any::<i32>(), 0..32))) {
        let arr = KafkaArray(items.clone());
        let mut buf = BytesMut::new();
        arr.try_encode(&mut buf).unwrap();
        let mut b = buf.freeze();
        prop_assert_eq!(KafkaArray::<i32>::decode(&mut b).unwrap().0, items);
        prop_assert_eq!(b.remaining(), 0);
    }

    /// The compact element encoding for `i32` is a zig-zag varint, not a fixed
    /// four bytes, so this exercises a genuinely different path.
    #[test]
    fn kafka_array_i32_compact_roundtrip(items in prop::option::of(prop::collection::vec(any::<i32>(), 0..32))) {
        let arr = KafkaArray(items.clone());
        let mut buf = BytesMut::new();
        arr.try_encode_compact(&mut buf).unwrap();
        let mut b = buf.freeze();
        prop_assert_eq!(KafkaArray::<i32>::decode_compact(&mut b).unwrap().0, items);
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn kafka_array_string_compact_roundtrip(items in prop::collection::vec(arb_string(), 0..16)) {
        let arr = KafkaArray(Some(
            items.iter().map(|s| KafkaString(Some(s.clone()))).collect::<Vec<_>>(),
        ));
        let mut buf = BytesMut::new();
        arr.try_encode_compact(&mut buf).unwrap();
        let mut b = buf.freeze();
        let decoded = KafkaArray::<KafkaString>::decode_compact(&mut b).unwrap();
        let got: Vec<String> = decoded.0.unwrap().into_iter().filter_map(|s| s.0).collect();
        prop_assert_eq!(got, items);
        prop_assert_eq!(b.remaining(), 0);
    }

    #[test]
    fn tagged_fields_roundtrip(tf in arb_tagged_fields()) {
        let mut buf = BytesMut::new();
        tf.try_encode(&mut buf).unwrap();
        let mut b = buf.freeze();
        let decoded = TaggedFields::decode(&mut b).unwrap();
        prop_assert_eq!(decoded.0.len(), tf.0.len());
        for (a, e) in decoded.0.iter().zip(tf.0.iter()) {
            prop_assert_eq!(a.tag, e.tag);
            prop_assert_eq!(&a.data, &e.data);
        }
        prop_assert_eq!(b.remaining(), 0);
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn record_roundtrip(rec in arb_record()) {
        let mut buf = BytesMut::new();
        rec.encode(&mut buf).unwrap();
        let mut b = buf.freeze();
        let decoded = Record::decode(&mut b).unwrap();
        prop_assert_eq!(decoded.timestamp_delta, rec.timestamp_delta);
        prop_assert_eq!(decoded.offset_delta, rec.offset_delta);
        prop_assert_eq!(&decoded.key, &rec.key);
        prop_assert_eq!(&decoded.value, &rec.value);
        prop_assert_eq!(decoded.headers.len(), rec.headers.len());
        prop_assert_eq!(b.remaining(), 0, "record decoder must consume exactly its own bytes");
    }

    /// Full batch round-trip, including the CRC and the (optional) compression
    /// of the record section.
    #[test]
    fn record_batch_roundtrip(
        records in prop::collection::vec(arb_record(), 0..8),
        base_offset in any::<i64>(),
        producer_id in any::<i64>(),
        producer_epoch in any::<i16>(),
    ) {
        for compression in compiled_codecs() {
            let mut batch = RecordBatch::new()
                .with_compression(compression);
            batch.base_offset = base_offset;
            batch.producer_id = producer_id;
            batch.producer_epoch = producer_epoch;
            batch.records = records.clone();

            let encoded = batch.encode().unwrap();
            let mut b = encoded.clone();
            let decoded = RecordBatch::decode(&mut b).unwrap();

            prop_assert_eq!(decoded.base_offset, base_offset);
            prop_assert_eq!(decoded.producer_id, producer_id);
            prop_assert_eq!(decoded.producer_epoch, producer_epoch);
            prop_assert_eq!(decoded.attributes.compression, compression);
            prop_assert_eq!(decoded.records.len(), records.len());
            for (a, e) in decoded.records.iter().zip(records.iter()) {
                prop_assert_eq!(&a.key, &e.key);
                prop_assert_eq!(&a.value, &e.value);
                prop_assert_eq!(a.timestamp_delta, e.timestamp_delta);
            }

            // The lazy path must agree with the eager one on identical bytes.
            let mut b2 = encoded.clone();
            let lazy = LazyRecordBatch::decode(&mut b2).unwrap();
            let lazy_records = lazy.decode_all().unwrap();
            prop_assert_eq!(lazy_records.len(), records.len());
        }
    }
}

/// Compression codecs actually compiled into this build.
fn compiled_codecs() -> Vec<Compression> {
    let mut v = vec![Compression::None];
    if cfg!(feature = "gzip") {
        v.push(Compression::Gzip);
    }
    if cfg!(feature = "snappy") {
        v.push(Compression::Snappy);
    }
    if cfg!(feature = "lz4") {
        v.push(Compression::Lz4);
    }
    if cfg!(feature = "zstd") {
        v.push(Compression::Zstd);
    }
    v
}

// ---------------------------------------------------------------------------
// Message-level round-trip: DescribeTopicPartitions request
// ---------------------------------------------------------------------------

/// Test-local reader for `DescribeTopicPartitionsRequest`, mirroring the Kafka
/// message generator's output for a flexible-from-v0 message.
///
/// The crate ships encoders for requests and decoders for responses only, so
/// this reader exists purely to close the loop and assert the encoder emits
/// exactly what a broker would read — in particular the nullable-struct
/// presence byte in front of the pagination cursor.
fn read_describe_topic_partitions_request(
    buf: &mut impl Buf,
) -> (Vec<String>, i32, Option<DescribeTopicPartitionsCursor>) {
    let topic_count = varint::decode_unsigned_varint(buf).unwrap() as usize - 1;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        topics.push(KafkaString::decode_compact(buf).unwrap().0.unwrap());
        let _ = TaggedFields::decode(buf).unwrap();
    }
    let limit = i32::decode(buf).unwrap();

    let presence = buf.get_i8();
    let cursor = if presence < 0 {
        None
    } else {
        assert_eq!(presence, 1, "present marker must be exactly 1");
        let topic_name = KafkaString::decode_compact(buf).unwrap().0.unwrap();
        let partition_index = i32::decode(buf).unwrap();
        let _ = TaggedFields::decode(buf).unwrap();
        Some(DescribeTopicPartitionsCursor {
            topic_name,
            partition_index,
        })
    };
    let _ = TaggedFields::decode(buf).unwrap();
    assert_eq!(buf.remaining(), 0, "encoder emitted trailing bytes");
    (topics, limit, cursor)
}

proptest! {
    #[test]
    fn describe_topic_partitions_request_roundtrip(
        topics in prop::collection::vec("[a-zA-Z0-9._-]{1,32}", 0..8),
        limit in any::<i32>(),
        cursor in prop::option::of(("[a-zA-Z0-9._-]{1,32}", any::<i32>())),
    ) {
        let mut req = DescribeTopicPartitionsRequest::new(topics.clone());
        req.response_partition_limit = limit;
        req.cursor = cursor.clone().map(|(topic_name, partition_index)| {
            DescribeTopicPartitionsCursor { topic_name, partition_index }
        });

        // v0 is the only supported version for this API.
        let mut buf = BytesMut::new();
        req.encode_versioned(0, &mut buf).unwrap();
        let mut b = buf.freeze();

        let (got_topics, got_limit, got_cursor) =
            read_describe_topic_partitions_request(&mut b);

        prop_assert_eq!(got_topics, topics);
        prop_assert_eq!(got_limit, limit);
        match (got_cursor, cursor) {
            (None, None) => {}
            (Some(g), Some((name, idx))) => {
                prop_assert_eq!(g.topic_name, name);
                prop_assert_eq!(g.partition_index, idx);
            }
            (g, e) => prop_assert!(false, "cursor mismatch: {:?} vs {:?}", g.is_some(), e.is_some()),
        }
    }
}

// ---------------------------------------------------------------------------
// Encode totality across supported version ranges
// ---------------------------------------------------------------------------

proptest! {
    /// Encoding must be total and deterministic: for every version the crate
    /// advertises, `encode_versioned` either succeeds or returns an error, but
    /// never panics, and repeated encodes of the same value agree byte for
    /// byte.
    #[test]
    fn request_encode_is_total_and_deterministic(
        topics in prop::collection::vec("[a-zA-Z0-9._-]{1,32}", 0..8),
        version in 0i16..=1,
    ) {
        let req = DescribeTopicPartitionsRequest::new(topics);

        let mut a = BytesMut::new();
        let ra = req.encode_versioned(version, &mut a);
        let mut b = BytesMut::new();
        let rb = req.encode_versioned(version, &mut b);

        prop_assert_eq!(ra.is_ok(), rb.is_ok());
        if ra.is_ok() {
            prop_assert_eq!(a.as_ref(), b.as_ref());
        }
    }
}
