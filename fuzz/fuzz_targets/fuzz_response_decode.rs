#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{
    CreateTopicsResponse, DeleteTopicsResponse, FetchResponse, MetadataResponse,
    OffsetForLeaderEpochResponse, ProduceResponse, VersionedDecode,
};

fuzz_target!(|data: &[u8]| {
    let buf = Bytes::copy_from_slice(data);

    // Fuzz ProduceResponse decode across versions (v0–v3)
    for v in 0..=3 {
        let _ = ProduceResponse::decode_versioned(v, &mut buf.clone());
    }

    // Fuzz FetchResponse decode across versions (v0–v4, v7–v11)
    for v in (0..=4).chain(7..=11) {
        let _ = FetchResponse::decode_versioned(v, &mut buf.clone());
    }

    // Fuzz MetadataResponse decode across all versions (v0–v8)
    for v in 0..=8 {
        let _ = MetadataResponse::decode_versioned(v, &mut buf.clone());
    }

    // Fuzz CreateTopicsResponse decode (v0–v2)
    for v in 0..=2 {
        let _ = CreateTopicsResponse::decode_versioned(v, &mut buf.clone());
    }

    // Fuzz DeleteTopicsResponse decode (v0–v1)
    for v in 0..=1 {
        let _ = DeleteTopicsResponse::decode_versioned(v, &mut buf.clone());
    }

    // Fuzz OffsetForLeaderEpochResponse decode (v0–v3)
    for v in 0..=3 {
        let _ = OffsetForLeaderEpochResponse::decode_versioned(v, &mut buf.clone());
    }
});
