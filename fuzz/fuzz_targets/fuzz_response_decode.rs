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
        let mut tmp = buf.clone();
        let _ = ProduceResponse::decode_versioned(v, &mut tmp);
    }

    // Fuzz FetchResponse decode across versions (v0–v4, v7–v11)
    for v in (0..=4).chain(7..=11) {
        let mut tmp = buf.clone();
        let _ = FetchResponse::decode_versioned(v, &mut tmp);
    }

    // Fuzz MetadataResponse decode across all versions (v0–v8)
    for v in 0..=8 {
        let mut tmp = buf.clone();
        let _ = MetadataResponse::decode_versioned(v, &mut tmp);
    }

    // Fuzz CreateTopicsResponse decode (v0–v2)
    for v in 0..=2 {
        let mut tmp = buf.clone();
        let _ = CreateTopicsResponse::decode_versioned(v, &mut tmp);
    }

    // Fuzz DeleteTopicsResponse decode (v0–v1)
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = DeleteTopicsResponse::decode_versioned(v, &mut tmp);
    }

    // Fuzz OffsetForLeaderEpochResponse decode (v0–v3)
    for v in 0..=3 {
        let mut tmp = buf.clone();
        let _ = OffsetForLeaderEpochResponse::decode_versioned(v, &mut tmp);
    }
});
