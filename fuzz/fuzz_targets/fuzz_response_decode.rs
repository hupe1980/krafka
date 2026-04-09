#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{
    AddOffsetsToTxnResponse, AddPartitionsToTxnResponse, ConsumerGroupHeartbeatResponse,
    CreateAclsResponse, CreateTopicsResponse, DeleteAclsResponse, DeleteTopicsResponse,
    DescribeAclsResponse, DescribeGroupsResponse, EndTxnResponse, FetchResponse,
    FindCoordinatorResponse, HeartbeatResponse, InitProducerIdResponse, JoinGroupResponse,
    LeaveGroupResponse, ListGroupsResponse, ListOffsetsResponse, MetadataResponse,
    OffsetCommitResponse, OffsetFetchResponse, OffsetForLeaderEpochResponse, ProduceResponse,
    SyncGroupResponse, TxnOffsetCommitResponse, VersionedDecode,
};

fuzz_target!(|data: &[u8]| {
    let buf = Bytes::copy_from_slice(data);

    // ProduceResponse v3–v11 (MIN=3, MAX=11)
    for v in 3..=11 {
        let mut tmp = buf.clone();
        let _ = ProduceResponse::decode_versioned(v, &mut tmp);
    }

    // FetchResponse v4–v12 (MIN=4, MAX=12)
    for v in 4..=12 {
        let mut tmp = buf.clone();
        let _ = FetchResponse::decode_versioned(v, &mut tmp);
    }

    // MetadataResponse v1–v13 (MIN=1, MAX=13)
    for v in 1..=13 {
        let mut tmp = buf.clone();
        let _ = MetadataResponse::decode_versioned(v, &mut tmp);
    }

    // ListOffsetsResponse v1–v8 (MIN=1, MAX=8)
    for v in 1..=8 {
        let mut tmp = buf.clone();
        let _ = ListOffsetsResponse::decode_versioned(v, &mut tmp);
    }

    // OffsetCommitResponse v2–v9 (MIN=2, MAX=9)
    for v in 2..=9 {
        let mut tmp = buf.clone();
        let _ = OffsetCommitResponse::decode_versioned(v, &mut tmp);
    }

    // OffsetFetchResponse v1–v9 (MIN=1, MAX=9)
    for v in 1..=9 {
        let mut tmp = buf.clone();
        let _ = OffsetFetchResponse::decode_versioned(v, &mut tmp);
    }

    // FindCoordinatorResponse v1–v6 (MIN=1, MAX=6)
    for v in 1..=6 {
        let mut tmp = buf.clone();
        let _ = FindCoordinatorResponse::decode_versioned(v, &mut tmp);
    }

    // JoinGroupResponse v4–v5 (MIN=4, MAX=5)
    for v in 4..=5 {
        let mut tmp = buf.clone();
        let _ = JoinGroupResponse::decode_versioned(v, &mut tmp);
    }

    // SyncGroupResponse v3–v5 (MIN=3, MAX=5)
    for v in 3..=5 {
        let mut tmp = buf.clone();
        let _ = SyncGroupResponse::decode_versioned(v, &mut tmp);
    }

    // HeartbeatResponse v3–v4 (MIN=3, MAX=4)
    for v in 3..=4 {
        let mut tmp = buf.clone();
        let _ = HeartbeatResponse::decode_versioned(v, &mut tmp);
    }

    // LeaveGroupResponse v3–v5 (MIN=3, MAX=5)
    for v in 3..=5 {
        let mut tmp = buf.clone();
        let _ = LeaveGroupResponse::decode_versioned(v, &mut tmp);
    }

    // CreateTopicsResponse v2 (MIN=2, MAX=2)
    {
        let mut tmp = buf.clone();
        let _ = CreateTopicsResponse::decode_versioned(2, &mut tmp);
    }

    // DeleteTopicsResponse v1 (MIN=1, MAX=1)
    {
        let mut tmp = buf.clone();
        let _ = DeleteTopicsResponse::decode_versioned(1, &mut tmp);
    }

    // DescribeAclsResponse v1 (MIN=1, MAX=1)
    {
        let mut tmp = buf.clone();
        let _ = DescribeAclsResponse::decode_versioned(1, &mut tmp);
    }

    // CreateAclsResponse v1 (MIN=1, MAX=1)
    {
        let mut tmp = buf.clone();
        let _ = CreateAclsResponse::decode_versioned(1, &mut tmp);
    }

    // DeleteAclsResponse v1 (MIN=1, MAX=1)
    {
        let mut tmp = buf.clone();
        let _ = DeleteAclsResponse::decode_versioned(1, &mut tmp);
    }

    // DescribeGroupsResponse v1 (MIN=1, MAX=1)
    {
        let mut tmp = buf.clone();
        let _ = DescribeGroupsResponse::decode_versioned(1, &mut tmp);
    }

    // ListGroupsResponse v1 (MIN=1, MAX=1)
    {
        let mut tmp = buf.clone();
        let _ = ListGroupsResponse::decode_versioned(1, &mut tmp);
    }

    // OffsetForLeaderEpochResponse v2–v4 (MIN=2, MAX=4)
    for v in 2..=4 {
        let mut tmp = buf.clone();
        let _ = OffsetForLeaderEpochResponse::decode_versioned(v, &mut tmp);
    }

    // ConsumerGroupHeartbeatResponse v0–v1 (MIN=0, MAX=1)
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = ConsumerGroupHeartbeatResponse::decode_versioned(v, &mut tmp);
    }

    // InitProducerIdResponse v0–v5 (MIN=0, MAX=5)
    for v in 0..=5 {
        let mut tmp = buf.clone();
        let _ = InitProducerIdResponse::decode_versioned(v, &mut tmp);
    }

    // AddPartitionsToTxnResponse v0–v5 (MIN=0, MAX=5)
    for v in 0..=5 {
        let mut tmp = buf.clone();
        let _ = AddPartitionsToTxnResponse::decode_versioned(v, &mut tmp);
    }

    // AddOffsetsToTxnResponse v0–v4 (MIN=0, MAX=4)
    for v in 0..=4 {
        let mut tmp = buf.clone();
        let _ = AddOffsetsToTxnResponse::decode_versioned(v, &mut tmp);
    }

    // EndTxnResponse v0–v5 (MIN=0, MAX=5)
    for v in 0..=5 {
        let mut tmp = buf.clone();
        let _ = EndTxnResponse::decode_versioned(v, &mut tmp);
    }

    // TxnOffsetCommitResponse v0–v5 (MIN=0, MAX=5)
    for v in 0..=5 {
        let mut tmp = buf.clone();
        let _ = TxnOffsetCommitResponse::decode_versioned(v, &mut tmp);
    }
});
