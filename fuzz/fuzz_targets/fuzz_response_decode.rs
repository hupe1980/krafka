#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{
    AddOffsetsToTxnResponse, AddPartitionsToTxnResponse, AlterClientQuotasResponse,
    ConsumerGroupDescribeResponse, ConsumerGroupHeartbeatResponse, CreateAclsResponse,
    CreateDelegationTokenResponse, CreatePartitionsResponse, CreateTopicsResponse,
    DeleteAclsResponse, DeleteGroupsResponse, DeleteRecordsResponse, DeleteTopicsResponse,
    DescribeAclsResponse, DescribeClientQuotasResponse, DescribeClusterResponse,
    DescribeConfigsResponse, DescribeDelegationTokenResponse, DescribeGroupsResponse,
    DescribeTopicPartitionsResponse, EndTxnResponse, ExpireDelegationTokenResponse, FetchResponse,
    FindCoordinatorResponse, GetTelemetrySubscriptionsResponse, HeartbeatResponse,
    IncrementalAlterConfigsResponse, InitProducerIdResponse, JoinGroupResponse,
    LeaveGroupResponse, ListClientMetricsResourcesResponse, ListGroupsResponse,
    ListOffsetsResponse, MetadataResponse, OffsetCommitResponse, OffsetFetchResponse,
    OffsetForLeaderEpochResponse, ProduceResponse, PushTelemetryResponse,
    RenewDelegationTokenResponse, SaslAuthenticateResponse, SaslHandshakeResponse,
    ShareAcknowledgeResponse, ShareFetchResponse, ShareGroupDescribeResponse,
    ShareGroupHeartbeatResponse, SyncGroupResponse, TxnOffsetCommitResponse, VersionedDecode,
};

fuzz_target!(|data: &[u8]| {
    // Use first two bytes to select one API and version per iteration,
    // dramatically improving fuzzing throughput over decoding all 206
    // API/version pairs for every input.
    if data.len() < 2 {
        return;
    }

    let api = data[0];
    let ver_byte = data[1];
    let mut buf = Bytes::copy_from_slice(&data[2..]);

    match api % 48 {
        0 => { let _ = ProduceResponse::decode_versioned(3 + (ver_byte % 11) as i16, &mut buf); }
        1 => { let _ = FetchResponse::decode_versioned(4 + (ver_byte % 15) as i16, &mut buf); }
        2 => { let _ = MetadataResponse::decode_versioned(1 + (ver_byte % 13) as i16, &mut buf); }
        3 => { let _ = ListOffsetsResponse::decode_versioned(1 + (ver_byte % 11) as i16, &mut buf); }
        4 => { let _ = OffsetCommitResponse::decode_versioned(2 + (ver_byte % 9) as i16, &mut buf); }
        5 => { let _ = OffsetFetchResponse::decode_versioned(1 + (ver_byte % 10) as i16, &mut buf); }
        6 => { let _ = FindCoordinatorResponse::decode_versioned(1 + (ver_byte % 6) as i16, &mut buf); }
        7 => { let _ = JoinGroupResponse::decode_versioned(4 + (ver_byte % 6) as i16, &mut buf); }
        8 => { let _ = SyncGroupResponse::decode_versioned(3 + (ver_byte % 3) as i16, &mut buf); }
        9 => { let _ = HeartbeatResponse::decode_versioned(3 + (ver_byte % 2) as i16, &mut buf); }
        10 => { let _ = LeaveGroupResponse::decode_versioned(3 + (ver_byte % 3) as i16, &mut buf); }
        11 => { let _ = CreateTopicsResponse::decode_versioned(2 + (ver_byte % 6) as i16, &mut buf); }
        12 => { let _ = DeleteTopicsResponse::decode_versioned(1 + (ver_byte % 6) as i16, &mut buf); }
        13 => { let _ = DescribeAclsResponse::decode_versioned(1 + (ver_byte % 3) as i16, &mut buf); }
        14 => { let _ = CreateAclsResponse::decode_versioned(1 + (ver_byte % 3) as i16, &mut buf); }
        15 => { let _ = DeleteAclsResponse::decode_versioned(1 + (ver_byte % 3) as i16, &mut buf); }
        16 => { let _ = DescribeGroupsResponse::decode_versioned(1 + (ver_byte % 6) as i16, &mut buf); }
        17 => { let _ = ListGroupsResponse::decode_versioned(1 + (ver_byte % 5) as i16, &mut buf); }
        18 => { let _ = OffsetForLeaderEpochResponse::decode_versioned(2 + (ver_byte % 3) as i16, &mut buf); }
        19 => { let _ = ConsumerGroupHeartbeatResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        20 => { let _ = InitProducerIdResponse::decode_versioned((ver_byte % 7) as i16, &mut buf); }
        21 => { let _ = AddPartitionsToTxnResponse::decode_versioned((ver_byte % 6) as i16, &mut buf); }
        22 => { let _ = AddOffsetsToTxnResponse::decode_versioned((ver_byte % 5) as i16, &mut buf); }
        23 => { let _ = EndTxnResponse::decode_versioned((ver_byte % 6) as i16, &mut buf); }
        24 => { let _ = TxnOffsetCommitResponse::decode_versioned((ver_byte % 6) as i16, &mut buf); }
        25 => { let _ = SaslHandshakeResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        26 => { let _ = SaslAuthenticateResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        27 => { let _ = DescribeConfigsResponse::decode_versioned((ver_byte % 5) as i16, &mut buf); }
        28 => { let _ = IncrementalAlterConfigsResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        29 => { let _ = CreatePartitionsResponse::decode_versioned((ver_byte % 4) as i16, &mut buf); }
        30 => { let _ = DeleteRecordsResponse::decode_versioned((ver_byte % 3) as i16, &mut buf); }
        31 => { let _ = DeleteGroupsResponse::decode_versioned((ver_byte % 3) as i16, &mut buf); }
        32 => { let _ = DescribeClusterResponse::decode_versioned((ver_byte % 3) as i16, &mut buf); }
        33 => { let _ = ConsumerGroupDescribeResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        34 => { let _ = ListClientMetricsResourcesResponse::decode_versioned(0, &mut buf); }
        35 => { let _ = DescribeTopicPartitionsResponse::decode_versioned(0, &mut buf); }
        36 => { let _ = DescribeClientQuotasResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        37 => { let _ = AlterClientQuotasResponse::decode_versioned((ver_byte % 2) as i16, &mut buf); }
        38 => { let _ = CreateDelegationTokenResponse::decode_versioned(1 + (ver_byte % 3) as i16, &mut buf); }
        39 => { let _ = RenewDelegationTokenResponse::decode_versioned(1 + (ver_byte % 2) as i16, &mut buf); }
        40 => { let _ = ExpireDelegationTokenResponse::decode_versioned(1 + (ver_byte % 2) as i16, &mut buf); }
        41 => { let _ = DescribeDelegationTokenResponse::decode_versioned(1 + (ver_byte % 3) as i16, &mut buf); }
        42 => { let _ = GetTelemetrySubscriptionsResponse::decode_versioned(0, &mut buf); }
        43 => { let _ = PushTelemetryResponse::decode_versioned(0, &mut buf); }
        44 => { let _ = ShareGroupHeartbeatResponse::decode_versioned(1, &mut buf); }
        45 => { let _ = ShareGroupDescribeResponse::decode_versioned(1, &mut buf); }
        46 => { let _ = ShareFetchResponse::decode_versioned(1 + (ver_byte % 2) as i16, &mut buf); }
        47 => { let _ = ShareAcknowledgeResponse::decode_versioned(1 + (ver_byte % 2) as i16, &mut buf); }
        _ => unreachable!(),
    }
});
