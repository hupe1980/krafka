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

    // Version ranges below must match each type's `VersionedDecode::decode_versioned`
    // match arms exactly. Out-of-range versions hit `unsupported_decode!` and
    // return an error immediately, wasting the fuzz input.
    macro_rules! fuzz_decode {
        // Decode `$ty` at version `$min + (ver_byte % $count)`.
        ($ty:ty, $min:expr, $count:expr) => {{
            let version = $min + (ver_byte % $count) as i16;
            let _ = <$ty>::decode_versioned(version, &mut buf);
        }};
    }

    match api % 48 {
        0  => fuzz_decode!(ProduceResponse, 3, 11),
        1  => fuzz_decode!(FetchResponse, 4, 15),
        2  => fuzz_decode!(MetadataResponse, 1, 13),
        3  => fuzz_decode!(ListOffsetsResponse, 1, 11),
        4  => fuzz_decode!(OffsetCommitResponse, 2, 9),
        5  => fuzz_decode!(OffsetFetchResponse, 1, 10),
        6  => fuzz_decode!(FindCoordinatorResponse, 1, 6),
        7  => fuzz_decode!(JoinGroupResponse, 4, 6),
        8  => fuzz_decode!(SyncGroupResponse, 3, 3),
        9  => fuzz_decode!(HeartbeatResponse, 3, 2),
        10 => fuzz_decode!(LeaveGroupResponse, 3, 3),
        11 => fuzz_decode!(CreateTopicsResponse, 2, 6),
        12 => fuzz_decode!(DeleteTopicsResponse, 1, 6),
        13 => fuzz_decode!(DescribeAclsResponse, 1, 3),
        14 => fuzz_decode!(CreateAclsResponse, 1, 3),
        15 => fuzz_decode!(DeleteAclsResponse, 1, 3),
        16 => fuzz_decode!(DescribeGroupsResponse, 1, 6),
        17 => fuzz_decode!(ListGroupsResponse, 1, 5),
        18 => fuzz_decode!(OffsetForLeaderEpochResponse, 2, 3),
        19 => fuzz_decode!(ConsumerGroupHeartbeatResponse, 0, 2),
        20 => fuzz_decode!(InitProducerIdResponse, 0, 7),
        21 => fuzz_decode!(AddPartitionsToTxnResponse, 0, 6),
        22 => fuzz_decode!(AddOffsetsToTxnResponse, 0, 5),
        23 => fuzz_decode!(EndTxnResponse, 0, 6),
        24 => fuzz_decode!(TxnOffsetCommitResponse, 0, 6),
        25 => fuzz_decode!(SaslHandshakeResponse, 0, 2),
        26 => fuzz_decode!(SaslAuthenticateResponse, 0, 2),
        27 => fuzz_decode!(DescribeConfigsResponse, 0, 5),
        28 => fuzz_decode!(IncrementalAlterConfigsResponse, 0, 2),
        29 => fuzz_decode!(CreatePartitionsResponse, 0, 4),
        30 => fuzz_decode!(DeleteRecordsResponse, 0, 3),
        31 => fuzz_decode!(DeleteGroupsResponse, 0, 3),
        32 => fuzz_decode!(DescribeClusterResponse, 0, 3),
        33 => fuzz_decode!(ConsumerGroupDescribeResponse, 0, 2),
        34 => fuzz_decode!(ListClientMetricsResourcesResponse, 0, 1),
        35 => fuzz_decode!(DescribeTopicPartitionsResponse, 0, 1),
        36 => fuzz_decode!(DescribeClientQuotasResponse, 0, 2),
        37 => fuzz_decode!(AlterClientQuotasResponse, 0, 2),
        38 => fuzz_decode!(CreateDelegationTokenResponse, 1, 3),
        39 => fuzz_decode!(RenewDelegationTokenResponse, 1, 2),
        40 => fuzz_decode!(ExpireDelegationTokenResponse, 1, 2),
        41 => fuzz_decode!(DescribeDelegationTokenResponse, 1, 3),
        42 => fuzz_decode!(GetTelemetrySubscriptionsResponse, 0, 1),
        43 => fuzz_decode!(PushTelemetryResponse, 0, 1),
        44 => fuzz_decode!(ShareGroupHeartbeatResponse, 1, 1),
        45 => fuzz_decode!(ShareGroupDescribeResponse, 1, 1),
        46 => fuzz_decode!(ShareFetchResponse, 1, 2),
        47 => fuzz_decode!(ShareAcknowledgeResponse, 1, 2),
        _ => unreachable!(),
    }
});
