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
    let buf = Bytes::copy_from_slice(data);

    // ProduceResponse v3–v13
    for v in 3..=13 {
        let mut tmp = buf.clone();
        let _ = ProduceResponse::decode_versioned(v, &mut tmp);
    }

    // FetchResponse v4–v18
    for v in 4..=18 {
        let mut tmp = buf.clone();
        let _ = FetchResponse::decode_versioned(v, &mut tmp);
    }

    // MetadataResponse v1–v13 (MIN=1, MAX=13)
    for v in 1..=13 {
        let mut tmp = buf.clone();
        let _ = MetadataResponse::decode_versioned(v, &mut tmp);
    }

    // ListOffsetsResponse v1–v11
    for v in 1..=11 {
        let mut tmp = buf.clone();
        let _ = ListOffsetsResponse::decode_versioned(v, &mut tmp);
    }

    // OffsetCommitResponse v2–v10
    for v in 2..=10 {
        let mut tmp = buf.clone();
        let _ = OffsetCommitResponse::decode_versioned(v, &mut tmp);
    }

    // OffsetFetchResponse v1–v10
    for v in 1..=10 {
        let mut tmp = buf.clone();
        let _ = OffsetFetchResponse::decode_versioned(v, &mut tmp);
    }

    // FindCoordinatorResponse v1–v6 (MIN=1, MAX=6)
    for v in 1..=6 {
        let mut tmp = buf.clone();
        let _ = FindCoordinatorResponse::decode_versioned(v, &mut tmp);
    }

    // JoinGroupResponse v4–v9
    for v in 4..=9 {
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

    // CreateTopicsResponse v2–v7
    for v in 2..=7 {
        let mut tmp = buf.clone();
        let _ = CreateTopicsResponse::decode_versioned(v, &mut tmp);
    }

    // DeleteTopicsResponse v1–v6
    for v in 1..=6 {
        let mut tmp = buf.clone();
        let _ = DeleteTopicsResponse::decode_versioned(v, &mut tmp);
    }

    // DescribeAclsResponse v1–v3 (MIN=1, MAX=3)
    for v in 1..=3 {
        let mut tmp = buf.clone();
        let _ = DescribeAclsResponse::decode_versioned(v, &mut tmp);
    }

    // CreateAclsResponse v1–v3 (MIN=1, MAX=3)
    for v in 1..=3 {
        let mut tmp = buf.clone();
        let _ = CreateAclsResponse::decode_versioned(v, &mut tmp);
    }

    // DeleteAclsResponse v1–v3 (MIN=1, MAX=3)
    for v in 1..=3 {
        let mut tmp = buf.clone();
        let _ = DeleteAclsResponse::decode_versioned(v, &mut tmp);
    }

    // DescribeGroupsResponse v1–v6
    for v in 1..=6 {
        let mut tmp = buf.clone();
        let _ = DescribeGroupsResponse::decode_versioned(v, &mut tmp);
    }

    // ListGroupsResponse v1–v5
    for v in 1..=5 {
        let mut tmp = buf.clone();
        let _ = ListGroupsResponse::decode_versioned(v, &mut tmp);
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

    // InitProducerIdResponse v0–v6
    for v in 0..=6 {
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

    // --- SASL (security-sensitive) ---

    // SaslHandshakeResponse v0–v1
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = SaslHandshakeResponse::decode_versioned(v, &mut tmp);
    }

    // SaslAuthenticateResponse v0–v1
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = SaslAuthenticateResponse::decode_versioned(v, &mut tmp);
    }

    // --- Config ---

    // DescribeConfigsResponse v0–v4
    for v in 0..=4 {
        let mut tmp = buf.clone();
        let _ = DescribeConfigsResponse::decode_versioned(v, &mut tmp);
    }

    // IncrementalAlterConfigsResponse v0–v1
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = IncrementalAlterConfigsResponse::decode_versioned(v, &mut tmp);
    }

    // --- Admin ---

    // CreatePartitionsResponse v0–v3
    for v in 0..=3 {
        let mut tmp = buf.clone();
        let _ = CreatePartitionsResponse::decode_versioned(v, &mut tmp);
    }

    // DeleteRecordsResponse v0–v2
    for v in 0..=2 {
        let mut tmp = buf.clone();
        let _ = DeleteRecordsResponse::decode_versioned(v, &mut tmp);
    }

    // DeleteGroupsResponse v0–v2
    for v in 0..=2 {
        let mut tmp = buf.clone();
        let _ = DeleteGroupsResponse::decode_versioned(v, &mut tmp);
    }

    // DescribeClusterResponse v0–v2
    for v in 0..=2 {
        let mut tmp = buf.clone();
        let _ = DescribeClusterResponse::decode_versioned(v, &mut tmp);
    }

    // ConsumerGroupDescribeResponse v0–v1 (KIP-848)
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = ConsumerGroupDescribeResponse::decode_versioned(v, &mut tmp);
    }

    // ListClientMetricsResourcesResponse v0 (KIP-714)
    {
        let mut tmp = buf.clone();
        let _ = ListClientMetricsResourcesResponse::decode_versioned(0, &mut tmp);
    }

    // DescribeTopicPartitionsResponse v0 (KIP-966)
    {
        let mut tmp = buf.clone();
        let _ = DescribeTopicPartitionsResponse::decode_versioned(0, &mut tmp);
    }

    // --- Quota ---

    // DescribeClientQuotasResponse v0–v1
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = DescribeClientQuotasResponse::decode_versioned(v, &mut tmp);
    }

    // AlterClientQuotasResponse v0–v1
    for v in 0..=1 {
        let mut tmp = buf.clone();
        let _ = AlterClientQuotasResponse::decode_versioned(v, &mut tmp);
    }

    // --- Delegation token ---

    // CreateDelegationTokenResponse v1–v3
    for v in 1..=3 {
        let mut tmp = buf.clone();
        let _ = CreateDelegationTokenResponse::decode_versioned(v, &mut tmp);
    }

    // RenewDelegationTokenResponse v1–v2
    for v in 1..=2 {
        let mut tmp = buf.clone();
        let _ = RenewDelegationTokenResponse::decode_versioned(v, &mut tmp);
    }

    // ExpireDelegationTokenResponse v1–v2
    for v in 1..=2 {
        let mut tmp = buf.clone();
        let _ = ExpireDelegationTokenResponse::decode_versioned(v, &mut tmp);
    }

    // DescribeDelegationTokenResponse v1–v3
    for v in 1..=3 {
        let mut tmp = buf.clone();
        let _ = DescribeDelegationTokenResponse::decode_versioned(v, &mut tmp);
    }

    // --- Telemetry (feature: telemetry) ---

    // GetTelemetrySubscriptionsResponse v0
    {
        let mut tmp = buf.clone();
        let _ = GetTelemetrySubscriptionsResponse::decode_versioned(0, &mut tmp);
    }

    // PushTelemetryResponse v0
    {
        let mut tmp = buf.clone();
        let _ = PushTelemetryResponse::decode_versioned(0, &mut tmp);
    }

    // --- Share groups (feature: unstable-protocol, KIP-932) ---

    // ShareGroupHeartbeatResponse v1
    {
        let mut tmp = buf.clone();
        let _ = ShareGroupHeartbeatResponse::decode_versioned(1, &mut tmp);
    }

    // ShareGroupDescribeResponse v1
    {
        let mut tmp = buf.clone();
        let _ = ShareGroupDescribeResponse::decode_versioned(1, &mut tmp);
    }

    // ShareFetchResponse v1–v2
    for v in 1..=2 {
        let mut tmp = buf.clone();
        let _ = ShareFetchResponse::decode_versioned(v, &mut tmp);
    }

    // ShareAcknowledgeResponse v1–v2
    for v in 1..=2 {
        let mut tmp = buf.clone();
        let _ = ShareAcknowledgeResponse::decode_versioned(v, &mut tmp);
    }
});
