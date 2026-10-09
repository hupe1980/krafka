#![no_main]

//! Every response decoder at every version krafka negotiates.
//!
//! The (API, version) pair comes from `SUPPORTED_API_VERSIONS`, the runtime
//! form of the `api_versions!` table, so a raised MAX or a new row is fuzzed
//! without editing this file. Only the API → response-type mapping is written
//! here, and `just fuzz-coverage` fails when an API in the table has no arm.
//!
//! The SASL responses are outside the table: the pre-authentication path
//! pins them (`SaslHandshake` v0 decode, `SaslAuthenticate` v1), and they are
//! fuzzed at exactly those versions.
//!
//! Input: byte 0 picks a table row (or an off-table decoder), byte 1 a version
//! inside the row's range, the rest is the response body.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use krafka::__private::protocol::versions::SUPPORTED_API_VERSIONS;
use krafka::__private::protocol::*;

/// Decode `buf` as the response of `api` at `version`.
fn decode(api: ApiKey, version: i16, buf: &mut Bytes) {
    macro_rules! dispatch {
        ($($key:ident => $ty:ty),* $(,)?) => {
            match api {
                $(ApiKey::$key => { let _ = <$ty>::decode_versioned(version, buf); })*
                ApiKey::ApiVersions => {
                    let _ = match version {
                        0 => ApiVersionsResponse::decode_v0(buf),
                        1..=2 => ApiVersionsResponse::decode_v1(buf),
                        _ => ApiVersionsResponse::decode_v3(buf),
                    };
                }
                other => panic!("no fuzz path for {other:?}: add an arm to fuzz_response_decode"),
            }
        };
    }
    dispatch! {
        Produce => ProduceResponse,
        Fetch => FetchResponse,
        ListOffsets => ListOffsetsResponse,
        Metadata => MetadataResponse,
        OffsetCommit => OffsetCommitResponse,
        OffsetFetch => OffsetFetchResponse,
        FindCoordinator => FindCoordinatorResponse,
        JoinGroup => JoinGroupResponse,
        Heartbeat => HeartbeatResponse,
        LeaveGroup => LeaveGroupResponse,
        SyncGroup => SyncGroupResponse,
        DescribeGroups => DescribeGroupsResponse,
        ListGroups => ListGroupsResponse,
        CreateTopics => CreateTopicsResponse,
        DeleteTopics => DeleteTopicsResponse,
        DeleteRecords => DeleteRecordsResponse,
        InitProducerId => InitProducerIdResponse,
        OffsetForLeaderEpoch => OffsetForLeaderEpochResponse,
        AddPartitionsToTxn => AddPartitionsToTxnResponse,
        AddOffsetsToTxn => AddOffsetsToTxnResponse,
        EndTxn => EndTxnResponse,
        WriteTxnMarkers => WriteTxnMarkersResponse,
        TxnOffsetCommit => TxnOffsetCommitResponse,
        DescribeAcls => DescribeAclsResponse,
        CreateAcls => CreateAclsResponse,
        DeleteAcls => DeleteAclsResponse,
        DescribeConfigs => DescribeConfigsResponse,
        AlterReplicaLogDirs => AlterReplicaLogDirsResponse,
        DescribeLogDirs => DescribeLogDirsResponse,
        CreatePartitions => CreatePartitionsResponse,
        CreateDelegationToken => CreateDelegationTokenResponse,
        RenewDelegationToken => RenewDelegationTokenResponse,
        ExpireDelegationToken => ExpireDelegationTokenResponse,
        DescribeDelegationToken => DescribeDelegationTokenResponse,
        DeleteGroups => DeleteGroupsResponse,
        ElectLeaders => ElectLeadersResponse,
        IncrementalAlterConfigs => IncrementalAlterConfigsResponse,
        AlterPartitionReassignments => AlterPartitionReassignmentsResponse,
        ListPartitionReassignments => ListPartitionReassignmentsResponse,
        OffsetDelete => OffsetDeleteResponse,
        DescribeClientQuotas => DescribeClientQuotasResponse,
        AlterClientQuotas => AlterClientQuotasResponse,
        DescribeUserScramCredentials => DescribeUserScramCredentialsResponse,
        AlterUserScramCredentials => AlterUserScramCredentialsResponse,
        DescribeQuorum => DescribeQuorumResponse,
        UpdateFeatures => UpdateFeaturesResponse,
        DescribeCluster => DescribeClusterResponse,
        DescribeProducers => DescribeProducersResponse,
        DescribeTransactions => DescribeTransactionsResponse,
        ListTransactions => ListTransactionsResponse,
        ConsumerGroupHeartbeat => ConsumerGroupHeartbeatResponse,
        ConsumerGroupDescribe => ConsumerGroupDescribeResponse,
        GetTelemetrySubscriptions => GetTelemetrySubscriptionsResponse,
        PushTelemetry => PushTelemetryResponse,
        ListConfigResources => ListConfigResourcesResponse,
        DescribeTopicPartitions => DescribeTopicPartitionsResponse,
        ShareGroupHeartbeat => ShareGroupHeartbeatResponse,
        ShareGroupDescribe => ShareGroupDescribeResponse,
        ShareFetch => ShareFetchResponse,
        ShareAcknowledge => ShareAcknowledgeResponse,
        StreamsGroupDescribe => StreamsGroupDescribeResponse,
        DescribeShareGroupOffsets => DescribeShareGroupOffsetsResponse,
        AlterShareGroupOffsets => AlterShareGroupOffsetsResponse,
        DeleteShareGroupOffsets => DeleteShareGroupOffsetsResponse,
    }
}

fuzz_target!(|data: &[u8]| {
    let [selector, ver_byte, body @ ..] = data else {
        return;
    };
    let mut buf = Bytes::copy_from_slice(body);
    let rows = SUPPORTED_API_VERSIONS.len();
    match *selector as usize % (rows + 4) {
        // Not responses: these blobs carry their own version and are written
        // by another client, so they are the least trusted bytes parsed.
        i if i == rows => {
            let _ = decode_consumer_protocol_subscription(&buf);
        }
        i if i == rows + 1 => {
            let _ = decode_consumer_protocol_assignment(&buf);
        }
        i if i == rows + 2 => {
            let _ = SaslHandshakeResponse::decode_v0(&mut buf);
        }
        i if i == rows + 3 => {
            let _ = SaslAuthenticateResponse::decode_v1(&mut buf);
        }
        i => {
            let row = &SUPPORTED_API_VERSIONS[i];
            let span = (row.max_version - row.min_version + 1) as u16;
            let version = row.min_version + (u16::from(*ver_byte) % span) as i16;
            decode(ApiKey::from_i16(row.api_key), version, &mut buf);
        }
    }
});
