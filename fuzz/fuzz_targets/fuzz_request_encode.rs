#![no_main]
//! Fuzz the **request encode** paths.
//!
//! Every response decoder is already exercised by `fuzz_response_decode`, but
//! the encode direction had no coverage at all. Encoding is reached with
//! application-supplied data (topic names, group ids, assignment blobs,
//! partition lists), so an encoder that panics — on an arithmetic overflow, a
//! slice index, or a length conversion — is a remote-triggerable crash in any
//! service that forwards user input into a Kafka call.
//!
//! The invariant asserted here is simply: **encoding must never panic**, for
//! any constructible request value at any version the crate advertises.
//! Returning `Err` is a perfectly good outcome (e.g. a topic name longer than
//! `i16::MAX`); aborting the process is not.

use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{
    AddOffsetsToTxnRequest, ConfigResourceType, CreatableTopic, CreateTopicsRequest,
    DeleteGroupsRequest, DeleteTopicState, DeleteTopicsRequest, DescribeClusterRequest,
    DescribeConfigsRequest, DescribeConfigsResource, DescribeGroupsRequest,
    DescribeProducersRequest, DescribeProducersTopicRequest, DescribeQuorumPartitionRequest,
    DescribeQuorumRequest, DescribeQuorumTopicRequest, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsRequest, DescribeUserScramCredentialsRequest, ElectLeadersRequest,
    ElectLeadersTopicPartitions, ElectionType, EndTxnRequest, FeatureUpdateKey,
    FindCoordinatorRequest, HeartbeatRequest, InitProducerIdRequest, JoinGroupRequest,
    JoinGroupRequestProtocol, LeaveGroupMember, LeaveGroupRequest, ListGroupsRequest,
    ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopic, ListTransactionsRequest,
    MetadataRequest, MetadataRequestTopic, OffsetCommitRequest, OffsetCommitRequestPartition,
    OffsetCommitRequestTopic, SaslAuthenticateRequest, SaslHandshakeRequest, SyncGroupRequest,
    SyncGroupRequestAssignment, UpdateFeaturesRequest, VersionedEncode,
};

/// Minimal byte-oriented reader used to derive request field values from the
/// fuzzer's input. Deliberately saturating rather than failing so that short
/// inputs still produce a valid (if boring) request.
struct Gen<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Gen<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn u8(&mut self) -> u8 {
        let b = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos = self.pos.saturating_add(1);
        b
    }

    fn i8(&mut self) -> i8 {
        self.u8() as i8
    }

    fn bool(&mut self) -> bool {
        self.u8() & 1 == 1
    }

    fn i16(&mut self) -> i16 {
        i16::from_le_bytes([self.u8(), self.u8()])
    }

    fn i32(&mut self) -> i32 {
        i32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }

    fn i64(&mut self) -> i64 {
        let mut b = [0u8; 8];
        for slot in &mut b {
            *slot = self.u8();
        }
        i64::from_le_bytes(b)
    }

    fn uuid(&mut self) -> [u8; 16] {
        let mut b = [0u8; 16];
        for slot in &mut b {
            *slot = self.u8();
        }
        b
    }

    /// A short, possibly non-ASCII string. Length is bounded so the fuzzer
    /// spends its budget on structure rather than on one huge name.
    fn string(&mut self) -> String {
        let len = (self.u8() % 24) as usize;
        let start = self.pos.min(self.data.len());
        let end = start.saturating_add(len).min(self.data.len());
        self.pos = end;
        String::from_utf8_lossy(&self.data[start..end]).into_owned()
    }

    fn opt_string(&mut self) -> Option<String> {
        if self.bool() {
            Some(self.string())
        } else {
            None
        }
    }

    fn bytes(&mut self) -> Bytes {
        let len = (self.u8() % 24) as usize;
        let start = self.pos.min(self.data.len());
        let end = start.saturating_add(len).min(self.data.len());
        self.pos = end;
        Bytes::copy_from_slice(&self.data[start..end])
    }

    /// Element count for a generated array, capped to keep iterations cheap.
    fn count(&mut self) -> usize {
        (self.u8() % 4) as usize
    }

    fn strings(&mut self) -> Vec<String> {
        (0..self.count()).map(|_| self.string()).collect()
    }

    fn i32s(&mut self) -> Vec<i32> {
        (0..self.count()).map(|_| self.i32()).collect()
    }
}

/// Encode `$req` at a version drawn from `$min ..= $min + $count - 1`.
///
/// The result is intentionally discarded: `Err` is a valid outcome, only a
/// panic is a bug.
macro_rules! enc {
    ($g:expr, $ver_byte:expr, $req:expr, $min:expr, $count:expr) => {{
        let version = $min + ($ver_byte % $count) as i16;
        let mut buf = BytesMut::new();
        let _ = $req.encode_versioned(version, &mut buf);
    }};
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let api = data[0];
    let ver_byte = data[1];
    let g = &mut Gen::new(&data[2..]);

    // Version ranges mirror each type's `VersionedEncode::encode_versioned`
    // match arms; out-of-range versions return immediately and waste the input.
    match api % 28 {
        0 => {
            let topics = (0..g.count())
                .map(|_| MetadataRequestTopic {
                    topic_id: if g.bool() { Some(g.uuid()) } else { None },
                    name: g.opt_string(),
                })
                .collect();
            let req = MetadataRequest {
                topics: if g.bool() { Some(topics) } else { None },
                allow_auto_topic_creation: g.bool(),
            };
            enc!(g, ver_byte, req, 1, 13);
        }
        1 => {
            // Exercises the nullable-struct presence byte on the pagination
            // cursor.
            let req = DescribeTopicPartitionsRequest {
                topics: g.strings(),
                response_partition_limit: g.i32(),
                cursor: if g.bool() {
                    Some(DescribeTopicPartitionsCursor {
                        topic_name: g.string(),
                        partition_index: g.i32(),
                    })
                } else {
                    None
                },
            };
            enc!(g, ver_byte, req, 0, 1);
        }
        2 => {
            let topics = (0..g.count())
                .map(|_| ListOffsetsRequestTopic {
                    name: g.string(),
                    partitions: (0..g.count())
                        .map(|_| ListOffsetsRequestPartition {
                            partition_index: g.i32(),
                            current_leader_epoch: g.i32(),
                            timestamp: g.i64(),
                        })
                        .collect(),
                })
                .collect();
            let req = ListOffsetsRequest {
                replica_id: g.i32(),
                isolation_level: g.i8(),
                topics,
                timeout_ms: if g.bool() { Some(g.i32()) } else { None },
            };
            enc!(g, ver_byte, req, 1, 11);
        }
        3 => {
            let topics = (0..g.count())
                .map(|_| OffsetCommitRequestTopic {
                    name: g.string(),
                    topic_id: if g.bool() { Some(g.uuid()) } else { None },
                    partitions: (0..g.count())
                        .map(|_| OffsetCommitRequestPartition {
                            partition_index: g.i32(),
                            committed_offset: g.i64(),
                            committed_leader_epoch: g.i32(),
                            commit_timestamp: g.i64(),
                            committed_metadata: g.opt_string(),
                        })
                        .collect(),
                })
                .collect();
            let req = OffsetCommitRequest {
                group_id: g.string(),
                generation_id: g.i32(),
                member_id: g.string(),
                group_instance_id: g.opt_string(),
                retention_time_ms: g.i64(),
                topics,
            };
            enc!(g, ver_byte, req, 2, 9);
        }
        4 => {
            let protocols = (0..g.count())
                .map(|_| JoinGroupRequestProtocol {
                    name: g.string(),
                    metadata: g.bytes(),
                })
                .collect();
            let req = JoinGroupRequest {
                group_id: g.string(),
                session_timeout_ms: g.i32(),
                rebalance_timeout_ms: g.i32(),
                member_id: g.string(),
                group_instance_id: g.opt_string(),
                protocol_type: g.string(),
                protocols,
                reason: g.opt_string(),
            };
            enc!(g, ver_byte, req, 4, 6);
        }
        5 => {
            let assignments = (0..g.count())
                .map(|_| SyncGroupRequestAssignment {
                    member_id: g.string(),
                    assignment: g.bytes(),
                })
                .collect();
            let req = SyncGroupRequest {
                group_id: g.string(),
                generation_id: g.i32(),
                member_id: g.string(),
                group_instance_id: g.opt_string(),
                protocol_type: g.opt_string(),
                protocol_name: g.opt_string(),
                assignments,
            };
            enc!(g, ver_byte, req, 3, 3);
        }
        6 => {
            let req = HeartbeatRequest {
                group_id: g.string(),
                generation_id: g.i32(),
                member_id: g.string(),
                group_instance_id: g.opt_string(),
            };
            enc!(g, ver_byte, req, 3, 2);
        }
        7 => {
            let members = (0..g.count())
                .map(|_| LeaveGroupMember {
                    member_id: g.string(),
                    group_instance_id: g.opt_string(),
                    reason: g.opt_string(),
                })
                .collect();
            let req = LeaveGroupRequest {
                group_id: g.string(),
                member_id: g.string(),
                members,
            };
            enc!(g, ver_byte, req, 3, 3);
        }
        8 => {
            let topics = (0..g.count())
                .map(|_| CreatableTopic {
                    name: g.string(),
                    num_partitions: g.i32(),
                    replication_factor: g.i16(),
                    assignments: Vec::new(),
                    configs: Vec::new(),
                })
                .collect();
            let req = CreateTopicsRequest {
                topics,
                timeout_ms: g.i32(),
                validate_only: g.bool(),
            };
            enc!(g, ver_byte, req, 2, 6);
        }
        9 => {
            let topics = (0..g.count())
                .map(|_| DeleteTopicState {
                    name: g.opt_string(),
                    topic_id: g.uuid(),
                })
                .collect();
            let req = DeleteTopicsRequest {
                topic_names: g.strings(),
                topics,
                timeout_ms: g.i32(),
            };
            enc!(g, ver_byte, req, 1, 6);
        }
        10 => {
            let resources = (0..g.count())
                .map(|_| DescribeConfigsResource {
                    resource_type: match g.u8() % 4 {
                        0 => ConfigResourceType::Unknown,
                        1 => ConfigResourceType::Topic,
                        2 => ConfigResourceType::Broker,
                        _ => ConfigResourceType::BrokerLogger,
                    },
                    resource_name: g.string(),
                    config_names: if g.bool() { Some(g.strings()) } else { None },
                })
                .collect();
            let req = DescribeConfigsRequest {
                resources,
                include_synonyms: g.bool(),
                include_documentation: g.bool(),
            };
            enc!(g, ver_byte, req, 0, 5);
        }
        11 => {
            let req = InitProducerIdRequest {
                transactional_id: g.opt_string(),
                transaction_timeout_ms: g.i32(),
                producer_id: g.i64(),
                producer_epoch: g.i16(),
                enable_2pc: g.bool(),
                keep_prepared_txn: g.bool(),
            };
            enc!(g, ver_byte, req, 0, 7);
        }
        12 => {
            let req = FindCoordinatorRequest {
                key: g.string(),
                key_type: g.i8(),
            };
            enc!(g, ver_byte, req, 1, 6);
        }
        13 => {
            let req = SaslHandshakeRequest {
                mechanism: g.string(),
            };
            enc!(g, ver_byte, req, 0, 2);
        }
        14 => {
            let req = SaslAuthenticateRequest {
                auth_bytes: g.bytes().to_vec(),
            };
            enc!(g, ver_byte, req, 0, 2);
        }
        15 => {
            let req = DeleteGroupsRequest {
                group_names: g.strings(),
            };
            enc!(g, ver_byte, req, 0, 3);
        }
        16 => {
            let req = ListGroupsRequest {
                states_filter: g.strings(),
                types_filter: g.strings(),
            };
            enc!(g, ver_byte, req, 1, 5);
        }
        17 => {
            let req = EndTxnRequest {
                transactional_id: g.string(),
                producer_id: g.i64(),
                producer_epoch: g.i16(),
                committed: g.bool(),
            };
            enc!(g, ver_byte, req, 0, 6);
        }
        18 => {
            let req = AddOffsetsToTxnRequest {
                transactional_id: g.string(),
                producer_id: g.i64(),
                producer_epoch: g.i16(),
                group_id: g.string(),
            };
            enc!(g, ver_byte, req, 0, 5);
        }
        19 => {
            let req = DescribeGroupsRequest {
                groups: g.strings(),
                include_authorized_operations: g.bool(),
            };
            enc!(g, ver_byte, req, 1, 6);
        }
        20 => {
            let tp = (0..g.count())
                .map(|_| ElectLeadersTopicPartitions {
                    topic: g.string(),
                    partitions: g.i32s(),
                })
                .collect();
            let req = ElectLeadersRequest {
                election_type: if g.bool() {
                    ElectionType::Preferred
                } else {
                    ElectionType::Unclean
                },
                topic_partitions: if g.bool() { Some(tp) } else { None },
                timeout_ms: g.i32(),
            };
            enc!(g, ver_byte, req, 0, 3);
        }
        21 => {
            let feature_updates = (0..g.count())
                .map(|_| {
                    if g.bool() {
                        FeatureUpdateKey::upgrade(g.string(), g.i16())
                    } else {
                        FeatureUpdateKey::safe_downgrade(g.string(), g.i16())
                    }
                })
                .collect();
            let req = UpdateFeaturesRequest {
                timeout_ms: g.i32(),
                feature_updates,
                validate_only: g.bool(),
            };
            enc!(g, ver_byte, req, 0, 2);
        }
        22 => {
            let req = DescribeClusterRequest {
                include_cluster_authorized_operations: g.bool(),
                endpoint_type: g.i8(),
                include_fenced_brokers: g.bool(),
            };
            enc!(g, ver_byte, req, 0, 3);
        }
        23 => {
            let req = ListTransactionsRequest {
                state_filters: g.strings(),
                producer_id_filters: (0..g.count()).map(|_| g.i64()).collect(),
                duration_filter: g.i64(),
                transactional_id_pattern: if g.bool() { Some(g.string()) } else { None },
            };
            enc!(g, ver_byte, req, 0, 3);
        }
        24 => {
            let topics = (0..g.count())
                .map(|_| DescribeProducersTopicRequest {
                    name: g.string(),
                    partition_indexes: g.i32s(),
                })
                .collect();
            let req = DescribeProducersRequest { topics };
            enc!(g, ver_byte, req, 0, 1);
        }
        25 => {
            let topics = (0..g.count())
                .map(|_| DescribeQuorumTopicRequest {
                    topic_name: g.string(),
                    partitions: (0..g.count())
                        .map(|_| DescribeQuorumPartitionRequest {
                            partition_index: g.i32(),
                        })
                        .collect(),
                })
                .collect();
            let req = DescribeQuorumRequest { topics };
            enc!(g, ver_byte, req, 0, 1);
        }
        26 => {
            let topics = (0..g.count())
                .map(|_| ListPartitionReassignmentsTopic {
                    name: g.string(),
                    partition_indexes: g.i32s(),
                })
                .collect();
            let req = ListPartitionReassignmentsRequest {
                timeout_ms: g.i32(),
                topics: if g.bool() { Some(topics) } else { None },
            };
            enc!(g, ver_byte, req, 0, 1);
        }
        27 => {
            let req = DescribeUserScramCredentialsRequest {
                users: if g.bool() { Some(g.strings()) } else { None },
            };
            enc!(g, ver_byte, req, 0, 1);
        }
        _ => unreachable!(),
    }
});
