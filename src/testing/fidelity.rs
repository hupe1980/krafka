//! Wire-level fidelity tests: the fake broker answers as Kafka does.
//!
//! Each test drives the broker with requests built by the crate's own
//! encoders over a raw connection, and reads the answers with the crate's own
//! decoders. No client logic sits in between, so each test pins one broker
//! rule (KIP-98, KIP-360, KIP-890, KIP-932, KIP-1206, KIP-1222) independent of
//! what any client does with it.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{Control, FakeBroker, TxnStatus};
use crate::error::ErrorCode;
use crate::protocol::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, AddPartitionsToTxnTopic, ApiKey,
    EndTxnRequest, EndTxnResponse, FetchPartitionRequest, FetchRequest, FetchResponse,
    FetchTopicRequest, InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest,
    ListOffsetsRequestPartition, ListOffsetsRequestTopic, ListOffsetsResponse, MetadataResponse,
    ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData, Record, RecordBatch,
    RequestHeader, ResponseHeader, ShareAcknowledgeRequest, ShareAcknowledgeResponse,
    ShareAcknowledgementBatch, ShareFetchPartition, ShareFetchRequest, ShareFetchResponse,
    ShareFetchTopic, ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, TaggedFields,
    VersionedDecode, VersionedEncode,
};

// ---------------------------------------------------------------------------
// A raw connection
// ---------------------------------------------------------------------------

/// One connection speaking the wire protocol directly.
struct Raw {
    stream: TcpStream,
    correlation_id: i32,
}

impl Raw {
    async fn connect(broker: &FakeBroker, node_id: i32) -> Self {
        let addr = broker.broker_addr(node_id).expect("broker exists");
        Self {
            stream: TcpStream::connect(addr).await.expect("connect"),
            correlation_id: 0,
        }
    }

    /// Send one request; `None` when the broker closed the connection instead
    /// of answering.
    async fn send(
        &mut self,
        api_key: ApiKey,
        version: i16,
        encode: impl FnOnce(&mut BytesMut),
    ) -> Option<Bytes> {
        self.correlation_id += 1;
        let mut body = BytesMut::new();
        RequestHeader::new(api_key, version, self.correlation_id)
            .with_client_id("fidelity")
            .encode(&mut body)
            .unwrap();
        encode(&mut body);
        let mut frame = BytesMut::new();
        frame.extend_from_slice(&(body.len() as i32).to_be_bytes());
        frame.extend_from_slice(&body);
        self.stream.write_all(&frame).await.ok()?;

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).await.ok()?;
        let mut response = vec![0u8; i32::from_be_bytes(len) as usize];
        self.stream.read_exact(&mut response).await.ok()?;
        let mut response = Bytes::from(response);
        assert_eq!(response.get_i32(), self.correlation_id);
        if ResponseHeader::header_version(api_key, version) == 1 {
            let _ = <TaggedFields as crate::protocol::Decode>::decode(&mut response).unwrap();
        }
        Some(response)
    }

    async fn call<Rq: VersionedEncode, Rs: VersionedDecode>(
        &mut self,
        api_key: ApiKey,
        version: i16,
        request: &Rq,
    ) -> Rs {
        let mut body = self
            .send(api_key, version, |buf| {
                request.encode_versioned(version, buf).unwrap()
            })
            .await
            .expect("the broker answered");
        Rs::decode_versioned(version, &mut body).expect("the answer decodes")
    }

    async fn produce(
        &mut self,
        version: i16,
        transactional_id: Option<&str>,
        topic: &str,
        records: Bytes,
    ) -> (ErrorCode, i64) {
        let request = ProduceRequest {
            transactional_id: transactional_id.map(str::to_string),
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![ProduceTopicData {
                name: topic.to_string(),
                topic_id: None,
                partition_data: vec![ProducePartitionData { index: 0, records }],
            }],
        };
        let response: ProduceResponse = self.call(ApiKey::Produce, version, &request).await;
        let p = &response.responses[0].partition_responses[0];
        (p.error_code, p.base_offset)
    }

    async fn init(
        &mut self,
        version: i16,
        transactional_id: Option<&str>,
        expected: (i64, i16),
    ) -> (ErrorCode, i64, i16) {
        let mut request = match transactional_id {
            Some(id) => InitProducerIdRequest::transactional(id, 60_000),
            None => InitProducerIdRequest::idempotent(),
        };
        (request.producer_id, request.producer_epoch) = expected;
        let response: InitProducerIdResponse =
            self.call(ApiKey::InitProducerId, version, &request).await;
        (
            response.error_code,
            response.producer_id,
            response.producer_epoch,
        )
    }

    async fn end_txn(
        &mut self,
        version: i16,
        transactional_id: &str,
        identity: (i64, i16),
        commit: bool,
    ) -> EndTxnResponse {
        let request = if commit {
            EndTxnRequest::commit(transactional_id, identity.0, identity.1)
        } else {
            EndTxnRequest::abort(transactional_id, identity.0, identity.1)
        };
        self.call(ApiKey::EndTxn, version, &request).await
    }

    async fn add_partition(
        &mut self,
        transactional_id: &str,
        identity: (i64, i16),
        topic: &str,
    ) -> ErrorCode {
        let mut request = AddPartitionsToTxnRequest::new(transactional_id, identity.0, identity.1);
        request.topics = vec![AddPartitionsToTxnTopic {
            name: topic.to_string(),
            partitions: vec![0],
        }];
        let response: AddPartitionsToTxnResponse =
            self.call(ApiKey::AddPartitionsToTxn, 0, &request).await;
        response.results[0].partitions[0].error_code
    }

    async fn fetch(&mut self, topic: &str, offset: i64, max_wait_ms: i32) -> FetchResponse {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms,
            min_bytes: 1,
            max_bytes: 1 << 20,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopicRequest {
                topic: topic.to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1 << 20,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: Vec::new(),
            rack_id: String::new(),
        };
        self.call(ApiKey::Fetch, 11, &request).await
    }

    async fn list_latest(&mut self, topic: &str, isolation_level: i8) -> i64 {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level,
            topics: vec![ListOffsetsRequestTopic {
                name: topic.to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    timestamp: -1,
                }],
            }],
            timeout_ms: None,
        };
        let response: ListOffsetsResponse = self.call(ApiKey::ListOffsets, 5, &request).await;
        response.topics[0].partitions[0].offset
    }
}

/// A record batch of `count` records from a producer.
fn batch(producer: (i64, i16), base_sequence: i32, count: i32, transactional: bool) -> Bytes {
    let mut b = RecordBatch::new();
    b.producer_id = producer.0;
    b.producer_epoch = producer.1;
    b.base_sequence = base_sequence;
    b.attributes.is_transactional = transactional;
    for i in 0..count {
        b.add_record(
            Record::new(None, Some(Bytes::from(format!("v{}", base_sequence + i))))
                .with_offset_delta(i),
        );
    }
    b.last_offset_delta = count - 1;
    b.encode().unwrap()
}

async fn broker_with(topic: &str) -> FakeBroker {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic(topic, 1);
    broker
}

// ---------------------------------------------------------------------------
// Producer state on the partition leader (KIP-98, KIP-360)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_retried_batch_is_acknowledged_at_its_first_offset_and_not_written_again() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, None, (-1, -1)).await;

    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 0, 3, false))
            .await,
        (ErrorCode::None, 0)
    );
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 3, 2, false))
            .await,
        (ErrorCode::None, 3)
    );
    // The retry of the first batch: same sequence range, same epoch.
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 0, 3, false))
            .await,
        (ErrorCode::None, 0),
        "a duplicate is acknowledged at the offset it was written at"
    );
    assert_eq!(
        broker.next_offset("t", 0),
        Some(5),
        "and is not written again"
    );
}

/// The negative control for de-duplication (`set_idempotence(false)`).
#[tokio::test]
async fn without_producer_state_a_retried_batch_is_written_twice() {
    let broker = broker_with("t").await;
    broker.set_idempotence(false);
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, None, (-1, -1)).await;

    raw.produce(12, None, "t", batch((pid, epoch), 0, 3, false))
        .await;
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 0, 3, false))
            .await,
        (ErrorCode::None, 3)
    );
    assert_eq!(broker.next_offset("t", 0), Some(6));
}

#[tokio::test]
async fn only_the_last_five_batches_are_remembered() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, None, (-1, -1)).await;
    for seq in 0..6 {
        raw.produce(12, None, "t", batch((pid, epoch), seq, 1, false))
            .await;
    }

    // Batch 0 has left the window: a retry of it is out of order.
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 0, 1, false))
            .await
            .0,
        ErrorCode::OutOfOrderSequenceNumber
    );
    // Batch 1 is the oldest one still remembered.
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 1, 1, false))
            .await,
        (ErrorCode::None, 1)
    );
    assert_eq!(broker.next_offset("t", 0), Some(6));
}

#[tokio::test]
async fn a_sequence_gap_is_out_of_order_and_writes_nothing() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, None, (-1, -1)).await;
    raw.produce(12, None, "t", batch((pid, epoch), 0, 2, false))
        .await;

    assert_eq!(
        raw.produce(12, None, "t", batch((pid, epoch), 3, 1, false))
            .await
            .0,
        ErrorCode::OutOfOrderSequenceNumber
    );
    assert_eq!(broker.next_offset("t", 0), Some(2));
}

#[tokio::test]
async fn epochs_fence_downward_and_restart_sequences_upward() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let pid = 77;
    raw.produce(12, None, "t", batch((pid, 3), 0, 2, false))
        .await;

    assert_eq!(
        raw.produce(12, None, "t", batch((pid, 2), 2, 1, false))
            .await
            .0,
        ErrorCode::InvalidProducerEpoch,
        "a lower epoch is fenced"
    );
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, 4), 2, 1, false))
            .await
            .0,
        ErrorCode::OutOfOrderSequenceNumber,
        "a higher epoch must start again at sequence 0"
    );
    assert_eq!(
        raw.produce(12, None, "t", batch((pid, 4), 0, 1, false))
            .await
            .0,
        ErrorCode::None
    );
    assert_eq!(broker.next_offset("t", 0), Some(3));
}

#[tokio::test]
async fn an_unknown_producer_is_accepted_at_any_sequence_unless_the_broker_predates_kip_360() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    raw.produce(12, None, "t", batch((5, 0), 0, 4, false)).await;
    assert!(broker.clear_producer_state("t", 0));

    // KIP-360: the leader lost the producer's state; it takes the batch.
    assert_eq!(
        raw.produce(12, None, "t", batch((5, 0), 4, 1, false))
            .await
            .0,
        ErrorCode::None
    );

    // A broker advertising InitProducerId below v3 is older than KIP-360.
    broker.set_api_versions(ApiKey::InitProducerId, 0, 2);
    assert!(broker.clear_producer_state("t", 0));
    assert_eq!(
        raw.produce(12, None, "t", batch((5, 0), 5, 1, false))
            .await
            .0,
        ErrorCode::UnknownProducerId
    );
}

// ---------------------------------------------------------------------------
// Transactional produce
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_tv1_write_needs_its_partition_added_and_a_tv2_write_adds_it() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;

    // Produce v11 is TV1: the partition must be in the transaction first.
    assert_eq!(
        raw.produce(11, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
            .await
            .0,
        ErrorCode::InvalidTxnState
    );
    assert_eq!(
        raw.add_partition("tx", (pid, epoch), "t").await,
        ErrorCode::None
    );
    assert_eq!(
        raw.produce(11, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
            .await
            .0,
        ErrorCode::None
    );
    assert_eq!(
        raw.end_txn(4, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::None
    );

    // Produce v12 is TV2: the write itself joins the partition.
    assert_eq!(
        raw.produce(12, Some("tx"), "t", batch((pid, epoch), 1, 1, true))
            .await
            .0,
        ErrorCode::None
    );
    assert!(broker.transaction_is_open("tx"));
    assert_eq!(broker.last_stable_offset("t", 0), Some(2));
}

#[tokio::test]
async fn a_fenced_epoch_cannot_write_transactional_records() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    let (_, _, new_epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    assert_eq!(new_epoch, epoch + 1);

    assert_eq!(
        raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
            .await
            .0,
        ErrorCode::InvalidProducerEpoch
    );
    assert_eq!(
        raw.produce(12, Some("tx"), "t", batch((pid + 1, 0), 0, 1, true))
            .await
            .0,
        ErrorCode::InvalidProducerIdMapping
    );
    assert_eq!(broker.next_offset("t", 0), Some(0));
}

// ---------------------------------------------------------------------------
// InitProducerId (KIP-360)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn init_producer_id_bumps_a_named_epoch_and_fences_a_stale_one() {
    let broker = FakeBroker::start().await.unwrap();
    let mut raw = Raw::connect(&broker, 0).await;

    let (code, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    assert_eq!((code, epoch), (ErrorCode::None, 0));

    // The producer names what it holds and gets the next epoch.
    assert_eq!(
        raw.init(5, Some("tx"), (pid, 0)).await,
        (ErrorCode::None, pid, 1)
    );
    // Its retry (the answer was lost) gets the same answer, not a second bump.
    assert_eq!(
        raw.init(5, Some("tx"), (pid, 0)).await,
        (ErrorCode::None, pid, 1)
    );
    // Any other epoch is a zombie.
    assert_eq!(
        raw.init(5, Some("tx"), (pid, 7)).await.0,
        ErrorCode::ProducerFenced
    );
    assert_eq!(
        raw.init(3, Some("tx"), (pid, 7)).await.0,
        ErrorCode::InvalidProducerEpoch,
        "PRODUCER_FENCED is new in v4"
    );
    assert_eq!(
        raw.init(5, Some("tx"), (pid + 1, 1)).await.0,
        ErrorCode::InvalidProducerIdMapping
    );
    assert_eq!(
        raw.init(5, Some("other"), (pid, 1)).await.0,
        ErrorCode::InvalidProducerIdMapping
    );
    // The non-flexible v0 and the first flexible version are served too.
    assert_eq!(
        raw.init(0, Some("tx"), (-1, -1)).await,
        (ErrorCode::None, pid, 2)
    );
    assert_eq!(
        raw.init(2, Some("tx"), (-1, -1)).await,
        (ErrorCode::None, pid, 3)
    );
}

/// Re-initialising a transactional ID aborts its open transaction: abort
/// markers at a bumped epoch, so the successor's commit cannot swallow the
/// zombie's records.
#[tokio::test]
async fn re_initialising_aborts_the_open_transaction() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
        .await;

    // The successor starts. The coordinator fences the open transaction and
    // asks it to retry.
    assert_eq!(
        raw.init(5, Some("tx"), (-1, -1)).await.0,
        ErrorCode::ConcurrentTransactions
    );
    assert_eq!(
        broker.transaction_status("tx"),
        Some(TxnStatus::CompleteAbort)
    );
    assert_eq!(broker.aborted_transactions("t", 0), vec![(pid, 0)]);

    let (code, _, successor) = raw.init(5, Some("tx"), (-1, -1)).await;
    assert_eq!((code, successor), (ErrorCode::None, epoch + 2));
    raw.produce(12, Some("tx"), "t", batch((pid, successor), 0, 1, true))
        .await;
    let committed = raw.end_txn(5, "tx", (pid, successor), true).await;
    assert_eq!(committed.error_code, ErrorCode::None);

    let values: Vec<_> = broker
        .committed_records("t")
        .unwrap()
        .into_iter()
        .map(|r| r.value.unwrap())
        .collect();
    assert_eq!(
        values,
        vec![Bytes::from("v0")],
        "only the successor's record"
    );
    assert_eq!(broker.all_records("t").unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// EndTxn and the coordinator state table (KIP-98, KIP-890)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_tv2_end_txn_bumps_the_epoch_and_its_retry_succeeds() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
        .await;

    let first = raw.end_txn(5, "tx", (pid, epoch), true).await;
    assert_eq!(first.error_code, ErrorCode::None);
    assert_eq!(first.producer_epoch, Some(epoch + 1));
    assert_eq!(
        broker.transaction_status("tx"),
        Some(TxnStatus::CompleteCommit)
    );

    // The retry carries the epoch the first attempt was sent with.
    let retry = raw.end_txn(5, "tx", (pid, epoch), true).await;
    assert_eq!(retry.error_code, ErrorCode::None);
    assert_eq!(retry.producer_id, Some(pid));
    assert_eq!(retry.producer_epoch, Some(epoch + 1));
    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch), false).await.error_code,
        ErrorCode::InvalidTxnState,
        "a retry must ask for the same outcome"
    );
    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch.wrapping_sub(1)), true)
            .await
            .error_code,
        ErrorCode::ProducerFenced
    );
    // Aborting with nothing open bumps the epoch under TV2.
    let empty_abort = raw.end_txn(5, "tx", (pid, epoch + 1), false).await;
    assert_eq!(empty_abort.error_code, ErrorCode::None);
    assert_eq!(empty_abort.producer_epoch, Some(epoch + 2));
}

#[tokio::test]
async fn a_tv1_end_txn_keeps_the_epoch_and_its_retry_succeeds() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    assert_eq!(
        raw.end_txn(4, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::InvalidTxnState,
        "nothing to commit"
    );
    raw.add_partition("tx", (pid, epoch), "t").await;
    raw.produce(11, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
        .await;

    assert_eq!(
        raw.end_txn(4, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::None
    );
    assert_eq!(broker.transactional_producer("tx"), Some((pid, epoch)));
    assert_eq!(
        raw.end_txn(4, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::None
    );
    assert_eq!(
        raw.end_txn(4, "tx", (pid, epoch), false).await.error_code,
        ErrorCode::InvalidTxnState
    );
    assert_eq!(
        raw.end_txn(4, "tx", (pid, epoch + 1), true)
            .await
            .error_code,
        ErrorCode::ProducerFenced
    );
}

#[tokio::test]
async fn held_markers_keep_the_transaction_prepared_until_written() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
        .await;
    broker.hold_transaction_markers(true);

    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::None
    );
    assert_eq!(
        broker.transaction_status("tx"),
        Some(TxnStatus::PrepareCommit)
    );
    assert_eq!(broker.last_stable_offset("t", 0), Some(0), "no marker yet");
    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::ConcurrentTransactions
    );
    assert_eq!(
        raw.init(5, Some("tx"), (-1, -1)).await.0,
        ErrorCode::ConcurrentTransactions
    );
    assert_eq!(
        raw.produce(12, Some("tx"), "t", batch((pid, epoch + 1), 0, 1, true))
            .await
            .0,
        ErrorCode::ConcurrentTransactions
    );

    broker.hold_transaction_markers(false);
    assert_eq!(
        broker.transaction_status("tx"),
        Some(TxnStatus::CompleteCommit)
    );
    assert_eq!(broker.committed_records("t").unwrap().len(), 1);
}

#[tokio::test]
async fn a_coordinator_abort_fences_the_producer() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
        .await;

    assert!(broker.abort_transaction("tx"));
    assert!(!broker.abort_transaction("tx"), "nothing left open");
    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::ProducerFenced
    );
    assert_eq!(
        raw.produce(12, Some("tx"), "t", batch((pid, epoch), 1, 1, true))
            .await
            .0,
        ErrorCode::InvalidProducerEpoch
    );
    assert!(broker.committed_records("t").unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Faults
// ---------------------------------------------------------------------------

/// "Outcome unknown": the request takes effect, the response never comes.
#[tokio::test]
async fn apply_then_disconnect_applies_the_request_and_drops_the_answer() {
    let broker = broker_with("t").await;
    broker.on_once(ApiKey::Produce, |_| {
        Control::ApplyThen(Box::new(Control::Disconnect))
    });
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, None, (-1, -1)).await;

    let records = batch((pid, epoch), 0, 2, false);
    let lost = raw
        .send(ApiKey::Produce, 12, |buf| {
            ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![ProduceTopicData {
                    name: "t".to_string(),
                    topic_id: None,
                    partition_data: vec![ProducePartitionData {
                        index: 0,
                        records: records.clone(),
                    }],
                }],
            }
            .encode_versioned(12, buf)
            .unwrap()
        })
        .await;
    assert!(lost.is_none(), "the connection closed without an answer");
    assert_eq!(
        broker.next_offset("t", 0),
        Some(2),
        "but the write happened"
    );

    // The retry on a new connection is recognised as the same batch.
    let mut raw = Raw::connect(&broker, 0).await;
    assert_eq!(
        raw.produce(12, None, "t", records).await,
        (ErrorCode::None, 0)
    );
    assert_eq!(broker.next_offset("t", 0), Some(2));
}

#[tokio::test]
async fn apply_then_error_answers_an_applied_end_txn_with_the_error() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 1, true))
        .await;
    broker.on_once(ApiKey::EndTxn, |_| {
        Control::ApplyThen(Box::new(Control::Error(ErrorCode::RequestTimedOut)))
    });

    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::RequestTimedOut
    );
    assert_eq!(
        broker.transaction_status("tx"),
        Some(TxnStatus::CompleteCommit)
    );
    assert_eq!(
        raw.end_txn(5, "tx", (pid, epoch), true).await.error_code,
        ErrorCode::None
    );
}

/// `Control::Error` reaches every API the broker serves.
#[tokio::test]
async fn injected_errors_decode_for_transaction_and_share_apis() {
    let broker = broker_with("t").await;
    let topic_id = broker.topic_id("t").unwrap();
    let mut raw = Raw::connect(&broker, 0).await;

    broker.on_once(ApiKey::EndTxn, |_| {
        Control::Error(ErrorCode::NotCoordinator)
    });
    assert_eq!(
        raw.end_txn(5, "tx", (1, 0), true).await.error_code,
        ErrorCode::NotCoordinator
    );
    broker.on_once(ApiKey::AddPartitionsToTxn, |_| {
        Control::Error(ErrorCode::ConcurrentTransactions)
    });
    assert_eq!(
        raw.add_partition("tx", (1, 0), "t").await,
        ErrorCode::ConcurrentTransactions
    );

    broker.on_once(ApiKey::ShareFetch, |_| {
        Control::Error(ErrorCode::ShareSessionNotFound)
    });
    let response = share_fetch(&mut raw, 2, 0, topic_id, Vec::new(), 0).await;
    assert_eq!(response.error_code, ErrorCode::ShareSessionNotFound);

    broker.on_once(ApiKey::ShareFetch, |_| {
        Control::Error(ErrorCode::NotLeaderForPartition)
    });
    let response = share_fetch(&mut raw, 1, 0, topic_id, Vec::new(), 0).await;
    assert_eq!(response.error_code, ErrorCode::None);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        ErrorCode::NotLeaderForPartition
    );

    broker.on_once(ApiKey::ShareAcknowledge, |_| {
        Control::Error(ErrorCode::InvalidShareSessionEpoch)
    });
    let response = share_acknowledge(&mut raw, 2, 1, topic_id, Vec::new()).await;
    assert_eq!(response.error_code, ErrorCode::InvalidShareSessionEpoch);
}

// ---------------------------------------------------------------------------
// Fetch long-poll, ListOffsets isolation, throttling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_fetch_with_nothing_to_return_waits_for_max_wait() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;

    let started = Instant::now();
    let response = raw.fetch("t", 0, 300).await;
    assert!(started.elapsed() >= Duration::from_millis(250));
    assert!(
        response.responses[0].partitions[0]
            .records
            .as_ref()
            .is_none_or(Bytes::is_empty)
    );

    // An error is answered at once.
    let started = Instant::now();
    let response = raw.fetch("t", 10, 5_000).await;
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        ErrorCode::OffsetOutOfRange
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn a_waiting_fetch_is_answered_as_soon_as_data_arrives() {
    let broker = broker_with("t").await;
    let mut consumer = Raw::connect(&broker, 0).await;
    let mut producer = Raw::connect(&broker, 0).await;

    let started = Instant::now();
    let fetch = tokio::spawn(async move { consumer.fetch("t", 0, 10_000).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    producer
        .produce(12, None, "t", batch((-1, -1), -1, 1, false))
        .await;

    let response = fetch.await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(
        response.responses[0].partitions[0]
            .records
            .as_ref()
            .is_some_and(|r| !r.is_empty())
    );
}

#[tokio::test]
async fn list_offsets_latest_is_the_last_stable_offset_for_read_committed() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    raw.produce(12, None, "t", batch((-1, -1), -1, 2, false))
        .await;
    let (_, pid, epoch) = raw.init(5, Some("tx"), (-1, -1)).await;
    raw.produce(12, Some("tx"), "t", batch((pid, epoch), 0, 3, true))
        .await;

    assert_eq!(raw.list_latest("t", 0).await, 5);
    assert_eq!(raw.list_latest("t", 1).await, 2);
}

#[tokio::test]
async fn set_throttle_reports_throttle_time_on_the_api() {
    let broker = broker_with("t").await;
    broker.set_throttle(ApiKey::Produce, Duration::from_millis(250));
    broker.set_throttle(ApiKey::InitProducerId, Duration::from_millis(40));
    let mut raw = Raw::connect(&broker, 0).await;

    let response: InitProducerIdResponse = raw
        .call(
            ApiKey::InitProducerId,
            5,
            &InitProducerIdRequest::idempotent(),
        )
        .await;
    assert_eq!(response.throttle_time_ms, 40);

    let request = ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![ProduceTopicData {
            name: "t".to_string(),
            topic_id: None,
            partition_data: vec![ProducePartitionData {
                index: 0,
                records: batch((-1, -1), -1, 1, false),
            }],
        }],
    };
    let response: ProduceResponse = raw.call(ApiKey::Produce, 12, &request).await;
    assert_eq!(response.throttle_time_ms, 250);

    broker.set_throttle(ApiKey::Produce, Duration::ZERO);
    let response: ProduceResponse = raw.call(ApiKey::Produce, 12, &request).await;
    assert_eq!(response.throttle_time_ms, 0);
}

// ---------------------------------------------------------------------------
// Share groups (KIP-932, KIP-1206, KIP-1222)
// ---------------------------------------------------------------------------

async fn share_fetch(
    raw: &mut Raw,
    version: i16,
    epoch: i32,
    topic_id: [u8; 16],
    acks: Vec<ShareAcknowledgementBatch>,
    max_records: i32,
) -> ShareFetchResponse {
    share_fetch_as(
        raw,
        "m1",
        version,
        epoch,
        topic_id,
        acks,
        max_records,
        0,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn share_fetch_as(
    raw: &mut Raw,
    member: &str,
    version: i16,
    epoch: i32,
    topic_id: [u8; 16],
    acks: Vec<ShareAcknowledgementBatch>,
    max_records: i32,
    acquire_mode: i8,
    renew: bool,
) -> ShareFetchResponse {
    let request = ShareFetchRequest {
        group_id: Some("sg".to_string()),
        member_id: Some(member.to_string()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        max_records,
        batch_size: 500,
        topics: vec![ShareFetchTopic {
            topic_id,
            partitions: vec![ShareFetchPartition {
                partition_index: 0,
                acknowledgement_batches: acks,
            }],
        }],
        forgotten_topics: Vec::new(),
    };
    let mut body = raw
        .send(ApiKey::ShareFetch, version, |buf| {
            if version >= 2 {
                request.encode_v2(buf, acquire_mode, renew).unwrap()
            } else {
                request.encode_v1(buf).unwrap()
            }
        })
        .await
        .expect("answered");
    ShareFetchResponse::decode_versioned(version, &mut body).unwrap()
}

async fn share_acknowledge(
    raw: &mut Raw,
    version: i16,
    epoch: i32,
    topic_id: [u8; 16],
    acks: Vec<ShareAcknowledgementBatch>,
) -> ShareAcknowledgeResponse {
    let request = ShareAcknowledgeRequest {
        group_id: Some("sg".to_string()),
        member_id: Some("m1".to_string()),
        share_session_epoch: epoch,
        topics: vec![crate::protocol::ShareAcknowledgeTopic {
            topic_id,
            partitions: vec![crate::protocol::ShareAcknowledgePartition {
                partition_index: 0,
                acknowledgement_batches: acks,
            }],
        }],
    };
    let mut body = raw
        .send(ApiKey::ShareAcknowledge, version, |buf| {
            if version >= 2 {
                request.encode_v2(buf, false).unwrap()
            } else {
                request.encode_v1(buf).unwrap()
            }
        })
        .await
        .expect("answered");
    ShareAcknowledgeResponse::decode_versioned(version, &mut body).unwrap()
}

fn ack(first: i64, last: i64, ack_type: i8) -> Vec<ShareAcknowledgementBatch> {
    vec![ShareAcknowledgementBatch {
        first_offset: first,
        last_offset: last,
        acknowledge_types: vec![ack_type],
    }]
}

async fn share_broker() -> (FakeBroker, [u8; 16]) {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    for _ in 0..3 {
        raw.produce(12, None, "t", batch((-1, -1), -1, 2, false))
            .await;
    }
    let topic_id = broker.topic_id("t").unwrap();
    (broker, topic_id)
}

#[tokio::test]
async fn share_sessions_validate_their_epoch() {
    let (broker, topic_id) = share_broker().await;
    let mut raw = Raw::connect(&broker, 0).await;

    assert_eq!(
        share_fetch(&mut raw, 1, 1, topic_id, Vec::new(), 0)
            .await
            .error_code,
        ErrorCode::ShareSessionNotFound
    );
    assert_eq!(
        share_fetch(&mut raw, 1, 0, topic_id, ack(0, 0, 1), 0)
            .await
            .error_code,
        ErrorCode::InvalidRequest,
        "acknowledgements cannot open a session"
    );
    assert_eq!(
        share_acknowledge(&mut raw, 1, 0, topic_id, ack(0, 0, 1))
            .await
            .error_code,
        ErrorCode::InvalidShareSessionEpoch
    );

    assert_eq!(
        share_fetch(&mut raw, 1, 0, topic_id, Vec::new(), 2)
            .await
            .error_code,
        ErrorCode::None
    );
    assert_eq!(
        share_fetch(&mut raw, 1, 2, topic_id, Vec::new(), 2)
            .await
            .error_code,
        ErrorCode::InvalidShareSessionEpoch
    );
    assert_eq!(
        share_acknowledge(&mut raw, 1, 1, topic_id, ack(0, 1, 1))
            .await
            .error_code,
        ErrorCode::None
    );
    assert_eq!(
        share_fetch(&mut raw, 1, 2, topic_id, Vec::new(), 2)
            .await
            .error_code,
        ErrorCode::None
    );
    assert_eq!(
        share_fetch(&mut raw, 1, -1, topic_id, Vec::new(), 0)
            .await
            .error_code,
        ErrorCode::None
    );
    assert_eq!(
        share_fetch(&mut raw, 1, 3, topic_id, Vec::new(), 0)
            .await
            .error_code,
        ErrorCode::ShareSessionNotFound,
        "the session is closed"
    );
}

#[tokio::test]
async fn acknowledging_a_record_the_member_does_not_hold_is_invalid_record_state() {
    let (broker, topic_id) = share_broker().await;
    let mut m1 = Raw::connect(&broker, 0).await;
    let mut m2 = Raw::connect(&broker, 0).await;

    let fetched = share_fetch(&mut m1, 1, 0, topic_id, Vec::new(), 2).await;
    let acquired = &fetched.responses[0].partitions[0].acquired_records;
    assert_eq!((acquired[0].first_offset, acquired[0].last_offset), (0, 1));
    share_fetch_as(&mut m2, "m2", 1, 0, topic_id, Vec::new(), 2, 0, false).await;

    // m1 holds 0..=1, m2 holds 2..=3, nobody holds 4..=5.
    let mut epoch = 1;
    for (first, last) in [(2, 2), (4, 4), (1, 2)] {
        let response = share_acknowledge(&mut m1, 1, epoch, topic_id, ack(first, last, 1)).await;
        assert_eq!(response.error_code, ErrorCode::None);
        assert_eq!(
            response.responses[0].partitions[0].error_code,
            ErrorCode::InvalidRecordState,
            "{first}..={last}"
        );
        epoch += 1;
    }
    let start =
        broker.with_state(|s| s.share_groups["sg"].partitions[&("t".into(), 0)].start_offset);
    assert_eq!(start, 0, "the refused range changed nothing");

    let response = share_acknowledge(&mut m1, 1, epoch, topic_id, ack(0, 1, 1)).await;
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        ErrorCode::None
    );
    let start =
        broker.with_state(|s| s.share_groups["sg"].partitions[&("t".into(), 0)].start_offset);
    assert_eq!(start, 2);
}

#[tokio::test]
async fn a_share_member_joins_with_a_subscription() {
    let broker = broker_with("t").await;
    let mut raw = Raw::connect(&broker, 0).await;
    let heartbeat = |subscribed: Option<Vec<String>>| ShareGroupHeartbeatRequest {
        group_id: "sg".to_string(),
        member_id: "m1".to_string(),
        member_epoch: 0,
        rack_id: None,
        subscribed_topic_names: subscribed,
    };
    let response: ShareGroupHeartbeatResponse = raw
        .call(ApiKey::ShareGroupHeartbeat, 1, &heartbeat(None))
        .await;
    assert_eq!(response.error_code, ErrorCode::InvalidRequest);

    let response: ShareGroupHeartbeatResponse = raw
        .call(
            ApiKey::ShareGroupHeartbeat,
            1,
            &heartbeat(Some(vec!["t".into()])),
        )
        .await;
    assert_eq!(response.error_code, ErrorCode::None);
    assert!(response.member_epoch > 0);
}

#[tokio::test]
async fn share_v2_honours_record_limit_and_renew() {
    let (broker, topic_id) = share_broker().await;
    let mut raw = Raw::connect(&broker, 0).await;

    // Record-limit mode acquires exactly max_records, splitting a batch.
    let response = share_fetch_as(&mut raw, "m1", 2, 0, topic_id, Vec::new(), 3, 1, false).await;
    let acquired = &response.responses[0].partitions[0].acquired_records;
    assert_eq!((acquired[0].first_offset, acquired[0].last_offset), (0, 2));

    // Batch-optimised mode finishes the batch it started.
    let response = share_fetch_as(&mut raw, "m2", 2, 0, topic_id, Vec::new(), 1, 0, false).await;
    let acquired = &response.responses[0].partitions[0].acquired_records;
    assert_eq!((acquired[0].first_offset, acquired[0].last_offset), (3, 3));

    // RENEW is a v2 acknowledgement; it keeps the record acquired and the
    // renew request fetches nothing.
    let response = share_fetch_as(&mut raw, "m1", 2, 1, topic_id, ack(0, 2, 4), 10, 0, true).await;
    let partition = &response.responses[0].partitions[0];
    assert_eq!(partition.acknowledge_error_code, ErrorCode::None);
    assert!(partition.acquired_records.is_empty());
    assert!(
        broker.with_state(|s| {
            s.share_groups["sg"].partitions[&("t".into(), 0)].held_by(0, 2, "m1")
        })
    );

    let mut v1 = Raw::connect(&broker, 0).await;
    share_fetch_as(&mut v1, "m3", 1, 0, topic_id, Vec::new(), 1, 0, false).await;
    let response = share_fetch_as(&mut v1, "m3", 1, 1, topic_id, ack(4, 5, 4), 0, 0, false).await;
    assert_eq!(
        response.responses[0].partitions[0].acknowledge_error_code,
        ErrorCode::InvalidRequest,
        "RENEW is not a v1 acknowledgement"
    );
}

#[tokio::test]
async fn a_share_fetch_with_nothing_to_acquire_waits() {
    let (broker, topic_id) = share_broker().await;
    let mut raw = Raw::connect(&broker, 0).await;
    share_fetch(&mut raw, 1, 0, topic_id, Vec::new(), 100).await;

    let request = ShareFetchRequest {
        group_id: Some("sg".to_string()),
        member_id: Some("m1".to_string()),
        share_session_epoch: 1,
        max_wait_ms: 300,
        min_bytes: 1,
        max_bytes: 1 << 20,
        max_records: 100,
        batch_size: 500,
        topics: vec![ShareFetchTopic {
            topic_id,
            partitions: vec![ShareFetchPartition {
                partition_index: 0,
                acknowledgement_batches: ack(0, 5, 1),
            }],
        }],
        forgotten_topics: Vec::new(),
    };
    let started = Instant::now();
    let mut body = raw
        .send(ApiKey::ShareFetch, 1, |buf| request.encode_v1(buf).unwrap())
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(250));
    let response = ShareFetchResponse::decode_versioned(1, &mut body).unwrap();
    let partition = &response.responses[0].partitions[0];
    assert_eq!(
        partition.acknowledge_error_code,
        ErrorCode::None,
        "acks applied once"
    );
    assert!(partition.acquired_records.is_empty());
}

// ---------------------------------------------------------------------------
// Cluster shape
// ---------------------------------------------------------------------------

#[tokio::test]
async fn racks_topic_recreation_and_unhandled_overrides_are_visible_on_the_wire() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("t", 1);
    broker.set_leader("t", 0, 1);
    broker.set_broker_rack(1, Some("rack-b"));
    let first_id = broker.topic_id("t").unwrap();

    assert!(broker.delete_topic("t"));
    assert!(!broker.delete_topic("t"));
    broker.create_topic("t", 1);
    let second_id = broker.topic_id("t").unwrap();
    assert_ne!(first_id, second_id, "a recreated topic has a new ID");

    let mut raw = Raw::connect(&broker, 0).await;
    let mut body = raw
        .send(ApiKey::Metadata, 12, |buf| {
            let request = crate::protocol::MetadataRequest::for_topics(vec!["t"]);
            request.encode_versioned(12, buf).unwrap()
        })
        .await
        .unwrap();
    let metadata = MetadataResponse::decode_versioned(12, &mut body).unwrap();
    let rack = metadata
        .brokers
        .iter()
        .find(|b| b.node_id == 1)
        .and_then(|b| b.rack.clone());
    assert_eq!(rack.as_deref(), Some("rack-b"));
    let topic = metadata.find_topic("t").unwrap();
    assert_eq!(topic.topic_id, Some(second_id));
    assert_eq!(topic.partitions[0].leader_epoch, 0);

    // An override names an API this broker has no handler for.
    broker.set_api_versions(ApiKey::DescribeCluster, 0, 1);
    let mut body = raw
        .send(ApiKey::ApiVersions, 3, |buf| {
            crate::protocol::ApiVersionsRequest::new()
                .encode_v3(buf)
                .unwrap()
        })
        .await
        .unwrap();
    let versions = crate::protocol::ApiVersionsResponse::decode_v3(&mut body).unwrap();
    assert!(
        versions
            .api_keys
            .iter()
            .any(|v| v.api_key == ApiKey::DescribeCluster && v.max_version == 1)
    );
    assert!(
        versions
            .api_keys
            .iter()
            .any(|v| v.api_key == ApiKey::ShareFetch && v.max_version == 2)
    );
}
