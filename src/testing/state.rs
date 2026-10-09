//! Mutable cluster state behind the fake broker.
//!
//! Everything a test can manipulate — brokers, topic leadership, group and
//! transaction coordinators, committed offsets, the in-memory logs — lives
//! here behind a single lock. Handlers take that lock for the duration of one
//! request, which is what makes request handling serialisable and the
//! resulting behaviour reproducible.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use bytes::Bytes;

use super::wire;
use crate::error::ErrorCode;
use crate::protocol::ApiKey;

/// Batches a partition leader remembers per producer for de-duplication,
/// matching Kafka's `ProducerStateEntry.NUM_BATCHES_TO_RETAIN`.
pub(crate) const PRODUCER_STATE_BATCHES: usize = 5;

/// A broker in the fake cluster's metadata.
#[derive(Debug, Clone)]
pub struct BrokerNode {
    /// Broker ID as advertised in Metadata responses.
    pub node_id: i32,
    /// Advertised host.
    pub host: String,
    /// Advertised port.
    pub port: i32,
    /// Advertised rack, if any.
    pub rack: Option<String>,
    /// Whether the broker is presented as reachable.
    ///
    /// A broker marked down is still listed in Metadata (real Kafka keeps
    /// listing brokers it has lost contact with) but is never chosen as a
    /// leader or coordinator by the cluster-manipulation helpers.
    pub online: bool,
}

/// One partition's log and leadership.
#[derive(Debug, Clone)]
pub struct PartitionState {
    /// Broker ID currently leading this partition.
    pub leader: i32,
    /// Current leader epoch, bumped on every leadership change.
    pub leader_epoch: i32,
    /// Replica set.
    pub replicas: Vec<i32>,
    /// In-sync replica set.
    pub isr: Vec<i32>,
    /// Stored record batches, already stamped with their broker-assigned
    /// base offsets.
    pub log: Vec<Bytes>,
    /// Offset of the first record still retained.
    pub log_start_offset: i64,
    /// Offset that the next appended record will receive. Because the fake
    /// broker acknowledges writes immediately, this doubles as the high
    /// watermark.
    pub next_offset: i64,
    /// First offset each producer with an open transaction wrote here, keyed
    /// by producer ID.
    ///
    /// The smallest of them pins the last stable offset: a `read_committed`
    /// consumer must not see past it until that transaction completes.
    pub open_transactions: HashMap<i64, i64>,
    /// Producer state the leader keeps for idempotence (KIP-98), keyed by
    /// producer ID: the epoch and the last five appended batches.
    ///
    /// It lives on the partition, so it moves with leadership the way a
    /// replicated producer snapshot does.
    pub producers: HashMap<i64, ProducerEntry>,
    /// Completed-but-aborted transactions, as
    /// `(producer_id, first_offset, marker_offset)`.
    ///
    /// The first two are what a `read_committed` fetch reports; the client
    /// uses them, together with the abort control batch, to drop the data
    /// records — so a broker that omits them makes aborted records look
    /// committed.
    ///
    /// `marker_offset` is bookkeeping. A fetch must report only the aborted
    /// transactions that **overlap the range it returns**: the client activates
    /// an entry as soon as it scans a batch at or past its `first_offset`, so
    /// an entry left over from an older, already-consumed transaction would
    /// mark its producer aborted again and silently filter that producer's
    /// *committed* batches later in the log.
    pub aborted_transactions: Vec<(i64, i64, i64)>,
}

impl PartitionState {
    fn new(leader: i32) -> Self {
        Self {
            leader,
            leader_epoch: 0,
            replicas: vec![leader],
            isr: vec![leader],
            log: Vec::new(),
            log_start_offset: 0,
            next_offset: 0,
            open_transactions: HashMap::new(),
            producers: HashMap::new(),
            aborted_transactions: Vec::new(),
        }
    }

    /// The last stable offset: the first offset a `read_committed` consumer
    /// may not read past.
    ///
    /// Derived rather than stored, so it cannot drift out of step with the
    /// open transactions it describes. With no transaction open it is the high
    /// watermark; otherwise it is the first offset of the oldest open one.
    pub(crate) fn last_stable_offset(&self) -> i64 {
        self.open_transactions
            .values()
            .copied()
            .min()
            .unwrap_or(self.next_offset)
    }

    /// Decide what the leader does with an idempotent or transactional batch,
    /// applying the broker's producer-state rules (`ProducerAppendInfo`).
    ///
    /// - An unknown producer is accepted at any sequence (KIP-360), unless
    ///   `pre_kip360` is set, which models a broker older than 2.5: a non-zero
    ///   first sequence is then `UNKNOWN_PRODUCER_ID`.
    /// - An epoch below the stored one is `INVALID_PRODUCER_EPOCH`; a higher
    ///   one must restart at sequence 0.
    /// - At the stored epoch, a batch matching one of the last five is a
    ///   duplicate, answered with the offset it was first written at; any other
    ///   batch must continue the sequence or it is
    ///   `OUT_OF_ORDER_SEQUENCE_NUMBER`.
    pub(crate) fn check_sequence(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        first_sequence: i32,
        record_count: i32,
        pre_kip360: bool,
    ) -> SequenceCheck {
        let last_sequence = last_sequence(first_sequence, record_count);
        let Some(entry) = self.producers.get(&producer_id) else {
            return if pre_kip360 && first_sequence != 0 {
                SequenceCheck::Reject(ErrorCode::UnknownProducerId)
            } else {
                SequenceCheck::Append
            };
        };
        if producer_epoch < entry.epoch {
            return SequenceCheck::Reject(ErrorCode::InvalidProducerEpoch);
        }
        if producer_epoch > entry.epoch {
            return if first_sequence == 0 {
                SequenceCheck::Append
            } else {
                SequenceCheck::Reject(ErrorCode::OutOfOrderSequenceNumber)
            };
        }
        if let Some(duplicate) = entry
            .batches
            .iter()
            .find(|b| b.first_sequence == first_sequence && b.last_sequence == last_sequence)
        {
            return SequenceCheck::Duplicate(duplicate.base_offset);
        }
        let in_sequence = match entry.batches.back() {
            Some(last) => next_sequence(last.last_sequence) == first_sequence,
            None => first_sequence == 0,
        };
        if in_sequence {
            SequenceCheck::Append
        } else {
            SequenceCheck::Reject(ErrorCode::OutOfOrderSequenceNumber)
        }
    }

    /// Remember an appended batch in its producer's entry, keeping the last
    /// [`PRODUCER_STATE_BATCHES`]. A new epoch starts a fresh entry.
    pub(crate) fn record_batch(
        &mut self,
        producer_id: i64,
        producer_epoch: i16,
        first_sequence: i32,
        record_count: i32,
        base_offset: i64,
    ) {
        let entry = self.producers.entry(producer_id).or_insert(ProducerEntry {
            epoch: producer_epoch,
            batches: VecDeque::new(),
        });
        if entry.epoch != producer_epoch {
            entry.epoch = producer_epoch;
            entry.batches.clear();
        }
        entry.batches.push_back(BatchMetadata {
            first_sequence,
            last_sequence: last_sequence(first_sequence, record_count),
            base_offset,
        });
        while entry.batches.len() > PRODUCER_STATE_BATCHES {
            entry.batches.pop_front();
        }
    }

    /// Write a transaction marker for `producer_id` and settle its open
    /// transaction here, returning the marker's offset.
    ///
    /// A marker at a higher epoch moves the producer's entry to that epoch, as
    /// a control batch does on a real leader, so the fenced epoch's writes are
    /// rejected from then on.
    pub(crate) fn append_marker(
        &mut self,
        committed: bool,
        producer_id: i64,
        producer_epoch: i16,
    ) -> i64 {
        let marker = wire::control_batch(committed, producer_id, producer_epoch);
        let marker_offset = self.append(&marker);
        let first_offset = self.open_transactions.remove(&producer_id);
        if !committed && let Some(first) = first_offset {
            self.aborted_transactions
                .push((producer_id, first, marker_offset));
        }
        let entry = self.producers.entry(producer_id).or_insert(ProducerEntry {
            epoch: producer_epoch,
            batches: VecDeque::new(),
        });
        if producer_epoch > entry.epoch {
            entry.epoch = producer_epoch;
            entry.batches.clear();
        }
        marker_offset
    }

    /// Append a producer's record batch, stamping it with the offset it was
    /// assigned, and return that base offset.
    pub(crate) fn append(&mut self, batch: &Bytes) -> i64 {
        let base_offset = self.next_offset;
        let count = wire::batch_record_count(batch).unwrap_or(0);
        self.log
            .push(wire::stamp_batch(batch, base_offset, self.leader_epoch));
        self.next_offset += count;
        base_offset
    }

    /// Concatenate every stored batch whose base offset is at or after
    /// `fetch_offset`.
    ///
    /// Batches are returned whole: a fetch landing in the middle of a batch
    /// gets the entire batch, exactly as a real broker does, leaving the
    /// client to discard the records below its requested offset.
    pub(crate) fn read_from(&self, fetch_offset: i64) -> Bytes {
        self.read_range(fetch_offset, i64::MAX)
    }

    /// As [`read_from`](Self::read_from), but stopping before `limit`.
    ///
    /// A `read_committed` fetch passes the last stable offset as `limit`: a
    /// batch belonging to a transaction that has not completed must not be
    /// returned at all, or the consumer could surface records that a later
    /// abort retracts.
    ///
    /// Whole batches only, as a real broker does — a batch straddling `limit`
    /// is withheld, since half of it is uncommitted.
    pub(crate) fn read_range(&self, fetch_offset: i64, limit: i64) -> Bytes {
        let mut out = Vec::new();
        for batch in &self.log {
            let base = wire::batch_base_offset(batch).unwrap_or(0);
            let count = wire::batch_record_count(batch).unwrap_or(0);
            if base + count > fetch_offset && base + count <= limit {
                out.extend_from_slice(batch);
            }
        }
        Bytes::from(out)
    }

    /// Aborted transactions overlapping `[fetch_offset, ..)`, as the
    /// `(producer_id, first_offset)` pairs a fetch response carries.
    ///
    /// An entry whose abort marker is already below `fetch_offset` describes a
    /// transaction this consumer has read past; reporting it would re-activate
    /// its producer and filter that producer's later committed batches.
    pub(crate) fn aborted_transactions_from(&self, fetch_offset: i64) -> Vec<(i64, i64)> {
        self.aborted_transactions
            .iter()
            .filter(|(_, _, marker_offset)| *marker_offset >= fetch_offset)
            .map(|(producer_id, first_offset, _)| (*producer_id, *first_offset))
            .collect()
    }
}

/// The last sequence of a batch, wrapping the way Kafka's sequences do.
fn last_sequence(first_sequence: i32, record_count: i32) -> i32 {
    let delta = record_count.max(1) - 1;
    if first_sequence > i32::MAX - delta {
        delta - (i32::MAX - first_sequence) - 1
    } else {
        first_sequence + delta
    }
}

/// The sequence that must follow `last_sequence`.
fn next_sequence(last_sequence: i32) -> i32 {
    if last_sequence == i32::MAX {
        0
    } else {
        last_sequence + 1
    }
}

/// What a partition leader knows about one producer ID.
#[derive(Debug, Clone, Default)]
pub struct ProducerEntry {
    /// Epoch of the producer's latest write or transaction marker here.
    pub epoch: i16,
    /// The last appended batches at that epoch, oldest first, at most five.
    pub batches: VecDeque<BatchMetadata>,
}

/// One appended batch, as remembered for de-duplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchMetadata {
    /// First sequence number in the batch.
    pub first_sequence: i32,
    /// Last sequence number in the batch.
    pub last_sequence: i32,
    /// Offset the batch was written at.
    pub base_offset: i64,
}

/// The leader's verdict on one producer batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SequenceCheck {
    /// Write it.
    Append,
    /// Already written at this offset; acknowledge without writing.
    Duplicate(i64),
    /// Refuse it with this error.
    Reject(ErrorCode),
}

/// A topic and its partitions.
#[derive(Debug, Clone)]
pub struct TopicState {
    /// Topic UUID. Only surfaced on API versions that carry it.
    pub topic_id: [u8; 16],
    /// Partitions, indexed by partition number.
    pub partitions: Vec<PartitionState>,
}

/// One transactional producer, as the transaction coordinator sees it.
///
/// Keyed by transactional ID rather than producer ID, because that is the
/// identity Kafka fences on: re-running `InitProducerId` for a known
/// transactional ID returns the **same** producer ID with a **higher** epoch,
/// which is what makes a zombie producer's writes rejected (KIP-360).
#[derive(Debug, Clone, Default)]
pub struct BrokerTransaction {
    /// Producer ID assigned to this transactional ID, stable across
    /// re-initialisation.
    pub producer_id: i64,
    /// Current epoch. Bumped by every `InitProducerId`, by fencing an open
    /// transaction, and by every `EndTxn` v5+ (KIP-890).
    pub producer_epoch: i16,
    /// The epoch before the last bump, or `-1`. Lets the coordinator answer a
    /// retried `InitProducerId` or `EndTxn` whose first attempt already
    /// bumped the epoch, instead of fencing the producer that sent it.
    pub last_producer_epoch: i16,
    /// Where the transaction is in the coordinator's state machine.
    pub status: TxnStatus,
    /// Transaction timeout the producer registered, in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Partitions this transaction has written to.
    ///
    /// Under TV1 the client registers them with `AddPartitionsToTxn`; under
    /// TV2 (KIP-890) the `Produce` request carries the transactional ID and
    /// the coordinator infers them. Both routes land here, which is what lets
    /// one commit path serve both protocols.
    pub partitions: Vec<(String, i32)>,
    /// Offsets staged by `TxnOffsetCommit`, keyed by group ID.
    ///
    /// Held back until commit: an aborted transaction must leave the group's
    /// committed offsets exactly as it found them, which is the half of
    /// exactly-once that a produce-only test never reaches.
    pub staged_offsets: HashMap<String, HashMap<(String, i32), CommittedOffset>>,
}

impl BrokerTransaction {
    /// Whether a transaction is open: records or offsets were added and it has
    /// not been ended.
    pub fn is_open(&self) -> bool {
        self.status == TxnStatus::Ongoing
    }

    /// Bump the epoch, remembering the previous one for retry detection.
    pub(crate) fn bump_epoch(&mut self) {
        self.last_producer_epoch = self.producer_epoch;
        self.producer_epoch = self.producer_epoch.saturating_add(1);
    }

    /// Mark the transaction ongoing, as the first partition or group added to
    /// it does.
    pub(crate) fn begin(&mut self) {
        if !matches!(
            self.status,
            TxnStatus::PrepareCommit | TxnStatus::PrepareAbort
        ) {
            self.status = TxnStatus::Ongoing;
        }
    }
}

/// The transaction coordinator's state for one transactional ID, named as
/// Kafka names it.
///
/// `PrepareCommit` and `PrepareAbort` last only while markers are held back
/// (see [`ClusterState::hold_transaction_markers`]); otherwise the markers are
/// written within the request that ends the transaction, and it moves straight
/// to the matching `Complete*` state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TxnStatus {
    /// No transaction has started since the producer ID was assigned.
    #[default]
    Empty,
    /// A partition or group was added; the transaction is open.
    Ongoing,
    /// `EndTxn(commit)` accepted; commit markers not yet written.
    PrepareCommit,
    /// `EndTxn(abort)` accepted, or the coordinator fenced or timed out the
    /// transaction; abort markers not yet written.
    PrepareAbort,
    /// Commit markers written.
    CompleteCommit,
    /// Abort markers written.
    CompleteAbort,
}

/// A member of a consumer group.
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Broker-assigned member ID.
    pub member_id: String,
    /// Static membership ID (KIP-345), if the member supplied one.
    pub group_instance_id: Option<String>,
    /// Subscription metadata the member sent in JoinGroup.
    pub metadata: Bytes,
    /// Client ID from the request header that joined, as DescribeGroups
    /// reports it.
    pub client_id: String,
    /// Client host, as DescribeGroups reports it.
    pub client_host: String,
}

/// A committed offset for one topic-partition in one group.
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    /// The committed offset.
    pub offset: i64,
    /// Leader epoch recorded alongside the commit, or `-1`.
    pub leader_epoch: i32,
    /// Opaque metadata attached to the commit.
    pub metadata: Option<String>,
}

/// A consumer group.
#[derive(Debug, Clone, Default)]
pub struct GroupState {
    /// Current generation, incremented on each completed join.
    pub generation_id: i32,
    /// Protocol type, e.g. `consumer`.
    pub protocol_type: String,
    /// Protocol the broker selected for the generation.
    pub protocol_name: Option<String>,
    /// Member ID of the group leader.
    pub leader: String,
    /// Current members.
    pub members: Vec<GroupMember>,
    /// Assignments distributed by SyncGroup, keyed by member ID.
    pub assignments: HashMap<String, Bytes>,
    /// Committed offsets, keyed by `(topic, partition)`.
    pub offsets: HashMap<(String, i32), CommittedOffset>,
    /// Counter behind generated member IDs.
    pub member_seq: u32,
    /// Classic group state, as DescribeGroups reports it.
    pub state: ClassicGroupState,
    /// KIP-848 members, keyed by client-generated member ID.
    ///
    /// Separate from [`Self::members`], which models the classic
    /// JoinGroup/SyncGroup protocol. The two protocols have different member
    /// identity and epoch rules, and conflating them in one map made it
    /// impossible to model either faithfully.
    pub consumer_members: HashMap<String, ConsumerGroupMemberState>,
    /// Epoch the whole group is on. Bumped whenever the set of members or
    /// their subscriptions changes, which is what forces reconciliation.
    pub group_epoch: i32,
}

/// Lifecycle state of a classic (JoinGroup/SyncGroup) group.
///
/// Only the states this harness can actually reach are modelled: there is no
/// `PreparingRebalance`, because a join here completes within the one request
/// that triggered it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClassicGroupState {
    /// No members.
    #[default]
    Empty,
    /// Members have joined; the leader has not distributed assignments yet.
    CompletingRebalance,
    /// Assignments distributed.
    Stable,
}

impl ClassicGroupState {
    /// The name Kafka puts on the wire.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Stable => "Stable",
        }
    }
}

/// One KIP-848 member's coordinator-side state.
#[derive(Debug, Clone, Default)]
pub struct ConsumerGroupMemberState {
    /// The epoch this member is currently on. `0` until its first assignment.
    pub member_epoch: i32,
    /// Static membership ID, if the member supplied one.
    pub instance_id: Option<String>,
    /// Topics the member last told the coordinator it subscribes to.
    pub subscribed_topics: Vec<String>,
    /// Partitions the coordinator has assigned, keyed by topic name.
    pub assignment: HashMap<String, Vec<i32>>,
    /// Partitions the member last *reported* owning.
    ///
    /// Distinct from [`Self::assignment`]: the coordinator may have granted
    /// partitions the member has not acknowledged yet, and may be waiting for
    /// the member to release partitions it still holds. Reconciliation is
    /// exactly the gap between these two fields.
    pub owned: HashMap<String, Vec<i32>>,
    /// Whether [`Self::assignment`] changed since it was last sent.
    ///
    /// The assignment field is only put on the wire when it moves; a null
    /// assignment means "keep what you have".
    pub assignment_dirty: bool,
    /// When the member last heartbeat, on Tokio's clock. A member silent for
    /// the session timeout is removed from the group; `None` never expires.
    pub last_heartbeat: Option<tokio::time::Instant>,
}

/// A share group (KIP-932).
///
/// # What is modelled, and what is not
///
/// A share group differs from a consumer group in the one way that matters
/// here: a partition is not *owned* by a member. The coordinator hands the
/// same partition to several members, and the broker — not the client —
/// decides which records each member gets. There is therefore no
/// revoke-before-assign reconciliation to model; assignment takes effect on
/// the heartbeat that carries it.
///
/// What *is* modelled is the share-partition state machine that replaces
/// committed offsets: a start offset (SPSO), the member holding each acquired
/// record, the records already acknowledged, and a per-record delivery count.
/// An acknowledgement is valid only for a record the acknowledging member
/// holds (`INVALID_RECORD_STATE` otherwise). `ACCEPT`, `REJECT` and `GAP`
/// archive the record; `RELEASE` makes it available again with a higher
/// delivery count; `RENEW` (KIP-1222) keeps it acquired.
///
/// Records a member holds are released when it leaves the group or closes its
/// share session.
///
/// What is **not** modelled: acquisition-lock *expiry* (an acquired record
/// comes back when its holder leaves, never on a timer) and
/// `group.share.delivery.attempts` limits. Tests must not be read as
/// validating those.
#[derive(Debug, Clone, Default)]
pub struct ShareGroupState {
    /// Epoch of the group as a whole, bumped when membership or subscriptions
    /// change.
    pub group_epoch: i32,
    /// Members, keyed by client-generated member ID.
    pub members: HashMap<String, ShareMemberState>,
    /// Per-share-partition delivery state, keyed by `(topic, partition)`.
    pub partitions: HashMap<(String, i32), SharePartitionState>,
}

/// A share session a client closed with the final epoch (`-1`), and the API
/// it used.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShareSessionClose {
    /// `ShareFetch` or `ShareAcknowledge`.
    pub api_key: ApiKey,
    /// The broker whose session was closed.
    pub node_id: i32,
    /// Share group ID.
    pub group_id: String,
    /// Member that closed the session.
    pub member_id: String,
}

/// One partition of a `ListOffsets` request, as the broker received it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ListOffsetsLookup {
    /// The broker the request reached.
    pub node_id: i32,
    /// The version the client negotiated.
    pub api_version: i16,
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// The requested timestamp or sentinel (`-1` latest, `-2` earliest, …).
    pub timestamp: i64,
}

/// One member of a `LeaveGroup` request, as the coordinator received it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct LeaveGroupMemberSeen {
    /// Group ID.
    pub group_id: String,
    /// The leaving member.
    pub member_id: String,
    /// Its static instance ID, if any.
    pub group_instance_id: Option<String>,
}

/// One `ConsumerGroupHeartbeat` (KIP-848), as the coordinator received it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConsumerGroupHeartbeatSeen {
    /// Group ID.
    pub group_id: String,
    /// Member ID.
    pub member_id: String,
    /// `0` to join, `-1` to leave, `-2` for a static member's temporary
    /// leave, else the member's current epoch.
    pub member_epoch: i32,
    /// Static instance ID, if any.
    pub instance_id: Option<String>,
    /// Requested server-side assignor, if any.
    pub server_assignor: Option<String>,
    /// Whether the heartbeat carried the subscription.
    pub full: bool,
}

/// One open share session on one broker (KIP-932).
///
/// Opened by a `ShareFetch` at epoch 0. Every later `ShareFetch` or
/// `ShareAcknowledge` must carry [`Self::epoch`], which then advances; epoch
/// `-1` closes it.
#[derive(Debug, Clone, Default)]
pub struct ShareSession {
    /// The epoch the next request in this session must carry.
    pub epoch: i32,
    /// Partitions in the session, as `(topic, partition)`.
    pub partitions: BTreeSet<(String, i32)>,
}

impl ShareSession {
    /// Advance to the epoch after `epoch`, wrapping from `i32::MAX` to 1 (0
    /// always opens a new session).
    pub(crate) fn advance(&mut self) {
        self.epoch = if self.epoch == i32::MAX {
            1
        } else {
            self.epoch + 1
        };
    }
}

/// One share-group member's coordinator-side state.
#[derive(Debug, Clone, Default)]
pub struct ShareMemberState {
    /// Epoch the coordinator last handed this member.
    pub member_epoch: i32,
    /// Topics the member last told the coordinator it subscribes to.
    pub subscribed_topics: Vec<String>,
    /// Partitions the coordinator has assigned, keyed by topic name.
    pub assignment: HashMap<String, Vec<i32>>,
    /// Whether [`Self::assignment`] changed since it was last put on the wire.
    pub assignment_dirty: bool,
}

/// Delivery state of one share partition.
#[derive(Debug, Clone, Default)]
pub struct SharePartitionState {
    /// Share-partition start offset (SPSO): nothing below this is ever
    /// delivered again.
    pub start_offset: i64,
    /// Records currently acquired, mapped to the member holding each.
    pub acquired: BTreeMap<i64, String>,
    /// Records at or above [`Self::start_offset`] that are acknowledged and
    /// will not be delivered again. The start offset advances over them as
    /// soon as they are contiguous with it.
    pub archived: BTreeSet<i64>,
    /// How many times each offset has been delivered, keyed by offset.
    pub delivery_counts: HashMap<i64, i16>,
    /// Every acknowledgement the broker applied, per offset, in the order it
    /// applied them. A record acknowledged twice, or accepted without the
    /// application having seen it, shows here.
    pub acknowledgements: BTreeMap<i64, Vec<ShareAckType>>,
}

/// One share acknowledgement type, as KIP-932 numbers them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShareAckType {
    /// 0: a gap in the log (a control record or a compacted offset).
    Gap,
    /// 1: processed.
    Accept,
    /// 2: hand to another member.
    Release,
    /// 3: archive without processing.
    Reject,
    /// 4: extend the acquisition lock (KIP-1222).
    Renew,
}

impl ShareAckType {
    fn from_wire(code: i8) -> Option<Self> {
        Some(match code {
            0 => Self::Gap,
            1 => Self::Accept,
            2 => Self::Release,
            3 => Self::Reject,
            4 => Self::Renew,
            _ => return None,
        })
    }
}

impl ShareGroupState {
    /// Release every record `member_id` holds, as a broker does when the
    /// member leaves or closes its share session.
    pub(crate) fn release_member(&mut self, member_id: &str) {
        for partition in self.partitions.values_mut() {
            partition.acquired.retain(|_, holder| holder != member_id);
        }
    }
}

impl SharePartitionState {
    /// Whether `offset` can be handed out: at or above the start offset, and
    /// neither acquired nor archived.
    pub(crate) fn is_available(&self, offset: i64) -> bool {
        offset >= self.start_offset
            && !self.acquired.contains_key(&offset)
            && !self.archived.contains(&offset)
    }

    /// Hand `offset` to `member_id`, returning its new delivery count.
    pub(crate) fn acquire(&mut self, offset: i64, member_id: &str) -> i16 {
        self.acquired.insert(offset, member_id.to_string());
        let count = self.delivery_counts.entry(offset).or_insert(0);
        *count = count.saturating_add(1);
        *count
    }

    /// Whether every offset in `[first, last]` is acquired by `member_id`, the
    /// precondition for acknowledging it.
    pub(crate) fn held_by(&self, first: i64, last: i64, member_id: &str) -> bool {
        (first..=last).all(|offset| {
            self.acquired
                .get(&offset)
                .is_some_and(|holder| holder == member_id)
        })
    }

    /// Apply one acknowledgement to `offset`.
    ///
    /// `acknowledge_type` is the KIP-932 wire value: 0 = GAP, 1 = ACCEPT,
    /// 2 = RELEASE, 3 = REJECT, 4 = RENEW (KIP-1222).
    pub(crate) fn acknowledge(&mut self, offset: i64, acknowledge_type: i8) {
        if let Some(ack) = ShareAckType::from_wire(acknowledge_type) {
            self.acknowledgements.entry(offset).or_default().push(ack);
        }
        match acknowledge_type {
            0 | 1 | 3 => {
                self.acquired.remove(&offset);
                self.delivery_counts.remove(&offset);
                self.archived.insert(offset);
                while self.archived.remove(&self.start_offset) {
                    self.start_offset += 1;
                }
            }
            2 => {
                self.acquired.remove(&offset);
            }
            // RENEW extends the acquisition lock. There is no lock timer here,
            // so the record simply stays acquired.
            _ => {}
        }
    }
}

/// A Streams group (KIP-1071), as far as `StreamsGroupDescribe` exposes it.
///
/// Deliberately a flat fixture rather than a simulation. krafka has no Streams
/// runtime, so there is no client behaviour to model here — only a response
/// for the describe path to decode. Modelling task assignment would be
/// inventing a coordinator whose behaviour nothing in this crate depends on.
#[derive(Debug, Clone, Default)]
pub struct StreamsGroupState {
    /// Group state string, e.g. `Stable`.
    pub group_state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Epoch of the initialized topology, or `None` for no topology at all.
    pub topology_epoch: Option<i32>,
    /// Subtopology IDs, or `None` for the "uninitialized / source topics
    /// missing" state, which the wire format distinguishes from an empty list.
    pub subtopologies: Option<Vec<String>>,
    /// Members.
    pub members: Vec<StreamsMemberState>,
}

/// One Streams group member.
#[derive(Debug, Clone, Default)]
pub struct StreamsMemberState {
    /// Member ID.
    pub member_id: String,
    /// Member epoch.
    pub member_epoch: i32,
    /// Epoch of the topology this member is running.
    pub topology_epoch: i32,
    /// Streams instance identity.
    pub process_id: String,
    /// Interactive Queries endpoint, if configured.
    pub user_endpoint: Option<(String, u16)>,
    /// Active tasks as `(subtopology_id, partitions)`.
    pub active_tasks: Vec<(String, Vec<i32>)>,
    /// Target active tasks. Differs from [`Self::active_tasks`] mid-rebalance.
    pub target_active_tasks: Vec<(String, Vec<i32>)>,
}

/// The whole fake cluster.
#[derive(Debug)]
pub struct ClusterState {
    /// Cluster ID reported in Metadata.
    pub cluster_id: String,
    /// Brokers, in advertised order.
    pub brokers: Vec<BrokerNode>,
    /// Broker ID currently acting as controller, or `-1` for none.
    pub controller_id: i32,
    /// Topics, keyed by name.
    pub topics: HashMap<String, TopicState>,
    /// Consumer groups, keyed by group ID.
    pub groups: HashMap<String, GroupState>,
    /// Share groups (KIP-932), keyed by group ID.
    ///
    /// Separate from [`Self::groups`]: a share group shares a namespace
    /// with consumer groups on a real broker, but has entirely different
    /// membership and delivery semantics, and conflating them would make
    /// neither modellable.
    pub share_groups: HashMap<String, ShareGroupState>,
    /// Streams groups (KIP-1071), keyed by group ID.
    ///
    /// Populated only by a test via [`ClusterState`] directly: krafka cannot
    /// *join* a Streams group — that needs `StreamsGroupHeartbeat` and an
    /// application topology — so there is nothing for the broker to derive
    /// this from. It exists so `describe_streams_groups` has something real to
    /// read back, which is the whole of what krafka does with KIP-1071.
    pub streams_groups: HashMap<String, StreamsGroupState>,
    /// Group coordinator overrides, keyed by group ID. Groups without an entry
    /// resolve to [`ClusterState::default_coordinator`].
    pub group_coordinators: HashMap<String, i32>,
    /// Transaction coordinator overrides, keyed by transactional ID.
    pub txn_coordinators: HashMap<String, i32>,
    /// Whether an unknown topic is created on first reference.
    pub auto_create_topics: bool,
    /// Partition count given to auto-created topics.
    pub default_partitions: i32,
    /// Counter behind allocated producer IDs.
    pub next_producer_id: i64,
    /// Transactions, keyed by transactional ID.
    pub transactions: HashMap<String, BrokerTransaction>,
    /// Largest transaction timeout `InitProducerId` accepts
    /// (`transaction.max.timeout.ms`, default 15 minutes).
    pub transaction_max_timeout_ms: i32,
    /// Whether partition leaders enforce producer state: sequence
    /// de-duplication and ordering, producer-epoch fencing, and the
    /// transactional checks on `Produce`. On by default, as on every broker;
    /// switching it off is the negative control for a test that relies on it.
    pub idempotence: bool,
    /// Whether `EndTxn` leaves the transaction in `PrepareCommit` or
    /// `PrepareAbort` instead of writing its markers at once.
    ///
    /// While held, the coordinator answers `CONCURRENT_TRANSACTIONS` to the
    /// same producer's next `InitProducerId`, `AddPartitionsToTxn`,
    /// `AddOffsetsToTxn` and TV2 `Produce`, as a real coordinator does while
    /// its markers are in flight.
    pub hold_transaction_markers: bool,
    /// `throttle_time_ms` to report per API (KIP-219). APIs without an entry
    /// report 0.
    pub throttle_time_ms: HashMap<ApiKey, i32>,
    /// Open share sessions, keyed by `(node_id, group_id, member_id)`.
    pub share_sessions: HashMap<(i32, String, String), ShareSession>,
    /// Cluster-finalized feature version levels (KIP-584), keyed by feature
    /// name. Written by `UpdateFeatures`, so a test can assert what the
    /// controller actually applied — or, under `validate_only`, did not.
    pub finalized_features: HashMap<String, i16>,
    /// Epoch of [`Self::finalized_features`], advanced on every change.
    pub finalized_features_epoch: i64,
    /// Advertised `ApiVersions` ranges that override the built-in table.
    ///
    /// The built-in table names one version per API — whatever the handlers
    /// actually speak. That makes every "the client must degrade against an
    /// older broker" path untestable, because there is no way to *be* an older
    /// broker: `validate_only` refused below `UpdateFeatures` v1, `Renew`
    /// stripped below `ShareFetch` v2, share-group lag absent below
    /// `DescribeShareGroupOffsets` v1. Each of those is a real branch guarding
    /// a real hazard, and each was covered only by a test that re-implemented
    /// the condition.
    pub api_version_overrides: HashMap<ApiKey, (i16, i16)>,
    /// Share sessions closed with the final epoch, in arrival order.
    pub share_session_closes: Vec<ShareSessionClose>,
    /// Every `ListOffsets` partition lookup, in arrival order.
    pub list_offsets_lookups: Vec<ListOffsetsLookup>,
    /// Every member named in a `LeaveGroup` request, in arrival order.
    pub leave_group_members: Vec<LeaveGroupMemberSeen>,
    /// Every `ConsumerGroupHeartbeat` that reached its coordinator, in
    /// arrival order.
    pub consumer_group_heartbeats: Vec<ConsumerGroupHeartbeatSeen>,
    /// SASL/PLAIN `(username, password)` every connection must authenticate
    /// with before any request but `ApiVersions`, or `None` for no SASL.
    pub sasl_plain: Option<(String, String)>,
    /// The KIP-714 subscription this cluster's client-telemetry plugin holds,
    /// or `None` for a cluster without one: `GetTelemetrySubscriptions` and
    /// `PushTelemetry` are then not advertised.
    pub telemetry: Option<TelemetrySubscription>,
    /// `PushTelemetry` requests received, in arrival order.
    pub telemetry_pushes: Vec<TelemetryPush>,
    /// Counter behind generated topic UUIDs.
    topic_id_seq: u64,
}

/// The subscription a fake cluster's client-telemetry plugin hands out
/// (KIP-714).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TelemetrySubscription {
    /// Metric-name prefixes the plugin wants; `"*"` is every metric, and an
    /// empty list none.
    pub requested_metrics: Vec<String>,
    /// How often clients push.
    pub push_interval: std::time::Duration,
    /// Whether sums are pushed as deltas.
    pub delta_temporality: bool,
    /// Compression types accepted for pushes, in preference order (Kafka's
    /// ids: 0 none, 1 gzip, …). Default: none only.
    pub accepted_compression_types: Vec<i8>,
    /// The client instance id assigned to a client that asks with the zero
    /// id.
    pub client_instance_id: [u8; 16],
    /// The subscription id; a push naming another is refused with
    /// `UNKNOWN_SUBSCRIPTION_ID`.
    pub subscription_id: i32,
}

impl TelemetrySubscription {
    /// A subscription to `requested_metrics`, pushed every `push_interval`,
    /// cumulative and uncompressed.
    pub fn new(
        requested_metrics: impl IntoIterator<Item = impl Into<String>>,
        push_interval: std::time::Duration,
    ) -> Self {
        Self {
            requested_metrics: requested_metrics.into_iter().map(Into::into).collect(),
            push_interval,
            delta_temporality: false,
            accepted_compression_types: vec![0],
            client_instance_id: *b"krafka-fake-inst",
            subscription_id: 1,
        }
    }
}

/// A `PushTelemetry` request as the fake broker received it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TelemetryPush {
    /// The client instance id the push named.
    pub client_instance_id: [u8; 16],
    /// The subscription id the push named.
    pub subscription_id: i32,
    /// Whether the client is closing.
    pub terminating: bool,
    /// The compression type of `metrics`.
    pub compression_type: i8,
    /// The OTLP `MetricsData` payload, as sent.
    pub metrics: bytes::Bytes,
}

impl ClusterState {
    /// Build a cluster with `broker_count` brokers, none of them bound to a
    /// listener yet. [`super::FakeBroker`] fills in the real host and port once
    /// the sockets are open.
    pub(crate) fn new(broker_count: usize) -> Self {
        let brokers = (0..broker_count)
            .map(|i| BrokerNode {
                node_id: i as i32,
                host: "127.0.0.1".to_string(),
                port: 0,
                rack: None,
                online: true,
            })
            .collect();
        Self {
            cluster_id: "krafka-fake-cluster".to_string(),
            brokers,
            controller_id: 0,
            topics: HashMap::new(),
            groups: HashMap::new(),
            share_groups: HashMap::new(),
            streams_groups: HashMap::new(),
            finalized_features: HashMap::new(),
            finalized_features_epoch: 0,
            api_version_overrides: HashMap::new(),
            share_session_closes: Vec::new(),
            list_offsets_lookups: Vec::new(),
            leave_group_members: Vec::new(),
            consumer_group_heartbeats: Vec::new(),
            sasl_plain: None,
            telemetry: None,
            telemetry_pushes: Vec::new(),
            group_coordinators: HashMap::new(),
            txn_coordinators: HashMap::new(),
            auto_create_topics: true,
            default_partitions: 1,
            next_producer_id: 1000,
            transactions: HashMap::new(),
            transaction_max_timeout_ms: 900_000,
            idempotence: true,
            hold_transaction_markers: false,
            throttle_time_ms: HashMap::new(),
            share_sessions: HashMap::new(),
            topic_id_seq: 1,
        }
    }

    /// The broker every group and transaction resolves to unless a test has
    /// moved it: the lowest-numbered online broker.
    pub fn default_coordinator(&self) -> i32 {
        self.brokers
            .iter()
            .find(|b| b.online)
            .map(|b| b.node_id)
            .unwrap_or(-1)
    }

    /// Resolve the coordinator for a consumer group.
    pub fn group_coordinator(&self, group_id: &str) -> i32 {
        self.group_coordinators
            .get(group_id)
            .copied()
            .unwrap_or_else(|| self.default_coordinator())
    }

    /// Resolve the coordinator for a transactional ID.
    pub fn txn_coordinator(&self, transactional_id: &str) -> i32 {
        self.txn_coordinators
            .get(transactional_id)
            .copied()
            .unwrap_or_else(|| self.default_coordinator())
    }

    /// Look up a broker by ID.
    pub fn broker(&self, node_id: i32) -> Option<&BrokerNode> {
        self.brokers.iter().find(|b| b.node_id == node_id)
    }

    /// Create a topic with `partitions` partitions, spreading leadership
    /// round-robin over the online brokers. Existing topics are left alone.
    pub fn create_topic(&mut self, name: &str, partitions: i32) -> bool {
        if self.topics.contains_key(name) {
            return false;
        }
        let online: Vec<i32> = self
            .brokers
            .iter()
            .filter(|b| b.online)
            .map(|b| b.node_id)
            .collect();
        let partition_states = (0..partitions.max(1))
            .map(|i| {
                let leader = online
                    .get(i as usize % online.len().max(1))
                    .copied()
                    .unwrap_or(0);
                PartitionState::new(leader)
            })
            .collect();

        let mut topic_id = [0u8; 16];
        topic_id[8..].copy_from_slice(&self.topic_id_seq.to_be_bytes());
        self.topic_id_seq += 1;

        self.topics.insert(
            name.to_string(),
            TopicState {
                topic_id,
                partitions: partition_states,
            },
        );
        true
    }

    /// Grow `name` to `partitions` partitions, mirroring a `CreatePartitions`
    /// admin call.
    ///
    /// Kafka can only ever add partitions to a topic, never remove them, so a
    /// request for fewer than the topic already has is a no-op. New partitions
    /// start empty, with leadership spread round-robin over the online brokers
    /// exactly as [`create_topic`](Self::create_topic) assigns it.
    ///
    /// Returns the number of partitions added.
    pub fn add_partitions(&mut self, name: &str, partitions: i32) -> usize {
        let online: Vec<i32> = self
            .brokers
            .iter()
            .filter(|b| b.online)
            .map(|b| b.node_id)
            .collect();
        let Some(topic) = self.topics.get_mut(name) else {
            return 0;
        };
        let existing = topic.partitions.len();
        let target = partitions.max(0) as usize;
        if target <= existing {
            return 0;
        }
        for i in existing..target {
            let leader = online.get(i % online.len().max(1)).copied().unwrap_or(0);
            topic.partitions.push(PartitionState::new(leader));
        }
        target - existing
    }

    /// Mutable access to one partition.
    pub fn partition_mut(&mut self, topic: &str, partition: i32) -> Option<&mut PartitionState> {
        self.topics
            .get_mut(topic)
            .and_then(|t| t.partitions.get_mut(usize::try_from(partition).ok()?))
    }

    /// Read-only access to one partition.
    pub fn partition(&self, topic: &str, partition: i32) -> Option<&PartitionState> {
        self.topics
            .get(topic)
            .and_then(|t| t.partitions.get(usize::try_from(partition).ok()?))
    }

    /// Allocate a fresh producer ID with epoch 0.
    pub fn allocate_producer_id(&mut self) -> (i64, i16) {
        let id = self.next_producer_id;
        self.next_producer_id += 1;
        (id, 0)
    }

    /// Delete a topic and everything stored for it. Returns `false` if it did
    /// not exist.
    ///
    /// Creating a topic of the same name afterwards gives it a new topic ID
    /// and fresh partitions at leader epoch 0, as a real cluster does.
    pub fn delete_topic(&mut self, name: &str) -> bool {
        if self.topics.remove(name).is_none() {
            return false;
        }
        for group in self.share_groups.values_mut() {
            group.partitions.retain(|(topic, _), _| topic != name);
        }
        for session in self.share_sessions.values_mut() {
            session.partitions.retain(|(topic, _)| topic != name);
        }
        true
    }

    /// The `throttle_time_ms` to report for `api_key`.
    pub(crate) fn throttle(&self, api_key: ApiKey) -> i32 {
        self.throttle_time_ms.get(&api_key).copied().unwrap_or(0)
    }

    /// Whether this cluster predates KIP-360: it advertises `InitProducerId`
    /// below v3, so a partition leader rejects a non-zero first sequence from
    /// a producer it has no state for with `UNKNOWN_PRODUCER_ID`.
    pub(crate) fn pre_kip360(&self) -> bool {
        self.api_version_overrides
            .get(&ApiKey::InitProducerId)
            .is_some_and(|&(_, max)| max < 3)
    }

    /// The transactional ID and state owning `producer_id`, if any.
    pub(crate) fn transaction_for_producer(&self, producer_id: i64) -> Option<String> {
        self.transactions
            .iter()
            .find(|(_, t)| t.producer_id == producer_id)
            .map(|(id, _)| id.clone())
    }

    /// Accept an `EndTxn`: move to `PrepareCommit` or `PrepareAbort`, bump the
    /// epoch first when `bump_epoch` (KIP-890 TV2), then write the markers
    /// unless they are held.
    pub(crate) fn end_transaction(
        &mut self,
        transactional_id: &str,
        committed: bool,
        bump_epoch: bool,
    ) {
        let Some(txn) = self.transactions.get_mut(transactional_id) else {
            return;
        };
        if bump_epoch {
            txn.bump_epoch();
        }
        txn.status = if committed {
            TxnStatus::PrepareCommit
        } else {
            TxnStatus::PrepareAbort
        };
        if !self.hold_transaction_markers {
            self.write_transaction_markers(transactional_id);
        }
    }

    /// Fence the open transaction of `transactional_id`: bump the epoch and
    /// abort it, as the coordinator does when the transactional ID is
    /// re-initialised or the transaction times out.
    pub(crate) fn fence_transaction(&mut self, transactional_id: &str) {
        if let Some(txn) = self.transactions.get_mut(transactional_id)
            && txn.status == TxnStatus::Ongoing
        {
            txn.bump_epoch();
            // A fenced epoch is never a retry of the new one.
            txn.last_producer_epoch = -1;
            txn.status = TxnStatus::PrepareAbort;
            if !self.hold_transaction_markers {
                self.write_transaction_markers(transactional_id);
            }
        }
    }

    /// Write the markers of a transaction in `PrepareCommit` or
    /// `PrepareAbort`, and complete it.
    ///
    /// Committing appends a commit marker to every partition in the
    /// transaction, which releases the last stable offset, and applies the
    /// staged offsets. Aborting appends an abort marker, records the aborted
    /// range for `read_committed` fetches, and drops the staged offsets.
    /// Markers carry the transaction's current epoch, which under TV2 is the
    /// bumped one.
    pub(crate) fn write_transaction_markers(&mut self, transactional_id: &str) {
        let Some(txn) = self.transactions.get_mut(transactional_id) else {
            return;
        };
        let committed = match txn.status {
            TxnStatus::PrepareCommit => true,
            TxnStatus::PrepareAbort => false,
            _ => return,
        };
        txn.status = if committed {
            TxnStatus::CompleteCommit
        } else {
            TxnStatus::CompleteAbort
        };
        let partitions = std::mem::take(&mut txn.partitions);
        let staged = std::mem::take(&mut txn.staged_offsets);
        let (producer_id, producer_epoch) = (txn.producer_id, txn.producer_epoch);

        for (topic, partition) in &partitions {
            // Markers go to whatever log holds the name now; a deleted topic
            // has nowhere to write.
            if let Some(p) = self.partition_mut(topic, *partition) {
                p.append_marker(committed, producer_id, producer_epoch);
            }
        }
        if committed {
            for (group_id, offsets) in staged {
                let group = self.groups.entry(group_id).or_default();
                group.offsets.extend(offsets);
            }
        }
    }

    /// Generate the next member ID for a group.
    pub fn next_member_id(&mut self, group_id: &str) -> String {
        let group = self.groups.entry(group_id.to_string()).or_default();
        group.member_seq += 1;
        format!("krafka-fake-member-{}", group.member_seq)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::{Record, RecordBatch};

    fn batch(values: &[&str]) -> Bytes {
        let mut b = RecordBatch::new();
        b.records = values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                Record::new(None, Some(Bytes::copy_from_slice(v.as_bytes())))
                    .with_offset_delta(i as i32)
            })
            .collect();
        b.encode().unwrap()
    }

    #[test]
    fn appending_assigns_consecutive_offsets() {
        let mut p = PartitionState::new(0);
        assert_eq!(p.append(&batch(&["a", "b"])), 0);
        assert_eq!(p.next_offset, 2);
        assert_eq!(p.append(&batch(&["c"])), 2);
        assert_eq!(p.next_offset, 3);
    }

    /// A fetch that lands inside a batch must still receive the whole batch,
    /// matching real broker behaviour.
    #[test]
    fn reading_returns_whole_batches_that_span_the_fetch_offset() {
        let mut p = PartitionState::new(0);
        p.append(&batch(&["a", "b"])); // offsets 0..=1
        p.append(&batch(&["c"])); // offset 2

        assert!(p.read_from(0).len() > p.read_from(2).len());
        assert!(!p.read_from(1).is_empty(), "offset 1 sits inside batch one");
        assert!(p.read_from(3).is_empty(), "nothing at or beyond the end");
    }

    #[test]
    fn coordinators_default_to_the_lowest_online_broker_and_follow_overrides() {
        let mut state = ClusterState::new(3);
        assert_eq!(state.group_coordinator("g"), 0);

        state.brokers[0].online = false;
        assert_eq!(state.group_coordinator("g"), 1);

        state.group_coordinators.insert("g".to_string(), 2);
        assert_eq!(state.group_coordinator("g"), 2);
    }

    #[test]
    fn topic_creation_spreads_leadership_over_online_brokers() {
        let mut state = ClusterState::new(3);
        assert!(state.create_topic("t", 3));
        assert!(!state.create_topic("t", 3), "re-creation is a no-op");

        let leaders: Vec<i32> = state.topics["t"]
            .partitions
            .iter()
            .map(|p| p.leader)
            .collect();
        assert_eq!(leaders, vec![0, 1, 2]);
    }
}
