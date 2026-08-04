//! Mutable cluster state behind the fake broker.
//!
//! Everything a test can manipulate — brokers, topic leadership, group and
//! transaction coordinators, committed offsets, the in-memory logs — lives
//! here behind a single lock. Handlers take that lock for the duration of one
//! request, which is what makes request handling serialisable and the
//! resulting behaviour reproducible.

use std::collections::HashMap;

use bytes::Bytes;

use super::wire;
use crate::protocol::ApiKey;

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
        }
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
        let mut out = Vec::new();
        for batch in &self.log {
            let base = wire::batch_base_offset(batch).unwrap_or(0);
            let count = wire::batch_record_count(batch).unwrap_or(0);
            if base + count > fetch_offset {
                out.extend_from_slice(batch);
            }
        }
        Bytes::from(out)
    }
}

/// A topic and its partitions.
#[derive(Debug, Clone)]
pub struct TopicState {
    /// Topic UUID. Only surfaced on API versions that carry it.
    pub topic_id: [u8; 16],
    /// Partitions, indexed by partition number.
    pub partitions: Vec<PartitionState>,
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
/// committed offsets: a start offset (SPSO), a cursor of records handed out
/// but not yet resolved, and a per-record delivery count. `ACCEPT` and
/// `REJECT` advance the start offset; `RELEASE` makes the record available
/// again with a higher delivery count.
///
/// Records acquired but never resolved are returned to the pool when the
/// member holding them leaves, which is what makes the start offset
/// load-bearing: it is the point a new member starts from, and only an
/// `ACCEPT` or `REJECT` moves it.
///
/// What is **not** modelled: acquisition-lock *expiry* (an in-flight record
/// comes back when its holder leaves, never on a timer), the archived state,
/// `group.share.delivery.attempts` limits, and `RENEW` (KIP-1222) — which is
/// accepted and has no effect, because with no lock timer there is nothing to
/// extend. Tests must not be read as validating any of those.
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
    /// Offset of the next record to hand out.
    ///
    /// Always at or above [`Self::start_offset`]. The gap between them is the
    /// set of records that are in flight — acquired by some member and not yet
    /// resolved.
    pub next_acquire: i64,
    /// How many times each offset has been delivered, keyed by offset. Only
    /// offsets delivered more than once are present.
    pub delivery_counts: HashMap<i64, i16>,
}

impl ShareGroupState {
    /// Return every in-flight record to the pool.
    ///
    /// A record between the start offset and the acquisition cursor has been
    /// handed to a member that has not resolved it. On a real broker it comes
    /// back when the acquisition lock expires; here the trigger is the holder
    /// leaving the group, which is the same event a client can actually cause.
    ///
    /// This is what makes an unacknowledged record distinguishable from an
    /// accepted one. Without it the cursor would only ever move forward, and
    /// "the client accepted the batch" and "the client dropped it on the
    /// floor" would produce identical broker state.
    pub(crate) fn release_in_flight(&mut self) {
        for partition in self.partitions.values_mut() {
            partition.next_acquire = partition.start_offset;
        }
    }
}

impl SharePartitionState {
    /// Record that `[first, last]` was handed to a member, returning the
    /// delivery count each of those offsets is now on.
    ///
    /// A real broker tracks a delivery count per record; this returns the
    /// maximum over the range, which is what goes in the single
    /// `delivery_count` field of an `AcquiredRecords` entry.
    pub(crate) fn acquire(&mut self, first: i64, last: i64) -> i16 {
        let mut max = 1;
        for offset in first..=last {
            let count = self.delivery_counts.entry(offset).or_insert(0);
            *count = count.saturating_add(1);
            max = max.max(*count);
        }
        self.next_acquire = self.next_acquire.max(last + 1);
        max
    }

    /// Apply one acknowledgement to `[first, last]`.
    ///
    /// `acknowledge_type` is the KIP-932 wire value: 1 = ACCEPT, 2 = RELEASE,
    /// 3 = REJECT, 4 = RENEW (KIP-1222). `0` is a gap marker and resolves
    /// nothing.
    pub(crate) fn acknowledge(&mut self, first: i64, last: i64, acknowledge_type: i8) {
        match acknowledge_type {
            // ACCEPT and REJECT both retire the record: neither is ever
            // delivered again. They differ only in what a real broker reports,
            // which nothing here observes.
            1 | 3 => {
                self.start_offset = self.start_offset.max(last + 1);
                for offset in first..=last {
                    self.delivery_counts.remove(&offset);
                }
            }
            // RELEASE returns the record to the pool. The cursor rewinds so it
            // is handed out again; the delivery count already recorded by
            // `acquire` is what makes the redelivery observable.
            2 => {
                self.next_acquire = self.next_acquire.min(first.max(self.start_offset));
            }
            // RENEW extends an acquisition lock. There is no lock timer here,
            // so there is nothing to extend — but it must not be treated as an
            // error either, or a client exercising KIP-1222 would see failures
            // a real broker would not produce.
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
    /// Producer epochs, keyed by producer ID.
    pub producer_epochs: HashMap<i64, i16>,
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
    /// Counter behind generated topic UUIDs.
    topic_id_seq: u64,
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
            group_coordinators: HashMap::new(),
            txn_coordinators: HashMap::new(),
            auto_create_topics: true,
            default_partitions: 1,
            next_producer_id: 1000,
            producer_epochs: HashMap::new(),
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
        self.producer_epochs.insert(id, 0);
        (id, 0)
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
