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
