//! Consumer groups: describe, list, delete.

use std::collections::HashMap;

use tracing::warn;

use crate::BrokerId;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::network::BrokerConnection;
use crate::protocol::{
    ApiKey, CONSUMER_PROTOCOL_TYPE, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse,
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    ListGroupsRequest, ListGroupsResponse, decode_consumer_protocol_assignment,
    decode_consumer_protocol_subscription, versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

/// Consumer group type (classic vs. the KIP-848 consumer protocol).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupType {
    /// Classic consumer group protocol (JoinGroup/SyncGroup/Heartbeat).
    Classic,
    /// Consumer group protocol (KIP-848, ConsumerGroupHeartbeat).
    Consumer,
    /// Another group type, by its name (`share`, `streams`, …).
    Unknown(String),
}

impl GroupType {
    fn parse(name: &str) -> Self {
        match name {
            "classic" => Self::Classic,
            "consumer" => Self::Consumer,
            other => Self::Unknown(other.to_string()),
        }
    }
}

impl std::fmt::Display for GroupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Classic => f.write_str("classic"),
            Self::Consumer => f.write_str("consumer"),
            Self::Unknown(s) => f.write_str(s),
        }
    }
}

/// Description of a consumer group, for classic groups (DescribeGroups) and
/// KIP-848 groups (ConsumerGroupDescribe) alike. Fields only one protocol
/// reports are `Option`s.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    /// Group ID.
    pub group_id: String,
    /// Group type.
    pub group_type: GroupType,
    /// Group state (`Stable`, `Empty`, `Dead`, `PreparingRebalance`, …).
    pub state: String,
    /// Protocol type (classic groups only, e.g. `consumer`).
    pub protocol_type: Option<String>,
    /// Assignor: the classic partition assignment strategy or the KIP-848
    /// server-side assignor.
    pub assignor: Option<String>,
    /// Group epoch (KIP-848 groups only).
    pub group_epoch: Option<i32>,
    /// Assignment epoch (KIP-848 groups only).
    pub assignment_epoch: Option<i32>,
    /// Group members.
    pub members: Vec<ConsumerGroupMember>,
    /// Authorized operations bitfield, when requested.
    pub authorized_operations: Option<i32>,
}

/// A member of a consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (static membership).
    pub instance_id: Option<String>,
    /// Rack the member runs in, when it reports one (KIP-881).
    pub rack_id: Option<String>,
    /// Current member epoch (KIP-848 groups only).
    pub member_epoch: Option<i32>,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Subscribed topic names; `None` on the same terms as
    /// [`assignment`](Self::assignment).
    pub subscribed_topic_names: Option<Vec<String>>,
    /// Subscribed topic regex (KIP-848 groups only).
    pub subscribed_topic_regex: Option<String>,
    /// Current partition assignment. `None` is unknown: a classic group whose
    /// embedded protocol is not `consumer`, or whose blob did not decode.
    /// `Some(vec![])` means the member owns nothing right now.
    pub assignment: Option<Vec<TopicPartitionAssignment>>,
    /// Target assignment (KIP-848 groups only).
    pub target_assignment: Option<Vec<TopicPartitionAssignment>>,
    /// Member type (KIP-848 groups only): -1 unknown, 0 classic, 1 consumer.
    pub member_type: Option<i8>,
}

/// Partitions of one topic in a member's assignment.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TopicPartitionAssignment {
    /// Topic ID; all zeros for classic groups, which carry names only.
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Partition indexes.
    pub partitions: Vec<i32>,
}

/// A listed consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupListing {
    /// Group ID.
    pub group_id: String,
    /// Protocol type (e.g. `consumer`).
    pub protocol_type: String,
    /// Group state, when the broker reports it (ListGroups v4+).
    pub state: Option<String>,
    /// Group type, when the broker reports it (ListGroups v5+).
    pub group_type: Option<GroupType>,
}

admin_options! {
    /// Options for [`AdminClient::describe_consumer_groups`].
    DescribeConsumerGroupsOptions {
        /// Ask for each group's authorized operations.
        include_authorized_operations: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::list_consumer_groups`]. The filters are
    /// applied by the broker; a broker too old for a filter ignores it.
    ListConsumerGroupsOptions {
        /// Only groups in these states (`Empty`, `Stable`, …; ListGroups v4+).
        states: Vec<String>,
        /// Only groups of these types (`classic`, `consumer`, `share`,
        /// `streams`; ListGroups v5+).
        types: Vec<String>,
    }
}

admin_options! {
    /// Options for [`AdminClient::delete_consumer_groups`].
    DeleteConsumerGroupsOptions {}
}

impl AdminClient {
    /// Describe consumer groups at their coordinators.
    ///
    /// A KIP-848 group is described with ConsumerGroupDescribe and a classic
    /// group with DescribeGroups, chosen per group. Returns a result per
    /// group; one group's failure (for example
    /// `GROUP_AUTHORIZATION_FAILED`) does not affect the others.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn describe_consumer_groups<I, S>(
        &self,
        group_ids: I,
        options: DescribeConsumerGroupsOptions,
    ) -> Result<HashMap<String, Result<ConsumerGroupDescription>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut groups: Vec<String> = group_ids
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let call = self.call("DescribeConsumerGroups", Mode::Read, options.timeout)?;
        let include_ops = options.include_authorized_operations;
        Ok(call
            .fan_out(
                groups,
                |g| Target::GroupCoordinator(g.clone()),
                |conn, groups| async move { describe_groups(&conn, groups, include_ops).await },
            )
            .await)
    }

    /// List consumer groups on every broker.
    ///
    /// Each broker knows the groups it coordinates, so the result is per
    /// broker: a broker that failed is an `Err` beside the others' listings,
    /// never silently missing.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client, or when the broker list cannot be
    /// fetched.
    pub async fn list_consumer_groups(
        &self,
        options: ListConsumerGroupsOptions,
    ) -> Result<HashMap<BrokerId, Result<Vec<ConsumerGroupListing>>>> {
        let call = self.call("ListConsumerGroups", Mode::Read, options.timeout)?;
        let brokers = self.broker_ids(&call).await?;
        let options = &options;
        Ok(call
            .fan_out(
                brokers,
                |id| Target::Broker(*id),
                |conn, ids| async move {
                    let request = ListGroupsRequest {
                        states_filter: options.states.clone(),
                        types_filter: options.types.clone(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::ListGroups,
                        versions::LIST_GROUPS_MIN,
                        versions::LIST_GROUPS_MAX,
                    )?;
                    let response: ListGroupsResponse =
                        exchange(&conn, ApiKey::ListGroups, version, &request).await?;
                    let listing = answer(response.error_code, None).map(|()| {
                        response
                            .groups
                            .into_iter()
                            .map(|g| ConsumerGroupListing {
                                group_id: g.group_id,
                                protocol_type: g.protocol_type,
                                state: g.group_state,
                                group_type: g.group_type.as_deref().map(GroupType::parse),
                            })
                            .collect()
                    });
                    Ok(ids.into_iter().map(|id| (id, listing.clone())).collect())
                },
            )
            .await)
    }

    /// Delete consumer groups at their coordinators. Returns a result per
    /// group; a group with members is `Err(Broker { code: NonEmptyGroup })`.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn delete_consumer_groups<I, S>(
        &self,
        group_ids: I,
        options: DeleteConsumerGroupsOptions,
    ) -> Result<HashMap<String, Result<()>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut groups: Vec<String> = group_ids
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        groups.sort_unstable();
        groups.dedup();
        let call = self.call("DeleteGroups", Mode::Write, options.timeout)?;
        Ok(call
            .fan_out(
                groups,
                |g| Target::GroupCoordinator(g.clone()),
                |conn, groups| async move {
                    let request = DeleteGroupsRequest::new(groups);
                    let version = negotiate(
                        &conn,
                        ApiKey::DeleteGroups,
                        versions::DELETE_GROUPS_MIN,
                        versions::DELETE_GROUPS_MAX,
                    )?;
                    let response: DeleteGroupsResponse =
                        exchange(&conn, ApiKey::DeleteGroups, version, &request).await?;
                    Ok(response
                        .results
                        .into_iter()
                        .map(|r| (r.group_id, answer(r.error_code, None)))
                        .collect())
                },
            )
            .await)
    }

    /// The IDs of every known broker, fetching the broker list when empty.
    pub(super) async fn broker_ids(&self, call: &super::driver::Call<'_>) -> Result<Vec<BrokerId>> {
        if self.metadata.brokers().is_empty() {
            tokio::time::timeout(call.remaining(), self.metadata.refresh())
                .await
                .map_err(|_| KrafkaError::timeout("fetching the broker list"))??;
        }
        let mut ids: Vec<BrokerId> = self.metadata.brokers().iter().map(|b| b.id()).collect();
        ids.sort_unstable();
        Ok(ids)
    }
}

/// Describe `groups` at one coordinator: ConsumerGroupDescribe first where
/// the broker has it, DescribeGroups for the classic groups.
async fn describe_groups(
    conn: &BrokerConnection,
    groups: Vec<String>,
    include_ops: bool,
) -> Result<Vec<(String, Result<ConsumerGroupDescription>)>> {
    let mut results: Vec<(String, Result<ConsumerGroupDescription>)> = Vec::new();
    let mut classic: Vec<String> = Vec::new();
    // KIP-848 descriptions with no members: on Kafka 3.7–3.8 a classic group
    // can come back like this, so the classic answer is preferred when it has
    // members.
    let mut memberless: HashMap<String, ConsumerGroupDescription> = HashMap::new();

    match conn.negotiate_api_version(
        ApiKey::ConsumerGroupDescribe,
        versions::CONSUMER_GROUP_DESCRIBE_MAX,
        versions::CONSUMER_GROUP_DESCRIBE_MIN,
    ) {
        Some(version) => {
            let mut request = ConsumerGroupDescribeRequest::new(groups);
            request.include_authorized_operations = include_ops;
            let response: ConsumerGroupDescribeResponse =
                exchange(conn, ApiKey::ConsumerGroupDescribe, version, &request).await?;
            for g in response.groups {
                // A classic group: GROUP_ID_NOT_FOUND on 3.7–3.8 and 4.0+,
                // UNSUPPORTED_VERSION on 3.9.
                if matches!(
                    g.error_code,
                    ErrorCode::GroupIdNotFound | ErrorCode::UnsupportedVersion
                ) {
                    classic.push(g.group_id);
                    continue;
                }
                if let Err(e) = answer(g.error_code, g.error_message) {
                    results.push((g.group_id, Err(e)));
                    continue;
                }
                let assignment = |tps: Vec<crate::protocol::DescribeGroupTopicPartition>| {
                    tps.into_iter()
                        .map(|tp| TopicPartitionAssignment {
                            topic_id: tp.topic_id,
                            topic_name: tp.topic_name,
                            partitions: tp.partitions,
                        })
                        .collect::<Vec<_>>()
                };
                let description = ConsumerGroupDescription {
                    group_id: g.group_id.clone(),
                    group_type: GroupType::Consumer,
                    state: g.group_state,
                    protocol_type: None,
                    assignor: Some(g.assignor_name),
                    group_epoch: Some(g.group_epoch),
                    assignment_epoch: Some(g.assignment_epoch),
                    members: g
                        .members
                        .into_iter()
                        .map(|m| ConsumerGroupMember {
                            member_id: m.member_id,
                            instance_id: m.instance_id,
                            rack_id: m.rack_id,
                            member_epoch: Some(m.member_epoch),
                            client_id: m.client_id,
                            client_host: m.client_host,
                            subscribed_topic_names: Some(m.subscribed_topic_names),
                            subscribed_topic_regex: m.subscribed_topic_regex,
                            assignment: Some(assignment(m.assignment.topic_partitions)),
                            target_assignment: Some(assignment(
                                m.target_assignment.topic_partitions,
                            )),
                            member_type: Some(m.member_type),
                        })
                        .collect(),
                    authorized_operations: include_ops.then_some(g.authorized_operations),
                };
                if description.members.is_empty() {
                    classic.push(description.group_id.clone());
                    memberless.insert(description.group_id.clone(), description);
                } else {
                    results.push((g.group_id, Ok(description)));
                }
            }
        }
        None => classic = groups,
    }

    if !classic.is_empty() {
        let version = negotiate(
            conn,
            ApiKey::DescribeGroups,
            versions::DESCRIBE_GROUPS_MIN,
            versions::DESCRIBE_GROUPS_MAX,
        )?;
        let request = DescribeGroupsRequest {
            groups: classic,
            include_authorized_operations: include_ops,
        };
        let response: DescribeGroupsResponse =
            exchange(conn, ApiKey::DescribeGroups, version, &request).await?;
        for g in response.groups {
            let consumer = memberless.remove(&g.group_id);
            if let Err(e) = answer(g.error_code, g.error_message.clone()) {
                // The KIP-848 answer stands when the classic one failed.
                let result = consumer.ok_or(e);
                results.push((g.group_id, result));
                continue;
            }
            let description = classic_description(g, include_ops);
            let result = match consumer {
                Some(consumer) if description.members.is_empty() => consumer,
                _ => description,
            };
            results.push((result.group_id.clone(), Ok(result)));
        }
    }
    results.extend(
        memberless
            .into_iter()
            .map(|(group, description)| (group, Ok(description))),
    );
    Ok(results)
}

/// A classic group's description, with each member's embedded consumer
/// protocol decoded.
fn classic_description(
    g: crate::protocol::DescribedGroup,
    include_ops: bool,
) -> ConsumerGroupDescription {
    let group_id = g.group_id;
    let protocol_type = g.protocol_type;
    let members = g
        .members
        .into_iter()
        .map(|m| {
            let decoded = classic_member_protocol(&group_id, &m.member_id, &protocol_type, &m);
            ConsumerGroupMember {
                member_id: m.member_id,
                instance_id: m.group_instance_id,
                rack_id: decoded.rack_id,
                member_epoch: None,
                client_id: m.client_id,
                client_host: m.client_host,
                subscribed_topic_names: decoded.subscribed_topic_names,
                subscribed_topic_regex: None,
                assignment: decoded.assignment,
                target_assignment: None,
                member_type: None,
            }
        })
        .collect();
    ConsumerGroupDescription {
        group_id,
        group_type: GroupType::Classic,
        state: g.group_state,
        protocol_type: Some(protocol_type),
        assignor: Some(g.protocol_data),
        group_epoch: None,
        assignment_epoch: None,
        members,
        authorized_operations: include_ops.then_some(g.authorized_operations),
    }
}

/// Decode one classic group member's embedded protocol blobs.
///
/// Both results are `None` when the value is unknown: the group's embedded
/// protocol is not `consumer` (Connect and Streams use their own formats), or
/// the blob is malformed — it was written by another client, so a bad one is
/// logged and skipped rather than failing the group.
fn classic_member_protocol(
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    member: &crate::protocol::DescribeGroupMember,
) -> ClassicMemberProtocol {
    if protocol_type != CONSUMER_PROTOCOL_TYPE {
        return ClassicMemberProtocol::default();
    }

    let subscription = match decode_consumer_protocol_subscription(&member.member_metadata) {
        Ok(subscription) => Some(subscription),
        Err(e) => {
            warn!(
                "failed to decode subscription for member '{member_id}' of classic group '{group_id}': {e}"
            );
            None
        }
    };
    let assignment = match decode_consumer_protocol_assignment(&member.member_assignment) {
        Ok(assignment) => Some(
            assignment
                .assigned_partitions
                .into_iter()
                .map(|tp| TopicPartitionAssignment {
                    topic_id: [0u8; 16],
                    topic_name: tp.topic,
                    partitions: tp.partitions,
                })
                .collect(),
        ),
        Err(e) => {
            warn!(
                "failed to decode assignment for member '{member_id}' of classic group '{group_id}': {e}"
            );
            None
        }
    };

    ClassicMemberProtocol {
        rack_id: subscription.as_ref().and_then(|s| s.rack_id.clone()),
        subscribed_topic_names: subscription.map(|s| s.topics),
        assignment,
    }
}

/// What [`classic_member_protocol`] recovered from a member's blobs.
#[derive(Default)]
struct ClassicMemberProtocol {
    subscribed_topic_names: Option<Vec<String>>,
    rack_id: Option<String>,
    assignment: Option<Vec<TopicPartitionAssignment>>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use bytes::BufMut;

    use super::*;
    use crate::protocol::{
        ConsumerProtocolAssignment, ConsumerProtocolSubscription, ConsumerProtocolTopicPartitions,
    };

    fn classic_member(
        subscription: &ConsumerProtocolSubscription,
        assigned: &[(&str, &[i32])],
    ) -> crate::protocol::DescribeGroupMember {
        let mut metadata = bytes::BytesMut::new();
        crate::protocol::encode_consumer_protocol_subscription(subscription, &mut metadata)
            .expect("subscription must encode");
        let assignment = ConsumerProtocolAssignment::new(
            0,
            assigned
                .iter()
                .map(|(topic, partitions)| ConsumerProtocolTopicPartitions {
                    topic: (*topic).to_string(),
                    partitions: partitions.to_vec(),
                })
                .collect(),
        );
        let mut assignment_bytes = bytes::BytesMut::new();
        crate::protocol::encode_consumer_protocol_assignment(&assignment, &mut assignment_bytes)
            .expect("assignment must encode");
        crate::protocol::DescribeGroupMember {
            member_id: "m1".to_string(),
            group_instance_id: None,
            client_id: "c1".to_string(),
            client_host: "/127.0.0.1".to_string(),
            member_metadata: metadata.freeze(),
            member_assignment: assignment_bytes.freeze(),
        }
    }

    #[test]
    fn a_classic_member_reports_its_subscription_and_its_assignment() {
        let subscription =
            ConsumerProtocolSubscription::new(vec!["orders".to_string(), "payments".to_string()])
                .with_rack_id("us-east-1a");
        let member = classic_member(&subscription, &[("orders", &[0, 3]), ("payments", &[1])]);
        let decoded = classic_member_protocol("g1", "m1", "consumer", &member);
        assert_eq!(
            decoded.subscribed_topic_names.as_deref(),
            Some(["orders".to_string(), "payments".to_string()].as_slice())
        );
        assert_eq!(decoded.rack_id.as_deref(), Some("us-east-1a"));
        let assignment = decoded.assignment.expect("the assignment must decode");
        assert_eq!(assignment.len(), 2);
        assert_eq!(assignment[0].partitions, vec![0, 3]);
        assert_eq!(assignment[0].topic_id, [0u8; 16]);
    }

    #[test]
    fn a_non_consumer_protocol_is_not_decoded() {
        let subscription = ConsumerProtocolSubscription::new(vec!["orders".to_string()]);
        let member = classic_member(&subscription, &[("orders", &[0])]);
        for protocol_type in ["connect", ""] {
            let decoded = classic_member_protocol("g1", "m1", protocol_type, &member);
            assert!(decoded.assignment.is_none());
            assert!(decoded.subscribed_topic_names.is_none());
        }
    }

    #[test]
    fn a_member_mid_rebalance_owns_nothing_rather_than_unknown() {
        let subscription = ConsumerProtocolSubscription::new(vec!["orders".to_string()]);
        let mut member = classic_member(&subscription, &[]);
        member.member_assignment = bytes::Bytes::new();
        let decoded = classic_member_protocol("g1", "m1", "consumer", &member);
        assert!(decoded.assignment.is_some_and(|a| a.is_empty()));
    }

    #[test]
    fn a_malformed_blob_degrades_to_unknown() {
        let subscription = ConsumerProtocolSubscription::new(vec!["orders".to_string()]);
        let mut member = classic_member(&subscription, &[("orders", &[0])]);
        let mut bad = bytes::BytesMut::new();
        bad.put_i16(0);
        bad.put_i32(1);
        bad.put_i16(2);
        bad.put_slice(&[0xff, 0xfe]);
        bad.put_i32(0);
        member.member_assignment = bad.freeze();
        let decoded = classic_member_protocol("g1", "m1", "consumer", &member);
        assert!(decoded.assignment.is_none());
        assert!(decoded.subscribed_topic_names.is_some());
    }

    #[test]
    fn test_group_type_parsing() {
        assert_eq!(GroupType::parse("classic"), GroupType::Classic);
        assert_eq!(GroupType::parse("consumer"), GroupType::Consumer);
        assert_eq!(
            GroupType::parse("share"),
            GroupType::Unknown("share".to_string())
        );
        assert_eq!(GroupType::Unknown("share".into()).to_string(), "share");
    }
}
