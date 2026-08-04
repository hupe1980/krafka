//! AdminClient operation group: Streams groups (KIP-1071).

use std::collections::HashMap;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, StreamsGroupDescribeRequest, StreamsGroupDescribeResponse, VersionedDecode,
    VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe Streams groups (KIP-1071, Kafka 4.1+).
    ///
    /// Returns each group's topology, members, and per-member task
    /// assignments and changelog offsets. Like every group API, the request
    /// goes to each group's **coordinator**, so groups spread across brokers
    /// are batched per coordinator and issued once per broker.
    ///
    /// # What this is for
    ///
    /// krafka has no Streams runtime and cannot join a Streams group — that is
    /// `StreamsGroupHeartbeat` (key 88), whose request carries the application
    /// topology. This is the observational half: it is what an operator needs
    /// to answer "is this Streams application healthy?" without running one.
    ///
    /// Two fields are the ones worth alerting on:
    ///
    /// * A member whose [`topology_epoch`](crate::protocol::StreamsGroupMember::topology_epoch)
    ///   is below the group's [`StreamsTopology::epoch`](crate::protocol::StreamsTopology::epoch)
    ///   is still running an older topology.
    /// * A member whose `assignment` differs from its `target_assignment` has
    ///   not finished rebalancing. Persistently so usually means restoration
    ///   is not keeping up — compare `task_offsets` against `task_end_offsets`
    ///   for the lag that explains it.
    ///
    /// # Errors
    ///
    /// Per-group failures are reported in each
    /// [`DescribedStreamsGroup::error_code`](crate::protocol::DescribedStreamsGroup::error_code)
    /// rather than failing the whole call, so one unknown group does not hide
    /// the others. A broker that does not support the API at all — anything
    /// before Kafka 4.1 — fails the call with
    /// [`ProtocolErrorKind::UnknownApiVersion`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// let groups = admin.describe_streams_groups(&["my-streams-app"]).await?;
    /// for group in &groups {
    ///     if !group.error_code.is_ok() {
    ///         eprintln!("{}: {:?}", group.group_id, group.error_code);
    ///         continue;
    ///     }
    ///     let topology_epoch = group.topology.as_ref().map_or(-1, |t| t.epoch);
    ///     for member in &group.members {
    ///         let lagging = member.topology_epoch < topology_epoch;
    ///         let rebalancing = member.assignment != member.target_assignment;
    ///         println!(
    ///             "{} active={} lagging_topology={lagging} rebalancing={rebalancing}",
    ///             member.member_id,
    ///             member.assignment.active_tasks.len(),
    ///         );
    ///     }
    /// }
    /// ```
    pub async fn describe_streams_groups(
        &self,
        group_ids: &[&str],
    ) -> Result<Vec<crate::protocol::DescribedStreamsGroup>> {
        self.check_not_closed()?;

        if group_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Batch by coordinator: one request per broker, not one per group.
        let mut by_coordinator: HashMap<(i32, String), Vec<String>> = HashMap::new();
        for group_id in group_ids {
            let (node_id, addr) = self.find_coordinator_node(group_id, false).await?;
            by_coordinator
                .entry((node_id, addr))
                .or_default()
                .push((*group_id).to_string());
        }

        let mut described = Vec::with_capacity(group_ids.len());

        for ((broker_id, addr), groups) in &by_coordinator {
            let conn = self.pool.get_connection_by_id(*broker_id, addr).await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::StreamsGroupDescribe,
                    versions::STREAMS_GROUP_DESCRIBE_MAX,
                    versions::STREAMS_GROUP_DESCRIBE_MIN,
                )
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "broker does not support StreamsGroupDescribe (KIP-1071); \
                         it requires Kafka 4.1 or newer",
                    )
                })?;

            let request = StreamsGroupDescribeRequest::new(groups.clone());
            let response_bytes = conn
                .send_request(ApiKey::StreamsGroupDescribe, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = StreamsGroupDescribeResponse::decode_versioned(version, &mut buf)?;
            described.extend(response.groups);
        }

        Ok(described)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use crate::protocol::{StreamsGroupDescribeRequest, VersionedEncode, versions};

    #[test]
    fn request_encodes_at_the_negotiated_version() {
        let request = StreamsGroupDescribeRequest::new(vec!["app-a".into(), "app-b".into()]);
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::STREAMS_GROUP_DESCRIBE_MAX, &mut buf)
            .expect("StreamsGroupDescribe must encode");
        assert!(!buf.is_empty());
    }

    /// The authorized-operations flag is opt-in; without it the broker returns
    /// the `i32::MIN` "not requested" sentinel.
    #[test]
    fn authorized_operations_is_off_by_default() {
        let request = StreamsGroupDescribeRequest::new(vec!["app".into()]);
        assert!(!request.include_authorized_operations);
        assert!(
            request
                .with_authorized_operations(true)
                .include_authorized_operations
        );
    }
}
