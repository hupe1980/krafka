//! Streams groups (KIP-1071): describe.

use std::collections::HashMap;

use crate::error::Result;
use crate::protocol::{
    ApiKey, DescribedStreamsGroup, StreamsGroupDescribeRequest, StreamsGroupDescribeResponse,
    versions,
};

use super::AdminClient;
use super::driver::{Mode, Target, answer, exchange, negotiate};

admin_options! {
    /// Options for [`AdminClient::describe_streams_groups`].
    DescribeStreamsGroupsOptions {
        /// Ask for each group's authorized operations.
        include_authorized_operations: bool,
    }
}

impl AdminClient {
    /// Describe Streams groups at their coordinators (Kafka 4.1+): topology,
    /// members, and each member's task assignment and offsets.
    ///
    /// krafka runs no Streams application; this is the operator's view. A
    /// member whose `topology_epoch` is below the group's topology epoch runs
    /// an older topology; one whose `assignment` differs from its
    /// `target_assignment` has not finished rebalancing.
    ///
    /// Returns a result per group.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn describe_streams_groups<I, S>(
        &self,
        group_ids: I,
        options: DescribeStreamsGroupsOptions,
    ) -> Result<HashMap<String, Result<DescribedStreamsGroup>>>
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
        let call = self.call("DescribeStreamsGroups", Mode::Read, options.timeout)?;
        let include_ops = options.include_authorized_operations;
        Ok(call
            .fan_out(
                groups,
                |g| Target::GroupCoordinator(g.clone()),
                |conn, groups| async move {
                    let version = negotiate(
                        &conn,
                        ApiKey::StreamsGroupDescribe,
                        versions::STREAMS_GROUP_DESCRIBE_MIN,
                        versions::STREAMS_GROUP_DESCRIBE_MAX,
                    )?;
                    let mut request = StreamsGroupDescribeRequest::new(groups);
                    request.include_authorized_operations = include_ops;
                    let response: StreamsGroupDescribeResponse =
                        exchange(&conn, ApiKey::StreamsGroupDescribe, version, &request).await?;
                    Ok(response
                        .groups
                        .into_iter()
                        .map(|g| {
                            let result =
                                answer(g.error_code, g.error_message.clone()).map(|()| g.clone());
                            (g.group_id, result)
                        })
                        .collect())
                },
            )
            .await)
    }
}
