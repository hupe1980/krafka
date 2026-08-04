//! AdminClient operation group: share-group offsets (KIP-932 / KIP-1226).
//!
//! krafka has shipped a [`ShareConsumer`](crate::share_consumer::ShareConsumer)
//! since share groups landed, but until these operations existed a share group
//! could be *run* and not *operated*: there was no way to read its
//! share-partition start offsets, reset them, or clean up state for a retired
//! topic. Those are the three things an on-call engineer needs at 3 a.m., and
//! all three live behind API keys 90–92.

use tracing::{info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
    AlterShareGroupOffsetsRequestTopic, AlterShareGroupOffsetsResponse,
    DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse,
    DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
    DescribeShareGroupOffsetsRequestTopic, DescribeShareGroupOffsetsResponse, VersionedDecode,
    VersionedEncode, validate_topic_name, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

/// One share partition's offset state, from
/// [`AdminClient::describe_share_group_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ShareGroupPartitionOffset {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Share-partition start offset — the earliest offset the group may still
    /// deliver.
    pub start_offset: i64,
    /// Leader epoch of the partition.
    pub leader_epoch: i32,
    /// Share-partition lag, or `None` when the broker did not report it
    /// (KIP-1226 added `Lag` in `DescribeShareGroupOffsets` v1; Kafka 4.2 and
    /// earlier answer at v0).
    pub lag: Option<i64>,
    /// Partition-level error, or `None` on success.
    pub error: Option<String>,
}

/// Result of [`AdminClient::describe_share_group_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsResult {
    /// Share group identifier.
    pub group_id: String,
    /// Group-level error, or `None` on success. When set, `partitions` is
    /// typically empty.
    pub error: Option<String>,
    /// Per-partition offset state.
    pub partitions: Vec<ShareGroupPartitionOffset>,
}

/// One partition's outcome from
/// [`AdminClient::alter_share_group_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ShareGroupOffsetAlteration {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Partition-level error, or `None` on success.
    pub error: Option<String>,
}

/// One topic's outcome from
/// [`AdminClient::delete_share_group_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ShareGroupOffsetDeletion {
    /// Topic name.
    pub topic: String,
    /// Topic-level error, or `None` on success.
    pub error: Option<String>,
}

impl AdminClient {
    /// Read a share group's share-partition start offsets (KIP-932).
    ///
    /// Pass `topics = None` to describe **every** topic-partition the group
    /// holds state for. Passing `Some(&[])` describes nothing — the wire
    /// protocol distinguishes a null topics array from an empty one, and so
    /// does this method.
    ///
    /// [`lag`](ShareGroupPartitionOffset::lag) is `Some(_)` only when the
    /// coordinator supports `DescribeShareGroupOffsets` v1 (KIP-1226, Kafka
    /// 4.3+); against an older broker it is `None` rather than a misleading
    /// zero.
    ///
    /// The request goes to the group coordinator, which is where share-group
    /// state lives.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Everything the group knows about.
    /// let all = admin.describe_share_group_offsets("orders-share", None).await?;
    /// for p in &all.partitions {
    ///     println!("{}-{} start={} lag={:?}", p.topic, p.partition, p.start_offset, p.lag);
    /// }
    ///
    /// // Just two partitions of one topic.
    /// let some = admin
    ///     .describe_share_group_offsets("orders-share", Some(&[("orders", &[0, 1][..])]))
    ///     .await?;
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the client is closed, a topic name is invalid, the
    /// coordinator cannot be found, or the broker supports no compatible
    /// `DescribeShareGroupOffsets` version.
    pub async fn describe_share_group_offsets(
        &self,
        group_id: &str,
        topics: Option<&[(&str, &[i32])]>,
    ) -> Result<DescribeShareGroupOffsetsResult> {
        self.check_not_closed()?;
        if let Some(topics) = topics {
            for (name, _) in topics {
                validate_topic_name(name)?;
            }
        }

        let coordinator = self.find_group_coordinator(group_id).await?;

        let request = DescribeShareGroupOffsetsRequest {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: group_id.to_string(),
                topics: topics.map(|topics| {
                    topics
                        .iter()
                        .map(|(name, partitions)| DescribeShareGroupOffsetsRequestTopic {
                            topic_name: (*name).to_string(),
                            partitions: partitions.to_vec(),
                        })
                        .collect()
                }),
            }],
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::DescribeShareGroupOffsets,
                versions::DESCRIBE_SHARE_GROUP_OFFSETS_MAX,
                versions::DESCRIBE_SHARE_GROUP_OFFSETS_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeShareGroupOffsets API version; \
                     share groups require Kafka 4.2 or later",
                )
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::DescribeShareGroupOffsets, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeShareGroupOffsetsResponse::decode_versioned(version, &mut buf)?;

        // One group was requested, so one group is expected back. A response
        // with none is a broker-side contract violation, not an empty result.
        let group = response.groups.into_iter().next().ok_or_else(|| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                format!(
                    "DescribeShareGroupOffsets returned no entry for group '{group_id}'; \
                     exactly one was requested"
                ),
            )
        })?;

        // `lag` is only meaningful from v1; below that the decoder reports the
        // -1 sentinel, which must not be handed to callers as a real lag.
        let lag_supported = version >= 1;

        let partitions = group
            .topics
            .into_iter()
            .flat_map(|topic| {
                let topic_name = topic.topic_name;
                topic
                    .partitions
                    .into_iter()
                    .map(move |p| ShareGroupPartitionOffset {
                        topic: topic_name.clone(),
                        partition: p.partition_index,
                        start_offset: p.start_offset,
                        leader_epoch: p.leader_epoch,
                        lag: if lag_supported && p.lag >= 0 {
                            Some(p.lag)
                        } else {
                            None
                        },
                        error: error_text(p.error_code, p.error_message),
                    })
            })
            .collect();

        Ok(DescribeShareGroupOffsetsResult {
            group_id: group.group_id,
            error: error_text(group.error_code, group.error_message),
            partitions,
        })
    }

    /// Reset a share group's share-partition start offsets (KIP-932).
    ///
    /// **This is a destructive operation.** Moving the start offset backwards
    /// re-delivers records the group already processed; moving it forwards
    /// skips records permanently.
    ///
    /// The group must be **empty**. A group with a live member is answered
    /// with `NON_EMPTY_GROUP`, for the same reason
    /// [`alter_consumer_group_offsets`](Self::alter_consumer_group_offsets)
    /// requires it: rewriting the start offset under an active member would
    /// hand it records it has already acquired.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Rewind two partitions to the beginning of the log.
    /// let results = admin
    ///     .alter_share_group_offsets("orders-share", &[("orders", &[(0, 0), (1, 0)][..])])
    ///     .await?;
    /// for r in &results {
    ///     if let Some(e) = &r.error {
    ///         eprintln!("{}-{} failed: {e}", r.topic, r.partition);
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the client is closed, a topic name is invalid, the
    /// coordinator cannot be found, the broker supports no compatible
    /// `AlterShareGroupOffsets` version, or the request fails at the top level
    /// (including `NON_EMPTY_GROUP`).
    pub async fn alter_share_group_offsets(
        &self,
        group_id: &str,
        topic_offsets: &[(&str, &[(i32, i64)])],
    ) -> Result<Vec<ShareGroupOffsetAlteration>> {
        self.check_not_closed()?;
        for (name, _) in topic_offsets {
            validate_topic_name(name)?;
        }

        let coordinator = self.find_group_coordinator(group_id).await?;

        let request = AlterShareGroupOffsetsRequest {
            group_id: group_id.to_string(),
            topics: topic_offsets
                .iter()
                .map(|(name, partitions)| AlterShareGroupOffsetsRequestTopic {
                    topic_name: (*name).to_string(),
                    partitions: partitions
                        .iter()
                        .map(|&(partition_index, start_offset)| {
                            AlterShareGroupOffsetsRequestPartition {
                                partition_index,
                                start_offset,
                            }
                        })
                        .collect(),
                })
                .collect(),
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::AlterShareGroupOffsets,
                versions::ALTER_SHARE_GROUP_OFFSETS_MAX,
                versions::ALTER_SHARE_GROUP_OFFSETS_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported AlterShareGroupOffsets API version; \
                     share groups require Kafka 4.2 or later",
                )
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::AlterShareGroupOffsets, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = AlterShareGroupOffsetsResponse::decode_versioned(version, &mut buf)?;

        // A top-level failure means nothing was applied. Surfacing it as an
        // error — with the broker's own code, so `is_retriable()` governs the
        // retry — beats returning an empty success list that reads like "no
        // partitions were requested".
        if !response.error_code.is_ok() {
            let msg = response
                .error_message
                .unwrap_or_else(|| format!("{:?}", response.error_code));
            return Err(KrafkaError::broker(response.error_code, msg));
        }

        let results: Vec<_> = response
            .responses
            .into_iter()
            .flat_map(|topic| {
                let topic_name = topic.topic_name;
                topic
                    .partitions
                    .into_iter()
                    .map(move |p| ShareGroupOffsetAlteration {
                        topic: topic_name.clone(),
                        partition: p.partition_index,
                        error: error_text(p.error_code, p.error_message),
                    })
            })
            .collect();

        let failed = results.iter().filter(|r| r.error.is_some()).count();
        if failed > 0 {
            warn!(
                group = group_id,
                failed,
                total = results.len(),
                "AlterShareGroupOffsets completed with partition-level failures"
            );
        } else {
            info!(
                group = group_id,
                partitions = results.len(),
                "AlterShareGroupOffsets completed"
            );
        }
        Ok(results)
    }

    /// Delete a share group's offset state for whole topics (KIP-932).
    ///
    /// **This is a destructive operation** — the deleted state cannot be
    /// recovered, and the group restarts those topics from its configured
    /// reset policy. The group must be **empty**.
    ///
    /// Use it after retiring a topic, so the coordinator stops carrying state
    /// for partitions that no longer exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is closed, a topic name is invalid, the
    /// coordinator cannot be found, the broker supports no compatible
    /// `DeleteShareGroupOffsets` version, or the request fails at the top level
    /// (including `NON_EMPTY_GROUP`).
    pub async fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: &[&str],
    ) -> Result<Vec<ShareGroupOffsetDeletion>> {
        self.check_not_closed()?;
        for name in topics {
            validate_topic_name(name)?;
        }

        let coordinator = self.find_group_coordinator(group_id).await?;

        let request = DeleteShareGroupOffsetsRequest {
            group_id: group_id.to_string(),
            topics: topics.iter().map(|t| (*t).to_string()).collect(),
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::DeleteShareGroupOffsets,
                versions::DELETE_SHARE_GROUP_OFFSETS_MAX,
                versions::DELETE_SHARE_GROUP_OFFSETS_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DeleteShareGroupOffsets API version; \
                     share groups require Kafka 4.2 or later",
                )
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::DeleteShareGroupOffsets, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DeleteShareGroupOffsetsResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            let msg = response
                .error_message
                .unwrap_or_else(|| format!("{:?}", response.error_code));
            return Err(KrafkaError::broker(response.error_code, msg));
        }

        Ok(response
            .responses
            .into_iter()
            .map(|t| ShareGroupOffsetDeletion {
                topic: t.topic_name,
                error: error_text(t.error_code, t.error_message),
            })
            .collect())
    }
}

/// Render a `(code, message)` pair as `Some(text)` on failure, `None` on
/// success — the shape every other admin result in this crate uses.
///
/// Falls back to the debug form of the code when the broker sent no message,
/// so a failure is never reported as an empty string.
fn error_text(code: crate::error::ErrorCode, message: Option<String>) -> Option<String> {
    if code.is_ok() {
        None
    } else {
        Some(message.unwrap_or_else(|| format!("{code:?}")))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn error_text_is_none_on_success() {
        assert_eq!(error_text(ErrorCode::None, None), None);
        assert_eq!(error_text(ErrorCode::None, Some("ignored".into())), None);
    }

    /// A failure without a broker message must still say *something*; an empty
    /// `Some("")` would render as a silent failure in operator tooling.
    #[test]
    fn error_text_falls_back_to_the_code() {
        let text = error_text(ErrorCode::UnknownServerError, None).expect("failure is Some");
        assert!(!text.is_empty());
        assert!(text.contains("UnknownServerError"), "got: {text}");

        assert_eq!(
            error_text(ErrorCode::UnknownServerError, Some("boom".into())),
            Some("boom".to_string())
        );
    }
}
