//! Group maintenance on the poll path: losses, auto-commit, joins, and one
//! function that applies every assignment change.
//!
//! Rebalance listener callbacks are awaited here with no consumer lock held
//! and no timeout.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use tracing::{Instrument, debug, warn};

use crate::PartitionId;
use crate::consumer::group::{GroupCoordinator, MemberAssignment, is_coordinator_retriable};
use crate::consumer::state::PartitionKey;
use crate::consumer::{Consumer, TopicPartition};
use crate::error::{ErrorCode, KrafkaError, Result};

/// How an assignment change reaches this member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    /// Classic eager: the whole assignment was revoked before the join.
    Eager,
    /// Classic cooperative (KIP-429): partitions moving away are revoked
    /// after the join, then the member rejoins.
    Cooperative,
    /// KIP-848: the coordinator reconciles; the member acknowledges.
    Kip848,
}

fn to_topic_partitions(keys: &[PartitionKey]) -> Vec<TopicPartition> {
    keys.iter()
        .map(|(topic, partition)| TopicPartition::new(topic.clone(), *partition))
        .collect()
}

impl Consumer {
    /// Bring the group membership up to date before records are handed out
    /// or fetched.
    ///
    /// Returns `true` while a join round is still running after `budget`: there
    /// is nothing to fetch mid-rebalance, so the poll returns empty and the
    /// round's outcome is applied by a later poll.
    pub(in crate::consumer) async fn maintain_group(&self, budget: Duration) -> Result<bool> {
        let Some(coordinator) = self.group_coordinator.clone() else {
            self.resolve_standalone_subscription(false).await?;
            return Ok(false);
        };

        if coordinator.poll_interval_exceeded() {
            warn!(
                group = coordinator.group_id(),
                max_poll_interval = ?coordinator.max_poll_interval(),
                "The application did not poll within max_poll_interval; the member left \
                 the group and its partitions are lost. Rejoining."
            );
            self.lose_partitions(&coordinator).await;
            coordinator.rejoin_after_expiry();
        }
        if coordinator.take_lost() {
            warn!(
                group = coordinator.group_id(),
                "The coordinator fenced this member or no longer knows it; its partitions \
                 are lost. Rejoining."
            );
            self.lose_partitions(&coordinator).await;
        }
        if let Some(error) = coordinator.take_fatal_error() {
            return Err(error);
        }
        coordinator.note_poll();
        self.maybe_auto_commit().await;

        if self.state.lock().subscription.is_empty() {
            return Ok(false);
        }

        if coordinator.is_consumer_protocol() {
            if coordinator.needs_rejoin() {
                coordinator.join_consumer_group().await?;
            }
            if coordinator.take_assignment_changed() {
                self.apply_assignment(&coordinator, coordinator.assignment(), Protocol::Kip848)
                    .await?;
                coordinator.acknowledge_assignment();
            }
            return Ok(false);
        }

        if let Some(outcome) = coordinator.take_pending_rebalance() {
            self.apply_join(&coordinator, outcome).await?;
        }
        if !coordinator.rejoin_in_flight() && coordinator.needs_rejoin() {
            if !coordinator.is_cooperative() {
                // Eager: commit and revoke everything before rejoining, so
                // the next owner starts only after this member stopped.
                self.revoke_all_in_group(&coordinator).await;
            }
            coordinator.spawn_rejoin(true);
        }
        if coordinator.rejoin_in_flight() {
            coordinator.await_rejoin(budget).await;
            match coordinator.take_pending_rebalance() {
                Some(outcome) => self.apply_join(&coordinator, outcome).await?,
                None => return Ok(true),
            }
        }
        Ok(false)
    }

    /// Apply the outcome of a classic join round.
    async fn apply_join(
        &self,
        coordinator: &Arc<GroupCoordinator>,
        outcome: Result<MemberAssignment>,
    ) -> Result<()> {
        match outcome {
            Ok(assignment) => {
                let protocol = if coordinator.is_cooperative() {
                    Protocol::Cooperative
                } else {
                    Protocol::Eager
                };
                self.apply_assignment(coordinator, assignment, protocol)
                    .await
            }
            Err(error)
                if error.is_retriable()
                    || is_coordinator_retriable(&error)
                    || matches!(
                        error,
                        KrafkaError::Broker {
                            code: ErrorCode::UnknownMemberId
                                | ErrorCode::IllegalGeneration
                                | ErrorCode::RebalanceInProgress
                                | ErrorCode::MemberIdRequired,
                            ..
                        }
                    ) =>
            {
                warn!(
                    group = coordinator.group_id(),
                    "Join failed, retrying: {error}"
                );
                coordinator.request_rejoin();
                Ok(())
            }
            Err(error) => {
                coordinator.request_rejoin();
                Err(error)
            }
        }
    }

    /// Apply an assignment change: compute what is revoked and what is new,
    /// commit before revoking, revoke, then assign — in that order for every
    /// protocol.
    async fn apply_assignment(
        &self,
        coordinator: &GroupCoordinator,
        assignment: MemberAssignment,
        protocol: Protocol,
    ) -> Result<()> {
        let span = crate::tracing_ext::rebalance_span(
            coordinator.group_id(),
            match protocol {
                Protocol::Eager => "eager",
                Protocol::Cooperative => "cooperative",
                Protocol::Kip848 => "consumer",
            },
            coordinator.group_metadata().map(|m| m.generation_id()),
        );
        self.apply_assignment_traced(coordinator, assignment, protocol, &span)
            .instrument(span.clone())
            .await
    }

    async fn apply_assignment_traced(
        &self,
        coordinator: &GroupCoordinator,
        assignment: MemberAssignment,
        protocol: Protocol,
        span: &tracing::Span,
    ) -> Result<()> {
        let new: HashSet<PartitionKey> = assignment
            .all_partitions()
            .map(|(t, p)| (t.to_string(), p))
            .collect();
        let (revoked, added) = {
            let state = self.state.lock();
            let old: HashSet<PartitionKey> = state.assigned_keys().into_iter().collect();
            let mut revoked: Vec<PartitionKey> = old.difference(&new).cloned().collect();
            let mut added: Vec<PartitionKey> = new.difference(&old).cloned().collect();
            revoked.sort();
            added.sort();
            (revoked, added)
        };

        if !revoked.is_empty() {
            if self.config.enable_auto_commit
                && let Err(e) = self.committer().commit_positions().await
            {
                warn!("Commit before revocation failed: {e}");
            }
            self.rebalance_listener
                .on_partitions_revoked_erased(&to_topic_partitions(&revoked))
                .await;
            self.forget_partitions(&revoked);
        }

        let current = {
            let mut state = self.state.lock();
            state.add_partitions(added.iter().cloned());
            state.assignment()
        };
        if !coordinator.is_consumer_protocol() {
            coordinator.set_owned(&current);
        }
        self.metrics.rebalances.inc();
        self.update_gauges();
        span.record("krafka.rebalance.assigned", added.len() as u64);
        span.record("krafka.rebalance.revoked", revoked.len() as u64);
        span.record("krafka.rebalance.partitions", new.len() as u64);

        self.rebalance_listener
            .on_partitions_assigned_erased(&to_topic_partitions(&added))
            .await;

        if protocol == Protocol::Cooperative && !revoked.is_empty() {
            // The partitions given up go to their new owner in a follow-up
            // round, which the next poll starts.
            coordinator.request_rejoin();
        }
        debug!(
            ?protocol,
            revoked = revoked.len(),
            added = added.len(),
            "Applied assignment"
        );
        Ok(())
    }

    /// Give up every assigned partition cleanly: commit, then
    /// `on_partitions_revoked`, then drop them. Used before an eager rejoin,
    /// and by `unsubscribe()` and `close()`.
    pub(in crate::consumer) async fn revoke_all_in_group(&self, coordinator: &GroupCoordinator) {
        let assigned = self.state.lock().assigned_keys();
        if assigned.is_empty() {
            return;
        }
        if self.config.enable_auto_commit
            && let Err(e) = self.committer().commit_positions().await
        {
            warn!("Commit before revocation failed: {e}");
        }
        self.rebalance_listener
            .on_partitions_revoked_erased(&to_topic_partitions(&assigned))
            .await;
        self.state.lock().remove_partitions(&assigned);
        coordinator.set_owned(&HashMap::new());
        self.update_gauges();
    }

    /// Drop every assigned partition without committing and report them to
    /// `on_partitions_lost`, once. They are gone before the callback runs, so
    /// nothing — not a commit from the callback, not `close()` — treats them
    /// as owned afterwards.
    async fn lose_partitions(&self, coordinator: &GroupCoordinator) {
        let lost = {
            let mut state = self.state.lock();
            let keys = state.assigned_keys();
            state.clear_assignment();
            keys
        };
        coordinator.set_owned(&HashMap::new());
        self.update_gauges();
        if !lost.is_empty() {
            self.rebalance_listener
                .on_partitions_lost_erased(&to_topic_partitions(&lost))
                .await;
        }
    }

    /// Commit when `auto_commit_interval` has elapsed since the last attempt.
    async fn maybe_auto_commit(&self) {
        if !self.config.enable_auto_commit || self.group_coordinator.is_none() {
            return;
        }
        let due = {
            let mut state = self.state.lock();
            if state.last_auto_commit.elapsed() >= self.config.auto_commit_interval {
                state.last_auto_commit = Instant::now();
                true
            } else {
                false
            }
        };
        if due && let Err(e) = self.committer().commit_positions().await {
            warn!("Auto-commit failed: {e}");
        }
    }

    /// Re-derive a group-less `subscribe()`'s partitions from cluster
    /// metadata: once per `metadata_max_age` while every topic resolves, on
    /// every poll while one does not (a topic waiting to be created should not
    /// wait out a five-minute timer — the refresh goes through the metadata
    /// writer, whose backoff governs the request rate). A failed refresh keeps
    /// the current assignment.
    pub(in crate::consumer) async fn resolve_standalone_subscription(
        &self,
        force: bool,
    ) -> Result<()> {
        let (topics, due) = {
            let state = self.state.lock();
            if state.standalone_topics.is_empty() {
                return Ok(());
            }
            let topics: Vec<String> = state.standalone_topics.iter().cloned().collect();
            let assigned_topics: HashSet<String> =
                state.assigned_keys().into_iter().map(|(t, _)| t).collect();
            let due = force
                || state.standalone_resolved.is_none_or(|at| {
                    at.elapsed() >= self.metadata.max_age()
                        || topics.iter().any(|t| !assigned_topics.contains(t))
                });
            (topics, due)
        };
        if !due {
            return Ok(());
        }

        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        if let Err(e) = self.metadata.refresh_for_topics(Some(&names)).await {
            debug!(error = %e, "metadata refresh for a group-less subscription failed");
            return Ok(());
        }

        let mut desired: HashSet<PartitionKey> = HashSet::new();
        for topic in &topics {
            if let Some(info) = self.metadata.topic_arc(topic) {
                desired.extend(info.partitions_iter().map(|p| (topic.clone(), p.partition)));
            }
        }
        let mut state = self.state.lock();
        let topic_set: HashSet<&String> = topics.iter().collect();
        let current: Vec<PartitionKey> = state
            .assigned_keys()
            .into_iter()
            .filter(|(t, _)| topic_set.contains(t))
            .collect();
        let gone: Vec<PartitionKey> = current
            .iter()
            .filter(|k| !desired.contains(*k))
            .cloned()
            .collect();
        let added: Vec<PartitionKey> = desired
            .into_iter()
            .filter(|k| !current.contains(k))
            .collect();
        if !gone.is_empty() {
            debug!(
                partitions = gone.len(),
                "group-less subscription lost partitions"
            );
            state.remove_partitions(&gone);
            let live: Vec<crate::BrokerId> =
                self.metadata.brokers().iter().map(|b| b.id()).collect();
            state.fetch_sessions.retain_brokers(&live);
        }
        if !added.is_empty() {
            debug!(
                partitions = added.len(),
                "group-less subscription gained partitions"
            );
            state.add_partitions(added);
        }
        state.standalone_resolved = Some(Instant::now());
        drop(state);
        self.update_gauges();
        Ok(())
    }

    /// Drop revoked partitions, and the fetch sessions of brokers that left
    /// the cluster, which can never become active again.
    fn forget_partitions(&self, keys: &[PartitionKey]) {
        let live: Vec<crate::BrokerId> = self.metadata.brokers().iter().map(|b| b.id()).collect();
        let mut state = self.state.lock();
        state.remove_partitions(keys);
        state.fetch_sessions.retain_brokers(&live);
    }

    /// Partitions grouped by topic.
    pub(in crate::consumer) fn by_topic(
        keys: &[PartitionKey],
    ) -> HashMap<String, Vec<PartitionId>> {
        let mut map: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in keys {
            map.entry(topic.clone()).or_default().push(*partition);
        }
        map
    }
}
