//! Share consumer (KIP-932).
//!
//! > Requires a Kafka **4.2+** broker — KIP-932 reached general availability
//! > in Kafka 4.2. Against an older broker, or Redpanda, which has no share
//! > groups, `subscribe()` fails with `UnknownApiVersion`, saying the cluster
//! > does not provide share groups.
//!
//! A share group gives queue semantics on a topic: the members share every
//! partition, and the broker hands each record to one member at a time under
//! an *acquisition lock*. The member acknowledges each record it received —
//! accept, release for redelivery, or reject — and a record whose lock
//! expires unacknowledged is redelivered. Delivery is at-least-once, with no
//! order across batches.
//!
//! # Acknowledgement
//!
//! - **Implicit** (default): the records a `poll()`/`recv()` returned are
//!   accepted when the next `poll()`/`recv()` starts, or by `commit()` and
//!   `close()`.
//! - **Explicit**: the application settles every record with
//!   [`ack`](ShareConsumer::ack), [`release`](ShareConsumer::release) or
//!   [`reject`](ShareConsumer::reject) before the next `poll()`, and may
//!   [`renew`](ShareConsumer::renew) a lock while it works.
//!
//! Acknowledgements ride on the next `ShareFetch` to the broker that acquired
//! the records, or go in a `ShareAcknowledge` when no fetch is due.
//! [`commit`](ShareConsumer::commit) sends them at once and returns the
//! outcome per partition; the
//! [acknowledgement-commit callback](ShareConsumerBuilder::acknowledgement_commit_callback)
//! receives the outcome of every one.
//!
//! # Example
//!
//! ```rust,no_run
//! # async fn example(kafka: krafka::Kafka) -> krafka::Result<()> {
//! let consumer = kafka.share_consumer("my-share-group").build().await?;
//! consumer.subscribe(["events"]).await?;
//!
//! while let Some(record) = consumer.recv().await? {
//!     println!("{}@{}", record.partition, record.offset);
//!     // Implicit mode: accepted when the next recv() starts.
//! }
//! # Ok(())
//! # }
//! ```

mod acks;
mod builder;
mod commit;
mod completed_fetch;
mod config;
mod membership;
mod request_manager;
mod session;
mod state;
mod stream;

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "test-broker"))]
mod broker_tests;

pub use builder::ShareConsumerBuilder;
pub use commit::{AcknowledgementCommit, AcknowledgementCommitCallback, CommitResults};
pub use config::{AcknowledgementMode, AcquireMode};
pub use stream::ShareConsumerStream;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tracing::{Instrument, debug, info, warn};

use crate::PartitionId;
use crate::client::CloseOptions;
use crate::consumer::ConsumerRecord;
use crate::error::{KrafkaError, Result};
use acks::AckType;
use state::Inner;

/// How long `ShareConsumer::close` may take without a timeout of its own.
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A Kafka share consumer (KIP-932).
///
/// Cheap to clone: clones share the connections, the membership and the
/// acknowledgement state. `poll()`/`recv()` calls are serialized.
///
/// Records fetched but not yet returned hold acquisition locks on the broker
/// until they are delivered and acknowledged; a fetch asks for at most
/// `max_poll_records` records, and no broker is fetched from again while
/// records it handed out are still buffered.
#[derive(Clone)]
pub struct ShareConsumer(Arc<Inner>);

impl std::fmt::Debug for ShareConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumer")
            .field("group_id", &self.0.config.group_id)
            .field("closed", &self.0.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// Clears the fetch demand a waiting `poll()` raised, however it ends.
struct Demand<'a>(&'a Inner);

impl Drop for Demand<'_> {
    fn drop(&mut self) {
        self.0.state.lock().fetch_wanted = false;
    }
}

impl ShareConsumer {
    /// Subscribe to `topics`, replacing the current subscription, and join
    /// the group. Takes any collection of topic names, like
    /// [`Consumer::subscribe`](crate::consumer::Consumer::subscribe).
    pub async fn subscribe(&self, topics: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
        if self.is_closed() {
            return Err(KrafkaError::closed("share consumer is closed"));
        }
        let mut subscription: Vec<String> =
            topics.into_iter().map(|t| t.as_ref().to_string()).collect();
        subscription.sort_unstable();
        subscription.dedup();
        let topics: Vec<&str> = subscription.iter().map(String::as_str).collect();
        crate::protocol::validate_topic_names(topics.iter().copied())?;
        {
            let mut member = self.0.member.lock();
            member.subscription = subscription.clone();
            member.subscription_acknowledged = false;
        }
        self.0.metadata.refresh_for_topics(Some(&topics)).await?;
        membership::join(&self.0).await?;
        membership::start(&self.0);
        debug!(group = %self.0.config.group_id, topics = topics.len(), "subscribed");
        Ok(())
    }

    /// The current subscription.
    pub fn subscription(&self) -> std::collections::HashSet<String> {
        self.0.member.lock().subscription.iter().cloned().collect()
    }

    /// The partitions assigned to this member, by topic.
    pub fn assignment(&self) -> std::collections::HashMap<String, Vec<PartitionId>> {
        let state = self.0.state.lock();
        let mut out: std::collections::HashMap<String, Vec<PartitionId>> =
            std::collections::HashMap::new();
        for assigned in &state.assigned {
            out.entry(assigned.partition.topic.clone())
                .or_default()
                .push(assigned.partition.partition);
        }
        out
    }

    /// The member id (client-generated, KIP-932).
    pub fn member_id(&self) -> String {
        self.0.member.lock().member_id.clone()
    }

    /// The current member epoch; `0` while (re)joining.
    pub fn member_epoch(&self) -> i32 {
        self.0.member.lock().member_epoch
    }

    /// Return up to `max_poll_records` records, waiting at most `timeout`.
    ///
    /// In implicit mode, starting a poll accepts what the previous one
    /// returned. In explicit mode every record the previous poll returned
    /// must be settled first, or this fails with `IllegalState`.
    ///
    /// A record that fails to deserialize ends the batch: the records before
    /// it are returned, the next call returns its
    /// [`RecordDeserialization`](KrafkaError::RecordDeserialization) error,
    /// and the record is released for redelivery; the records after it follow
    /// in later polls.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Records leave the buffer, and count as
    /// delivered, only at the synchronous point where the call returns them: a
    /// dropped call returns nothing and accepts nothing (its records come with
    /// a later call), and acknowledgements already made stay queued.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let span = self.span(crate::tracing_ext::poll_span, "poll");
        let result = self.poll_records(timeout).instrument(span.clone()).await;
        crate::tracing_ext::record_poll_outcome(&span, result.as_ref().map(Vec::len));
        result
    }

    /// A `poll` or `commit` span, named after the subscription's topic when
    /// it has exactly one.
    fn span(
        &self,
        make: fn(Option<&str>, &str) -> tracing::Span,
        operation: &'static str,
    ) -> tracing::Span {
        let span = make(
            Some(self.0.config.group_id.as_str()),
            self.0.metrics_source.client_id(),
        );
        if !span.is_disabled() {
            let member = self.0.member.lock();
            crate::tracing_ext::record_destination(
                &span,
                operation,
                member.subscription.iter().map(String::as_str),
            );
        }
        span
    }

    async fn poll_records(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let _timer = self.0.metrics.poll_latency.start();
        self.0.metrics.polls.inc();
        let deadline = tokio::time::Instant::now() + timeout;
        let max = self.0.config.max_poll_records.max(1) as usize;
        let result = self.next_records(Some(deadline), max).await;
        match result {
            Ok(Some(records)) => Ok(records),
            Ok(None) => {
                self.0.metrics.empty_polls.inc();
                Ok(Vec::new())
            }
            Err(error) => {
                self.0.metrics.record_error();
                Err(error)
            }
        }
    }

    /// Receive one record, waiting as long as it takes.
    ///
    /// `Ok(None)` means the consumer is closed, and nothing else; an idle
    /// topic makes this wait. [`wakeup()`](Self::wakeup) interrupts it with
    /// an error.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. As for [`poll`](Self::poll): a dropped call
    /// returns nothing and accepts nothing.
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        let span = self.span(crate::tracing_ext::poll_span, "poll");
        let result = match self.next_records(None, 1).instrument(span.clone()).await {
            Ok(records) => Ok(records.and_then(|mut r| r.pop())),
            Err(KrafkaError::Closed { .. }) => Ok(None),
            Err(error) => Err(error),
        };
        crate::tracing_ext::record_poll_outcome(
            &span,
            result.as_ref().map(|record| usize::from(record.is_some())),
        );
        result
    }

    async fn next_records(
        &self,
        deadline: Option<tokio::time::Instant>,
        max: usize,
    ) -> Result<Option<Vec<ConsumerRecord>>> {
        let inner = &self.0;
        let _serial = inner.poll_lock.lock().await;
        self.check_usable()?;
        if inner.config.acknowledgement_mode == AcknowledgementMode::Explicit
            && !inner.state.lock().outstanding.is_empty()
        {
            return Err(KrafkaError::illegal_state(
                "every record the previous poll returned must be acknowledged first",
            ));
        }
        if inner.accept_delivered() {
            inner.wake_nodes();
        }
        if let Some(error) = inner.state.lock().deferred_error.take() {
            return Err(error);
        }

        let mut demand = None;
        loop {
            let ready = inner.records_ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();

            if let Some(records) = inner.take_records(max)? {
                return Ok(Some(records));
            }
            self.check_usable()?;
            if demand.is_none() {
                inner.state.lock().fetch_wanted = true;
                demand = Some(Demand(inner));
                inner.wake_nodes();
            }
            match deadline {
                Some(deadline) => {
                    if tokio::time::timeout_at(deadline, ready).await.is_err() {
                        return Ok(None);
                    }
                }
                None => ready.await,
            }
        }
    }

    fn check_usable(&self) -> Result<()> {
        if self.is_closed() {
            return Err(KrafkaError::closed("share consumer is closed"));
        }
        if self.0.wakeup.swap(false, Ordering::AcqRel) {
            return Err(KrafkaError::Wakeup);
        }
        if let Some(error) = &self.0.state.lock().fatal {
            return Err(error.clone());
        }
        Ok(())
    }

    /// Accept a delivered record (explicit mode).
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. It records the acknowledgement
    /// synchronously; the next `poll`, `recv` or `commit`, or a background
    /// fetch, sends it. Dropping one of those does not drop it: it reaches the
    /// broker exactly once.
    pub fn ack(&self, record: &ConsumerRecord) -> Result<()> {
        self.0.acknowledge(record, AckType::Accept)
    }

    /// Release a delivered record for redelivery (explicit mode).
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. It records the acknowledgement
    /// synchronously; the next `poll`, `recv` or `commit`, or a background
    /// fetch, sends it. Dropping one of those does not drop it: it reaches the
    /// broker exactly once.
    pub fn release(&self, record: &ConsumerRecord) -> Result<()> {
        self.0.acknowledge(record, AckType::Release)
    }

    /// Reject a delivered record: it is archived and not delivered again
    /// (explicit mode).
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. It records the acknowledgement
    /// synchronously; the next `poll`, `recv` or `commit`, or a background
    /// fetch, sends it. Dropping one of those does not drop it: it reaches the
    /// broker exactly once.
    pub fn reject(&self, record: &ConsumerRecord) -> Result<()> {
        self.0.acknowledge(record, AckType::Reject)
    }

    /// Extend a delivered record's acquisition lock (KIP-1222, explicit
    /// mode). The record stays pending: settle it later with `ack`,
    /// `release` or `reject`.
    ///
    /// Fails with an `UnknownApiVersion` protocol error naming KIP-1222 when
    /// the broker that acquired the record is older than Kafka 4.2.
    ///
    /// ```rust,no_run
    /// # use std::time::{Duration, Instant};
    /// # use krafka::share_consumer::ShareConsumer;
    /// # async fn work(c: &ShareConsumer) -> krafka::error::Result<()> {
    /// let lock = c.acquisition_lock_timeout().unwrap_or(Duration::from_secs(30));
    /// for record in c.poll(Duration::from_secs(1)).await? {
    ///     let mut renewed = Instant::now();
    ///     for _step in 0..10 {
    ///         // ... a slice of slow work ...
    ///         if renewed.elapsed() >= lock / 2 {
    ///             c.renew(&record)?;
    ///             renewed = Instant::now();
    ///         }
    ///     }
    ///     c.ack(&record)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. It records the acknowledgement
    /// synchronously; the next `poll`, `recv` or `commit`, or a background
    /// fetch, sends it. Dropping one of those does not drop it: it reaches the
    /// broker exactly once.
    pub fn renew(&self, record: &ConsumerRecord) -> Result<()> {
        self.0.acknowledge(record, AckType::Renew)
    }

    /// Send every pending acknowledgement now and return the outcome for
    /// each partition acknowledgements were sent for (and only those).
    ///
    /// In implicit mode the records the last `poll()` returned are accepted
    /// first. Bounded by `request_timeout`: acknowledgements without an
    /// answer by then are reported as `Timeout`.
    ///
    /// A failed acknowledgement is not retried unless its request never
    /// reached a usable share session (network error, timeout, session
    /// error); `NOT_LEADER_OR_FOLLOWER` and broker refusals such as
    /// `INVALID_RECORD_STATE` (the acquisition lock expired) are reported
    /// here and dropped.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Acknowledgements stay with the consumer
    /// until a broker answers them: a dropped commit stops only the wait for
    /// their outcome, and they are still sent exactly once, by the next commit,
    /// poll or close.
    pub async fn commit(&self) -> Result<CommitResults> {
        let deadline = tokio::time::Instant::now() + self.0.config.request_timeout;
        let span = self.span(crate::tracing_ext::commit_span, "commit");
        let result = commit::commit(&self.0, deadline)
            .instrument(span.clone())
            .await;
        match &result {
            Err(error) => crate::tracing_ext::record_error(&span, error),
            Ok(results) => {
                if let Some(error) = results.values().find_map(|r| r.as_ref().err()) {
                    crate::tracing_ext::record_error(&span, error);
                }
            }
        }
        result
    }

    /// Create an async stream of records; it ends when the consumer closes.
    pub fn stream(&self) -> ShareConsumerStream<'_> {
        ShareConsumerStream::new(self)
    }

    /// Leave the group: commit pending acknowledgements (best effort), send
    /// the leave heartbeat and clear local state. The next `subscribe()`
    /// joins as a new member.
    pub async fn unsubscribe(&self) {
        membership::stop(&self.0);
        if let Err(error) = self.commit().await {
            warn!("committing acknowledgements during unsubscribe failed: {error}");
        }
        if let Err(error) = membership::leave(&self.0).await {
            warn!("leaving the share group failed: {error}");
        }
        {
            let mut member = self.0.member.lock();
            *member = membership::MemberState::new();
        }
        let resolved = self
            .0
            .drop_partition_state(&KrafkaError::closed("unsubscribed"));
        self.0.install_assignment(Vec::new(), Vec::new());
        self.0.report(&resolved);
        self.0.session_generation.fetch_add(1, Ordering::AcqRel);
        self.0.wake_nodes();
        debug!(group = %self.0.config.group_id, "unsubscribed");
    }

    /// Close within 30 s; see [`close_with`](Self::close_with).
    pub async fn close(&self) -> Result<()> {
        self.close_with(CloseOptions::default()).await
    }

    /// Close the consumer within `options`' timeout (default 30 s).
    /// Idempotent; a `recv()` waiting in another task returns `Ok(None)`.
    ///
    /// In implicit mode the records the last `poll()` returned are accepted.
    /// Every broker with a share session receives a final `ShareAcknowledge`
    /// (epoch `-1`) carrying the remaining acknowledgements, after which it
    /// releases whatever this member still holds; then the member leaves the
    /// group. Outcomes reach the acknowledgement-commit callback. Returns the
    /// leave error, if any.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Once polled, the consumer is closed even
    /// if the future is then dropped: `recv` returns `Ok(None)` and calling it
    /// again returns at once, but the final acknowledgements and leaving the
    /// group may not have happened, in which case the broker releases the
    /// member's records when its session expires.
    pub async fn close_with(&self, options: CloseOptions) -> Result<()> {
        let inner = &self.0;
        if inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let timeout = options.timeout.unwrap_or(DEFAULT_CLOSE_TIMEOUT);
        let deadline = tokio::time::Instant::now() + timeout;
        inner.records_ready.notify_waiters();
        membership::stop(inner);
        inner.accept_delivered();

        inner.state.lock().closing = true;
        inner.wake_nodes();
        inner.shut_down.store(true, Ordering::Release);
        let nodes: Vec<_> = inner.nodes.lock().drain().map(|(_, h)| h).collect();
        // Leave a quarter of the budget for leaving the group.
        let sessions_deadline = deadline - timeout / 4;
        for node in nodes {
            let mut task = node.task;
            if tokio::time::timeout_at(sessions_deadline, &mut task)
                .await
                .is_err()
            {
                debug!("a share session close did not finish in time");
                task.abort();
            }
        }

        let left = match tokio::time::timeout_at(deadline, membership::leave(inner)).await {
            Ok(result) => result,
            Err(_) => Err(KrafkaError::timeout("leaving the share group")),
        };
        let resolved = inner.drop_partition_state(&KrafkaError::closed("share consumer closed"));
        inner.report(&resolved);
        inner.install_assignment(Vec::new(), Vec::new());
        inner
            .telemetry
            .close(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await;
        info!(group = %inner.config.group_id, "share consumer closed");
        left
    }

    /// Whether the consumer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire)
    }

    /// Interrupt a waiting [`poll()`](Self::poll)/[`recv()`](Self::recv), or
    /// the next one, with [`KrafkaError::Wakeup`]. The consumer stays usable.
    #[inline]
    pub fn wakeup(&self) {
        self.0.wakeup.store(true, Ordering::Release);
        self.0.records_ready.notify_waiters();
    }

    /// How long an acquisition lock lasts, as last reported by a broker
    /// (KIP-1222); `None` before the first fetch and on brokers older than
    /// Kafka 4.2.
    ///
    /// The lock starts when the broker builds the fetch response, so treat
    /// it as an upper bound on the time left once `poll()` returns.
    #[inline]
    #[must_use]
    pub fn acquisition_lock_timeout(&self) -> Option<Duration> {
        let ms = self.0.acquisition_lock_timeout_ms.load(Ordering::Relaxed);
        (ms > 0).then(|| Duration::from_millis(ms as u64))
    }

    /// This consumer's [`Metrics`](crate::metrics::Metrics): polls, records
    /// and bytes received, commits and errors, and the connection counters
    /// of the pool it shares. An owned snapshot, read without blocking.
    pub fn metrics(&self) -> crate::metrics::Metrics {
        self.0.metrics_source.snapshot()
    }

    /// The id the cluster assigned this consumer for KIP-714 telemetry,
    /// waiting at most `timeout` for it (Java `clientInstanceId`). `None`
    /// when the cluster does not support client telemetry.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when
    /// [`metrics_push`](ShareConsumerBuilder::metrics_push) is off;
    /// [`KrafkaError::Timeout`] when no broker answered in time.
    pub async fn client_instance_id(
        &self,
        timeout: Duration,
    ) -> Result<Option<crate::metrics::ClientInstanceId>> {
        self.0.telemetry.client_instance_id(timeout).await
    }
}
