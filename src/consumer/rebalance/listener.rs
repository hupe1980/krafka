//! The rebalance listener: application callbacks for partition changes.

use std::pin::Pin;

use crate::consumer::TopicPartition;

/// Async callback interface for partition rebalance events.
///
/// Implement this trait to receive notifications when the consumer's
/// partition assignment changes. Every method is awaited to completion inside
/// the `poll()`, `unsubscribe()` or `close()` call that applies the change;
/// there is no timeout. The coordinator's rebalance timeout
/// (`max_poll_interval`) is the only bound: a member that takes longer is
/// removed from the group.
///
/// # Execution contract
///
/// - No consumer lock is held while a callback runs, so a callback may call
///   back into the consumer — [`commit`](crate::consumer::Consumer::commit),
///   [`position`](crate::consumer::Consumer::position),
///   [`seek`](crate::consumer::Consumer::seek) on a newly assigned
///   partition.
/// - Panics inside callbacks propagate to the caller of `poll()`.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::consumer::{ConsumerRebalanceListener, TopicPartition};
///
/// struct MyListener;
///
/// impl ConsumerRebalanceListener for MyListener {
///     async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
///         println!("Assigned: {:?}", partitions);
///     }
///
///     async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
///         // commit offsets directly — fully async, no blocking needed
///         println!("Revoked: {:?}", partitions);
///     }
/// }
/// ```
pub trait ConsumerRebalanceListener: Send + Sync {
    /// Called after partitions have been assigned to this consumer.
    ///
    /// The `partitions` slice contains the **newly added** partitions for this
    /// rebalance round.  The semantics match the Java client:
    ///
    /// | Rebalance protocol | `partitions` contains |
    /// |---|---|
    /// | Initial join (first poll after subscribe) | all assigned partitions |
    /// | Eager rebalance (classic protocol) | all assigned partitions (entire set is new after revoke-all) |
    /// | Cooperative rebalance (KIP-429) | **only newly added** partitions (delta vs previous round) |
    /// | KIP-848 / new consumer protocol | **only newly added** partitions (diff-based) |
    ///
    /// For cooperative and KIP-848 rebalances the slice may be empty if the
    /// rebalance left this consumer's assignment unchanged.  To obtain the
    /// **full** post-rebalance assignment call
    /// [`crate::consumer::Consumer::assignment`] from inside the callback.
    fn on_partitions_assigned<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a;

    /// Called before partitions are taken away from this consumer, while it
    /// still owns them.
    ///
    /// This is the place for a final flush and commit: the partitions are
    /// released only after the returned future completes. Under the eager
    /// protocols (`Range`, `RoundRobin`) it runs for the whole assignment
    /// before the member rejoins; under the cooperative and KIP-848 protocols
    /// only for the partitions moving away. It also runs on `unsubscribe()`
    /// and `close()`, but never for partitions already reported to
    /// [`on_partitions_lost`](Self::on_partitions_lost).
    fn on_partitions_revoked<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a;

    /// Called when partitions were taken away without a clean revocation
    /// (default: no-op): the member exceeded `max_poll_interval`, was fenced,
    /// or is no longer known to the coordinator.
    ///
    /// Fires once per loss; the partitions are no longer assigned when it
    /// runs. Do **not** commit offsets here — another consumer may already
    /// own these partitions and a commit would overwrite their progress.
    fn on_partitions_lost<'a>(
        &'a self,
        _partitions: &'a [TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        async {}
    }
}

/// A no-op rebalance listener that does nothing on rebalance events.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpRebalanceListener;

impl ConsumerRebalanceListener for NoOpRebalanceListener {
    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) {}
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) {}
}

/// Blanket impl so callers can share a listener via `Arc` while still passing
/// `listener.clone()` directly (i.e. `Arc<T>`) to `ConsumerBuilder::rebalance_listener`.
impl<T: ConsumerRebalanceListener + Send + Sync> ConsumerRebalanceListener for std::sync::Arc<T> {
    fn on_partitions_assigned<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        (**self).on_partitions_assigned(partitions)
    }

    fn on_partitions_revoked<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        (**self).on_partitions_revoked(partitions)
    }

    fn on_partitions_lost<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        (**self).on_partitions_lost(partitions)
    }
}

// ── Object-safe erased trait for Arc<dyn …> storage ──────────────────────
//
// `ConsumerRebalanceListener` uses `async fn` (RPITIT) which is not
// dyn-compatible.  `ErasedRebalanceListener` mirrors it with
// `Pin<Box<dyn Future>>` returns so the Consumer can store
// `Arc<dyn ErasedRebalanceListener>` without generic parameters.
// A blanket impl converts any `ConsumerRebalanceListener` transparently.
// This is the same pattern used for `SchemaRegistryClient`.
pub(crate) trait ErasedRebalanceListener: Send + Sync {
    fn on_partitions_assigned_erased<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

    fn on_partitions_revoked_erased<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

    fn on_partitions_lost_erased<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

impl<T: ConsumerRebalanceListener> ErasedRebalanceListener for T {
    fn on_partitions_assigned_erased<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.on_partitions_assigned(partitions))
    }

    fn on_partitions_revoked_erased<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.on_partitions_revoked(partitions))
    }

    fn on_partitions_lost_erased<'a>(
        &'a self,
        partitions: &'a [TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.on_partitions_lost(partitions))
    }
}
