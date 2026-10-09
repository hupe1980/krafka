//! The producer's side of the send engine: the byte budget, admission, and
//! the handle a caller awaits.
//!
//! `enqueue` is reserve-then-commit. Its only await that holds anything is
//! the wait for `buffer_memory`; once the bytes are reserved, handing the
//! record to the engine is synchronous. A dropped `enqueue` future therefore
//! means "not enqueued", with nothing reserved.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

use tokio::sync::{Semaphore, mpsc, oneshot};
use tracing::warn;

use super::batch::{BufferedRecord, RecordData, Reservation, Waiter};
use super::engine::{self, Append, Command, EngineConfig, Shared};
use super::gate::TxnTicket;
use super::record::{RecordMetadata, RoutedRecord, TopicHandle};
use crate::PartitionId;
use crate::barrier::InFlightBarrier;
use crate::error::{KrafkaError, Result};
use crate::interceptor::RecordContext;
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerRecorder;

/// Reject a record that can never be admitted, before waiting for memory.
pub(crate) fn check_record_admission(
    record_size: usize,
    memory_capacity: usize,
    max_request_size: usize,
) -> Result<()> {
    let semaphore_limit = Semaphore::MAX_PERMITS.min(u32::MAX as usize);
    if record_size > semaphore_limit {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds the largest reservation the producer can make \
             ({semaphore_limit} B)"
        )));
    }
    if record_size > max_request_size {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds max_request_size ({max_request_size} B); the \
             broker would reject it with MESSAGE_TOO_LARGE — raise max_request_size or shrink \
             the record",
        )));
    }
    if record_size > memory_capacity {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds buffer_memory ({memory_capacity} B); raise \
             buffer_memory or shrink the record"
        )));
    }
    Ok(())
}

/// `buffer_memory`, clamped to what a semaphore can hold.
pub(crate) fn effective_memory_capacity(buffer_memory: usize) -> usize {
    if buffer_memory > Semaphore::MAX_PERMITS {
        warn!(
            requested = buffer_memory,
            effective = Semaphore::MAX_PERMITS,
            "buffer_memory exceeds Semaphore::MAX_PERMITS; clamping"
        );
        Semaphore::MAX_PERMITS
    } else {
        buffer_memory
    }
}

/// A record's place in the produce stream, and a future for its acknowledgement.
///
/// Returned by [`Producer::enqueue`](crate::producer::Producer::enqueue) once
/// the record is **queued**. Awaiting it waits for the broker.
///
/// # Ordering
///
/// The ordering guarantee is established when the enqueue returns, not when
/// this handle is awaited. Two records enqueued in a known order are produced
/// in that order, whatever order their handles are polled in — so a caller may
/// collect handles into a `FuturesUnordered`, await them out of order, or drop
/// them, without disturbing the stream.
///
/// # Dropping
///
/// Dropping a handle does **not** cancel the send: the record is already
/// queued and will be produced. It only discards the acknowledgement. Use
/// [`Producer::flush`](crate::producer::Producer::flush) or
/// [`close`](crate::producer::Producer::close) to wait for records whose
/// handles were dropped.
///
/// # Cancel safety
///
/// This type is cancel safe. Awaiting a handle in a `select!` branch that loses
/// leaves it intact, and dropping it discards the outcome, never the record:
/// the record is still delivered.
#[derive(Debug)]
#[must_use = "a dropped DeliveryHandle discards the acknowledgement; the record is still sent"]
pub struct DeliveryHandle {
    response: oneshot::Receiver<Result<RecordMetadata>>,
    partition: PartitionId,
}

impl DeliveryHandle {
    /// The partition this record was routed to.
    ///
    /// Known at enqueue time, so it is available without awaiting.
    #[inline]
    #[must_use]
    pub fn partition(&self) -> PartitionId {
        self.partition
    }
}

impl Future for DeliveryHandle {
    type Output = Result<RecordMetadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.response).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(KrafkaError::closed(
                "the producer stopped before the record had an outcome",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// An enqueue that was refused, carrying back what the caller needs to report
/// it to the interceptor: the record's context and headers.
#[derive(Debug)]
pub(crate) struct EnqueueRejected {
    pub(crate) error: KrafkaError,
    pub(crate) context: RecordContext,
    pub(crate) record: RoutedRecord,
}

impl EnqueueRejected {
    /// Boxed because it is returned in an `Err`: unboxed it would widen every
    /// `Result` on the enqueue path to the size of its failure case.
    fn new(error: KrafkaError, context: RecordContext, record: RoutedRecord) -> Box<Self> {
        Box::new(Self {
            error,
            context,
            record,
        })
    }
}

/// The producer's handle on its send engine.
#[derive(Debug, Clone)]
pub(crate) struct Accumulator {
    commands: mpsc::UnboundedSender<Command>,
    /// FIFO byte budget (`buffer_memory`): reserved at enqueue, released when
    /// the record has its outcome.
    memory: Arc<Semaphore>,
    memory_capacity: usize,
    max_request_size: usize,
    max_block: Duration,
    barrier: Arc<InFlightBarrier>,
    metrics: Arc<ProducerRecorder>,
    shared: Arc<Shared>,
}

impl Accumulator {
    /// Start the engine and return its handle.
    pub(crate) fn spawn(
        config: EngineConfig,
        buffer_memory: usize,
        max_block: Duration,
        metadata: Arc<ClusterMetadata>,
        metrics: Arc<ProducerRecorder>,
        barrier: Arc<InFlightBarrier>,
    ) -> Self {
        let memory_capacity = effective_memory_capacity(buffer_memory);
        let memory = Arc::new(Semaphore::new(memory_capacity));
        let shared = Arc::new(Shared::default());
        let max_request_size = config.max_request_size;
        let (commands, engine) =
            engine::spawn(config, metadata, Arc::clone(&metrics), Arc::clone(&shared));
        // If the engine ever dies, close the budget so no caller waits on it
        // forever.
        let watched = Arc::clone(&memory);
        tokio::spawn(async move {
            if let Err(error) = engine.await {
                tracing::error!("producer send engine failed: {error}");
                watched.close();
            }
        });
        Self {
            commands,
            memory,
            memory_capacity,
            max_request_size,
            max_block,
            barrier,
            metrics,
            shared,
        }
    }

    /// The error every send fails with once the producer cannot continue.
    pub(crate) fn fatal(&self) -> Option<KrafkaError> {
        self.shared.fatal()
    }

    /// Queue a routed record.
    ///
    /// Waits for buffer memory for what is left of `max_block` since
    /// `send_started_at`, then — synchronously — registers the send with the
    /// flush barrier, runs `admit` (the transaction gate), and hands the
    /// record to the engine. On failure the record's interceptor context and
    /// headers come back in [`EnqueueRejected`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue(
        &self,
        topic: TopicHandle,
        record: RoutedRecord,
        record_size: usize,
        partition: PartitionId,
        send_started_at: Instant,
        obligation: &mut super::SendObligation<'_>,
        admit: impl FnOnce() -> Result<Option<TxnTicket>>,
    ) -> std::result::Result<DeliveryHandle, Box<EnqueueRejected>> {
        if let Some(error) = self.fatal() {
            return Err(EnqueueRejected::new(
                error,
                obligation.take_context(),
                record,
            ));
        }
        if let Err(error) =
            check_record_admission(record_size, self.memory_capacity, self.max_request_size)
        {
            return Err(EnqueueRejected::new(
                error,
                obligation.take_context(),
                record,
            ));
        }

        // The only await that holds anything. Dropped here, the permit future
        // cancels cleanly and nothing is reserved.
        let remaining = self.max_block.saturating_sub(send_started_at.elapsed());
        let permits = u32::try_from(record_size).unwrap_or(u32::MAX);
        let permit = match obligation
            .suspend(tokio::time::timeout(
                remaining,
                self.memory.acquire_many(permits),
            ))
            .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err(EnqueueRejected::new(
                    KrafkaError::closed("the producer's send engine stopped"),
                    obligation.take_context(),
                    record,
                ));
            }
            Err(_) => {
                return Err(EnqueueRejected::new(
                    KrafkaError::timeout("max_block elapsed while waiting for buffer memory"),
                    obligation.take_context(),
                    record,
                ));
            }
        };
        permit.forget();
        let reservation = Reservation {
            bytes: record_size,
            memory: Arc::clone(&self.memory),
        };

        // From here on nothing awaits: the record is either queued or refused.
        let operation = match self.barrier.start("producer") {
            Ok(operation) => operation,
            Err(error) => {
                return Err(EnqueueRejected::new(
                    error,
                    obligation.take_context(),
                    record,
                ));
            }
        };
        let txn = match admit() {
            Ok(txn) => txn,
            Err(error) => {
                return Err(EnqueueRejected::new(
                    error,
                    obligation.take_context(),
                    record,
                ));
            }
        };
        let generation = operation.generation();
        let (response, receiver) = oneshot::channel();
        let waiter = Waiter {
            response,
            span: std::mem::replace(&mut obligation.span, tracing::Span::none()),
            context: obligation.take_context(),
            txn,
            _operation: operation,
            _reservation: reservation,
            _buffered: BufferedRecord::new(Arc::clone(&self.metrics)),
        };
        let append = Append {
            topic,
            partition,
            data: RecordData::new(record, now_millis()),
            size: record_size,
            generation,
            waiter,
        };
        if let Err(mpsc::error::SendError(command)) = self.commands.send(Command::Append(append)) {
            let Command::Append(append) = command else {
                unreachable_command();
                return Err(EnqueueRejected::new(
                    KrafkaError::closed("the producer's send engine stopped"),
                    RecordContext::new(),
                    RoutedRecord::empty(),
                ));
            };
            let Append { data, waiter, .. } = append;
            let Waiter { context, txn, .. } = waiter;
            drop(txn);
            return Err(EnqueueRejected::new(
                KrafkaError::closed("the producer's send engine stopped"),
                context,
                RoutedRecord {
                    key: data.key,
                    value: data.value,
                    timestamp: Some(data.timestamp),
                    headers: data.headers,
                },
            ));
        }
        Ok(DeliveryHandle {
            response: receiver,
            partition,
        })
    }

    /// Seal everything buffered so nothing of `generation` waits for linger.
    pub(crate) fn flush(&self, generation: u64) {
        let _ = self.commands.send(Command::Flush { generation });
    }

    /// Fail every batch that is not on the wire with `error`; returns once
    /// the engine has done so.
    pub(crate) async fn fail_unsent(&self, error: KrafkaError) {
        let (done, wait) = oneshot::channel();
        if self
            .commands
            .send(Command::FailUnsent { error, done })
            .is_ok()
        {
            let _ = wait.await;
        }
    }

    /// Hand the engine a transactional identity.
    pub(crate) fn set_identity(&self, producer_id: i64, epoch: i16, tv2: bool) {
        let _ = self.commands.send(Command::SetIdentity {
            producer_id,
            epoch,
            tv2,
        });
    }

    /// Fail everything, on the wire included, and refuse new sends.
    pub(crate) fn terminate(&self, error: KrafkaError) {
        let _ = self.commands.send(Command::Terminate { error });
    }
}

#[cold]
fn unreachable_command() {
    debug_assert!(false, "the channel hands back the message it was given");
}

/// Milliseconds since the Unix epoch, the create time of a record that has
/// none.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn admission_rejects_a_record_larger_than_the_budget() {
        let err = check_record_admission(2048, 1024, usize::MAX).unwrap_err();
        assert!(err.to_string().contains("buffer_memory"), "{err}");
    }

    #[test]
    fn admission_rejects_a_record_larger_than_a_request() {
        let err = check_record_admission(2048, usize::MAX, 1024).unwrap_err();
        assert!(err.to_string().contains("max_request_size"), "{err}");
    }

    #[test]
    fn admission_rejects_a_record_beyond_the_semaphore() {
        let err =
            check_record_admission(u32::MAX as usize + 1, usize::MAX, usize::MAX).unwrap_err();
        assert!(err.to_string().contains("largest reservation"), "{err}");
    }

    #[test]
    fn the_capacity_is_clamped_to_the_semaphore() {
        assert_eq!(
            effective_memory_capacity(usize::MAX),
            Semaphore::MAX_PERMITS
        );
        assert_eq!(effective_memory_capacity(1024), 1024);
    }
}
