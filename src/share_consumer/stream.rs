//! Async stream wrapper for share consumer records.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_core::stream::FusedStream;

use super::ShareConsumer;
use crate::consumer::ConsumerRecord;
use crate::error::Result;

type RecvFuture<'a> = Pin<Box<dyn Future<Output = Result<Option<ConsumerRecord>>> + Send + 'a>>;

/// An async `Stream` that yields individual records from a [`ShareConsumer`].
///
/// Created by [`ShareConsumer::stream()`]. Each call to `poll_next` drives
/// the consumer's `recv()` method, which internally calls `poll()` when the
/// internal buffer is empty.
///
/// The stream ends **only** when the consumer is closed. An idle topic makes
/// the stream pend rather than terminate, so a
/// `while let Some(record) = stream.next().await` loop keeps running across
/// quiet periods.
///
/// Dropping the stream mid-poll is safe: acknowledgements drained by the
/// in-flight `poll()` are re-queued rather than lost.
///
/// This type also implements [`FusedStream`], so stream combinators such as
/// `futures::stream::select` can detect termination without an extra poll.
pub struct ShareConsumerStream<'a> {
    consumer: &'a ShareConsumer,
    fut: Option<RecvFuture<'a>>,
    /// Set to `true` once `recv()` returns `Ok(None)`, which happens only when
    /// the consumer has been closed.
    done: bool,
}

impl<'a> ShareConsumerStream<'a> {
    pub(super) fn new(consumer: &'a ShareConsumer) -> Self {
        Self {
            consumer,
            fut: None,
            done: false,
        }
    }
}

impl Stream for ShareConsumerStream<'_> {
    type Item = Result<ConsumerRecord>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.done {
            return Poll::Ready(None);
        }

        let fut = this
            .fut
            .get_or_insert_with(|| Box::pin(this.consumer.recv()));

        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.fut = None;
                match result {
                    Ok(Some(record)) => Poll::Ready(Some(Ok(record))),
                    // `recv()` returns `Ok(None)` only on close, so latching
                    // termination here cannot end the stream on an idle topic.
                    Ok(None) => {
                        this.done = true;
                        Poll::Ready(None)
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
        }
    }
}

impl FusedStream for ShareConsumerStream<'_> {
    /// Returns `true` after the share consumer has been closed.
    ///
    /// Stream combinators (e.g. `select`, `merge`) use this to short-circuit
    /// polling after the consumer shuts down.
    fn is_terminated(&self) -> bool {
        self.done
    }
}
