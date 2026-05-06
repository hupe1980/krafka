//! Async [`Stream`] adapter for the consumer.
//!
//! [`ConsumerStream`] wraps a reference to a [`Consumer`] and implements
//! [`futures_core::Stream`], yielding individual [`ConsumerRecord`]s.
//! This enables interop with the `tokio-stream` combinator ecosystem
//! (`.map()`, `.filter()`, `.take()`, `.buffer_unordered()`, etc.).
//!
//! # Example
//!
//! ```ignore
//! use tokio_stream::StreamExt;
//!
//! let mut stream = consumer.stream();
//! while let Some(result) = stream.next().await {
//!     let record = result?;
//!     println!("{}: {}", record.topic, record.offset);
//! }
//! ```
//!
//! [`Stream`]: futures_core::Stream

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use futures_core::stream::FusedStream;
use tokio_util::sync::ReusableBoxFuture;

use super::Consumer;
use super::record::ConsumerRecord;
use crate::error::{RecvError, Result};

/// Async stream of [`ConsumerRecord`]s from a [`Consumer`].
///
/// Created by [`Consumer::stream()`]. Each call to [`Stream::poll_next()`]
/// drives the underlying [`Consumer::recv()`] method, which handles polling
/// the broker, buffering, auto-commit, rebalancing, and shutdown detection.
///
/// The stream terminates (returns `None`) when the consumer is closed.
/// Errors from the broker or network are propagated as `Some(Err(...))`.
///
/// This type also implements [`FusedStream`], so stream combinators such as
/// `futures::stream::select` can detect termination without an extra poll.
pub struct ConsumerStream<'a> {
    consumer: &'a Consumer,
    /// Reusable boxed future for the in-progress `recv()` call.
    /// Avoids a fresh heap allocation per record by reusing the box
    /// when the future's size and alignment match (which they always
    /// do since `recv()` returns the same concrete type each time).
    fut: ReusableBoxFuture<'a, std::result::Result<ConsumerRecord, RecvError>>,
    /// Set to `true` once `recv()` returns `Err(RecvError::Closed)`.
    /// After that, `poll_next` returns `None` without starting new calls.
    done: bool,
}

impl<'a> ConsumerStream<'a> {
    /// Create a new stream wrapping the given consumer.
    pub(super) fn new(consumer: &'a Consumer) -> Self {
        Self {
            fut: ReusableBoxFuture::new(consumer.recv()),
            consumer,
            done: false,
        }
    }
}

impl Stream for ConsumerStream<'_> {
    type Item = Result<ConsumerRecord>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.done {
            return Poll::Ready(None);
        }

        match this.fut.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => match result {
                Ok(record) => {
                    // Reuse the allocation for the next recv() call.
                    this.fut.set(this.consumer.recv());
                    Poll::Ready(Some(Ok(record)))
                }
                Err(RecvError::Closed) => {
                    // Consumer closed — fuse the stream.
                    this.done = true;
                    Poll::Ready(None)
                }
                Err(RecvError::Error(e)) => {
                    // Reuse the allocation for the next recv() call.
                    this.fut.set(this.consumer.recv());
                    Poll::Ready(Some(Err(e)))
                }
            },
        }
    }
}

impl FusedStream for ConsumerStream<'_> {
    /// Returns `true` after the consumer has been closed.
    ///
    /// Stream combinators (e.g. `select`, `merge`) use this to short-circuit
    /// polling after the consumer shuts down.
    fn is_terminated(&self) -> bool {
        self.done
    }
}
