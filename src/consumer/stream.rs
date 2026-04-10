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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use super::Consumer;
use super::record::ConsumerRecord;
use crate::error::Result;

/// Boxed future type for the in-progress `recv()` call.
type RecvFuture<'a> = Pin<Box<dyn Future<Output = Result<Option<ConsumerRecord>>> + Send + 'a>>;

/// Async stream of [`ConsumerRecord`]s from a [`Consumer`].
///
/// Created by [`Consumer::stream()`]. Each call to [`Stream::poll_next()`]
/// drives the underlying [`Consumer::recv()`] method, which handles polling
/// the broker, buffering, auto-commit, rebalancing, and shutdown detection.
///
/// The stream terminates (returns `None`) when the consumer is closed.
/// Errors from the broker or network are propagated as `Some(Err(...))`.
pub struct ConsumerStream<'a> {
    consumer: &'a Consumer,
    /// In-progress `recv()` future, lazily created on each `poll_next`.
    fut: Option<RecvFuture<'a>>,
}

impl<'a> ConsumerStream<'a> {
    /// Create a new stream wrapping the given consumer.
    pub(super) fn new(consumer: &'a Consumer) -> Self {
        Self {
            consumer,
            fut: None,
        }
    }
}

impl Stream for ConsumerStream<'_> {
    type Item = Result<ConsumerRecord>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Lazily create the recv() future if we don't have one in flight.
        let fut = this
            .fut
            .get_or_insert_with(|| Box::pin(this.consumer.recv()));

        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                // Clear the completed future so we create a fresh one next time.
                this.fut = None;
                match result {
                    Ok(Some(record)) => Poll::Ready(Some(Ok(record))),
                    Ok(None) => Poll::Ready(None), // consumer closed
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
        }
    }
}
