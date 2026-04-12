//! Async stream wrapper for share consumer records.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use super::ShareConsumer;
use crate::consumer::ConsumerRecord;
use crate::error::Result;

type RecvFuture<'a> = Pin<Box<dyn Future<Output = Result<Option<ConsumerRecord>>> + Send + 'a>>;

/// An async `Stream` that yields individual records from a [`ShareConsumer`].
///
/// Created by [`ShareConsumer::stream()`]. Each call to `poll_next` drives
/// the consumer's `recv()` method, which internally calls `poll()` when the
/// internal buffer is empty.
pub struct ShareConsumerStream<'a> {
    consumer: &'a ShareConsumer,
    fut: Option<RecvFuture<'a>>,
}

impl<'a> ShareConsumerStream<'a> {
    pub(super) fn new(consumer: &'a ShareConsumer) -> Self {
        Self {
            consumer,
            fut: None,
        }
    }
}

impl Stream for ShareConsumerStream<'_> {
    type Item = Result<ConsumerRecord>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let fut = this
            .fut
            .get_or_insert_with(|| Box::pin(this.consumer.recv()));

        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.fut = None;
                match result {
                    Ok(Some(record)) => Poll::Ready(Some(Ok(record))),
                    Ok(None) => Poll::Ready(None),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
        }
    }
}
