//! The cooperative bound of the local runners: a subscription yields to the executor after a run
//! of deliveries that never waited.
//!
//! An embedded app is one embassy task, and embassy has no budget of its own: a stream that is
//! always ready (a handler that retries at once, a full queue) would keep the task inside one
//! poll and starve every other task of the executor.

// The stream and the deliveries it carries are not `Send`, on the app's one task.
#![allow(clippy::future_not_send)]

use core::pin::Pin;
use core::task::{Context, Poll};

use futures::Stream;
use pin_project_lite::pin_project;

use crate::LocalSubscriber;

/// How many consecutive ready items a subscription's stream hands out before it yields once to
/// the executor.
pub(crate) const YIELD_AFTER: u32 = 128;

/// A subscriber whose stream yields to the executor after [`YIELD_AFTER`] ready items in a row.
#[derive(Debug)]
pub(crate) struct Cooperative<Sub>(pub(crate) Sub);

impl<Sub: LocalSubscriber> LocalSubscriber for Cooperative<Sub> {
    type Message = Sub::Message;
    type Error = Sub::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + '_ {
        Bounded::new(self.0.stream(), YIELD_AFTER)
    }
}

pin_project! {
    /// A stream that returns `Pending` once, after waking its task, every `limit` ready items.
    pub(crate) struct Bounded<Inner> {
        #[pin]
        inner: Inner,
        ready: u32,
        limit: u32,
    }
}

impl<Inner> Bounded<Inner> {
    /// Bounds `inner` to `limit` ready items in a row.
    #[must_use]
    pub(crate) const fn new(inner: Inner, limit: u32) -> Self {
        Self {
            inner,
            ready: 0,
            limit,
        }
    }
}

impl<Inner: Stream> Stream for Bounded<Inner> {
    type Item = Inner::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.ready >= *this.limit {
            // Woken before it parks: the task is polled again once the others have had a turn.
            *this.ready = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let next = this.inner.poll_next(cx);
        if next.is_ready() {
            *this.ready += 1;
        } else {
            *this.ready = 0;
        }
        next
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};

    use futures::Stream;
    use futures::stream;

    use super::Bounded;

    /// Polls `stream` `count` times and says which polls were ready.
    fn readiness<S: Stream>(stream: S, count: usize) -> Vec<bool> {
        let mut stream = pin!(stream);
        let mut cx = Context::from_waker(Waker::noop());
        (0..count)
            .map(|_| stream.as_mut().poll_next(&mut cx).is_ready())
            .collect()
    }

    #[test]
    fn a_ready_stream_yields_once_after_exactly_the_limit() {
        let seen = readiness(Bounded::new(stream::iter(0..100), 3), 9);
        assert_eq!(
            seen,
            [true, true, true, false, true, true, true, false, true]
        );
    }

    #[test]
    fn a_pending_from_the_stream_restarts_the_count() {
        // Two ready items, a pending of the stream's own, then ready ones.
        let mut script = [true, true, false]
            .into_iter()
            .chain(core::iter::repeat(true));
        let inner = stream::poll_fn(move |_| match script.next() {
            Some(true) => Poll::Ready(Some(0)),
            _ => Poll::Pending,
        });
        let seen = readiness(Bounded::new(inner, 3), 8);
        assert_eq!(seen, [true, true, false, true, true, true, false, true]);
    }
}
