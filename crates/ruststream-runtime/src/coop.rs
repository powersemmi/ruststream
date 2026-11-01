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

use ruststream_core::ReadyHint;

use crate::{BatchStream, Subscriber};

/// How many consecutive ready items a subscription's stream hands out before it yields once to
/// the executor.
pub(crate) const YIELD_AFTER: u32 = 128;

/// A subscriber whose stream yields to the executor after [`YIELD_AFTER`] ready items in a row.
#[derive(Debug)]
pub(crate) struct Cooperative<Sub>(pub(crate) Sub);

impl<Sub: Subscriber> Subscriber for Cooperative<Sub> {
    type Message = Sub::Message;
    type Error = Sub::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + '_ {
        Bounded::new(self.0.stream(), YIELD_AFTER)
    }

    // The bound adds nothing to the answer: a stream that yielded to the executor after a run of
    // deliveries left them waiting, so the probe still says ready.
    fn ready_hint(&self) -> impl ReadyHint + 'static {
        self.0.ready_hint()
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

/// A batch stream bounded the same way: one fill counts as one ready item.
impl<Inner, Slot> BatchStream<Slot> for Bounded<Inner>
where
    Inner: BatchStream<Slot>,
{
    type Error = Inner::Error;

    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Slot,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        let this = self.project();
        if *this.ready >= *this.limit {
            *this.ready = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let next = this.inner.poll_fill(cx, slot);
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
    use core::future::{Future, ready};
    use core::iter;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};

    use futures::Stream;
    use futures::stream;

    use core::convert::Infallible;

    use ruststream_core::{ReadyHint, Settle, Subscriber};

    use super::{Bounded, Cooperative};

    /// Polls `stream` `count` times and says which polls were ready.
    fn readiness<S: Stream>(stream: S, count: usize) -> Vec<bool> {
        let mut stream = pin!(stream);
        let mut cx = Context::from_waker(Waker::noop());
        iter::repeat_with(|| stream.as_mut().poll_next(&mut cx).is_ready())
            .take(count)
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
        let mut script = [true, true, false].into_iter().chain(iter::repeat(true));
        let inner = stream::poll_fn(move |_| match script.next() {
            Some(true) => Poll::Ready(Some(0)),
            _ => Poll::Pending,
        });
        let seen = readiness(Bounded::new(inner, 3), 8);
        assert_eq!(seen, [true, true, false, true, true, true, false, true]);
    }

    /// A probe that says nothing waits.
    struct NothingWaits;

    impl ReadyHint for NothingWaits {
        fn may_be_ready(&self) -> bool {
            false
        }
    }

    /// A subscriber that declares a probe and yields nothing.
    struct Hinted;

    impl Subscriber for Hinted {
        type Message = Waiting;
        type Error = Infallible;

        fn stream(&mut self) -> impl Stream<Item = Result<Waiting, Infallible>> + '_ {
            stream::pending()
        }

        fn ready_hint(&self) -> impl ReadyHint + 'static {
            NothingWaits
        }
    }

    /// The message type of the subscribers above; none is ever yielded.
    struct Waiting;

    impl ruststream_core::IncomingMessage for Waiting {
        fn payload(&self) -> &[u8] {
            &[]
        }

        fn headers(&self) -> &ruststream_core::HeaderMap {
            unreachable!("no delivery is ever yielded")
        }
    }

    impl Settle for Waiting {
        fn ack(self) -> impl Future<Output = Result<(), ruststream_core::AckError>> {
            ready(Ok(()))
        }

        fn nack(
            self,
            _requeue: bool,
        ) -> impl Future<Output = Result<(), ruststream_core::AckError>> {
            ready(Ok(()))
        }
    }

    #[test]
    fn the_cooperative_bound_hands_on_the_probe_of_the_subscriber_it_wraps() {
        let bounded = Cooperative(Hinted);
        assert!(!bounded.ready_hint().may_be_ready());
    }
}
