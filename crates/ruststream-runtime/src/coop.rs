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
pub(crate) struct Cooperative<Sub>(Sub);

impl<Sub> Cooperative<Sub> {
    /// Bounds `subscriber`'s stream to [`YIELD_AFTER`] ready items in a row.
    #[must_use]
    pub(crate) const fn new(subscriber: Sub) -> Self {
        Self(subscriber)
    }
}

impl<Sub: Subscriber> Subscriber for Cooperative<Sub> {
    type Message = Sub::Message;
    type Error = Sub::Error;

    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + '_ {
        let probe = self.0.ready_hint();
        Bounded::new(self.0.stream(), YIELD_AFTER, probe)
    }

    // The bound adds nothing to the answer: it yields only while the probe says more may be
    // waiting, so a yielded stream still reads as ready.
    fn ready_hint(&self) -> impl ReadyHint + 'static + use<Sub> {
        self.0.ready_hint()
    }
}

pin_project! {
    /// A stream that returns `Pending` once, after waking its task, every `limit` ready items,
    /// when its probe says more may be waiting.
    ///
    /// A yield returns `Pending` without polling the stream, so the stream registers no waker. An
    /// app skips a lane whose probe says nothing is ready, so a yield past the last delivery would
    /// leave the lane parked with no waker for the next one. The bound yields only while the probe
    /// says more may be waiting; otherwise it polls the stream, which parks with its waker.
    pub(crate) struct Bounded<Inner, Probe> {
        #[pin]
        inner: Inner,
        ready: u32,
        limit: u32,
        probe: Probe,
    }
}

impl<Inner, Probe> Bounded<Inner, Probe> {
    /// Bounds `inner` to `limit` ready items in a row, yielding while `probe` says more may be
    /// waiting.
    #[must_use]
    pub(crate) const fn new(inner: Inner, limit: u32, probe: Probe) -> Self {
        Self {
            inner,
            ready: 0,
            limit,
            probe,
        }
    }
}

impl<Inner: Stream, Probe: ReadyHint> Stream for Bounded<Inner, Probe> {
    type Item = Inner::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.ready >= *this.limit {
            *this.ready = 0;
            if this.probe.may_be_ready() {
                // Woken before it parks: the task is polled again once the others have had a turn.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
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
impl<Inner, Probe, Slot> BatchStream<Slot> for Bounded<Inner, Probe>
where
    Inner: BatchStream<Slot>,
    Probe: ReadyHint,
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
            if this.probe.may_be_ready() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
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
#[cfg(ruststream_alloc)]
mod tests {
    use alloc::sync::Arc;
    use alloc::task::Wake;
    use alloc::vec::Vec;
    use core::future::{Pending, pending, poll_fn, ready};
    use core::iter;
    use core::pin::{Pin, pin};
    use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use core::task::{Context, Poll, Waker};
    use std::sync::Mutex;

    use futures::Stream;
    use futures::stream;

    use core::convert::Infallible;

    use ruststream_core::{ReadyHint, Settle, Subscriber};

    use super::{Bounded, Cooperative, YIELD_AFTER};
    use crate::ShutdownWait;
    use crate::dispatch::idle;

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
        let seen = readiness(Bounded::new(stream::iter(0..100), 3, ()), 9);
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
        let seen = readiness(Bounded::new(inner, 3, ()), 8);
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

        fn ready_hint(&self) -> impl ReadyHint + 'static + use<> {
            NothingWaits
        }
    }

    /// The message type of the subscribers above; none is ever yielded.
    struct Waiting;

    impl ruststream_core::IncomingMessage for Waiting {
        fn payload(&self) -> &[u8] {
            &[]
        }

        fn headers(&self) -> &ruststream_core::Headers {
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
        let bounded = Cooperative::new(Hinted);
        assert!(!bounded.ready_hint().may_be_ready());
    }

    /// A subscription's queue: the stream registers its waker where it finds the queue empty,
    /// and a push wakes it.
    #[derive(Default)]
    struct Line {
        queued: usize,
        waker: Option<Waker>,
    }

    impl Line {
        fn push(&mut self) {
            self.queued += 1;
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }

    struct LineStream(Arc<Mutex<Line>>);

    impl Stream for LineStream {
        type Item = Result<Waiting, Infallible>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut line = self.0.lock().unwrap();
            let next = if line.queued == 0 {
                line.waker = Some(cx.waker().clone());
                Poll::Pending
            } else {
                line.queued -= 1;
                Poll::Ready(Some(Ok(Waiting)))
            };
            drop(line);
            next
        }
    }

    struct LineWaiting(Arc<Mutex<Line>>);

    impl ReadyHint for LineWaiting {
        fn may_be_ready(&self) -> bool {
            self.0.lock().unwrap().queued > 0
        }
    }

    struct Queue(Arc<Mutex<Line>>);

    impl Subscriber for Queue {
        type Message = Waiting;
        type Error = Infallible;

        fn stream(&mut self) -> impl Stream<Item = Result<Waiting, Infallible>> + '_ {
            LineStream(Arc::clone(&self.0))
        }

        fn ready_hint(&self) -> impl ReadyHint + 'static + use<> {
            LineWaiting(Arc::clone(&self.0))
        }
    }

    /// A shutdown that is never signalled.
    struct Never;

    impl ShutdownWait for Never {
        type Wait<'a> = Pending<()>;

        fn is_cancelled(&self) -> bool {
            false
        }

        fn cancelled(&self) -> Pending<()> {
            pending()
        }
    }

    /// The task's waker: only a wake makes the driver poll the task again.
    #[derive(Default)]
    struct Wakes(AtomicBool);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn a_delivery_after_a_full_run_of_the_bound_wakes_the_idle_subscription() {
        let line = Arc::new(Mutex::new(Line::default()));
        line.lock().unwrap().queued = YIELD_AFTER as usize;
        let mut queue = Cooperative::new(Queue(Arc::clone(&line)));
        let handled = Arc::new(AtomicU32::new(0));
        // The loop of a subscription: the stream is polled first, and a parked subscription is
        // not polled again while its probe says nothing waits.
        let counted = Arc::clone(&handled);
        let mut task = pin!(async move {
            let probe = queue.ready_hint();
            let mut stream = pin!(queue.stream());
            loop {
                let mut parked = false;
                let next = poll_fn(|cx| {
                    if idle(parked, &probe, &Never) {
                        return Poll::Pending;
                    }
                    let polled = stream.as_mut().poll_next(cx);
                    parked = polled.is_pending();
                    polled
                })
                .await;
                assert!(next.is_some(), "the stream does not end");
                counted.fetch_add(1, Ordering::Relaxed);
            }
        });
        let wakes = Arc::new(Wakes(AtomicBool::new(true)));
        let task_waker = Waker::from(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&task_waker);
        let mut run_until_quiet = || {
            while wakes.0.swap(false, Ordering::Relaxed) {
                let _ = task.as_mut().poll(&mut cx);
            }
        };

        run_until_quiet();
        assert_eq!(handled.load(Ordering::Relaxed), YIELD_AFTER);
        line.lock().unwrap().push();
        run_until_quiet();

        assert_eq!(
            handled.load(Ordering::Relaxed),
            YIELD_AFTER + 1,
            "the delivery after the run was left unread"
        );
    }
}
