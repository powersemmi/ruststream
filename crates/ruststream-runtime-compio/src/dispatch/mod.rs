//! The compio half of the dispatch: the loops a subscription runs in, on core 0 and on the cores
//! its deliveries are handed to, all driving the shared delivery body of `ruststream-runtime`.
//!
//! - Core 0 receives every subscription. A sequential mount runs [`spawn_dispatch`] there and
//!   yields to the core after each delivery.
//! - `workers(n)` and `threads(n)` hand deliveries to lanes on other cores ([`spawn_dispatch_workers`]) through
//!   the rings of `ruststream-runtime`: one queue push and a wake per delivery, no allocation
//!   once the lanes have started. `by_key` hashes the key on core 0.
//! - [`Layout`] places the mounts over the cores at startup and [`Fabric`] starts the lanes there.

mod batch;
mod cores;
mod slots;
mod workers;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use compio::runtime::{self, JoinHandle};
use futures::Stream;
use pin_project_lite::pin_project;
use ruststream_core::{BuildContext, ReadyHint, Subscriber};
use ruststream_runtime::{
    Copying, Delivery, DispatchFailure, Handler, HeadersOnDemand, Lent, RetryCopies, RetryRoute,
    run_sequential,
};

use crate::{Stop, Tasks};

pub use batch::spawn_batch_dispatch;
pub use cores::{Fabric, Layout, LayoutError, Mount, StartLaneError};
pub use workers::spawn_dispatch_workers;

/// What one subscription's loop dispatches under.
///
/// It holds the shared body's delivery context and the runner it hands leftovers to, with the
/// registration's retry route in its concrete type, kept in the one allocation a subscription
/// makes for its context.
#[derive(Debug)]
pub struct Hosted<C, Retry> {
    /// The delivery context.
    pub delivery: Delivery<C>,
    /// Where the work a delivery leaves behind runs.
    pub host: Copying<Tasks, Retry>,
}

impl<C, Retry> Hosted<C, Retry> {
    /// `delivery` dispatched on compio, its leftovers counted by `tasks`, its retry copies leaving
    /// through `route`.
    #[must_use]
    pub fn new(delivery: Delivery<C>, route: Option<RetryRoute<Retry>>, tasks: Tasks) -> Self {
        Self {
            delivery,
            host: Copying::new(tasks, route.map(Arc::new)),
        }
    }
}

impl<C, Retry: RetryCopies<C>> Lent<C> for Hosted<C, Retry> {
    type Host = Copying<Tasks, Retry>;

    #[inline]
    fn delivery(&self) -> &Delivery<C> {
        &self.delivery
    }

    #[inline]
    fn host(&self) -> &Copying<Tasks, Retry> {
        &self.host
    }
}

/// What the loop shares with every lane it starts, behind one reference count taken once per lane.
pub(crate) struct Shared<Body, State, Cx, Retry> {
    pub(crate) handler: Arc<Body>,
    pub(crate) name: Arc<str>,
    pub(crate) state: Arc<State>,
    pub(crate) hosted: Arc<Hosted<Cx, Retry>>,
    pub(crate) failure: DispatchFailure<Stop>,
}

/// A subscriber whose stream hands the core a turn after each delivery.
///
/// compio has no budget of its own: a loop whose handler never waits would keep the core inside
/// one poll, and the continuations, hooks and retry timers on that core would wait for the whole
/// burst. The yield comes before the next poll of the stream, after the delivery was settled.
pub(crate) struct Yielding<Inner>(pub(crate) Inner);

impl<Inner> Yielding<Inner> {
    pub(crate) const fn new(inner: Inner) -> Self {
        Self(inner)
    }
}

impl<Inner: Subscriber> Subscriber for Yielding<Inner> {
    type Message = Inner::Message;
    type Error = Inner::Error;

    fn stream(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + ruststream_core::MaybeSend + '_
    {
        let probe = self.0.ready_hint();
        YieldingStream::new(self.0.stream(), probe)
    }

    // The yield adds nothing to the answer: it is taken only while the probe says more may be
    // waiting, so a yielded stream still reads as ready.
    fn ready_hint(&self) -> impl ReadyHint + ruststream_core::MaybeSendSync + 'static + use<Inner> {
        self.0.ready_hint()
    }
}

pin_project! {
    /// A stream that returns `Pending` once after each ready item, having woken its task, when its
    /// probe says more may be waiting.
    ///
    /// A yield returns `Pending` without polling the stream, so the stream registers no waker, and
    /// a loop skips a subscription whose probe says nothing is ready. A yield past the last
    /// delivery would leave the subscription parked with no waker for the next one, so the yield
    /// is taken only while the probe says more may be waiting; otherwise the stream is polled and
    /// parks with its waker.
    struct YieldingStream<Items, Probe> {
        #[pin]
        inner: Items,
        owed: bool,
        probe: Probe,
    }
}

impl<Items, Probe> YieldingStream<Items, Probe> {
    const fn new(inner: Items, probe: Probe) -> Self {
        Self {
            inner,
            owed: false,
            probe,
        }
    }
}

impl<Items: Stream, Probe: ReadyHint> Stream for YieldingStream<Items, Probe> {
    type Item = Items::Item;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.owed {
            *this.owed = false;
            if this.probe.may_be_ready() {
                // Woken before it parks: the task is polled again once the core's other tasks
                // have had a turn.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        let polled = this.inner.poll_next(cx);
        *this.owed = matches!(polled, Poll::Ready(Some(_)));
        polled
    }
}

/// Spawns, on the calling core's runtime, a task that drives `subscriber` through `handler` until
/// `stop` is raised or the stream terminates: [`run_sequential`], yielding to the core after each
/// delivery.
///
/// The task ends when its handle is dropped; the caller holds it for the life of the
/// subscription. Call it from core 0, which receives every subscription.
pub fn spawn_dispatch<S, H, C, St, Retry>(
    subscriber: S,
    handler: Arc<H>,
    stop: Stop,
    name: Arc<str>,
    state: Arc<St>,
    hosted: Arc<Hosted<C, Retry>>,
    failure: DispatchFailure<Stop>,
) -> JoinHandle<()>
where
    Retry: RetryCopies<C> + 'static,
    S: Subscriber + 'static,
    H: Handler<S::Message, C, St> + 'static,
    C: BuildContext<S::Message> + Send + Sync + 'static,
    St: Send + Sync + 'static,
{
    runtime::spawn(run_sequential(
        Yielding::new(subscriber),
        handler,
        stop,
        name,
        state,
        hosted,
        failure,
        (),
        HeadersOnDemand,
        (),
        BytesMut::new(),
    ))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use futures::Stream;
    use futures::stream::poll_fn;
    use ruststream_core::ReadyHint;

    use super::YieldingStream;

    /// A probe with a fixed answer.
    struct Probe(bool);

    impl ReadyHint for Probe {
        fn may_be_ready(&self) -> bool {
            self.0
        }
    }

    /// A waker that notes it was woken.
    #[derive(Default)]
    struct Woken(AtomicBool);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A stream with one item, then pending; counts its polls.
    fn one_item(polls: Rc<Cell<u32>>) -> impl Stream<Item = u8> {
        poll_fn(move |_cx| {
            polls.set(polls.get() + 1);
            if polls.get() == 1 {
                Poll::Ready(Some(7))
            } else {
                Poll::Pending
            }
        })
    }

    #[test]
    fn after_the_last_delivery_the_stream_is_polled_and_parks_with_its_waker() {
        let polls = Rc::new(Cell::new(0));
        let mut stream = pin!(YieldingStream::new(
            one_item(Rc::clone(&polls)),
            Probe(false)
        ));
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(7)));
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Pending);
        assert_eq!(
            polls.get(),
            2,
            "the stream registered for the next delivery"
        );
        assert!(!woken.0.load(Ordering::SeqCst), "no yield was taken");
    }

    #[test]
    fn while_more_may_be_waiting_the_stream_yields_once_after_a_delivery() {
        let polls = Rc::new(Cell::new(0));
        let mut stream = pin!(YieldingStream::new(
            one_item(Rc::clone(&polls)),
            Probe(true)
        ));
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(7)));
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Pending);
        assert_eq!(polls.get(), 1, "the yield did not poll the stream");
        assert!(woken.0.load(Ordering::SeqCst), "the yield woke its task");
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Pending);
        assert_eq!(
            polls.get(),
            2,
            "the poll after the yield reached the stream"
        );
    }
}
