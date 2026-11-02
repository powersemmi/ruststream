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
use compio::runtime::JoinHandle;
use futures::Stream;
use pin_project_lite::pin_project;
use ruststream_core::{BuildContext, ReadyHint, Subscriber};
use ruststream_runtime::{
    Copying, Delivery, DispatchFailure, Handler, HeadersOnDemand, Lent, RetryCopies, RetryRoute,
    run_sequential,
};

use crate::spawn::spawn;
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

/// A subscriber whose stream hands the core a turn after a run of [`YIELD_AFTER`] deliveries that
/// never waited.
///
/// compio has no budget of its own: a loop whose handler never waits would keep the core inside
/// one poll, and the continuations, hooks and retry timers on that core would wait for the whole
/// burst. The yield comes before the next poll of the stream, after the delivery was settled; a
/// yield per delivery would cost an executor turn per message.
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

/// How many deliveries in a row a subscription takes before it hands the core a turn: the budget
/// tokio gives a task, and the bound of the embedded runner.
const YIELD_AFTER: u32 = 128;

/// A lane's count of deliveries since its last turn to the core.
///
/// A lane has no readiness probe to consult: the yield after the run is a plain [`yield_now`],
/// which completes on its next poll, so taking it with nothing else waiting costs one turn per
/// [`YIELD_AFTER`] deliveries and never strands the lane.
///
/// [`yield_now`]: crate::yield_now
#[derive(Debug, Default)]
pub(crate) struct Budget {
    spent: u32,
}

impl Budget {
    /// Counts one handled delivery; `true` when the run has used the budget and the lane owes the
    /// core a turn.
    #[inline]
    pub(crate) const fn spend(&mut self) -> bool {
        self.spent += 1;
        if self.spent < YIELD_AFTER {
            return false;
        }
        self.spent = 0;
        true
    }
}

pin_project! {
    /// A stream that returns `Pending` once after [`YIELD_AFTER`] ready items in a row, having
    /// woken its task, when its probe says more may be waiting.
    ///
    /// A yield returns `Pending` without polling the stream, so the stream registers no waker, and
    /// a loop skips a subscription whose probe says nothing is ready. A yield past the last
    /// delivery would leave the subscription parked with no waker for the next one, so the yield
    /// is taken only while the probe says more may be waiting; otherwise the stream is polled and
    /// parks with its waker.
    struct YieldingStream<Items, Probe> {
        #[pin]
        inner: Items,
        ready: u32,
        probe: Probe,
    }
}

impl<Items, Probe> YieldingStream<Items, Probe> {
    const fn new(inner: Items, probe: Probe) -> Self {
        Self {
            inner,
            ready: 0,
            probe,
        }
    }
}

impl<Items: Stream, Probe: ReadyHint> Stream for YieldingStream<Items, Probe> {
    type Item = Items::Item;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.ready >= YIELD_AFTER {
            *this.ready = 0;
            if this.probe.may_be_ready() {
                // Woken before it parks: the task is polled again once the core's other tasks
                // have had a turn.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        let polled = this.inner.poll_next(cx);
        if matches!(polled, Poll::Ready(Some(_))) {
            *this.ready += 1;
        } else {
            *this.ready = 0;
        }
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
    spawn(run_sequential(
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

    use super::{Budget, YIELD_AFTER, YieldingStream};

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

    #[test]
    fn after_a_run_the_stream_is_polled_when_the_probe_says_nothing_waits() {
        let polls = Rc::new(Cell::new(0));
        let mut stream = pin!(YieldingStream::new(
            always_ready(Rc::clone(&polls)),
            Probe(false)
        ));
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        for _ in 0..=YIELD_AFTER {
            assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(7)));
        }
        assert_eq!(
            polls.get(),
            YIELD_AFTER + 1,
            "the poll after the run reached the stream, which registers for the next delivery"
        );
        assert!(!woken.0.load(Ordering::SeqCst), "no yield was taken");
    }

    /// A stream that is always ready; counts its polls.
    fn always_ready(polls: Rc<Cell<u32>>) -> impl Stream<Item = u8> {
        poll_fn(move |_cx| {
            polls.set(polls.get() + 1);
            Poll::Ready(Some(7))
        })
    }

    #[test]
    fn while_more_may_be_waiting_the_stream_yields_once_after_a_run_of_deliveries() {
        let polls = Rc::new(Cell::new(0));
        let mut stream = pin!(YieldingStream::new(
            always_ready(Rc::clone(&polls)),
            Probe(true)
        ));
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        for _ in 0..YIELD_AFTER {
            assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(7)));
        }
        assert!(!woken.0.load(Ordering::SeqCst), "no yield inside the run");
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Pending);
        assert_eq!(
            polls.get(),
            YIELD_AFTER,
            "the yield did not poll the stream"
        );
        assert!(woken.0.load(Ordering::SeqCst), "the yield woke its task");
        assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(7)));
        assert_eq!(
            polls.get(),
            YIELD_AFTER + 1,
            "the poll after the yield reached the stream"
        );
    }

    #[test]
    fn a_stream_that_waited_starts_its_run_again() {
        let polls = Rc::new(Cell::new(0));
        // Ready except every hundredth poll: no run reaches the bound.
        let items = poll_fn({
            let polls = Rc::clone(&polls);
            move |_cx| {
                polls.set(polls.get() + 1);
                if polls.get() % 100 == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready(Some(7))
                }
            }
        });
        let mut stream = pin!(YieldingStream::new(items, Probe(true)));
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        for _ in 0..3 * YIELD_AFTER {
            let _ = stream.as_mut().poll_next(&mut cx);
        }
        assert!(
            !woken.0.load(Ordering::SeqCst),
            "no run was long enough to yield"
        );
        assert_eq!(
            polls.get(),
            3 * YIELD_AFTER,
            "every poll reached the stream"
        );
    }

    #[test]
    fn a_lane_owes_the_core_a_turn_once_per_run_of_deliveries() {
        let mut budget = Budget::default();
        for _ in 1..YIELD_AFTER {
            assert!(!budget.spend(), "no turn inside the run");
        }
        assert!(budget.spend(), "a turn once the run used the budget");
        for _ in 1..YIELD_AFTER {
            assert!(!budget.spend(), "the next run starts from zero");
        }
        assert!(budget.spend());
    }
}
