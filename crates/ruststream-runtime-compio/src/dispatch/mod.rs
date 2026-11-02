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
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};

use bytes::BytesMut;
use compio::runtime::{self, JoinHandle};
use futures::Stream;
use pin_project_lite::pin_project;
use ruststream_core::{BuildContext, ReadyHint, Subscriber};
use ruststream_runtime::{
    Copying, Delivery, DispatchFailure, Handler, HeadersOnDemand, Lent, RetryCopies, RetryRoute,
    run_sequential,
};

use crate::{Stop, Tasks, YieldNow, yield_now};

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
///
/// While a yield is owed the subscription's probe answers that something may be ready: the yield
/// completes only in a poll of the stream, and a loop that skipped that poll on an idle probe
/// would never be woken again, the wake of the stream having been spent on the delivery before.
pub(crate) struct Yielding<Inner> {
    inner: Inner,
    owed: Arc<AtomicBool>,
}

impl<Inner> Yielding<Inner> {
    pub(crate) fn new(inner: Inner) -> Self {
        Self {
            inner,
            owed: Arc::default(),
        }
    }
}

/// The probe of a [`Yielding`] subscriber: the inner probe, or a yield still owed.
struct Owing<Probe> {
    probe: Probe,
    owed: Arc<AtomicBool>,
}

impl<Probe: ReadyHint> ReadyHint for Owing<Probe> {
    const GATED: bool = Probe::GATED;

    #[inline]
    fn may_be_ready(&self) -> bool {
        self.owed.load(Ordering::Relaxed) || self.probe.may_be_ready()
    }
}

/// Whether the runtime consults `probe` at all.
const fn gated<Probe: ReadyHint>(_probe: &Probe) -> bool {
    Probe::GATED
}

impl<Inner: Subscriber> Subscriber for Yielding<Inner> {
    type Message = Inner::Message;
    type Error = Inner::Error;

    fn stream(
        &mut self,
    ) -> impl Stream<Item = Result<Self::Message, Self::Error>> + ruststream_core::MaybeSend + '_
    {
        // Only a gated probe reads the flag, so a subscriber without one never writes it.
        let flag = gated(&self.inner.ready_hint()).then(|| Arc::clone(&self.owed));
        YieldingStream {
            inner: self.inner.stream(),
            owed: None,
            flag,
        }
    }

    fn ready_hint(&self) -> impl ReadyHint + ruststream_core::MaybeSendSync + 'static + use<Inner> {
        Owing {
            probe: self.inner.ready_hint(),
            owed: Arc::clone(&self.owed),
        }
    }
}

pin_project! {
    struct YieldingStream<Items> {
        #[pin]
        inner: Items,
        owed: Option<YieldNow>,
        flag: Option<Arc<AtomicBool>>,
    }
}

impl<Items: Stream> Stream for YieldingStream<Items> {
    type Item = Items::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(owed) = this.owed {
            ready!(Pin::new(owed).poll(cx));
            *this.owed = None;
            if let Some(flag) = this.flag {
                flag.store(false, Ordering::Relaxed);
            }
        }
        let polled = this.inner.poll_next(cx);
        if matches!(polled, Poll::Ready(Some(_))) {
            *this.owed = Some(yield_now());
            if let Some(flag) = this.flag {
                flag.store(true, Ordering::Relaxed);
            }
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
