//! The host without `alloc`: each worker keeps one continuation in a slot of its own.
//!
//! A delivery hands its continuation over through [`Detach::detach`]; the call is pending while
//! the worker's slot is busy (the delivery waits and its worker takes no next one) and ready once the
//! slot holds the continuation. The worker's driver polls the continuation in its own frame, beside
//! the worker's deliveries, with the app task's waker, as embassy's timer queue requires.

// The slot holds work that is not `Send`, and the driver runs it on the app's one task.
#![allow(clippy::future_not_send)]

use core::cell::{Cell, RefCell};
use core::future::poll_fn;
use core::pin::pin;
use core::task::Poll;
use core::time::Duration;

use embassy_sync::waitqueue::WakerRegistration;
use futures::FutureExt as _;
use futures::future::join;
use ruststream_core::diag::error;
use ruststream_runtime::{
    Clock, Continuation, Detach, HostDetach, MaybeSend, MaybeSendSync, Parks,
};

use crate::clock::EmbassyClock;

/// One continuation's room and the hand-over around it.
pub(crate) struct Slot {
    /// The continuation handed over and not yet taken by the driver.
    waiting: RefCell<Option<Continuation>>,
    /// Set from the hand-over until the continuation has finished.
    busy: Cell<bool>,
    /// Set once nothing more will be handed over: the driver ends when the slot is empty.
    closed: Cell<bool>,
    /// The delivery waiting for the slot.
    giver: RefCell<WakerRegistration>,
    /// The driver waiting for a continuation.
    driver: RefCell<WakerRegistration>,
}

impl Slot {
    pub(crate) const fn new() -> Self {
        Self {
            waiting: RefCell::new(None),
            busy: Cell::new(false),
            closed: Cell::new(false),
            giver: RefCell::new(WakerRegistration::new()),
            driver: RefCell::new(WakerRegistration::new()),
        }
    }

    /// Hands `work` over; pending while the slot is busy.
    pub(crate) fn hand(&self, work: Continuation) -> impl Future<Output = ()> {
        let mut work = Some(work);
        poll_fn(move |cx| {
            if self.busy.get() {
                self.giver.borrow_mut().register(cx.waker());
                return Poll::Pending;
            }
            self.busy.set(true);
            *self.waiting.borrow_mut() = work.take();
            self.driver.borrow_mut().wake();
            Poll::Ready(())
        })
    }

    /// Whether a continuation was handed over and has not finished: its timers wake the app task,
    /// so the worker that owns this slot must be resumed on that wake, idle or not.
    pub(crate) fn is_busy(&self) -> bool {
        self.busy.get()
    }

    /// Stops taking work: the driver ends once the continuation it holds has finished.
    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.driver.borrow_mut().wake();
    }

    /// Runs what is handed over, one continuation at a time, until [`close`](Self::close).
    pub(crate) async fn drive(&self) {
        loop {
            let next = poll_fn(|cx| {
                if let Some(work) = self.waiting.borrow_mut().take() {
                    return Poll::Ready(Some(work));
                }
                if self.closed.get() {
                    return Poll::Ready(None);
                }
                self.driver.borrow_mut().register(cx.waker());
                Poll::Pending
            })
            .await;
            let Some(work) = next else { return };
            pin!(work).await;
            self.busy.set(false);
            self.giver.borrow_mut().wake();
        }
    }
}

/// The app's own host: it holds the continuations of deliveries that reach it directly.
pub(crate) struct Detached<Timers: Clock = EmbassyClock> {
    slot: Slot,
    #[allow(dead_code)] // reserved for delayed work, which the app host refuses
    clock: Timers,
}

impl<Timers: Clock> Detached<Timers> {
    /// A host on `clock`.
    pub(crate) const fn on_clock(clock: Timers) -> Self {
        Self {
            slot: Slot::new(),
            clock,
        }
    }

    /// Stops taking work: the driver ends once what it holds has finished.
    pub(crate) fn close(&self) {
        self.slot.close();
    }

    /// Runs what deliveries hand over, until [`close`](Self::close) and the last continuation
    /// finished.
    pub(crate) fn drive(&self) -> impl Future<Output = ()> + '_ {
        self.slot.drive()
    }
}

impl<Timers: Clock<Sleep: MaybeSend + 'static> + MaybeSendSync + 'static> Detach
    for Detached<Timers>
{
    // A copy waits in the store its mount sized with `.retry_capacity`.
    const STORES_COPIES: bool = true;

    fn detach(&self, work: Continuation) -> impl Future<Output = ()> + MaybeSend {
        self.slot.hand(work)
    }

    // Reached only after a reservation, which this host refuses.
    fn detach_after<Work>(&self, _delay: Duration, _work: Work)
    where
        Work: Future<Output = ()> + MaybeSend + 'static,
    {
        error!(
            target: "ruststream::embassy",
            "delayed work reached a host that keeps none",
        );
    }

    fn reserve(&self) -> bool {
        false
    }
}

/// The app's own host.
pub(crate) type AppHost = Detached;

impl AppHost {
    /// The app's host.
    pub(crate) const fn new() -> Self {
        Self::on_clock(EmbassyClock)
    }
}

/// The host one worker lends its deliveries: the continuation slot of the worker, in front of the
/// host the worker runs on.
pub(crate) struct WorkerHost<'host, Inner> {
    inner: &'host Inner,
    slot: Slot,
}

impl<'host, Inner> WorkerHost<'host, Inner> {
    pub(crate) const fn new(inner: &'host Inner) -> Self {
        Self {
            inner,
            slot: Slot::new(),
        }
    }

    /// Whether the worker's continuation is still running.
    pub(crate) fn is_busy(&self) -> bool {
        self.slot.is_busy()
    }

    /// `park` with its idleness withheld while the worker's continuation runs.
    pub(crate) const fn parks<Park: Parks>(&self, park: Park) -> WorkerParks<'_, Park> {
        WorkerParks {
            park,
            busy: &self.slot.busy,
        }
    }

    /// Runs `worker`, the worker's deliveries, beside its continuation slot's driver, and ends the
    /// driver when the worker has ended.
    ///
    /// Not an `async fn`: the combinators hold the worker's future without a frame of their own, so
    /// the app's future nests no deeper for the slot than for the join.
    pub(crate) fn around(
        &self,
        worker: impl Future<Output = ()>,
    ) -> impl Future<Output = ((), ())> {
        self.around_then(worker, || {})
    }

    /// [`around`](Self::around) that also runs `then` when the worker has ended, in the same step.
    pub(crate) fn around_then(
        &self,
        worker: impl Future<Output = ()>,
        then: impl FnOnce(),
    ) -> impl Future<Output = ((), ())> {
        join(
            worker.map(|()| {
                self.slot.close();
                then();
            }),
            self.slot.drive(),
        )
    }
}

/// Where a worker records that it is idle: a worker whose continuation still runs is never idle, so
/// the join above keeps resuming it until the continuation's timer has fired.
#[derive(Clone, Copy)]
pub(crate) struct WorkerParks<'worker, Park> {
    park: Park,
    busy: &'worker Cell<bool>,
}

impl<Park: Parks> Parks for WorkerParks<'_, Park> {
    fn park(self, parked: bool) {
        self.park.park(parked && !self.busy.get());
    }
}

impl<Inner: HostDetach> HostDetach for WorkerHost<'_, Inner> {
    const STORES_COPIES: bool = Inner::STORES_COPIES;

    fn detach_continuation(&self, work: Continuation) -> impl Future<Output = ()> + MaybeSend {
        self.slot.hand(work)
    }

    #[inline]
    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + MaybeSend + 'static,
        Work: Future<Output = ()> + MaybeSend + 'static,
    {
        self.inner.detach_after_with(delay, make);
    }

    #[inline]
    fn reserve(&self) -> bool {
        self.inner.reserve()
    }

    #[inline]
    fn release(&self) {
        self.inner.release();
    }

    type Kept = Inner::Kept;

    #[inline]
    fn kept(&self) -> Option<&Inner::Kept> {
        self.inner.kept()
    }

    type Retry = Inner::Retry;
}

#[cfg(test)]
mod tests {
    use core::pin::pin;
    use core::sync::atomic::{AtomicU32, Ordering};
    use core::task::{Context, Poll, Waker};

    use ruststream_runtime::{Continuation, Detach};

    // The driver on embassy's clock polls embassy's timer, whose queue lives in the executor.
    use embassy_executor as _;

    use super::{Detached, WorkerHost};

    /// Pending on its first poll, ready on the second.
    struct YieldOnce(bool);

    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: core::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                return Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn the_driver_runs_a_handed_continuation_and_ends_once_closed() {
        static RAN: AtomicU32 = AtomicU32::new(0);
        let host = Detached::new();
        let mut driver = pin!(host.drive());
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        let mut hand = pin!(host.detach(Continuation::new(async {
            YieldOnce(false).await;
            RAN.fetch_add(1, Ordering::Relaxed);
        })));
        assert_eq!(hand.as_mut().poll(&mut cx), Poll::Ready(()));
        host.close();
        // The continuation suspends once, so the driver needs a second poll to finish it.
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(RAN.load(Ordering::Relaxed), 1, "the continuation ran");
    }

    #[test]
    fn a_second_continuation_waits_until_the_slot_is_free() {
        static RAN: AtomicU32 = AtomicU32::new(0);
        let host = Detached::new();
        let mut driver = pin!(host.drive());
        let mut cx = Context::from_waker(Waker::noop());
        let mut first = pin!(host.detach(Continuation::new(async {
            YieldOnce(false).await;
            RAN.fetch_add(1, Ordering::Relaxed);
        })));
        assert_eq!(first.as_mut().poll(&mut cx), Poll::Ready(()));
        let mut second = pin!(host.detach(Continuation::new(async {
            RAN.fetch_add(10, Ordering::Relaxed);
        })));
        assert_eq!(
            second.as_mut().poll(&mut cx),
            Poll::Pending,
            "the slot is busy with the first continuation"
        );
        // The driver takes the first one and runs it to its first suspension, then finishes it.
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(second.as_mut().poll(&mut cx), Poll::Ready(()));
        host.close();
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(RAN.load(Ordering::Relaxed), 11);
    }

    #[test]
    fn a_worker_host_keeps_a_slot_of_its_own() {
        static RAN: AtomicU32 = AtomicU32::new(0);
        let app = Detached::new();
        let worker = WorkerHost::new(&app);
        let work = async {
            let mut cx = Context::from_waker(Waker::noop());
            // The worker's deliveries hand a continuation over and end.
            ruststream_runtime::HostDetach::detach_continuation(
                &worker,
                Continuation::new(async {
                    YieldOnce(false).await;
                    RAN.fetch_add(1, Ordering::Relaxed);
                }),
            )
            .await;
            let _ = &mut cx;
        };
        let mut around = pin!(worker.around(work));
        let mut cx = Context::from_waker(Waker::noop());
        let mut polls = 0;
        while around.as_mut().poll(&mut cx).is_pending() {
            polls += 1;
            assert!(polls < 10, "the worker ends once its continuation finished");
        }
        assert_eq!(RAN.load(Ordering::Relaxed), 1);
    }
}
