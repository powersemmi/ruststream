//! Where the work a delivery leaves behind runs: its continuations, kept beside the
//! subscriptions and polled by the app's own task, so every timer they arm is the task's own.
//!
//! A registration's retry copies wait in its own store (`.retry_capacity::<CAP>()`), as data the
//! registration's loop publishes once due; this host keeps none.

// The store holds work that is not `Send`, and the driver runs it on the app's one task.
#![allow(clippy::future_not_send)]

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::future::poll_fn;
use core::pin::Pin;
use core::task::{Poll, Waker};
use core::time::Duration;

use ruststream_runtime::{Clock, LocalDetach};

use crate::clock::EmbassyClock;

/// One piece of detached work.
type Work = Pin<Box<dyn Future<Output = ()>>>;

/// What a delivery hands over and the driver has not taken yet.
struct Inbox {
    works: Vec<Work>,
    waker: Option<Waker>,
}

/// The continuations deliveries left behind, run by [`drive`](Self::drive) on the task that
/// polls it.
///
/// A delivery hands work over through [`LocalDetach`]; the driver takes it at its next poll and
/// polls it with the task's waker, as embassy's integrated timer queue requires of a timer. It
/// keeps no delayed copy: [`reserve`](LocalDetach::reserve) answers `false`, so a `retry_after`
/// on a registration without a store of its own requeues the delivery. The work need not be
/// `Send`: deliveries hand it over and the driver runs it on the app's one task, so the inbox is
/// a plain cell and an idle driver reads one flag and compares its waker.
pub(crate) struct Detached<Timers: Clock = EmbassyClock> {
    inbox: RefCell<Inbox>,
    /// Set when the inbox holds work the driver has not taken.
    handed: Cell<bool>,
    /// Set once nothing more will be handed over: the driver ends when it has run what it holds.
    closed: Cell<bool>,
    clock: Timers,
}

impl<Timers: Clock<Sleep: 'static> + 'static> Detached<Timers> {
    /// A host on `clock`.
    pub(crate) const fn on_clock(clock: Timers) -> Self {
        Self {
            inbox: RefCell::new(Inbox {
                works: Vec::new(),
                waker: None,
            }),
            handed: Cell::new(false),
            closed: Cell::new(false),
            clock,
        }
    }

    /// Stops taking work: the driver ends once what it holds has finished.
    pub(crate) fn close(&self) {
        self.closed.set(true);
        self.wake();
    }

    /// Runs what deliveries hand over, until [`close`](Self::close) and the last piece of work
    /// finished.
    pub(crate) fn drive(&self) -> impl Future<Output = ()> + '_ {
        let mut running: Vec<Work> = Vec::new();
        let mut known: Option<Waker> = None;
        poll_fn(move |cx| {
            if !known
                .as_ref()
                .is_some_and(|waker| waker.will_wake(cx.waker()))
            {
                let waker = cx.waker().clone();
                self.inbox.borrow_mut().waker = Some(waker.clone());
                known = Some(waker);
            }
            // Read before the inbox is taken, so work handed over after the take raises it again.
            let closed = self.closed.get();
            if self.handed.get() {
                self.handed.set(false);
                running.append(&mut self.inbox.borrow_mut().works);
            }
            if !running.is_empty() {
                running.retain_mut(|work| work.as_mut().poll(cx).is_pending());
            }
            if closed && running.is_empty() && !self.handed.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    fn hand(&self, work: Work) {
        self.inbox.borrow_mut().works.push(work);
        self.handed.set(true);
        self.wake();
    }

    fn wake(&self) {
        if let Some(waker) = &self.inbox.borrow().waker {
            waker.wake_by_ref();
        }
    }
}

impl<Timers: Clock<Sleep: 'static> + 'static> LocalDetach for Detached<Timers> {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        self.hand(Box::pin(work));
    }

    // Reached only after a reservation, which this host refuses; the work still runs after its
    // delay, on the task's own timer.
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + 'static,
    {
        let timer = self.clock.sleep(delay);
        self.hand(Box::pin(async move {
            timer.await;
            work.await;
        }));
    }

    fn reserve(&self) -> bool {
        false
    }
}

/// The app's own host: it runs every continuation a delivery leaves.
pub(crate) type AppHost = Detached;

impl AppHost {
    /// The app's host.
    pub(crate) const fn new() -> Self {
        Self::on_clock(EmbassyClock)
    }
}

#[cfg(test)]
mod tests {
    use alloc::rc::Rc;
    use alloc::sync::Arc;
    use core::cell::Cell;
    use core::pin::pin;
    use core::sync::atomic::{AtomicBool, Ordering};
    use core::task::{Context, Poll, Waker};

    use ruststream_runtime::LocalDetach;

    // The driver on embassy's clock polls embassy's timer, whose queue lives in the executor.
    use embassy_executor as _;

    use super::Detached;

    #[test]
    fn the_driver_runs_handed_work_and_ends_once_closed() {
        let host = Detached::new();
        let ran = Arc::new(AtomicBool::new(false));
        let mut driver = pin!(host.drive());
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        let flag = Arc::clone(&ran);
        host.detach(async move { flag.store(true, Ordering::Relaxed) });
        host.close();
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert!(ran.load(Ordering::Relaxed), "the handed work ran");
    }

    #[test]
    fn the_store_runs_work_that_is_not_send() {
        let host = Detached::new();
        let ran = Rc::new(Cell::new(0));
        let mut driver = pin!(host.drive());
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        let held = Rc::clone(&ran);
        LocalDetach::detach(&host, async move { held.set(held.get() + 1) });
        host.close();
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(ran.get(), 1, "pieces of work that hold an `Rc` and ran");
    }

    #[test]
    fn the_apps_own_host_keeps_no_delayed_copy() {
        assert!(!Detached::new().reserve());
    }
}
