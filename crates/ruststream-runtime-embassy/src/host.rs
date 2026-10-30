//! Where the work a delivery leaves behind runs: futures kept beside the subscriptions and polled
//! by the app's own task, so every timer they arm is the task's own.

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::future::poll_fn;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use core::task::{Context, Poll, Waker};
use core::time::Duration;

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use ruststream_runtime::{Clock, Detach};

use crate::clock::EmbassyClock;

/// A piece of detached work, and whether it holds a reservation for a delayed copy.
struct Work {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    delayed: bool,
}

/// What a delivery hands over and the driver has not taken yet.
struct Inbox {
    works: Vec<Work>,
    waker: Option<Waker>,
}

/// The work deliveries left behind, run by [`drive`](Self::drive) on the task that polls it.
///
/// A delivery hands work over through [`Detach`]; the driver takes it at its next poll and polls
/// it with the task's waker, as embassy's integrated timer queue requires of a timer. `room`
/// bounds the delayed copies of `retry_after`: a reservation past it is refused, and the body
/// requeues the delivery. Handing work over takes a critical section; an idle driver reads one
/// flag and compares its waker.
pub(crate) struct Detached {
    inbox: Mutex<CriticalSectionRawMutex, RefCell<Inbox>>,
    /// Set when the inbox holds work the driver has not taken.
    handed: AtomicBool,
    /// Set once nothing more will be handed over: the driver ends when it has run what it holds.
    closed: AtomicBool,
    /// Delayed copies reserved or running.
    reserved: AtomicUsize,
    room: usize,
}

impl Detached {
    /// A host with room for `room` delayed copies at once.
    pub(crate) const fn with_room(room: usize) -> Self {
        Self {
            inbox: Mutex::new(RefCell::new(Inbox {
                works: Vec::new(),
                waker: None,
            })),
            handed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            reserved: AtomicUsize::new(0),
            room,
        }
    }

    /// Stops taking work: the driver ends once what it holds has finished.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
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
                self.inbox
                    .lock(|inbox| inbox.borrow_mut().waker = Some(waker.clone()));
                known = Some(waker);
            }
            // Read before the inbox is taken, so work handed over after the take raises it again.
            let closed = self.closed.load(Ordering::Relaxed);
            if self.handed.load(Ordering::Relaxed) {
                self.handed.store(false, Ordering::Relaxed);
                self.inbox
                    .lock(|inbox| running.append(&mut inbox.borrow_mut().works));
            }
            if !running.is_empty() {
                self.poll_running(&mut running, cx);
            }
            if closed && running.is_empty() && !self.handed.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    fn poll_running(&self, running: &mut Vec<Work>, cx: &mut Context<'_>) {
        running.retain_mut(|work| {
            if work.future.as_mut().poll(cx).is_pending() {
                return true;
            }
            if work.delayed {
                self.give_back();
            }
            false
        });
    }

    fn hand(&self, work: Work) {
        self.inbox.lock(|inbox| inbox.borrow_mut().works.push(work));
        self.handed.store(true, Ordering::Relaxed);
        self.wake();
    }

    fn wake(&self) {
        self.inbox.lock(|inbox| {
            if let Some(waker) = &inbox.borrow().waker {
                waker.wake_by_ref();
            }
        });
    }

    fn give_back(&self) {
        // One task hands over and drives, so a load and a store suffice where a
        // read-modify-write is not available.
        let reserved = self.reserved.load(Ordering::Relaxed);
        self.reserved
            .store(reserved.saturating_sub(1), Ordering::Relaxed);
    }
}

impl Detach for Detached {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.hand(self::Work {
            future: Box::pin(work),
            delayed: false,
        });
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.hand(self::Work {
            future: Box::pin(async move {
                EmbassyClock.sleep(delay).await;
                work.await;
            }),
            delayed: true,
        });
    }

    fn reserve(&self) -> bool {
        let reserved = self.reserved.load(Ordering::Relaxed);
        if reserved >= self.room {
            return false;
        }
        self.reserved.store(reserved + 1, Ordering::Relaxed);
        true
    }

    fn release(&self) {
        self.give_back();
    }
}

/// The app's own host: it runs every continuation a delivery leaves, and keeps no delayed copy,
/// so `retry_after` on a mount that declared no retry capacity requeues the delivery.
pub(crate) type AppHost = Detached;

impl AppHost {
    /// The app's host.
    pub(crate) const fn new() -> Self {
        Self::with_room(0)
    }
}

#[cfg(test)]
mod tests {
    use alloc::sync::Arc;
    use core::pin::pin;
    use core::sync::atomic::{AtomicBool, Ordering};
    use core::task::{Context, Poll, Waker};

    use ruststream_runtime::Detach;

    use super::Detached;

    #[test]
    fn the_driver_runs_handed_work_and_ends_once_closed() {
        let host = Detached::with_room(0);
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
    fn room_bounds_the_delayed_copies_and_a_release_gives_it_back() {
        let host = Detached::with_room(2);
        assert!(host.reserve());
        assert!(host.reserve());
        assert!(!host.reserve(), "a third copy has no room");
        host.release();
        assert!(host.reserve(), "a released reservation is room again");
    }

    #[test]
    fn the_apps_own_host_keeps_no_delayed_copy() {
        assert!(!Detached::new().reserve());
    }
}
