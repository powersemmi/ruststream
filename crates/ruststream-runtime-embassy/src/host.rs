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

use crate::clock::{EmbassyClock, Schedule};

/// A piece of detached work: a continuation, or a delayed copy of `retry_after` with the
/// instant it is due at.
struct Work<At> {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    /// When a delayed copy is due; `None` for a continuation, which runs at once.
    due: Option<At>,
}

/// What a delivery hands over and the driver has not taken yet.
struct Inbox<At> {
    works: Vec<Work<At>>,
    waker: Option<Waker>,
}

/// The work deliveries left behind, run by [`drive`](Self::drive) on the task that polls it.
///
/// A delivery hands work over through [`Detach`]; the driver takes it at its next poll and polls
/// it with the task's waker, as embassy's integrated timer queue requires of a timer. `room`
/// bounds the delayed copies of `retry_after`: a reservation past it is refused, and the body
/// requeues the delivery. A delayed copy is not polled before it is due: the driver waits for
/// the earliest one on one timer, so a wake costs one timer however many copies wait. Handing
/// work over takes a critical section; an idle driver reads one flag and compares its waker.
pub(crate) struct Detached<Timers: Clock = EmbassyClock> {
    inbox: Mutex<CriticalSectionRawMutex, RefCell<Inbox<Timers::Instant>>>,
    /// Set when the inbox holds work the driver has not taken.
    handed: AtomicBool,
    /// Set once nothing more will be handed over: the driver ends when it has run what it holds.
    closed: AtomicBool,
    /// Delayed copies reserved, waiting or running.
    reserved: AtomicUsize,
    room: usize,
    clock: Timers,
}

impl Detached {
    /// A host on embassy's clock with room for `room` delayed copies at once.
    pub(crate) const fn with_room(room: usize) -> Self {
        Self::on_clock(EmbassyClock, room)
    }
}

/// What the driver holds: the work it polls on every wake, and the delayed copies not due yet
/// with the one timer they wait on.
struct Held<Timers: Clock> {
    running: Vec<Work<Timers::Instant>>,
    waiting: Vec<Work<Timers::Instant>>,
    /// The timer of the earliest waiting copy, and the instant it ends at.
    next: Option<(Timers::Instant, Timers::Sleep)>,
}

impl<Timers: Schedule> Detached<Timers> {
    /// A host on `clock` with room for `room` delayed copies at once.
    pub(crate) const fn on_clock(clock: Timers, room: usize) -> Self {
        Self {
            inbox: Mutex::new(RefCell::new(Inbox {
                works: Vec::new(),
                waker: None,
            })),
            handed: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            reserved: AtomicUsize::new(0),
            room,
            clock,
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
        let mut held = Held::<Timers> {
            running: Vec::new(),
            waiting: Vec::new(),
            next: None,
        };
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
                self.take_handed(&mut held);
            }
            if !held.waiting.is_empty() {
                self.release_due(&mut held, cx);
            }
            if !held.running.is_empty() {
                self.poll_running(&mut held.running, cx);
            }
            if closed
                && held.running.is_empty()
                && held.waiting.is_empty()
                && !self.handed.load(Ordering::Relaxed)
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }

    /// Moves what was handed over into the driver: continuations to run, delayed copies to
    /// wait, the timer moved earlier when a new copy is due first.
    fn take_handed(&self, held: &mut Held<Timers>) {
        self.inbox.lock(|inbox| {
            for work in inbox.borrow_mut().works.drain(..) {
                let Some(due) = work.due else {
                    held.running.push(work);
                    continue;
                };
                if held.next.as_ref().is_none_or(|(next, _)| due < *next) {
                    held.next = Some((due, self.clock.sleep_until(due)));
                }
                held.waiting.push(work);
            }
        });
    }

    /// Polls the earliest copy's timer; once it ends, moves every copy due by then to the
    /// running work and arms the timer for the next one.
    fn release_due(&self, held: &mut Held<Timers>, cx: &mut Context<'_>) {
        let Some((_, timer)) = held.next.as_mut() else {
            return;
        };
        if Pin::new(timer).poll(cx).is_pending() {
            return;
        }
        let now = self.clock.now();
        let mut earliest: Option<Timers::Instant> = None;
        let mut index = 0;
        while index < held.waiting.len() {
            match held.waiting[index].due {
                Some(due) if due > now => {
                    earliest = Some(earliest.map_or(due, |at| at.min(due)));
                    index += 1;
                }
                _ => held.running.push(held.waiting.swap_remove(index)),
            }
        }
        held.next = earliest.map(|due| (due, self.clock.sleep_until(due)));
        if held.next.is_some() {
            // The new timer registers this task's waker on its first poll.
            self.release_due(held, cx);
        }
    }

    fn poll_running(&self, running: &mut Vec<Work<Timers::Instant>>, cx: &mut Context<'_>) {
        running.retain_mut(|work| {
            if work.future.as_mut().poll(cx).is_pending() {
                return true;
            }
            if work.due.is_some() {
                self.give_back();
            }
            false
        });
    }

    fn hand(&self, work: Work<Timers::Instant>) {
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

impl<Timers: Schedule> Detach for Detached<Timers> {
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.hand(self::Work {
            future: Box::pin(work),
            due: None,
        });
    }

    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        self.hand(self::Work {
            future: Box::pin(work),
            due: Some(self.clock.deadline(delay)),
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
    use core::pin::{Pin, pin};
    use core::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use core::task::{Context, Poll, Waker};
    use core::time::Duration;

    use ruststream_runtime::{Clock, Detach};

    // The driver on embassy's clock polls embassy's timer, whose queue lives in the executor.
    use embassy_executor as _;

    use super::Detached;
    use crate::clock::Schedule;

    /// A clock the test moves by hand, counting how often its timers are polled.
    #[derive(Clone, Default)]
    struct Probe {
        now: Arc<AtomicU64>,
        polls: Arc<AtomicU32>,
    }

    impl Probe {
        fn advance_to(&self, at: u64) {
            self.now.store(at, Ordering::Relaxed);
        }

        fn polls(&self) -> u32 {
            self.polls.load(Ordering::Relaxed)
        }
    }

    /// A timer of the probe clock.
    struct ProbeSleep {
        at: u64,
        clock: Probe,
    }

    impl Future for ProbeSleep {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.clock.polls.fetch_add(1, Ordering::Relaxed);
            if self.clock.now.load(Ordering::Relaxed) >= self.at {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    impl Clock for Probe {
        type Instant = u64;
        type Sleep = ProbeSleep;

        fn now(&self) -> u64 {
            self.now.load(Ordering::Relaxed)
        }

        fn sleep(&self, duration: Duration) -> ProbeSleep {
            self.sleep_until(self.deadline(duration))
        }

        fn sleep_until(&self, at: u64) -> ProbeSleep {
            ProbeSleep {
                at,
                clock: self.clone(),
            }
        }
    }

    impl Schedule for Probe {
        fn deadline(&self, after: Duration) -> u64 {
            self.now() + after.as_secs()
        }
    }

    #[test]
    fn waiting_copies_cost_one_timer_per_wake_and_run_once_due() {
        let clock = Probe::default();
        let host = Detached::on_clock(clock.clone(), 3);
        let ran = Arc::new(AtomicU32::new(0));
        for delay in [10, 20, 30] {
            assert!(host.reserve(), "room for the copy");
            let ran = Arc::clone(&ran);
            host.detach_after(Duration::from_secs(delay), async move {
                ran.fetch_add(1, Ordering::Relaxed);
            });
        }
        let mut driver = pin!(host.drive());
        let mut cx = Context::from_waker(Waker::noop());
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        for _ in 0..10 {
            // A continuation a delivery leaves wakes the driver, as each delivery would.
            host.detach(async {});
            assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        }
        assert_eq!(
            clock.polls(),
            11,
            "timer polls over eleven wakes with three copies waiting"
        );
        clock.advance_to(20);
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(ran.load(Ordering::Relaxed), 2, "copies run once due");
        assert!(
            host.reserve() && host.reserve(),
            "the finished copies gave their room back"
        );
        assert!(!host.reserve(), "the waiting copy keeps its room");
        host.release();
        host.release();
        clock.advance_to(30);
        host.close();
        assert_eq!(driver.as_mut().poll(&mut cx), Poll::Ready(()));
        assert_eq!(ran.load(Ordering::Relaxed), 3, "every copy ran");
    }

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
