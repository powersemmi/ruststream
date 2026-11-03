//! The clock a test moves: the test build's embassy-time driver, held by one harness at a time
//! in the process.
//!
//! The harness is the time driver of the test build, so it knows the deadline of every timer the
//! app armed: [`advance`](ClockLease::advance) moves the clock by what the test asks, and
//! [`advance_to_next`](ClockLease::advance_to_next) to the next deadline, which a shutdown needs
//! to let the work it waits on finish.

use std::cell::{Cell, RefCell};
use std::sync::{Condvar, Mutex, PoisonError};
use std::task::Waker;
use std::time::Duration;

use critical_section::Mutex as CsMutex;
use embassy_time::Duration as Ticks;
use embassy_time_driver::{Driver, time_driver_impl};
use embassy_time_queue_utils::Queue;

/// Whether a harness holds the process's one clock. A harness holds it from start to drop, so
/// tests that move it run one at a time.
// Why a mutex and not an atomic: a harness that finds the clock held waits on the condition
// variable below, which needs the lock.
#[allow(clippy::mutex_atomic)]
static HOLDER: Mutex<bool> = Mutex::new(false);

/// Wakes a harness waiting for the clock when the one holding it drops.
static RELEASED: Condvar = Condvar::new();

thread_local! {
    /// Whether this thread holds the clock: a second harness on the same thread would wait on
    /// itself.
    static HELD: Cell<bool> = const { Cell::new(false) };
}

/// The time the app reads and the timers it armed, in embassy-time's ticks.
#[derive(Debug)]
struct Timeline {
    now: u64,
    timers: Queue,
}

impl Timeline {
    const fn new() -> Self {
        Self {
            now: 0,
            timers: Queue::new(),
        }
    }

    /// Wakes every timer due by now, and returns the next deadline: `u64::MAX` when no timer is
    /// armed.
    fn wake_due(&mut self) -> u64 {
        self.timers.next_expiration(self.now)
    }
}

/// The test build's time driver: a clock that moves only when the harness moves it.
#[derive(Debug)]
struct HarnessDriver(CsMutex<RefCell<Timeline>>);

impl HarnessDriver {
    const fn new() -> Self {
        Self(CsMutex::new(RefCell::new(Timeline::new())))
    }

    /// Runs `step` on the timeline. The lock is a critical section: embassy-time's contract for
    /// a driver, and on the host a process-wide mutex.
    fn with<Out>(&self, step: impl FnOnce(&mut Timeline) -> Out) -> Out {
        critical_section::with(|cs| step(&mut self.0.borrow_ref_mut(cs)))
    }
}

impl Driver for HarnessDriver {
    fn now(&self) -> u64 {
        self.with(|timeline| timeline.now)
    }

    fn schedule_wake(&self, at: u64, waker: &Waker) {
        self.with(|timeline| {
            timeline.timers.schedule_wake(at, waker);
            // A deadline already past wakes at once, as a running clock would.
            timeline.wake_due();
        });
    }
}

time_driver_impl!(static DRIVER: HarnessDriver = HarnessDriver::new());

/// The clock, held by one harness: started at zero with no timer armed.
pub(super) struct ClockLease {
    _private: (),
}

impl ClockLease {
    /// Takes the clock, waiting for the harness of another test to drop it, and resets it.
    ///
    /// # Panics
    ///
    /// Panics if this thread already holds it: a second `TestApp` while the first is alive.
    pub(super) fn take() -> Self {
        assert!(
            !HELD.get(),
            "a TestApp of the embedded runtime is already running on this thread: the clock is \
             one per process, so one TestApp runs at a time"
        );
        // The flag is only read and written under the lock, so a panic elsewhere cannot leave it
        // half-written: a poisoned lock is as good as a clean one.
        let mut held = HOLDER.lock().unwrap_or_else(PoisonError::into_inner);
        while *held {
            held = RELEASED.wait(held).unwrap_or_else(PoisonError::into_inner);
        }
        *held = true;
        drop(held);
        HELD.set(true);
        DRIVER.with(|timeline| *timeline = Timeline::new());
        Self { _private: () }
    }

    /// Moves the clock forward by `by`, waking every timer that fell due.
    // Why a method: the lease is the proof that this harness holds the clock it moves.
    #[allow(clippy::unused_self)]
    pub(super) fn advance(&self, by: Duration) {
        let by = Ticks::try_from(by).map_or(u64::MAX, |by| by.as_ticks());
        DRIVER.with(|timeline| {
            timeline.now = timeline.now.saturating_add(by);
            timeline.wake_due();
        });
    }

    /// Moves the clock to the next deadline an armed timer waits for, waking that timer.
    /// Returns `false`, leaving the clock where it is, when no timer is armed.
    // Why a method: as `advance`.
    #[allow(clippy::unused_self)]
    pub(super) fn advance_to_next(&self) -> bool {
        DRIVER.with(|timeline| {
            let next = timeline.wake_due();
            if next == u64::MAX {
                return false;
            }
            timeline.now = next;
            timeline.wake_due();
            true
        })
    }
}

impl Drop for ClockLease {
    fn drop(&mut self) {
        HELD.set(false);
        *HOLDER.lock().unwrap_or_else(PoisonError::into_inner) = false;
        RELEASED.notify_one();
    }
}
