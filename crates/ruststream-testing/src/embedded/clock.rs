//! The mock clock of embassy-time a test moves, held by one harness at a time in the process.

use std::cell::Cell;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

use embassy_time::MockDriver;

/// Whether a harness holds the process's one mock clock. A harness holds it from start to drop,
/// so tests that move it run one at a time.
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
            "a TestApp of the embedded runtime is already running on this thread: the mock clock \
             is one per process, so one TestApp runs at a time"
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
        MockDriver::get().reset();
        Self { _private: () }
    }

    /// Moves the clock forward by `by`, waking every timer that fell due.
    // Why a method: the lease is the proof that this harness holds the clock it moves.
    #[allow(clippy::unused_self)]
    pub(super) fn advance(&self, by: Duration) {
        let micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
        MockDriver::get().advance(embassy_time::Duration::from_micros(micros));
    }
}

impl Drop for ClockLease {
    fn drop(&mut self) {
        HELD.set(false);
        *HOLDER.lock().unwrap_or_else(PoisonError::into_inner) = false;
        RELEASED.notify_one();
    }
}
