//! The timers of one core on the wall clock: a map of deadlines the core's driver task fires.
//!
//! compio's own timers belong to the runtime that made them and cannot leave its thread, so a
//! [`Sleep`](super::Sleep) built on them would not be `Send`. The wheel is the part of a timer that
//! can: a sleep holds a reference to the wheel of the core that first polled it and a key into
//! it, and the driver task of that core, which owns the one compio timer the wheel needs at a time,
//! wakes the sleeps that fell due. Nothing in a sleep is tied to a thread.
//!
//! Cost: arming is one lock and one `BTreeMap` node (shared, like compio's, by up to eleven
//! timers), cancelling and each poll after the first one lock. The wheel and its driver task are
//! made once per core, on its first sleep.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::mem;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use compio::runtime;
use compio::time::sleep_until;
use futures::{FutureExt, select_biased};

thread_local! {
    /// The wheel of the runtime this thread runs, once a sleep needed it.
    static WHEEL: RefCell<Option<Arc<Wheel>>> = const { RefCell::new(None) };
}

/// One armed timer: ordered by its deadline, then by the order it was armed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Key {
    deadline: Instant,
    serial: u64,
}

/// What a poll of an armed timer found.
pub(super) enum Status {
    /// The deadline has passed.
    Ready,
    /// The timer waits for its deadline.
    Pending,
    /// The runtime that drove the wheel is gone, and the timer must be armed again.
    Orphaned,
}

#[derive(Debug, Default)]
struct Timers {
    next_serial: u64,
    armed: BTreeMap<Key, Option<Waker>>,
    /// The driver task, parked until a timer earlier than the one it waits for is armed.
    driver: Option<Waker>,
    /// An earlier timer was armed since the driver last looked.
    earlier: bool,
    /// The driver task is gone with its runtime.
    orphaned: bool,
}

/// The timers of one core.
#[derive(Debug, Default)]
pub(super) struct Wheel {
    timers: Mutex<Timers>,
}

impl Wheel {
    /// The wheel of the runtime this thread runs, made with its driver task on the first call.
    ///
    /// # Panics
    ///
    /// Panics when the thread runs no compio runtime, as compio's own timers do.
    pub(super) fn current() -> Arc<Self> {
        WHEEL.with_borrow_mut(|slot| {
            if let Some(wheel) = slot
                && !wheel.lock().orphaned
            {
                return Arc::clone(wheel);
            }
            let wheel = Arc::new(Self::default());
            runtime::spawn(drive(Arc::clone(&wheel))).detach();
            *slot = Some(Arc::clone(&wheel));
            wheel
        })
    }

    fn lock(&self) -> MutexGuard<'_, Timers> {
        self.timers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Arms a timer for `deadline`; `None` where it has already passed.
    pub(super) fn arm(&self, deadline: Instant, waker: &Waker) -> Option<Key> {
        if deadline <= Instant::now() {
            return None;
        }
        let mut timers = self.lock();
        let key = Key {
            deadline,
            serial: timers.next_serial,
        };
        timers.next_serial += 1;
        let earliest = timers
            .armed
            .first_key_value()
            .is_none_or(|(first, _)| key < *first);
        timers.armed.insert(key, Some(waker.clone()));
        let driver = if earliest {
            timers.earlier = true;
            timers.driver.take()
        } else {
            None
        };
        drop(timers);
        // Outside the lock: the driver may be on another thread and take it at once.
        if let Some(driver) = driver {
            driver.wake();
        }
        Some(key)
    }

    /// Polls an armed timer, keeping `waker` as the one to wake.
    pub(super) fn poll(&self, key: &Key, waker: &Waker) -> Status {
        let mut timers = self.lock();
        if timers.orphaned {
            return Status::Orphaned;
        }
        timers.armed.get_mut(key).map_or(Status::Ready, |slot| {
            if !slot.as_ref().is_some_and(|armed| armed.will_wake(waker)) {
                *slot = Some(waker.clone());
            }
            Status::Pending
        })
    }

    /// Takes a timer off the wheel: its sleep was dropped before it fired.
    pub(super) fn cancel(&self, key: &Key) {
        let removed = self.lock().armed.remove(key);
        // A waker dropped outside the lock may run a destructor of the task.
        drop(removed);
    }
}

/// Marks the wheel orphaned when its driver task is dropped with the runtime.
struct Orphan(Arc<Wheel>);

impl Drop for Orphan {
    fn drop(&mut self) {
        let armed = {
            let mut timers = self.0.lock();
            timers.orphaned = true;
            timers.driver = None;
            mem::take(&mut timers.armed)
        };
        drop(armed);
    }
}

/// Resolves when a timer earlier than the one the driver waits for was armed.
struct Earlier<'wheel>(&'wheel Wheel);

impl Future for Earlier<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut timers = self.0.lock();
        if mem::take(&mut timers.earlier) {
            return Poll::Ready(());
        }
        if !timers
            .driver
            .as_ref()
            .is_some_and(|parked| parked.will_wake(cx.waker()))
        {
            timers.driver = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

/// The task of a core that wakes the sleeps of its wheel as their deadlines pass.
// Why: the task waits on a compio timer, which belongs to its core; it is spawned there and never
// moves.
#[allow(clippy::future_not_send)]
async fn drive(wheel: Arc<Wheel>) {
    let _orphan = Orphan(Arc::clone(&wheel));
    let mut due: Vec<Waker> = Vec::new();
    loop {
        let next = {
            let mut timers = wheel.lock();
            let now = Instant::now();
            while let Some(first) = timers.armed.first_entry() {
                if first.key().deadline > now {
                    break;
                }
                due.extend(first.remove());
            }
            timers.earlier = false;
            timers.armed.first_key_value().map(|(key, _)| key.deadline)
        };
        for waker in &due {
            waker.wake_by_ref();
        }
        // Cleared, not drained: the buffer keeps its room for the next round.
        due.clear();
        let mut earlier = pin!(Earlier(&wheel).fuse());
        match next {
            Some(deadline) => {
                let mut passed = pin!(sleep_until(deadline).fuse());
                select_biased! {
                    () = passed => {},
                    () = earlier => {},
                }
            }
            None => earlier.await,
        }
    }
}
