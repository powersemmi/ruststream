//! The clock a test moves: a timeline held by one thread, which `CompioClock` and
//! [`time::sleep`](crate::time::sleep) read in place of the wall clock.
//!
//! compio has no driver hook and no paused clock, so the timeline is the runner's own. It is
//! thread-local: the harness runs the app on the test's own thread, and a lease taken there cannot
//! be seen from another thread, so two tests never share a clock and need no lock between them.
//!
//! Cost: a timer armed on the timeline is one `BTreeMap` node, as compio's own; an `advance` walks
//! the due nodes once and wakes their tasks.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::mem;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

thread_local! {
    /// The timeline of the lease this thread holds, if any.
    static TIMELINE: RefCell<Option<Timeline>> = const { RefCell::new(None) };
}

/// How far a timeline may be moved: past it a deadline would overflow `Instant`.
const HORIZON: Duration = Duration::from_hours(24 * 365 * 30);

/// One armed timer: ordered by its deadline, then by the order it was armed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TimerKey {
    deadline: Instant,
    id: u64,
}

/// The time the app reads and the timers it armed.
#[derive(Debug)]
struct Timeline {
    base: Instant,
    elapsed: Duration,
    next_id: u64,
    timers: BTreeMap<TimerKey, Option<Waker>>,
}

impl Timeline {
    fn new() -> Self {
        Self {
            base: Instant::now(),
            elapsed: Duration::ZERO,
            next_id: 0,
            timers: BTreeMap::new(),
        }
    }

    fn now(&self) -> Instant {
        self.base + self.elapsed
    }

    /// Takes every timer due by now out of the timeline and returns their wakers.
    fn take_due(&mut self) -> Vec<Waker> {
        let now = self.now();
        let later = self.timers.split_off(&TimerKey {
            deadline: now,
            id: u64::MAX,
        });
        mem::replace(&mut self.timers, later)
            .into_values()
            .flatten()
            .collect()
    }
}

/// Runs `step` on this thread's timeline, if a lease is held.
fn with<Out>(step: impl FnOnce(&mut Timeline) -> Out) -> Option<Out> {
    TIMELINE
        .try_with(|slot| slot.borrow_mut().as_mut().map(step))
        .ok()
        .flatten()
}

/// The virtual time, where this thread holds a lease.
pub(crate) fn now() -> Option<Instant> {
    with(|timeline| timeline.now())
}

/// What arming a timer on the timeline did.
pub(crate) enum Armed {
    /// The deadline had already passed.
    Due,
    /// The timer waits for the timeline to reach its deadline.
    Waiting(TimerKey),
}

/// Arms a timer for `deadline`; `None` where this thread holds no lease.
pub(crate) fn arm(deadline: Instant, cx: &Context<'_>) -> Option<Armed> {
    with(|timeline| {
        if deadline <= timeline.now() {
            return Armed::Due;
        }
        let key = TimerKey {
            deadline,
            id: timeline.next_id,
        };
        timeline.next_id += 1;
        timeline.timers.insert(key, Some(cx.waker().clone()));
        Armed::Waiting(key)
    })
}

/// Polls an armed timer; `None` where the lease is gone.
pub(crate) fn poll(key: &TimerKey, cx: &Context<'_>) -> Option<Poll<()>> {
    with(|timeline| {
        let Some(slot) = timeline.timers.get_mut(key) else {
            return Poll::Ready(());
        };
        if !slot
            .as_ref()
            .is_some_and(|woken| woken.will_wake(cx.waker()))
        {
            *slot = Some(cx.waker().clone());
        }
        Poll::Pending
    })
}

/// Takes a timer off the timeline: its sleep was dropped before it fired.
pub(crate) fn cancel(key: &TimerKey) {
    with(|timeline| timeline.timers.remove(key));
}

/// The virtual clock, held by one harness on the thread it runs on.
///
/// The timeline starts at the moment of the lease, with no timer armed, and moves only when
/// [`advance`](Self::advance) or [`advance_to_next`](Self::advance_to_next) moves it. The lease is
/// not `Send`: the timeline lives on the thread that took it.
///
/// # Panics
///
/// [`take`](Self::take) panics where the thread already holds a lease.
#[doc(hidden)]
#[derive(Debug)]
pub struct VirtualClock {
    // Not `Send`: the lease is the thread's.
    _thread: PhantomData<Rc<()>>,
}

impl VirtualClock {
    /// Takes the clock of this thread, started at zero with no timer armed.
    ///
    /// # Panics
    ///
    /// Panics if this thread already holds a lease: a second harness while the first is alive.
    #[must_use]
    pub fn take() -> Self {
        let taken = TIMELINE.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_some() {
                return false;
            }
            *slot = Some(Timeline::new());
            true
        });
        assert!(
            taken,
            "a virtual clock is already held on this thread: one TestApp runs at a time"
        );
        Self {
            _thread: PhantomData,
        }
    }

    /// The time on the clock now.
    #[must_use]
    pub fn now(&self) -> Instant {
        now().unwrap_or_else(Instant::now)
    }

    /// Moves the clock forward by `by`, waking every timer that fell due.
    pub fn advance(&self, by: Duration) {
        let due = with(|timeline| {
            timeline.elapsed = timeline.elapsed.saturating_add(by).min(HORIZON);
            timeline.take_due()
        })
        .unwrap_or_default();
        wake_all(due);
    }

    /// Moves the clock to the next deadline an armed timer waits for, waking that timer. Returns
    /// `false`, leaving the clock where it is, when no timer is armed.
    #[must_use = "false means no timer is armed"]
    pub fn advance_to_next(&self) -> bool {
        let due = with(|timeline| {
            let (next, _) = timeline.timers.first_key_value()?;
            let to = next.deadline.saturating_duration_since(timeline.base);
            timeline.elapsed = timeline.elapsed.max(to);
            Some(timeline.take_due())
        })
        .flatten();
        let moved = due.is_some();
        wake_all(due.unwrap_or_default());
        moved
    }
}

/// Wakes outside the timeline's borrow: a woken task may touch the clock before it parks.
fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

impl Drop for VirtualClock {
    fn drop(&mut self) {
        // A timer still armed belongs to a sleep that outlives the lease; it falls back to the
        // wall clock the next time it is polled.
        TIMELINE.with(|slot| slot.borrow_mut().take());
    }
}
