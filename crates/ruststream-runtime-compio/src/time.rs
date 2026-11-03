//! Time for service code: the sleep that runs on the app's clock.
//!
//! A handler that waits sleeps through [`sleep`], not through compio's timer directly: the sleep is
//! a timer of the clock the app runs on, so the test harness moves it with `advance` like every
//! timer the framework arms. compio's own `time::sleep` reads the wall clock, and `advance` does
//! not move it.

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::time::wheel::{Key, Status, Wheel};
#[cfg(feature = "testing")]
use crate::virtual_clock::{self, Armed, TimerKey};

mod wheel;

/// How far ahead a delay too long for an `Instant` waits instead.
const FAR: Duration = Duration::from_hours(24 * 365 * 30);

/// The timer of the app's clock: ready once its deadline has passed.
///
/// A sleep is `Send` and may be polled by any core, at any point of its life: it holds its deadline
/// and, once polled, a place in the timers of the core that polled it first. That core wakes it, so
/// a sleep handed to another core after its first poll still ends, as long as the first core runs.
/// The first poll must run on a compio runtime, as compio's own timers must.
///
/// Cost: the first poll takes one lock and one share of a `BTreeMap` node, and every poll after
/// that one lock. Only `retry_after`, `Buffered`, the teardown and a handler's own waits sleep,
/// never the sequential delivery path.
pub struct Sleep {
    deadline: Instant,
    state: State,
}

enum State {
    /// Not polled yet.
    Fresh,
    /// A timer in the wheel of the core that polled the sleep first.
    Wall(&'static Wheel, Key),
    /// A timer on the thread's virtual timeline.
    #[cfg(feature = "testing")]
    Virtual(TimerKey),
}

impl Sleep {
    /// A timer that completes at `deadline`.
    pub(crate) const fn until(deadline: Instant) -> Self {
        Self {
            deadline,
            state: State::Fresh,
        }
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        loop {
            match &mut this.state {
                State::Fresh => {
                    #[cfg(feature = "testing")]
                    match virtual_clock::arm(this.deadline, cx) {
                        Some(Armed::Due) => return Poll::Ready(()),
                        Some(Armed::Waiting(key)) => {
                            this.state = State::Virtual(key);
                            return Poll::Pending;
                        }
                        None => {}
                    }
                    let wheel = Wheel::current();
                    let Some(key) = wheel.arm(this.deadline, cx.waker()) else {
                        return Poll::Ready(());
                    };
                    this.state = State::Wall(wheel, key);
                    return Poll::Pending;
                }
                State::Wall(wheel, key) => match wheel.poll(key, cx.waker()) {
                    Status::Ready => return Poll::Ready(()),
                    Status::Pending => return Poll::Pending,
                    // The core that drove the timer is gone: it goes on where it is polled now.
                    Status::Orphaned => this.state = State::Fresh,
                },
                #[cfg(feature = "testing")]
                State::Virtual(key) => match virtual_clock::poll(key, cx) {
                    Some(poll) => return poll,
                    // The lease is gone: the timer goes on by the wall clock.
                    None => this.state = State::Fresh,
                },
            }
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        match &self.state {
            State::Wall(wheel, key) => wheel.cancel(key),
            #[cfg(feature = "testing")]
            State::Virtual(key) => virtual_clock::cancel(key),
            State::Fresh => {}
        }
    }
}

/// The instant `delay` after `now`, or a point far ahead where the sum would overflow.
pub(crate) fn after(now: Instant, delay: Duration) -> Instant {
    now.checked_add(delay).unwrap_or_else(|| now + FAR)
}

/// Waits until `duration` has passed on the app's clock.
///
/// One compio runtime per thread drives its timers: a thread runs its runtimes one after another,
/// as every core thread of the runner does.
///
/// # Examples
///
/// A handler that backs off before it answers; under the harness, `advance` moves the sleep:
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream_runtime_compio::time;
///
/// async fn throttled(order_id: u64) -> bool {
///     time::sleep(Duration::from_millis(250)).await;
///     order_id != 0
/// }
/// # }
/// ```
#[must_use]
pub fn sleep(duration: Duration) -> Sleep {
    Sleep::until(after(crate::CompioClock::instant(), duration))
}
