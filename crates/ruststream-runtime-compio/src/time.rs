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

use compio::time::sleep_until;
use send_wrapper::SendWrapper;

#[cfg(feature = "testing")]
use crate::virtual_clock::{self, Armed, TimerKey};

/// How far ahead a delay too long for an `Instant` waits instead.
const FAR: Duration = Duration::from_hours(24 * 365 * 30);

/// The timer of the app's clock: ready once its deadline has passed.
///
/// The compio timer is made on the first poll, on the core that polls it, because compio's timers
/// belong to the runtime of their core. A sleep that has not been polled holds only its deadline,
/// so it is `Send` and may be handed to another core; one that has been polled stays on its core.
///
/// Cost: the first poll makes one box for compio's timer, which inserts one `BTreeMap` node, and
/// each poll after that is a dynamic call. Only `retry_after`, `Buffered`, the teardown and a
/// handler's own waits sleep, never the sequential delivery path.
pub struct Sleep {
    deadline: Instant,
    state: State,
}

enum State {
    /// Not polled yet.
    Fresh,
    /// compio's timer, made on the core that polled the sleep first.
    Wall(SendWrapper<Pin<Box<dyn Future<Output = ()>>>>),
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
                    let wall = Box::pin(sleep_until(this.deadline));
                    this.state = State::Wall(SendWrapper::new(wall));
                }
                State::Wall(timer) => return timer.as_mut().poll(cx),
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

#[cfg(feature = "testing")]
impl Drop for Sleep {
    fn drop(&mut self) {
        if let State::Virtual(key) = &self.state {
            virtual_clock::cancel(key);
        }
    }
}

/// The instant `delay` after `now`, or a point far ahead where the sum would overflow.
pub(crate) fn after(now: Instant, delay: Duration) -> Instant {
    now.checked_add(delay).unwrap_or_else(|| now + FAR)
}

/// Waits until `duration` has passed on the app's clock.
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
