//! The runner's clock over embassy-time.

use core::time::Duration;

use embassy_time::{Duration as Ticks, Instant, Timer};
use ruststream_runtime::Clock;

/// embassy-time's clock: its `Instant` and its `Timer`, read through the driver the binary links.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmbassyClock;

impl Clock for EmbassyClock {
    type Instant = Instant;
    type Sleep = Timer;

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> Timer {
        Timer::at(self.deadline(duration))
    }

    fn sleep_until(&self, deadline: Instant) -> Timer {
        Timer::at(deadline)
    }
}

/// A clock the runner's own timers wait on: it names the deadline a delay ends at.
pub(crate) trait Schedule: Clock<Instant: Send, Sleep: Unpin> + Send + Sync {
    /// The instant `after` from now.
    fn deadline(&self, after: Duration) -> Self::Instant;
}

impl Schedule for EmbassyClock {
    fn deadline(&self, after: Duration) -> Instant {
        // A delay past the clock's range waits forever rather than panicking on the addition
        // `Timer::after` makes.
        Ticks::try_from(after)
            .ok()
            .and_then(|ticks| Instant::now().checked_add(ticks))
            .unwrap_or(Instant::MAX)
    }
}
