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
        // A delay past the clock's range waits forever rather than panicking on the addition
        // `Timer::after` makes.
        let deadline = Ticks::try_from(duration)
            .ok()
            .and_then(|ticks| Instant::now().checked_add(ticks))
            .unwrap_or(Instant::MAX);
        Timer::at(deadline)
    }

    fn sleep_until(&self, deadline: Instant) -> Timer {
        Timer::at(deadline)
    }
}
