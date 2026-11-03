//! The compio runner's clock.

use std::time::{Duration, Instant};

use ruststream_runtime::Clock;

use crate::time::{self, Sleep};
#[cfg(feature = "testing")]
use crate::virtual_clock;

/// The compio runner's clock: compio's timers over `std::time::Instant`.
///
/// Every timer the runner arms for the app (a delayed retry copy, a batch deadline, the teardown's
/// grace period, [`time::sleep`]) is a timer of this clock. Under the `testing` feature, a thread
/// that holds a [`VirtualClock`](crate::testing::VirtualClock) reads its time and arms its timers
/// there, so a test moves all of them at once; compio has no paused clock of its own.
///
/// Cost: [`now`](Clock::now) is `Instant::now` (a vDSO call), or one thread-local read more under
/// `testing`; a timer is one `BTreeMap` node in the runtime of the core that polls it.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use compio::runtime::Runtime;
/// use ruststream_runtime::Clock;
/// use ruststream_runtime_compio::CompioClock;
///
/// # fn main() -> std::io::Result<()> {
/// Runtime::new()?.block_on(async {
///     let clock = CompioClock;
///     let started = clock.now();
///     clock.sleep(Duration::from_millis(5)).await;
///     assert!(clock.now() - started >= Duration::from_millis(5));
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CompioClock;

impl CompioClock {
    /// The time on the clock: virtual where the thread holds a lease.
    #[inline]
    pub(crate) fn instant() -> Instant {
        #[cfg(feature = "testing")]
        if let Some(now) = virtual_clock::now() {
            return now;
        }
        Instant::now()
    }
}

impl Clock for CompioClock {
    type Instant = Instant;
    type Sleep = Sleep;

    #[inline]
    fn now(&self) -> Instant {
        Self::instant()
    }

    #[inline]
    fn sleep(&self, duration: Duration) -> Sleep {
        time::sleep(duration)
    }

    #[inline]
    fn sleep_until(&self, deadline: Instant) -> Sleep {
        Sleep::until(deadline)
    }
}
