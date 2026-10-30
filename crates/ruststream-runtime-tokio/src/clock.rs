//! The tokio runner's clock.

use core::time::Duration;

use ruststream_runtime::Clock;
use tokio::time::{self, Instant, Sleep};

/// The tokio runner: its clock is tokio's timer wheel.
///
/// Every timer the runner arms for the app (a delayed retry copy, a batch deadline, the
/// teardown's grace period, [`time::sleep`](crate::time::sleep)) is a tokio timer, so a test that
/// pauses tokio's clock moves all of them at once.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::runtime::Clock;
/// use ruststream_runtime_tokio::Tokio;
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// let clock = Tokio;
/// let started = clock.now();
/// clock.sleep(Duration::from_secs(5)).await;
/// assert!(clock.now() - started >= Duration::from_secs(5));
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Tokio;

impl Clock for Tokio {
    type Instant = Instant;
    type Sleep = Sleep;

    #[inline]
    fn now(&self) -> Instant {
        Instant::now()
    }

    #[inline]
    fn sleep(&self, duration: Duration) -> Sleep {
        time::sleep(duration)
    }

    #[inline]
    fn sleep_until(&self, deadline: Instant) -> Sleep {
        time::sleep_until(deadline)
    }
}
