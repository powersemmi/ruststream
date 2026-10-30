//! What a runner lends the shared delivery body: its clock and the wait on the app's shutdown.
//!
//! The delivery body (decode, middleware, the handler, the reply, the settlement) is the same on
//! every runtime. What differs is where it runs: who spawns it, which clock its timers read, and
//! how a parked loop learns that the app is stopping. A runner crate implements these traits for
//! its runtime and spawns the body; the body is generic over them, so a timer is the runtime's
//! own future and a shutdown check is the runtime's own flag, with nothing boxed between them.

use core::time::Duration;

/// A runtime's clock: the time it reads and the timers it arms.
///
/// The timer is an associated type, so a sleep is the runtime's own future, held where the
/// caller holds it: no box and no dynamic call per timer. A test build may implement it over a
/// clock the harness moves by hand, and every timer the body arms follows that clock.
///
/// # Examples
///
/// A runner implements it over its runtime's timers; the tokio runner's clock is the model:
///
/// ```
/// use core::time::Duration;
///
/// use ruststream::runtime::Clock;
///
/// /// A clock over tokio's timers.
/// #[derive(Clone, Copy, Debug, Default)]
/// struct TokioTimers;
///
/// impl Clock for TokioTimers {
///     type Instant = tokio::time::Instant;
///     type Sleep = tokio::time::Sleep;
///
///     fn now(&self) -> Self::Instant {
///         tokio::time::Instant::now()
///     }
///
///     fn sleep(&self, duration: Duration) -> Self::Sleep {
///         tokio::time::sleep(duration)
///     }
///
///     fn sleep_until(&self, deadline: Self::Instant) -> Self::Sleep {
///         tokio::time::sleep_until(deadline)
///     }
/// }
///
/// /// What the body does with any clock: wait out a delay on the runtime's own timer.
/// async fn deferred<Timers: Clock>(clock: &Timers, delay: Duration) {
///     clock.sleep(delay).await;
/// }
///
/// # #[tokio::main(flavor = "current_thread", start_paused = true)]
/// # async fn main() {
/// deferred(&TokioTimers, Duration::from_secs(5)).await;
/// # }
/// ```
pub trait Clock {
    /// A point in time on this clock.
    type Instant: Copy + Ord;

    /// The future a timer of this clock is.
    type Sleep: Future<Output = ()>;

    /// The current time on this clock.
    fn now(&self) -> Self::Instant;

    /// A timer that completes once `duration` has passed.
    fn sleep(&self, duration: Duration) -> Self::Sleep;

    /// A timer that completes at `deadline`, or at once where `deadline` has passed.
    fn sleep_until(&self, deadline: Self::Instant) -> Self::Sleep;
}

/// The app's shutdown as a running loop reads it: a flag checked on every turn and a wait a
/// parked loop parks on.
///
/// The flag is what a busy subscription pays for shutdown, so an implementation answers
/// [`is_cancelled`](Self::is_cancelled) with one relaxed load. The wait is polled only when the
/// subscription has nothing ready, and a loop builds it once and keeps it pinned, so a parked
/// turn registers with the shutdown's waiters once rather than on every iteration.
///
/// # Examples
///
/// A loop that serves until the app stops:
///
/// ```
/// use core::pin::pin;
/// use core::sync::atomic::{AtomicBool, Ordering};
///
/// use futures::future::{self, Either};
/// use ruststream::runtime::ShutdownWait;
/// use tokio::sync::Notify;
///
/// /// A shutdown over a flag and a `Notify`.
/// #[derive(Default)]
/// struct Stop {
///     flag: AtomicBool,
///     notify: Notify,
/// }
///
/// impl Stop {
///     fn stop(&self) {
///         self.flag.store(true, Ordering::Relaxed);
///         self.notify.notify_waiters();
///     }
/// }
///
/// impl ShutdownWait for Stop {
///     type Wait<'a> = tokio::sync::futures::Notified<'a>;
///
///     fn is_cancelled(&self) -> bool {
///         self.flag.load(Ordering::Relaxed)
///     }
///
///     fn cancelled(&self) -> Self::Wait<'_> {
///         self.notify.notified()
///     }
/// }
///
/// async fn serve<Shutdown: ShutdownWait>(shutdown: &Shutdown, mut next: impl AsyncFnMut()) {
///     let mut stopping = pin!(shutdown.cancelled());
///     while !shutdown.is_cancelled() {
///         match future::select(pin!(next()), stopping.as_mut()).await {
///             Either::Left(((), _)) => {}
///             Either::Right(((), _)) => break,
///         }
///     }
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let stop = Stop::default();
/// let mut served = 0;
/// serve(&stop, async || {
///     served += 1;
///     if served == 3 {
///         stop.stop();
///     }
/// })
/// .await;
/// assert_eq!(served, 3);
/// # }
/// ```
pub trait ShutdownWait {
    /// The future [`cancelled`](Self::cancelled) returns.
    type Wait<'a>: Future<Output = ()>
    where
        Self: 'a;

    /// Whether shutdown was signalled.
    fn is_cancelled(&self) -> bool;

    /// Resolves once shutdown is signalled.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the future between polls loses nothing, and a signal raised while
    /// no wait exists is still seen by [`is_cancelled`](Self::is_cancelled).
    fn cancelled(&self) -> Self::Wait<'_>;
}
