//! What a runner lends the shared delivery body: its clock, the wait on the app's shutdown, the
//! way a loop reports a failure, and where the work a delivery leaves behind runs.
//!
//! The delivery body (decode, middleware, the handler, the reply, the settlement) is the same on
//! every runtime. What differs is where it runs: who spawns it, which clock its timers read, and
//! how a parked loop learns that the app is stopping. A runner crate implements these traits for
//! its runtime and spawns the body; the body is generic over them, so a timer is the runtime's
//! own future and a shutdown check is the runtime's own flag, with nothing boxed between them.
//! A fail-fast goes through the object-safe [`Halt`], which a delivery's context holds; the work
//! a delivery leaves behind (a continuation, a deferred copy) goes to the runner's [`Detach`],
//! which the loop carries as a type parameter.

use core::time::Duration;

#[cfg(feature = "alloc")]
use crate::HeaderMap;
#[cfg(feature = "alloc")]
use crate::lifecycle::BoxError;

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

/// What a running body reports to the app it runs in: a failure that stops the service, and a
/// subscription whose stream ended.
///
/// Object-safe, so a delivery's context holds it behind a reference without a type parameter of
/// its own; it is called only on a fail-fast and at the end of a stream, never per delivery. The
/// runner implements it on its shutdown.
#[doc(hidden)]
pub trait Halt: Sync {
    /// Records `reason` against `subscription` (the first one recorded wins) and starts the
    /// service's teardown.
    fn fail(&self, subscription: &str, reason: &str);

    /// Reports that the stream of `subscription` ended: nothing more where the service is
    /// stopping, the app's configured reaction otherwise.
    fn stream_ended(&self, subscription: &str);
}

/// Work a delivery leaves behind, handed to the runner that runs it: an `and_after`
/// continuation, a post-settle hook, the timer of a deferred retry copy.
///
/// The runner tracks what it is handed, so a graceful shutdown waits for it. The body reaches it
/// by static dispatch: the work is the caller's own future, spawned as it is, and the delayed
/// form takes the delay rather than a timer, so the runner arms it on its own clock.
#[doc(hidden)]
pub trait Detach: Send + Sync {
    /// Runs `work` beside the delivery that left it.
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static;

    /// Runs `work` once `delay` has passed on the runner's clock.
    ///
    /// The body calls it only after a [`reserve`](Self::reserve) that returned `true`, and the
    /// call takes that reservation.
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static;

    /// Reserves room for one delayed piece of work, before the delivery it copies is settled.
    ///
    /// A runner that keeps its delayed work in a store of fixed capacity answers `false` when
    /// the store is full, and the body requeues the delivery instead of dropping it for a copy
    /// it could not keep. A runner with room for any amount keeps the default, `true`.
    fn reserve(&self) -> bool {
        true
    }

    /// Gives back a reservation the body did not use: the settle it was made for failed.
    fn release(&self) {}
}

/// Where the delivery body hands the work a delivery leaves behind, on a runner of either
/// family: every [`Detach`] is one, and a [`LocalDetach`] is one through [`OnOneThread`].
///
/// The body's own leftovers (an `and_after` continuation, a post-settle hook) are `Send`, so a
/// one-thread runner takes them as they are. Machinery; never named directly.
#[doc(hidden)]
pub trait HostDetach {
    /// Whether the runner keeps a mount's retry copies in a store the mount sizes itself: see
    /// [`LocalDetach::STORES_COPIES`].
    const STORES_COPIES: bool = false;

    /// Runs `work` beside the delivery that left it.
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static;

    /// Runs `work` once `delay` has passed on the runner's clock.
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static;

    /// Takes room for one delayed copy; `false` when the host has none left.
    fn reserve(&self) -> bool;

    /// Gives back room taken by [`reserve`](Self::reserve) for a copy that was not handed over.
    fn release(&self);

    /// The store of retry copies this host keeps in place: [`NoKept`] where the copies leave
    /// through the subscription's erased retry publisher instead.
    #[cfg(feature = "alloc")]
    type Kept: KeptCopies;

    /// The store of retry copies this host keeps in place, if it keeps any.
    #[cfg(feature = "alloc")]
    fn kept(&self) -> Option<&Self::Kept>;
}

impl<Host: Detach> HostDetach for Host {
    #[inline]
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        Detach::detach(self, work);
    }

    #[inline]
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        Detach::detach_after(self, delay, work);
    }

    #[inline]
    fn reserve(&self) -> bool {
        Detach::reserve(self)
    }

    #[inline]
    fn release(&self) {
        Detach::release(self);
    }

    #[cfg(feature = "alloc")]
    type Kept = NoKept;

    #[cfg(feature = "alloc")]
    #[inline]
    fn kept(&self) -> Option<&NoKept> {
        None
    }
}

/// A [`LocalDetach`] lent to the delivery body: a one-thread runner's host, taking the body's
/// leftovers into its own storage. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct OnOneThread<'host, Host>(pub &'host Host);

impl<Host: LocalDetach> HostDetach for OnOneThread<'_, Host> {
    const STORES_COPIES: bool = Host::STORES_COPIES;

    #[inline]
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        LocalDetach::detach(self.0, work);
    }

    #[inline]
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        LocalDetach::detach_after(self.0, delay, work);
    }

    #[inline]
    fn reserve(&self) -> bool {
        LocalDetach::reserve(self.0)
    }

    #[inline]
    fn release(&self) {
        LocalDetach::release(self.0);
    }

    #[cfg(feature = "alloc")]
    type Kept = NoKept;

    #[cfg(feature = "alloc")]
    #[inline]
    fn kept(&self) -> Option<&NoKept> {
        None
    }
}

/// A [`LocalDetach`] lent to the delivery body together with a store of retry copies kept in
/// place: the host of a registration that declared room for its delayed copies. Its copies
/// wait in `Kept`'s records rather than as detached work. Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
#[derive(Debug)]
pub struct Keeping<'host, Host, Kept>(pub &'host Host, pub &'host Kept);

#[cfg(feature = "alloc")]
impl<Host: LocalDetach, Kept: KeptCopies> HostDetach for Keeping<'_, Host, Kept> {
    const STORES_COPIES: bool = Host::STORES_COPIES;

    #[inline]
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        LocalDetach::detach(self.0, work);
    }

    #[inline]
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + Send + 'static,
    {
        LocalDetach::detach_after(self.0, delay, work);
    }

    #[inline]
    fn reserve(&self) -> bool {
        LocalDetach::reserve(self.0)
    }

    #[inline]
    fn release(&self) {
        LocalDetach::release(self.0);
    }

    type Kept = Kept;

    #[inline]
    fn kept(&self) -> Option<&Kept> {
        Some(self.1)
    }
}

/// Retry copies kept in place: a fixed number of records, each holding one copy's bytes and
/// headers until it is published, by the store's own driver once it is due or by the delivery
/// body at once.
///
/// The body fills a record before it settles the delivery the record copies, so a store with
/// every record taken has the delivery requeued instead of dropped. A record reuses its buffer
/// from one copy to the next. Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub trait KeptCopies {
    /// Takes a free record for one copy; `None` when every record holds one.
    fn take_record(&self) -> Option<usize>;

    /// Writes the copy into `record`, taken by [`take_record`](Self::take_record).
    fn fill(&self, record: usize, payload: &[u8], headers: HeaderMap);

    /// Publishes the copy in `record` to the subscription once `delay` has passed, and frees
    /// the record then.
    fn arm(&self, record: usize, delay: Duration);

    /// Publishes the copy in `record` now, to `destination` or, without one, to the
    /// subscription, and frees the record.
    ///
    /// # Errors
    ///
    /// What the publisher returned; the copy is lost.
    fn send_now(
        &self,
        record: usize,
        destination: Option<&str>,
    ) -> impl Future<Output = Result<(), BoxError>>;

    /// Frees `record` without publishing what it holds: the settle it was taken for failed.
    fn free(&self, record: usize);
}

/// The store of a host that keeps no retry copy in place: there is none.
#[cfg(feature = "alloc")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub enum NoKept {}

// No value of the type exists, so no method is ever called: each body is the empty match that
// proves it, the idiom `core::convert::Infallible`'s own impls use.
#[cfg(feature = "alloc")]
#[allow(clippy::uninhabited_references, clippy::unused_async_trait_impl)]
impl KeptCopies for NoKept {
    fn take_record(&self) -> Option<usize> {
        match *self {}
    }

    fn fill(&self, _record: usize, _payload: &[u8], _headers: HeaderMap) {
        match *self {}
    }

    fn arm(&self, _record: usize, _delay: Duration) {
        match *self {}
    }

    async fn send_now(&self, _record: usize, _destination: Option<&str>) -> Result<(), BoxError> {
        match *self {}
    }

    fn free(&self, _record: usize) {
        match *self {}
    }
}

/// Work a delivery leaves behind on a runner that polls every delivery on one thread: the
/// [`Detach`] of the local family, whose work need not be `Send`.
///
/// A runner that hosts the whole app in one task (embassy's) keeps what it is handed in storage
/// of its own and polls it from that task, so the work may hold the local broker's `Rc`s and its
/// `!Send` publishers. A store of fixed capacity answers [`reserve`](Self::reserve) with `false`
/// when it is full, and the body requeues the delivery instead of dropping it for a copy it could
/// not keep.
#[doc(hidden)]
pub trait LocalDetach {
    /// Whether this runner keeps a mount's retry copies in a store the mount sizes itself, so a
    /// delayed retry on a mount without one names the step that adds it.
    const STORES_COPIES: bool = false;

    /// Runs `work` beside the delivery that left it.
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + 'static;

    /// Runs `work` once `delay` has passed on the runner's clock.
    ///
    /// The body calls it only after a [`reserve`](Self::reserve) that returned `true`, and the
    /// call takes that reservation.
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + 'static;

    /// Reserves room for one delayed piece of work, before the delivery it copies is settled.
    ///
    /// A runner with room for any amount keeps the default, `true`.
    fn reserve(&self) -> bool {
        true
    }

    /// Gives back a reservation the body did not use: the settle it was made for failed.
    fn release(&self) {}
}
