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

#[cfg(feature = "alloc")]
use bytes::Bytes;
use core::time::Duration;
use ruststream_core::{MaybeSend, MaybeSendSync, MaybeSync};

#[cfg(feature = "alloc")]
use alloc::sync::Arc;
#[cfg(feature = "alloc")]
use core::convert::Infallible;

#[cfg(feature = "alloc")]
use crate::handler::Continuation;
#[cfg(feature = "alloc")]
use crate::publish_failure::Reaction;
#[cfg(feature = "alloc")]
use crate::redelivery::{NoRetry, RetryCopies, RetryRoute};
#[cfg(feature = "alloc")]
use crate::{HeaderMap, Settle};

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
pub trait Clock: MaybeSendSync {
    /// A point in time on this clock.
    type Instant: Copy + Ord + MaybeSendSync;

    /// The future a timer of this clock is.
    type Sleep: Future<Output = ()> + MaybeSend;

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
pub trait Halt: MaybeSync {
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
pub trait Detach: MaybeSendSync {
    /// Whether this runner keeps a mount's retry copies in a store the mount sizes itself, so a
    /// delayed retry on a mount without one names the step that adds it.
    const STORES_COPIES: bool = false;

    /// Runs `work` beside the delivery that left it.
    fn detach<Work>(&self, work: Work)
    where
        Work: Future<Output = ()> + MaybeSend + 'static;

    /// Runs a continuation the delivery already boxed: an `and_after` continuation or a
    /// post-settle hook.
    ///
    /// The box is [`Continuation`], `Send` where the runtime crate's `tokio` feature is on. A
    /// runner built on this trait is a multi-thread one, so it is built with that feature, and
    /// it spawns the box as the future it is.
    #[cfg(feature = "alloc")]
    fn detach_boxed(&self, work: Continuation) {
        self.detach(work);
    }

    /// Runs `work` once `delay` has passed on the runner's clock.
    ///
    /// The body calls it only after a [`reserve`](Self::reserve) that returned `true`, and the
    /// call takes that reservation.
    fn detach_after<Work>(&self, delay: Duration, work: Work)
    where
        Work: Future<Output = ()> + MaybeSend + 'static;

    /// Runs the work `make` builds once `delay` has passed on the runner's clock.
    ///
    /// The work is built after the delay, so a runner that holds it in a task of its own holds
    /// the timer and the work one after the other, in the room of the larger. The body calls it
    /// only after a [`reserve`](Self::reserve) that returned `true`, and the call takes that
    /// reservation. The default builds the work now and hands it to
    /// [`detach_after`](Self::detach_after).
    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + MaybeSend + 'static,
        Work: Future<Output = ()> + MaybeSend + 'static,
    {
        self.detach_after(delay, make());
    }

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

/// Where the delivery body hands the work a delivery leaves behind: every [`Detach`] is one, and
/// so is a host with a retry route or a store of copies. Machinery; never named directly.
#[doc(hidden)]
pub trait HostDetach {
    /// Whether the runner keeps a mount's retry copies in a store the mount sizes itself: see
    /// [`Detach::STORES_COPIES`].
    const STORES_COPIES: bool = false;

    /// Runs work the delivery already boxed: an `and_after` continuation or a post-settle hook.
    ///
    /// A runner that keeps its work boxed takes this box as it is, so a continuation costs one
    /// allocation and not a box around a box.
    #[cfg(feature = "alloc")]
    fn detach_boxed(&self, work: Continuation);

    /// Runs the work `make` builds once `delay` has passed on the runner's clock: see
    /// [`Detach::detach_after_with`].
    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + MaybeSend + 'static,
        Work: Future<Output = ()> + MaybeSend + 'static;

    /// Takes room for one delayed copy; `false` when the host has none left.
    fn reserve(&self) -> bool;

    /// Gives back room taken by [`reserve`](Self::reserve) for a copy that was not handed over.
    fn release(&self);

    /// The store of retry copies this host keeps in place: [`NoKept`] where the copies leave
    /// through the subscription's retry publisher instead.
    #[cfg(feature = "alloc")]
    type Kept: KeptCopies;

    /// The store of retry copies this host keeps in place, if it keeps any.
    #[cfg(feature = "alloc")]
    fn kept(&self) -> Option<&Self::Kept>;

    /// The retry publisher of the registration this host serves, as its concrete type:
    /// [`NoRetry`] where the host has none.
    #[cfg(feature = "alloc")]
    type Retry: 'static;

    /// The registration's retry route, if the host carries one.
    #[cfg(feature = "alloc")]
    fn retry(&self) -> Option<&Arc<RetryRoute<Self::Retry>>>;
}

impl<Host: Detach> HostDetach for Host {
    const STORES_COPIES: bool = Host::STORES_COPIES;

    #[cfg(feature = "alloc")]
    #[inline]
    fn detach_boxed(&self, work: Continuation) {
        Detach::detach_boxed(self, work);
    }

    #[inline]
    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + MaybeSend + 'static,
        Work: Future<Output = ()> + MaybeSend + 'static,
    {
        Detach::detach_after_with(self, delay, make);
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

    #[cfg(feature = "alloc")]
    type Retry = NoRetry;

    #[cfg(feature = "alloc")]
    #[inline]
    fn retry(&self) -> Option<&Arc<RetryRoute<NoRetry>>> {
        None
    }
}

/// A host of the multi-thread family for deliveries whose broker context is `Cx`: it and what it
/// carries are shared between threads, so a future holding it can be sent, and it keeps no copy
/// in place. Every [`Detach`]
/// qualifies, and so does one with a retry route ([`Copying`]). Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub trait SendHost<Cx>:
    HostDetach<Kept: PrepareKept<Cx> + MaybeSendSync, Retry: RetryCopies<Cx>> + MaybeSendSync
{
}

#[cfg(feature = "alloc")]
impl<Cx, Host> SendHost<Cx> for Host where
    Host: HostDetach<Kept: PrepareKept<Cx> + MaybeSendSync, Retry: RetryCopies<Cx>> + MaybeSendSync
{
}

/// A [`Detach`] lent to the delivery body together with the retry route of the registration it
/// serves: the host of a multi-thread runner, whose retry copies leave through the registration's
/// concrete retry publisher once their timer has run. Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
#[derive(Debug)]
pub struct Copying<Host, Retry> {
    /// The runner's own handle.
    pub host: Host,
    /// The registration's retry route; `None` where the broker moves a spent delivery itself.
    pub route: Option<Arc<RetryRoute<Retry>>>,
}

#[cfg(feature = "alloc")]
impl<Host, Retry> Copying<Host, Retry> {
    /// The runner's handle `host` with the registration's retry `route`.
    #[must_use]
    pub const fn new(host: Host, route: Option<Arc<RetryRoute<Retry>>>) -> Self {
        Self { host, route }
    }
}

#[cfg(feature = "alloc")]
impl<Host: Detach, Retry: 'static> HostDetach for Copying<Host, Retry> {
    #[inline]
    fn detach_boxed(&self, work: Continuation) {
        Detach::detach_boxed(&self.host, work);
    }

    #[inline]
    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + MaybeSend + 'static,
        Work: Future<Output = ()> + MaybeSend + 'static,
    {
        Detach::detach_after_with(&self.host, delay, make);
    }

    #[inline]
    fn reserve(&self) -> bool {
        Detach::reserve(&self.host)
    }

    #[inline]
    fn release(&self) {
        Detach::release(&self.host);
    }

    type Kept = NoKept;

    #[inline]
    fn kept(&self) -> Option<&NoKept> {
        None
    }

    type Retry = Retry;

    #[inline]
    fn retry(&self) -> Option<&Arc<RetryRoute<Retry>>> {
        self.route.as_ref()
    }
}

/// A [`Detach`] lent to the delivery body together with a store of retry copies kept in
/// place: the host of a registration that declared room for its delayed copies. Its copies
/// wait in `Kept`'s records rather than as detached work. Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
#[derive(Debug)]
pub struct Keeping<'host, Host, Kept>(pub &'host Host, pub &'host Kept);

#[cfg(feature = "alloc")]
impl<Host: Detach, Kept: KeptCopies> HostDetach for Keeping<'_, Host, Kept> {
    const STORES_COPIES: bool = Host::STORES_COPIES;

    #[cfg(feature = "alloc")]
    #[inline]
    fn detach_boxed(&self, work: Continuation) {
        Detach::detach_boxed(self.0, work);
    }

    #[inline]
    fn detach_after_with<Make, Work>(&self, delay: Duration, make: Make)
    where
        Make: FnOnce() -> Work + MaybeSend + 'static,
        Work: Future<Output = ()> + MaybeSend + 'static,
    {
        Detach::detach_after_with(self.0, delay, make);
    }

    #[inline]
    fn reserve(&self) -> bool {
        Detach::reserve(self.0)
    }

    #[inline]
    fn release(&self) {
        Detach::release(self.0);
    }

    type Kept = Kept;

    #[inline]
    fn kept(&self) -> Option<&Kept> {
        Some(self.1)
    }

    type Retry = NoRetry;

    #[inline]
    fn retry(&self) -> Option<&Arc<RetryRoute<NoRetry>>> {
        None
    }
}

/// Retry copies kept in place: a fixed number of records, each holding one copy's bytes and
/// headers until it is published, by the store's own driver once it is due or by the delivery
/// body at once.
///
/// The body fills a record before it settles the delivery the record copies. A copy that would
/// wait for its delay in a store with every record taken is a failed publish like any other
/// ([`NoFreeRecordError`](crate::NoFreeRecordError)): the handler's `.error` continuation gets
/// it, and without one the copy is lost with a warning. A record reuses its buffer from one copy
/// to the next. Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub trait KeptCopies {
    /// A copy that leaves without a record: the mount's answer to a full store. It holds what
    /// the copy is made of until [`send_spilled`](Self::send_spilled) publishes it.
    type Spilled: MaybeSend;

    /// Takes a free record for one copy; `None` when every record holds one.
    fn take_record(&self) -> Option<usize>;

    /// Publishes a copy that took no record, now, to `destination` or, without one, to the
    /// subscription.
    ///
    /// A publish that fails loses the copy and hands the error to the handler's `.error`
    /// continuation in `reaction`, or warns without one; one that succeeds hands on the `.then`
    /// registered beside it. What follows comes back for the caller to run beside the delivery.
    fn send_spilled(
        &self,
        spilled: Self::Spilled,
        destination: Option<&str>,
        reaction: Option<Reaction>,
    ) -> impl Future<Output = Option<Continuation>> + MaybeSend;

    /// A copy that would wait for its delay found every record taken: the failure of a publish
    /// that never started. The error is [`NoFreeRecordError`](crate::NoFreeRecordError); `reaction`
    /// gets it with the copy (`payload` is `Some` exactly when there is a continuation to give it
    /// to), and without one a warning names it. What it makes of the failure
    /// comes back for the caller to run beside the delivery.
    fn full(
        &self,
        payload: Option<Bytes>,
        headers: HeaderMap,
        reaction: Option<Reaction>,
    ) -> Option<Continuation>;

    /// Writes the copy into `record`, taken by [`take_record`](Self::take_record).
    fn fill(&self, record: usize, payload: &[u8], headers: HeaderMap);

    /// Publishes the copy in `record` to the subscription once `delay` has passed, and frees
    /// the record then. A publish that fails then hands its error to `reaction`, which the record
    /// holds until it is published.
    fn arm(&self, record: usize, delay: Duration, reaction: Option<Reaction>);

    /// Publishes the copy in `record` now, to `destination` or, without one, to the
    /// subscription, and frees the record. A publish that fails is handled as in
    /// [`send_spilled`](Self::send_spilled).
    fn send_now(
        &self,
        record: usize,
        destination: Option<&str>,
        reaction: Option<Reaction>,
    ) -> impl Future<Output = Option<Continuation>> + MaybeSend;

    /// Frees `record` without publishing what it holds: the settle it was taken for failed.
    fn free(&self, record: usize);
}

/// The delivery a kept copy is made of, while it is still at hand: the subscription it came
/// through, the message, and how the broker's context for it is built. What the transforms on a
/// kept copy read. Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
#[derive(Debug)]
pub struct KeptDelivery<'a, M, Cx> {
    /// The subscription's name.
    pub subscription: &'a str,
    /// The delivery being copied.
    pub message: &'a M,
    /// Builds the broker's context for the delivery, which only a transform reads.
    pub build_cx: fn(&M) -> Cx,
}

/// A store of kept copies that prepares the copy of a delivery whose broker context is `Cx`:
/// runs the mount site's transforms over a filled record, while the delivery is still at hand.
/// Machinery; never named directly.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub trait PrepareKept<Cx>: KeptCopies {
    /// Prepares the copy in `record`, filled by [`fill`](KeptCopies::fill), bound for
    /// `destination` or, without one, for the subscription.
    fn prepare<M: Settle>(
        &self,
        record: usize,
        destination: Option<&str>,
        delivery: &KeptDelivery<'_, M, Cx>,
    );

    /// Prepares a copy bound for `destination` or, without one, for the subscription, in a
    /// buffer of its own because no record is free; it allocates.
    fn spill<M: Settle>(
        &self,
        payload: &[u8],
        headers: HeaderMap,
        destination: Option<&str>,
        delivery: &KeptDelivery<'_, M, Cx>,
    ) -> Self::Spilled;
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
    type Spilled = Infallible;

    fn take_record(&self) -> Option<usize> {
        match *self {}
    }

    async fn send_spilled(
        &self,
        _spilled: Self::Spilled,
        _destination: Option<&str>,
        _reaction: Option<Reaction>,
    ) -> Option<Continuation> {
        match *self {}
    }

    fn full(
        &self,
        _payload: Option<Bytes>,
        _headers: HeaderMap,
        _reaction: Option<Reaction>,
    ) -> Option<Continuation> {
        match *self {}
    }

    fn fill(&self, _record: usize, _payload: &[u8], _headers: HeaderMap) {
        match *self {}
    }

    fn arm(&self, _record: usize, _delay: Duration, _reaction: Option<Reaction>) {
        match *self {}
    }

    async fn send_now(
        &self,
        _record: usize,
        _destination: Option<&str>,
        _reaction: Option<Reaction>,
    ) -> Option<Continuation> {
        match *self {}
    }

    fn free(&self, _record: usize) {
        match *self {}
    }
}

#[cfg(feature = "alloc")]
#[allow(clippy::uninhabited_references)]
impl<Cx> PrepareKept<Cx> for NoKept {
    fn prepare<M: Settle>(
        &self,
        _record: usize,
        _destination: Option<&str>,
        _delivery: &KeptDelivery<'_, M, Cx>,
    ) {
        match *self {}
    }

    fn spill<M: Settle>(
        &self,
        _payload: &[u8],
        _headers: HeaderMap,
        _destination: Option<&str>,
        _delivery: &KeptDelivery<'_, M, Cx>,
    ) -> Self::Spilled {
        match *self {}
    }
}
