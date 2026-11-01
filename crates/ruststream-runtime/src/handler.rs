//! Handler abstraction and the [`HandlerOutcome`] settlement unit returned to the router.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::{convert::Infallible, fmt, pin::Pin, time::Duration};

use super::context::Context;

/// A boxed, owned continuation run after a message is settled. Private: a [`HandlerOutcome`]
/// hands it to the dispatcher, which spawns it; it never crosses the public API by itself.
type AfterFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// The broker status half of a [`HandlerOutcome`].
///
/// It is what the router does with the message after the handler returns. Internal machinery - the policies, the dispatcher's settle match and
/// the test seams read it; user code constructs and inspects [`HandlerOutcome`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum HandlerResult {
    /// Acknowledge the message; the broker will remove it from the queue.
    Ack,
    /// Negatively acknowledge the message; `requeue = true` asks the broker to redeliver.
    Nack {
        /// Whether the broker should redeliver the message.
        requeue: bool,
    },
    /// Negatively acknowledge the message, asking the broker to redeliver it no sooner than
    /// `delay` from now.
    ///
    /// A broker with native delayed redelivery (`JetStream` `NAK` with delay) honours the delay
    /// itself, through [`Settle::nack_after`](crate::Settle::nack_after); on one
    /// without, the runtime drops the delivery and re-publishes a copy of it to its own source
    /// after the delay.
    NackAfter {
        /// How long the broker should wait before redelivering.
        delay: Duration,
    },
}

impl HandlerResult {
    /// Constructor for `Nack { requeue: true }`, mirroring [`HandlerOutcome::retry`].
    #[must_use]
    pub(crate) const fn retry() -> Self {
        Self::Nack { requeue: true }
    }

    /// Constructor for [`NackAfter`](Self::NackAfter), mirroring
    /// [`HandlerOutcome::retry_after`].
    #[must_use]
    #[doc(hidden)]
    pub const fn retry_after(delay: Duration) -> Self {
        Self::NackAfter { delay }
    }

    /// Constructor for `Nack { requeue: false }`, mirroring [`HandlerOutcome::drop`].
    #[must_use]
    #[doc(hidden)]
    pub const fn drop() -> Self {
        Self::Nack { requeue: false }
    }
}

/// The settlement of one dispatched message: the broker status the dispatcher acts on, plus an
/// optional post-settle continuation.
///
/// A handler's `Err` side carries one (`Result<(), HandlerOutcome>`, or per element
/// `Vec<HandlerOutcome>` on a batch); `Ok` acks. Build one with the short constructors -
/// [`ack`](Self::ack), [`retry`](Self::retry), [`retry_after`](Self::retry_after),
/// [`drop`](Self::drop) - and attach follow-up work with [`and_after`](Self::and_after).
///
/// # Cancel safety
///
/// The continuation runs after the message is already settled, so it is at-most-once: a panic or
/// a crash before it completes never redelivers the message. See [`and_after`](Self::and_after).
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Payment {
///     id: u64,
///     settled: bool,
///     valid: bool,
/// }
///
/// async fn release_hold(_id: u64) {}
///
/// #[subscriber("payments")]
/// async fn reconcile(payment: &Payment) -> HandlerOutcome {
///     if !payment.valid {
///         // Never coming right: off the queue for good.
///         return HandlerOutcome::drop();
///     }
///     if !payment.settled {
///         return HandlerOutcome::retry();
///     }
///     // Settled, and the hold is released once the ack is through.
///     let id = payment.id;
///     HandlerOutcome::ack().and_after(async move { release_hold(id).await })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// A batch handler settles each element with its own outcome:
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// /// One database round-trip per batch; entries that are not ready come back alone.
/// #[subscriber("orders")]
/// async fn reconcile(orders: &[Order]) -> Vec<HandlerOutcome> {
///     orders
///         .iter()
///         .map(|order| {
///             if order.id == 0 {
///                 HandlerOutcome::retry()
///             } else {
///                 HandlerOutcome::ack()
///             }
///         })
///         .collect()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(reconcile.batch(nonzero!(64)))
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[must_use]
pub struct HandlerOutcome {
    outcome: HandlerResult,
    after: Option<AfterFut>,
}

impl HandlerOutcome {
    /// Acknowledge the message; the broker removes it from the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn accept(order: &Order) -> HandlerOutcome {
    ///     tracing::info!(order.id, "accepted");
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub const fn ack() -> Self {
        Self {
            outcome: HandlerResult::Ack,
            after: None,
        }
    }

    /// Negatively acknowledge the message, asking the broker to redeliver it.
    ///
    /// Under a registration's [`max_attempts`](super::RouterWith::max_attempts) cap this obeys
    /// the cap as `retry_after` does. Where the transport counts its own redeliveries
    /// ([`redelivery_count`](crate::IncomingMessage::redelivery_count)) it stays the broker's
    /// requeue and that count is read; where it counts none the runtime republishes the delivery
    /// at once so the framework's count travels with it, and on that path an immediate retry is
    /// no longer a broker requeue.
    pub const fn retry() -> Self {
        Self {
            outcome: HandlerResult::retry(),
            after: None,
        }
    }

    /// Redeliver, but not before `delay` has passed - the not-ready-yet case (a dependency has
    /// not arrived, an upstream is rate-limited), where an immediate redelivery would just spin.
    ///
    /// A broker with native delayed redelivery (`JetStream` `NAK` with delay) honours the delay
    /// itself, through [`Settle::nack_after`](crate::Settle::nack_after); on one
    /// without, the runtime drops the delivery and re-publishes a copy of it to its own source
    /// after the delay, with the [`RETRY_COUNT_HEADER`](super::RETRY_COUNT_HEADER) incremented.
    /// The publisher that copy leaves through is on every registration already, from the broker's
    /// own [`DefaultPublish`](crate::DefaultPublish) policy, and `.out_retry(policy)` replaces it
    /// (see [`Retry`](super::Retry)). That copy is at-most-once over the delay window: it rides a
    /// detached task, so a process that exits before the timer fires loses it.
    ///
    /// How many times a message comes back, and where it goes when the attempts run out, is the
    /// registration's own declaration: see
    /// [`max_attempts`](super::RouterWith::max_attempts).
    ///
    /// # Examples
    ///
    /// A payment not settled yet comes back after five seconds, at most five times, then goes to a dead-letter destination:
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream::memory::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Payment {
    ///     settled: bool,
    /// }
    ///
    /// #[subscriber("payments")]
    /// async fn reconcile(payment: &Payment) -> HandlerOutcome {
    ///     if !payment.settled {
    ///         return HandlerOutcome::retry_after(Duration::from_secs(5));
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(reconcile)
    ///             .max_attempts(nonzero!(5u32))
    ///             .dead_letter("payments.dead")
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub const fn retry_after(delay: Duration) -> Self {
        Self {
            outcome: HandlerResult::retry_after(delay),
            after: None,
        }
    }

    /// Negatively acknowledge the message without asking for redelivery.
    pub const fn drop() -> Self {
        Self {
            outcome: HandlerResult::drop(),
            after: None,
        }
    }

    /// Attaches a post-settle continuation to this outcome.
    ///
    /// The dispatcher first settles the message by this outcome (ack / nack), then runs `fut` on
    /// a tracked task that graceful shutdown drains. Use it for a non-critical side effect that
    /// must not gate the settlement decision or affect redelivery: a notification, slow
    /// follow-up work, a cache warm-up. The continuation runs after *any* settle, so
    /// `drop().and_after(..)` is valid. Attaching a second continuation replaces the first.
    ///
    /// # Cancel safety
    ///
    /// At-most-once: the message is already settled when `fut` runs, so a continuation that
    /// panics or is lost on a crash never triggers redelivery. Do not put work whose loss must
    /// redeliver the message in here; settle by outcome and let the broker retry instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use ruststream::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// async fn notify_downstream(_id: u64) {}
    ///
    /// #[subscriber("orders")]
    /// async fn accept(order: &Order) -> HandlerOutcome {
    ///     let id = order.id;
    ///     // Runs once the ack is through; its loss never brings the order back.
    ///     HandlerOutcome::ack().and_after(async move { notify_downstream(id).await })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn and_after<F>(self, fut: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            outcome: self.outcome,
            after: Some(Box::pin(fut)),
        }
    }

    /// Whether this outcome acknowledges the message.
    #[must_use]
    pub const fn is_ack(&self) -> bool {
        matches!(self.outcome, HandlerResult::Ack)
    }

    /// Whether this outcome asks for redelivery (an immediate or a delayed retry).
    #[must_use]
    pub const fn is_retry(&self) -> bool {
        matches!(
            self.outcome,
            HandlerResult::Nack { requeue: true } | HandlerResult::NackAfter { .. }
        )
    }

    /// Whether this outcome drops the message (a negative acknowledgement without redelivery).
    #[must_use]
    pub const fn is_drop(&self) -> bool {
        matches!(self.outcome, HandlerResult::Nack { requeue: false })
    }

    /// The redelivery delay of a [`retry_after`](Self::retry_after) outcome; `None` for every
    /// other status.
    #[must_use]
    pub const fn retry_delay(&self) -> Option<Duration> {
        match self.outcome {
            HandlerResult::NackAfter { delay } => Some(delay),
            HandlerResult::Ack | HandlerResult::Nack { .. } => None,
        }
    }

    /// The status the dispatcher settles the message by.
    #[doc(hidden)]
    #[must_use]
    pub const fn outcome(&self) -> HandlerResult {
        self.outcome
    }

    /// Takes the post-settle continuation out of this settlement, leaving none. The dispatcher
    /// calls this after settling, to spawn the continuation on its tracked task set.
    /// Whether a post-settle continuation is attached.
    pub(crate) const fn has_after(&self) -> bool {
        self.after.is_some()
    }

    pub(crate) fn take_after(&mut self) -> Option<AfterFut> {
        self.after.take()
    }
}

impl fmt::Debug for HandlerOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerOutcome")
            .field("outcome", &self.outcome)
            .field("after", &self.after.is_some())
            .finish()
    }
}

impl From<HandlerResult> for HandlerOutcome {
    fn from(outcome: HandlerResult) -> Self {
        Self {
            outcome,
            after: None,
        }
    }
}

impl From<Infallible> for HandlerOutcome {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

/// Conversion into a [`HandlerOutcome`], so `#[subscriber]` handlers can return a plain value
/// instead of always constructing one.
///
/// Implemented for [`HandlerOutcome`] (identity), `()` (always ack), `Result<_, E>` (`Ok` acks,
/// `Err` drops), and `Result<HandlerOutcome, E>` (`Err` drops). Machinery behind the macro
/// expansion; never named in user code.
#[doc(hidden)]
pub trait IntoOutcome {
    /// Converts `self` into the settlement the dispatcher acts on.
    fn into_outcome(self) -> HandlerOutcome;
}

impl IntoOutcome for HandlerOutcome {
    fn into_outcome(self) -> HandlerOutcome {
        self
    }
}

impl IntoOutcome for () {
    fn into_outcome(self) -> HandlerOutcome {
        HandlerOutcome::ack()
    }
}

impl<E> IntoOutcome for Result<(), E> {
    fn into_outcome(self) -> HandlerOutcome {
        match self {
            Ok(()) => HandlerOutcome::ack(),
            Err(_) => HandlerOutcome::drop(),
        }
    }
}

impl<E> IntoOutcome for Result<HandlerOutcome, E> {
    fn into_outcome(self) -> HandlerOutcome {
        self.unwrap_or_else(|_| HandlerOutcome::drop())
    }
}

/// A handler invoked on each input it is given.
///
/// The same trait serves both pipeline levels: a raw delivery (`Handler<M>` where
/// `M: IncomingMessage`) and a decoded value (`Handler<T>`). The input is only ever borrowed, so
/// it may be unsized (`Handler<[u8]>` for a byte-level handler). Implementations are
/// `Send + Sync` so a single handler can be shared across many concurrent inputs.
///
/// # Examples
///
/// Closures implement `Handler` automatically:
///
/// ```
/// use std::time::Instant;
///
/// use ruststream::runtime::{Context, Handler, HandlerOutcome};
///
/// /// A middleware's wrapper: it is a handler itself, and hands each input on to the one inside.
/// struct Timed<H>(H);
///
/// impl<M, C, S, H> Handler<M, C, S> for Timed<H>
/// where
///     M: Send + Sync,
///     C: Send,
///     S: Send + Sync,
///     H: Handler<M, C, S>,
/// {
///     async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
///         let started = Instant::now();
///         let outcome = self.0.handle(msg, ctx).await;
///         tracing::info!(subscription = ctx.name(), elapsed = ?started.elapsed(), "handled");
///         outcome
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot handle `{M}`",
    note = "a handler is a named type with an `impl Handler<{M}>` whose `handle` method returns \
            the settlement, or a closure `|msg: &{M}, ctx: &mut Context| async {{ .. }}` whose \
            future does not borrow its arguments"
)]
pub trait Handler<M: ?Sized, C = (), S = ()>: Send + Sync {
    /// Handle one input, with the per-delivery [`Context`] (carrying the broker's typed context
    /// `C` and the shared application state `S`). The returned [`HandlerOutcome`] carries the
    /// status the dispatcher settles by and any post-settle continuation.
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send;
}

impl<M: ?Sized, C, S, F, Fut> Handler<M, C, S> for F
where
    F: Fn(&M, &mut Context<'_, C, S>) -> Fut + Send + Sync,
    Fut: Future + Send,
    Fut::Output: IntoOutcome,
{
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        // Build the inner future before the async block so it owns the closure's output and the
        // returned future is outcome-valued for any accepted return shape.
        let fut = (self)(msg, ctx);
        async move { fut.await.into_outcome() }
    }
}

impl<M, C, S, H> Handler<M, C, S> for Arc<H>
where
    H: Handler<M, C, S>,
{
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + Send {
        (**self).handle(msg, ctx)
    }
}

/// A handler whose future may stay on the thread that polls it: the local twin of [`Handler`].
///
/// The delivery body runs every handler through this trait, so a runner whose executor polls on
/// one thread (a device's embassy executor, a thread-per-core runtime) hosts handlers that hold
/// `Rc`s or `RefCell`s across an await. An `Arc` of every [`Handler`] is one through a
/// forwarding impl: a runner that spawns onto a multi-thread runtime shares its handler in an
/// `Arc` and bounds on [`Handler`], so the future it spawns is still proven `Send`.
///
/// Import it only where a handler is written for such a runner: with both traits in scope, a
/// call to `handle` on a value that implements both is ambiguous.
///
/// # Examples
///
/// ```
/// use std::cell::Cell;
/// use std::rc::Rc;
///
/// use ruststream::runtime::{Context, HandlerOutcome, LocalHandler};
///
/// /// Counts the readings a device took, in a cell only its own executor touches.
/// struct Readings(Rc<Cell<u32>>);
///
/// impl LocalHandler<[u8]> for Readings {
///     async fn handle(&self, reading: &[u8], _ctx: &mut Context<'_>) -> HandlerOutcome {
///         let taken = Rc::clone(&self.0);
///         if reading.is_empty() {
///             return HandlerOutcome::drop();
///         }
///         taken.set(taken.get() + 1);
///         HandlerOutcome::ack()
///     }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot handle `{M}`",
    note = "a handler is a named type with an `impl Handler<{M}>` (or, for a runner that polls on \
            one thread, an `impl LocalHandler<{M}>`) whose `handle` method returns the settlement"
)]
pub trait LocalHandler<M: ?Sized, C = (), S = ()> {
    /// Handle one input, with the per-delivery [`Context`]; see [`Handler::handle`].
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = HandlerOutcome>;
}

// A forwarding call: the future is the `Handler`'s own, so a runner that knows its handler is a
// `Handler` proves the body's future `Send` through this impl. It is the `Arc` and not every
// `Handler` that forwards, so an adapter generic over its message implements `LocalHandler`
// directly without overlapping a `Handler` impl some other crate could write for it.
impl<M: ?Sized, C, S, H> LocalHandler<M, C, S> for Arc<H>
where
    H: Handler<M, C, S>,
{
    #[inline]
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = HandlerOutcome> {
        <H as Handler<M, C, S>>::handle(&**self, msg, ctx)
    }
}

/// The runner family an adapter calls its inner handler through: [`SendCall`] on a runner that
/// may move the future across threads, [`LocalCall`] on one that polls it where it started.
///
/// One adapter body serves both families: the tokio impls call it with [`SendCall`], so the
/// future is the `Handler`'s own and provably `Send`, and the local twins with [`LocalCall`].
pub(crate) trait CallHandler<Inner, M: ?Sized, C, S> {
    /// Runs `inner` on one input.
    fn handle(
        inner: &Inner,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome>;
}

/// Calls the inner handler as a [`Handler`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct SendCall;

/// Calls the inner handler as a [`LocalHandler`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalCall;

impl<Inner, M, C, S> CallHandler<Inner, M, C, S> for SendCall
where
    Inner: Handler<M, C, S>,
    M: ?Sized,
{
    #[inline]
    fn handle(
        inner: &Inner,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> {
        Handler::handle(inner, msg, ctx)
    }
}

impl<Inner, M, C, S> CallHandler<Inner, M, C, S> for LocalCall
where
    Inner: LocalHandler<M, C, S>,
    M: ?Sized,
{
    #[inline]
    fn handle(
        inner: &Inner,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> {
        LocalHandler::handle(inner, msg, ctx)
    }
}

#[cfg(test)]
mod tests {
    use alloc::format;
    use core::time::Duration;

    use super::{HandlerOutcome, HandlerResult, IntoOutcome};

    #[test]
    fn constructors_map_to_statuses() {
        assert_eq!(HandlerOutcome::ack().outcome(), HandlerResult::Ack);
        assert_eq!(
            HandlerOutcome::retry().outcome(),
            HandlerResult::Nack { requeue: true }
        );
        assert_eq!(
            HandlerOutcome::drop().outcome(),
            HandlerResult::Nack { requeue: false }
        );
        assert_eq!(
            HandlerOutcome::retry_after(Duration::from_secs(2)).outcome(),
            HandlerResult::NackAfter {
                delay: Duration::from_secs(2)
            }
        );
    }

    #[test]
    fn predicates_read_the_status() {
        assert!(HandlerOutcome::ack().is_ack());
        assert!(HandlerOutcome::retry().is_retry());
        assert!(HandlerOutcome::retry_after(Duration::from_secs(1)).is_retry());
        assert!(HandlerOutcome::drop().is_drop());
        assert_eq!(
            HandlerOutcome::retry_after(Duration::from_secs(1)).retry_delay(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(HandlerOutcome::retry().retry_delay(), None);
    }

    #[test]
    fn into_outcome_covers_every_return_shape() {
        // Unit and Result shapes never carry a continuation.
        assert_outcome(().into_outcome(), HandlerResult::Ack, false);
        assert_outcome(Ok::<(), ()>(()).into_outcome(), HandlerResult::Ack, false);
        assert_outcome(
            Err::<(), ()>(()).into_outcome(),
            HandlerResult::drop(),
            false,
        );
        assert_outcome(
            Ok::<HandlerOutcome, ()>(HandlerOutcome::retry()).into_outcome(),
            HandlerResult::retry(),
            false,
        );
        assert_outcome(
            Err::<HandlerOutcome, ()>(()).into_outcome(),
            HandlerResult::drop(),
            false,
        );

        // An outcome (and a Result of one) is the identity and keeps its continuation.
        let with_after = HandlerOutcome::ack().and_after(async {});
        assert_outcome(with_after.into_outcome(), HandlerResult::Ack, true);
        let ok: Result<HandlerOutcome, ()> = Ok(HandlerOutcome::drop().and_after(async {}));
        assert_outcome(ok.into_outcome(), HandlerResult::drop(), true);
        let err: Result<HandlerOutcome, ()> = Err(());
        assert_outcome(err.into_outcome(), HandlerResult::drop(), false);
    }

    #[test]
    fn the_debug_form_says_whether_a_continuation_rides_along() {
        let outcome = HandlerOutcome::ack().and_after(async {});
        assert!(format!("{outcome:?}").contains("after: true"));

        let plain = HandlerOutcome::retry();
        assert!(format!("{plain:?}").contains("after: false"));
    }

    fn assert_outcome(mut outcome: HandlerOutcome, status: HandlerResult, has_after: bool) {
        assert_eq!(outcome.outcome(), status);
        assert_eq!(outcome.take_after().is_some(), has_after);
    }
}
