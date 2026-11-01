//! Handler abstraction and the [`HandlerOutcome`] settlement unit returned to the router.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::{convert::Infallible, fmt, pin::Pin, time::Duration};

use super::context::Context;
use crate::publish_failure::{ErrorHook, Failures};
pub(crate) use ruststream_core::{MaybeSend, MaybeSendSync};

/// A boxed, owned continuation run after a message is settled: what a [`HandlerOutcome`] hands
/// the dispatcher, and what the dispatcher hands the runner's host.
///
/// With the runtime crate's `tokio` feature it is `Send`, because the Tokio runner may poll it on
/// any worker thread. Without it the box carries no `Send`, so a continuation on a device runner
/// may hold the local bus's publisher or an `Rc`.
#[cfg(feature = "tokio")]
#[doc(hidden)]
pub type Continuation = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// See the `tokio` form: without the feature a continuation is not required to be `Send`.
#[cfg(not(feature = "tokio"))]
#[doc(hidden)]
pub type Continuation = Pin<Box<dyn Future<Output = ()> + 'static>>;

/// A future a [`HandlerOutcome`] or a context hook can keep as its continuation: `Send` where the
/// runtime crate's `tokio` feature is on.
///
/// # Examples
///
/// A handler leaves a continuation that publishes a receipt once the ack is through:
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
/// async fn send_receipt(_id: u64) {}
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     // `and_after` takes any `ContinuationFuture`: a plain `async` block is one.
///     HandlerOutcome::ack().and_after(async move { send_receipt(id).await })
/// }
/// # }
/// # fn main() {}
/// ```
#[cfg(feature = "tokio")]
#[diagnostic::on_unimplemented(
    message = "the continuation `{Self}` must be `Send` to run on the Tokio runner",
    label = "this continuation holds a value that is not `Send`",
    note = "a continuation runs on a worker thread of the Tokio runner; keep a non-`Send` value out of it, or build for the embedded runtime alone"
)]
pub trait ContinuationFuture: Future<Output = ()> + Send + 'static {}

#[cfg(feature = "tokio")]
impl<Work: Future<Output = ()> + Send + 'static> ContinuationFuture for Work {}

/// A future a [`HandlerOutcome`] or a context hook can keep as its continuation: any
/// `'static` future where the runtime crate's `tokio` feature is off.
///
/// # Examples
///
/// A handler leaves a continuation that publishes a receipt once the ack is through:
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
/// async fn send_receipt(_id: u64) {}
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     // `and_after` takes any `ContinuationFuture`: a plain `async` block is one.
///     HandlerOutcome::ack().and_after(async move { send_receipt(id).await })
/// }
/// # }
/// # fn main() {}
/// ```
#[cfg(not(feature = "tokio"))]
pub trait ContinuationFuture: Future<Output = ()> + 'static {}

#[cfg(not(feature = "tokio"))]
impl<Work: Future<Output = ()> + 'static> ContinuationFuture for Work {}

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
    outcome: Settlement,
    after: Option<Continuation>,
}

/// How the outcome settles the message: the status alone, or the status with the `.error`
/// continuation of the publish the runtime makes for it. The registered case is boxed, so an
/// outcome without `.error` is no larger than one with nothing to add: the status keeps the
/// room it had.
enum Settlement {
    Plain(HandlerResult),
    Erroring(Box<(HandlerResult, ErrorHook)>),
}

impl Settlement {
    const fn status(&self) -> HandlerResult {
        match self {
            Self::Plain(status) => *status,
            Self::Erroring(registered) => registered.0,
        }
    }
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
            outcome: Settlement::Plain(HandlerResult::Ack),
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
            outcome: Settlement::Plain(HandlerResult::retry()),
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
            outcome: Settlement::Plain(HandlerResult::retry_after(delay)),
            after: None,
        }
    }

    /// Negatively acknowledge the message without asking for redelivery.
    pub const fn drop() -> Self {
        Self {
            outcome: Settlement::Plain(HandlerResult::drop()),
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
    /// With the Tokio runner on, the future is `Send` and runs on a tracked task. On the embedded
    /// runtime alone it carries no `Send`, so it may hold the local bus's publisher or an `Rc`;
    /// the embedded runner's one task polls it, in the one box this call made.
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
        F: ContinuationFuture,
    {
        Self {
            outcome: self.outcome,
            after: Some(Box::pin(fut)),
        }
    }

    /// Reacts to a failed publish the runtime makes for this outcome: the delayed retry copy of a
    /// [`retry_after`](Self::retry_after), and the dead-letter copy the mount's
    /// `max_attempts(..).dead_letter(..)` turns this retry into at the cap.
    ///
    /// The continuation receives the [`Failures`]: one [`Failed`](crate::Failed) per message that
    /// failed to go out, each with its error and the very message the runtime tried to send
    /// (bytes and headers), which another `Out` republishes as it is. It returns nothing and runs
    /// after the delivery is settled, as an [`and_after`](Self::and_after) continuation does; the
    /// handler never runs again because of the failure. Whatever the reaction needs (a client,
    /// state, a counter) the handler moves into the closure. Attaching a second one replaces the
    /// first.
    ///
    /// The closure is one-shot, like an [`and_after`](Self::and_after) continuation, so it may
    /// own what it captures. A single delivery's `Failures` holds one failure. An outcome returned
    /// for a whole batch runs the closure once, with the failure of every element whose copy
    /// failed, in the batch's order; its `and_after` runs once, when at least one element's copy
    /// was published. A batch handler that needs a reaction per element returns one outcome per
    /// element instead.
    ///
    /// Without `.error` a failed copy is logged with the subscription, the destination, the
    /// message type and the error, and is lost. On a mount whose broker retries and dead-letters
    /// natively the runtime publishes nothing, so the continuation never runs; a warning names the
    /// subscription each time such an outcome is returned. With the Tokio runner on, the closure
    /// and its future are `Send`; on the embedded runtime alone they carry no `Send`.
    ///
    /// # Cancel safety
    ///
    /// At-most-once, like [`and_after`](Self::and_after): the message is already settled when the
    /// continuation runs.
    ///
    /// # Examples
    ///
    /// A retry copy that cannot be published is logged with the bytes it carried instead of
    /// being lost silently:
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Payment {
    ///     settled: bool,
    /// }
    ///
    /// #[subscriber("payments")]
    /// async fn reconcile(payment: &Payment) -> HandlerOutcome {
    ///     if payment.settled {
    ///         return HandlerOutcome::ack();
    ///     }
    ///     HandlerOutcome::retry_after(Duration::from_secs(5)).error(async move |failures: Failures| {
    ///         for failed in failures {
    ///             tracing::error!(
    ///                 error = %failed.error(),
    ///                 destination = failed.message().destination(),
    ///                 "the retry is lost",
    ///             );
    ///         }
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    ///
    /// A lost copy goes to a dead-letter destination through an `Out` publisher the handler clones
    /// into the closure (`Out<impl Publisher>`), as it was, with its headers:
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// #[subscriber("orders")]
    /// async fn accept(
    ///     _order: &Order,
    ///     Out(dead): Out<impl Publisher>,
    /// ) -> HandlerOutcome {
    ///     let dead = dead.clone();
    ///     HandlerOutcome::retry_after(Duration::from_secs(5)).error(async move |failures: Failures| {
    ///         for failed in failures {
    ///             let lost = failed.into_message();
    ///             // The republish may fail too; there is nobody left to tell.
    ///             let _ = dead
    ///                 .message(&lost)
    ///                 .to("orders.dead")
    ///                 .with_headers(lost.headers().clone())
    ///                 .publish()
    ///                 .await;
    ///         }
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn error<React, Reaction>(mut self, react: React) -> Self
    where
        React: FnOnce(Failures) -> Reaction + MaybeSend + 'static,
        Reaction: ContinuationFuture,
    {
        let status = self.outcome.status();
        self.outcome = Settlement::Erroring(Box::new((status, ErrorHook::new(react))));
        self
    }

    /// Attaches an `.error` hook the dispatcher already holds, as [`error`](Self::error) does a
    /// closure.
    pub(crate) fn error_hook(mut self, hook: ErrorHook) -> Self {
        let status = self.outcome.status();
        self.outcome = Settlement::Erroring(Box::new((status, hook)));
        self
    }

    /// Whether this outcome acknowledges the message.
    #[must_use]
    pub const fn is_ack(&self) -> bool {
        matches!(self.outcome.status(), HandlerResult::Ack)
    }

    /// Whether this outcome asks for redelivery (an immediate or a delayed retry).
    #[must_use]
    pub const fn is_retry(&self) -> bool {
        matches!(
            self.outcome.status(),
            HandlerResult::Nack { requeue: true } | HandlerResult::NackAfter { .. }
        )
    }

    /// Whether this outcome drops the message (a negative acknowledgement without redelivery).
    #[must_use]
    pub const fn is_drop(&self) -> bool {
        matches!(
            self.outcome.status(),
            HandlerResult::Nack { requeue: false }
        )
    }

    /// The redelivery delay of a [`retry_after`](Self::retry_after) outcome; `None` for every
    /// other status.
    #[must_use]
    pub const fn retry_delay(&self) -> Option<Duration> {
        match self.outcome.status() {
            HandlerResult::NackAfter { delay } => Some(delay),
            HandlerResult::Ack | HandlerResult::Nack { .. } => None,
        }
    }

    /// The status the dispatcher settles the message by.
    #[doc(hidden)]
    #[must_use]
    pub const fn outcome(&self) -> HandlerResult {
        self.outcome.status()
    }

    /// Takes the post-settle continuation out of this settlement, leaving none. The dispatcher
    /// calls this after settling, to spawn the continuation on its tracked task set.
    /// Whether a post-settle continuation is attached.
    pub(crate) const fn has_after(&self) -> bool {
        self.after.is_some()
    }

    /// Attaches an already boxed continuation, as [`and_after`](Self::and_after) does a future.
    pub(crate) fn and_after_boxed(mut self, after: Continuation) -> Self {
        self.after = Some(after);
        self
    }

    /// Takes the outcome apart: its status, its post-settle continuation and its `.error`.
    #[inline]
    pub(crate) fn into_parts(self) -> (HandlerResult, Option<Continuation>, Option<ErrorHook>) {
        match self.outcome {
            Settlement::Plain(status) => (status, self.after, None),
            Settlement::Erroring(registered) => {
                let (status, error) = *registered;
                (status, self.after, Some(error))
            }
        }
    }
}

impl fmt::Debug for HandlerOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerOutcome")
            .field("outcome", &self.outcome.status())
            .field("after", &self.after.is_some())
            .field("error", &matches!(self.outcome, Settlement::Erroring(_)))
            .finish()
    }
}

impl From<HandlerResult> for HandlerOutcome {
    fn from(outcome: HandlerResult) -> Self {
        Self {
            outcome: Settlement::Plain(outcome),
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
pub trait Handler<M: ?Sized, C = (), S = ()>: MaybeSendSync {
    /// Handle one input, with the per-delivery [`Context`] (carrying the broker's typed context
    /// `C` and the shared application state `S`). The returned [`HandlerOutcome`] carries the
    /// status the dispatcher settles by and any post-settle continuation.
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + MaybeSend;
}

impl<M: ?Sized, C, S, F, Fut> Handler<M, C, S> for F
where
    F: Fn(&M, &mut Context<'_, C, S>) -> Fut + MaybeSendSync,
    Fut: Future + MaybeSend,
    Fut::Output: IntoOutcome,
{
    fn handle(
        &self,
        msg: &M,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = HandlerOutcome> + MaybeSend {
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
    ) -> impl Future<Output = HandlerOutcome> + MaybeSend {
        (**self).handle(msg, ctx)
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

    #[test]
    fn an_outcome_with_an_error_continuation_is_no_larger_than_one_without() {
        // Every message pays the size of the outcome it returns, whether or not it uses `.error`.
        assert_eq!(size_of::<HandlerOutcome>(), 32);
        assert_eq!(size_of::<super::Settlement>(), size_of::<HandlerResult>());
    }

    fn assert_outcome(outcome: HandlerOutcome, status: HandlerResult, has_after: bool) {
        let (settled, after, _error) = outcome.into_parts();
        assert_eq!(settled, status);
        assert_eq!(after.is_some(), has_after);
    }
}
