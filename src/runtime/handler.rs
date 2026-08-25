//! Handler abstraction, the [`HandlerResult`] decision enum, and the [`Settle`] settlement unit
//! returned to the router.

use std::{convert::Infallible, future::Future, pin::Pin, sync::Arc, time::Duration};

use super::context::Context;

/// A boxed, owned continuation run after a message is settled. Private: a [`Settle`] hands it to
/// the dispatcher, which spawns it; it never crosses the public API by itself.
type AfterFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// What the router should do with the message after the handler returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    /// The delay is a hint, honoured by brokers with native delayed redelivery (`JetStream`
    /// `NAK` with delay); brokers without it fall back to an immediate requeue (see
    /// [`IncomingMessage::nack_after`](crate::IncomingMessage::nack_after)).
    NackAfter {
        /// How long the broker should wait before redelivering.
        delay: Duration,
    },
}

impl From<Infallible> for HandlerResult {
    fn from(never: Infallible) -> Self {
        match never {}
    }
}

impl HandlerResult {
    /// Convenience constructor for [`Ack`](Self::Ack), for symmetry with [`retry`](Self::retry),
    /// [`retry_after`](Self::retry_after), and [`drop`](Self::drop) - so a handler reads the same
    /// whether it acks or nacks, and so [`and_after`](Self::and_after) chains off it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::HandlerResult;
    ///
    /// # fn check() -> Result<(), Box<dyn std::error::Error>> {
    /// assert_eq!(HandlerResult::ack(), HandlerResult::Ack);
    /// # Ok(())
    /// # }
    /// # check().unwrap();
    /// ```
    #[must_use]
    pub const fn ack() -> Self {
        Self::Ack
    }

    /// Attaches a post-settle continuation to this outcome, producing a [`Settle`].
    ///
    /// The dispatcher first settles the message by this outcome (ack / nack), then runs `fut` on a
    /// tracked task that graceful shutdown drains. Use it for a non-critical side effect that must
    /// not gate the settlement decision or affect redelivery: a notification, slow follow-up work,
    /// a cache warm-up. The continuation runs after *any* settle, so `drop().and_after(..)` is
    /// valid.
    ///
    /// # Cancel safety
    ///
    /// At-most-once: the message is already settled when `fut` runs, so a continuation that panics
    /// or is lost on a crash never triggers redelivery. Do not put work whose loss must redeliver
    /// the message in here; settle by outcome and let the broker retry instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::HandlerResult;
    ///
    /// # fn check() -> Result<(), Box<dyn std::error::Error>> {
    /// let settle = HandlerResult::ack().and_after(async move {
    ///     // runs after this message is acked
    /// });
    /// assert_eq!(settle.outcome(), HandlerResult::Ack);
    /// # Ok(())
    /// # }
    /// # check().unwrap();
    /// ```
    pub fn and_after<F>(self, fut: F) -> Settle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Settle {
            outcome: self,
            after: Some(Box::pin(fut)),
        }
    }

    /// Convenience constructor for `Nack { requeue: true }`.
    #[must_use]
    pub const fn retry() -> Self {
        Self::Nack { requeue: true }
    }

    /// Convenience constructor for [`NackAfter`](Self::NackAfter): redeliver, but not before
    /// `delay` has passed - the not-ready-yet case (a dependency has not arrived, an upstream is
    /// rate-limited), where an immediate redelivery would just spin.
    #[must_use]
    pub const fn retry_after(delay: Duration) -> Self {
        Self::NackAfter { delay }
    }

    /// Convenience constructor for `Nack { requeue: false }`.
    #[must_use]
    pub const fn drop() -> Self {
        Self::Nack { requeue: false }
    }
}

/// The settlement of one dispatched message: the [`HandlerResult`] outcome the dispatcher acts on,
/// plus an optional post-settle continuation.
///
/// `Settle` is the universal unit flowing through the handler pipeline. A single handler returns
/// `impl Into<Settle>` and a batch handler returns `Vec<Settle>`; a plain [`HandlerResult`] return
/// still works through [`From<HandlerResult>`](From), which leaves the continuation empty. Build one
/// with a continuation via [`HandlerResult::and_after`].
///
/// # Cancel safety
///
/// The continuation runs after the message is already settled, so it is at-most-once: a panic or a
/// crash before it completes never redelivers the message. See [`HandlerResult::and_after`].
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{HandlerResult, Settle};
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// // A plain outcome converts with no continuation.
/// let plain: Settle = HandlerResult::ack().into();
/// assert_eq!(plain.outcome(), HandlerResult::Ack);
///
/// // Or carry a continuation that runs after the settle.
/// let with_after = HandlerResult::drop().and_after(async move { /* cleanup */ });
/// assert_eq!(with_after.outcome(), HandlerResult::drop());
/// # Ok(())
/// # }
/// # check().unwrap();
/// ```
#[must_use]
pub struct Settle {
    outcome: HandlerResult,
    after: Option<AfterFut>,
}

impl Settle {
    /// The outcome the dispatcher settles the message by.
    #[must_use]
    pub const fn outcome(&self) -> HandlerResult {
        self.outcome
    }

    /// Takes the post-settle continuation out of this settlement, leaving none. The dispatcher
    /// calls this after settling, to spawn the continuation on its tracked task set.
    pub(crate) fn take_after(&mut self) -> Option<AfterFut> {
        self.after.take()
    }
}

impl std::fmt::Debug for Settle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settle")
            .field("outcome", &self.outcome)
            .field("after", &self.after.is_some())
            .finish()
    }
}

impl From<HandlerResult> for Settle {
    fn from(outcome: HandlerResult) -> Self {
        Self {
            outcome,
            after: None,
        }
    }
}

/// Conversion into a [`Settle`], so `#[subscriber]` handlers can return a plain value instead of
/// always constructing one.
///
/// Implemented for [`Settle`] (identity), [`HandlerResult`] (no continuation), `()` (always
/// [`Ack`](HandlerResult::Ack)), `Result<_, E>` (`Ok` acks, `Err` drops), and `Result<Settle, E>`
/// / `Result<HandlerResult, E>` (`Err` drops).
pub trait IntoSettle {
    /// Converts `self` into the settlement the dispatcher acts on.
    fn into_settle(self) -> Settle;
}

impl IntoSettle for Settle {
    fn into_settle(self) -> Settle {
        self
    }
}

impl IntoSettle for HandlerResult {
    fn into_settle(self) -> Settle {
        self.into()
    }
}

impl IntoSettle for () {
    fn into_settle(self) -> Settle {
        HandlerResult::Ack.into()
    }
}

impl<E> IntoSettle for Result<(), E> {
    fn into_settle(self) -> Settle {
        match self {
            Ok(()) => HandlerResult::Ack,
            Err(_) => HandlerResult::drop(),
        }
        .into()
    }
}

impl<E> IntoSettle for Result<HandlerResult, E> {
    fn into_settle(self) -> Settle {
        self.unwrap_or_else(|_| HandlerResult::drop()).into()
    }
}

impl<E> IntoSettle for Result<Settle, E> {
    fn into_settle(self) -> Settle {
        self.unwrap_or_else(|_| HandlerResult::drop().into())
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
/// use ruststream::IncomingMessage;
/// use ruststream::runtime::{Context, Handler, HandlerResult};
///
/// fn assert_handler<M, H>(_: H)
/// where
///     M: IncomingMessage,
///     H: Handler<M>,
/// {
/// }
///
/// fn use_closure<M: IncomingMessage + 'static>() {
///     // A closure may return any `Into<Settle>`, including a bare `HandlerResult`.
///     assert_handler::<M, _>(|_msg: &M, _ctx: &mut Context| async { HandlerResult::Ack });
/// }
/// ```
pub trait Handler<M: ?Sized, C = (), S = ()>: Send + Sync {
    /// Handle one input, with the per-delivery [`Context`] (carrying the broker's typed context
    /// `C` and the shared application state `S`). The returned [`Settle`] carries the outcome the
    /// dispatcher settles by and any post-settle continuation.
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = Settle> + Send;
}

impl<M: ?Sized, C, S, F, Fut> Handler<M, C, S> for F
where
    F: Fn(&M, &mut Context<'_, C, S>) -> Fut + Send + Sync,
    Fut: Future + Send,
    Fut::Output: IntoSettle,
{
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = Settle> + Send {
        // Build the inner future before the async block so it owns the closure's output and the
        // returned future is `Settle`-valued for any `Into<Settle>` return shape.
        let fut = (self)(msg, ctx);
        async move { fut.await.into_settle() }
    }
}

impl<M, C, S, H> Handler<M, C, S> for Arc<H>
where
    H: Handler<M, C, S>,
{
    fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> impl Future<Output = Settle> + Send {
        (**self).handle(msg, ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{HandlerResult, IntoSettle, Settle};

    #[test]
    fn convenience_constructors_map_to_variants() {
        assert_eq!(HandlerResult::ack(), HandlerResult::Ack);
        assert_eq!(
            HandlerResult::retry(),
            HandlerResult::Nack { requeue: true }
        );
        assert_eq!(
            HandlerResult::drop(),
            HandlerResult::Nack { requeue: false }
        );
        assert_eq!(
            HandlerResult::retry_after(Duration::from_secs(2)),
            HandlerResult::NackAfter {
                delay: Duration::from_secs(2)
            }
        );
    }

    #[test]
    fn into_settle_covers_every_return_shape() {
        // Bare outcomes and unit / Result shapes never carry a continuation.
        assert_outcome(HandlerResult::Ack.into_settle(), HandlerResult::Ack, false);
        assert_outcome(().into_settle(), HandlerResult::Ack, false);
        assert_outcome(Ok::<(), ()>(()).into_settle(), HandlerResult::Ack, false);
        assert_outcome(
            Err::<(), ()>(()).into_settle(),
            HandlerResult::drop(),
            false,
        );
        assert_outcome(
            Ok::<HandlerResult, ()>(HandlerResult::retry()).into_settle(),
            HandlerResult::retry(),
            false,
        );
        assert_outcome(
            Err::<HandlerResult, ()>(()).into_settle(),
            HandlerResult::drop(),
            false,
        );

        // A Settle (and a Result<Settle, E>) is the identity and keeps its continuation.
        let with_after = HandlerResult::ack().and_after(async {});
        assert_outcome(with_after.into_settle(), HandlerResult::Ack, true);
        let ok: Result<Settle, ()> = Ok(HandlerResult::drop().and_after(async {}));
        assert_outcome(ok.into_settle(), HandlerResult::drop(), true);
        let err: Result<Settle, ()> = Err(());
        assert_outcome(err.into_settle(), HandlerResult::drop(), false);
    }

    #[test]
    fn and_after_carries_the_outcome_and_continuation() {
        let settle = HandlerResult::ack().and_after(async {});
        assert_eq!(settle.outcome(), HandlerResult::Ack);
        assert!(format!("{settle:?}").contains("after: true"));

        let plain: Settle = HandlerResult::retry().into();
        assert_eq!(plain.outcome(), HandlerResult::retry());
        assert!(format!("{plain:?}").contains("after: false"));
    }

    fn assert_outcome(mut settle: Settle, outcome: HandlerResult, has_after: bool) {
        assert_eq!(settle.outcome(), outcome);
        assert_eq!(settle.take_after().is_some(), has_after);
    }
}
