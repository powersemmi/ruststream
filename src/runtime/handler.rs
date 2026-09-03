//! Handler abstraction and the [`HandlerOutcome`] settlement unit returned to the router.

use std::{convert::Infallible, future::Future, pin::Pin, sync::Arc, time::Duration};

use super::context::Context;

/// A boxed, owned continuation run after a message is settled. Private: a [`HandlerOutcome`]
/// hands it to the dispatcher, which spawns it; it never crosses the public API by itself.
type AfterFut = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// The broker status half of a [`HandlerOutcome`]: what the router does with the message after
/// the handler returns. Internal machinery - the policies, the dispatcher's settle match and
/// the test seams read it; user code constructs and inspects [`HandlerOutcome`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum HandlerResult {
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

impl HandlerResult {
    /// Constructor for `Nack { requeue: true }`, mirroring [`HandlerOutcome::retry`].
    #[must_use]
    pub(crate) const fn retry() -> Self {
        Self::Nack { requeue: true }
    }

    /// Constructor for [`NackAfter`](Self::NackAfter), mirroring
    /// [`HandlerOutcome::retry_after`].
    #[must_use]
    pub(crate) const fn retry_after(delay: Duration) -> Self {
        Self::NackAfter { delay }
    }

    /// Constructor for `Nack { requeue: false }`, mirroring [`HandlerOutcome::drop`].
    #[must_use]
    pub(crate) const fn drop() -> Self {
        Self::Nack { requeue: false }
    }
}

/// The settlement of one dispatched message: the broker status the dispatcher acts on, plus an
/// optional post-settle continuation.
///
/// A handler's `Err` side carries one (`Result<(), HandlerOutcome>`, or per element
/// `Vec<HandlerOutcome>` on a page); `Ok` acks. Build one with the short constructors -
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
/// use ruststream::runtime::HandlerOutcome;
///
/// # fn check() -> Result<(), Box<dyn std::error::Error>> {
/// // A plain outcome settles and does nothing else.
/// let plain = HandlerOutcome::ack();
/// assert!(plain.is_ack());
///
/// // Or carry a continuation that runs after the settle.
/// let with_after = HandlerOutcome::drop().and_after(async move { /* cleanup */ });
/// assert!(with_after.is_drop());
/// # Ok(())
/// # }
/// # check().unwrap();
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
    /// use ruststream::runtime::HandlerOutcome;
    ///
    /// # fn check() -> Result<(), Box<dyn std::error::Error>> {
    /// assert!(HandlerOutcome::ack().is_ack());
    /// # Ok(())
    /// # }
    /// # check().unwrap();
    /// ```
    pub const fn ack() -> Self {
        Self {
            outcome: HandlerResult::Ack,
            after: None,
        }
    }

    /// Negatively acknowledge the message, asking the broker to redeliver it.
    pub const fn retry() -> Self {
        Self {
            outcome: HandlerResult::retry(),
            after: None,
        }
    }

    /// Redeliver, but not before `delay` has passed - the not-ready-yet case (a dependency has
    /// not arrived, an upstream is rate-limited), where an immediate redelivery would just spin.
    ///
    /// The delay is a hint, honoured by brokers with native delayed redelivery (`JetStream`
    /// `NAK` with delay); brokers without it fall back to an immediate requeue (see
    /// [`IncomingMessage::nack_after`](crate::IncomingMessage::nack_after)).
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
    /// use ruststream::runtime::HandlerOutcome;
    ///
    /// # fn check() -> Result<(), Box<dyn std::error::Error>> {
    /// let outcome = HandlerOutcome::ack().and_after(async move {
    ///     // runs after this message is acked
    /// });
    /// assert!(outcome.is_ack());
    /// # Ok(())
    /// # }
    /// # check().unwrap();
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
    pub(crate) const fn outcome(&self) -> HandlerResult {
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

impl std::fmt::Debug for HandlerOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
/// use ruststream::IncomingMessage;
/// use ruststream::runtime::{Context, Handler, HandlerOutcome};
///
/// fn assert_handler<M, H>(_: H)
/// where
///     M: IncomingMessage,
///     H: Handler<M>,
/// {
/// }
///
/// fn use_closure<M: IncomingMessage + 'static>() {
///     // A closure may return any shape the outcome conversion accepts.
///     assert_handler::<M, _>(|_msg: &M, _ctx: &mut Context| async { HandlerOutcome::ack() });
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

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
    fn and_after_carries_the_outcome_and_continuation() {
        let outcome = HandlerOutcome::ack().and_after(async {});
        assert_eq!(outcome.outcome(), HandlerResult::Ack);
        assert!(format!("{outcome:?}").contains("after: true"));

        let plain = HandlerOutcome::retry();
        assert_eq!(plain.outcome(), HandlerResult::retry());
        assert!(format!("{plain:?}").contains("after: false"));
    }

    fn assert_outcome(mut outcome: HandlerOutcome, status: HandlerResult, has_after: bool) {
        assert_eq!(outcome.outcome(), status);
        assert_eq!(outcome.take_after().is_some(), has_after);
    }
}
