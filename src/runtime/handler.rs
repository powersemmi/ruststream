//! Handler abstraction and the [`HandlerResult`] enum returned to the router.

use std::{future::Future, sync::Arc, time::Duration};

use super::context::Context;

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

impl HandlerResult {
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

/// Conversion into a [`HandlerResult`], so `#[subscriber]` handlers can return a plain value
/// instead of always constructing one.
///
/// Implemented for [`HandlerResult`] (identity), `()` (always [`Ack`](HandlerResult::Ack)), and
/// `Result<_, E>` (`Ok` acks, `Err` drops).
pub trait IntoHandlerResult {
    /// Converts `self` into the outcome the dispatcher acts on.
    fn into_handler_result(self) -> HandlerResult;
}

impl IntoHandlerResult for HandlerResult {
    fn into_handler_result(self) -> HandlerResult {
        self
    }
}

impl IntoHandlerResult for () {
    fn into_handler_result(self) -> HandlerResult {
        HandlerResult::Ack
    }
}

impl<E> IntoHandlerResult for Result<(), E> {
    fn into_handler_result(self) -> HandlerResult {
        match self {
            Ok(()) => HandlerResult::Ack,
            Err(_) => HandlerResult::drop(),
        }
    }
}

impl<E> IntoHandlerResult for Result<HandlerResult, E> {
    fn into_handler_result(self) -> HandlerResult {
        self.unwrap_or_else(|_| HandlerResult::drop())
    }
}

/// A handler invoked on each input it is given.
///
/// The same trait serves both pipeline levels: a raw delivery (`Handler<M>` where
/// `M: IncomingMessage`) and a decoded value (`Handler<T>`). Implementations are `Send + Sync` so a
/// single handler can be shared across many concurrent inputs.
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
///     assert_handler::<M, _>(|_msg: &M, _ctx: &mut Context| async { HandlerResult::Ack });
/// }
/// ```
pub trait Handler<M>: Send + Sync {
    /// Handle one input, with the per-delivery [`Context`].
    fn handle(&self, msg: &M, ctx: &mut Context) -> impl Future<Output = HandlerResult> + Send;
}

impl<M, F, Fut> Handler<M> for F
where
    F: Fn(&M, &mut Context) -> Fut + Send + Sync,
    Fut: Future<Output = HandlerResult> + Send,
{
    fn handle(&self, msg: &M, ctx: &mut Context) -> impl Future<Output = HandlerResult> + Send {
        (self)(msg, ctx)
    }
}

impl<M, H> Handler<M> for Arc<H>
where
    H: Handler<M>,
{
    fn handle(&self, msg: &M, ctx: &mut Context) -> impl Future<Output = HandlerResult> + Send {
        (**self).handle(msg, ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{HandlerResult, IntoHandlerResult};

    #[test]
    fn convenience_constructors_map_to_variants() {
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
    fn into_handler_result_covers_every_return_shape() {
        assert_eq!(HandlerResult::Ack.into_handler_result(), HandlerResult::Ack);
        assert_eq!(().into_handler_result(), HandlerResult::Ack);
        assert_eq!(Ok::<(), ()>(()).into_handler_result(), HandlerResult::Ack);
        assert_eq!(
            Err::<(), ()>(()).into_handler_result(),
            HandlerResult::drop()
        );
        assert_eq!(
            Ok::<HandlerResult, ()>(HandlerResult::retry()).into_handler_result(),
            HandlerResult::retry()
        );
        assert_eq!(
            Err::<HandlerResult, ()>(()).into_handler_result(),
            HandlerResult::drop()
        );
    }
}
