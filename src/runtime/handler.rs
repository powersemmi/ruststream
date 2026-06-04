//! Handler abstraction and the [`HandlerResult`] enum returned to the router.

use std::{future::Future, sync::Arc};

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
}

impl HandlerResult {
    /// Convenience constructor for `Nack { requeue: true }`.
    #[must_use]
    pub const fn retry() -> Self {
        Self::Nack { requeue: true }
    }

    /// Convenience constructor for `Nack { requeue: false }`.
    #[must_use]
    pub const fn drop() -> Self {
        Self::Nack { requeue: false }
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
/// use ruststream::runtime::{Handler, HandlerResult};
///
/// fn assert_handler<M, H>(_: H)
/// where
///     M: IncomingMessage,
///     H: Handler<M>,
/// {
/// }
///
/// fn use_closure<M: IncomingMessage + 'static>() {
///     assert_handler::<M, _>(|_msg: &M| async { HandlerResult::Ack });
/// }
/// ```
pub trait Handler<M>: Send + Sync {
    /// Handle one input.
    fn handle(&self, msg: &M) -> impl Future<Output = HandlerResult> + Send;
}

impl<M, F, Fut> Handler<M> for F
where
    F: Fn(&M) -> Fut + Send + Sync,
    Fut: Future<Output = HandlerResult> + Send,
{
    fn handle(&self, msg: &M) -> impl Future<Output = HandlerResult> + Send {
        (self)(msg)
    }
}

impl<M, H> Handler<M> for Arc<H>
where
    H: Handler<M>,
{
    fn handle(&self, msg: &M) -> impl Future<Output = HandlerResult> + Send {
        (**self).handle(msg)
    }
}
