//! The [`Publisher`] trait and its declaration-side counterpart, [`PublishPolicy`].

use std::{error::Error as StdError, future::Future};

use crate::{ConnectedBroker, OutgoingMessage};

/// A producer that sends messages into the broker.
///
/// `Publisher` is `Send + Sync` so a single instance can be shared across tasks. Implementations
/// are expected to be cheap to clone; expensive shared state (connection pool, batch buffers)
/// should live behind an [`Arc`].
///
/// # Examples
///
/// ```
/// use ruststream::{OutgoingMessage, Publisher};
///
/// async fn emit<P: Publisher>(publisher: &P) -> Result<(), P::Error> {
///     let msg = OutgoingMessage::new("orders.created", b"{}".as_slice());
///     publisher.publish(msg).await
/// }
/// ```
///
/// [`Arc`]: std::sync::Arc
pub trait Publisher: Send + Sync {
    /// The error type returned by [`publish`].
    ///
    /// [`publish`]: Self::publish
    type Error: StdError + Send + Sync + 'static;

    /// Publishes a message to the broker.
    ///
    /// # Cancel safety
    ///
    /// Cancel safety is implementation-defined: most brokers will leave a message in an
    /// indeterminate state if the future is dropped mid-flight. Implementors must document the
    /// guarantees their broker provides.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the message, the connection is lost, or
    /// the operation times out.
    fn publish(
        &self,
        msg: OutgoingMessage<'_>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// The declaration half of a publisher: pure policy, no connection, no publish surface.
///
/// A broker publisher is a bundle of policy (an exchange, a queue timeout, a transactional id)
/// paired with the live connection. `PublishPolicy` is that bundle alone, freely constructible
/// anywhere - before startup, in router definitions, in configuration - because it holds no
/// connection and no broker instance identity. [`pair`](Self::pair) joins it with a
/// [`ConnectedBroker`] witness to produce the live [`Publisher`], so "not connected" is not
/// representable on this path: a publisher exists only after the connection does.
///
/// This is the publish-side mirror of [`SubscriptionSource`](crate::SubscriptionSource). Core
/// combinators ([`TypedPublisher`](crate::runtime::TypedPublisher), transform stacks) compose
/// over a policy leaf exactly as they compose over a live one, and implement `PublishPolicy`
/// functorially: pairing resolves the leaf and keeps the stack, fully monomorphized.
///
/// `pair` is async and fallible because some brokers do real work when a publisher comes alive
/// (a transactional producer initializing its transactions); for most it is a cheap constructor
/// call.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), ruststream::memory::MemoryError> {
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::{Broker, OutgoingMessage, PublishPolicy, Publisher};
///
/// let policy = MemoryPublish; // no broker in sight
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = policy.pair(&connected).await?; // live only past this point
/// publisher
///     .publish(OutgoingMessage::new("orders", b"{}".as_slice()))
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait PublishPolicy<C: ConnectedBroker> {
    /// The live form this policy pairs into: a [`Publisher`] for a leaf policy, or the live
    /// wiring form for a combinator stack (a typed publisher over a policy pairs into the same
    /// typed publisher over the live leaf).
    type Live;

    /// Pairs the policy with a connected broker, producing the live publisher.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectedBroker::Error`] when bringing the publisher alive requires broker work
    /// and that work fails (most policies pair infallibly).
    fn pair(self, connected: &C) -> impl Future<Output = Result<Self::Live, C::Error>> + Send;
}
