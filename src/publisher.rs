//! The [`Publisher`] trait and its declaration-side counterpart, [`PublishPolicy`].

use std::{error::Error as StdError, future::Future};

use thiserror::Error;

use crate::{ConnectedBroker, Headers, OutgoingMessage};

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
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publisher",
    note = "for an `Out<impl Publisher, _>` slot, the type comes from the policy attached at \
            the include site: attach one whose live form publishes"
)]
pub trait Publisher: Send + Sync {
    /// The error type returned by [`publish`].
    ///
    /// [`publish`]: Self::publish
    type Error: StdError + Send + Sync + 'static;

    /// Publishes a message to the broker.
    ///
    /// This is the contract a broker implements, and the direct call a broker crate used on its
    /// own - without this one - is written against. Inside a service built on `ruststream` it is
    /// the layer underneath: what a handler sends goes through the publish builder
    /// ([`message`](crate::runtime::PublishExt::message) / [`raw`](crate::runtime::PublishExt::raw)),
    /// which resolves the destination, the codec and the headers and assembles the
    /// [`OutgoingMessage`] itself. Reach for this one where the message is already built: a
    /// publish transform, a middleware, a post-settle hook.
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

    /// The headers this publisher contributes to every message sent through the publish builder,
    /// underneath whatever the call site names.
    ///
    /// `None` by default: a plain broker publisher contributes nothing and the builder starts from
    /// an empty map. A handle that carries an argument for a run of publishes - a partition key, a
    /// tenant, a delivery option the broker expresses as a header - returns it here instead of
    /// stamping it into the message inside [`publish`](Self::publish). The builder then assembles
    /// the outgoing map once: the base first, the publish's own headers written over it key by
    /// key, so the call site wins over the handle.
    ///
    /// The map is borrowed, never rebuilt per publish, so a handle that has one keeps it in its
    /// own state.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "memory")]
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Headers, OutgoingMessage, Publisher};
    ///
    /// // A handle that tags every message it sends, without touching the message itself.
    /// struct Tenanted<P>(P, Headers);
    ///
    /// impl<P: Publisher> Publisher for Tenanted<P> {
    ///     type Error = P::Error;
    ///
    ///     async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
    ///         self.0.publish(msg).await
    ///     }
    ///
    ///     fn base_headers(&self) -> Option<&Headers> {
    ///         Some(&self.1)
    ///     }
    /// }
    ///
    /// let broker = MemoryBroker::new();
    /// let base = [("tenant", "acme")].into_iter().collect();
    /// let publisher = Tenanted(broker.publisher(), base);
    /// publisher.raw(b"{}").to("orders").publish().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn base_headers(&self) -> Option<&Headers> {
        None
    }
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
/// call. The error is the type-erased [`PairError`]: pairing runs once at startup, never on the
/// hot path, and a cross-broker token pairs against a different broker than the scope's, so a
/// broker-typed error could not name one broker anyway.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, PublishPolicy};
///
/// let policy = MemoryPublish; // no broker in sight
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = policy.pair(&connected).await?; // live only past this point
/// publisher.raw(b"{}").to("orders").publish().await?;
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
    /// Returns [`PairError`] when bringing the publisher alive requires broker work and that
    /// work fails (most policies pair infallibly).
    fn pair(self, connected: &C) -> impl Future<Output = Result<Self::Live, PairError>> + Send;
}

/// The error of [`PublishPolicy::pair`]: whatever the broker reported while bringing a publisher
/// alive, type-erased.
///
/// Pairing runs once per publisher at startup (a cold path), and a cross-broker token pairs
/// against a broker other than the including scope's, so the error is erased rather than typed
/// to one broker.
#[derive(Debug, Error)]
#[error("pairing a publisher failed: {0}")]
pub struct PairError(#[source] Box<dyn StdError + Send + Sync>);

impl PairError {
    /// Wraps a broker's pairing failure.
    #[must_use]
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }

    /// Wraps an already-boxed failure, or a plain message.
    #[must_use]
    pub fn from_boxed(source: Box<dyn StdError + Send + Sync>) -> Self {
        Self(source)
    }
}

/// A connected broker that names its plain publish policy, so the runtime can build a default
/// reply publisher when a `publish("dest")` handler is included without an explicit one.
///
/// Implement it alongside [`ConnectedBroker`](crate::ConnectedBroker) when the broker has a
/// publish policy whose default configuration is usable as-is (most are). Brokers whose
/// publishers always need explicit options simply do not implement it, and their users attach a
/// policy at every registration.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # fn demo() {
/// use ruststream::DefaultPublish;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryPublish};
///
/// fn default_policy<C: DefaultPublish>() -> C::Policy {
///     C::Policy::default()
/// }
/// let _: MemoryPublish = default_policy::<ConnectedMemoryBroker>();
/// # }
/// ```
pub trait DefaultPublish: ConnectedBroker {
    /// The broker's plain publish policy, constructible with its defaults.
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
