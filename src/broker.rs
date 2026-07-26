//! The [`Broker`] / [`ConnectedBroker`] ladder: the entry point of any broker implementation.

use std::{error::Error as StdError, future::Future};

/// An unconnected broker: configuration captured, no I/O performed yet.
///
/// `Broker` is the entry point of any broker crate (`ruststream-nats`, `ruststream-kafka`, ...).
/// The lifecycle is a ladder of consuming transitions, so each state is a distinct type and
/// out-of-order calls do not compile:
///
/// ```text
/// B::new(config)                      unconnected: sync, I/O-free construction
/// broker.connect(self)  -> Connected  the live connection, a typed witness
/// connected.shutdown(self) -> Closed  the terminal witness (may carry diagnostics)
/// ```
///
/// Subscribing is described separately by a [`SubscriptionSource`](crate::SubscriptionSource)
/// (or the [`Subscribe`](crate::Subscribe) capability for the by-name case), resolved against the
/// [`Connected`](Self::Connected) form. Publishers are likewise produced by broker-specific
/// constructors.
///
/// `Send + Sync` is required so the runtime can move the broker across tasks.
///
/// # Lazy startup contract
///
/// Implementations MUST be constructible **synchronously**, without performing I/O: expose a plain
/// `new(..)` constructor that only captures configuration (addresses, credentials). All network
/// setup happens in [`connect`], which the runtime calls once at startup, after the synchronous
/// `#[ruststream::app]` builder has run. This is what lets a service be assembled with the app
/// macro regardless of broker. A broker that can only be built by connecting (an `async` "connect
/// and return the handle" constructor) does not satisfy this contract. Each broker also ships a
/// [`SubscriptionSource`](crate::SubscriptionSource) for its subjects, resolved against the
/// connected form.
/// [`conformance::harness::lifecycle`](crate::conformance::harness::lifecycle) checks the whole
/// ladder: synchronous construction, `connect`, subscribe through the source, deliver, ack,
/// `shutdown`, and the post-shutdown behaviour of aliased handles below.
///
/// # Shutdown is a type, not a flag
///
/// [`ConnectedBroker::shutdown`] consumes the connected broker, so misuse by the owner of the
/// handle (a publish or subscribe after shutdown) is a compile error, not a runtime one. What
/// remains dynamic is transport reality, not contract bookkeeping: handles aliasing the
/// connection (publishers created before shutdown, clones of a shareable broker) MUST surface an
/// error when used after the connection closed, never a silent success against a dead
/// connection. The conformance lifecycle check verifies that aliased-handle behaviour.
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, ConnectedBroker};
///
/// async fn ladder<B: Broker>(broker: B) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///     let connected = broker.connect().await?;
///     let _closed = connected.shutdown().await?;
///     Ok(())
/// }
/// ```
///
/// [`connect`]: Self::connect
pub trait Broker: Send + Sync + Sized {
    /// The error type returned by broker-level operations.
    type Error: StdError + Send + Sync + 'static;

    /// The connected form of this broker: the typed witness that [`connect`](Self::connect)
    /// succeeded.
    type Connected: ConnectedBroker;

    /// Establishes the connection to the broker, consuming the unconnected form.
    ///
    /// The returned [`Connected`](Self::Connected) value is the only way to reach the
    /// connection-bound surface (subscriptions, and for shareable brokers the live handles), so
    /// "not connected" is not representable on the owner's path.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker is unreachable, authentication fails, or the
    /// configuration is invalid.
    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send;
}

/// A connected broker: the typed witness of a live connection.
///
/// Obtained only from [`Broker::connect`]. The `'static` supertrait keeps the connected form an
/// owned value the runtime can hold and erase; a connected broker borrowing from elsewhere could
/// not travel through startup.
///
/// # Examples
///
/// ```
/// use ruststream::ConnectedBroker;
///
/// async fn stop<C: ConnectedBroker>(connected: C) -> Result<C::Closed, C::Error> {
///     connected.shutdown().await
/// }
/// ```
pub trait ConnectedBroker: Send + Sync + Sized + 'static {
    /// The error type returned by connected-broker operations.
    type Error: StdError + Send + Sync + 'static;

    /// The terminal witness returned by [`shutdown`](Self::shutdown).
    ///
    /// A closed broker has no publish or subscribe surface; the witness may carry teardown
    /// diagnostics (flush results, drop counts) as plain data.
    type Closed: Send;

    /// Closes the broker connection, flushing in-flight publishes and stopping background tasks,
    /// consuming the connected form.
    ///
    /// Consuming `self` makes a second shutdown, or any operation after shutdown, a compile
    /// error for the owner of the handle. Aliased handles (publishers handed out earlier,
    /// clones of a shareable broker) surface the transport's own error when used afterwards;
    /// see the [`Broker`] contract.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the disconnect or a background flush
    /// fails to complete.
    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send;
}

/// Shorthand for a broker's connected form, so bounds read
/// `S: SubscriptionSource<Connected<B>>` instead of spelling the associated type projection.
pub type Connected<B> = <B as Broker>::Connected;
