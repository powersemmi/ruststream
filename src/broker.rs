//! The [`Broker`] trait: the entry point of any broker implementation.

use std::{error::Error as StdError, future::Future};

/// A connection to a message broker, owning its lifecycle.
///
/// `Broker` is the entry point of any broker crate (`ruststream-nats`, `ruststream-kafka`, ...).
/// It owns only the connection lifecycle: implementations establish their network connection in
/// [`connect`] and release all resources in [`shutdown`]. Subscribing is described separately by a
/// [`SubscriptionSource`](crate::SubscriptionSource) (or the [`Subscribe`](crate::Subscribe)
/// capability for the by-name case), so a single broker can offer several subscription kinds with
/// different subscriber types (`Redis` pub/sub vs streams vs lists). Publishers are likewise
/// produced by broker-specific constructors and registered on the app.
///
/// `Send + Sync` is required so the router can share the broker handle across tasks.
///
/// # Lazy startup contract
///
/// Implementations MUST be constructible **synchronously**, without performing I/O: expose a plain
/// `new(..)` constructor that only captures configuration (addresses, credentials). All network
/// setup happens in [`connect`], which the runtime calls once at startup, after the synchronous
/// `#[ruststream::app]` builder has run. This is what lets a service be assembled with the app
/// macro regardless of broker. A broker that can only be built by connecting (an `async` "connect
/// and return the handle" constructor) does not satisfy this contract. Each broker also ships a
/// [`SubscriptionSource`](crate::SubscriptionSource) for its subjects, resolved after `connect`.
/// [`conformance::harness::lifecycle`](crate::conformance::harness::lifecycle) checks the whole
/// path: synchronous construction, `connect`, subscribe through the source, deliver, ack, shutdown.
///
/// # Examples
///
/// ```
/// use ruststream::Broker;
///
/// async fn lifecycle<B: Broker>(broker: &B) -> Result<(), B::Error> {
///     broker.connect().await?;
///     broker.shutdown().await
/// }
/// ```
///
/// [`connect`]: Self::connect
/// [`shutdown`]: Self::shutdown
pub trait Broker: Send + Sync {
    /// The error type returned by broker-level operations.
    type Error: StdError + Send + Sync + 'static;

    /// Establishes the connection to the broker. Idempotent: calling multiple times must not
    /// open additional sockets.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker is unreachable, authentication fails, or the
    /// configuration is invalid.
    fn connect(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Closes the broker connection, flushing in-flight publishes and stopping background tasks.
    ///
    /// After a successful `shutdown` the broker handle must not be used again.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the broker rejects the disconnect or a background flush
    /// fails to complete.
    fn shutdown(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
