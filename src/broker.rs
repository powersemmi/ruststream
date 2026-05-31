//! The [`Broker`] trait: the entry point of any broker implementation.

use std::{error::Error as StdError, future::Future};

use crate::{Publisher, Subscriber};

/// A connection to a message broker, exposing typed subscriber and publisher handles.
///
/// `Broker` is the entry point of any broker crate (`ruststream-nats`, `ruststream-kafka`, ...).
/// It owns the lifecycle: implementations must establish their network connection in
/// [`connect`] and release all resources in [`shutdown`].
///
/// `Send + Sync` is required so the router can share the broker handle across tasks.
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
    /// The subscriber type produced by this broker.
    type Subscriber: Subscriber;

    /// The publisher type produced by this broker.
    type Publisher: Publisher;

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
