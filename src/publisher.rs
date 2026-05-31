//! The [`Publisher`] trait: a handle that produces broker messages.

use std::{error::Error as StdError, future::Future};

use crate::OutgoingMessage;

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
