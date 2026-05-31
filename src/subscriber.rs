//! The [`Subscriber`] trait: a stream of broker deliveries.

use std::error::Error as StdError;

use futures::Stream;

use crate::IncomingMessage;

/// A consumer attached to one or more broker topics.
///
/// `Subscriber` yields messages via a [`Stream`], so users get back-pressure and integration with
/// the rest of the futures ecosystem for free. Each yielded item is a broker-specific
/// [`IncomingMessage`] that must be acknowledged.
///
/// # Cancel safety
///
/// Polling [`stream`] is cancel-safe: dropping the returned `Stream` between polls is allowed.
/// Implementations must not buffer partially-decoded frames in `&self` state; if buffering is
/// required, it belongs in `&mut self`.
///
/// [`stream`]: Self::stream
pub trait Subscriber: Send {
    /// The message type yielded by this subscriber.
    type Message: IncomingMessage;

    /// The error type yielded by the stream when delivery fails.
    type Error: StdError + Send + Sync + 'static;

    /// Returns a stream of broker deliveries.
    ///
    /// The stream terminates when the subscription is cancelled or the broker connection closes.
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_;
}
