//! Type-erased publisher, used by the broker-agnostic `retry_after` fallback.
//!
//! The deferred-redelivery fallback (see [`BrokerScope::retry_via`](super::BrokerScope::retry_via))
//! re-publishes a message to its own source subject through a publisher whose concrete type does
//! not matter to the runtime, so it is held erased behind [`ErasedPublisher`]. To share a publisher
//! with handlers, put it in the typed application state instead and read it with
//! [`Context::state`](super::Context::state).

use crate::{Headers, OutgoingMessage, Publisher};

use super::lifecycle::{BoxError, BoxFuture};

/// A publisher with its concrete type and error erased.
///
/// Blanket-implemented for every [`Publisher`], so any broker's publisher can be held as
/// `Arc<dyn ErasedPublisher>` for the `retry_after` fallback.
pub trait ErasedPublisher: Send + Sync {
    /// Publishes `payload` to `name`, with no headers.
    ///
    /// # Errors
    ///
    /// Returns the underlying publisher's error, boxed, if the broker rejects the publish.
    fn publish_bytes<'a>(
        &'a self,
        name: &'a str,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(), BoxError>>;

    /// Publishes `payload` to `name` with `headers`.
    ///
    /// # Errors
    ///
    /// Returns the underlying publisher's error, boxed, if the broker rejects the publish.
    fn publish_message<'a>(
        &'a self,
        name: &'a str,
        payload: &'a [u8],
        headers: &'a Headers,
    ) -> BoxFuture<'a, Result<(), BoxError>>;
}

impl<P: Publisher> ErasedPublisher for P {
    fn publish_bytes<'a>(
        &'a self,
        name: &'a str,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.publish(OutgoingMessage::new(name, payload))
                .await
                .map_err(|e| Box::new(e) as BoxError)
        })
    }

    fn publish_message<'a>(
        &'a self,
        name: &'a str,
        payload: &'a [u8],
        headers: &'a Headers,
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move {
            self.publish(OutgoingMessage::new(name, payload).with_headers(headers.clone()))
                .await
                .map_err(|e| Box::new(e) as BoxError)
        })
    }
}
