//! Type-erased publishers, registered by name on [`RustStream`](super::RustStream).
//!
//! A handler subscribed on one broker may need to publish to another (a different connection, or a
//! different broker entirely). Publishers of different brokers have different types, so the
//! registry stores them erased behind [`ErasedPublisher`], keyed by name. Resolve one in a broker
//! scope with [`BrokerScope::publisher`](super::BrokerScope::publisher).

use crate::{Headers, OutgoingMessage, Publisher};

use super::lifecycle::{BoxError, BoxFuture};

/// A publisher with its concrete type and error erased.
///
/// Blanket-implemented for every [`Publisher`], so any broker's publisher can be registered by
/// name and shared as `Arc<dyn ErasedPublisher>`.
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
