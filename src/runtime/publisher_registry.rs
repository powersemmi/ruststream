//! Type-erased publishers, registered by compile-time key on [`RustStream`](super::RustStream).
//!
//! A handler subscribed on one broker may need to publish to another (a different connection, or a
//! different broker entirely). Publishers of different brokers have different types, so the
//! registry stores them erased behind [`ErasedPublisher`], keyed by a zero-sized
//! [`PublisherKey`]. Resolve one in a broker scope with
//! [`BrokerScope::publisher`](super::BrokerScope::publisher) or in a handler with
//! [`Context::publisher`](super::Context::publisher).

use crate::{Headers, OutgoingMessage, Publisher};

use super::lifecycle::{BoxError, BoxFuture};

/// A compile-time key identifying a named publisher, in place of a string name.
///
/// Each key is a distinct zero-sized type, so a misspelled key is a compile error (an undeclared
/// identifier) where a misspelled string name was only a runtime `None`, and the key is unique by
/// construction (correct for cross-broker publishing). Declare one with
/// [`publisher_key!`](crate::publisher_key), bind it to a publisher at build time with
/// [`RustStream::publisher`](super::RustStream::publisher), and resolve it from a handler with
/// [`Context::publisher`](super::Context::publisher).
///
/// # Examples
///
/// ```
/// use ruststream::publisher_key;
/// use ruststream::runtime::PublisherKey;
///
/// publisher_key!(pub Orders);
/// assert_eq!(Orders::NAME, "Orders");
/// ```
pub trait PublisherKey: 'static {
    /// A human-readable label (the key's identifier), for diagnostics.
    const NAME: &'static str;
}

/// Declares a [`PublisherKey`]: a zero-sized type usable as a publisher key at both registration
/// (`RustStream::publisher(KEY, p)`) and resolution (`ctx.publisher(KEY)`).
///
/// The key is a distinct type, so referencing an undeclared one is a compile error. Group keys in a
/// shared module so the registration and the handler import the same type.
///
/// # Examples
///
/// ```
/// use ruststream::publisher_key;
///
/// publisher_key!(
///     /// Outbound order events.
///     pub Orders
/// );
/// publisher_key!(pub(crate) Payments);
/// ```
#[macro_export]
macro_rules! publisher_key {
    ($(#[$meta:meta])* $vis:vis $name:ident) => {
        $(#[$meta])*
        #[derive(::core::clone::Clone, ::core::marker::Copy, ::core::fmt::Debug)]
        $vis struct $name;

        impl $crate::runtime::PublisherKey for $name {
            const NAME: &'static str = ::core::stringify!($name);
        }
    };
}

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
