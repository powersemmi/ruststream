//! Message types and the [`IncomingMessage`] trait.

use std::future::Future;

use bytes::Bytes;

use crate::{AckError, Headers};

/// An owned snapshot of a message as it travels through the framework.
///
/// `RawMessage` is what the runtime hands to type-erased handlers and what test clients return
/// from assertions. Broker-specific subscribers usually expose a richer
/// [`IncomingMessage`] type that wraps the broker's native delivery handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    name: String,
    payload: Bytes,
    headers: Headers,
}

impl RawMessage {
    /// Constructs a new message for the given name and payload, with no headers.
    pub fn new(name: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            payload: payload.into(),
            headers: Headers::new(),
        }
    }

    /// Builder-style setter that replaces the header map.
    #[must_use]
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// Returns the name / subject this message was published to.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the message payload as a byte slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns a clone of the payload bytes. Cheap because `Bytes` is reference-counted.
    #[must_use]
    pub fn payload_bytes(&self) -> Bytes {
        self.payload.clone()
    }

    /// Returns a shared reference to the message headers.
    #[must_use]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// Returns a mutable reference to the message headers.
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }
}

/// A message ready to be published, holding borrowed payload and name.
///
/// Borrowed fields let publishers send messages without an allocation when the caller already
/// owns the buffers. Use [`OutgoingMessage::new`] and the builder-style setters to construct.
///
/// # Examples
///
/// ```
/// use ruststream::{Headers, OutgoingMessage};
///
/// let payload = b"{\"hello\":\"world\"}";
/// let mut headers = Headers::new();
/// headers.insert("Content-Type", "application/json");
///
/// let msg = OutgoingMessage::new("orders.created", payload).with_headers(headers);
/// assert_eq!(msg.name(), "orders.created");
/// assert_eq!(msg.payload(), payload);
/// ```
#[derive(Debug, Clone)]
pub struct OutgoingMessage<'a> {
    name: &'a str,
    payload: &'a [u8],
    headers: Headers,
}

impl<'a> OutgoingMessage<'a> {
    /// Constructs a new outgoing message for the given name and payload, with no headers.
    #[must_use]
    pub fn new(name: &'a str, payload: &'a [u8]) -> Self {
        Self {
            name,
            payload,
            headers: Headers::new(),
        }
    }

    /// Builder-style setter that replaces the header map.
    #[must_use]
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// Returns the name / subject this message will be published to.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the payload to be published.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// Returns a shared reference to the headers.
    #[must_use]
    pub fn headers(&self) -> &Headers {
        &self.headers
    }
}

/// A message delivered by a [`Subscriber`].
///
/// Consumers inspect [`payload`] and [`headers`], then must call either [`ack`] or [`nack`]
/// exactly once. The consuming `self` receiver in those methods makes double acknowledgement
/// a compile-time error.
///
/// # Cancel safety
///
/// Implementations must document whether [`ack`] and [`nack`] are cancel-safe. If a future is
/// dropped before completion, the broker may treat the message as still pending, leading to
/// redelivery once the visibility timeout expires.
///
/// [`Subscriber`]: crate::Subscriber
/// [`payload`]: Self::payload
/// [`headers`]: Self::headers
/// [`ack`]: Self::ack
/// [`nack`]: Self::nack
pub trait IncomingMessage: Send + Sync {
    /// Returns the raw payload of the message.
    fn payload(&self) -> &[u8];

    /// Returns the headers attached to the message.
    fn headers(&self) -> &Headers;

    /// Returns the routing key the broker partitioned this message by, or `None` when the
    /// message carries no key.
    ///
    /// Defaulted to `None` so existing implementations keep compiling. Brokers whose messages
    /// implement the [`Partitioned`](crate::Partitioned) capability override this to return the
    /// same key, which lets the runtime preserve per-key ordering in keyed worker lanes
    /// (`workers(n, by_key)`) without a `Partitioned` bound on every dispatch path.
    fn partition_key(&self) -> Option<&[u8]> {
        None
    }

    /// Acknowledges successful processing. Consumes the message handle.
    ///
    /// # Errors
    ///
    /// Returns [`AckError`] when the broker rejects the acknowledgement, the operation times out,
    /// or acknowledgement is not supported by this transport.
    fn ack(self) -> impl Future<Output = Result<(), AckError>> + Send;

    /// Negatively acknowledges the message. When `requeue` is `true` the broker should
    /// redeliver according to its own retry policy; when `false` it should drop or dead-letter
    /// the message.
    ///
    /// # Errors
    ///
    /// Returns [`AckError`] under the same conditions as [`ack`].
    ///
    /// [`ack`]: Self::ack
    fn nack(self, requeue: bool) -> impl Future<Output = Result<(), AckError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_message_construction() {
        let msg = RawMessage::new("name.a", b"payload".as_slice());
        assert_eq!(msg.name(), "name.a");
        assert_eq!(msg.payload(), b"payload");
        assert!(msg.headers().is_empty());
    }

    #[test]
    fn raw_message_with_headers() {
        let mut headers = Headers::new();
        headers.insert("X-Tenant", "acme");

        let msg = RawMessage::new("name.a", Bytes::from_static(b"data")).with_headers(headers);
        assert_eq!(msg.headers().get_str("x-tenant"), Some("acme"));
    }

    #[test]
    fn outgoing_message_holds_borrows() {
        let name = String::from("orders");
        let payload = vec![1u8, 2, 3];

        let msg = OutgoingMessage::new(&name, &payload);
        assert_eq!(msg.name(), "orders");
        assert_eq!(msg.payload(), &[1, 2, 3]);
    }
}
