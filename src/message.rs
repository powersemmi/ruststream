//! Message types and the [`IncomingMessage`] trait.

use std::{future::Future, time::Duration};

use bytes::Bytes;

use crate::{AckError, HeaderMap, SerializeHeadersError};

/// An owned snapshot of a message as it travels through the framework.
///
/// `RawMessage` is what the runtime hands to type-erased handlers and what test clients return
/// from assertions. Broker-specific subscribers usually expose a richer
/// [`IncomingMessage`] type that wraps the broker's native delivery handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    name: String,
    payload: Bytes,
    headers: HeaderMap,
}

impl RawMessage {
    /// Constructs a new message for the given name and payload, with no headers.
    pub fn new(name: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            name: name.into(),
            payload: payload.into(),
            headers: HeaderMap::new(),
        }
    }

    /// Builder-style setter that replaces the header map.
    #[must_use]
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
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
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Returns a mutable reference to the message headers.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
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
/// use ruststream::{HeaderMap, OutgoingMessage};
///
/// let payload = b"{\"hello\":\"world\"}";
/// let mut headers = HeaderMap::new();
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
    headers: HeaderMap,
}

impl<'a> OutgoingMessage<'a> {
    /// Constructs a new outgoing message for the given name and payload, with no headers.
    #[must_use]
    pub fn new(name: &'a str, payload: &'a [u8]) -> Self {
        Self {
            name,
            payload,
            headers: HeaderMap::new(),
        }
    }

    /// Builder-style setter that replaces the header map.
    #[must_use]
    pub fn with_headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Builder-style setter that serializes a typed contract into the headers, one entry per
    /// field (see [`HeaderMap::insert_typed`]). Entries already present under other names are kept.
    ///
    /// # Errors
    ///
    /// Returns [`SerializeHeadersError`] when the value is not a flat struct or string-keyed map.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::OutgoingMessage;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct ChunkMeta {
    ///     task_id: u64,
    /// }
    ///
    /// let msg = OutgoingMessage::new("chunks.done", b"{}")
    ///     .with_typed_headers(&ChunkMeta { task_id: 7 })?;
    /// assert_eq!(msg.headers().get_str("task_id"), Some("7"));
    /// # Ok::<(), ruststream::SerializeHeadersError>(())
    /// ```
    pub fn with_typed_headers<T: serde::Serialize + ?Sized>(
        mut self,
        value: &T,
    ) -> Result<Self, SerializeHeadersError> {
        self.headers.insert_typed(value)?;
        Ok(self)
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
    pub fn headers(&self) -> &HeaderMap {
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
    fn headers(&self) -> &HeaderMap;

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

    /// Reports whether this transport can honor [`nack_after`](Self::nack_after) natively.
    ///
    /// Defaulted to `false`: a transport without native delayed redelivery cannot hold a
    /// message back for `delay` on its own. The runtime reads this BEFORE settling a
    /// [`retry_after`](crate::runtime::HandlerOutcome::retry_after) outcome: when it returns `true`
    /// the runtime calls [`nack_after`](Self::nack_after) and trusts the broker timer; when it
    /// returns `false` the runtime applies its broker-agnostic deferred-republish fallback
    /// instead, so the delay is never silently dropped. Brokers with native delayed redelivery
    /// (`JetStream` `NAK` with delay, a durable delayed queue) override this to `true` and
    /// override `nack_after`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{AckError, HeaderMap, IncomingMessage};
    ///
    /// struct CoreMessage {
    ///     payload: Vec<u8>,
    ///     headers: HeaderMap,
    /// }
    ///
    /// impl IncomingMessage for CoreMessage {
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    ///     fn headers(&self) -> &HeaderMap {
    ///         &self.headers
    ///     }
    ///     async fn ack(self) -> Result<(), AckError> {
    ///         Ok(())
    ///     }
    ///     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
    ///         Ok(())
    ///     }
    ///     // No native delayed redelivery: keep the default, opting into the runtime fallback.
    /// }
    ///
    /// let msg = CoreMessage { payload: Vec::new(), headers: HeaderMap::new() };
    /// assert!(!msg.supports_nack_after());
    /// ```
    fn supports_nack_after(&self) -> bool {
        false
    }

    /// Negatively acknowledges the message, asking the broker to redeliver it no sooner than
    /// `delay` from now.
    ///
    /// The default returns [`AckError::Unsupported`]: a transport without native delayed
    /// redelivery cannot honor the delay, and the default reports that honestly rather than
    /// silently degrading to an immediate `nack(true)`. The runtime never relies on this default
    /// to honor a delay; it checks [`supports_nack_after`](Self::supports_nack_after) first and
    /// only calls `nack_after` when that is `true`, otherwise running its own deferred-republish
    /// fallback. Brokers with native delayed redelivery override both methods.
    ///
    /// # Errors
    ///
    /// Returns [`AckError::Unsupported`] by default. Overrides return [`AckError`] under the same
    /// conditions as [`nack`](Self::nack).
    fn nack_after(self, delay: Duration) -> impl Future<Output = Result<(), AckError>> + Send
    where
        Self: Sized,
    {
        let _ = delay;
        std::future::ready(Err(AckError::Unsupported))
    }
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
        let mut headers = HeaderMap::new();
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

    #[test]
    fn raw_message_payload_bytes_and_headers_mut() {
        let mut msg = RawMessage::new("n", b"data".as_slice());
        assert_eq!(msg.payload_bytes(), Bytes::from_static(b"data"));
        msg.headers_mut().insert("k", "v");
        assert_eq!(msg.headers().get_str("k"), Some("v"));
    }

    #[tokio::test]
    async fn incoming_message_defaults_apply_without_override() {
        use std::future::ready;

        use crate::AckError;

        // A minimal IncomingMessage that overrides nothing optional, pinning the trait defaults
        // (the in-memory and broker impls override them).
        struct Stub {
            payload: Vec<u8>,
            headers: HeaderMap,
        }

        impl IncomingMessage for Stub {
            fn payload(&self) -> &[u8] {
                &self.payload
            }

            fn headers(&self) -> &HeaderMap {
                &self.headers
            }

            fn ack(self) -> impl Future<Output = Result<(), AckError>> {
                ready(Ok(()))
            }

            fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
                ready(Ok(()))
            }
        }

        let stub = Stub {
            payload: b"body".to_vec(),
            headers: HeaderMap::new(),
        };
        assert_eq!(stub.payload(), b"body");
        // The default partition_key is None (no key).
        assert!(stub.partition_key().is_none());
        // The default reports no native delayed redelivery, so the runtime uses its fallback.
        assert!(!stub.supports_nack_after());
        // The default nack_after signals "not honored" rather than silently degrading.
        assert!(matches!(
            stub.nack_after(Duration::from_secs(1)).await,
            Err(AckError::Unsupported)
        ));
    }
}
