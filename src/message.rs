//! Message types and the [`IncomingMessage`] trait.

use std::{future::Future, time::Duration};

use bytes::{Bytes, BytesMut};
use bytes_utils::Str;

use crate::codec::CodecError;
use crate::{AckError, HeaderMap, SerializeHeadersError};

/// An owned snapshot of a message as it travels through the framework.
///
/// `RawMessage` is what a broker's publish log and the test harness hand back: a delivery with
/// its settlement handle dropped, kept for assertions. Broker-specific subscribers expose a
/// richer [`IncomingMessage`] type that wraps the broker's native delivery handle, and that is
/// what a handler receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessage {
    name: Str,
    payload: Bytes,
    headers: HeaderMap,
}

impl RawMessage {
    /// Constructs a new message for the given name and payload, with no headers.
    ///
    /// The name is a shared buffer: a broker that already holds the delivery's name as a [`Str`]
    /// hands it over without a copy, and cloning the message copies neither name nor payload.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{Bytes, RawMessage, Str};
    ///
    /// /// A broker crate's in-process transport: what it delivers is also what it reports to the
    /// /// test harness as published, with the name buffer shared rather than copied.
    /// struct PublishLog {
    ///     entries: Vec<RawMessage>,
    /// }
    ///
    /// impl PublishLog {
    ///     fn record(&mut self, name: Str, payload: Bytes) {
    ///         self.entries.push(RawMessage::new(name, payload));
    ///     }
    ///
    ///     fn published(&self, name: &str) -> Vec<RawMessage> {
    ///         self.entries
    ///             .iter()
    ///             .filter(|entry| entry.name() == name)
    ///             .cloned()
    ///             .collect()
    ///     }
    /// }
    /// ```
    pub fn new(name: impl Into<Str>, payload: impl Into<Bytes>) -> Self {
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

/// A message ready to be published: a borrowed destination, a payload in the form its publisher
/// declared, and the header map the publish filled.
///
/// The payload type is the publisher's own declaration ([`Publisher::Payload`]): `&'a [u8]` for a
/// transport that reads the bytes, [`BytesMut`] for one whose client keeps them. It defaults to
/// the lending form, so `OutgoingMessage<'a>` is the message a reading transport receives.
/// [`payload`](Self::payload) reads either, [`into_payload`](Self::into_payload) takes the form
/// itself, and [`into_parts`](Self::into_parts) takes the destination, the payload and the map in
/// one move, nothing copied - what a transport that consumes the message calls.
///
/// [`new`](Self::new) builds one over bytes the caller holds, whichever form the publisher asked
/// for: free where it lends, one copy where it keeps them. A publish that produced the buffer
/// itself - the codec's output - hands it over with [`produced`](Self::produced).
///
/// [`Publisher::Payload`]: crate::Publisher::Payload
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use ruststream::{Bytes, BytesMut, HeaderMap, OutgoingMessage, Publisher, Take};
/// # struct Client;
/// # impl Client {
/// #     async fn send(
/// #         &self,
/// #         _topic: &str,
/// #         _payload: Bytes,
/// #         _headers: HeaderMap,
/// #     ) -> io::Result<()> {
/// #         Ok(())
/// #     }
/// # }
///
/// /// A transport whose client keeps the message: it takes the buffer, so the framework hands it
/// /// over instead of lending it.
/// struct ClientPublisher {
///     client: Client,
/// }
///
/// impl Publisher for ClientPublisher {
///     type Payload = Take;
///     type Error = io::Error;
///     type Options = ();
///
///     async fn publish(
///         &self,
///         msg: OutgoingMessage<'_, BytesMut>,
///         _options: Option<&()>,
///     ) -> io::Result<()> {
///         let (topic, payload, headers) = msg.into_parts();
///         self.client.send(topic, payload.freeze(), headers).await
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct OutgoingMessage<'a, Payload = &'a [u8]> {
    name: &'a str,
    payload: Payload,
    headers: HeaderMap,
}

// Each form is its three fields and nothing on top: no tag, no padding. The promise is checked
// here rather than left to be measured later.
const _: () = assert!(
    size_of::<OutgoingMessage<'static>>()
        == size_of::<&str>() + size_of::<&[u8]>() + size_of::<HeaderMap>()
);
const _: () = assert!(
    size_of::<OutgoingMessage<'static, BytesMut>>()
        == size_of::<&str>() + size_of::<BytesMut>() + size_of::<HeaderMap>()
);

impl<'a, Payload> OutgoingMessage<'a, Payload> {
    /// Constructs a new outgoing message over a payload already in the publisher's form, with no
    /// headers.
    ///
    /// What a stage that rebuilds a message uses, so a buffer already handed over is not
    /// downgraded to a borrow on the way.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{BytesMut, OutgoingMessage, Publisher, Take};
    ///
    /// /// A publisher that files every message under the tenant's namespace. The buffer the
    /// /// publish handed over moves across as it is, never downgraded to a borrow.
    /// struct Namespaced<P> {
    ///     inner: P,
    ///     tenant: String,
    /// }
    ///
    /// impl<P: Publisher<Payload = Take>> Publisher for Namespaced<P> {
    ///     type Payload = Take;
    ///     type Error = P::Error;
    ///     type Options = P::Options;
    ///
    ///     async fn publish(
    ///         &self,
    ///         msg: OutgoingMessage<'_, BytesMut>,
    ///         options: Option<&Self::Options>,
    ///     ) -> Result<(), Self::Error> {
    ///         let name = format!("{}.{}", self.tenant, msg.name());
    ///         let (_, payload, headers) = msg.into_parts();
    ///         let renamed = OutgoingMessage::with_payload(&name, payload).with_headers(headers);
    ///         self.inner.publish(renamed, options).await
    ///     }
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub fn with_payload(name: &'a str, payload: Payload) -> Self {
        Self::assembled(name, payload, HeaderMap::new())
    }

    /// The whole message at once, for a stage that already holds all three parts.
    ///
    /// The builder pair (`with_payload` then `with_headers`) writes the header field twice and
    /// drops the empty map it started from; the publish path assembles enough messages per
    /// delivery for that to be worth avoiding.
    #[inline]
    pub(crate) fn assembled(name: &'a str, payload: Payload, headers: HeaderMap) -> Self {
        Self {
            name,
            payload,
            headers,
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
    /// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
    /// # mod demo {
    /// use std::time::Duration;
    ///
    /// use ruststream::OutgoingMessage;
    /// use ruststream::memory::prelude::*;
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Order {
    ///     id: u64,
    ///     tenant: String,
    /// }
    ///
    /// /// The header contract the pricing service reads off every ask.
    /// #[derive(Serialize)]
    /// struct AskMeta<'a> {
    ///     tenant: &'a str,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// struct Pricing;
    ///
    /// #[subscriber("orders")]
    /// async fn price(
    ///     order: &Order,
    ///     Out(pricing): Out<impl RequestReply, Pricing>,
    /// ) -> HandlerOutcome {
    ///     let id = order.id.to_string();
    ///     let Ok(ask) =
    ///         OutgoingMessage::new("quotes.ask", id.as_bytes()).with_typed_headers(&AskMeta {
    ///             tenant: &order.tenant,
    ///         })
    ///     else {
    ///         return HandlerOutcome::drop();
    ///     };
    ///     match pricing.request(ask, Duration::from_secs(2)).await {
    ///         Ok(_quote) => HandlerOutcome::ack(),
    ///         Err(_) => HandlerOutcome::retry(),
    ///     }
    /// }
    ///
    /// fn app() -> RustStream {
    ///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
    ///         b.include(price).out(Pricing, Request).build();
    ///     })
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    pub fn with_typed_headers<T: serde::Serialize + ?Sized>(
        mut self,
        value: &T,
    ) -> Result<Self, SerializeHeadersError> {
        self.headers.insert_typed(value)?;
        Ok(self)
    }

    /// Returns the name / subject this message will be published to.
    ///
    /// The name is the caller's, not the message's, so it outlives the message: a transport
    /// reads the destination here and then consumes the message for its payload.
    #[inline]
    #[must_use]
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// Returns the payload in the form this publisher declared.
    ///
    /// What a transport calls to send: a lending one gets the slice it reads, a taking one the
    /// buffer it keeps. Publishing takes the message by value, so there is no by-reference
    /// counterpart - a borrow cannot hand a buffer over, so it would have to copy.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io;
    ///
    /// use ruststream::{Bytes, BytesMut, OutgoingMessage, Publisher, Take};
    /// # struct Client;
    /// # impl Client {
    /// #     async fn send(&self, _topic: &str, _payload: Bytes) -> io::Result<()> {
    /// #         Ok(())
    /// #     }
    /// # }
    ///
    /// /// A transport that carries no headers: it takes the payload buffer and nothing else.
    /// struct BarePublisher {
    ///     client: Client,
    /// }
    ///
    /// impl Publisher for BarePublisher {
    ///     type Payload = Take;
    ///     type Error = io::Error;
    ///     type Options = ();
    ///
    ///     async fn publish(
    ///         &self,
    ///         msg: OutgoingMessage<'_, BytesMut>,
    ///         _options: Option<&()>,
    ///     ) -> io::Result<()> {
    ///         let topic = msg.name();
    ///         self.client.send(topic, msg.into_payload().freeze()).await
    ///     }
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub fn into_payload(self) -> Payload {
        self.payload
    }

    /// Returns a shared reference to the headers.
    ///
    /// What a transport that only walks the map reads. One that hands the map on to its client
    /// takes it with [`into_parts`](Self::into_parts) rather than cloning it.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The destination, the payload and the header map, taken in one move.
    ///
    /// What a transport that consumes the message calls: publishing owns the message, so the
    /// name it was addressed to, the payload in its declared form and the map the transforms
    /// filled all arrive at once, with nothing copied. The name is the caller's, so it outlives
    /// the message it came out of. There is no second consuming accessor to pair this with:
    /// [`into_payload`](Self::into_payload) already ends the message, so a transport that wants
    /// the map as well asks for everything here.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io;
    ///
    /// use ruststream::{Bytes, BytesMut, HeaderMap, OutgoingMessage, Publisher, Take};
    /// # struct Client;
    /// # impl Client {
    /// #     async fn send(
    /// #         &self,
    /// #         _topic: &str,
    /// #         _payload: Bytes,
    /// #         _headers: HeaderMap,
    /// #     ) -> io::Result<()> {
    /// #         Ok(())
    /// #     }
    /// # }
    ///
    /// struct ClientPublisher {
    ///     client: Client,
    /// }
    ///
    /// impl Publisher for ClientPublisher {
    ///     type Payload = Take;
    ///     type Error = io::Error;
    ///     type Options = ();
    ///
    ///     async fn publish(
    ///         &self,
    ///         msg: OutgoingMessage<'_, BytesMut>,
    ///         _options: Option<&()>,
    ///     ) -> io::Result<()> {
    ///         // The destination, the buffer and the map the transforms filled, in one move.
    ///         let (topic, payload, headers) = msg.into_parts();
    ///         self.client.send(topic, payload.freeze(), headers).await
    ///     }
    /// }
    /// ```
    #[inline]
    #[must_use]
    pub fn into_parts(self) -> (&'a str, Payload, HeaderMap) {
        (self.name, self.payload, self.headers)
    }
}

impl<'a, Payload: From<&'a [u8]>> OutgoingMessage<'a, Payload> {
    /// Constructs a new outgoing message over bytes the caller holds, with no headers.
    ///
    /// The bytes arrive in the publisher's own form: lent as they are to a transport that reads
    /// them, copied into a buffer of its own for one that keeps them. A publish that produced
    /// the buffer itself hands it over with [`produced`](Self::produced) instead, which copies
    /// nowhere.
    #[inline]
    #[must_use]
    pub fn new(name: &'a str, payload: &'a [u8]) -> Self {
        Self::with_payload(name, Payload::from(payload))
    }
}

impl<'a> OutgoingMessage<'a, BytesMut> {
    /// Constructs a new outgoing message handing over a buffer just written, with no headers.
    ///
    /// What a publish that produced the payload for a taking transport uses: the buffer travels
    /// as it was written, and whoever takes it decides what to make of it.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "json")]
    /// # mod demo {
    /// use std::error::Error;
    ///
    /// use ruststream::codec::{Codec, JsonCodec};
    /// use ruststream::{BytesMut, OutgoingMessage, Publisher, Take};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Order {
    ///     id: u64,
    /// }
    ///
    /// /// A broker crate's outbox relay: it encodes each row itself, and the buffer it wrote is
    /// /// the buffer the transport keeps.
    /// pub async fn relay<P>(publisher: &P, order: &Order) -> Result<(), Box<dyn Error>>
    /// where
    ///     P: Publisher<Payload = Take>,
    ///     P::Error: Error + 'static,
    /// {
    ///     let mut buf = BytesMut::new();
    ///     JsonCodec.encode_into(order, &mut buf)?;
    ///     publisher
    ///         .publish(OutgoingMessage::produced("orders", buf), None)
    ///         .await?;
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    #[inline]
    #[must_use]
    pub fn produced(name: &'a str, payload: BytesMut) -> Self {
        Self::with_payload(name, payload)
    }
}

impl<Payload: AsRef<[u8]>> OutgoingMessage<'_, Payload> {
    /// Returns the payload to be published.
    #[inline]
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
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

    /// How many times the broker has delivered this message, counting this delivery. `None`
    /// where the transport counts nothing.
    ///
    /// Defaulted to `None`, which is the honest answer for a transport with no counter of its
    /// own; the runtime then counts with its own
    /// [`RETRY_COUNT_HEADER`](crate::runtime::RETRY_COUNT_HEADER) alone. A broker whose
    /// deliveries carry a count reports it here - `JetStream`'s `num_delivered`, a claimed Redis
    /// stream entry's delivery count, Pulsar's `redelivery_count`, SQS's
    /// `ApproximateReceiveCount`, Pub/Sub's `delivery_attempt`, AMQP 1.0's `delivery-count` - so
    /// that a registration's `max_attempts(..)` cap counts the broker's own redeliveries rather
    /// than only the copies this process published. The first delivery of a message answers `1`.
    ///
    /// Where the transport counts, this count is the only one a cap reads: the framework's header
    /// is never mixed in, and a copy your crate republishes starts the transport's count afresh.
    /// A delay or a requeue the count does not grow on is the broker's behaviour to document, not
    /// something to work around by adding the header back.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::{AckError, HeaderMap, IncomingMessage};
    ///
    /// struct Delivered {
    ///     payload: Vec<u8>,
    ///     headers: HeaderMap,
    ///     delivered: u64,
    /// }
    ///
    /// impl IncomingMessage for Delivered {
    ///     fn payload(&self) -> &[u8] {
    ///         &self.payload
    ///     }
    ///     fn headers(&self) -> &HeaderMap {
    ///         &self.headers
    ///     }
    ///     fn redelivery_count(&self) -> Option<u64> {
    ///         Some(self.delivered)
    ///     }
    ///     async fn ack(self) -> Result<(), AckError> {
    ///         Ok(())
    ///     }
    ///     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn redelivery_count(&self) -> Option<u64> {
        None
    }

    /// The error the broker met while reading this delivery, or `None` when it read the delivery
    /// whole.
    ///
    /// A broker that reads a delivery into a value itself, such as a database queue reading a row
    /// through its driver, can fail where no codec runs: a column whose type differs from the
    /// field's. It reports that error here. The runtime asks before it materializes the handler's
    /// input, on every input lane and for each delivery of a batch, and settles a delivery that
    /// reports an error by the subscription's decode-failure policy
    /// ([`on_failure(decode = ..)`](crate::runtime::FailurePolicies)), as it settles a payload that
    /// does not decode: the handler does not run, and the warning names the subscription, the
    /// message type and the error.
    ///
    /// A batch that lends its values as one slice ([`CarriesBatch`](crate::CarriesBatch)) has no
    /// value for such a delivery: the body reads the slice before the runtime reaches the
    /// deliveries, so the batch puts the delivery past the end of the slice, with the deliveries
    /// whose value is gone.
    ///
    /// Defaulted to `None`, which costs a broker that keeps it nothing: the check compiles to a
    /// constant and folds away. A broker that overrides it pays one branch per delivery.
    ///
    /// # Examples
    ///
    /// A queue table's delivery keeps the row its driver read, or the error the driver met:
    ///
    /// ```
    /// use std::str;
    ///
    /// use ruststream::codec::CodecError;
    /// use ruststream::{AckError, Carries, HeaderMap, IncomingMessage};
    ///
    /// /// One row of the queue table.
    /// struct SendEmail {
    ///     to: String,
    /// }
    ///
    /// /// A claimed row: the bytes the table stores, and the row read from them or the error met
    /// /// reading it.
    /// struct Claimed {
    ///     bytes: Vec<u8>,
    ///     headers: HeaderMap,
    ///     row: Result<SendEmail, CodecError>,
    /// }
    ///
    /// impl Claimed {
    ///     /// Reads the row the way the driver does: the recipient column holds text.
    ///     fn read(bytes: Vec<u8>, headers: HeaderMap) -> Self {
    ///         let row = str::from_utf8(&bytes)
    ///             .map(|to| SendEmail { to: to.to_owned() })
    ///             .map_err(|err| CodecError::Decode(Box::new(err)));
    ///         Self { bytes, headers, row }
    ///     }
    /// }
    ///
    /// impl Carries<SendEmail> for Claimed {
    ///     fn carried(&self) -> Option<&SendEmail> {
    ///         self.row.as_ref().ok()
    ///     }
    /// }
    ///
    /// impl IncomingMessage for Claimed {
    ///     fn payload(&self) -> &[u8] {
    ///         &self.bytes
    ///     }
    ///     fn headers(&self) -> &HeaderMap {
    ///         &self.headers
    ///     }
    ///     fn decode_error(&self) -> Option<&CodecError> {
    ///         self.row.as_ref().err()
    ///     }
    ///     async fn ack(self) -> Result<(), AckError> {
    ///         Ok(())
    ///     }
    ///     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
    ///         Ok(())
    ///     }
    /// }
    ///
    /// // A recipient that is not text: the runtime settles this claim by the decode policy.
    /// let claimed = Claimed::read(vec![0xff, 0xfe], HeaderMap::new());
    /// assert!(claimed.decode_error().is_some());
    /// ```
    fn decode_error(&self) -> Option<&CodecError> {
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
        assert_eq!(*msg.headers(), HeaderMap::new());
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

        // The form a publisher declares picks the payload type; a message built for nobody in
        // particular names the lending one.
        let msg: OutgoingMessage<'_> = OutgoingMessage::new(&name, &payload);
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
        // (the in-memory and broker impls override them). The broker-authors page shows this
        // test, because it is the one place the defaults are visible: every broker in the
        // workspace overrides them, so nothing else can be pointed at to say what "do nothing"
        // gets you, and a default that changes under the page fails here.
        // --8<-- [start:incoming_defaults]
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
        // The default reports no decode error, so the runtime reads every delivery itself.
        assert!(stub.decode_error().is_none());
        // The default reports no native delayed redelivery, so the runtime uses its fallback.
        assert!(!stub.supports_nack_after());
        // The default nack_after signals "not honored" rather than silently degrading.
        assert!(matches!(
            stub.nack_after(Duration::from_secs(1)).await,
            Err(AckError::Unsupported)
        ));
        // --8<-- [end:incoming_defaults]
    }
}
