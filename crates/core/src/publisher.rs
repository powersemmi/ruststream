//! The [`Publisher`] trait and its declaration-side counterpart, [`PublishPolicy`].

#[cfg(feature = "alloc")]
use alloc::boxed::Box;
use core::error::Error as StdError;
use core::fmt;

#[cfg(feature = "alloc")]
use bytes::Bytes;
use thiserror::Error;

#[cfg(feature = "asyncapi")]
use crate::bindings::Bindings;
use crate::codec::CodecError;
use crate::outgoing::{Outgoing, OutgoingName};
use crate::refusal::{Places, SinkOutcome, SinkRefusal};
use crate::wire::{Serialized, WireFailure};
use crate::{ConnectedBroker, Headers, MaybeSend, MaybeSendSync, OutgoingMessage};
use crate::{EncodeBuf, PayloadBuf};

/// How a transport consumes the payload of a publish: [`Lend`] where it reads the bytes,
/// [`Take`] where its client keeps them.
///
/// Every [`Publisher`] names one as [`Publisher::Payload`], and the message its `publish`
/// receives follows the declaration: a lending publisher is handed `&[u8]`, a taking one a
/// [`BytesMut`](crate::BytesMut) it owns. There is no third form and no arm that never arrives, so a transport
/// matches on nothing and the runtime branches on nothing: it produces the payload the way the
/// type said before the first message was ever published.
///
/// The declaration is what the framework's own buffers follow. A lending publisher inside a
/// dispatch loop is lent one encode buffer the loop reuses, so a reply through it allocates
/// nothing per message; a taking one is handed a fresh buffer per publish, which it keeps.
///
/// Sealed: the two forms are the whole set.
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use ruststream::{Lend, OutgoingMessage, Publisher, PublishOutcome, PublishMessageError};
/// # #[derive(Clone)]
/// # struct Socket;
/// # impl Socket {
/// #     async fn send_frame(&self, _name: &str, _payload: &[u8]) -> io::Result<()> {
/// #         Ok(())
/// #     }
/// # }
///
/// /// A transport that packs the payload into a frame of its own reads it and keeps nothing.
/// #[derive(Clone)]
/// struct Framed {
///     socket: Socket,
/// }
///
/// impl Publisher for Framed {
///     type Payload = Lend;
///     type Error = io::Error;
///     type Options = ();
///
///     async fn publish<'msg>(
///         &self,
///         msg: OutgoingMessage<'msg, &'msg [u8]>,
///         _options: Option<&()>,
///     ) -> PublishOutcome<'msg, io::Error, Lend> {
///         let sent = self.socket.send_frame(msg.name(), msg.payload()).await;
///         // The frame was copied, so the refused message goes back as it came.
///         sent.map_err(|err| PublishMessageError::with_message(err, msg))
///     }
/// }
/// ```
pub trait PayloadForm: sealed::Sealed {
    /// Whether a publish in this form writes into a scratch buffer the framework lends: `true`
    /// for [`Lend`], `false` for [`Take`], whose buffer is the one the codec produces and the
    /// transport keeps.
    ///
    /// A constant, so a publish path decides at compile time whether it needs a scratch at all.
    #[doc(hidden)]
    const LENDS: bool;

    /// The room a publish in this form can carry, as a type a mount's bound reads: [`Unbounded`]
    /// for [`Lend`], and for [`Take`] the room of the buffer the codec produces, which a build
    /// without `alloc` fixes at [`PAYLOAD_CAPACITY`](crate::PAYLOAD_CAPACITY).
    #[doc(hidden)]
    type Room;

    /// The payload as this form carries it: `&'a [u8]` for [`Lend`], [`BytesMut`](crate::BytesMut) for [`Take`].
    ///
    /// The bounds are what every publish position needs of it: the bytes are readable, the
    /// message crosses a task boundary, and bytes the caller holds can be handed over in this
    /// form (free where the transport lends, one copy where it keeps them).
    #[cfg(feature = "alloc")]
    type Form<'a>: AsRef<[u8]> + Send + From<&'a [u8]>;

    /// The payload as this form carries it: `&'a [u8]` for [`Lend`], [`PayloadBuf`] for [`Take`].
    ///
    /// Bytes the caller holds are handed over in this form where they fit: a fixed buffer
    /// refuses a payload past its room.
    #[cfg(not(feature = "alloc"))]
    type Form<'a>: AsRef<[u8]> + Send + TryFrom<&'a [u8]>;

    /// What a publish in this form carries to its encode: the scratch buffer where the form
    /// writes into one, and nothing at all where it does not.
    ///
    /// A publish to a transport that keeps the payload is handed the codec's own buffer, so
    /// there is nothing for it to write into and the publish path carries no buffer - which a
    /// dispatch's futures would otherwise hold for the length of every delivery.
    #[doc(hidden)]
    type Encode<'a>: Send;

    /// The publish path's scratch, as this form takes it: inside a dispatch loop, the one buffer
    /// that loop reuses.
    #[doc(hidden)]
    fn encode_slot(buf: &mut EncodeBuf) -> Self::Encode<'_>;

    /// The same scratch again, for the next message of a batch: what a form that carries a
    /// buffer lends on, and nothing for one that carries none.
    #[doc(hidden)]
    fn encode_again<'s, 'a: 's>(slot: &'s mut Self::Encode<'a>) -> Self::Encode<'s>;

    /// The codec's output in this form: written into the scratch and lent, or a buffer of its
    /// own.
    ///
    /// The scratch is emptied here, so what it held for the previous message never leaves with
    /// this one.
    ///
    /// The framework's own side of the declaration, and the reason it is on this trait: a form
    /// is one decision with two ends, what the transport is handed and what the runtime writes
    /// into, and splitting them would let the two drift. Sealed, so nothing outside implements
    /// it; you never call it.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] when the codec rejects the value.
    #[doc(hidden)]
    fn encoded<'b, E>(encode: &E, buf: Self::Encode<'b>) -> Result<Self::Form<'b>, CodecError>
    where
        E: PayloadEncode;

    /// The bytes a [`Serialized`] value publishes, in this form: the ones it holds, or the ones
    /// it writes into the scratch.
    ///
    /// # Errors
    ///
    /// Returns the value's own error when its encoder rejects it.
    #[doc(hidden)]
    fn serialized<'v, T>(
        value: &'v T,
        buf: Self::Encode<'v>,
    ) -> Result<Self::Form<'v>, WireFailure<T::Error>>
    where
        T: Serialized;

    /// What a refused publish keeps of the payload it got back: the bytes themselves where the
    /// form owns them, and where they sit where the form lent them.
    ///
    /// A taking form was handed the buffer and gives it back, so it is frozen without a copy. A
    /// lending form was handed a view, and the address and length are all that outlive the
    /// borrow, so the caller that owns the bytes decides how to take them.
    #[doc(hidden)]
    fn kept(payload: Self::Form<'_>) -> KeptPayload;

    /// Puts back into `out` what a refused publish returned, so the stage that owns `out` reads
    /// the message that was really sent: the buffer a taking form was handed, and nothing for a
    /// lending one, whose bytes never left `out`.
    #[doc(hidden)]
    fn restored(kept: KeptPayload, out: &mut Outgoing<'_>);

    /// A message in this form, put back on the publish pipeline for the stages above the leaf.
    #[doc(hidden)]
    fn rebuilt<'a>(
        name: OutgoingName<'a>,
        payload: Self::Form<'a>,
        headers: Headers,
    ) -> Outgoing<'a>;

    /// The message that leaves the pipeline, in this form.
    ///
    /// The broker is the last reader, so a taking transport is handed the buffer the stages
    /// wrote and a lending one the bytes where they are; the map the transforms filled moves
    /// into the message where there is a heap, and is borrowed where it lies where there is not.
    #[doc(hidden)]
    fn leaving<'a>(out: &'a mut Outgoing<'_>) -> OutgoingMessage<'a, Self::Form<'a>>;

    /// The payload over a shorter borrow, so a stage can lend it beside a map of its own that
    /// lives shorter than the payload.
    ///
    /// Every form is covariant in its lifetime (a lent slice, an owned buffer); a generic stage
    /// cannot see that, so the form says it here.
    #[doc(hidden)]
    #[cfg(not(feature = "alloc"))]
    fn shortened<'short, 'long: 'short>(payload: Self::Form<'long>) -> Self::Form<'short>;

    /// What a stage that owns its [`Outgoing`] has to keep alive for the length of the publish
    /// it hands on: a destination it owns, the buffer a stage above wrote where the transport
    /// reads the payload, and the header map where there is no heap, since the message borrows it.
    ///
    /// The stage holds one of these instead of the message it spent, so a publish to a taking
    /// transport carries no buffer across its send, and a message addressed to a literal
    /// carries nothing at all.
    #[doc(hidden)]
    type Spent: Default + Send;

    /// The message a stage that owns its [`Outgoing`] hands on, spending it into `spent`.
    ///
    /// [`leaving`](Self::leaving) is the same step for a stage that only borrows the message
    /// from the pipeline and therefore cannot spend it.
    #[doc(hidden)]
    fn handed_on<'a>(
        out: Outgoing<'a>,
        spent: &'a mut Self::Spent,
    ) -> OutgoingMessage<'a, Self::Form<'a>>;

    /// Settles what a refused publish kept of the payload, once the stage that spent the message
    /// takes back what it kept alive for the send.
    ///
    /// Bytes a lending transport was shown that sit in the buffer the stage wrote are moved out of
    /// it whole, with no copy; bytes anywhere else stay where they were, for the caller that
    /// lent them.
    #[doc(hidden)]
    fn reclaimed(spent: &mut Self::Spent, kept: &mut KeptPayload);
}

/// The payload of a refused publish, as [`PayloadForm::kept`] leaves it.
#[doc(hidden)]
// A fixed buffer sits inline beside a pair of numbers, by design: there is no heap to hold it.
#[cfg_attr(not(feature = "alloc"), allow(clippy::large_enum_variant))]
#[derive(Debug)]
pub enum KeptPayload {
    /// The bytes, owned: a taking transport gave its buffer back.
    #[cfg(feature = "alloc")]
    Owned(Bytes),
    /// The bytes, owned: a taking transport gave its buffer back.
    #[cfg(not(feature = "alloc"))]
    Owned(PayloadBuf),
    /// The bytes a lending transport was shown, which stay where they were: their address and
    /// length.
    Lent {
        /// Where the bytes start.
        addr: usize,
        /// How many there are.
        len: usize,
    },
}

/// A value and the codec that encodes it, as the publish path hands them to
/// [`PayloadForm::encoded`]: into the path's own buffer for a form that lends the payload, into a
/// buffer of the codec's for one that hands it over.
#[doc(hidden)]
pub trait PayloadEncode {
    /// Appends the encoded value to `buf`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the value does not encode.
    fn encode_into(&self, buf: &mut EncodeBuf) -> Result<(), CodecError>;

    /// Encodes the value into a buffer of its own.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::Encode`] when the value does not encode.
    fn encode(&self) -> Result<PayloadBuf, CodecError>;
}

/// The declaration of a transport that reads the payload and keeps nothing: it is handed
/// `&[u8]`.
///
/// What every transport that writes the payload into a frame of its own declares. See
/// [`PayloadForm`].
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use ruststream::{Lend, OutgoingMessage, Publisher, PublishOutcome, PublishMessageError};
/// # #[derive(Clone)]
/// # struct Connection;
/// # impl Connection {
/// #     async fn write(&self, _subject: &str, _payload: &[u8]) -> io::Result<()> {
/// #         Ok(())
/// #     }
/// # }
///
/// /// The client copies the bytes onto its socket, so it borrows them where the framework wrote
/// /// them and the dispatch loop reuses that buffer for the next reply.
/// #[derive(Clone)]
/// struct SocketPublisher {
///     connection: Connection,
/// }
///
/// impl Publisher for SocketPublisher {
///     type Payload = Lend;
///     type Error = io::Error;
///     type Options = ();
///
///     async fn publish<'msg>(
///         &self,
///         msg: OutgoingMessage<'msg, &'msg [u8]>,
///         _options: Option<&()>,
///     ) -> PublishOutcome<'msg, io::Error, Lend> {
///         let written = self.connection.write(msg.name(), msg.payload()).await;
///         // The client only read the bytes, so the refused message goes back as it came.
///         written.map_err(|err| PublishMessageError::with_message(err, msg))
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Lend;

/// A payload room with no limit of its own: the transport reads the bytes where the framework
/// wrote them. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Unbounded;

/// A payload room of `BYTES` bytes, the buffer a transport that keeps its payload is handed.
/// Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Capped<const BYTES: usize>;

/// The declaration of a transport whose client keeps the payload: it is handed a [`BytesMut`](crate::BytesMut)
/// of its own.
///
/// The buffer arrives as it was written, so `Vec::from` and [`BytesMut::freeze`](crate::BytesMut::freeze) turn it into
/// what the client speaks without copying it. See [`PayloadForm`].
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use ruststream::{Bytes, BytesMut, OutgoingMessage, Publisher, Take, PublishOutcome, PublishMessageError};
/// # #[derive(Clone)]
/// # struct Producer;
/// # impl Producer {
/// #     async fn send(&self, _topic: &str, _payload: Bytes) -> io::Result<()> {
/// #         Ok(())
/// #     }
/// # }
///
/// /// The client queues the record and sends it later, so it keeps the buffer: `freeze` turns it
/// /// into the `Bytes` the client speaks without a copy.
/// #[derive(Clone)]
/// struct QueuedPublisher {
///     producer: Producer,
/// }
///
/// impl Publisher for QueuedPublisher {
///     type Payload = Take;
///     type Error = io::Error;
///     type Options = ();
///
///     async fn publish<'msg>(
///         &self,
///         msg: OutgoingMessage<'msg, BytesMut>,
///         _options: Option<&()>,
///     ) -> PublishOutcome<'msg, io::Error, Take> {
///         let topic = msg.name();
///         self.producer
///             .send(topic, msg.into_payload().freeze())
///             .await
///             // The client consumed the buffer, so the refusal has no message to return.
///             .map_err(PublishMessageError::new)
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Take;

/// The message a publisher of form `Form` is handed: [`OutgoingMessage`] over that form's
/// payload.
///
/// A broker names the form itself (`OutgoingMessage<'_, &[u8]>`, `OutgoingMessage<'_, BytesMut>`)
/// and never needs this alias; a wrapper generic over the publisher underneath it does, because
/// the form is not known there.
///
/// # Examples
///
/// ```
/// use ruststream::{Headers, OutgoingFor, Publisher, PublishOutcome};
///
/// // A handle that forwards every publish to the publisher it wraps, whichever form that one
/// // declared.
/// #[derive(Clone)]
/// struct Tagged<P>(P, Headers);
///
/// impl<P: Publisher> Publisher for Tagged<P> {
///     type Payload = P::Payload;
///     type Error = P::Error;
///     type Options = P::Options;
///
///     async fn publish<'msg>(
///         &self,
///         msg: OutgoingFor<'msg, Self::Payload>,
///         options: Option<&Self::Options>,
///     ) -> PublishOutcome<'msg, Self::Error, Self::Payload> {
///         self.0.publish(msg, options).await
///     }
///
///     fn base_headers(&self) -> Option<&Headers> {
///         Some(&self.1)
///     }
/// }
/// ```
pub type OutgoingFor<'a, Form> = OutgoingMessage<'a, <Form as PayloadForm>::Form<'a>>;

/// What a [`Publisher::publish`] returns: nothing on success, and on failure the broker's error
/// with the message it did not send.
///
/// `Form` is the publisher's [`Payload`](Publisher::Payload) declaration. A broker author writes
/// it as the return type of `publish`, naming the lifetime of the message it was handed.
pub type PublishOutcome<'msg, Error, Form> =
    Result<(), PublishMessageError<Error, OutgoingFor<'msg, Form>>>;

/// A refused publish: the broker's own error, and the message it did not send when the client
/// gave it back.
///
/// The message is `None` only where the client consumed a [`Take`] buffer and did not return it.
/// Everything a publisher is handed by reference comes back: the destination and the headers
/// are never consumed by a client that fails, and a [`Lend`] payload is only borrowed. Success
/// carries nothing extra, so a publish that works costs what it did before.
///
/// In a build without `alloc` a refusal that crosses the runtime's sinks keeps the message in a
/// place of fixed size that the publishing side keeps, with the destination in a room of 64 bytes.
/// A destination longer than that, or a refusal that finds every place taken, comes back without
/// its message.
///
/// The error converts into this type with [`From`], so `Err(error.into())` and `?` on a
/// client call whose error is `Self::Error` hand the broker's error over with no message.
/// [`with_message`](Self::with_message) is the form for a client that returns the message.
///
/// # Examples
///
/// A broker author's `publish` that hands the message back when its client refuses it:
///
/// ```
/// use std::io;
///
/// use ruststream::{BytesMut, OutgoingMessage, PublishMessageError, PublishOutcome, Publisher, Take};
/// # #[derive(Clone)]
/// # struct Producer;
/// # impl Producer {
/// #     // A client that gives the buffer back when its queue is full.
/// #     async fn try_send(&self, _topic: &str, payload: BytesMut) -> Result<(), (io::Error, BytesMut)> {
/// #         Err((io::Error::other("queue full"), payload))
/// #     }
/// # }
///
/// #[derive(Clone)]
/// struct QueuedPublisher {
///     producer: Producer,
/// }
///
/// impl Publisher for QueuedPublisher {
///     type Payload = Take;
///     type Error = io::Error;
///     type Options = ();
///
///     async fn publish<'msg>(
///         &self,
///         msg: OutgoingMessage<'msg, BytesMut>,
///         _options: Option<&()>,
///     ) -> PublishOutcome<'msg, io::Error, Take> {
///         let (name, payload, headers) = msg.into_parts();
///         match self.producer.try_send(name, payload).await {
///             Ok(()) => Ok(()),
///             Err((error, payload)) => {
///                 let unsent = OutgoingMessage::with_payload(name, payload).with_headers(headers);
///                 Err(PublishMessageError::with_message(error, unsent))
///             }
///         }
///     }
/// }
/// ```
pub struct PublishMessageError<Error, Message> {
    error: Error,
    // Boxed so the outcome of a publish that went out stays the size of the error: the message
    // is moved into the box only on a refusal, which is off the hot path.
    message: Option<Held<Message>>,
}

impl<Error, Message> PublishMessageError<Error, Message> {
    /// A refusal whose message the client did not give back.
    #[must_use]
    pub const fn new(error: Error) -> Self {
        Self {
            error,
            message: None,
        }
    }

    /// A refusal that returns the message it did not send.
    #[must_use]
    pub fn with_message(error: Error, message: Message) -> Self {
        Self {
            error,
            message: Some(hold(message)),
        }
    }

    /// The broker's own error.
    #[must_use]
    pub const fn error(&self) -> &Error {
        &self.error
    }

    /// The message the broker did not send, when the client gave it back.
    #[must_use]
    pub fn message(&self) -> Option<&Message> {
        self.message.as_ref().map(held_ref)
    }

    /// The broker's error, dropping the message.
    #[must_use]
    pub fn into_error(self) -> Error {
        self.error
    }

    /// The message the broker did not send, dropping the error.
    #[must_use]
    pub fn into_message(self) -> Option<Message> {
        self.message.map(unhold)
    }

    /// The error and the message, both owned.
    #[must_use]
    pub fn into_parts(self) -> (Error, Option<Message>) {
        (self.error, self.message.map(unhold))
    }

    /// The same refusal over another message type, with no message.
    ///
    /// What a wrapper that sends a message it built itself uses: the message the inner publisher
    /// returns is the wrapper's, not the caller's, so it is dropped and the error travels on.
    #[must_use]
    pub fn without_message<Other>(self) -> PublishMessageError<Error, Other> {
        PublishMessageError::new(self.error)
    }
}

impl<Error, Message> From<Error> for PublishMessageError<Error, Message> {
    fn from(error: Error) -> Self {
        Self::new(error)
    }
}

impl<Error: fmt::Display, Message> fmt::Display for PublishMessageError<Error, Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

// Transparent over the error, and the message is left out: the form a `Take` publisher is handed
// is a buffer of any size, and the error is logged far more often than the message is read.
impl<Error: fmt::Debug, Message> fmt::Debug for PublishMessageError<Error, Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl<Error: StdError + 'static, Message> StdError for PublishMessageError<Error, Message> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.error.source()
    }
}

/// A refused message is held behind a `Box` where there is a heap, so the outcome of a publish that
/// went out stays the size of the error, and inline where there is none.
#[cfg(feature = "alloc")]
type Held<Value> = Box<Value>;
#[cfg(not(feature = "alloc"))]
type Held<Value> = Value;

#[cfg(feature = "alloc")]
fn hold<Value>(value: Value) -> Held<Value> {
    Box::new(value)
}
#[cfg(not(feature = "alloc"))]
const fn hold<Value>(value: Value) -> Held<Value> {
    value
}

#[cfg(feature = "alloc")]
#[expect(clippy::boxed_local, reason = "one signature for both variants")]
fn unhold<Value>(held: Held<Value>) -> Value {
    *held
}
#[cfg(not(feature = "alloc"))]
fn unhold<Value>(held: Held<Value>) -> Value {
    held
}

fn held_ref<Value>(held: &Held<Value>) -> &Value {
    held
}

mod sealed {
    /// Seals [`PayloadForm`](super::PayloadForm): a transport reads the payload or keeps it,
    /// and a third answer would be one the runtime has no buffer strategy for.
    pub trait Sealed {}

    impl Sealed for super::Lend {}
    impl Sealed for super::Take {}
}

/// A producer that sends messages into the broker.
///
/// `Publisher` is `Send + Sync` so a single instance can be shared across tasks, and
/// `Clone` so a handler can move its `Out` publisher into a continuation such as
/// `.error(..)`. A clone is another handle on the same connection: expensive shared state
/// (connection pool, batch buffers) lives behind an [`Arc`].
///
/// # Examples
///
/// ```
/// use ruststream::{OutgoingMessage, PublishMessageError, Publisher};
///
/// async fn emit<P: Publisher>(publisher: &P) -> Result<(), P::Error> {
///     // Bytes the caller holds, in whichever form this publisher declared.
///     let msg = OutgoingMessage::new("orders.created", b"{}".as_slice());
///     // The refusal carries the unsent message; this caller only needs the error.
///     publisher
///         .publish(msg, None)
///         .await
///         .map_err(PublishMessageError::into_error)
/// }
/// ```
///
/// [`Arc`]: std::sync::Arc
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publisher",
    note = "for an `Out<impl Publisher, _>` slot, the type comes from the policy attached \
                    at the include site: attach one whose live form publishes"
)]
pub trait Publisher: MaybeSendSync + Clone {
    /// How this transport consumes the payload: [`Lend`] where it reads the bytes and keeps
    /// nothing, [`Take`] where its client keeps them.
    ///
    /// The declaration decides what [`publish`] receives - `&[u8]` under `Lend`, a
    /// [`BytesMut`](crate::BytesMut) of its own under `Take` - and what the framework does with its own buffers
    /// above it: a lending publisher inside a dispatch loop is lent the one encode buffer that
    /// loop reuses, so a reply through it allocates nothing per message, while a taking one is
    /// handed a fresh buffer it may keep. Declare `Lend` unless the client keeps the bytes past
    /// the call.
    ///
    /// [`publish`]: Self::publish
    type Payload: PayloadForm;

    /// The error type returned by [`publish`].
    ///
    /// [`publish`]: Self::publish
    type Error: StdError + Send + Sync + 'static;

    /// The broker's per-message settings: a `QoS`, a priority, an ordering key, whatever this
    /// transport lets one message differ from the next in.
    ///
    /// Every field is optional, because a publish carries only what its call site adjusted: the
    /// rest keeps what the [`PublishPolicy`] fixed when it paired this publisher. A broker with
    /// no per-message setting writes `type Options = ();`.
    ///
    /// The type is the broker's own, and so are the builder steps that fill it: a broker ships
    /// an extension trait over
    /// [`PublishBuilder`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.PublishBuilder.html)
    /// bounded on this type, so its steps appear on a builder over its own publisher and
    /// nowhere else.
    ///
    /// `Clone` and `'static` are what the test harness needs: it copies the options of each
    /// publish the runtime makes and hands them back to the test as this type
    /// (`with_options(..)` on `tb.out::<Marker>()` or on a broker's `published(..)`, under the
    /// `testing` feature). Deriving `Debug` and `PartialEq` as well lets a service's test
    /// assert on the value `publish` received rather than on the protocol field it became.
    ///
    /// A setting takes one shape: a field here, a default on the policy, and a step on the
    /// publish builder, shipped from the broker's prelude. The step writes through
    /// [`PublishBuilder::options_mut`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.PublishBuilder.html#method.options_mut),
    /// so nothing wraps the publisher and the publish still goes through the mount site's
    /// entry, with its codec and transforms. A send in a trait of the broker's own would pass
    /// outside the slot view a test asserts on, and a setting carried as a header would be
    /// bytes `publish` parses back. A value the transport cannot honour fails the publish; it
    /// never falls back to the default silently.
    ///
    /// # Examples
    ///
    /// A priority the policy defaults and a call site adjusts:
    ///
    /// ```
    /// # mod demo {
    /// use ruststream::memory::{ConnectedMemoryBroker, MemoryError, MemoryPublish, MemoryPublisher};
    /// use ruststream::runtime::{PublishBuilder, PublishExt, PublishSink};
    /// use ruststream::{
    ///     PublishOutcome,
    ///     BytesMut, OutgoingMessage, Outgoing, PairError, PublishPolicy, Publisher, Serialized,
    ///     Take,
    /// };
    ///
    /// /// Every field optional: what a call leaves unset keeps what the policy fixed.
    /// #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    /// pub struct PriorityOptions {
    ///     priority: Option<u8>,
    /// }
    ///
    /// /// The policy: constructible anywhere, and the place the default is configured.
    /// #[derive(Debug, Clone, Copy, Default)]
    /// pub struct PriorityPublish {
    ///     priority: u8,
    /// }
    ///
    /// /// The live publisher: the connection and the default the policy carried.
    /// #[derive(Clone)]
    /// pub struct PriorityPublisher {
    ///     inner: MemoryPublisher,
    ///     default_priority: u8,
    /// }
    ///
    /// impl PublishPolicy<ConnectedMemoryBroker> for PriorityPublish {
    ///     type Live = PriorityPublisher;
    ///
    ///     async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<Self::Live, PairError> {
    ///         let inner = MemoryPublish.pair(connected).await?;
    ///         Ok(PriorityPublisher { inner, default_priority: self.priority })
    ///     }
    /// }
    ///
    /// impl Publisher for PriorityPublisher {
    ///     type Payload = Take;
    ///     type Error = MemoryError;
    ///     type Options = PriorityOptions;
    ///
    ///     async fn publish<'msg>(
    ///         &self,
    ///         msg: OutgoingMessage<'msg, BytesMut>,
    ///         options: Option<&Self::Options>,
    ///     ) -> PublishOutcome<'msg, Self::Error, Take> {
    ///         let priority = options
    ///             .and_then(|options| options.priority)
    ///             .unwrap_or(self.default_priority);
    ///         // A real client takes `priority` as the protocol field it is; the in-memory bus
    ///         // has none, so this one passes the message on unchanged.
    ///         let _ = priority;
    ///         self.inner.publish(msg, None).await
    ///     }
    /// }
    ///
    /// /// The step a service imports from the broker's prelude. The bound on the sink's options
    /// /// keeps it off a builder over any other broker's publisher.
    /// pub trait PriorityStep {
    ///     /// Sends this one message at `priority`.
    ///     #[must_use]
    ///     fn priority(self, priority: u8) -> Self;
    /// }
    ///
    /// impl<Sink, Body, Enc, Hdrs, Dest> PriorityStep for PublishBuilder<Sink, Body, Enc, Hdrs, Dest>
    /// where
    ///     Sink: PublishSink<Options = PriorityOptions>,
    /// {
    ///     fn priority(mut self, priority: u8) -> Self {
    ///         self.options_mut().get_or_insert_with(PriorityOptions::default).priority =
    ///             Some(priority);
    ///         self
    ///     }
    /// }
    ///
    /// // Bytes that are already the payload, so this example needs no codec feature.
    /// #[derive(Outgoing, Serialized)]
    /// pub struct Alert(Vec<u8>);
    ///
    /// pub async fn page(publisher: &PriorityPublisher) -> Result<(), Box<dyn std::error::Error>> {
    ///     publisher.message(&Alert(b"disk full".to_vec())).to("alerts").priority(9).publish().await?;
    ///     Ok(())
    /// }
    /// # }
    /// # fn main() {}
    /// ```
    type Options: Clone + Send + Sync + 'static;

    /// Publishes a message to the broker, with the per-message settings the call site adjusted.
    ///
    /// This is the contract a broker implements, and the direct call a broker crate used on its
    /// own - without this one - is written against. Inside a service built on `ruststream` it
    /// is the layer underneath: what a handler sends goes through the publish builder
    /// ([`message`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.PublishExt.html#method.message)),
    /// which resolves the destination, the codec and the headers and assembles the
    /// [`OutgoingMessage`] itself. Reach for this one where the message is already built: a
    /// publish transform, a middleware, a post-settle hook.
    ///
    /// `options` is `None` on every path with no call site to adjust them - a reply, a deferred
    /// redelivery - and the policy's own settings apply.
    ///
    /// The message arrives by value, and so do its payload and its header map. Read them with
    /// [`OutgoingMessage::payload`] and [`OutgoingMessage::headers`]; a transport that consumes
    /// the message takes its parts with [`OutgoingMessage::into_parts`] - the destination, the
    /// payload and the map in one move, nothing copied.
    ///
    /// What the payload is follows [`Payload`](Self::Payload). A [`Lend`] publisher is handed
    /// `&[u8]` valid for the length of the call: read it, write it into your frame, and hold
    /// nothing afterwards. A [`Take`] publisher is handed the [`BytesMut`](crate::BytesMut) the framework wrote,
    /// as it was written, so `Vec::from(payload)` and [`BytesMut::freeze`](crate::BytesMut::freeze) turn it into what
    /// the client speaks with no copy of the body; only a publish lending bytes the framework
    /// does not own is copied into that buffer.
    ///
    /// # Cancel safety
    ///
    /// Cancel safety is implementation-defined: most brokers will leave a message in an
    /// indeterminate state if the future is dropped mid-flight. Implementors must document the
    /// guarantees their broker provides.
    ///
    /// # Errors
    ///
    /// Returns a [`PublishMessageError`] over [`Self::Error`] when the broker rejects the
    /// message, the connection is lost, or the operation times out. It carries the message the
    /// broker did not send: a [`Lend`] publisher always hands it back, and a [`Take`] publisher
    /// hands it back when its client returns the buffer. A caller keeps it to try another
    /// destination, as the error of an `Out` publish does for a handler.
    ///
    /// A broker author's `publish` that hands the message back:
    ///
    /// ```
    /// use std::io;
    ///
    /// use ruststream::{Lend, OutgoingMessage, PublishMessageError, PublishOutcome, Publisher};
    /// # #[derive(Clone)]
    /// # struct Socket;
    /// # impl Socket {
    /// #     async fn write(&self, _subject: &str, _payload: &[u8]) -> io::Result<()> {
    /// #         Err(io::Error::other("connection reset"))
    /// #     }
    /// # }
    ///
    /// #[derive(Clone)]
    /// struct SocketPublisher {
    ///     socket: Socket,
    /// }
    ///
    /// impl Publisher for SocketPublisher {
    ///     type Payload = Lend;
    ///     type Error = io::Error;
    ///     type Options = ();
    ///
    ///     async fn publish<'msg>(
    ///         &self,
    ///         msg: OutgoingMessage<'msg, &'msg [u8]>,
    ///         _options: Option<&()>,
    ///     ) -> PublishOutcome<'msg, io::Error, Lend> {
    ///         let written = self.socket.write(msg.name(), msg.payload()).await;
    ///         // The socket only read the bytes, so the refused message goes back whole.
    ///         written.map_err(|error| PublishMessageError::with_message(error, msg))
    ///     }
    /// }
    /// ```
    fn publish<'msg>(
        &self,
        msg: OutgoingFor<'msg, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = PublishOutcome<'msg, Self::Error, Self::Payload>> + MaybeSend;
    /// [`publish`](Self::publish) with the refused message returned owned: moved into a box with
    /// `alloc`, and without it into one of `places`, which the side that publishes keeps.
    ///
    /// Machinery of the runtime's own publish path: a broker never calls or overrides it.
    ///
    /// # Errors
    ///
    /// Returns the failure of [`publish`](Self::publish) with the message it did not send.
    fn publish_kept<'p>(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        places: Places<'p>,
    ) -> impl Future<Output = SinkOutcome<'p, Self::Error>> + MaybeSend {
        // The publish future is built inside the block, where the block's state holds it: built
        // outside and captured, it would sit in the block's upvars and be copied into the await
        // slot on the first poll, a copy of the whole broker future per publish.
        async move {
            self.publish(msg, options)
                .await
                .map_err(|failed| SinkRefusal::returned::<Self::Payload>(failed, places))
        }
    }

    /// The headers this publisher contributes to every message it sends, underneath whatever
    /// the message itself names.
    ///
    /// `None` by default: a plain broker publisher contributes nothing and the outgoing map
    /// starts empty. This is the place for a constant of the publisher itself - a tenant, a
    /// producer name, a schema id every message of this handle carries - laid down first, with
    /// the message's own headers written over it key by key, so the call site wins over the
    /// handle.
    ///
    /// A delivery setting is not one of those: it belongs to the message, and a broker carries
    /// it as a field of [`Options`](Self::Options) that the policy defaults and the call site
    /// adjusts.
    ///
    /// Every message the runtime sends through this publisher starts from that base: a publish
    /// through the builder, and the reply a `reply("dest")` handler returns (whose
    /// [`PublishTransform`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.PublishTransform.html)
    /// stack then writes over it, like any other call site). A publish a handler body issues on
    /// a value it obtained outside the framework is that body's own call and reaches nothing
    /// here.
    ///
    /// The map is borrowed, never rebuilt per publish, so a handle that has one keeps it in its
    /// own state.
    ///
    /// # Examples
    ///
    /// ```
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::PublishOutcome;
    /// use ruststream::runtime::PublishExt;
    /// use ruststream::{Headers, Outgoing, OutgoingFor, Publisher, Serialized};
    ///
    /// // A handle that tags every message it sends, without touching the message itself.
    /// #[derive(Clone)]
    /// struct Tenanted<P>(P, Headers);
    ///
    /// impl<P: Publisher> Publisher for Tenanted<P> {
    ///     type Payload = P::Payload;
    ///     type Error = P::Error;
    ///     type Options = P::Options;
    ///
    ///     async fn publish<'msg>(
    ///         &self,
    ///         msg: OutgoingFor<'msg, Self::Payload>,
    ///         options: Option<&Self::Options>,
    ///     ) -> PublishOutcome<'msg, Self::Error, Self::Payload> {
    ///         self.0.publish(msg, options).await
    ///     }
    ///
    ///     fn base_headers(&self) -> Option<&Headers> {
    ///         Some(&self.1)
    ///     }
    /// }
    ///
    /// // Bytes that are already the payload, so this example needs no codec feature.
    /// #[derive(Outgoing, Serialized)]
    /// struct Order(Vec<u8>);
    ///
    /// let broker = MemoryBroker::new();
    /// let base = [("tenant", "acme")].into_iter().collect();
    /// let publisher = Tenanted(broker.publisher(), base);
    /// publisher.message(&Order(b"{}".to_vec())).to("orders").publish().await?;
    /// # Ok(())
    /// # }
    /// ```
    fn base_headers(&self) -> Option<&Headers> {
        None
    }
}

/// The declaration half of a publisher: pure policy, no connection, no publish surface.
///
/// A broker publisher is a bundle of policy (an exchange, a queue timeout, a transactional
/// id) paired with the live connection. `PublishPolicy` is that bundle alone, freely
/// constructible anywhere - before startup, in router definitions, in configuration -
/// because it holds no connection and no broker instance identity. [`pair`](Self::pair)
/// joins it with a [`ConnectedBroker`] witness to produce the live [`Publisher`], so "not
/// connected" is not representable on this path: a publisher exists only after the
/// connection does.
///
/// The policy is also where a broker's per-message settings ([`Publisher::Options`]) get
/// their defaults: the mount site configures the policy, pairing hands the live publisher
/// whatever it fixed, and a call site adjusts single fields over that.
///
/// This is the publish-side mirror of [`SubscriptionSource`](crate::SubscriptionSource).
/// The reply wiring a mount site's chain builds over a policy is itself a `PublishPolicy`,
/// resolved functorially: pairing swaps the leaf for its live publisher and keeps the codec
/// and transform stacks the chain named, fully monomorphized.
///
/// `pair` is async and fallible because some brokers do real work when a publisher comes
/// alive (a transactional producer initializing its transactions); for most it is a cheap
/// constructor call. It reports the type-erased [`PairError`]: a broker wraps its own error
/// with [`PairError::new`]. A policy pairs once, at startup, so `pair` never reaches the
/// hot path.
///
/// # Implementing it
///
/// A broker ships one policy and one live form per genuine publishing mode, and the choice
/// of mode is a transition of the policy type, not a runtime flag: a `transactional_id(..)`
/// step moves the plain policy to a distinct transactional policy whose live form
/// implements [`TransactionalPublisher`](crate::TransactionalPublisher), and the plain
/// publisher has no transactional surface at all. Where the plain policy is usable with its
/// defaults, the connected form also implements [`DefaultPublish`]. The in-memory broker's
/// `MemoryPublish` and `MemoryRequest`, empty structs because they have no options, are the
/// minimal reference.
///
/// # Examples
///
/// ```
/// # async fn demo() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::runtime::PublishExt;
/// use ruststream::{Broker, Outgoing, PublishPolicy, Serialized};
///
/// // Bytes that are already the payload, so this example needs no codec feature.
/// #[derive(Outgoing, Serialized)]
/// struct Order(Vec<u8>);
///
/// let policy = MemoryPublish; // no broker in sight
/// let connected = MemoryBroker::new().connect().await?;
/// let publisher = policy.pair(&connected).await?; // live only past this point
/// publisher.message(&Order(b"{}".to_vec())).to("orders").publish().await?;
/// # Ok(())
/// # }
/// ```
pub trait PublishPolicy<C: ConnectedBroker> {
    /// The live form this policy pairs into: a [`Publisher`] for a leaf policy, or the live
    /// wiring form for a combinator stack (a typed publisher over a policy pairs into the same
    /// typed publisher over the live leaf).
    type Live;

    /// Pairs the policy with a connected broker, producing the live publisher.
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when bringing the publisher alive requires broker work and that
    /// work fails (most policies pair infallibly).
    fn pair(self, connected: &C) -> impl Future<Output = Result<Self::Live, PairError>>;

    #[cfg(feature = "asyncapi")]
    #[must_use]
    /// What a publish through this policy adds to its channel in the generated `AsyncAPI`
    /// document.
    ///
    /// The publish-side mirror of
    /// [`SubscriptionSource::channel_bindings`](crate::SubscriptionSource::channel_bindings),
    /// and the same three rules bound it. The value is computed from the policy and the name it
    /// is handed, because the document is built before anything connects, so a Kafka topic's
    /// real partition count cannot come from here. A credential never goes in, for the reason
    /// [`DescribeServer`](crate::DescribeServer) gives. And a protocol the specification has no
    /// binding for goes in [`Binding::extension`](crate::bindings::Binding::extension).
    ///
    /// The policy answers for every position it is bound on: the reply of a `reply(..)`
    /// registration, an
    /// [`Out`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.Out.html) slot, and
    /// the publisher a dead-lettered delivery leaves through. A
    /// [`Bound`](crate::bindable::Bound) token answers with the policy it carries, so a
    /// cross-broker publish is described by the broker it reaches. The channel's `servers` list
    /// does not follow it: that comes from the label the registration's own broker was
    /// registered under, which is the only broker the runtime can name for a channel.
    ///
    /// `channel` is the destination the mount site resolved for that position - the reply
    /// type's own name or the `reply("dest")` clause, the slot entry's name, the
    /// `dead_letter("dlq")` declaration - and it is what the document reports as the channel's
    /// `address`. An SNS topic and an SQS queue are named by their binding's required `name`
    /// field, and this is where that name comes from: a policy declares a broker's settings and
    /// never a destination. Where a naming transform decides the destination per delivery, the
    /// document reports no address and the mount site's fallback name arrives here instead.
    ///
    /// The default says nothing, and a policy that says nothing changes no document.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # fn demo() -> Result<(), ruststream::asyncapi::BindingError> {
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct SnsChannel {
    ///     name: String,
    /// }
    ///
    /// # struct SnsPublish;
    /// # impl SnsPublish {
    /// fn channel_bindings(&self, channel: &str) -> Bindings {
    ///     let body = SnsChannel { name: channel.to_owned() };
    ///     // A binding that fails to build is a binding the document goes without: a broker
    ///     // never holds up a service over a description of itself.
    ///     match Binding::new("sns", "0.1.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!SnsPublish.channel_bindings("orders").is_empty());
    /// # Ok(())
    /// # }
    /// ```
    fn channel_bindings(&self, channel: &str) -> Bindings {
        let _ = channel;
        Bindings::new()
    }

    #[cfg(feature = "asyncapi")]
    #[must_use]
    /// What a publish through this policy adds to its `send` operation in the document.
    ///
    /// The producer's own settings live here rather than on the channel: a Kafka client id, an
    /// MQTT `QoS`, an SQS delay. The rules of [`channel_bindings`](Self::channel_bindings)
    /// apply unchanged, `channel` included: it is the same resolved destination.
    ///
    /// A reply has no `send` operation - it is the `reply` of the operation it answers, and the
    /// specification gives that object no bindings - so what this returns reaches the document
    /// from a slot and from a dead-letter destination only.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # fn demo() -> Result<(), ruststream::asyncapi::BindingError> {
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct SqsOperation {
    ///     queues: Vec<SqsQueue>,
    /// }
    ///
    /// #[derive(Serialize)]
    /// struct SqsQueue {
    ///     name: String,
    /// }
    ///
    /// # struct SqsPublish;
    /// # impl SqsPublish {
    /// fn operation_bindings(&self, channel: &str) -> Bindings {
    ///     let body = SqsOperation { queues: vec![SqsQueue { name: channel.to_owned() }] };
    ///     match Binding::new("sqs", "0.3.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!SqsPublish.operation_bindings("orders").is_empty());
    /// # Ok(())
    /// # }
    /// ```
    fn operation_bindings(&self, channel: &str) -> Bindings {
        let _ = channel;
        Bindings::new()
    }

    #[cfg(feature = "asyncapi")]
    #[must_use]
    /// What a publish through this policy adds to the messages that leave through it.
    ///
    /// A Kafka record's key schema, a Pub/Sub ordering key. The rules of
    /// [`channel_bindings`](Self::channel_bindings) apply unchanged, `channel` included: it is
    /// the same resolved destination.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # fn demo() -> Result<(), ruststream::asyncapi::BindingError> {
    /// use ruststream::asyncapi::{Binding, Bindings};
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct GooglePubSubMessage {
    ///     #[serde(rename = "orderingKey")]
    ///     ordering_key: &'static str,
    /// }
    ///
    /// # struct PubSubPublish;
    /// # impl PubSubPublish {
    /// fn message_bindings(&self, _channel: &str) -> Bindings {
    ///     let body = GooglePubSubMessage { ordering_key: "tenant" };
    ///     match Binding::new("googlepubsub", "0.2.0", &body) {
    ///         Ok(binding) => Bindings::new().with(binding),
    ///         Err(_) => Bindings::new(),
    ///     }
    /// }
    /// # }
    /// # assert!(!PubSubPublish.message_bindings("orders").is_empty());
    /// # Ok(())
    /// # }
    /// ```
    fn message_bindings(&self, channel: &str) -> Bindings {
        let _ = channel;
        Bindings::new()
    }

    #[cfg(feature = "asyncapi")]
    #[must_use]
    /// Where a client finds the address of a reply this policy publishes, as a runtime
    /// expression.
    ///
    /// A broker that answers a request through a reply-to header names that header here:
    /// `"$message.header#/reply-to"` is the specification's form, and the part after the `#` is
    /// a JSON Pointer into the request's headers. The document then reports the reply channel
    /// with `address: null` and puts the expression in the operation's
    /// `reply.address.location`, so a reader knows the destination is decided per delivery and
    /// where to read it.
    ///
    /// It reaches the document only where a
    /// [`PublishTransform`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.PublishTransform.html)
    /// on the reply position declares [`Destination =
    /// Names`](https://docs.rs/ruststream/latest/ruststream/runtime/trait.PublishTransform.html#associatedtype.Destination):
    /// that transform is what actually redirects the reply, and without it the reply goes to
    /// the name the mount site declared, which is what the document reports.
    ///
    /// The default is `None`: a broker whose replies carry no such address says nothing, and
    /// the document keeps the declared name.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "asyncapi")]
    /// # {
    /// # struct NatsPublish;
    /// # impl NatsPublish {
    /// /// NATS replies go to the inbox a request names in its `reply-to` header.
    /// fn reply_address_location(&self) -> Option<&'static str> {
    ///     Some("$message.header#/reply-to")
    /// }
    /// # }
    /// # }
    /// ```
    fn reply_address_location(&self) -> Option<&'static str> {
        None
    }
}

/// The error of [`PublishPolicy::pair`]: whatever the broker reported while bringing a publisher
/// alive, type-erased.
///
/// A cross-broker token pairs against a broker other than the including scope's, so the error
/// cannot be typed to one broker. Without `alloc` the cause is a `Cause`, which is
/// `Copy`.
#[derive(Debug, Error)]
#[error("pairing a publisher failed: {0}")]
pub struct PairError(
    #[cfg(feature = "alloc")]
    #[source]
    Box<dyn StdError + Send + Sync>,
    #[cfg(not(feature = "alloc"))]
    #[source]
    crate::Cause,
);

impl PairError {
    /// Wraps a broker's pairing failure.
    #[cfg(feature = "alloc")]
    #[must_use]
    pub fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }

    /// Wraps an already-boxed failure, or a plain message.
    #[cfg(feature = "alloc")]
    #[must_use]
    pub fn from_boxed(source: Box<dyn StdError + Send + Sync>) -> Self {
        Self(source)
    }

    /// Wraps a broker's pairing failure.
    #[cfg(not(feature = "alloc"))]
    #[must_use]
    pub const fn new(cause: crate::Cause) -> Self {
        Self(cause)
    }
}

/// A connected broker that names its plain publish policy, so the runtime can build a
/// default reply publisher when a `reply("dest")` handler is included without an explicit
/// one.
///
/// Implement it alongside [`ConnectedBroker`](crate::ConnectedBroker) when the broker has a
/// publish policy whose default configuration is usable as-is (most are). Brokers whose
/// publishers always need explicit options simply do not implement it, and their users
/// attach a policy at every registration.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// struct Confirmed {
///     id: u64,
/// }
///
/// #[subscriber("orders", reply("orders.confirmed"))]
/// async fn confirm(order: &Order) -> Confirmed {
///     Confirmed { id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         // No policy named: the reply leaves through the broker's default one.
///         b.include(confirm)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait DefaultPublish: ConnectedBroker {
    /// The broker's plain publish policy, constructible with its defaults.
    type Policy: PublishPolicy<Self> + Default + 'static;
}

#[cfg(test)]
mod tests {
    use core::future::ready;
    use core::hint::black_box;
    use std::io;

    use super::*;

    /// A publisher whose publish future holds a large buffer across its await point, as a
    /// client's own connection and statement futures do.
    #[derive(Clone)]
    struct Large;

    impl Publisher for Large {
        type Payload = Lend;
        type Error = io::Error;
        type Options = ();

        async fn publish<'msg>(
            &self,
            msg: OutgoingMessage<'msg, &'msg [u8]>,
            _options: Option<&()>,
        ) -> PublishOutcome<'msg, io::Error, Lend> {
            let buffer = [0_u8; 8192];
            ready(()).await;
            black_box((&buffer, msg));
            Ok(())
        }
    }

    #[test]
    fn the_kept_publish_holds_the_broker_future_once() {
        let payload = b"order";
        let publish = size_of_val(&Large.publish(OutgoingMessage::new("orders", payload), None));
        let kept = size_of_val(&Large.publish_kept(
            OutgoingMessage::new("orders", payload),
            None,
            Places::none(),
        ));
        let message = size_of::<OutgoingMessage<'_, &[u8]>>();
        // The wrapper adds the message it carries until the first poll, never a second copy of
        // the broker's future.
        assert!(
            kept <= publish + message + 64,
            "publish_kept: {kept} bytes for a {publish}-byte future and a {message}-byte message"
        );
    }

    // The refused message is boxed where there is a heap.
    #[test]
    #[cfg(feature = "alloc")]
    fn a_publish_that_went_out_returns_an_outcome_of_the_cause_alone() {
        assert!(
            size_of::<PublishOutcome<'_, io::Error, Take>>()
                <= size_of::<io::Error>() + size_of::<usize>()
        );
    }
}

#[cfg(all(test, not(feature = "alloc")))]
mod fixed_tests {
    use std::io;

    use super::*;

    // Without a heap a refusal holds the message inline, so every publish returns an outcome as
    // large as the message: a name, the bytes and a borrow of the headers, never their room.
    #[test]
    fn a_publish_that_went_out_returns_an_outcome_of_the_cause_and_a_lent_message() {
        assert!(
            size_of::<PublishOutcome<'_, io::Error, Lend>>()
                <= size_of::<io::Error>()
                    + size_of::<&str>()
                    + size_of::<&[u8]>()
                    + size_of::<&Headers>()
        );
    }
}
