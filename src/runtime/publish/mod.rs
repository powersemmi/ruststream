//! Outgoing message and the publish middleware pipeline.
//!
//! Every publish a handler makes - the reply of a `#[subscriber(.., publish(..))]` form, and every
//! message that leaves through an injected [`Out`](super::Out) slot - flows through a chain of
//! [`PublishLayer`] before reaching the broker publisher. Middleware transform the
//! payload (for example, wrap it in a Confluent / Avro envelope) and enrich the headers
//! (content-type, schema id), or observe it (publish metrics). The chain is symmetric to the
//! consume-side static [`Stack`](super::Stack).

use std::borrow::Cow;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::pin::Pin;

use bytes::BytesMut;
use bytes_utils::Str;

use crate::HeaderMap;
use crate::runtime::lifecycle::BoxError;

// The boxed future of the DYNAMIC middleware path only (PublishDynLayer / PublishDynNext).
// The static pipeline returns unboxed RPITIT futures; only the opt-in runtime-composed list
// pays this allocation.
pub(super) type PublishFut<'a> = Pin<Box<dyn Future<Output = Result<(), BoxError>> + Send + 'a>>;

/// A mutable outgoing message flowing through the publish pipeline.
///
/// The [`name`](Self::name) is an [`OutgoingName`], which carries a destination from any of the
/// three places one comes from without copying it: the macro reply path borrows a string literal
/// (`reply_name(&self) -> &str`), a computed name moves in owned, and a name read off the
/// delivery being answered arrives as the delivery's own buffer. The payload behaves the same
/// way: codec output moves in whole, a rebuilt message lends the bytes it was given, and the
/// first [`payload_mut`](Self::payload_mut) or [`set_payload`](Self::set_payload) is what makes
/// the buffer this message's own. Middleware may change the name, transform the payload, and
/// enrich the [`headers`](Self::headers_mut) before the message is sent.
#[derive(Debug, Clone)]
pub struct Outgoing<'a> {
    name: OutgoingName<'a>,
    payload: Payload<'a>,
    headers: HeaderMap,
}

/// The destination of an [`Outgoing`]: a borrow, an owned string, or a shared buffer.
///
/// Each form is a place a name comes from, and none of them copies it. A literal at a mount site
/// borrows for the length of the publish call. A name built per delivery moves in owned. A name
/// the delivery already carries - the queue an AMQP request named in its `reply-to` header, a
/// `ROUTER` peer identity - arrives as the buffer it was read from, through
/// [`HeaderMap::get_shared`] and [`Str`], so a transform that answers where the request asked
/// allocates nothing.
///
/// Every one of those forms converts, so [`Outgoing::set_name`] takes the name as the call site
/// has it.
///
/// # Examples
///
/// ```
/// use ruststream::Str;
/// use ruststream::runtime::{Outgoing, OutgoingName};
///
/// let mut out = Outgoing::new("answers", b"{}".as_slice());
/// assert_eq!(out.name(), "answers");
///
/// // A name the delivery already holds: the buffer is shared, not copied.
/// out.set_name(Str::from_static("replies.inbox"));
/// assert_eq!(out.name(), "replies.inbox");
///
/// let computed = OutgoingName::from(format!("replies.{}", 7));
/// assert_eq!(&*computed, "replies.7");
/// ```
#[derive(Debug, Clone)]
pub enum OutgoingName<'a> {
    /// A name valid as long as the publish call is: a literal, or a slice of something the caller
    /// keeps alive.
    Borrowed(&'a str),
    /// A name built for this message.
    Owned(String),
    /// A name sharing a buffer with whatever produced it, counted by reference.
    Shared(Str),
}

impl OutgoingName<'_> {
    /// The name as a string slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::OutgoingName;
    ///
    /// assert_eq!(OutgoingName::from("orders").as_str(), "orders");
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(name) => name,
            Self::Owned(name) => name,
            Self::Shared(name) => name,
        }
    }
}

impl Deref for OutgoingName<'_> {
    type Target = str;

    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for OutgoingName<'_> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Display for OutgoingName<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// The three forms are three ways to hold one name, so equality and hashing read the name and
// never the form it came in.
impl PartialEq for OutgoingName<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for OutgoingName<'_> {}

impl Hash for OutgoingName<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl<'a> From<&'a str> for OutgoingName<'a> {
    fn from(name: &'a str) -> Self {
        Self::Borrowed(name)
    }
}

impl From<String> for OutgoingName<'_> {
    fn from(name: String) -> Self {
        Self::Owned(name)
    }
}

impl From<Str> for OutgoingName<'_> {
    fn from(name: Str) -> Self {
        Self::Shared(name)
    }
}

impl<'a> From<Cow<'a, str>> for OutgoingName<'a> {
    fn from(name: Cow<'a, str>) -> Self {
        match name {
            Cow::Borrowed(name) => Self::Borrowed(name),
            Cow::Owned(name) => Self::Owned(name),
        }
    }
}

/// An [`Outgoing`] payload: the bytes as they arrived, until something writes to them.
///
/// The [`Cow`] of the payload position. A publish whose stages only read it - which every
/// transform that stamps a header or a setting does - travels on the buffer that is already
/// there, and the copy happens where a stage actually asks to write.
#[derive(Debug, Clone)]
pub(crate) enum Payload<'a> {
    /// The caller's bytes, valid as long as the message is.
    Lent(&'a [u8]),
    /// A buffer of this message's own: codec output, or the copy a write asked for.
    Owned(BytesMut),
}

impl Payload<'_> {
    /// The bytes, wherever they live.
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Lent(bytes) => bytes,
            Self::Owned(buf) => buf,
        }
    }

    /// The buffer, making it this message's own on the first call.
    fn to_mut(&mut self) -> &mut BytesMut {
        if let Self::Lent(bytes) = *self {
            *self = Self::Owned(BytesMut::from(bytes));
        }
        match self {
            Self::Owned(buf) => buf,
            // The branch above leaves nothing lent, which the borrow checker cannot carry across
            // this match; `Cow::to_mut` in the standard library is written the same way.
            Self::Lent(_) => unreachable!(),
        }
    }
}

impl<'a> Outgoing<'a> {
    /// Creates an outgoing message with no headers.
    ///
    /// Pass a `&str` (a borrowed destination, the no-allocation case), a `String` (a computed
    /// owned one) or a [`Str`] (a buffer something else already holds) for `name`; pass a
    /// [`BytesMut`] (codec output moves in) or a `&[u8]` for the payload.
    #[must_use]
    pub fn new(name: impl Into<OutgoingName<'a>>, payload: impl Into<BytesMut>) -> Self {
        Self {
            name: name.into(),
            payload: Payload::Owned(payload.into()),
            headers: HeaderMap::new(),
        }
    }

    /// The same over bytes the caller keeps alive: what a publish stage rebuilding a message
    /// starts from, so a message nothing writes to is never copied.
    pub(crate) fn lending(name: impl Into<OutgoingName<'a>>, payload: &'a [u8]) -> Self {
        Self {
            name: name.into(),
            payload: Payload::Lent(payload),
            headers: HeaderMap::new(),
        }
    }

    /// The destination name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets the destination name.
    ///
    /// A literal or a borrow costs nothing, a computed `String` moves in, and a [`Str`] read off
    /// the delivery shares its buffer rather than copying out of it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::Outgoing;
    ///
    /// let mut out = Outgoing::new("answers", b"{}".as_slice());
    /// out.set_name("replies.inbox");
    /// assert_eq!(out.name(), "replies.inbox");
    /// ```
    pub fn set_name(&mut self, name: impl Into<OutgoingName<'a>>) {
        self.name = name.into();
    }

    /// The payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    /// The payload bytes, mutably (for envelope wrapping).
    ///
    /// A message that is still lending the bytes it was built from copies them here, once.
    pub fn payload_mut(&mut self) -> &mut BytesMut {
        self.payload.to_mut()
    }

    /// Replaces the payload.
    pub fn set_payload(&mut self, payload: impl Into<BytesMut>) {
        self.payload = Payload::Owned(payload.into());
    }

    /// The outgoing headers.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The outgoing headers, mutably.
    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.headers
    }

    /// The name, the payload and the header map, taken apart.
    ///
    /// What the last stage of a publish uses to hand the message on: the map the transforms
    /// filled travels into the broker's message rather than being cloned into it.
    pub(crate) fn into_parts(self) -> (OutgoingName<'a>, Payload<'a>, HeaderMap) {
        (self.name, self.payload, self.headers)
    }
}

mod sealed {
    /// Seals [`ReplyPublisher`](super::ReplyPublisher): the reply-publishing strategies are the
    /// two live sinks above, not an extension point.
    pub trait Sealed {}

    impl<P, C, PL, BL> Sealed for super::TypedPublisher<P, C, PL, BL> {}
    impl<P, C, PL, BL> Sealed for super::Transactional<P, C, PL, BL> {}
}

mod builder;
mod ext;
mod out;
mod pipeline;
mod publisher;
mod reply;
mod sink;
mod transaction;
mod transform;
mod wiring;

pub(crate) use builder::message_of;
#[cfg(test)]
pub(crate) use builder::raw_of;
pub use builder::{
    BoundSegment, EncodeOutcome, EncodedWire, HeaderSource, HeadersUnset, MapHeaders, MessageBody,
    MessageWire, MissingSegment, PayloadError, PublishAt, PublishBuilder, PublishError,
    PublishHeaders, ResolvedName, SatisfiesContract, SerializePayloadError, Serialized,
    SerializedWire, SuppliedName, TemplateAddress, TypedHeaders, WirePayload,
};
pub use ext::PublishExt;
pub use out::{
    LowerOutTransforms, NamedDestinationSend, NarrowToUse, OutPipeline, PipelinePublishError,
    SendOnlyPolicy, SlotStackUse, SlotTransforms,
};
pub use pipeline::{
    PublishDynLayer, PublishDynNext, PublishDynStack, PublishIdentity, PublishLayer, PublishNext,
    PublishPipeline, PublishStack,
};
pub use publisher::{Transactional, TypedPublisher};
pub use reply::ReplyPublisher;
pub use sink::{CallCodec, PublishCodec, PublishSink, UnnamedCodec};
pub use transaction::{
    Admits, AnyDeclared, TransactionPublishError, TransactionScope, TypedTransaction,
};
pub use transform::{
    BatchPublishTransform, BatchPublishTransformStack, BatchTransformIdentity, ContextKind,
    DestinationSettled, DestinationUse, Either, FitsOffer, FitsTaken, ForBatch, ForReply, ForSlot,
    Names, NamesDestination, NamingOffered, NamingUntaken, PublishContext, PublishTransform,
    PublishTransformIdentity, PublishTransformStack, Reads, SlotContext, for_batch,
};
pub use wiring::{
    AddBatchReplyTransform, AddReplyTransform, CodecSlotOpen, Direct, InTransaction,
    MapReplyPolicy, NameReplyCodec, PublishingDirectly, RawReplyWiring, ReplyWiring,
    TransactionalReply,
};

#[cfg(test)]
mod tests;
