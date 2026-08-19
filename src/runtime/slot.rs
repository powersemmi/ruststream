//! Marker-identified `Out` slots: the type-level machinery pairing each
//! [`Out`](super::Out) parameter with the publish policy attached at the include site.
//!
//! A handler declares `Out(out): Out<impl Publisher, MySlot>`; the marker (`MySlot`, or the
//! implicit [`DefaultSlot`]) identifies the slot, and the concrete publisher type is inferred
//! from the attachment's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - fully monomorphized, no erasure. The pieces:
//!
//! - [`OutSlot`] / [`DefaultSlot`]: the marker vocabulary (`#[derive(OutSlot)]` for named ones).
//! - [`SlotPublisher`]: the transparent wrapper the handler actually receives; it delegates the
//!   publisher capabilities and, under the `testing` feature, attributes publishes to the slot.
//! - [`HasSlots`] / [`BindSlots`]: the macro-implemented contract on a definition (the marker
//!   list, and the instantiation of the publisher-generic definition from the bound sources).
//! - [`BindSlot`] / [`InitSlots`] / [`MissingSlot`]: the include-site builder machinery that
//!   places each `.out(marker, policy)` attachment into its marker's position, in any order;
//!   a registration commits only when no `MissingSlot` remains, so a forgotten binding is a
//!   compile error naming the slot.

use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Duration;

use serde::Serialize;
use thiserror::Error;

use crate::codec::{Codec, CodecError};
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publish::{HeadersUnset, MessageBody, Publish, RawBody, message_of, raw_of};
#[cfg(feature = "testing")]
use crate::testing::coordinator::record_slot_publish;
use crate::{
    CallerName, ConnectedBroker, MessageHeaders, NoHeaders, OutgoingDestination, OutgoingMessage,
    OwnedTransactions, Publisher, RequestReply, SerializeHeadersError, TransactionalPublisher,
    WithHeaders,
};

/// A slot marker: the identity of one [`Out`](super::Out) injection.
///
/// Markers decouple a handler signature from the concrete publisher type: the handler names the
/// slot, the include site names the policy, and the runtime pairs them. Derive it on a unit
/// struct with `#[derive(OutSlot)]` (`macros` feature); [`NAME`](Self::NAME) labels the slot in
/// startup errors and test assertions.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::OutSlot;
///
/// #[derive(Clone, Copy, Debug)]
/// struct Encoded;
///
/// impl OutSlot for Encoded {
///     const NAME: &'static str = "Encoded";
/// }
///
/// assert_eq!(<Encoded as OutSlot>::NAME, "Encoded");
/// ```
pub trait OutSlot: 'static {
    /// The human-readable slot name, used by diagnostics and test assertions.
    const NAME: &'static str;

    /// The slot's publish dictionary as `AsyncAPI` metadata, one entry per
    /// `#[publishes(..)]` pair. The derive fills this in; the default (a marker without a
    /// dictionary) publishes nothing declared. Called once at registration; never on the
    /// publish path.
    #[must_use]
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new()
    }
}

/// The implicit marker of a single unnamed `Out<impl Publisher>` parameter.
///
/// A handler with one `Out` parameter needs no marker: the parameter binds to this slot, and
/// the include site attaches its policy with the plain
/// [`publisher`](super::IncludeWith::publisher) call.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{DefaultSlot, OutSlot};
///
/// assert_eq!(<DefaultSlot as OutSlot>::NAME, "default");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DefaultSlot;

impl OutSlot for DefaultSlot {
    const NAME: &'static str = "default";
}

/// The live publisher an [`Out`](super::Out) slot injects: the attachment's paired publisher,
/// wrapped with the slot identity.
///
/// The wrapper is a zero-cost newtype (static dispatch, no boxing): every capability of the
/// underlying publisher ([`Publisher`], [`TransactionalPublisher`], [`OwnedTransactions`],
/// [`RequestReply`]) is delegated, so an `Out<impl OwnedTransactions, M>` bound holds exactly
/// when the attached policy's live form supports it. Under the `testing` feature, publishes made
/// through the wrapper inside a `TestApp`-driven handler are also recorded against the slot,
/// which is what backs the harness's per-slot assertions.
///
/// You never name this type: the handler sees it as `impl Publisher` (or a capability
/// refinement), and the include machinery constructs it.
#[derive(Debug)]
pub struct SlotPublisher<P, M> {
    inner: P,
    _slot: PhantomData<fn() -> M>,
}

impl<P, M> SlotPublisher<P, M> {
    pub(crate) fn new(inner: P) -> Self {
        Self {
            inner,
            _slot: PhantomData,
        }
    }

    /// The paired value the slot wraps: the extension point for broker-defined capabilities.
    ///
    /// The core capability vocabulary ([`Publisher`], [`TransactionalPublisher`],
    /// [`OwnedTransactions`], [`RequestReply`]) is delegated by this wrapper directly. A broker
    /// whose paired value offers more than that - or is not a publisher at all (a per-partition
    /// producer cache, a shard router) - declares its own capability trait and grafts it onto
    /// the wrapper with a blanket impl delegating through this accessor; handlers then bound
    /// their slot with that trait (`Out<impl PartitionLanes>`). The wrapper type itself never
    /// appears in handler code, so this accessor is reachable only from such generic impls.
    ///
    /// Publishes made through values obtained this way bypass the slot's test-capture
    /// attribution, like a settled owned transaction's buffer: assert on the broker's publish
    /// log for those.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{OutSlot, SlotPublisher};
    ///
    /// /// A broker-defined capability the core knows nothing about.
    /// trait PartitionLanes {
    ///     fn lane_id(&self, partition: i32) -> String;
    /// }
    ///
    /// // The broker crate grafts it onto the slot wrapper once, for every marker:
    /// impl<P: PartitionLanes, M: OutSlot> PartitionLanes for SlotPublisher<P, M> {
    ///     fn lane_id(&self, partition: i32) -> String {
    ///         self.inner().lane_id(partition)
    ///     }
    /// }
    /// ```
    #[must_use]
    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: Publisher, M: OutSlot> Publisher for SlotPublisher<P, M> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        #[cfg(feature = "testing")]
        record_slot_publish(M::NAME, &msg);
        self.inner.publish(msg).await
    }
}

impl<P: TransactionalPublisher, M: OutSlot> TransactionalPublisher for SlotPublisher<P, M> {
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.inner.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.inner.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.inner.abort().await
    }
}

// Transaction values own their buffer and leave the slot's scope, so publishes made through
// them are visible in the broker's publish log but are not attributed to the slot.
impl<P: OwnedTransactions, M: OutSlot> OwnedTransactions for SlotPublisher<P, M> {
    type Transaction = P::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.inner.transaction().await
    }
}

impl<P: RequestReply, M: OutSlot> RequestReply for SlotPublisher<P, M> {
    type Reply = P::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        #[cfg(feature = "testing")]
        record_slot_publish(M::NAME, &msg);
        self.inner.request(msg, timeout).await
    }
}

/// One entry of a slot's publish dictionary: the message type `Self` publishes to
/// [`CHANNEL`](Self::CHANNEL) through the slot marked `M`.
///
/// Declared on the marker with `#[publishes(Type = "channel", ..)]` (on `#[derive(OutSlot)]`),
/// one impl per listed type. The dictionary is what
/// [`publish_typed`](TypedSlot::publish_typed) compiles against: the destination comes from
/// the declaration, and a type outside the dictionary does not compile. Several types may
/// share one channel; one type maps to exactly one channel per slot (the derive rejects a
/// duplicate).
///
/// Deprecated: a message type now declares its own destination with `#[derive(Outgoing)]`, so
/// the marker's dictionary is a list of types (`#[publishes(ChunkDone, Progress)]`) and the
/// destination no longer depends on which slot the message leaves through.
///
/// # Examples
///
/// ```
/// # #![allow(deprecated)]
/// use ruststream::runtime::{OutMessage, OutSlot};
///
/// struct Progress {
///     percent: u8,
/// }
///
/// struct Events;
///
/// impl OutSlot for Events {
///     const NAME: &'static str = "Events";
/// }
///
/// // What `#[derive(OutSlot)]` + `#[publishes(Progress = "chunks.progress")]` generates:
/// impl OutMessage<Events> for Progress {
///     const CHANNEL: &'static str = "chunks.progress";
/// }
///
/// assert_eq!(<Progress as OutMessage<Events>>::CHANNEL, "chunks.progress");
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not in the publish dictionary of the `{M}` slot",
    note = "declare the destination on the message type instead: #[derive(Outgoing)] with \
            #[outgoing(name = \"<channel>\")], and list the type on the marker as \
            #[publishes({Self})]"
)]
#[deprecated(
    since = "0.6.4",
    note = "the destination moved onto the message type: #[derive(Outgoing)] with \
            #[outgoing(name = \"..\")], and #[publishes(Type, ..)] on the marker"
)]
pub trait OutMessage<M: OutSlot> {
    /// The channel / subject the message type publishes to through this slot.
    const CHANNEL: &'static str;
}

/// The error of the typed publish path ([`TypedSlot::publish_typed`]): encoding the payload,
/// serializing the typed headers, or the broker publish itself.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PublishTypedError<E> {
    /// The slot's codec failed to encode the message payload.
    #[error("encoding the typed message failed: {0}")]
    Encode(#[source] CodecError),
    /// The typed headers did not serialize into a header map.
    #[error("serializing the typed headers failed: {0}")]
    Headers(#[source] SerializeHeadersError),
    /// The underlying publisher failed.
    #[error("publishing the typed message failed: {0}")]
    Publish(#[source] E),
}

/// Membership of a message type in an `Out` parameter's declared message list.
///
/// The declaration is a tuple listing types, a set-defining type (a `#[derive(Message)]` type
/// declares itself, a `#[derive(OutMessages)]` enum declares its variants' models), or the
/// unrestricted `()` (any dictionary type). The `Index` parameter is inferred per call, like
/// the slot-binding machinery's positions; a duplicate type in a declaration is rejected where
/// it is declared, so the index is always unambiguous.
#[diagnostic::on_unimplemented(
    message = "`{T}` is not in this Out parameter's declared message set `{Self}`",
    note = "the third Out argument declares what the handler publishes: list the type there \
            (`Out<impl Publisher, Marker, (A, B)>`), leave the slot unrestricted with `()`, or \
            publish through another slot"
)]
pub trait ContainsMessage<T, Index> {}

/// The [`ContainsMessage`] index of the unrestricted `()` declaration.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct Unrestricted;

// The unrestricted declaration: any message the marker's dictionary admits.
impl<T> ContainsMessage<T, Unrestricted> for () {}

/// Implements [`ContainsMessage`] for each (arity, position) pair of the message-list tuple.
macro_rules! impl_contains_message {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<T $(, $before)* $(, $after)*> ContainsMessage<T, SlotPos<$pos>>
            for ($($before,)* T, $($after,)*)
        {
        }
    )+};
}

impl_contains_message! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
    (@ 0, A1, A2, A3)
    (A0, @ 1, A2, A3)
    (A0, A1, @ 2, A3)
    (A0, A1, A2, @ 3)
}

/// A declared message set's `AsyncAPI` contribution: one
/// [`OutgoingMessageMetadata`] entry per member, with channels read off the `M` dictionary.
///
/// Implemented by `#[derive(Message)]` (the type declares itself) and by
/// `#[derive(OutMessages)]` on a set enum (each variant's model); `()` falls back to the whole
/// dictionary. A tuple declaration needs no impl - the `#[subscriber]` macro enumerates its
/// elements itself. The impls carry `OutMessage<M>` bounds per member, so naming a set whose
/// member is outside the slot's dictionary fails to compile at the handler.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::{Message, OutMessages};
/// use serde::Serialize;
///
/// #[derive(Message, Serialize)]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Message, Serialize)]
/// struct ChunkDone {
///     output_key: String,
/// }
///
/// // A reusable named set: the variants' models are what a handler naming `ConvertSends` as
/// // its third Out argument publishes. The enum is never constructed.
/// #[derive(OutMessages)]
/// enum ConvertSends {
///     Progress(Progress),
///     Done(ChunkDone),
/// }
/// # }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not define a message set for the `{M}` slot",
    note = "the third Out argument is a tuple of types, `()` (unrestricted), a \
            #[derive(Message)] type (declares itself), or a #[derive(OutMessages)] enum \
            (declares its variants' models); every member must be in the marker's \
            #[publishes(..)] dictionary"
)]
pub trait OutMessages<M: OutSlot> {
    /// The set's declared outgoing messages, for the generated document. Called once at
    /// registration; never on the publish path.
    #[must_use]
    fn outgoing() -> Vec<OutgoingMessageMetadata>;
}

// The unrestricted declaration documents the whole dictionary.
impl<M: OutSlot> OutMessages<M> for () {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        M::outgoing()
    }
}

/// The live value behind an [`Out`](super::Out) parameter: the slot's publisher plus the scope
/// codec, pinned to the parameter's declared message set.
///
/// The handler receives it destructured (`Out(out)`) and publishes declared messages with the
/// builder ([`message`](Self::message) for a value, [`raw`](Self::raw) for bytes); the declared
/// publisher capability stays reachable through `Deref` (transactions, broker-defined capability
/// traits). You never name this type: the `#[subscriber]` macro wires it from the parameter
/// declaration.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::runtime::{HandlerResult, Out};
/// use ruststream::{Outgoing, OutSlot, Publisher, subscriber};
/// use serde::{Deserialize, Serialize};
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[derive(Serialize, Deserialize)]
/// struct DoneMeta {
///     task_id: u64,
/// }
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.progress")]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.done", headers = DoneMeta)]
/// struct ChunkDone {
///     output_key: String,
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(ChunkDone, Progress)]
/// struct Events;
///
/// #[subscriber("chunks.raw")]
/// async fn convert(
///     event: &Event,
///     Out(out): Out<impl Publisher, Events, (ChunkDone, Progress)>,
/// ) -> HandlerResult {
///     // No headers contract on Progress: publish straight away.
///     if out.message(&Progress { percent: 100 }).publish().await.is_err() {
///         return HandlerResult::retry();
///     }
///     // ChunkDone declares DoneMeta: with_headers is demanded by the contract.
///     let done = ChunkDone { output_key: format!("out/{}", event.id) };
///     let meta = DoneMeta { task_id: event.id };
///     if out.message(&done).with_headers(&meta).publish().await.is_err() {
///         return HandlerResult::retry();
///     }
///     HandlerResult::Ack
/// }
/// # }
/// ```
#[derive(Debug)]
pub struct TypedSlot<P, Body, M, EncodeCodec> {
    slot: P,
    codec: EncodeCodec,
    _pinned: PhantomData<fn() -> (Body, M)>,
}

impl<P, Body, M, EncodeCodec> TypedSlot<P, Body, M, EncodeCodec> {
    pub(crate) fn new(slot: P, codec: EncodeCodec) -> Self {
        Self {
            slot,
            codec,
            _pinned: PhantomData,
        }
    }
}

// The declared capability rides the publisher inside; Deref keeps its whole surface (the
// byte-level publish, transactions, broker-defined capability traits) reachable without
// re-delegating every trait on this wrapper.
impl<P, Body, M, EncodeCodec> Deref for TypedSlot<P, Body, M, EncodeCodec> {
    type Target = P;

    fn deref(&self) -> &P {
        &self.slot
    }
}

// The core capability vocabulary is also delegated directly (not only through Deref), so an
// injected slot passes into generic positions demanding the capability (`fn f(p: &impl
// Publisher)`), exactly like the wrapped slot publisher itself.
impl<P, Body, M, EncodeCodec> Publisher for TypedSlot<P, Body, M, EncodeCodec>
where
    P: Publisher,
    M: OutSlot,
    EncodeCodec: Send + Sync,
{
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.slot.publish(msg).await
    }
}

impl<P, Body, M, EncodeCodec> TransactionalPublisher for TypedSlot<P, Body, M, EncodeCodec>
where
    P: TransactionalPublisher,
    M: OutSlot,
    EncodeCodec: Send + Sync,
{
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.slot.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.slot.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.slot.abort().await
    }
}

impl<P, Body, M, EncodeCodec> OwnedTransactions for TypedSlot<P, Body, M, EncodeCodec>
where
    P: OwnedTransactions,
    M: OutSlot,
    EncodeCodec: Send + Sync,
{
    type Transaction = P::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.slot.transaction().await
    }
}

impl<P, Body, M, EncodeCodec> RequestReply for TypedSlot<P, Body, M, EncodeCodec>
where
    P: RequestReply,
    M: OutSlot,
    EncodeCodec: Send + Sync,
{
    type Reply = P::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        self.slot.request(msg, timeout).await
    }
}

impl<P, Body, M, EncodeCodec> TypedSlot<P, Body, M, EncodeCodec> {
    /// Starts a typed publish through the slot, encoded with the include site's scope codec.
    ///
    /// The message type has to be in the parameter's declared message set; everything else -
    /// the destination and the header contract - comes from the type's `#[derive(Outgoing)]`
    /// declaration, so the builder demands exactly the positions that declaration leaves open
    /// (see [`Publish`]).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerResult, Out};
    /// use ruststream::{Outgoing, OutSlot, Publisher, subscriber};
    /// use serde::Serialize;
    /// # #[derive(serde::Deserialize)]
    /// # struct Event { id: u64 }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "chunks.progress")]
    /// struct Progress {
    ///     percent: u8,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Progress)]
    /// struct Events;
    ///
    /// #[subscriber("chunks.raw")]
    /// async fn convert(
    ///     event: &Event,
    ///     Out(out): Out<impl Publisher, Events, Progress>,
    /// ) -> HandlerResult {
    ///     if out.message(&Progress { percent: 100 }).publish().await.is_err() {
    ///         return HandlerResult::retry();
    ///     }
    ///     HandlerResult::Ack
    /// }
    /// # }
    /// ```
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> Publish<&'a P, MessageBody<'a, T>, &'a EncodeCodec, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination,
    {
        message_of(&self.slot, value, &self.codec)
    }

    /// Starts a byte publish through the slot: the payload travels as it is, to the destination
    /// named with `to(..)`.
    ///
    /// The declared message set does not restrict this path - bytes carry no message type - so
    /// it stays the escape hatch for a payload the service already holds encoded.
    pub fn raw<'a, B>(
        &'a self,
        payload: &'a B,
    ) -> Publish<&'a P, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(&self.slot, payload)
    }
}

#[allow(deprecated)]
impl<P, Body, M, EncodeCodec> TypedSlot<P, Body, M, EncodeCodec>
where
    P: Publisher,
    M: OutSlot,
    EncodeCodec: Codec + Send + Sync,
{
    /// Publishes a declared message: the destination comes from the marker's
    /// `#[publishes(..)]` dictionary, the payload is encoded with the include site's scope
    /// codec. Compiles only for types in the parameter's message list whose contract carries
    /// no typed headers; a type declaring `#[message(headers(..))]` publishes through
    /// [`with_headers`](Self::with_headers) instead (forgetting the headers is a compile
    /// error).
    ///
    /// # Errors
    ///
    /// Returns [`PublishTypedError`] when encoding fails or the broker rejects the publish.
    ///
    /// # Cancel safety
    ///
    /// As cancel-safe as the underlying publisher's [`publish`](Publisher::publish): dropping
    /// the future mid-flight may leave the message in an indeterminate state.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerResult, Out};
    /// use ruststream::{Message, OutSlot, Publisher, subscriber};
    /// use serde::Serialize;
    /// # #[derive(serde::Deserialize)]
    /// # struct Event { id: u64 }
    ///
    /// #[derive(Message, Serialize)]
    /// struct Progress {
    ///     percent: u8,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Progress = "chunks.progress")]
    /// struct Events;
    ///
    /// #[subscriber("chunks.raw")]
    /// async fn convert(
    ///     event: &Event,
    ///     Out(out): Out<impl Publisher, Events, Progress>,
    /// ) -> HandlerResult {
    ///     if out.publish_typed(&Progress { percent: 100 }).await.is_err() {
    ///         return HandlerResult::retry();
    ///     }
    ///     HandlerResult::Ack
    /// }
    /// # }
    /// ```
    #[deprecated(
        since = "0.6.4",
        note = "use the publish builder: out.message(&value).publish()"
    )]
    pub async fn publish_typed<T, Index>(
        &self,
        value: &T,
    ) -> Result<(), PublishTypedError<P::Error>>
    where
        Body: ContainsMessage<T, Index>,
        T: OutMessage<M> + MessageHeaders<Contract = NoHeaders> + Serialize + Sync,
    {
        let payload = self
            .codec
            .encode(value)
            .map_err(PublishTypedError::Encode)?;
        let msg = OutgoingMessage::new(T::CHANNEL, payload.as_ref());
        self.slot
            .publish(msg)
            .await
            .map_err(PublishTypedError::Publish)
    }

    /// Supplies the typed headers for a declared message whose contract demands them:
    /// `out.with_headers(&meta).publish_typed(&value)`. The headers type is checked against the
    /// message's `#[message(headers(..))]` contract, so a mismatched or missing headers value
    /// is a compile error. The borrow is cheap; nothing is serialized until the publish.
    ///
    /// # Examples
    ///
    /// The full flow, with the contract-carrying message, lives on the [`TypedSlot`] type
    /// docs; the shape of the call:
    ///
    /// ```text
    /// out.with_headers(&DoneMeta { task_id: 7 }).publish_typed(&done).await?;
    /// ```
    #[must_use]
    #[deprecated(
        since = "0.6.4",
        note = "use the publish builder: out.message(&value).with_headers(&meta).publish()"
    )]
    pub fn with_headers<'a, H>(
        &'a self,
        headers: &'a H,
    ) -> TypedSlotWithHeaders<'a, P, Body, M, EncodeCodec, H> {
        TypedSlotWithHeaders {
            slot: self,
            headers,
        }
    }
}

/// A [`TypedSlot`] borrow paired with the typed headers of the next publish.
///
/// Built by [`TypedSlot::with_headers`]; its [`publish_typed`](Self::publish_typed) compiles
/// only for declared messages whose `#[message(headers(..))]` contract names exactly the
/// borrowed headers type. You never name this type.
#[derive(Debug)]
pub struct TypedSlotWithHeaders<'a, P, Body, M, EncodeCodec, H> {
    slot: &'a TypedSlot<P, Body, M, EncodeCodec>,
    headers: &'a H,
}

#[allow(deprecated)]
impl<P, Body, M, EncodeCodec, H> TypedSlotWithHeaders<'_, P, Body, M, EncodeCodec, H>
where
    P: Publisher,
    M: OutSlot,
    EncodeCodec: Codec + Send + Sync,
    H: Serialize + Sync,
{
    /// Publishes a declared message together with the borrowed typed headers: the destination
    /// comes from the marker's `#[publishes(..)]` dictionary, the payload is encoded with the
    /// scope codec, and the headers are serialized into the header map (one entry per field,
    /// string-encoded).
    ///
    /// # Errors
    ///
    /// Returns [`PublishTypedError`] when encoding the payload or serializing the headers
    /// fails, or the broker rejects the publish.
    ///
    /// # Cancel safety
    ///
    /// As cancel-safe as the underlying publisher's [`publish`](Publisher::publish): dropping
    /// the future mid-flight may leave the message in an indeterminate state.
    #[deprecated(
        since = "0.6.4",
        note = "use the publish builder: out.message(&value).with_headers(&meta).publish()"
    )]
    pub async fn publish_typed<T, Index>(
        &self,
        value: &T,
    ) -> Result<(), PublishTypedError<P::Error>>
    where
        Body: ContainsMessage<T, Index>,
        T: OutMessage<M> + MessageHeaders<Contract = WithHeaders<H>> + Serialize + Sync,
    {
        let payload = self
            .slot
            .codec
            .encode(value)
            .map_err(PublishTypedError::Encode)?;
        let msg = OutgoingMessage::new(T::CHANNEL, payload.as_ref())
            .with_typed_headers(self.headers)
            .map_err(PublishTypedError::Headers)?;
        self.slot
            .slot
            .publish(msg)
            .await
            .map_err(PublishTypedError::Publish)
    }
}

/// The "not bound yet" placeholder of the `Out` slot marked `M` at the include site.
///
/// The marker rides in the type so the compile error of an incomplete registration (a
/// `.mount()` whose attachment tuple still contains a `MissingSlot<..>`) names the slot that
/// was forgotten. A value of this type never reaches the runtime: committing requires every
/// position bound.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingSlot<M>(PhantomData<fn() -> M>);

impl<M> MissingSlot<M> {
    pub(crate) fn new() -> Self {
        Self(PhantomData)
    }
}

/// A user-attached publisher source, wrapped so the bound and unbound attachment states live on
/// different type constructors (disjoint impls, no negative reasoning needed).
#[doc(hidden)]
#[derive(Debug)]
pub struct WithSource<Source>(Source);

impl<Source> WithSource<Source> {
    pub(crate) fn new(source: Source) -> Self {
        Self(source)
    }
}

impl<Source> IntoSlotSource for WithSource<Source> {
    type Source = Source;

    fn into_source(self) -> Self::Source {
        self.0
    }
}

/// A definition whose handler carries marker-identified `Out` slots.
///
/// Implemented by `#[subscriber]` on the value you pass to `include`; [`Markers`](Self::Markers)
/// lists the slot markers in signature order, which is the order the positional machinery
/// ([`InitSlots`], [`BindSlot`], [`BindSlots`]) works in. You never implement this by hand
/// unless you hand-write a definition.
pub trait HasSlots {
    /// The slot markers, as a tuple in the handler signature's parameter order.
    type Markers;
}

/// Builds the initial all-unbound attachment tuple for a definition's markers. Machinery behind
/// `include`; never named directly.
#[doc(hidden)]
pub trait InitSlots {
    /// One [`MissingSlot`] per marker, carrying it.
    type Init;

    fn init() -> Self::Init;
}

/// Unwraps one include-site attachment into the policy the runtime pairs: a bound
/// [`WithSource`] yields its policy. Machinery; never named directly.
#[doc(hidden)]
pub trait IntoSlotSource {
    /// The publish policy this attachment resolves to.
    type Source;

    fn into_source(self) -> Self::Source;
}

/// Positional placement of one `.out(marker, policy)` attachment: finds the (still unbound)
/// element carrying the marker and replaces it. The `Index` parameter is inferred per call,
/// which is what makes the calls order-independent; binding the same slot twice finds no
/// still-unbound element and fails to compile. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler has no unbound Out slot marked `{M}`",
    note = "each .out(marker, policy) call binds one still-unbound slot the handler declares; \
            check the marker, and that the slot was not bound twice"
)]
pub trait BindSlot<M, Src, Index> {
    /// The attachment tuple with the marker's position bound.
    type Out;

    fn bind(self, src: Src) -> Self::Out;
}

/// The position tokens inferred by [`BindSlot`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct SlotPos<const N: usize>;

/// Instantiates a slot-carrying definition from the bound sources.
///
/// Implemented by `#[subscriber]` next to [`HasSlots`]: given the source tuple (in marker
/// order), it names the fully-applied definition type - each slot's publisher is
/// [`SlotPublisher`] over the source's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - and pads the sources into the
/// definition's injection-extra tuple (a unit for a trailing `Seek` parameter). The connected
/// broker type `C` is the pairing target the publisher types are computed against.
pub trait BindSlots<C: ConnectedBroker, Sources>: Sized {
    /// The publisher-applied definition type.
    type Bound;

    /// The extra tuple [`FromStartup`](super::FromStartup) resolves the injections against:
    /// the sources, padded to the injection tuple's arity.
    type Extra;

    /// Instantiates the definition and arranges the sources for resolution.
    fn bind(self, sources: Sources) -> (Self::Bound, Self::Extra);
}

/// Implements [`InitSlots`] for each marker-tuple arity: every position starts as its marker's
/// [`MissingSlot`].
macro_rules! impl_init_slots {
    ($(($($marker:ident),+))+) => {$(
        impl<$($marker),+> InitSlots for ($($marker,)+) {
            type Init = ($(MissingSlot<$marker>,)+);

            fn init() -> Self::Init {
                ($(MissingSlot::<$marker>::new(),)+)
            }
        }
    )+};
}

impl_init_slots! {
    (M0)
    (M0, M1)
    (M0, M1, M2)
}

/// Implements [`BindSlot`] for each (arity, position) pair: the element carrying the marker
/// (`@ <position>`) becomes [`WithSource`], the surrounding elements pass through unchanged.
macro_rules! impl_bind_slot {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<M, Src $(, $before)* $(, $after)*> BindSlot<M, Src, SlotPos<$pos>>
            for ($($before,)* MissingSlot<M>, $($after,)*)
        {
            type Out = ($($before,)* WithSource<Src>, $($after,)*);

            fn bind(self, src: Src) -> Self::Out {
                #[allow(non_snake_case)]
                let ($($before,)* _missing, $($after,)*) = self;
                ($($before,)* WithSource::new(src), $($after,)*)
            }
        }
    )+};
}

impl_bind_slot! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

#[cfg(test)]
mod tests;
