//! Marker-identified `Out` slots: the type-level machinery pairing each
//! [`Out`](super::Out) parameter with the publish policy attached at the include site.
//!
//! A handler declares `Out(out): Out<impl Publisher, MySlot>`; the marker (`MySlot`, or the
//! implicit [`DefaultSlot`]) identifies the slot, and the concrete publisher type is inferred
//! from the attachment's [`PublishPolicy::Live`](crate::PublishPolicy::Live) - fully monomorphized, no erasure. The pieces:
//!
//! - [`OutSlot`] / [`DefaultSlot`]: the marker vocabulary (`#[derive(OutSlot)]` for named ones).
//! - [`PublishedThrough`]: membership in a marker's `#[publishes(..)]` dictionary, so what the
//!   document reports as leaving a slot is exactly what the publish builder admits.
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

use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publish::{
    HeadersUnset, MessageBody, PublishBuilder, RawBody, message_of, raw_of,
};
#[cfg(feature = "testing")]
use crate::testing::coordinator::record_slot_publish;
use crate::{
    CallerName, ConnectedBroker, HeaderMap, OutgoingDestination, OutgoingMessage,
    OwnedTransactions, Publisher, RequestReply, TransactionalPublisher,
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
    /// `#[publishes(..)]` type. The derive fills this in; the default (a marker without a
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

/// Membership of a message type in a slot marker's `#[publishes(..)]` dictionary: the type may
/// leave through a slot identified by `Slot`.
///
/// `#[derive(OutSlot)]` emits one impl per listed type, and the publish builder's typed entry
/// point ([`TypedSlot::message`]) requires it. That is what keeps
/// the generated document honest: an unrestricted `Out<impl Publisher, Marker>` reports the
/// marker's dictionary as what the handler sends, so a message outside it would be a publish the
/// document never declared.
///
/// The membership is declared on the message type, with the slot as the parameter, so the
/// compile error names the message that is not in the dictionary.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{OutSlot, PublishedThrough};
///
/// struct Progress;
///
/// struct Events;
///
/// impl OutSlot for Events {
///     const NAME: &'static str = "Events";
/// }
///
/// // What `#[derive(OutSlot)]` + `#[publishes(Progress)]` generates:
/// impl PublishedThrough<Events> for Progress {}
///
/// fn admits<T: PublishedThrough<Slot>, Slot>() {}
/// admits::<Progress, Events>();
/// ```
#[diagnostic::on_unimplemented(
    message = "the `{Slot}` slot does not publish `{Self}`",
    note = "the marker lists what leaves through the slot, and the generated document reports \
            that list: add the type to it (`#[derive(OutSlot)] #[publishes({Self}, ..)]`), or \
            publish through a slot that lists it"
)]
pub trait PublishedThrough<Slot> {}

// The implicit slot has no declaration site to list types on, so it admits every message.
impl<T> PublishedThrough<DefaultSlot> for T {}

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

    // The slot is attribution, not policy: whatever the broker's publisher contributes to every
    // message it sends has to reach the builder through the wrapper unchanged.
    fn base_headers(&self) -> Option<&HeaderMap> {
        self.inner.base_headers()
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

/// Membership of a message type in an `Out` parameter's declared message list.
///
/// The declaration is a tuple listing types, a set-defining type (a `#[derive(MessageInfo)]` type
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
/// [`OutgoingMessageMetadata`] entry per member, each read off the member's own declaration.
///
/// Implemented by `#[derive(Outgoing)]` (the type declares itself and where it goes) and by
/// `#[derive(OutMessages)]` on a set enum (each variant's model); `()` falls back to the whole
/// dictionary. A tuple declaration needs no impl - the `#[subscriber]` macro enumerates its
/// elements itself.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "json"))]
/// # mod demo {
/// use ruststream::{OutMessages, Outgoing};
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.progress")]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.done")]
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
            #[derive(Outgoing)] type (declares itself and where it goes), or a \
            #[derive(OutMessages)] enum (declares its variants' models)"
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
/// use ruststream::runtime::{HandlerOutcome, Out};
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
/// ) -> HandlerOutcome {
///     // No headers contract on Progress: publish straight away.
///     if out.message(&Progress { percent: 100 }).publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     // ChunkDone declares DoneMeta: with_headers is demanded by the contract.
///     let done = ChunkDone { output_key: format!("out/{}", event.id) };
///     let meta = DoneMeta { task_id: event.id };
///     if out.message(&done).with_headers(&meta).publish().await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
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

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.slot.base_headers()
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
    /// The message type has to be in the marker's `#[publishes(..)]` dictionary (see
    /// [`PublishedThrough`]) and in the parameter's declared message set; everything else - the
    /// destination and the header contract - comes from the type's `#[derive(Outgoing)]`
    /// declaration, so the builder demands exactly the positions that declaration leaves open
    /// (see [`PublishBuilder`]).
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerOutcome, Out};
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
    /// ) -> HandlerOutcome {
    ///     if out.message(&Progress { percent: 100 }).publish().await.is_err() {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a P, MessageBody<'a, T>, &'a EncodeCodec, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
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
    ) -> PublishBuilder<&'a P, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(&self.slot, payload)
    }
}

/// The "not bound yet" placeholder of the `Out` slot marked `M` at the include site.
///
/// The marker rides in the type so the compile error of an incomplete registration (a
/// `.build()` whose attachment tuple still contains a `MissingSlot<..>`) names the slot that
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
/// definition's injection-extra tuple. The connected
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
