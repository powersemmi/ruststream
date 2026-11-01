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

use alloc::vec::Vec;
use core::marker::PhantomData;
use core::time::Duration;

use crate::metadata::OutgoingMessageMetadata;
use crate::publish::{DestinationUse, Names, Reads};
#[cfg(feature = "testing")]
use crate::testing::coordinator::{PipelinePublish, record_slot_publish};
use crate::{
    CallerName, ConnectedBroker, FixedName, HeaderMap, NameTemplate, OutgoingDestination,
    OutgoingFor, OwnedTransactions, Publisher, RequestReply, TransactionalPublisher,
};

mod position;

pub use position::{
    AdmitsAt, AdmitsSlotAt, AnyTransform, BatchTransformLast, BindAt, CodecAt, CodecLast,
    MapPolicyAt, MapPolicyLast, MountOffer, NamedStep, NoOutBound, OutAttachment, OutPosition,
    PositionOptions, ReplyLast, ReplyOffer, ReplyOpen, ReplyProbe, ReplyStep, SlotCodec,
    SlotPolicy, SlotProbe, TransactionalLast, TransformAt, TransformLast, TransformPosition,
    TransformProbe,
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
/// #[outgoing(name = "audit")]
/// struct Audited {
///     id: u64,
/// }
///
/// /// The slot the handler names; `Audit` is how startup errors and `tb.out::<Audit>()` call it.
/// #[derive(OutSlot)]
/// #[publishes(Audited)]
/// struct Audit;
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order, Out(audit): Out<impl Publisher, Audit>) -> HandlerOutcome {
///     match audit.message(&Audited { id: order.id }).publish().await {
///         Ok(()) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(accept).out(Audit, Publish)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
///
/// # Extending the vocabulary (broker authors)
///
/// An `Out<impl X, Marker>` parameter accepts any `X` the live value behind the slot implements,
/// and the core delegates its own capabilities ([`Publisher`],
/// [`TransactionalPublisher`], [`OwnedTransactions`], [`RequestReply`]). A broker whose live value
/// offers more, or is not a publisher at all (a per-partition producer cache, a shard router),
/// declares a capability trait of its own and implements it for that value.
///
/// The body holds the arena entry, [`Slot`](super::Slot), a transparent window onto the value.
/// Autoderef carries a method call through it but not a trait bound, so a helper written as
/// `fn issue<L: Lanes>(lanes: &L)` rejects the entry. One blanket impl over `Slot` that delegates
/// through its `Deref` makes helpers and bodies generic over the capability take the entry as it
/// is, and the concrete type still never appears in application code.
///
/// A capability like this is router-shaped: it hands out a publisher and never sends itself. A
/// publish through that publisher passes outside the slot view, so the test harness does not
/// attribute it to the slot; a test asserts it on the broker's publish log. A capability that sets
/// one per-message setting and ends in one publish is no trait at all: it is a field of
/// [`Publisher::Options`](crate::Publisher::Options) and a step on the publish builder.
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::memory::{ConnectedMemoryBroker, MemoryPublisher};
/// use ruststream::runtime::Slot;
/// use ruststream::PairError;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize, Outgoing)]
/// struct Event {
///     id: u64,
/// }
///
/// /// The broker's live value: a lane router, not a publisher.
/// pub struct LaneRouter {
///     publisher: MemoryPublisher,
/// }
///
/// /// The broker-defined capability: pick a destination lane for a shard.
/// pub trait ShardLanes {
///     fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str);
/// }
///
/// impl ShardLanes for LaneRouter {
///     fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str) {
///         let dest = if shard % 2 == 0 { "lanes.even" } else { "lanes.odd" };
///         (&self.publisher, dest)
///     }
/// }
///
/// // The one blanket impl: the entry a body holds now satisfies the bound.
/// impl<M, W: ShardLanes, E, Pipe, Body> ShardLanes for Slot<M, W, E, Pipe, Body> {
///     fn lane(&self, shard: u64) -> (&MemoryPublisher, &'static str) {
///         (**self).lane(shard)
///     }
/// }
///
/// /// The policy that pairs into the router.
/// pub struct LanePolicy;
///
/// impl PublishPolicy<ConnectedMemoryBroker> for LanePolicy {
///     type Live = LaneRouter;
///
///     async fn pair(self, connected: &ConnectedMemoryBroker) -> Result<LaneRouter, PairError> {
///         Ok(LaneRouter { publisher: Publish.pair(connected).await? })
///     }
/// }
///
/// async fn sent<L: ShardLanes + Sync>(lanes: &L, event: &Event) -> bool {
///     let (publisher, dest) = lanes.lane(event.id);
///     publisher.message(event).to(dest).publish().await.is_ok()
/// }
///
/// #[subscriber("events")]
/// async fn route(event: &Event, Out(lanes): Out<impl ShardLanes>) -> HandlerOutcome {
///     if sent(lanes, event).await { HandlerOutcome::ack() } else { HandlerOutcome::retry() }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("lanes", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(route).out(DefaultSlot, LanePolicy)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
pub trait OutSlot: 'static {
    /// The human-readable slot name, used by diagnostics and test assertions.
    const NAME: &'static str;

    /// The most a transform on this slot may do to a message's destination.
    ///
    /// [`Names`] only where every type the slot publishes leaves its destination open, because a
    /// type that fixes its own channel is published there and the generated document says so.
    /// `#[derive(OutSlot)]` folds this out of the `#[publishes(..)]` list through
    /// [`ListOffer`]; a marker with no list has nothing to fold and writes [`Reads`], which is
    /// what the implicit [`DefaultSlot`] does - it admits every message, so it can promise
    /// nothing. Stable Rust has no default for an associated type, so every marker writes the
    /// line.
    type Destination: DestinationUse;

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
/// A handler with one `Out` parameter needs no marker: the parameter binds to this slot, and the
/// include site attaches its policy by naming the marker like any other, `.out(DefaultSlot,
/// policy)`.
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
/// #[outgoing(name = "orders.accepted")]
/// struct Accepted {
///     id: u64,
/// }
///
/// /// One `Out` parameter and no marker: it binds to `DefaultSlot`.
/// #[subscriber("orders")]
/// async fn accept(order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
///     match out.message(&Accepted { id: order.id }).publish().await {
///         Ok(()) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(accept).out(DefaultSlot, Publish)
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DefaultSlot;

// The implicit slot admits every declared message, so it can promise nothing about their
// destinations and lets no transform name one.
impl OutSlot for DefaultSlot {
    const NAME: &'static str = "default";
    type Destination = Reads;
}

/// The marker of a handler's reply position: `.out(Reply, policy)` names the publish policy the
/// value a `reply("dest")` handler returns leaves through.
///
/// A mount site attaches every publish policy with one verb, and this is the marker of the one
/// position that is not an [`Out`](super::Out) slot. It carries no dictionary: what a reply may be
/// is the reply type's own [`ReplyShape`](crate::ReplyShape), while what leaves a slot is
/// the slot marker's `#[publishes(..)]` list.
///
/// Without the call the reply takes the broker's own
/// [`DefaultPublish`](crate::DefaultPublish) policy, so naming it is the exception, not the rule.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// # use ruststream::subscriber;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
/// # #[derive(serde::Serialize, schemars::JsonSchema, ruststream::Outgoing)]
/// # struct Confirmation { id: u64 }
///
/// #[subscriber("orders", reply("confirmations"))]
/// async fn confirm(order: &Order) -> Confirmation {
///     Confirmation { id: order.id }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(confirm).out(Reply, Publish)
///     })
/// }
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Reply;

/// The reply attachment of a registration whose handler declares no reply: there is no
/// [`Reply`] position to bind, so `.out(Reply, ..)` on it does not compile.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoReply;

/// Membership of a message type in a slot marker's `#[publishes(..)]` dictionary: the type may
/// leave through a slot identified by `Slot`.
///
/// `#[derive(OutSlot)]` emits one impl per listed type, and the publish builder's typed entry
/// point ([`Slot::message`](crate::Slot::message)) requires it. That is what keeps
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
/// #[outgoing(name = "chunks.progress")]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "billing.invoices")]
/// struct Invoice {
///     id: u64,
/// }
///
/// /// `Progress: PublishedThrough<Events>`, and nothing else is.
/// #[derive(OutSlot)]
/// #[publishes(Progress)]
/// struct Events;
///
/// #[subscriber("orders")]
/// async fn track(order: &Order, Out(events): Out<impl Publisher, Events>) -> HandlerOutcome {
///     tracing::info!(order.id, "tracking");
///     match events.message(&Progress { percent: 100 }).publish().await {
///         Ok(()) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
/// # }
/// # fn main() {}
/// ```
///
/// A message outside the dictionary does not compile:
///
/// ```compile_fail
/// # #[cfg(not(feature = "json"))]
/// # compile_error!("the example needs the macros, memory and json features");
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
/// #[outgoing(name = "chunks.progress")]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "billing.invoices")]
/// struct Invoice {
///     id: u64,
/// }
///
/// /// `Progress: PublishedThrough<Events>`, and nothing else is.
/// #[derive(OutSlot)]
/// #[publishes(Progress)]
/// struct Events;
///
/// #[subscriber("orders")]
/// async fn track(order: &Order, Out(events): Out<impl Publisher, Events>) -> HandlerOutcome {
///     // An invoice is not in the slot's dictionary: the publish names it in the error.
///     match events.message(&Invoice { id: order.id }).publish().await {
///         Ok(()) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
/// # }
/// # fn main() {}
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

/// What one destination form lets a transform do: [`Names`] where nothing is declared, [`Reads`]
/// where something is.
///
/// A type deriving `Outgoing` without `#[outgoing(name = "..")]` is published where `.to(..)`
/// says, so a transform that names the destination replaces a choice the call site was making
/// anyway. A type that fixes its own channel is published there and the document says so, and
/// nothing may move it.
#[doc(hidden)]
pub trait DestinationOffer {
    /// The most a transform may do to a message declared this way.
    type Offer: DestinationUse;
}

impl DestinationOffer for CallerName {
    type Offer = Names;
}

impl DestinationOffer for FixedName {
    type Offer = Reads;
}

impl DestinationOffer for NameTemplate {
    type Offer = Reads;
}

/// The offer of a whole `#[publishes(..)]` dictionary: [`Names`] only while every member leaves
/// its destination open, because one member that fixes its channel settles the whole slot.
///
/// The dual of [`Either`](crate::publish::Either), which combines what a transform stack
/// does. Machinery behind [`OutSlot::Destination`].
#[doc(hidden)]
pub trait Both<Rhs> {
    /// The combined offer.
    type Out;
}

impl Both<Self> for Names {
    type Out = Self;
}

impl Both<Reads> for Names {
    type Out = Reads;
}

impl<Rhs> Both<Rhs> for Reads {
    type Out = Self;
}

/// A slot marker's `#[publishes(..)]` dictionary as a type-level list, `(First, (Second, ()))`,
/// folded into what the slot offers a transform.
///
/// [`OutSlot::outgoing`] reports the same list as data, for the generated document; this folds it
/// as types, so `#[derive(OutSlot)]` computes [`OutSlot::Destination`] from the list rather than
/// the author reasoning it out. A hand-written marker either projects the fold the same way or
/// writes the offer directly.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::prelude::*;
/// use ruststream::runtime::ListOffer;
/// use serde::Serialize;
///
/// #[derive(Outgoing, Serialize)]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Outgoing, Serialize)]
/// #[outgoing(name = "chunks.done")]
/// struct ChunkDone {
///     key: String,
/// }
///
/// struct Events;
///
/// // What `#[derive(OutSlot)] #[publishes(ChunkDone, Progress)]` writes. `ChunkDone` fixes its
/// // channel, so the fold comes out `Reads` and the slot lets no transform name a destination.
/// impl OutSlot for Events {
///     const NAME: &'static str = "Events";
///     type Destination = <(ChunkDone, (Progress, ())) as ListOffer>::Offer;
/// }
/// # }
/// ```
pub trait ListOffer {
    /// What every member of the list allows, taken together.
    type Offer: DestinationUse;
}

impl ListOffer for () {
    type Offer = Names;
}

impl<Head, Tail> ListOffer for (Head, Tail)
where
    Head: OutgoingDestination<Form: DestinationOffer>,
    Tail: ListOffer,
    <<Head as OutgoingDestination>::Form as DestinationOffer>::Offer:
        Both<Tail::Offer, Out: DestinationUse>,
{
    type Offer = <<<Head as OutgoingDestination>::Form as DestinationOffer>::Offer as Both<
        Tail::Offer,
    >>::Out;
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

    /// The paired value this wrapper delegates to.
    ///
    /// This is the wrapper's own delegation seam, not the extension point for broker-defined
    /// capabilities: a handler body holds the arena entry
    /// ([`Slot`](crate::Slot)), never this wrapper, so a trait grafted here is
    /// unreachable from one. A broker whose paired value offers more than the core capability
    /// vocabulary ([`Publisher`], [`TransactionalPublisher`], [`OwnedTransactions`],
    /// [`RequestReply`]) - or is not a publisher at all (a per-partition producer cache, a shard
    /// router) - implements its capability trait for the live value and grafts it onto the entry
    /// instead, as [`Slot`](crate::Slot) documents.
    ///
    /// Publishes made through the value obtained here bypass the slot's test-capture
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
    /// // The wrapper's own delegation: a slot entry reaches the live value through it. The
    /// // impl a handler body needs is the one on `Slot`, not this one.
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
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = P::Options;

    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        #[cfg(feature = "testing")]
        let hooks = record_slot_publish(M::NAME, &msg, options);
        let sent = self.inner.publish(msg, options).await;
        #[cfg(feature = "testing")]
        hooks.run().await;
        sent
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
        msg: OutgoingFor<'_, Self::Payload>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        // A request takes no call-site options, so what it carried is the policy's defaults.
        #[cfg(feature = "testing")]
        let hooks = record_slot_publish::<P::Options, _>(M::NAME, &msg, None);
        // A request reaches the broker directly rather than through the publish pipeline, so it
        // is recorded here for the channel view as well.
        #[cfg(feature = "testing")]
        let recorded = PipelinePublish::capture::<_, P::Options>(&msg, None);
        let reply = self.inner.request(msg, timeout).await;
        #[cfg(feature = "testing")]
        if reply.is_ok() {
            recorded.sent().await;
        }
        #[cfg(feature = "testing")]
        hooks.run().await;
        reply
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
/// #[outgoing(name = "chunks.progress")]
/// struct Progress {
///     percent: u8,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "chunks.done")]
/// struct ChunkDone {
///     output_key: String,
/// }
///
/// /// A reusable named set: the variants' models are what a handler naming `ConvertSends` as
/// /// its third `Out` argument publishes. The enum is never constructed.
/// #[derive(OutMessages)]
/// enum ConvertSends {
///     Progress(Progress),
///     Done(ChunkDone),
/// }
///
/// #[derive(OutSlot)]
/// #[publishes(Progress, ChunkDone)]
/// struct Events;
///
/// #[subscriber("orders")]
/// async fn convert(
///     order: &Order,
///     Out(events): Out<impl Publisher, Events, ConvertSends>,
/// ) -> HandlerOutcome {
///     let done = ChunkDone {
///         output_key: format!("out/{}", order.id),
///     };
///     if events
///         .message(&Progress { percent: 100 })
///         .publish()
///         .await
///         .is_err()
///         || events.message(&done).publish().await.is_err()
///     {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("convert", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(convert).out(Events, Publish)
///     })
/// }
/// # }
/// # fn main() {}
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

    /// Borrows the wrapped source, so a mount can read what it declares before consuming it.
    pub(crate) const fn source(&self) -> &Source {
        &self.0
    }

    /// Grows the wrapped source in place: how a mount site's reply chain adds one step to the
    /// wiring it already attached.
    pub(crate) fn map<NewSource>(
        self,
        f: impl FnOnce(Source) -> NewSource,
    ) -> WithSource<NewSource> {
        WithSource(f(self.0))
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

/// [`HasSlots`] for a definition whose body is a [`LocalHandle`](crate::LocalHandle): its arena
/// holds [`LocalSlot`](crate::LocalSlot) entries, which is what a local chain reads to bind
/// them. Machinery; never named directly.
#[doc(hidden)]
pub trait LocalHasSlots {
    /// The slot markers, in the handler signature's parameter order.
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

    /// The extra tuple the startup resolution (`FromStartup`) resolves the injections against:
    /// the sources, padded to the injection tuple's arity.
    type Extra;

    /// Instantiates the definition and arranges the sources for resolution.
    fn bind(self, sources: Sources) -> (Self::Bound, Self::Extra);
}

/// [`BindSlots`] for a chain started with [`Router::local`](crate::Router::local): instantiates a
/// slot-carrying definition from sources paired against a broker of the local family.
///
/// Implemented next to [`HasSlots`] by a definition whose body is a
/// [`LocalHandle`](crate::LocalHandle); each slot is a [`LocalSlot`](crate::LocalSlot) over the
/// source's [`PublishPolicy::Live`](crate::PublishPolicy::Live). Machinery; never named
/// directly.
#[doc(hidden)]
pub trait LocalBindSlots<C: ConnectedBroker, Sources>: Sized {
    /// The publisher-applied definition type.
    type Bound;

    /// The extra tuple the startup resolution resolves the injections against.
    type Extra;

    /// Instantiates the definition and arranges the sources for resolution.
    fn bind_local(self, sources: Sources) -> (Self::Bound, Self::Extra);
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
