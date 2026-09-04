//! The injections arena: the second body argument, built statically by the include site's
//! `.out(marker, policy)` chain.
//!
//! A body that publishes declares the arena in its `O` position - one [`Slot`] entry per
//! marker, generic over the wired live value and the include site's codec, with the broker
//! capability it needs as a mandatory bound on that value:
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::codec::Codec;
//! use ruststream::prelude::*;
//! # #[derive(serde::Deserialize, schemars::JsonSchema)]
//! # struct Order { id: u64 }
//! # #[derive(serde::Serialize, schemars::JsonSchema)]
//! # struct Event { id: u64 }
//! # impl OutgoingDestination for Event { type Form = CallerName; }
//! # impl MessageHeaders for Event { type Contract = NoHeaders; }
//! # struct Primary;
//! # impl OutSlot for Primary { const NAME: &'static str = "Primary"; }
//! # impl PublishedThrough<Primary> for Event {}
//!
//! struct Mirror;
//!
//! impl<W, E> Handle<Order, (), Outs<(Slot<Primary, W, E>,)>> for Mirror
//! where
//!     W: Publisher,
//!     E: Codec + Send + Sync,
//! {
//!     async fn handle(
//!         &self,
//!         order: &Order,
//!         outs: &Outs<(Slot<Primary, W, E>,)>,
//!         _ctx: &mut Context<'_>,
//!     ) -> Result<(), HandlerOutcome> {
//!         if outs
//!             .get(Primary)
//!             .message(&Event { id: order.id })
//!             .to("mirror")
//!             .publish()
//!             .await
//!             .is_err()
//!         {
//!             return Err(HandlerOutcome::retry());
//!         }
//!         Ok(())
//!     }
//! }
//! # }
//! ```
//!
//! The include site binds each marker with `.out(marker, policy)` in any order and seals with
//! `.build()`; a missing, duplicate or extra binding, or a policy whose live form lacks the
//! body's declared capability, fails to compile naming the marker. The capability a body states
//! is the broker vocabulary and never a broker type, so the same body mounts on a production
//! broker and on its in-process test transport unchanged. Under each of the publisher
//! capabilities the entry offers that capability's typed form, over the include site's codec and
//! the marker's `#[publishes(..)]` dictionary:
//!
//! | broker capability | typed operation on the entry | what it gives |
//! |---|---|---|
//! | [`Publisher`] | [`message`](Slot::message) | the publish builder |
//! | [`TransactionalPublisher`] | [`begin`](Slot::begin) | a [`TransactionScope`] |
//! | [`OwnedTransactions`] | [`transaction`](Slot::transaction) | a [`TypedTransaction`] |
//! | [`RequestReply`] | [`request`](RequestReply::request) | the correlated reply |
//!
//! Each capability is also delegated raw on the entry, through the attributed leaf, so a body
//! driving `begin_transaction` / `commit` / `abort` by hand keeps the slot's test-capture
//! attribution. Where a typed operation and a raw one share a name
//! ([`transaction`](Slot::transaction)), the inherent typed one wins, and the raw form stays
//! reachable as `OwnedTransactions::transaction(entry)`.
//!
//! The slot's wired value is the policy's live form itself, so a body needing a broker-defined
//! capability bounds `W` with the broker's own trait (or pins the entry to the concrete live
//! type, `Slot<Lanes, LaneRouter, E>`) and calls it directly through the entry's transparent
//! `Deref`. Everything is monomorphized: the arena is built once at startup, and a delivery only
//! ever passes a reference to it.

use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::time::Duration;

use crate::codec::Codec;
use crate::runtime::batch::BatchResult;
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::context::Context;
use crate::runtime::handler::HandlerOutcome;
use crate::runtime::inject::FromStartup;
use crate::runtime::inject::{InjectCall, InjectDef};
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publish::{
    Admits, HeadersUnset, MessageBody, OutPipeline, PublishBuilder, PublishIdentity,
    TransactionScope, TypedTransaction, message_of,
};
use crate::runtime::router::IncludeDef;
use crate::runtime::slot::{
    BindSlots, ContainsMessage, HasSlots, OutSlot, PublishedThrough, SlotPublisher,
};
use crate::{
    Connected, ConnectedBroker, HeaderMap, Name, OutgoingDestination, OutgoingMessage,
    OwnedTransactions, PairError, PublishPolicy, Publisher, RequestReply, TransactionalPublisher,
    Unnamed,
};

use super::Handle;
use super::axis::{
    Axis, AxisDocs, Deserialized, Input, Message, Page, PagePair, PagedAxis, Solo, SoloAxis,
    SoloDeserialized, SoloPair,
};
use super::eager::{construct, run_page, settle_solo};
use super::value::{HandleValue, Sealed};

// ------------------------------------------------------------------------------------- slots

/// One arena entry: the wired live value of the marker `M`, plus the publish path the include
/// site gave it - its encode codec and the pipeline every message leaving the slot travels.
///
/// The wired value is the bound policy's [`Live`](crate::PublishPolicy::Live) form, and the
/// entry is a transparent window onto it: `Deref` reaches every method the live value offers -
/// the broker capability vocabulary ([`TransactionalPublisher`](crate::TransactionalPublisher),
/// [`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply)) and
/// any broker-defined capability trait alike, so a body pins the entry to the broker's concrete
/// live type (or bounds `W` with the broker's trait) and calls it directly. Under each of the
/// core capability bounds the entry also offers that capability's typed form -
/// [`message`](Self::message), [`begin`](Self::begin), [`transaction`](Self::transaction) - over
/// the include site's codec and the marker's dictionary.
///
/// `Pipe` is that publish path: the app's own publish pipeline (the
/// [`publish_layer`](crate::runtime::RustStream::publish_layer) chain) with the slot's
/// `.transform(..)` steps composed on top. It is [`PublishIdentity`] - nothing in the way, the
/// bare leaf call - until a mount site names either, so a body generic over its entry leaves it
/// generic and bounds it with [`OutPipeline`].
///
/// `Body` is the entry's declared message set, `()` (any dictionary type) unless the
/// `#[subscriber]` parameter's third `Out` position narrows it;
/// [`message`](Slot::message) checks it at compile time (see
/// [`ContainsMessage`](crate::runtime::ContainsMessage)).
pub struct Slot<M, W, E, Pipe = PublishIdentity, Body = ()> {
    wired: SlotPublisher<W, M>,
    codec: E,
    pipeline: Pipe,
    _declared: PhantomData<fn() -> Body>,
}

impl<M, W, E, Pipe, Body> fmt::Debug for Slot<M, W, E, Pipe, Body> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

// The transparent window: the wired live value's whole surface (broker-defined capability
// traits included) is reachable without any grafting machinery. Calls that resolve on the
// entry itself (its typed operations, the delegated core capabilities) travel the slot's
// publish path and keep its test-capture attribution; calls reaching the live value through
// this `Deref` leave through the unwrapped value and bypass both, like a settled owned
// transaction's buffer.
impl<M, W, E, Pipe, Body> Deref for Slot<M, W, E, Pipe, Body> {
    type Target = W;

    fn deref(&self) -> &W {
        self.wired.inner()
    }
}

// A transaction opened on an entry admits exactly what the entry's own typed publish admits:
// the marker's dictionary, narrowed by the parameter's declared set. No bound on `W` or `E`:
// a body generic over the entry knows neither, and the gate must resolve for it.
impl<M, W, E, Pipe, Body, T, Index> Admits<T, Index> for Slot<M, W, E, Pipe, Body>
where
    T: PublishedThrough<M>,
    Body: ContainsMessage<T, Index>,
{
}

impl<M: OutSlot, W: Publisher, E: Codec + Send + Sync, Pipe: OutPipeline, Body>
    Slot<M, W, E, Pipe, Body>
{
    /// Starts a typed publish through the slot, on the message type's own wire
    /// ([`MessageWire`](crate::runtime::MessageWire)): a `serde::Serialize` value encodes with
    /// the include site's codec, a [`Serialized`](crate::runtime::Serialized) one carries its
    /// bytes and they leave as they are. The
    /// message type has to be in the marker's `#[publishes(..)]` dictionary (see
    /// [`PublishedThrough`](crate::runtime::PublishedThrough)) and, when the entry carries a
    /// declared message set, in that set (see
    /// [`ContainsMessage`](crate::runtime::ContainsMessage)); everything else - the
    /// destination and the header contract - comes from the type's `#[derive(Outgoing)]`
    /// declaration, so the builder demands exactly the positions that declaration leaves open.
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
    // The builder rides the entry itself, so a publish issued here travels the slot's publish
    // path - its transforms, then the app-wide pipeline - and is recorded against the slot by
    // the test harness on the way out.
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, &'a E, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
    {
        message_of(self, value, &self.codec)
    }
}

// The transaction openers do not travel the slot's pipeline, but the entry they borrow crosses
// the await with them, so it still has to be shareable.
impl<M: OutSlot, W: TransactionalPublisher, E: Send + Sync, Pipe: Send + Sync, Body>
    Slot<M, W, E, Pipe, Body>
{
    /// Opens the entry's broker transaction and returns the [`TransactionScope`] that owns it:
    /// publishes go through the scope, and [`commit`](TransactionScope::commit) or
    /// [`abort`](TransactionScope::abort) consume it, so a commit without a begin, a second
    /// commit, or a publish after settling do not compile.
    ///
    /// This is the borrowed kind: the wired publisher carries at most one broker-side
    /// transaction, so one scope per entry is open at a time. The scope's typed entry admits
    /// what the slot's own does (the marker's dictionary, narrowed by the parameter's declared
    /// set), and publishes issued through it keep the slot's test-capture attribution. They go
    /// to the broker as they are: the scope opens on the attributed leaf, so the slot's
    /// transforms and the app-wide pipeline stay on the direct publish path (see
    /// [`TransactionScope`]).
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to start a transaction, or when
    /// one is already open on this handle.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerOutcome, Out};
    /// use ruststream::{Outgoing, OutSlot, TransactionalPublisher, subscriber};
    /// use serde::Serialize;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "ledger.settled")]
    /// struct Settled {
    ///     id: u64,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Settled)]
    /// struct Journal;
    ///
    /// #[subscriber("ledger.orders")]
    /// async fn settle(
    ///     order: &Order,
    ///     Out(journal): Out<impl TransactionalPublisher, Journal, Settled>,
    /// ) -> HandlerOutcome {
    ///     let Ok(scope) = journal.begin().await else {
    ///         return HandlerOutcome::retry();
    ///     };
    ///     if scope.message(&Settled { id: order.id }).publish().await.is_err()
    ///         || scope.commit().await.is_err()
    ///     {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    pub async fn begin(
        &self,
    ) -> Result<TransactionScope<'_, SlotPublisher<W, M>, &'_ E, Self>, W::Error> {
        TransactionScope::open(&self.wired, &self.codec).await
    }
}

impl<M: OutSlot, W: OwnedTransactions, E: Send + Sync, Pipe: Send + Sync, Body>
    Slot<M, W, E, Pipe, Body>
{
    /// Opens an independent, caller-owned broker transaction and returns the
    /// [`TypedTransaction`] that owns it: publishes buffer into the value, and
    /// [`commit`](TypedTransaction::commit) or [`abort`](TypedTransaction::abort) consume it.
    ///
    /// Every call opens its own transaction, so any number can be open on one entry at a time;
    /// the transaction's typed entry admits what the slot's own does. The buffer settles outside
    /// the slot, so its publishes land in the broker's publish log and are not attributed to the
    /// slot (the documented capture boundary).
    ///
    /// # Errors
    ///
    /// Returns the publisher's error when the broker refuses to open a transaction; pure
    /// client-buffer implementations are infallible in practice.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # mod demo {
    /// use ruststream::runtime::{HandlerOutcome, Out};
    /// use ruststream::{Outgoing, OutSlot, OwnedTransactions, subscriber};
    /// use serde::Serialize;
    /// # #[derive(serde::Deserialize)]
    /// # struct Order { id: u64 }
    ///
    /// #[derive(Outgoing, Serialize)]
    /// #[outgoing(name = "ledger.settled")]
    /// struct Settled {
    ///     id: u64,
    /// }
    ///
    /// #[derive(OutSlot)]
    /// #[publishes(Settled)]
    /// struct Ledger;
    ///
    /// #[subscriber("ledger.orders")]
    /// async fn settle(
    ///     order: &Order,
    ///     Out(ledger): Out<impl OwnedTransactions, Ledger, Settled>,
    /// ) -> HandlerOutcome {
    ///     let Ok(mut txn) = ledger.transaction().await else {
    ///         return HandlerOutcome::retry();
    ///     };
    ///     if txn.message(&Settled { id: order.id }).publish().await.is_err()
    ///         || txn.commit().await.is_err()
    ///     {
    ///         return HandlerOutcome::retry();
    ///     }
    ///     HandlerOutcome::ack()
    /// }
    /// # }
    /// ```
    // Shares its name with `OwnedTransactions::transaction`, which the entry also implements:
    // the inherent typed form is what a body wants, and the raw one stays reachable through the
    // trait path.
    pub async fn transaction(
        &self,
    ) -> Result<TypedTransaction<W::Transaction, &'_ E, Self>, W::Error> {
        TypedTransaction::open(&self.wired, &self.codec).await
    }
}

// The broker capability vocabulary is also delegated on the entry itself (not only through
// Deref), so an entry passes into generic positions demanding the capability and a direct
// `publish` / `request` keeps the slot's test-capture attribution.
//
// `publish` is the one that travels the slot's publish path: the pipeline runs above the
// attributed leaf, so what the harness records and what the broker receives are the same
// stamped message. The transaction calls and the request round trip reach the leaf directly -
// the pipeline ends in a send, and neither of those is one - and report their errors in the
// entry's own error type.
impl<M: OutSlot, W: Publisher, E: Send + Sync, Pipe: OutPipeline, Body> Publisher
    for Slot<M, W, E, Pipe, Body>
{
    type Error = Pipe::Error<W::Error>;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.pipeline.send(&self.wired, msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.wired.base_headers()
    }
}

impl<M: OutSlot, W: TransactionalPublisher, E: Send + Sync, Pipe: OutPipeline, Body>
    TransactionalPublisher for Slot<M, W, E, Pipe, Body>
{
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.wired
            .begin_transaction()
            .await
            .map_err(Pipe::from_publish_error)
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.wired.commit().await.map_err(Pipe::from_publish_error)
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.wired.abort().await.map_err(Pipe::from_publish_error)
    }
}

impl<M: OutSlot, W: OwnedTransactions, E: Send + Sync, Pipe: OutPipeline, Body> OwnedTransactions
    for Slot<M, W, E, Pipe, Body>
{
    type Transaction = W::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.wired
            .transaction()
            .await
            .map_err(Pipe::from_publish_error)
    }
}

impl<M: OutSlot, W: RequestReply, E: Send + Sync, Pipe: OutPipeline, Body> RequestReply
    for Slot<M, W, E, Pipe, Body>
{
    type Reply = W::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        self.wired
            .request(msg, timeout)
            .await
            .map_err(Pipe::from_publish_error)
    }
}

#[cfg(test)]
impl<M, W, E, Pipe, Body> Slot<M, W, E, Pipe, Body> {
    /// Builds an entry directly for the crate's own unit tests; production entries are only
    /// ever paired by the runtime at startup.
    pub(crate) fn test_entry(wired: W, codec: E, pipeline: Pipe) -> Self {
        Self {
            wired: SlotPublisher::new(wired),
            codec,
            pipeline,
            _declared: PhantomData,
        }
    }
}

/// The injected slot: pairs the marker's attached policy against the connected broker and
/// stores the live value under the include site's publish path (its encode codec and the
/// pipeline the mount composed). A failing pair surfaces at startup with the slot's name; an
/// unbound slot never gets this far (the include site does not compile).
impl<B, Sub, Policy, E, Pipe, M, Body> FromStartup<B, Sub, (Policy, E, Pipe)>
    for Slot<M, <Policy as PublishPolicy<Connected<B>>>::Live, E, Pipe, Body>
where
    B: crate::Broker,
    Sub: Sync,
    Policy: PublishPolicy<Connected<B>> + Send,
    E: Send,
    Pipe: Send,
    M: OutSlot,
{
    async fn resolve(
        (policy, codec, pipeline): (Policy, E, Pipe),
        connected: &Connected<B>,
        _subscriber: &Sub,
    ) -> Result<Self, PairError> {
        let live = policy.pair(connected).await.map_err(|err| {
            PairError::from_boxed(Box::from(format!(
                "pairing the publisher for the `{}` slot failed: {err}",
                M::NAME,
            )))
        })?;
        Ok(Self {
            wired: SlotPublisher::new(live),
            codec,
            pipeline,
            _declared: PhantomData,
        })
    }
}

// ------------------------------------------------------------------------------- the arena

/// The injections arena a slot body receives: its entries mirror the marker tuple the body
/// declared, and [`get`](Self::get) picks one by marker.
pub struct Outs<E> {
    entries: E,
}

impl<E> fmt::Debug for Outs<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outs").finish_non_exhaustive()
    }
}

impl<E> Outs<E> {
    /// Picks the entry of `marker`: the wired live value behind its transparent [`Slot`]
    /// window. The position is inferred, so the call reads the same wherever the marker sits in
    /// the declaration.
    // The marker travels by value like every marker selector (`.out(marker, ..)`): it is a
    // unit token whose only job is naming the slot at the call site.
    #[allow(clippy::needless_pass_by_value)]
    pub fn get<M, I>(&self, marker: M) -> &<E as SelectSlot<M, I>>::Picked
    where
        E: SelectSlot<M, I>,
    {
        let _ = marker;
        self.entries.pick()
    }
}

/// Positional pick of one marker's entry; the index is inferred per call. Machinery; never
/// named in user code.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this arena has no `{M}` slot",
    note = "the body's `Outs<(..)>` declaration lists the slots it may publish through; check \
            the marker"
)]
pub trait SelectSlot<M, I> {
    /// The picked entry.
    type Picked;

    fn pick(&self) -> &Self::Picked;
}

/// The position tokens of [`SelectSlot`], one per arity slot.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct OutPos<const N: usize>;

macro_rules! impl_select_slot {
    ($(($($before:ident,)* @ $pos:literal $(, $after:ident)*))+) => {$(
        impl<M, W, E, Pipe, Body $(, $before)* $(, $after)*> SelectSlot<M, OutPos<$pos>>
            for ($($before,)* Slot<M, W, E, Pipe, Body>, $($after,)*)
        {
            type Picked = Slot<M, W, E, Pipe, Body>;

            fn pick(&self) -> &Slot<M, W, E, Pipe, Body> {
                #[allow(non_snake_case)]
                let ($($before,)* picked, $($after,)*) = self;
                $(let _ = $before;)*
                $(let _ = $after;)*
                picked
            }
        }
    )+};
}

impl_select_slot! {
    (@ 0)
    (@ 0, A1)
    (A0, @ 1)
    (@ 0, A1, A2)
    (A0, @ 1, A2)
    (A0, A1, @ 2)
}

impl<B, Sub, Extra, E> FromStartup<B, Sub, Extra> for Outs<E>
where
    B: crate::Broker,
    Sub: Sync,
    Extra: Send,
    E: FromStartup<B, Sub, Extra>,
{
    async fn resolve(
        extra: Extra,
        connected: &Connected<B>,
        subscriber: &Sub,
    ) -> Result<Self, PairError> {
        Ok(Self {
            entries: E::resolve(extra, connected, subscriber).await?,
        })
    }
}

/// The marker tuple of an arena's entry tuple, in declaration order. Machinery; never named in
/// user code.
#[doc(hidden)]
pub trait EntryMarkers {
    /// The markers, as [`HasSlots::Markers`] reports them.
    type Markers;

    /// The markers' `AsyncAPI` dictionaries, one entry set per slot.
    fn outgoing() -> Vec<OutgoingMessageMetadata>;
}

macro_rules! impl_entry_markers {
    ($(($($m:ident: $w:ident / $e:ident / $p:ident / $b:ident),+))+) => {$(
        impl<$($m: OutSlot, $w, $e, $p, $b),+> EntryMarkers
            for ($(Slot<$m, $w, $e, $p, $b>,)+)
        {
            type Markers = ($($m,)+);

            fn outgoing() -> Vec<OutgoingMessageMetadata> {
                let mut declared = Vec::new();
                $(declared.extend($m::outgoing());)+
                declared
            }
        }
    )+};
}

impl_entry_markers! {
    (M0: W0 / E0 / P0 / B0)
    (M0: W0 / E0 / P0 / B0, M1: W1 / E1 / P1 / B1)
    (M0: W0 / E0 / P0 / B0, M1: W1 / E1 / P1 / B1, M2: W2 / E2 / P2 / B2)
}

// -------------------------------------------------------------------- the slot definitions

impl<A, E, C, H, Doc> IncludeDef for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    A: Axis,
{
    type Form = A::SlotForm;
}

impl<A, E, C, H, Doc> HasSlots for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    E: EntryMarkers,
{
    type Markers = E::Markers;
}

/// Ties the declared arena to the bound policies: the entry a body left generic unifies with
/// its marker's paired live value, so the definition is its own bound form and the body's
/// capability bounds are checked right here.
macro_rules! impl_bind_slots {
    ($(($($m:ident / $p:ident: $e:ident / $pipe:ident / $b:ident),+))+) => {$(
        impl<Conn, A, C, H, Doc, $($m, $p, $e, $pipe, $b),+>
            BindSlots<Conn, ($(($p, $e, $pipe),)+)>
            for Sealed<
                HandleValue<
                    A,
                    (),
                    Outs<($(Slot<$m, <$p as PublishPolicy<Conn>>::Live, $e, $pipe, $b>,)+)>,
                    C,
                    H,
                    Doc,
                >,
            >
        where
            Conn: ConnectedBroker,
            $(
                $m: OutSlot,
                $p: PublishPolicy<Conn>,
            )+
        {
            type Bound = Self;
            type Extra = ($(($p, $e, $pipe),)+);

            fn bind(self, sources: ($(($p, $e, $pipe),)+)) -> (Self, Self::Extra) {
                (self, sources)
            }
        }
    )+};
}

impl_bind_slots! {
    (M0 / P0: E0 / Pipe0 / B0)
    (M0 / P0: E0 / Pipe0 / B0, M1 / P1: E1 / Pipe1 / B1)
    (M0 / P0: E0 / Pipe0 / B0, M1 / P1: E1 / Pipe1 / B1, M2 / P2: E2 / Pipe2 / B2)
}

impl<A, C, H, Doc, E> InjectDef for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    A: SoloAxis,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    E: EntryMarkers + Send + Sync,
{
    type Input = A::Kind;
    type Context = C;
    // See the eager cells: the settings builder carries the real source.
    type Source = Unnamed<Name>;
    type Injections = Outs<E>;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        self.0.docs.outgoing.clone().unwrap_or_else(E::outgoing)
    }
}

impl<T, C, S, H, Doc, E> InjectCall<S> for Sealed<HandleValue<Solo<T>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <Solo<T> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    T: Input<Axis = Solo<T>> + Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<T, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &T,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

impl<F, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloDeserialized<F>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloDeserialized<F> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    F: Deserialized + Send + Sync + 'static,
    for<'p> F::Output<'p>: Input<Axis = SoloDeserialized<F>>,
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<F::Output<'p>, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &[u8],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        let input = match construct::<F, C, S>(input, ctx) {
            Ok(input) => input,
            Err(outcome) => return outcome,
        };
        settle_solo(self.0.body.handle(&input, injections, ctx).await)
    }
}

impl<Hd, P, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloPair<Hd, P>, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = <SoloPair<Hd, P> as Axis>::Kind, Context = C, Injections = Outs<E>>,
    Message<Hd, P>: Input<Axis = SoloPair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<Message<Hd, P>, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &Message<Hd, P>,
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        settle_solo(self.0.body.handle(input, injections, ctx).await)
    }
}

impl<A, C, H, Doc, E> BatchInjectDef for Sealed<HandleValue<A, (), Outs<E>, C, H, Doc>>
where
    A: PagedAxis,
    C: Send + Sync,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    E: EntryMarkers + Send + Sync,
{
    type Input = A::Kind;
    type Source = Unnamed<Name>;
    type Injections = Outs<E>;
    type Context = C;

    fn source(&self) -> Unnamed<Name> {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.0.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.0
            .docs
            .input_schema
            .clone()
            .or_else(Doc::payload_schema)
    }

    fn headers_schema(&self) -> Option<String> {
        self.0
            .docs
            .headers_schema
            .clone()
            .or_else(Doc::headers_schema)
    }

    fn message_name(&self) -> Option<&'static str> {
        self.0.docs.message_name
    }

    fn message_description(&self) -> Option<&'static str> {
        self.0.docs.message_description
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        self.0.docs.outgoing.clone().unwrap_or_else(E::outgoing)
    }
}

impl<T, C, S, H, Doc, E> BatchInjectCall<S> for Sealed<HandleValue<Page<T>, (), Outs<E>, C, H, Doc>>
where
    Self: BatchInjectDef<Input = <Page<T> as Axis>::Kind, Injections = Outs<E>, Context = C>,
    [T]: Input<Axis = Page<T>>,
    T: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[T], (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[T],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> BatchResult {
        run_page(&self.0.body, injections, batch, ctx).await
    }
}

impl<Hd, P, C, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<PagePair<Hd, P>, (), Outs<E>, C, H, Doc>>
where
    Self:
        BatchInjectDef<Input = <PagePair<Hd, P> as Axis>::Kind, Injections = Outs<E>, Context = C>,
    [Message<Hd, P>]: Input<Axis = PagePair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    C: Send + Sync,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[Message<Hd, P>],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> BatchResult {
        run_page(&self.0.body, injections, batch, ctx).await
    }
}
