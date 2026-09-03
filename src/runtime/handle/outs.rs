//! The injections arena: the second body argument, built statically by the include site's
//! `.out(marker, policy)` chain.
//!
//! A body that publishes declares the arena in its `O` position - one [`Slot`] entry per
//! marker, generic over the wired live value and the include site's codec, with the capability
//! it needs as a mandatory bound:
//!
//! ```
//! # #[cfg(all(feature = "memory", feature = "json"))]
//! # mod demo {
//! use ruststream::prelude::*;
//! use ruststream::runtime::{Outs, Publish, PublishedThrough, Slot};
//! # #[derive(serde::Deserialize, schemars::JsonSchema)]
//! # struct Order { id: u64 }
//! # #[derive(serde::Serialize, schemars::JsonSchema)]
//! # struct Event { id: u64 }
//! # impl ruststream::OutgoingDestination for Event { type Form = ruststream::CallerName; }
//! # impl ruststream::MessageHeaders for Event { type Contract = ruststream::NoHeaders; }
//! # struct Primary;
//! # impl OutSlot for Primary { const NAME: &'static str = "Primary"; }
//! # impl PublishedThrough<Primary> for Event {}
//!
//! struct Mirror;
//!
//! impl<W, E> Handle<Order, (), Outs<(Slot<Primary, W, E>,)>> for Mirror
//! where
//!     Slot<Primary, W, E>: Publish,
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
//! body's declared capability, fails to compile naming the marker. The slot's wired value is
//! the policy's live form itself: a body needing a broker-defined capability pins the entry to
//! the concrete live type (`Slot<Lanes, LaneRouter, E>`) - or bounds `W` with the broker's own
//! capability trait - and calls it directly through the entry's transparent `Deref`. Everything
//! is monomorphized: the arena is built once at startup, and a delivery only ever passes a
//! reference to it.

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
use crate::runtime::publish::{HeadersUnset, MessageBody, PublishBuilder, message_of};
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
use super::eager::{construct, settle_page, settle_solo};
use super::value::{HandleValue, Sealed};

// ------------------------------------------------------------------------------------- slots

/// One arena entry: the wired live value of the marker `M`, plus the include site's encode
/// codec.
///
/// The wired value is the bound policy's [`Live`](crate::PublishPolicy::Live) form, and the
/// entry is a transparent window onto it: `Deref` reaches every method the live value offers -
/// the core capability vocabulary ([`TransactionalPublisher`](crate::TransactionalPublisher),
/// [`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply)) and
/// any broker-defined capability trait alike, so a body pins the entry to the broker's concrete
/// live type (or bounds `W` with the broker's trait) and calls it directly. A publisher-shaped
/// entry additionally offers the typed publish builder through [`Publish`].
///
/// `Body` is the entry's declared message set, `()` (any dictionary type) unless the
/// `#[subscriber]` parameter's third `Out` position narrows it; [`message`](Self::message)
/// checks it at compile time (see [`ContainsMessage`]).
pub struct Slot<M, W, E, Body = ()> {
    wired: SlotPublisher<W, M>,
    codec: E,
    _declared: PhantomData<fn() -> Body>,
}

impl<M, W, E, Body> fmt::Debug for Slot<M, W, E, Body> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

// The transparent window: the wired live value's whole surface (broker-defined capability
// traits included) is reachable without any grafting machinery. Calls that resolve on the
// entry itself (`Publish::message`, the delegated core capabilities) keep the slot's
// test-capture attribution; calls reaching the live value through this `Deref` leave through
// the unwrapped value and bypass it, like a settled owned transaction's buffer.
impl<M, W, E, Body> Deref for Slot<M, W, E, Body> {
    type Target = W;

    fn deref(&self) -> &W {
        self.wired.inner()
    }
}

/// The publish capability of a wired slot: what a body's mandatory bound names to start typed
/// publishes through the slot.
///
/// The bound is stated on the whole entry (`Slot<Marker, W, E>: Publish`), and holds exactly
/// when the bound policy's live form is a [`Publisher`] and the include site's codec encodes.
/// The other capabilities keep their own vocabulary: a slot whose body begins transactions
/// bounds its `W` parameter with [`TransactionalPublisher`](crate::TransactionalPublisher) (or
/// [`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply), a
/// broker-defined trait) next to - or instead of - this one, and the include site's policy must
/// pair a live form carrying it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a publish-capable slot entry",
    note = "a slot body's publish bound is stated on the whole entry: `impl<W, E> Handle<T, (), \
            Outs<(Slot<Marker, W, E>,)>> for Body where Slot<Marker, W, E>: Publish`"
)]
pub trait Publish: Send + Sync {
    /// The attributed live publisher under the entry. The bound is stated here so a body
    /// generic over the entry can drive the whole publish builder off the one `Publish` bound.
    #[doc(hidden)]
    type Leaf: Publisher;

    /// The encode codec of the include site.
    #[doc(hidden)]
    type EncodeCodec: Codec + Send + Sync;

    #[doc(hidden)]
    fn leaf(&self) -> &Self::Leaf;

    #[doc(hidden)]
    fn encode_codec(&self) -> &Self::EncodeCodec;
}

impl<M: OutSlot, W: Publisher, E: Codec + Send + Sync, Body> Publish for Slot<M, W, E, Body> {
    type Leaf = SlotPublisher<W, M>;
    type EncodeCodec = E;

    fn leaf(&self) -> &SlotPublisher<W, M> {
        &self.wired
    }

    fn encode_codec(&self) -> &E {
        &self.codec
    }
}

impl<M: OutSlot, W, E, Body> Slot<M, W, E, Body> {
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
    // The builder is spelled through the `Publish` projections (not `W` and `E` directly) so a
    // body generic over the entry needs only its declared `Slot<..>: Publish` bound to publish.
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<
        &'a <Self as Publish>::Leaf,
        MessageBody<'a, T>,
        &'a <Self as Publish>::EncodeCodec,
        HeadersUnset,
        T::Form,
    >
    where
        Self: Publish,
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
    {
        message_of(self.leaf(), value, self.encode_codec())
    }
}

// The core capability vocabulary is also delegated on the entry itself (not only through
// Deref), so an entry passes into generic positions demanding the capability and a direct
// `publish` / `request` keeps the slot's test-capture attribution.
impl<M: OutSlot, W: Publisher, E: Send + Sync, Body> Publisher for Slot<M, W, E, Body> {
    type Error = W::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.wired.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.wired.base_headers()
    }
}

impl<M: OutSlot, W: TransactionalPublisher, E: Send + Sync, Body> TransactionalPublisher
    for Slot<M, W, E, Body>
{
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.wired.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.wired.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.wired.abort().await
    }
}

impl<M: OutSlot, W: OwnedTransactions, E: Send + Sync, Body> OwnedTransactions
    for Slot<M, W, E, Body>
{
    type Transaction = W::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.wired.transaction().await
    }
}

impl<M: OutSlot, W: RequestReply, E: Send + Sync, Body> RequestReply for Slot<M, W, E, Body> {
    type Reply = W::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        self.wired.request(msg, timeout).await
    }
}

#[cfg(test)]
impl<M, W, E, Body> Slot<M, W, E, Body> {
    /// Builds an entry directly for the crate's own unit tests; production entries are only
    /// ever paired by the runtime at startup.
    pub(crate) fn test_entry(wired: W, codec: E) -> Self {
        Self {
            wired: SlotPublisher::new(wired),
            codec,
            _declared: PhantomData,
        }
    }
}

/// The injected slot: pairs the marker's attached policy against the connected broker and
/// stores the live value under the include site's encode codec. A failing pair surfaces at
/// startup with the slot's name; an unbound slot never gets this far (the include site does not
/// compile).
impl<B, Sub, Policy, E, M, Body> FromStartup<B, Sub, (Policy, E)>
    for Slot<M, <Policy as PublishPolicy<Connected<B>>>::Live, E, Body>
where
    B: crate::Broker,
    Sub: Sync,
    Policy: PublishPolicy<Connected<B>> + Send,
    E: Send,
    M: OutSlot,
{
    async fn resolve(
        (policy, codec): (Policy, E),
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
        impl<M, W, E, Body $(, $before)* $(, $after)*> SelectSlot<M, OutPos<$pos>>
            for ($($before,)* Slot<M, W, E, Body>, $($after,)*)
        {
            type Picked = Slot<M, W, E, Body>;

            fn pick(&self) -> &Slot<M, W, E, Body> {
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
    ($(($($m:ident: $w:ident / $e:ident / $b:ident),+))+) => {$(
        impl<$($m: OutSlot, $w, $e, $b),+> EntryMarkers for ($(Slot<$m, $w, $e, $b>,)+) {
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
    (M0: W0 / E0 / B0)
    (M0: W0 / E0 / B0, M1: W1 / E1 / B1)
    (M0: W0 / E0 / B0, M1: W1 / E1 / B1, M2: W2 / E2 / B2)
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
    ($(($($m:ident / $p:ident: $e:ident / $b:ident),+))+) => {$(
        impl<Conn, A, C, H, Doc, $($m, $p, $e, $b),+> BindSlots<Conn, ($(($p, $e),)+)>
            for Sealed<
                HandleValue<
                    A,
                    (),
                    Outs<($(Slot<$m, <$p as PublishPolicy<Conn>>::Live, $e, $b>,)+)>,
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
            type Extra = ($(($p, $e),)+);

            fn bind(self, sources: ($(($p, $e),)+)) -> (Self, Self::Extra) {
                (self, sources)
            }
        }
    )+};
}

impl_bind_slots! {
    (M0 / P0: E0 / B0)
    (M0 / P0: E0 / B0, M1 / P1: E1 / B1)
    (M0 / P0: E0 / B0, M1 / P1: E1 / B1, M2 / P2: E2 / B2)
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
        let verdict = self.0.body.handle(batch, injections, ctx).await;
        settle_page(verdict, batch.len(), ctx.name())
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
        let verdict = self.0.body.handle(batch, injections, ctx).await;
        settle_page(verdict, batch.len(), ctx.name())
    }
}
