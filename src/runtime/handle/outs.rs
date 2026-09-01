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
    HeadersUnset, MessageBody, PublishBuilder, RawBody, message_of, raw_of,
};
use crate::runtime::router::IncludeDef;
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot, PublishedThrough, SlotPublisher};
use crate::{
    CallerName, Connected, ConnectedBroker, HeaderMap, Name, OutgoingDestination, OutgoingMessage,
    OwnedTransactions, PairError, PublishPolicy, Publisher, RequestReply, TransactionalPublisher,
    Unnamed,
};

use super::Handle;
use super::axis::{
    Axis, AxisDocs, Input, Message, Page, PagePair, PagedAxis, Payload, Solo, SoloAxis, SoloBytes,
    SoloPair,
};
use super::eager::{settle_page, settle_solo};
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
/// entry additionally offers the typed and raw publish builders through [`Publish`].
pub struct Slot<M, W, E> {
    wired: SlotPublisher<W, M>,
    codec: E,
}

impl<M, W, E> fmt::Debug for Slot<M, W, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

// The transparent window: the wired live value's whole surface (broker-defined capability
// traits included) is reachable without any grafting machinery. Calls that resolve on the
// entry itself (`Publish::message`, the delegated core capabilities) keep the slot's
// test-capture attribution; calls reaching the live value through this `Deref` leave through
// the unwrapped value and bypass it, like a settled owned transaction's buffer.
impl<M, W, E> Deref for Slot<M, W, E> {
    type Target = W;

    fn deref(&self) -> &W {
        self.wired.inner()
    }
}

/// The publish capability of a wired slot: what a body's mandatory bound names to start typed
/// (or raw) publishes through the slot.
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

impl<M: OutSlot, W: Publisher, E: Codec + Send + Sync> Publish for Slot<M, W, E> {
    type Leaf = SlotPublisher<W, M>;
    type EncodeCodec = E;

    fn leaf(&self) -> &SlotPublisher<W, M> {
        &self.wired
    }

    fn encode_codec(&self) -> &E {
        &self.codec
    }
}

impl<M: OutSlot, W, E> Slot<M, W, E> {
    /// Starts a typed publish through the slot, encoded with the include site's codec. The
    /// message type has to be in the marker's `#[publishes(..)]` dictionary (see
    /// [`PublishedThrough`](crate::runtime::PublishedThrough)).
    // The builder is spelled through the `Publish` projections (not `W` and `E` directly) so a
    // body generic over the entry needs only its declared `Slot<..>: Publish` bound to publish.
    pub fn message<'a, T>(
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
        T: OutgoingDestination + PublishedThrough<M>,
    {
        message_of(self.leaf(), value, self.encode_codec())
    }

    /// Starts a byte publish through the slot: the payload travels as it is, to the destination
    /// named with `to(..)`. The dictionary does not restrict this path - bytes carry no message
    /// type.
    pub fn raw<'a, B>(
        &'a self,
        payload: &'a B,
    ) -> PublishBuilder<&'a <Self as Publish>::Leaf, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        Self: Publish,
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(self.leaf(), payload)
    }
}

// The core capability vocabulary is also delegated on the entry itself (not only through
// Deref), so an entry passes into generic positions demanding the capability and a direct
// `publish` / `request` keeps the slot's test-capture attribution.
impl<M: OutSlot, W: Publisher, E: Send + Sync> Publisher for Slot<M, W, E> {
    type Error = W::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.wired.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.wired.base_headers()
    }
}

impl<M: OutSlot, W: TransactionalPublisher, E: Send + Sync> TransactionalPublisher
    for Slot<M, W, E>
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

impl<M: OutSlot, W: OwnedTransactions, E: Send + Sync> OwnedTransactions for Slot<M, W, E> {
    type Transaction = W::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.wired.transaction().await
    }
}

impl<M: OutSlot, W: RequestReply, E: Send + Sync> RequestReply for Slot<M, W, E> {
    type Reply = W::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        self.wired.request(msg, timeout).await
    }
}

/// The injected slot: pairs the marker's attached policy against the connected broker and
/// stores the live value under the include site's encode codec. A failing pair surfaces at
/// startup with the slot's name; an unbound slot never gets this far (the include site does not
/// compile).
impl<B, Sub, Policy, E, M> FromStartup<B, Sub, (Policy, E)>
    for Slot<M, <Policy as PublishPolicy<Connected<B>>>::Live, E>
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
        impl<M, W, E $(, $before)* $(, $after)*> SelectSlot<M, OutPos<$pos>>
            for ($($before,)* Slot<M, W, E>, $($after,)*)
        {
            type Picked = Slot<M, W, E>;

            fn pick(&self) -> &Slot<M, W, E> {
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
    ($(($($m:ident: $w:ident / $e:ident),+))+) => {$(
        impl<$($m: OutSlot, $w, $e),+> EntryMarkers for ($(Slot<$m, $w, $e>,)+) {
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
    (M0: W0 / E0)
    (M0: W0 / E0, M1: W1 / E1)
    (M0: W0 / E0, M1: W1 / E1, M2: W2 / E2)
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
    ($(($($m:ident / $p:ident: $e:ident),+))+) => {$(
        impl<Conn, A, C, H, Doc, $($m, $p, $e),+> BindSlots<Conn, ($(($p, $e),)+)>
            for Sealed<
                HandleValue<
                    A,
                    (),
                    Outs<($(Slot<$m, <$p as PublishPolicy<Conn>>::Live, $e>,)+)>,
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
    (M0 / P0: E0)
    (M0 / P0: E0, M1 / P1: E1)
    (M0 / P0: E0, M1 / P1: E1, M2 / P2: E2)
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

impl<C, S, H, Doc, E> InjectCall<S> for Sealed<HandleValue<SoloBytes, (), Outs<E>, C, H, Doc>>
where
    Self: InjectDef<Input = crate::runtime::RawBytes, Context = C, Injections = Outs<E>>,
    C: Send + Sync,
    S: Send + Sync,
    H: for<'p> Handle<Payload<'p>, (), Outs<E>, C, S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        input: &[u8],
        injections: &Outs<E>,
        ctx: &mut Context<'_, C, S>,
    ) -> HandlerOutcome {
        let payload = Payload::new(input);
        settle_solo(self.0.body.handle(&payload, injections, ctx).await)
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

impl<A, H, Doc, E> BatchInjectDef for Sealed<HandleValue<A, (), Outs<E>, (), H, Doc>>
where
    A: PagedAxis,
    H: Send + Sync,
    Doc: AxisDocs<A> + Send + Sync,
    E: EntryMarkers + Send + Sync,
{
    type Input = A::Kind;
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

impl<T, S, H, Doc, E> BatchInjectCall<S> for Sealed<HandleValue<Page<T>, (), Outs<E>, (), H, Doc>>
where
    Self: BatchInjectDef<Input = <Page<T> as Axis>::Kind, Injections = Outs<E>>,
    [T]: Input<Axis = Page<T>>,
    T: Send + Sync + 'static,
    S: Send + Sync,
    H: Handle<[T], (), Outs<E>, (), S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[T],
        injections: &Outs<E>,
        ctx: &mut Context<'_, (), S>,
    ) -> BatchResult {
        let verdict = self.0.body.handle(batch, injections, ctx).await;
        settle_page(verdict, batch.len(), ctx.name())
    }
}

impl<Hd, P, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<PagePair<Hd, P>, (), Outs<E>, (), H, Doc>>
where
    Self: BatchInjectDef<Input = <PagePair<Hd, P> as Axis>::Kind, Injections = Outs<E>>,
    [Message<Hd, P>]: Input<Axis = PagePair<Hd, P>>,
    Hd: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: Send + Sync,
    H: Handle<[Message<Hd, P>], (), Outs<E>, (), S>,
    E: Send + Sync,
{
    async fn call(
        &self,
        batch: &[Message<Hd, P>],
        injections: &Outs<E>,
        ctx: &mut Context<'_, (), S>,
    ) -> BatchResult {
        let verdict = self.0.body.handle(batch, injections, ctx).await;
        settle_page(verdict, batch.len(), ctx.name())
    }
}
