//! The injections arena: the second body argument, built statically by the include site's
//! `.out(marker, policy)` chain.
//!
//! A body that publishes declares the arena in its `O` position - one [`Slot`] entry per
//! marker, generic over the wired publisher, with the capability it needs as a mandatory bound:
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
//! # struct Primary;
//! # impl OutSlot for Primary { const NAME: &'static str = "Primary"; }
//! # impl PublishedThrough<Primary> for Event {}
//!
//! struct Mirror;
//!
//! impl<PA> Handle<Order, (), Outs<(Slot<Primary, PA>,)>> for Mirror
//! where
//!     PA: Publish,
//! {
//!     async fn handle(
//!         &self,
//!         order: &Order,
//!         outs: &Outs<(Slot<Primary, PA>,)>,
//!         _ctx: &mut Context<'_>,
//!     ) -> Result<(), HandlerResult> {
//!         if outs
//!             .get(Primary)
//!             .message(&Event { id: order.id })
//!             .to("mirror")
//!             .publish()
//!             .await
//!             .is_err()
//!         {
//!             return Err(HandlerResult::retry());
//!         }
//!         Ok(())
//!     }
//! }
//! # }
//! ```
//!
//! The include site binds each marker with `.out(marker, policy)` in any order and seals with
//! `.build()`; a missing, duplicate or extra binding, or a policy whose live form lacks the
//! body's declared capability, fails to compile naming the marker. Everything is monomorphized:
//! the arena is built once at startup, and a delivery only ever passes a reference to it.

use std::fmt;
use std::marker::PhantomData;
use std::time::Duration;

use crate::codec::Codec;
use crate::runtime::batch::BatchResult;
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::context::Context;
use crate::runtime::handler::{HandlerResult, Settle};
use crate::runtime::inject::FromStartup;
use crate::runtime::inject::{InjectCall, InjectDef};
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publish::{
    HeadersUnset, MessageBody, PublishBuilder, RawBody, message_of, raw_of,
};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot, PublishedThrough, SlotPublisher};
use crate::{
    CallerName, Connected, ConnectedBroker, HeaderMap, Name, OutgoingDestination, OutgoingMessage,
    OwnedTransactions, PairError, PublishPolicy, Publisher, RequestReply, TransactionalPublisher,
    Unnamed,
};

use super::axis::{
    Axis, AxisDocs, Input, Message, Page, PagePair, PagedAxis, Payload, Solo, SoloAxis, SoloBytes,
    SoloPair,
};
use super::value::{HandleValue, Sealed};
use super::{Handle, IntoVerdict};

// ------------------------------------------------------------------------------ wired stacks

/// A slot's wired publish stack: the paired live publisher under the include site's encode
/// codec. You never name this type; a body sees it through the [`Publish`] capability bound on
/// its [`Slot`] parameter.
pub struct OutStack<P, E> {
    publisher: P,
    codec: E,
}

impl<P, E> fmt::Debug for OutStack<P, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutStack").finish_non_exhaustive()
    }
}

/// The publish capability of a wired slot: what a body's mandatory bound names to start typed
/// (or raw) publishes through the slot.
///
/// The other capabilities keep their own vocabulary: a slot whose body begins transactions
/// bounds [`TransactionalPublisher`](crate::TransactionalPublisher) (or
/// [`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply))
/// next to - or instead of - this one, and the include site's policy must pair a live form
/// carrying it.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a wired publish stack",
    note = "a slot parameter's capability bound is stated on the `Slot`'s second parameter: \
            `impl<P> Handle<T, (), Outs<(Slot<Marker, P>,)>> for Body where P: Publish`"
)]
pub trait Publish: Send + Sync {
    /// The live publisher under the stack.
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

impl<P: Publisher, E: Codec + Send + Sync> Publish for OutStack<P, E> {
    type Leaf = P;
    type EncodeCodec = E;

    fn leaf(&self) -> &P {
        &self.publisher
    }

    fn encode_codec(&self) -> &E {
        &self.codec
    }
}

impl<P: Publisher, E: Send + Sync> Publisher for OutStack<P, E> {
    type Error = P::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.publisher.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.publisher.base_headers()
    }
}

impl<P: TransactionalPublisher, E: Send + Sync> TransactionalPublisher for OutStack<P, E> {
    async fn begin_transaction(&self) -> Result<(), Self::Error> {
        self.publisher.begin_transaction().await
    }

    async fn commit(&self) -> Result<(), Self::Error> {
        self.publisher.commit().await
    }

    async fn abort(&self) -> Result<(), Self::Error> {
        self.publisher.abort().await
    }
}

impl<P: OwnedTransactions, E: Send + Sync> OwnedTransactions for OutStack<P, E> {
    type Transaction = P::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.publisher.transaction().await
    }
}

impl<P: RequestReply, E: Send + Sync> RequestReply for OutStack<P, E> {
    type Reply = P::Reply;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        self.publisher.request(msg, timeout).await
    }
}

// ------------------------------------------------------------------------------------- slots

/// One arena entry: the wired publish stack of the marker `M`.
///
/// The body publishes through it with [`message`](Self::message) (typed, admitted by the
/// marker's `#[publishes(..)]` dictionary) or [`raw`](Self::raw) (bytes as they are); the
/// broker capability vocabulary ([`TransactionalPublisher`](crate::TransactionalPublisher),
/// [`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply)) is
/// delegated, so a capability bound on the entry's `W` parameter is reachable directly on the
/// entry.
pub struct Slot<M, W> {
    wired: W,
    _marker: PhantomData<fn() -> M>,
}

impl<M, W> fmt::Debug for Slot<M, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

impl<M: OutSlot, W: Publish> Slot<M, W> {
    /// Starts a typed publish through the slot, encoded with the include site's codec. The
    /// message type has to be in the marker's `#[publishes(..)]` dictionary (see
    /// [`PublishedThrough`](crate::runtime::PublishedThrough)).
    pub fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a W::Leaf, MessageBody<'a, T>, &'a W::EncodeCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination + PublishedThrough<M>,
    {
        message_of(self.wired.leaf(), value, self.wired.encode_codec())
    }

    /// Starts a byte publish through the slot: the payload travels as it is, to the destination
    /// named with `to(..)`. The dictionary does not restrict this path - bytes carry no message
    /// type.
    pub fn raw<'a, B>(
        &'a self,
        payload: &'a B,
    ) -> PublishBuilder<&'a W::Leaf, RawBody<'a>, (), HeadersUnset, CallerName>
    where
        B: AsRef<[u8]> + ?Sized,
    {
        raw_of(self.wired.leaf(), payload)
    }
}

impl<M: OutSlot, W: Publisher> Publisher for Slot<M, W> {
    type Error = W::Error;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.wired.publish(msg).await
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.wired.base_headers()
    }
}

impl<M: OutSlot, W: TransactionalPublisher> TransactionalPublisher for Slot<M, W> {
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

impl<M: OutSlot, W: OwnedTransactions> OwnedTransactions for Slot<M, W> {
    type Transaction = W::Transaction;

    async fn transaction(&self) -> Result<Self::Transaction, Self::Error> {
        self.wired.transaction().await
    }
}

impl<M: OutSlot, W: RequestReply> RequestReply for Slot<M, W> {
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
/// wires it under the include site's encode codec. A failing pair surfaces at startup with the
/// slot's name; an unbound slot never gets this far (the include site does not compile).
impl<B, Sub, Policy, E, M> FromStartup<B, Sub, (Policy, E)>
    for Slot<M, OutStack<SlotPublisher<Policy::Live, M>, E>>
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
            wired: OutStack {
                publisher: SlotPublisher::new(live),
                codec,
            },
            _marker: PhantomData,
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
    /// Picks the entry of `marker`. The position is inferred, so the call reads the same
    /// wherever the marker sits in the declaration.
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
        impl<M, W $(, $before)* $(, $after)*> SelectSlot<M, OutPos<$pos>>
            for ($($before,)* Slot<M, W>, $($after,)*)
        {
            type Picked = Slot<M, W>;

            fn pick(&self) -> &Slot<M, W> {
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
    ($(($($m:ident: $w:ident),+))+) => {$(
        impl<$($m: OutSlot, $w),+> EntryMarkers for ($(Slot<$m, $w>,)+) {
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
    (M0: W0)
    (M0: W0, M1: W1)
    (M0: W0, M1: W1, M2: W2)
}

// -------------------------------------------------------------------- the slot definitions

impl<A, E, C, S, H, Doc> IncludeDef for Sealed<HandleValue<A, (), Outs<E>, C, S, H, Doc>>
where
    A: Axis,
{
    type Form = A::SlotForm;
}

impl<A, E, C, S, H, Doc> HasSlots for Sealed<HandleValue<A, (), Outs<E>, C, S, H, Doc>>
where
    E: EntryMarkers,
{
    type Markers = E::Markers;
}

/// Ties the declared arena to the bound policies: the entry a body left generic unifies with
/// the wired stack of its marker's paired live publisher, so the definition is its own bound
/// form and the body's capability bounds are checked right here.
macro_rules! impl_bind_slots {
    ($(($($m:ident / $p:ident: $e:ident),+))+) => {$(
        impl<Conn, A, C, S, H, Doc, $($m, $p, $e),+> BindSlots<Conn, ($(($p, $e),)+)>
            for Sealed<
                HandleValue<
                    A,
                    (),
                    Outs<($(Slot<$m, OutStack<SlotPublisher<$p::Live, $m>, $e>>,)+)>,
                    C,
                    S,
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

impl<A, C, S, H, Doc, E> InjectDef for Sealed<HandleValue<A, (), Outs<E>, C, S, H, Doc>>
where
    A: SoloAxis,
    C: Send + Sync,
    S: Send + Sync,
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
        Doc::payload_schema()
    }

    fn headers_schema(&self) -> Option<String> {
        Doc::headers_schema()
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        E::outgoing()
    }
}

impl<T, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<Solo<T>, (), Outs<E>, C, S, H, Doc>>
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
    ) -> Settle {
        match self.0.body.handle(input, injections, ctx).await.into_verdict() {
            Ok(()) => HandlerResult::Ack.into(),
            Err(settle) => settle,
        }
    }
}

impl<C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloBytes, (), Outs<E>, C, S, H, Doc>>
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
    ) -> Settle {
        let payload = Payload::new(input);
        match self
            .0
            .body
            .handle(&payload, injections, ctx)
            .await
            .into_verdict()
        {
            Ok(()) => HandlerResult::Ack.into(),
            Err(settle) => settle,
        }
    }
}

impl<Hd, P, C, S, H, Doc, E> InjectCall<S>
    for Sealed<HandleValue<SoloPair<Hd, P>, (), Outs<E>, C, S, H, Doc>>
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
    ) -> Settle {
        match self.0.body.handle(input, injections, ctx).await.into_verdict() {
            Ok(()) => HandlerResult::Ack.into(),
            Err(settle) => settle,
        }
    }
}

impl<A, S, H, Doc, E> BatchInjectDef for Sealed<HandleValue<A, (), Outs<E>, (), S, H, Doc>>
where
    A: PagedAxis,
    S: Send + Sync,
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
        Doc::payload_schema()
    }

    fn headers_schema(&self) -> Option<String> {
        Doc::headers_schema()
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        E::outgoing()
    }
}

impl<T, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<Page<T>, (), Outs<E>, (), S, H, Doc>>
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
        let verdict = self
            .0
            .body
            .handle(batch, injections, ctx)
            .await
            .into_verdict();
        super::eager::settle_page(verdict, batch.len(), ctx.name())
    }
}

impl<Hd, P, S, H, Doc, E> BatchInjectCall<S>
    for Sealed<HandleValue<PagePair<Hd, P>, (), Outs<E>, (), S, H, Doc>>
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
        let verdict = self
            .0
            .body
            .handle(batch, injections, ctx)
            .await
            .into_verdict();
        super::eager::settle_page(verdict, batch.len(), ctx.name())
    }
}
