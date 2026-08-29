//! The batch slot-carrying value definitions: a page-settling body over injected publishers,
//! with or without a reply per element.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::{ConnectedBroker, PublishPolicy};
use crate::{Name, Unnamed};

use crate::runtime::batch::BatchResult;
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingDef};
use crate::runtime::context::Context;
use crate::runtime::handler::HandlerResult;
use crate::runtime::inject::Out;
use crate::runtime::input::Decoded;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot, SlotPublisher};

use super::IntoSource;
use super::replying::{DeclaredName, To};
use super::slots::SlotSetOutgoing;
use super::subscribing::{Docs, DocumentedValue, docs_metadata};

/// The variance-neutral marker of a definition's carried type parameters.
type Carried<T> = PhantomData<fn() -> T>;

/// A page-settling handler body over startup-injected parameters: the batch counterpart of
/// [`SlotsHandler`](super::SlotsHandler).
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle batches of `{T}` with the injection tuple `{Slots}`",
    note = "implement `SlotsSliceHandler` generically over each slot's publisher and codec, with \
            the tuple listing the markers in the order the constructor names them"
)]
pub trait SlotsSliceHandler<T, Slots, S = ()>: Send + Sync {
    /// Handles one decoded batch with the live injections.
    fn handle_slice(
        &self,
        batch: &[T],
        slots: &Slots,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = BatchResult> + Send;
}

/// A page-settling, reply-producing body over startup-injected parameters: the batch
/// counterpart of [`SlotsReply`](super::SlotsReply).
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not produce replies for batches of `{T}` with the injection tuple \
               `{Slots}`",
    note = "implement `SlotsBatchReply` generically over each slot's publisher and codec, with \
            `type Out` naming the reply element"
)]
pub trait SlotsBatchReply<T, Slots, S = ()>: Send + Sync {
    /// The reply element type; each entry of the returned `Vec` is published.
    type Out;

    /// Produces the page's replies with the live injections.
    fn reply(
        &self,
        batch: &[T],
        slots: &Slots,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<Self::Out>, HandlerResult>> + Send;
}

/// A batch slot-carrying definition built from a value: what
/// `batch_with_slots(source, handler)` returns. The include site's bindings instantiate it
/// into [`BoundBatchSlots`].
pub struct BatchSlotsValue<T, H, Markers, Dest = (), F = forms::BatchOut> {
    pub(crate) handler: H,
    pub(crate) dest: Dest,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(T, Markers, F)>,
}

impl<T, H, Markers, Dest, F> fmt::Debug for BatchSlotsValue<T, H, Markers, Dest, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BatchSlotsValue").finish_non_exhaustive()
    }
}

impl<T, H, Markers, Dest, F> IncludeDef for BatchSlotsValue<T, H, Markers, Dest, F> {
    type Form = F;
}

impl<T, H, Markers, Dest, F> HasSlots for BatchSlotsValue<T, H, Markers, Dest, F> {
    type Markers = Markers;
}

impl<T, H, Markers, Dest, F> DocumentedValue for BatchSlotsValue<T, H, Markers, Dest, F> {
    type Payload = T;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

/// The publisher-applied form of a [`BatchSlotsValue`]. You never name this type.
pub struct BoundBatchSlots<T, H, Slots, Markers, Dest = ()> {
    handler: H,
    dest: Dest,
    docs: Docs,
    _types: Carried<(T, Slots, Markers)>,
}

impl<T, H, Slots, Markers, Dest> fmt::Debug for BoundBatchSlots<T, H, Slots, Markers, Dest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundBatchSlots").finish_non_exhaustive()
    }
}

/// Implements [`BindSlots`] for each marker-tuple arity, mirroring `SlotsValue`.
macro_rules! impl_batch_value_slots {
    ($(($(($marker:ident, $policy:ident, $codec:ident)),+))+) => {$(
        impl<Conn, T, H, Dest, F, $($marker, $policy, $codec),+>
            BindSlots<Conn, ($(($policy, $codec),)+)>
            for BatchSlotsValue<T, H, ($($marker,)+), Dest, F>
        where
            Conn: ConnectedBroker,
            $(
                $marker: OutSlot,
                $policy: PublishPolicy<Conn>,
            )+
        {
            type Bound = BoundBatchSlots<
                T,
                H,
                ($(Out<SlotPublisher<<$policy as PublishPolicy<Conn>>::Live, $marker>, $marker, (), $codec>,)+),
                ($($marker,)+),
                Dest,
            >;
            type Extra = ($(($policy, $codec),)+);

            fn bind(self, sources: Self::Extra) -> (Self::Bound, Self::Extra) {
                (
                    BoundBatchSlots {
                        handler: self.handler,
                        dest: self.dest,
                        docs: self.docs,
                        _types: PhantomData,
                    },
                    sources,
                )
            }
        }
    )+};
}

impl_batch_value_slots! {
    ((M0, P0, E0))
    ((M0, P0, E0), (M1, P1, E1))
    ((M0, P0, E0), (M1, P1, E1), (M2, P2, E2))
}

impl<T, H, Slots, Markers> BatchInjectDef for BoundBatchSlots<T, H, Slots, Markers, ()>
where
    T: Send + Sync + 'static,
    H: Send + Sync,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
{
    type Input = Decoded<T>;
    // The stored value never builds a source: the settings builder wrapping it carries the real
    // one (see `SubscriberValue::Source`).
    type Source = Unnamed<Name>;
    type Injections = Slots;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    docs_metadata!();

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        Markers::outgoing()
    }
}

impl<T, H, Slots, Markers, S> BatchInjectCall<S> for BoundBatchSlots<T, H, Slots, Markers, ()>
where
    T: Send + Sync + 'static,
    H: SlotsSliceHandler<T, Slots, S>,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
    S: Send + Sync,
{
    fn call(
        &self,
        batch: &[T],
        injections: &Slots,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = BatchResult> + Send {
        self.handler.handle_slice(batch, injections, ctx)
    }
}

/// The reply element type the body produces over the unit state, which every state-generic body
/// pins for all states alike.
type BatchReplyOf<H, T, Slots> = <H as SlotsBatchReply<T, Slots>>::Out;

/// The metadata methods shared by the two destination states of the replying form.
macro_rules! batch_replying_slots_common {
    () => {
        type Input = Decoded<T>;
        type Injections = Slots;
        type Reply = BatchReplyOf<H, T, Slots>;
        type Source = Unnamed<Name>;

        fn source(&self) -> Self::Source {
            Unnamed::new()
        }

        docs_metadata!();

        fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
            let mut declared = vec![self.docs.reply_outgoing(
                self.reply_name().to_owned(),
                type_name::<BatchReplyOf<H, T, Slots>>(),
            )];
            declared.extend(Markers::outgoing());
            declared
        }
    };
}

impl<T, H, Slots, Markers> BatchPublishingDef for BoundBatchSlots<T, H, Slots, Markers, To>
where
    T: Send + Sync + 'static,
    H: SlotsBatchReply<T, Slots>,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
{
    batch_replying_slots_common!();

    fn reply_name(&self) -> &str {
        &self.dest.0
    }
}

impl<T, H, Slots, Markers, S> BatchPublishingCall<S> for BoundBatchSlots<T, H, Slots, Markers, To>
where
    Self: BatchPublishingDef<
            Input = Decoded<T>,
            Injections = Slots,
            Reply = BatchReplyOf<H, T, Slots>,
        >,
    T: Send + Sync + 'static,
    H: SlotsBatchReply<T, Slots> + SlotsBatchReply<T, Slots, S, Out = BatchReplyOf<H, T, Slots>>,
    Slots: Send + Sync,
    S: Send + Sync,
{
    fn call(
        &self,
        batch: &[T],
        injections: &Slots,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<Vec<BatchReplyOf<H, T, Slots>>, HandlerResult>> + Send {
        self.handler.reply(batch, injections, ctx)
    }
}

impl<T, H, Markers, F, Src, State, DC>
    SubscriberBuilder<BatchSlotsValue<T, H, Markers, DeclaredName, F>, Src, State, DC>
{
    /// Names the subject the page's replies are published to. Mandatory on this form: a batch
    /// reply has no per-element declared destination to fall back on.
    #[must_use]
    pub fn to(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<BatchSlotsValue<T, H, Markers, To, F>, Src, State, DC> {
        self.map_def(|def| BatchSlotsValue {
            handler: def.handler,
            dest: To(name.into()),
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

/// Binds a page-settling, slot-carrying `handler` to the batch subscription `source`: the
/// value-path counterpart of a `batch(..)` attribute body with `Out(..)` parameters.
///
/// Mount it with `include`, binding each marker with `.out(marker, policy)` (or
/// `.publisher(policy)` for a single slot) and committing with `.mount()`. The message and
/// marker types are named explicitly, as for [`with_slots`](super::with_slots).
#[must_use]
pub fn batch_with_slots<T, Markers, Src, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<BatchSlotsValue<T, H, Markers>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
{
    SubscriberBuilder::new(
        BatchSlotsValue {
            handler,
            dest: (),
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// Binds a reply-producing, slot-carrying page handler to the batch subscription `source`: the
/// value-path counterpart of `#[subscriber(batch(..), publish(..))]` with `Out(..)` parameters.
///
/// Mount it with `include`: the reply policy chains with `.publisher(..)` (or stays on the
/// broker's default), each slot binds with `.out(marker, policy)`, the statement commits with
/// `.mount()`, and [`to`](SubscriberBuilder::to) names the reply subject (mandatory: a batch
/// reply has no declared-destination fallback).
#[must_use]
pub fn batch_replying_with_slots<T, Markers, Src, H>(
    source: Src,
    handler: H,
) -> SubscriberBuilder<
    BatchSlotsValue<T, H, Markers, DeclaredName, forms::BatchPublishingOut>,
    Src::Source,
    AllOpen,
>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
{
    SubscriberBuilder::new(
        BatchSlotsValue {
            handler,
            dest: DeclaredName,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}
