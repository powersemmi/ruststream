//! The slot-carrying value definition: the caller writes the markers and the body; the binding
//! machinery (`HasSlots`, `BindSlots`, the publisher-applied shell) is one generic impl here.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use crate::{ConnectedBroker, PublishPolicy};
use crate::{Name, Unnamed};

use crate::runtime::context::Context;
use crate::runtime::handler::Settle;
use crate::runtime::inject::{InjectCall, InjectDef, Out};
use crate::runtime::input::InputKind;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::SubscriberBuilder;
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot, SlotPublisher};

use super::subscribing::{Docs, DocumentedValue, docs_metadata};
use super::{HandledInput, IntoSource};

/// A handler body over startup-injected parameters: the value-path counterpart of a
/// `#[subscriber]` body with `Out(..)` (or `Seek(..)`) parameters.
///
/// `Slots` is the tuple of injected parameters, in marker order for the `Out` forms; the
/// include site binds each marker to a policy (`.out(marker, policy)`, or `.publisher(policy)`
/// for a single [`DefaultSlot`](crate::runtime::DefaultSlot)), while a `Seek` injection
/// resolves off the subscription itself. The publisher types are resolved from those policies,
/// so an implementation is generic over them - it states the capability it needs
/// (`P: Publisher`, ...) and mounts on a production broker and its test transport unchanged.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle `{T}` with the injection tuple `{Slots}`",
    note = "implement `SlotsHandler` generically over each slot's publisher and codec \
            (`impl<P, E, S> SlotsHandler<{T}, (Out<P, Marker, (), E>,), (), S> for ..`); the \
            tuple must list the markers in the order the constructor names them"
)]
pub trait SlotsHandler<T: ?Sized, Slots, C = (), S = ()>: Send + Sync {
    /// Handles one input with the live injections.
    fn handle(
        &self,
        msg: &T,
        slots: &Slots,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Settle> + Send;
}

/// The variance-neutral marker of a definition's carried type parameters.
type Carried<T> = PhantomData<fn() -> T>;

/// A slot-carrying definition built from a value: what `with_slots(source, handler)` returns,
/// wrapped in the settings builder.
///
/// `In` is the input kind the constructor resolved off the body's parameter type. The include
/// site's slot bindings instantiate it into [`BoundSlotsValue`].
pub struct SlotsValue<In, H, Markers, C = ()> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(In, Markers, C)>,
}

impl<In, H, Markers, C> fmt::Debug for SlotsValue<In, H, Markers, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotsValue").finish_non_exhaustive()
    }
}

impl<In, H, Markers, C> IncludeDef for SlotsValue<In, H, Markers, C> {
    type Form = forms::Out;
}

impl<In, H, Markers, C> HasSlots for SlotsValue<In, H, Markers, C> {
    type Markers = Markers;
}

impl<In: InputKind, H, Markers, C> DocumentedValue for SlotsValue<In, H, Markers, C> {
    type Payload = In::Target;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

/// The publisher-applied form of a [`SlotsValue`]: what its [`BindSlots`] impl instantiates
/// once the include site bound every marker. You never name this type.
pub struct BoundSlotsValue<In, H, Slots, Markers, C = ()> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(In, Slots, Markers, C)>,
}

impl<In, H, Slots, Markers, C> fmt::Debug for BoundSlotsValue<In, H, Slots, Markers, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundSlotsValue").finish_non_exhaustive()
    }
}

/// The declared outgoing messages of a marker tuple, summed in marker order.
pub(super) trait SlotSetOutgoing {
    fn outgoing() -> Vec<OutgoingMessageMetadata>;
}

/// Implements [`BindSlots`] and [`SlotSetOutgoing`] for each marker-tuple arity, mirroring the
/// arities of the positional binding machinery in `slot.rs`.
macro_rules! impl_value_slots {
    ($(($(($marker:ident, $policy:ident, $codec:ident)),+))+) => {$(
        impl<$($marker: OutSlot),+> SlotSetOutgoing for ($($marker,)+) {
            fn outgoing() -> Vec<OutgoingMessageMetadata> {
                let mut declared = Vec::new();
                $(declared.extend(<$marker as OutSlot>::outgoing());)+
                declared
            }
        }

        impl<Conn, T, H, C, $($marker, $policy, $codec),+> BindSlots<Conn, ($(($policy, $codec),)+)>
            for SlotsValue<T, H, ($($marker,)+), C>
        where
            Conn: ConnectedBroker,
            $(
                $marker: OutSlot,
                $policy: PublishPolicy<Conn>,
            )+
        {
            type Bound = BoundSlotsValue<
                T,
                H,
                ($(Out<SlotPublisher<<$policy as PublishPolicy<Conn>>::Live, $marker>, $marker, (), $codec>,)+),
                ($($marker,)+),
                C,
            >;
            type Extra = ($(($policy, $codec),)+);

            fn bind(self, sources: Self::Extra) -> (Self::Bound, Self::Extra) {
                (
                    BoundSlotsValue {
                        handler: self.handler,
                        docs: self.docs,
                        _types: PhantomData,
                    },
                    sources,
                )
            }
        }
    )+};
}

impl_value_slots! {
    ((M0, P0, E0))
    ((M0, P0, E0), (M1, P1, E1))
    ((M0, P0, E0), (M1, P1, E1), (M2, P2, E2))
}

impl<In, H, Slots, Markers, C> InjectDef for BoundSlotsValue<In, H, Slots, Markers, C>
where
    In: InputKind,
    H: Send + Sync,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
    C: Send + Sync,
{
    type Input = In;
    type Context = C;
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

impl<In, H, Slots, Markers, C, S> InjectCall<S> for BoundSlotsValue<In, H, Slots, Markers, C>
where
    In: InputKind,
    H: SlotsHandler<In::Target, Slots, C, S>,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
    C: Send + Sync,
    S: Send + Sync,
{
    fn call(
        &self,
        input: &In::Target,
        injections: &Slots,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Settle> + Send {
        self.handler.handle(input, injections, ctx)
    }
}

impl<In, H, Markers, C, Src, State, DC>
    SubscriberBuilder<SlotsValue<In, H, Markers, C>, Src, State, DC>
{
    /// Names the broker's typed per-delivery context the body reads, replacing the unit
    /// default. The body's bound is checked at the mount.
    #[must_use]
    pub fn context<C2>(self) -> SubscriberBuilder<SlotsValue<In, H, Markers, C2>, Src, State, DC> {
        self.map_def(|def| SlotsValue {
            handler: def.handler,
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

/// Binds a slot-carrying `handler` to the subscription `source`: the value-path counterpart of
/// a `#[subscriber]` handler with `Out(..)` parameters.
///
/// Mount it with `include`, binding each marker with `.out(marker, policy)` (or
/// `.publisher(policy)` for a single [`DefaultSlot`](crate::runtime::DefaultSlot)) and
/// committing with `.mount()`.
///
/// The message and marker types are named explicitly (`with_slots::<Event, (Primary, Shadow)>`):
/// the body is generic over the publishers the bindings resolve, so nothing else pins them. The
/// state axis needs no `_in` variant here - the body's bound is checked at the mount, where the
/// app's state type is known.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct Event {
///     id: u64,
/// }
///
/// struct Primary;
///
/// impl OutSlot for Primary {
///     const NAME: &'static str = "Primary";
/// }
///
/// struct Mirror;
///
/// // Generic over the publisher the binding resolves and the scope codec the slot carries: the
/// // body states the capability it needs, nothing broker-specific appears.
/// impl<P, E, S> SlotsHandler<Event, (Out<P, Primary, (), E>,), (), S> for Mirror
/// where
///     P: Publisher,
///     E: Send + Sync,
///     S: Send + Sync,
/// {
///     async fn handle(
///         &self,
///         event: &Event,
///         slots: &(Out<P, Primary, (), E>,),
///         _ctx: &mut Context<'_, (), S>,
///     ) -> Settle {
///         let Out(primary) = &slots.0;
///         let payload = event.id.to_be_bytes();
///         if primary.raw(&payload).to("mirror-primary").publish().await.is_err() {
///             return HandlerResult::retry().into();
///         }
///         HandlerResult::ack().into()
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("mirror", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(with_slots::<Event, (Primary,), _, _>("mirror", Mirror))
///             .out(Primary, MemoryPublish);
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn with_slots<T, Markers, Src, H>(
    source: Src,
    handler: H,
) -> super::ValueBuilder<SlotsValue<T::Kind, H, Markers>, Src>
where
    Src: IntoSource,
    T: ?Sized + HandledInput,
{
    SubscriberBuilder::new(
        SlotsValue {
            handler,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}
