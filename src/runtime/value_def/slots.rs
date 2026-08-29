//! The slot-carrying value definition: the caller writes the markers and the body; the binding
//! machinery (`HasSlots`, `BindSlots`, the publisher-applied shell) is one generic impl here.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::{ConnectedBroker, PublishPolicy};
use crate::{Name, Unnamed};

use crate::runtime::context::Context;
use crate::runtime::handler::Settle;
use crate::runtime::inject::{InjectCall, InjectDef, Out};
use crate::runtime::input::Decoded;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot, SlotPublisher};

use super::IntoSource;
use super::subscribing::Docs;

/// A handler body over startup-injected publisher slots: the value-path counterpart of a
/// `#[subscriber]` body with `Out(..)` parameters.
///
/// `Slots` is the tuple of [`Out`] parameters in marker order; the include site binds each
/// marker to a policy (`.out(marker, policy)`, or `.publisher(policy)` for a single
/// [`DefaultSlot`](crate::runtime::DefaultSlot)). The publisher types are resolved from those
/// policies, so an implementation is generic over them - it states the capability it needs
/// (`P: Publisher`, ...) and mounts on a production broker and its test transport unchanged.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not handle `{T}` with the slot tuple `{Slots}`",
    note = "implement `SlotsHandler` generically over each slot's publisher and codec \
            (`impl<P, E, S> SlotsHandler<{T}, (Out<P, Marker, (), E>,), (), S> for ..`); the \
            tuple must list the markers in the order `with_slots` names them"
)]
pub trait SlotsHandler<T, Slots, C = (), S = ()>: Send + Sync {
    /// Handles one decoded input with the live slots.
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
/// wrapped in the settings builder. The include site's slot bindings instantiate it into
/// [`BoundSlotsValue`].
pub struct SlotsValue<T, H, Markers> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(T, Markers)>,
}

impl<T, H, Markers> fmt::Debug for SlotsValue<T, H, Markers> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotsValue").finish_non_exhaustive()
    }
}

impl<T, H, Markers> IncludeDef for SlotsValue<T, H, Markers> {
    type Form = forms::Out;
}

impl<T, H, Markers> HasSlots for SlotsValue<T, H, Markers> {
    type Markers = Markers;
}

/// The publisher-applied form of a [`SlotsValue`]: what its [`BindSlots`] impl instantiates
/// once the include site bound every marker. You never name this type.
pub struct BoundSlotsValue<T, H, Slots, Markers> {
    pub(crate) handler: H,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(T, Slots, Markers)>,
}

impl<T, H, Slots, Markers> fmt::Debug for BoundSlotsValue<T, H, Slots, Markers> {
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

        impl<Conn, T, H, $($marker, $policy, $codec),+> BindSlots<Conn, ($(($policy, $codec),)+)>
            for SlotsValue<T, H, ($($marker,)+)>
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

impl<T, H, Slots, Markers> InjectDef for BoundSlotsValue<T, H, Slots, Markers>
where
    T: Send + Sync + 'static,
    H: Send + Sync,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
{
    type Input = Decoded<T>;
    type Context = ();
    // The stored value never builds a source: the settings builder wrapping it carries the real
    // one (see `SubscriberValue::Source`).
    type Source = Unnamed<Name>;
    type Injections = Slots;

    fn source(&self) -> Self::Source {
        Unnamed::new()
    }

    fn description(&self) -> Option<&str> {
        self.docs.description()
    }

    fn input_schema(&self) -> Option<String> {
        self.docs.schema()
    }

    fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
        Markers::outgoing()
    }
}

impl<T, H, Slots, Markers, S> InjectCall<S> for BoundSlotsValue<T, H, Slots, Markers>
where
    T: Send + Sync + 'static,
    H: SlotsHandler<T, Slots, (), S>,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
    S: Send + Sync,
{
    fn call(
        &self,
        input: &T,
        injections: &Slots,
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Settle> + Send {
        self.handler.handle(input, injections, ctx)
    }
}

impl<T, H, Markers, Src, State> SubscriberBuilder<SlotsValue<T, H, Markers>, Src, State> {
    /// Sets the handler's human description for the generated `AsyncAPI` document, the
    /// value-path counterpart of the attribute reading the handler's doc comment.
    #[must_use]
    pub fn describe(self, text: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        self.map_def(|mut def| {
            def.docs.description = Some(text.into());
            def
        })
    }

    /// Reports the input type's JSON Schema in the generated `AsyncAPI` document. See
    /// [`documented`](SubscriberBuilder::documented) on the plain form.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn documented(self) -> Self
    where
        T: schemars::JsonSchema,
    {
        self.map_def(|mut def| {
            def.docs.schema = Some(super::schema_json_of::<T>);
            def
        })
    }
}

/// Binds a slot-carrying `handler` to the subscription `source`: the value-path counterpart of
/// a `#[subscriber]` handler with `Out(..)` parameters.
///
/// Mount it with `include`, binding each marker with `.out(marker, policy)` (or
/// `.publisher(policy)` for a single [`DefaultSlot`](crate::runtime::DefaultSlot)).
///
/// The message and marker types are named explicitly (`with_slots::<Event, (Primary, Shadow)>`):
/// the body is generic over the publishers the bindings resolve, so nothing else pins them.
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
) -> SubscriberBuilder<SlotsValue<T, H, Markers>, Src::Source, AllOpen>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
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
