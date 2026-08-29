//! The combined form: a reply-producing body that also publishes through `Out` slots.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::schema::{FixedName, OutgoingDestination};
use crate::{ConnectedBroker, Name, PublishPolicy, Unnamed};

use crate::runtime::context::Context;
use crate::runtime::handler::HandlerResult;
use crate::runtime::inject::Out;
use crate::runtime::input::Decoded;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::SubscriberBuilder;
use crate::runtime::slot::{BindSlots, HasSlots, OutSlot, SlotPublisher};

use super::IntoSource;
use super::replying::{DeclaredName, To};
use super::slots::SlotSetOutgoing;
use super::subscribing::{Docs, DocumentedValue, docs_metadata};

/// A reply-producing handler body over startup-injected publisher slots: the value-path
/// counterpart of a `#[subscriber(.., publish(..))]` body with `Out(..)` parameters.
///
/// The combination of [`Reply`](super::Reply) and [`SlotsHandler`](super::SlotsHandler):
/// `Ok(reply)` is published to the definition's destination, the slots publish whatever the
/// body sends on the side, and `Err(result)` skips the reply.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not produce a reply for `{T}` with the injection tuple `{Slots}`",
    note = "implement `SlotsReply` generically over each slot's publisher and codec \
            (`impl<P, E, S> SlotsReply<{T}, (Out<P, Marker, (), E>,), (), S> for ..`), with \
            `type Out` naming the reply"
)]
pub trait SlotsReply<T, Slots, C = (), S = ()>: Send + Sync {
    /// The reply type, published to the destination.
    type Out;

    /// Produces the reply for one decoded input, with the live slots.
    fn reply(
        &self,
        msg: &T,
        slots: &Slots,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self::Out, HandlerResult>> + Send;
}

/// The variance-neutral marker of the definition's carried type parameters.
type Carried<T> = PhantomData<fn() -> T>;

/// A slot-carrying reply definition built from a value: what
/// `replying_with_slots(source, handler)` (or `raw_replying_with_slots`) returns, wrapped in
/// the settings builder.
pub struct ReplyingSlotsValue<T, H, Markers, Dest, C = (), F = forms::PublishingOut> {
    pub(crate) handler: H,
    pub(crate) dest: Dest,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(T, Markers, C, F)>,
}

impl<T, H, Markers, Dest, C, F> fmt::Debug for ReplyingSlotsValue<T, H, Markers, Dest, C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyingSlotsValue").finish_non_exhaustive()
    }
}

impl<T, H, Markers, Dest, C, F> IncludeDef for ReplyingSlotsValue<T, H, Markers, Dest, C, F> {
    type Form = F;
}

impl<T, H, Markers, Dest, C, F> HasSlots for ReplyingSlotsValue<T, H, Markers, Dest, C, F> {
    type Markers = Markers;
}

impl<T, H, Markers, Dest, C, F> DocumentedValue for ReplyingSlotsValue<T, H, Markers, Dest, C, F> {
    type Payload = T;
    type Reply = ();

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

/// The publisher-applied form of a [`ReplyingSlotsValue`]. You never name this type.
pub struct BoundReplyingSlots<T, H, Slots, Markers, Dest, C = ()> {
    handler: H,
    dest: Dest,
    docs: Docs,
    _types: Carried<(T, Slots, Markers, C)>,
}

impl<T, H, Slots, Markers, Dest, C> fmt::Debug
    for BoundReplyingSlots<T, H, Slots, Markers, Dest, C>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundReplyingSlots").finish_non_exhaustive()
    }
}

/// Implements [`BindSlots`] for each marker-tuple arity, mirroring `SlotsValue`.
macro_rules! impl_replying_slots_bind {
    ($(($(($marker:ident, $policy:ident, $codec:ident)),+))+) => {$(
        impl<Conn, T, H, Dest, C, F, $($marker, $policy, $codec),+>
            BindSlots<Conn, ($(($policy, $codec),)+)>
            for ReplyingSlotsValue<T, H, ($($marker,)+), Dest, C, F>
        where
            Conn: ConnectedBroker,
            $(
                $marker: OutSlot,
                $policy: PublishPolicy<Conn>,
            )+
        {
            type Bound = BoundReplyingSlots<
                T,
                H,
                ($(Out<SlotPublisher<<$policy as PublishPolicy<Conn>>::Live, $marker>, $marker, (), $codec>,)+),
                ($($marker,)+),
                Dest,
                C,
            >;
            type Extra = ($(($policy, $codec),)+);

            fn bind(self, sources: Self::Extra) -> (Self::Bound, Self::Extra) {
                (
                    BoundReplyingSlots {
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

impl_replying_slots_bind! {
    ((M0, P0, E0))
    ((M0, P0, E0), (M1, P1, E1))
    ((M0, P0, E0), (M1, P1, E1), (M2, P2, E2))
}

/// The reply type the body produces over the unit state, which every state-generic body pins for
/// all states alike.
type ReplyOf<H, T, Slots, C> = <H as SlotsReply<T, Slots, C>>::Out;

/// The metadata methods shared by the two destination states.
macro_rules! replying_slots_def_common {
    () => {
        type Input = Decoded<T>;
        type Injections = Slots;
        type Reply = ReplyOf<H, T, Slots, C>;
        type Context = C;
        // The stored value never builds a source: the settings builder wrapping it carries the
        // real one (see `SubscriberValue::Source`).
        type Source = Unnamed<Name>;

        fn source(&self) -> Self::Source {
            Unnamed::new()
        }

        docs_metadata!();

        fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
            let mut declared = vec![self.docs.reply_outgoing(
                self.reply_name().to_owned(),
                type_name::<ReplyOf<H, T, Slots, C>>(),
            )];
            declared.extend(Markers::outgoing());
            declared
        }
    };
}

impl<T, H, Slots, Markers, C> PublishingDef for BoundReplyingSlots<T, H, Slots, Markers, To, C>
where
    T: Send + Sync + 'static,
    H: SlotsReply<T, Slots, C>,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
    C: Send + Sync,
{
    replying_slots_def_common!();

    fn reply_name(&self) -> &str {
        &self.dest.0
    }
}

impl<T, H, Slots, Markers, C> PublishingDef
    for BoundReplyingSlots<T, H, Slots, Markers, DeclaredName, C>
where
    T: Send + Sync + 'static,
    H: SlotsReply<T, Slots, C>,
    ReplyOf<H, T, Slots, C>: OutgoingDestination<Form = FixedName>,
    Slots: Send + Sync,
    Markers: SlotSetOutgoing,
    C: Send + Sync,
{
    replying_slots_def_common!();

    fn reply_name(&self) -> &str {
        ReplyOf::<H, T, Slots, C>::ADDRESS
    }
}

impl<T, H, Slots, Markers, Dest, C, S> PublishingCall<S>
    for BoundReplyingSlots<T, H, Slots, Markers, Dest, C>
where
    Self: PublishingDef<
            Input = Decoded<T>,
            Injections = Slots,
            Reply = ReplyOf<H, T, Slots, C>,
            Context = C,
        >,
    T: Send + Sync + 'static,
    H: SlotsReply<T, Slots, C> + SlotsReply<T, Slots, C, S, Out = ReplyOf<H, T, Slots, C>>,
    C: Send + Sync,
    S: Send + Sync,
{
    fn call(
        &self,
        input: &T,
        injections: &Slots,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<ReplyOf<H, T, Slots, C>, HandlerResult>> + Send {
        self.handler.reply(input, injections, ctx)
    }
}

/// The chain over a caller-named destination: what [`to`](SubscriberBuilder::to) hands back.
type Renamed<T, H, Markers, C, F, Src, State, DC> =
    SubscriberBuilder<ReplyingSlotsValue<T, H, Markers, To, C, F>, Src, State, DC>;

/// The chain over a renamed broker context: what
/// [`context`](SubscriberBuilder::context) hands back.
type Recontexted<T, H, Markers, Dest, C2, F, Src, State, DC> =
    SubscriberBuilder<ReplyingSlotsValue<T, H, Markers, Dest, C2, F>, Src, State, DC>;

impl<T, H, Markers, C, F, Src, State, DC>
    SubscriberBuilder<ReplyingSlotsValue<T, H, Markers, DeclaredName, C, F>, Src, State, DC>
{
    /// Names the subject the reply is published to. See
    /// [`to`](SubscriberBuilder::to) on the plain reply form.
    #[must_use]
    pub fn to(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> Renamed<T, H, Markers, C, F, Src, State, DC> {
        self.map_def(|def| ReplyingSlotsValue {
            handler: def.handler,
            dest: To(name.into()),
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

impl<T, H, Markers, Dest, C, F, Src, State, DC>
    SubscriberBuilder<ReplyingSlotsValue<T, H, Markers, Dest, C, F>, Src, State, DC>
{
    /// Names the broker's typed per-delivery context the body reads, replacing the unit
    /// default. The body's bound is checked at the mount.
    #[must_use]
    pub fn context<C2>(self) -> Recontexted<T, H, Markers, Dest, C2, F, Src, State, DC> {
        self.map_def(|def| ReplyingSlotsValue {
            handler: def.handler,
            dest: def.dest,
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

fn build_replying_slots<T, Markers, Src, H, F>(
    source: Src,
    handler: H,
) -> super::ValueBuilder<ReplyingSlotsValue<T, H, Markers, DeclaredName, (), F>, Src>
where
    Src: IntoSource,
{
    SubscriberBuilder::new(
        ReplyingSlotsValue {
            handler,
            dest: DeclaredName,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// Binds a slot-carrying, reply-producing `handler` to the subscription `source`: the
/// value-path counterpart of `#[subscriber("in", publish("out"))]` with `Out(..)` parameters.
///
/// Mount it with `include`: the reply policy chains with `.publisher(..)` (or stays on the
/// broker's default), each slot marker binds with `.out(marker, policy)`, and the statement
/// commits with `.mount()`. The destination follows the reply type's declaration, with a
/// mandatory [`to`](SubscriberBuilder::to) when it names none - exactly as for
/// [`replying`](super::replying). The message and marker types are named explicitly, as for
/// [`with_slots`](super::with_slots).
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
/// #[derive(Deserialize)]
/// struct Request {
///     id: u64,
/// }
///
/// #[derive(Serialize)]
/// struct Response {
///     ok: bool,
/// }
///
/// struct Gateway;
///
/// impl<P, E, S> SlotsReply<Request, (Out<P, DefaultSlot, (), E>,), (), S> for Gateway
/// where
///     P: Publisher,
///     E: Send + Sync,
///     S: Send + Sync,
/// {
///     type Out = Response;
///
///     async fn reply(
///         &self,
///         req: &Request,
///         slots: &(Out<P, DefaultSlot, (), E>,),
///         _ctx: &mut Context<'_, (), S>,
///     ) -> Result<Response, HandlerResult> {
///         let Out(audit) = &slots.0;
///         let payload = req.id.to_be_bytes();
///         if audit.raw(&payload).to("gateway-audit").publish().await.is_err() {
///             return Err(HandlerResult::retry());
///         }
///         Ok(Response { ok: true })
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("gateway", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(
///             replying_with_slots::<Request, (DefaultSlot,), _, _>("gateway-requests", Gateway)
///                 .to("gateway-responses"),
///         )
///         .out(DefaultSlot, MemoryPublish)
///         .mount();
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn replying_with_slots<T, Markers, Src, H>(
    source: Src,
    handler: H,
) -> super::ValueBuilder<ReplyingSlotsValue<T, H, Markers, DeclaredName>, Src>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
{
    build_replying_slots(source, handler)
}

/// [`replying_with_slots`] whose reply travels a bare publisher, byte for byte: the value-path
/// counterpart of `publish_raw(..)` next to `Out(..)` parameters.
///
/// The body's `Out` must be byte-shaped (`AsRef<[u8]>`), which the mount checks.
#[must_use]
pub fn raw_replying_with_slots<T, Markers, Src, H>(
    source: Src,
    handler: H,
) -> super::ValueBuilder<ReplyingSlotsValue<T, H, Markers, DeclaredName, (), forms::RawReplyOut>, Src>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
{
    build_replying_slots(source, handler)
}
