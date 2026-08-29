//! The reply-publishing value definitions: a body producing a reply, and where the reply goes.
//!
//! One generic definition serves the encoded and the byte-reply forms alike: the form token
//! parameter picks the wiring (`forms::Publishing` encodes the reply through the attached
//! `TypedPublisher` stack, `forms::RawReply` sends its bytes bare), and the input axis follows
//! the body - a [`Reply<T>`] body decodes, a [`Reply<[u8]>`](Reply) body takes the payload raw.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use crate::schema::{FixedName, OutgoingDestination};
use crate::{Name, Unnamed};

use crate::runtime::context::Context;
use crate::runtime::handler::HandlerResult;
use crate::runtime::input::InputKind;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};

use super::subscribing::{Docs, DocumentedValue, docs_metadata};
use super::{HandledInput, IntoSource};

/// A handler body producing a reply: the value-path counterpart of a
/// `#[subscriber(.., publish(..))]` body.
///
/// `Ok(reply)` is published to the definition's destination, then the incoming message is
/// acked; `Err(result)` skips publishing and the dispatcher acts on the returned
/// [`HandlerResult`]. Closures implement it, and a named type implements it generically over
/// the state parameter `S` (reading typed state through
/// [`Context::state`](crate::runtime::Context::state) is checked where the definition mounts).
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not produce a reply for `{T}`",
    note = "implement `Reply<{T}>` on the handler type - `type Out` names the reply, and `reply` \
            returns `Result<Self::Out, HandlerResult>` - or pass a closure with that return type"
)]
pub trait Reply<T: ?Sized, C = (), S = ()>: Send + Sync {
    /// The reply type, published to the destination.
    type Out;

    /// Produces the reply for one input.
    fn reply(
        &self,
        msg: &T,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self::Out, HandlerResult>> + Send;
}

impl<T: ?Sized, C, S, F, Fut, R> Reply<T, C, S> for F
where
    F: Fn(&T, &mut Context<'_, C, S>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<R, HandlerResult>> + Send,
{
    type Out = R;

    fn reply(
        &self,
        msg: &T,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<R, HandlerResult>> + Send {
        (self)(msg, ctx)
    }
}

/// The destination state of a fresh [`replying`] definition: the reply type's own declared
/// name.
///
/// Mountable only when the reply type fixes one (`#[outgoing(name = "...")]`); otherwise chain
/// [`to`](SubscriberBuilder::to), and until one of the two names the destination the definition
/// is no [`PublishingDef`] at all, so mounting it does not compile.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct DeclaredName;

/// The destination state after [`to`](SubscriberBuilder::to): a caller-named subject.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct To(pub(crate) Cow<'static, str>);

/// The variance-neutral marker of a definition's carried type parameters.
type Carried<T> = PhantomData<fn() -> T>;

/// A reply-publishing definition built from a value: what `replying(source, handler)` (and its
/// `raw_replying` / `_in` variants) returns, wrapped in the settings builder.
pub struct ReplyingValue<In, R, H, Dest, C = (), F = forms::Publishing> {
    pub(crate) handler: H,
    pub(crate) dest: Dest,
    pub(crate) docs: Docs,
    pub(crate) _types: Carried<(In, R, C, F)>,
}

impl<In, R, H, Dest, C, F> fmt::Debug for ReplyingValue<In, R, H, Dest, C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyingValue").finish_non_exhaustive()
    }
}

impl<In, R, H, Dest, C, F> IncludeDef for ReplyingValue<In, R, H, Dest, C, F> {
    type Form = F;
}

impl<In: InputKind, R, H, Dest, C, F> DocumentedValue for ReplyingValue<In, R, H, Dest, C, F> {
    type Payload = In::Target;
    type Reply = R;

    fn docs_mut(&mut self) -> &mut Docs {
        &mut self.docs
    }
}

/// The metadata methods shared by the two destination states.
macro_rules! replying_def_common {
    () => {
        type Input = In;
        type Injections = ();
        type Reply = R;
        type Context = C;
        // The stored value never builds a source: the settings builder wrapping it carries the
        // real one (see `SubscriberValue::Source`).
        type Source = Unnamed<Name>;

        fn source(&self) -> Self::Source {
            Unnamed::new()
        }

        docs_metadata!();

        fn outgoing(&self) -> Vec<OutgoingMessageMetadata> {
            vec![
                OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R>())
                    .with_payload_schema(self.docs.reply_schema()),
            ]
        }
    };
}

// The caller-named destination: `.to(..)` stored the subject.
impl<In, R, H, C, F> PublishingDef for ReplyingValue<In, R, H, To, C, F>
where
    In: InputKind,
    R: Send + Sync,
    H: Send + Sync,
    C: Send + Sync,
    F: Send + Sync,
{
    replying_def_common!();

    fn reply_name(&self) -> &str {
        &self.dest.0
    }
}

// The declared destination: the reply type fixed its own name, so no call site names one.
impl<In, R, H, C, F> PublishingDef for ReplyingValue<In, R, H, DeclaredName, C, F>
where
    In: InputKind,
    R: OutgoingDestination<Form = FixedName> + Send + Sync,
    H: Send + Sync,
    C: Send + Sync,
    F: Send + Sync,
{
    replying_def_common!();

    fn reply_name(&self) -> &str {
        R::ADDRESS
    }
}

impl<In, R, H, Dest, C, F, S> PublishingCall<S> for ReplyingValue<In, R, H, Dest, C, F>
where
    Self: PublishingDef<Input = In, Injections = (), Reply = R, Context = C>,
    In: InputKind,
    H: Reply<In::Target, C, S, Out = R>,
    C: Send + Sync,
    S: Send + Sync,
{
    fn call(
        &self,
        input: &In::Target,
        _injections: &(),
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<R, HandlerResult>> + Send {
        self.handler.reply(input, ctx)
    }
}

/// The chain over a caller-named destination: what [`to`](SubscriberBuilder::to) hands back,
/// every other piece carried unchanged.
type Renamed<In, R, H, C, F, Src, State, DC> =
    SubscriberBuilder<ReplyingValue<In, R, H, To, C, F>, Src, State, DC>;

impl<In, R, H, C, F, Src, State, DC>
    SubscriberBuilder<ReplyingValue<In, R, H, DeclaredName, C, F>, Src, State, DC>
{
    /// Names the subject the reply is published to.
    ///
    /// Without it the destination comes from the reply type's own `#[outgoing(name = "...")]`
    /// declaration; a definition with neither is not mountable.
    #[must_use]
    pub fn to(self, name: impl Into<Cow<'static, str>>) -> Renamed<In, R, H, C, F, Src, State, DC> {
        self.map_def(|def| ReplyingValue {
            handler: def.handler,
            dest: To(name.into()),
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

/// What [`replying`] returns: the settings builder over the reply definition, its destination
/// still the reply type's declared one.
pub type ReplyingBuilder<Src, Tgt, H, C = (), F = forms::Publishing, S = ()> = SubscriberBuilder<
    ReplyingValue<<Tgt as HandledInput>::Kind, <H as Reply<Tgt, C, S>>::Out, H, DeclaredName, C, F>,
    <Src as IntoSource>::Source,
    AllOpen,
>;

fn build_replying<Src, In, R, H, C, F>(
    source: Src,
    handler: H,
) -> super::ValueBuilder<ReplyingValue<In, R, H, DeclaredName, C, F>, Src>
where
    Src: IntoSource,
{
    SubscriberBuilder::new(
        ReplyingValue {
            handler,
            dest: DeclaredName,
            docs: Docs::none(),
            _types: PhantomData,
        },
        source.into_source(),
    )
}

/// Binds a reply-producing `handler` to the subscription `source`: the value-path counterpart
/// of `#[subscriber("in", publish("out"))]`.
///
/// Mount it with `include`, chaining `.publisher(..)` to attach the reply publish policy (or
/// nothing, for the broker's default). The input follows the body: a [`Reply<T>`] body decodes
/// the payload into `T`, a [`Reply<[u8]>`](Reply) body takes it raw. The destination follows
/// the reply type's `#[outgoing(name = "...")]` declaration; a type declaring none takes a
/// mandatory [`to`](SubscriberBuilder::to) call, and leaving both out does not compile. A
/// templated declaration does not fit a reply (nothing binds its placeholders per publish), so
/// it also requires [`to`](SubscriberBuilder::to). A body implemented for one concrete app
/// state type takes [`replying_in`] instead.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::future::{Future, ready};
///
/// use ruststream::memory::MemoryBroker;
/// use ruststream::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
///     quantity: u32,
/// }
///
/// #[derive(Serialize)]
/// struct Confirmation {
///     id: u64,
///     accepted: bool,
/// }
///
/// struct Confirm;
///
/// impl Reply<Order> for Confirm {
///     type Out = Confirmation;
///
///     fn reply(
///         &self,
///         order: &Order,
///         _ctx: &mut Context<'_>,
///     ) -> impl Future<Output = Result<Confirmation, HandlerResult>> + Send {
///         ready(Ok(Confirmation {
///             id: order.id,
///             accepted: order.quantity > 0,
///         }))
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(replying("orders", Confirm).to("confirmations"));
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn replying<Src, Tgt, C, H>(source: Src, handler: H) -> ReplyingBuilder<Src, Tgt, H, C>
where
    Src: IntoSource,
    Tgt: ?Sized + HandledInput,
    H: Reply<Tgt, C>,
{
    build_replying(source, handler)
}

/// [`replying`] for a body implemented for one concrete app state type. See [`subscriber_in`]
/// for the split.
///
/// [`subscriber_in`]: super::subscriber_in
#[must_use]
pub fn replying_in<Src, Tgt, C, St, H>(
    source: Src,
    handler: H,
) -> ReplyingBuilder<Src, Tgt, H, C, forms::Publishing, St>
where
    Src: IntoSource,
    Tgt: ?Sized + HandledInput,
    H: Reply<Tgt, C, St>,
{
    build_replying(source, handler)
}

/// [`replying`] whose reply travels a bare publisher, byte for byte: the value-path counterpart
/// of `#[subscriber("in", publish_raw("out"))]`.
///
/// The body's `Out` must be byte-shaped (`AsRef<[u8]>`), which the mount checks; no codec
/// touches the reply, so no codec feature is demanded when the input side is raw too.
#[must_use]
pub fn raw_replying<Src, Tgt, C, H>(
    source: Src,
    handler: H,
) -> ReplyingBuilder<Src, Tgt, H, C, forms::RawReply>
where
    Src: IntoSource,
    Tgt: ?Sized + HandledInput,
    H: Reply<Tgt, C>,
{
    build_replying(source, handler)
}

/// [`raw_replying`] for a body implemented for one concrete app state type. See
/// [`subscriber_in`] for the split.
///
/// [`subscriber_in`]: super::subscriber_in
#[must_use]
pub fn raw_replying_in<Src, Tgt, C, St, H>(
    source: Src,
    handler: H,
) -> ReplyingBuilder<Src, Tgt, H, C, forms::RawReply, St>
where
    Src: IntoSource,
    Tgt: ?Sized + HandledInput,
    H: Reply<Tgt, C, St>,
{
    build_replying(source, handler)
}
