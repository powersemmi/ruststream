//! The reply-publishing value definition: a body producing a reply, and where the reply goes.

use std::any::type_name;
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;

use crate::schema::{FixedName, OutgoingDestination};
use crate::{Name, Unnamed};

use crate::runtime::context::Context;
use crate::runtime::handler::HandlerResult;
use crate::runtime::input::Decoded;
use crate::runtime::metadata::OutgoingMessageMetadata;
use crate::runtime::publishing::{PublishingCall, PublishingDef};
use crate::runtime::router::{IncludeDef, forms};
use crate::runtime::settings::{AllOpen, SubscriberBuilder};

use super::IntoSource;
use super::subscribing::Docs;

/// A handler body producing a reply: the value-path counterpart of a
/// `#[subscriber(.., publish(..))]` body.
///
/// `Ok(reply)` is encoded and published to the definition's destination, then the incoming
/// message is acked; `Err(result)` skips publishing and the dispatcher acts on the returned
/// [`HandlerResult`]. Closures implement it, and a named type implements it generically over the
/// state parameter `S` (reading typed state through
/// [`Context::state`](crate::runtime::Context::state) is checked where the definition mounts).
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not produce a reply for `{T}`",
    note = "implement `Reply<{T}>` on the handler type - `type Out` names the reply, and `reply` \
            returns `Result<Self::Out, HandlerResult>` - or pass a closure with that return type"
)]
pub trait Reply<T, C = (), S = ()>: Send + Sync {
    /// The reply type, encoded and published.
    type Out;

    /// Produces the reply for one decoded input.
    fn reply(
        &self,
        msg: &T,
        ctx: &mut Context<'_, C, S>,
    ) -> impl Future<Output = Result<Self::Out, HandlerResult>> + Send;
}

impl<T, C, S, F, Fut, R> Reply<T, C, S> for F
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

/// What [`replying`] returns: the settings builder over the reply definition, its destination
/// still the reply type's declared one.
pub type ReplyingBuilder<Src, T, H> = SubscriberBuilder<
    ReplyingValue<T, <H as Reply<T>>::Out, H, DeclaredName>,
    <Src as IntoSource>::Source,
    AllOpen,
>;

/// A reply-publishing definition built from a value: what `replying(source, handler)` returns,
/// wrapped in the settings builder.
pub struct ReplyingValue<T, R, H, Dest> {
    pub(crate) handler: H,
    pub(crate) dest: Dest,
    pub(crate) docs: Docs,
    pub(crate) _types: PhantomData<fn() -> (T, R)>,
}

impl<T, R, H, Dest> fmt::Debug for ReplyingValue<T, R, H, Dest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyingValue").finish_non_exhaustive()
    }
}

impl<T, R, H, Dest> IncludeDef for ReplyingValue<T, R, H, Dest> {
    type Form = forms::Publishing;
}

/// The metadata methods shared by the two destination states.
macro_rules! replying_def_common {
    () => {
        type Input = Decoded<T>;
        type Injections = ();
        type Reply = R;
        type Context = ();
        // The stored value never builds a source: the settings builder wrapping it carries the
        // real one (see `SubscriberValue::Source`).
        type Source = Unnamed<Name>;

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
            vec![
                OutgoingMessageMetadata::new(self.reply_name().to_owned(), type_name::<R>())
                    .with_payload_schema(self.docs.reply_schema.and_then(|capture| capture())),
            ]
        }
    };
}

// The caller-named destination: `.to(..)` stored the subject.
impl<T, R, H> PublishingDef for ReplyingValue<T, R, H, To>
where
    T: Send + Sync + 'static,
    R: Send + Sync,
    H: Send + Sync,
{
    replying_def_common!();

    fn reply_name(&self) -> &str {
        &self.dest.0
    }
}

// The declared destination: the reply type fixed its own name, so no call site names one.
impl<T, R, H> PublishingDef for ReplyingValue<T, R, H, DeclaredName>
where
    T: Send + Sync + 'static,
    R: OutgoingDestination<Form = FixedName> + Send + Sync,
    H: Send + Sync,
{
    replying_def_common!();

    fn reply_name(&self) -> &str {
        R::ADDRESS
    }
}

impl<T, R, H, Dest, S> PublishingCall<S> for ReplyingValue<T, R, H, Dest>
where
    Self: PublishingDef<Input = Decoded<T>, Injections = (), Reply = R, Context = ()>,
    T: Send + Sync + 'static,
    H: Reply<T, (), S, Out = R>,
    S: Send + Sync,
{
    fn call(
        &self,
        input: &T,
        _injections: &(),
        ctx: &mut Context<'_, (), S>,
    ) -> impl Future<Output = Result<R, HandlerResult>> + Send {
        self.handler.reply(input, ctx)
    }
}

impl<T, R, H, Src, State> SubscriberBuilder<ReplyingValue<T, R, H, DeclaredName>, Src, State> {
    /// Names the subject the reply is published to.
    ///
    /// Without it the destination comes from the reply type's own `#[outgoing(name = "...")]`
    /// declaration; a definition with neither is not mountable.
    #[must_use]
    pub fn to(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<ReplyingValue<T, R, H, To>, Src, State> {
        self.map_def(|def| ReplyingValue {
            handler: def.handler,
            dest: To(name.into()),
            docs: def.docs,
            _types: PhantomData,
        })
    }
}

impl<T, R, H, Dest, Src, State> SubscriberBuilder<ReplyingValue<T, R, H, Dest>, Src, State> {
    /// Sets the handler's human description for the generated `AsyncAPI` document, the
    /// value-path counterpart of the attribute reading the handler's doc comment.
    #[must_use]
    pub fn describe(self, text: impl Into<Cow<'static, str>>) -> Self {
        self.map_def(|mut def| {
            def.docs.description = Some(text.into());
            def
        })
    }

    /// Reports the input and reply types' JSON Schemas in the generated `AsyncAPI` document.
    /// See [`documented`](SubscriberBuilder::documented) on the plain form.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub fn documented(self) -> Self
    where
        T: schemars::JsonSchema,
        R: schemars::JsonSchema,
    {
        self.map_def(|mut def| {
            def.docs.schema = Some(super::schema_json_of::<T>);
            def.docs.reply_schema = Some(super::schema_json_of::<R>);
            def
        })
    }
}

/// Binds a reply-producing `handler` to the subscription `source`: the value-path counterpart
/// of `#[subscriber("in", publish("out"))]`.
///
/// Mount it with `include`, chaining `.publisher(..)` to attach the reply publish policy (or
/// nothing, for the broker's default).
///
/// The destination follows the reply type's `#[outgoing(name = "...")]` declaration; a type
/// declaring none takes a mandatory [`to`](SubscriberBuilder::to) call, and leaving both out
/// does not compile. A templated declaration does not fit a reply (nothing binds its
/// placeholders per publish), so it also requires [`to`](SubscriberBuilder::to).
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
pub fn replying<Src, T, H>(source: Src, handler: H) -> ReplyingBuilder<Src, T, H>
where
    Src: IntoSource,
    T: DeserializeOwned + Send + Sync + 'static,
    H: Reply<T>,
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
