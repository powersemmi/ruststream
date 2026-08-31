//! The definition chain behind [`subscriber`]: the value the constructor returns, the steps it
//! grows by, and the `.build()` seal the mount machinery accepts.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use crate::runtime::router::{DefaultBareReply, DefaultReply};
use crate::runtime::settings::SubscriberBuilder;

use super::axis::{Input, PagedAxis};
use super::docs::{Docs, Documented, Undocumented};
use super::{Handle, IntoSource, ValueBuilder};

/// The definition under construction: what [`subscriber`] returns, wrapped in the settings
/// builder. You never name this type; chain on it and seal with
/// [`build`](SubscriberBuilder::build).
pub struct HandleValue<A, R, O, C, S, H, Doc = Documented> {
    pub(super) body: H,
    pub(super) docs: Docs,
    pub(super) page_cap: Option<NonZeroUsize>,
    pub(super) _axes: PhantomData<fn() -> (A, R, O, C, S, Doc)>,
}

impl<A, R, O, C, S, H, Doc> fmt::Debug for HandleValue<A, R, O, C, S, H, Doc> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleValue").finish_non_exhaustive()
    }
}

impl<A, R, O, C, S, H, Doc> HandleValue<A, R, O, C, S, H, Doc> {
    /// Rewraps the value at another documentation state, keeping everything else.
    fn with_doc<NewDoc>(self) -> HandleValue<A, R, O, C, S, H, NewDoc> {
        HandleValue {
            body: self.body,
            docs: self.docs,
            page_cap: self.page_cap,
            _axes: PhantomData,
        }
    }
}

/// Binds a [`Handle`] body to its subscription source; the one mounting verb of the manual
/// path.
///
/// Every axis comes from the body's `impl Handle<..>`: the input spelling (single, raw, page,
/// typed-headers pair), the reply type, the injections arena, the broker context and the typed
/// application state. The chain then carries what is not in the signature - the declarative
/// settings ([`workers`](crate::runtime::SubscriberSettings::workers),
/// [`on_failure`](crate::runtime::SubscriberSettings::on_failure),
/// [`buffered`](crate::runtime::SubscriberSettings::buffered), ...), the reply wiring
/// ([`reply`](SubscriberBuilder::reply), [`on`](SubscriberBuilder::on),
/// [`publisher`](SubscriberBuilder::publisher)), the native page cap
/// ([`batch`](SubscriberBuilder::batch)) and the documentation opt-out
/// ([`undocumented`](SubscriberBuilder::undocumented)) - and
/// [`build`](SubscriberBuilder::build) seals the definition for
/// [`include`](crate::runtime::Router::include).
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # mod demo {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::prelude::*;
/// # #[derive(serde::Deserialize, schemars::JsonSchema)]
/// # struct Order { id: u64 }
///
/// struct Audit;
///
/// impl Handle<Order> for Audit {
///     async fn handle(&self, order: &Order, _outs: &(), _ctx: &mut Context<'_>) -> Result<(), HandlerResult> {
///         println!("order {}", order.id);
///         Ok(())
///     }
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(subscriber("orders", Audit).workers(nonzero!(4)).build());
///     })
/// }
/// # }
/// ```
#[must_use]
pub fn subscriber<Src, In, R, O, C, S, H>(
    source: Src,
    body: H,
) -> ValueBuilder<HandleValue<In::Axis, R, O, C, S, H>, Src>
where
    Src: IntoSource,
    In: ?Sized + Input,
    H: Handle<In, R, O, C, S>,
{
    SubscriberBuilder::new(
        HandleValue {
            body,
            docs: Docs::default(),
            page_cap: None,
            _axes: PhantomData,
        },
        source.into_source(),
    )
}

// ------------------------------------------------------------------------- the reply wiring

/// The reply destination still unnamed: it resolves from the reply type's own
/// `#[outgoing(name = "..")]` declaration, and a type declaring none takes a mandatory
/// [`on`](SubscriberBuilder::on).
#[derive(Debug, Clone, Copy, Default)]
pub struct DeclaredDest;

/// The reply destination the chain named with [`on`](SubscriberBuilder::on).
#[derive(Debug, Clone)]
pub struct NamedDest(pub(super) Cow<'static, str>);

/// The encoded reply route: the reply serializes through the reply publisher's codec.
#[derive(Debug, Clone, Copy)]
pub struct EncodedReply;

/// The bare reply route: the reply bytes leave as they are, through a bare publisher.
#[derive(Debug, Clone, Copy)]
pub struct BareReply;

/// A definition whose body's reply the chain is wiring: what
/// [`reply`](SubscriberBuilder::reply) wraps the definition in.
pub struct ReplyValue<V, Dest, Route, Attach> {
    pub(super) value: V,
    pub(super) dest: Dest,
    pub(super) attach: Attach,
    pub(super) _route: PhantomData<fn() -> Route>,
}

impl<V, Dest, Route, Attach> fmt::Debug for ReplyValue<V, Dest, Route, Attach> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyValue").finish_non_exhaustive()
    }
}

impl<V, Dest, Route, Attach> ReplyValue<V, Dest, Route, Attach> {
    fn map_value<W>(self, f: impl FnOnce(V) -> W) -> ReplyValue<W, Dest, Route, Attach> {
        ReplyValue {
            value: f(self.value),
            dest: self.dest,
            attach: self.attach,
            _route: PhantomData,
        }
    }
}

/// The not-yet-sealed chain's form token: it has no mount on any surface, so `include` on a
/// chain missing its `.build()` fails to compile with this token in the message.
#[derive(Debug, Clone, Copy)]
pub struct UnbuiltDefinition;

// The settings chain (`.workers(..)`, `.buffered(..)`, ...) rides the `Declared` blanket over
// `IncludeDef`, so the unsealed values carry the diagnostic form token: settings chain freely,
// and a mount before `.build()` names the missing step.
impl<A, R, O, C, S, H, Doc> crate::runtime::router::IncludeDef
    for HandleValue<A, R, O, C, S, H, Doc>
{
    type Form = UnbuiltDefinition;
}

impl<V, Dest, Route, Attach> crate::runtime::router::IncludeDef
    for ReplyValue<V, Dest, Route, Attach>
{
    type Form = UnbuiltDefinition;
}

/// The sealed definition: what [`build`](SubscriberBuilder::build) wraps the chain in, and the
/// only form `include` mounts on the manual path.
pub struct Sealed<V>(pub(super) V);

impl<V> fmt::Debug for Sealed<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sealed").finish_non_exhaustive()
    }
}

// ------------------------------------------------------------------ steps on the plain chain

impl<A, R, O, C, S, H, Doc, Src, State, DC>
    SubscriberBuilder<HandleValue<A, R, O, C, S, H, Doc>, Src, State, DC>
{
    /// Sets the handler's human description for the generated document, the value-path
    /// counterpart of the attribute reading the handler's doc comment.
    #[must_use]
    pub fn describe(
        self,
        text: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<HandleValue<A, R, O, C, S, H, Doc>, Src, State, DC> {
        self.map_def(|mut def| {
            def.docs.description = Some(text.into());
            def
        })
    }

    /// Opts this registration out of the generated document's schemas, lifting the
    /// `JsonSchema` obligation from its message types.
    ///
    /// Registrations are documented by default under the `asyncapi` feature; this is the
    /// per-registration exit.
    #[must_use]
    pub fn undocumented(
        self,
    ) -> SubscriberBuilder<HandleValue<A, R, O, C, S, H, Undocumented>, Src, State, DC>
    where
        Doc: IsDocumented,
    {
        self.map_def(HandleValue::with_doc)
    }

    /// Caps a native page at `max` elements: a larger batch is fed to the body in chunks of at
    /// most `max`, each settled on its own.
    ///
    /// Only a page body (`&[T]` and friends) has pages to cap; client-side batching over a
    /// single-message subscription is [`buffered`](crate::runtime::SubscriberSettings::buffered).
    #[must_use]
    pub fn batch(
        self,
        max: NonZeroUsize,
    ) -> SubscriberBuilder<HandleValue<A, R, O, C, S, H, Doc>, Src, State, DC>
    where
        A: PagedAxis,
    {
        self.map_def(|mut def| {
            def.page_cap = Some(max);
            def
        })
    }

    /// Declares the body's reply wired for publishing: the reply type's declared destination
    /// applies (name one with [`on`](SubscriberBuilder::on)), and the reply publish policy
    /// attaches with [`publisher`](SubscriberBuilder::publisher) (the broker's default without
    /// it).
    #[must_use]
    pub fn reply(
        self,
    ) -> SubscriberBuilder<
        ReplyValue<HandleValue<A, R, O, C, S, H, Doc>, DeclaredDest, EncodedReply, DefaultReply>,
        Src,
        State,
        DC,
    > {
        self.map_def(|value| ReplyValue {
            value,
            dest: DeclaredDest,
            attach: DefaultReply,
            _route: PhantomData,
        })
    }

    /// Declares the body's reply published bare: the returned bytes leave as they are, with no
    /// codec, through a bare publisher. The destination is named with
    /// [`on`](SubscriberBuilder::on).
    #[must_use]
    pub fn reply_raw(
        self,
    ) -> SubscriberBuilder<
        ReplyValue<HandleValue<A, R, O, C, S, H, Doc>, DeclaredDest, BareReply, DefaultBareReply>,
        Src,
        State,
        DC,
    > {
        self.map_def(|value| ReplyValue {
            value,
            dest: DeclaredDest,
            attach: DefaultBareReply,
            _route: PhantomData,
        })
    }

    /// Seals the definition for `include`.
    #[must_use]
    pub fn build(self) -> SubscriberBuilder<Sealed<HandleValue<A, R, O, C, S, H, Doc>>, Src, State, DC> {
        self.map_def(Sealed)
    }
}

/// The state a chain starts in: [`undocumented`](SubscriberBuilder::undocumented) has not been
/// called yet. Naming the opt-out twice is a compile error carrying this bound.
#[diagnostic::on_unimplemented(
    message = "this registration is already undocumented",
    label = "`.undocumented()` was already chained"
)]
pub trait IsDocumented {}
impl IsDocumented for Documented {}

// ------------------------------------------------------------------ steps on the reply chain

impl<V, Route, Attach, Src, State, DC>
    SubscriberBuilder<ReplyValue<V, DeclaredDest, Route, Attach>, Src, State, DC>
{
    /// Names the subject the reply is published to, overriding nothing: without this call the
    /// destination comes from the reply type's own `#[outgoing(name = "..")]` declaration, and
    /// a type declaring none does not mount.
    #[must_use]
    pub fn on(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<ReplyValue<V, NamedDest, Route, Attach>, Src, State, DC> {
        self.map_def(|def| ReplyValue {
            value: def.value,
            dest: NamedDest(name.into()),
            attach: def.attach,
            _route: PhantomData,
        })
    }
}

/// A reply attach still at the broker's default, replaceable by
/// [`publisher`](SubscriberBuilder::publisher). Naming a policy twice is a compile error
/// carrying this bound.
#[diagnostic::on_unimplemented(
    message = "this reply's publish policy is already attached",
    label = "`.publisher(..)` was already chained"
)]
pub trait DefaultReplyAttach {}
impl DefaultReplyAttach for DefaultReply {}
impl DefaultReplyAttach for DefaultBareReply {}

impl<V, Dest, Route, Attach: DefaultReplyAttach, Src, State, DC>
    SubscriberBuilder<ReplyValue<V, Dest, Route, Attach>, Src, State, DC>
{
    /// Attaches the reply publish policy. Policies are connection-free declarations, so the
    /// definition carries it; without this call the broker's default policy applies.
    #[must_use]
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> SubscriberBuilder<
        ReplyValue<V, Dest, Route, crate::runtime::slot::WithSource<Policy>>,
        Src,
        State,
        DC,
    > {
        self.map_def(|def| ReplyValue {
            value: def.value,
            dest: def.dest,
            attach: crate::runtime::slot::WithSource::new(policy),
            _route: PhantomData,
        })
    }
}

// The inner-value steps stay reachable after `.reply()`: `.batch(..)`, `.describe(..)` and
// `.undocumented()` reach through the wrapper, so the chain order is free.
impl<A, R, O, C, S, H, Doc, Dest, Route, Attach, Src, State, DC>
    SubscriberBuilder<
        ReplyValue<HandleValue<A, R, O, C, S, H, Doc>, Dest, Route, Attach>,
        Src,
        State,
        DC,
    >
{
    /// See [`describe`](SubscriberBuilder::describe) on the plain chain.
    #[must_use]
    pub fn describe(
        self,
        text: impl Into<Cow<'static, str>>,
    ) -> Self {
        self.map_def(|def| {
            def.map_value(|mut value| {
                value.docs.description = Some(text.into());
                value
            })
        })
    }

    /// See [`undocumented`](SubscriberBuilder::undocumented) on the plain chain.
    #[must_use]
    pub fn undocumented(
        self,
    ) -> SubscriberBuilder<
        ReplyValue<HandleValue<A, R, O, C, S, H, Undocumented>, Dest, Route, Attach>,
        Src,
        State,
        DC,
    >
    where
        Doc: IsDocumented,
    {
        self.map_def(|def| def.map_value(HandleValue::with_doc))
    }

    /// See [`batch`](SubscriberBuilder::batch) on the plain chain.
    #[must_use]
    pub fn batch(self, max: NonZeroUsize) -> Self
    where
        A: PagedAxis,
    {
        self.map_def(|def| {
            def.map_value(|mut value| {
                value.page_cap = Some(max);
                value
            })
        })
    }

    /// Seals the definition for `include`.
    #[must_use]
    pub fn build(
        self,
    ) -> SubscriberBuilder<
        Sealed<ReplyValue<HandleValue<A, R, O, C, S, H, Doc>, Dest, Route, Attach>>,
        Src,
        State,
        DC,
    > {
        self.map_def(Sealed)
    }
}
