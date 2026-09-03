//! The definition chain behind [`subscriber`]: the value the constructor returns, the steps it
//! grows by, and the `.build()` seal the mount machinery accepts.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use crate::runtime::publish::{Transactional, TypedPublisher};
use crate::runtime::router::DefaultReply;
use crate::runtime::settings::SubscriberBuilder;
use crate::runtime::slot::WithSource;

use super::axis::{Axis, Input, PagedAxis};
use super::docs::{Docs, Documented, Probed, ProbedDocs, Undocumented};
use super::reply::ReplyRoute;
use super::{Handle, IntoSource};

/// The phantom carrying a definition's axes.
///
/// The state axis `S` is deliberately absent: state is a quantification point, not data, so it
/// lives on the body's [`Handle`] impl (a concrete state pins it, a generic impl mounts on any
/// app) and the adapters quantify over it at their own impls.
type HandleAxes<A, R, O, C, Doc> = PhantomData<fn() -> (A, R, O, C, Doc)>;

/// The chain over a plain definition at the documentation state `Doc`.
type PlainChain<A, R, O, C, H, Doc, Src, State, DC> =
    SubscriberBuilder<HandleValue<A, R, O, C, H, Doc>, Src, State, DC>;

/// The chain over a reply-wired definition.
type ReplyChain<V, Dest, Attach, Src, State, DC> =
    SubscriberBuilder<ReplyValue<V, Dest, Attach>, Src, State, DC>;

/// The chain [`reply`](SubscriberBuilder::reply) hands back: the wrapped definition at the
/// declared destination and the default attach.
type ReplyStart<A, R, O, C, H, Doc, Attach, Src, State, DC> =
    ReplyChain<HandleValue<A, R, O, C, H, Doc>, DeclaredDest, Attach, Src, State, DC>;

/// The sealed chain [`build`](SubscriberBuilder::build) hands back.
type SealedChain<V, Src, State, DC> = SubscriberBuilder<Sealed<V>, Src, State, DC>;

/// The chain [`publisher`](SubscriberBuilder::publisher) hands back: the wrapped attachment.
type WiredReplyChain<A, R, O, C, H, Doc, Dest, Wire, Fam, Src, State, DC> = SubscriberBuilder<
    ReplyValue<
        HandleValue<A, R, O, C, H, Doc>,
        Dest,
        <Wire as ReplyAttach<<R as ReplyRoute<Fam>>::Wire>>::Attach,
    >,
    Src,
    State,
    DC,
>;

/// The fresh chain [`subscriber`] hands back.
type FreshChain<A, R, O, C, H, Src> = super::ValueBuilder<HandleValue<A, R, O, C, H>, Src>;

/// The sealed plain chain.
type SealedPlainChain<A, R, O, C, H, Doc, Src, State, DC> =
    SealedChain<HandleValue<A, R, O, C, H, Doc>, Src, State, DC>;

/// The reply chain at the opted-out documentation state.
type UndocumentedReplyChain<A, R, O, C, H, Dest, Attach, Src, State, DC> =
    ReplyChain<HandleValue<A, R, O, C, H, Undocumented>, Dest, Attach, Src, State, DC>;

/// The sealed reply chain.
type SealedReplyChain<A, R, O, C, H, Doc, Dest, Attach, Src, State, DC> =
    SealedChain<ReplyValue<HandleValue<A, R, O, C, H, Doc>, Dest, Attach>, Src, State, DC>;

/// The definition under construction: what [`subscriber`] returns, wrapped in the settings
/// builder. You never name this type; chain on it and seal with
/// [`build`](SubscriberBuilder::build).
pub struct HandleValue<A, R, O, C, H, Doc = Documented> {
    pub(super) body: H,
    pub(super) docs: Docs,
    pub(super) page_cap: Option<NonZeroUsize>,
    pub(super) _axes: HandleAxes<A, R, O, C, Doc>,
}

impl<A, R, O, C, H, Doc> fmt::Debug for HandleValue<A, R, O, C, H, Doc> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleValue").finish_non_exhaustive()
    }
}

impl<A, R, O, C, H, Doc> HandleValue<A, R, O, C, H, Doc> {
    /// Rewraps the value at another documentation state, keeping everything else.
    fn with_doc<NewDoc>(self) -> HandleValue<A, R, O, C, H, NewDoc> {
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
/// ([`reply`](SubscriberBuilder::reply), [`to`](SubscriberBuilder::to),
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
///     async fn handle(&self, order: &Order, _outs: &(), _ctx: &mut Context<'_>) -> Result<(), HandlerOutcome> {
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
) -> FreshChain<In::Axis, R, O, C, H, Src>
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

// --------------------------------------------------------------------- the macro expansion seam

/// Builds a `#[subscriber]` expansion's sealed plain definition: the same value the
/// `subscriber(..) .. .build()` chain produces, at the probe-captured documentation state
/// (see [`ProbedDocs`]) instead of the documented-by-default obligations. Machinery behind the
/// macro expansion; not part of the public API.
#[doc(hidden)]
#[must_use]
pub fn probed_def<A, R, O, C, H>(
    body: H,
    docs: ProbedDocs,
) -> Sealed<HandleValue<A, R, O, C, H, Probed>> {
    Sealed(HandleValue {
        body,
        docs: docs.into_docs(),
        page_cap: None,
        _axes: PhantomData,
    })
}

/// The sealed reply definition [`probed_reply_def`] builds. Names the projection once; the
/// macro spells the concrete form itself.
#[doc(hidden)]
pub type ProbedReplyDef<A, R, O, C, H> =
    Sealed<ReplyValue<HandleValue<A, R, O, C, H, Probed>, NamedDest, DefaultReply>>;

/// Builds a `#[subscriber]` expansion's sealed reply definition: the plain definition wrapped
/// at the clause-named destination with the default attach - the reply's wire comes from the
/// reply type itself, as everywhere. Machinery behind the macro expansion; not part of the
/// public API.
// The explicit `&'static str` parameter keeps a wrongly-typed destination expression a plain
// type error at the expansion site, as the attribute always reported it.
#[doc(hidden)]
#[must_use]
pub fn probed_reply_def<A, R, O, C, H>(
    body: H,
    docs: ProbedDocs,
    dest: &'static str,
) -> ProbedReplyDef<A, R, O, C, H> {
    let Sealed(value) = probed_def(body, docs);
    Sealed(ReplyValue {
        value,
        dest: NamedDest(Cow::Borrowed(dest)),
        attach: DefaultReply,
    })
}

// ------------------------------------------------------------------------- the reply wiring

/// The reply destination still unnamed: it resolves from the reply type's own
/// `#[outgoing(name = "..")]` declaration, and a type declaring none takes a mandatory
/// [`to`](SubscriberBuilder::to).
#[derive(Debug, Clone, Copy, Default)]
pub struct DeclaredDest;

/// The reply destination the chain named with [`to`](SubscriberBuilder::to).
#[derive(Debug, Clone)]
pub struct NamedDest(pub(super) Cow<'static, str>);

/// The encoded reply wire: the framework's reply codec serializes the value
/// (`serde::Serialize` replies, and [`Message`](super::Message) pairs of them).
#[derive(Debug, Clone, Copy)]
pub struct EncodedReply;

/// The serialized reply wire: the value already carries its bytes
/// ([`Serialized`](super::Serialized) replies), and they leave as they are, with no codec.
#[derive(Debug, Clone, Copy)]
pub struct SerializedReply;

/// What [`publisher`](SubscriberBuilder::publisher) accepts on the reply chain, per the reply
/// type's wire.
///
/// The wire follows the reply type: a `serde::Serialize` reply encodes through a
/// [`TypedPublisher`] (or its [`Transactional`] batch wiring), a
/// [`Serialized`](super::Serialized) reply takes the publish policy itself and its bytes leave
/// as they are.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not attach this reply's publisher",
    note = "an encoded reply (`serde::Serialize`) takes `TypedPublisher::new(policy)` \
            (`.transactional()` publishes a page's replies in one transaction); a `Serialized` \
            reply takes the publish policy itself"
)]
pub trait ReplyAttach<Wire> {
    /// The attach the chain stores.
    #[doc(hidden)]
    type Attach;

    /// Wraps the attachment for the chain.
    #[doc(hidden)]
    fn into_attach(self) -> Self::Attach;
}

impl<P, C, PL, BL> ReplyAttach<EncodedReply> for TypedPublisher<P, C, PL, BL> {
    type Attach = WithSource<Self>;

    fn into_attach(self) -> WithSource<Self> {
        WithSource::new(self)
    }
}

impl<P, C, PL, BL> ReplyAttach<EncodedReply> for Transactional<P, C, PL, BL> {
    type Attach = WithSource<Self>;

    fn into_attach(self) -> WithSource<Self> {
        WithSource::new(self)
    }
}

// Any policy attaches to a serialized reply: the wire needs no codec, so there is nothing to
// wrap the policy in.
impl<Policy> ReplyAttach<SerializedReply> for Policy {
    type Attach = WithSource<Policy>;

    fn into_attach(self) -> WithSource<Policy> {
        WithSource::new(self)
    }
}

/// A definition whose body's reply the chain is wiring: what
/// [`reply`](SubscriberBuilder::reply) wraps the definition in.
pub struct ReplyValue<V, Dest, Attach> {
    pub(super) value: V,
    pub(super) dest: Dest,
    pub(super) attach: Attach,
}

impl<V, Dest, Attach> fmt::Debug for ReplyValue<V, Dest, Attach> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyValue").finish_non_exhaustive()
    }
}

impl<V, Dest, Attach> ReplyValue<V, Dest, Attach> {
    fn map_value<W>(self, f: impl FnOnce(V) -> W) -> ReplyValue<W, Dest, Attach> {
        ReplyValue {
            value: f(self.value),
            dest: self.dest,
            attach: self.attach,
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
impl<A, R, O, C, H, Doc> crate::runtime::router::IncludeDef for HandleValue<A, R, O, C, H, Doc> {
    type Form = UnbuiltDefinition;
}

impl<V, Dest, Attach> crate::runtime::router::IncludeDef for ReplyValue<V, Dest, Attach> {
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

impl<A, R, O, C, H, Doc, Src, State, DC>
    SubscriberBuilder<HandleValue<A, R, O, C, H, Doc>, Src, State, DC>
{
    /// Sets the handler's human description for the generated document, the value-path
    /// counterpart of the attribute reading the handler's doc comment.
    #[must_use]
    pub fn describe(self, text: impl Into<Cow<'static, str>>) -> Self {
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
    pub fn undocumented(self) -> PlainChain<A, R, O, C, H, Undocumented, Src, State, DC>
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
    pub fn batch(self, max: NonZeroUsize) -> Self
    where
        A: PagedAxis,
    {
        self.map_def(|mut def| {
            def.page_cap = Some(max);
            def
        })
    }

    /// Declares the body's reply wired for publishing: the reply type's declared destination
    /// applies (name one with [`to`](SubscriberBuilder::to)), and the reply publish policy
    /// attaches with [`publisher`](SubscriberBuilder::publisher) (the broker's default without
    /// it).
    ///
    /// The wire follows the reply type: a `serde::Serialize` reply encodes through the reply
    /// publisher's codec, a [`Serialized`](super::Serialized) reply's bytes leave as they are.
    #[must_use]
    pub fn reply(self) -> ReplyStart<A, R, O, C, H, Doc, DefaultReply, Src, State, DC> {
        self.map_def(|value| ReplyValue {
            value,
            dest: DeclaredDest,
            attach: DefaultReply,
        })
    }

    /// Seals the definition for `include`.
    #[must_use]
    pub fn build(self) -> SealedPlainChain<A, R, O, C, H, Doc, Src, State, DC> {
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

impl<V, Attach, Src, State, DC>
    SubscriberBuilder<ReplyValue<V, DeclaredDest, Attach>, Src, State, DC>
{
    /// Names the subject the reply is published to, overriding nothing: without this call the
    /// destination comes from the reply type's own `#[outgoing(name = "..")]` declaration, and
    /// a type declaring none does not mount.
    #[must_use]
    pub fn to(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<ReplyValue<V, NamedDest, Attach>, Src, State, DC> {
        self.map_def(|def| ReplyValue {
            value: def.value,
            dest: NamedDest(name.into()),
            attach: def.attach,
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

impl<A, R, O, C, H, Doc, Dest, Attach: DefaultReplyAttach, Src, State, DC>
    SubscriberBuilder<ReplyValue<HandleValue<A, R, O, C, H, Doc>, Dest, Attach>, Src, State, DC>
{
    /// Attaches the reply publish policy. Policies are connection-free declarations, so the
    /// definition carries the attachment; without this call the broker's default policy
    /// applies.
    ///
    /// The reply type's wire decides the argument's form (see [`ReplyAttach`]): an encoded
    /// reply takes a [`TypedPublisher`] naming the reply codec (or its [`Transactional`] batch
    /// wiring), a [`Serialized`](super::Serialized) reply takes the publish policy itself.
    // Every parameter is one axis or chain state the wire step carries through; the count is
    // the typestate itself, not incidental nesting an alias could hide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn publisher<Wire>(
        self,
        wire: Wire,
    ) -> WiredReplyChain<A, R, O, C, H, Doc, Dest, Wire, A::Family, Src, State, DC>
    where
        A: Axis,
        R: ReplyRoute<A::Family>,
        Wire: ReplyAttach<R::Wire>,
    {
        self.map_def(|def| ReplyValue {
            value: def.value,
            dest: def.dest,
            attach: wire.into_attach(),
        })
    }
}

// The inner-value steps stay reachable after `.reply()`: `.batch(..)`, `.describe(..)` and
// `.undocumented()` reach through the wrapper, so the chain order is free.
impl<A, R, O, C, H, Doc, Dest, Attach, Src, State, DC>
    SubscriberBuilder<ReplyValue<HandleValue<A, R, O, C, H, Doc>, Dest, Attach>, Src, State, DC>
{
    /// See [`describe`](SubscriberBuilder::describe) on the plain chain.
    #[must_use]
    pub fn describe(self, text: impl Into<Cow<'static, str>>) -> Self {
        self.map_def(|def| {
            def.map_value(|mut value| {
                value.docs.description = Some(text.into());
                value
            })
        })
    }

    /// See [`undocumented`](SubscriberBuilder::undocumented) on the plain chain.
    #[must_use]
    pub fn undocumented(self) -> UndocumentedReplyChain<A, R, O, C, H, Dest, Attach, Src, State, DC>
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
    // Every parameter is one axis or chain state the seal carries through; the count is the
    // typestate itself, not incidental nesting an alias could hide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn build(self) -> SealedReplyChain<A, R, O, C, H, Doc, Dest, Attach, Src, State, DC> {
        self.map_def(Sealed)
    }
}
