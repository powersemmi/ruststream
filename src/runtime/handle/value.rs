//! The definition chain behind [`subscriber`]: the value the constructor returns, the steps it
//! grows by, and the `.build()` seal the mount machinery accepts.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use crate::runtime::settings::{CapsBatches, SubscriberBuilder};

use super::axis::{Batch, BatchDeserialized, BatchPair, Input};
use super::docs::{Docs, Documented, Probed, ProbedDocs, Undocumented};
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
type ReplyChain<V, Dest, Src, State, DC> = SubscriberBuilder<ReplyValue<V, Dest>, Src, State, DC>;

/// The chain [`reply`](SubscriberBuilder::reply) hands back: the wrapped definition at the
/// declared destination.
type ReplyStart<A, R, O, C, H, Doc, Src, State, DC> =
    ReplyChain<HandleValue<A, R, O, C, H, Doc>, DeclaredDest, Src, State, DC>;

/// The sealed chain [`build`](SubscriberBuilder::build) hands back.
type SealedChain<V, Src, State, DC> = SubscriberBuilder<Sealed<V>, Src, State, DC>;

/// The fresh chain [`subscriber`] hands back.
type FreshChain<A, R, O, C, H, Src> = super::ValueBuilder<HandleValue<A, R, O, C, H>, Src>;

/// The sealed plain chain.
type SealedPlainChain<A, R, O, C, H, Doc, Src, State, DC> =
    SealedChain<HandleValue<A, R, O, C, H, Doc>, Src, State, DC>;

/// The reply chain at the opted-out documentation state.
type UndocumentedReplyChain<A, R, O, C, H, Dest, Src, State, DC> =
    ReplyChain<HandleValue<A, R, O, C, H, Undocumented>, Dest, Src, State, DC>;

/// The sealed reply chain.
type SealedReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC> =
    SealedChain<ReplyValue<HandleValue<A, R, O, C, H, Doc>, Dest>, Src, State, DC>;

/// The definition under construction: what [`subscriber`] returns, wrapped in the settings
/// builder. You never name this type; chain on it and seal with
/// [`build`](SubscriberBuilder::build).
pub struct HandleValue<A, R, O, C, H, Doc = Documented> {
    pub(super) body: H,
    pub(super) docs: Docs,
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
            _axes: PhantomData,
        }
    }
}

/// Binds a [`Handle`] body to its subscription source; the one mounting verb of the manual
/// path.
///
/// Every axis comes from the body's `impl Handle<..>`: the input spelling (single, raw, batch,
/// typed-headers pair), the reply type, the injections arena, the broker context and the typed
/// application state. The chain then carries what is not in the signature - the declarative
/// settings ([`workers`](crate::runtime::SubscriberSettings::workers),
/// [`on_failure`](crate::runtime::SubscriberSettings::on_failure),
/// [`start_at`](crate::runtime::SubscriberSettings::start_at), ...), what the body replies with
/// and where ([`reply`](SubscriberBuilder::reply), [`to`](SubscriberBuilder::to)), the batch size
/// ([`batch`](crate::runtime::SubscriberSettings::batch)) and the documentation opt-out
/// ([`undocumented`](SubscriberBuilder::undocumented)) - and
/// [`build`](SubscriberBuilder::build) seals the definition for
/// [`include`](crate::runtime::Router::include).
///
/// What the definition never carries is who publishes: a policy is a broker's, and a definition
/// is broker-agnostic, so the reply's publisher is named at the mount site with
/// `.out(Reply, policy)`.
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
            _axes: PhantomData,
        },
        source.into_source(),
    )
}
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
        _axes: PhantomData,
    })
}

/// The sealed reply definition [`probed_reply_def`] builds. Names the projection once; the
/// macro spells the concrete form itself.
#[doc(hidden)]
pub type ProbedReplyDef<A, R, O, C, H> =
    Sealed<ReplyValue<HandleValue<A, R, O, C, H, Probed>, NamedDest>>;

/// Builds a `#[subscriber]` expansion's sealed reply definition: the plain definition wrapped
/// at the clause-named destination - the reply's wire comes from the reply type itself, as
/// everywhere. Machinery behind the macro expansion; not part of the public API.
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
    })
}
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

/// A definition whose body declares a reply: what [`reply`](SubscriberBuilder::reply) wraps the
/// definition in.
///
/// It carries what the reply is and where it goes, and nothing about who publishes it: that is a
/// broker's policy, named at the mount site with `.out(Reply, policy)`.
pub struct ReplyValue<V, Dest> {
    pub(super) value: V,
    pub(super) dest: Dest,
}

impl<V, Dest> fmt::Debug for ReplyValue<V, Dest> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyValue").finish_non_exhaustive()
    }
}

impl<V, Dest> ReplyValue<V, Dest> {
    fn map_value<W>(self, f: impl FnOnce(V) -> W) -> ReplyValue<W, Dest> {
        ReplyValue {
            value: f(self.value),
            dest: self.dest,
        }
    }
}

/// The not-yet-sealed chain's form token: it has no mount on any surface, so `include` on a
/// chain missing its `.build()` fails to compile with this token in the message.
#[derive(Debug, Clone, Copy)]
pub struct UnbuiltDefinition;

// The settings chain (`.workers(..)`, `.batch(..)`, ...) rides the `Declared` blanket over
// `IncludeDef`, so the unsealed values carry the diagnostic form token: settings chain freely,
// and a mount before `.build()` names the missing step.
impl<A, R, O, C, H, Doc> crate::runtime::router::IncludeDef for HandleValue<A, R, O, C, H, Doc> {
    type Form = UnbuiltDefinition;
}

impl<V, Dest> crate::runtime::router::IncludeDef for ReplyValue<V, Dest> {
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

// Which definitions may name a batch size: every batch form, with the reply axis and the
// injections arena free, because the size is the subscription's parameter and none of those
// axes changes how a batch is opened. The size itself rides the settings builder, not the
// definition; this only says the step belongs here.
//
// The three batch spellings are named one by one rather than through a `BatchedAxis` bound on one
// impl: a bound inside a matching impl is what the compiler reports back, and the axis marker's
// name is machinery. With no impl matching a single-message definition, the missing `CapsBatches`
// carries the message instead.
impl<T, R, O, C, H, Doc> CapsBatches for HandleValue<Batch<T>, R, O, C, H, Doc> {}

impl<F, R, O, C, H, Doc> CapsBatches for HandleValue<BatchDeserialized<F>, R, O, C, H, Doc> {}

impl<Hd, P, R, O, C, H, Doc> CapsBatches for HandleValue<BatchPair<Hd, P>, R, O, C, H, Doc> {}

// The reply wrapper and the seal are transparent to the step, so the attribute path sizes the
// very definition the `subscriber(..)` chain does.
impl<V: CapsBatches, Dest> CapsBatches for ReplyValue<V, Dest> {}

impl<V: CapsBatches> CapsBatches for Sealed<V> {}
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

    /// Declares the body's reply published: the reply type's declared destination applies (name
    /// one with [`to`](SubscriberBuilder::to)).
    ///
    /// The wire follows the reply type: a `serde::Serialize` reply encodes through the reply
    /// publisher's codec, a [`Serialized`](super::Serialized) reply's bytes leave as they are.
    /// Which publisher that is belongs to the mount site (`.out(Reply, policy)`), so the
    /// definition stays broker-agnostic; without a call there the broker's own default applies.
    #[must_use]
    pub fn reply(self) -> ReplyStart<A, R, O, C, H, Doc, Src, State, DC> {
        self.map_def(|value| ReplyValue {
            value,
            dest: DeclaredDest,
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
impl<V, Src, State, DC> SubscriberBuilder<ReplyValue<V, DeclaredDest>, Src, State, DC> {
    /// Names the subject the reply is published to, overriding nothing: without this call the
    /// destination comes from the reply type's own `#[outgoing(name = "..")]` declaration, and
    /// a type declaring none does not mount.
    #[must_use]
    pub fn to(
        self,
        name: impl Into<Cow<'static, str>>,
    ) -> SubscriberBuilder<ReplyValue<V, NamedDest>, Src, State, DC> {
        self.map_def(|def| ReplyValue {
            value: def.value,
            dest: NamedDest(name.into()),
        })
    }
}

// The inner-value steps stay reachable after `.reply()`: `.describe(..)` and `.undocumented()`
// reach through the wrapper, so the chain order is free.
impl<A, R, O, C, H, Doc, Dest, Src, State, DC>
    SubscriberBuilder<ReplyValue<HandleValue<A, R, O, C, H, Doc>, Dest>, Src, State, DC>
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
    pub fn undocumented(self) -> UndocumentedReplyChain<A, R, O, C, H, Dest, Src, State, DC>
    where
        Doc: IsDocumented,
    {
        self.map_def(|def| def.map_value(HandleValue::with_doc))
    }

    /// Seals the definition for `include`.
    #[must_use]
    pub fn build(self) -> SealedReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC> {
        self.map_def(Sealed)
    }
}
