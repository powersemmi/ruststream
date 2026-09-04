//! The definition chain behind [`subscriber`]: the value the constructor returns, the steps it
//! grows by, and the `.build()` seal the mount machinery accepts.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::publish::{
    AddBatchReplyTransform, AddReplyTransform, CodecSlotOpen, NameReplyCodec, PublishingDirectly,
    ReplyWiring, TransactionalReply,
};
use crate::runtime::router::DefaultReply;
use crate::runtime::settings::{
    BatchStep, CapsPages, FailureStep, MapSourceStep, NameStep, StartAtStep, SubscriberBuilder,
    WorkersStep,
};
use crate::runtime::slot::WithSource;

use super::axis::{Axis, Input, Page, PageDeserialized, PagePair};
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
/// Every axis comes from the body's `impl Handle<..>`: the input spelling (single, raw, page,
/// typed-headers pair), the reply type, the injections arena, the broker context and the typed
/// application state. The chain then carries what is not in the signature - the declarative
/// settings ([`workers`](crate::runtime::SubscriberSettings::workers),
/// [`on_failure`](crate::runtime::SubscriberSettings::on_failure),
/// [`buffered`](crate::runtime::SubscriberSettings::buffered), ...), the reply wiring
/// ([`reply`](SubscriberBuilder::reply), [`to`](SubscriberBuilder::to),
/// [`publisher`](SubscriberBuilder::publisher)), the page size
/// ([`batch`](crate::runtime::SubscriberSettings::batch)) and the documentation opt-out
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

/// What [`publisher`](SubscriberBuilder::publisher) makes of the policy it is handed, per the
/// reply type's wire.
///
/// The wire follows the reply type: a `serde::Serialize` reply opens a [`ReplyWiring`] the rest
/// of the chain grows (`.codec(..)`, `.transform(..)`, `.transactional()`), while a
/// [`Serialized`](super::Serialized) reply carries the publish policy itself and its bytes leave
/// as they are.
#[doc(hidden)]
pub trait ReplyAttach<Wire> {
    /// The wiring the chain carries until it seals.
    type Wiring;

    /// Opens it.
    fn into_wiring(self) -> Self::Wiring;
}

impl<Policy> ReplyAttach<EncodedReply> for Policy {
    type Wiring = ReplyWiring<Policy>;

    fn into_wiring(self) -> ReplyWiring<Policy> {
        ReplyWiring::new(self)
    }
}

// A serialized reply needs no codec and has no transform stack, so the policy travels bare.
impl<Policy> ReplyAttach<SerializedReply> for Policy {
    type Wiring = Policy;

    fn into_wiring(self) -> Policy {
        self
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

// The settings chain (`.workers(..)`, `.batch(..)`, ...) rides the `Declared` blanket over
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

// Which definitions may name a page size: every page form, with the reply axis and the
// injections arena free, because the size is the subscription's parameter and none of those
// axes changes how a page is opened. The size itself rides the settings builder, not the
// definition; this only says the step belongs here.
//
// The three page spellings are named one by one rather than through a `PagedAxis` bound on one
// impl: a bound inside a matching impl is what the compiler reports back, and the axis marker's
// name is machinery. With no impl matching a single-message definition, the missing `CapsPages`
// carries the message instead.
impl<T, R, O, C, H, Doc> CapsPages for HandleValue<Page<T>, R, O, C, H, Doc> {}

impl<F, R, O, C, H, Doc> CapsPages for HandleValue<PageDeserialized<F>, R, O, C, H, Doc> {}

impl<Hd, P, R, O, C, H, Doc> CapsPages for HandleValue<PagePair<Hd, P>, R, O, C, H, Doc> {}

// The reply wiring and the seal are transparent to the step, so the attribute path sizes the
// very definition the `subscriber(..)` chain does.
impl<V: CapsPages, Dest, Attach> CapsPages for ReplyValue<V, Dest, Attach> {}

impl<V: CapsPages> CapsPages for Sealed<V> {}

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
    /// Names the reply's publish policy and opens the reply wiring. Policies are connection-free
    /// declarations, so the definition carries the attachment; without this call the broker's
    /// default policy applies.
    ///
    /// The reply type's wire decides what the call opens (see [`ReplyAttach`]): an encoded reply
    /// gets a wiring the chain grows with [`codec`](ReplyWiringChain::codec),
    /// [`transform`](ReplyWiringChain::transform),
    /// [`batch_transform`](ReplyWiringChain::batch_transform) and
    /// [`transactional`](ReplyWiringChain::transactional), while a
    /// [`Serialized`](super::Serialized) reply carries the policy alone. Either way the chain
    /// seals with [`build`](ReplyWiringChain::build).
    // Every parameter is one axis or chain state the wire step carries through; the count is
    // the typestate itself, not incidental nesting an alias could hide.
    #[allow(clippy::type_complexity)]
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> ReplyWiringChain<
        UnwiredReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC>,
        <Policy as ReplyAttach<<R as ReplyRoute<A::Family>>::Wire>>::Wiring,
    >
    where
        A: Axis,
        R: ReplyRoute<A::Family>,
        Policy: ReplyAttach<R::Wire>,
    {
        ReplyWiringChain {
            chain: self.map_def(|def| ReplyValue {
                value: def.value,
                dest: def.dest,
                attach: (),
            }),
            wiring: policy.into_wiring(),
        }
    }
}

/// The reply chain with its wiring lifted out: what [`publisher`](SubscriberBuilder::publisher)
/// keeps hold of while the wiring steps run.
pub type UnwiredReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC> =
    ReplyChain<HandleValue<A, R, O, C, H, Doc>, Dest, (), Src, State, DC>;

/// The reply wiring under construction: what `.publisher(..)` opens on the manual path.
///
/// [`codec`](Self::codec), [`transform`](Self::transform),
/// [`batch_transform`](Self::batch_transform) and [`transactional`](Self::transactional) fill the
/// wiring's slots - each once, so naming one twice does not compile - and [`build`](Self::build)
/// folds it back into the definition and seals it for
/// [`include`](crate::runtime::Router::include).
///
/// Every other step of the chain reaches through the wiring unchanged: the value steps
/// ([`describe`](Self::describe), [`undocumented`](Self::undocumented)) and the declarative
/// settings ([`workers`](crate::runtime::SubscriberSettings::workers),
/// [`batch`](crate::runtime::SubscriberSettings::batch),
/// [`on_failure`](crate::runtime::SubscriberSettings::on_failure), ...) apply on either side of
/// `.publisher(..)` and produce the same definition, each still fixed once. Opening the wiring
/// therefore closes nothing: the chain has no order to remember.
#[must_use = "the reply wiring seals with .build()"]
pub struct ReplyWiringChain<Chain, W> {
    chain: Chain,
    wiring: W,
}

impl<Chain, W> fmt::Debug for ReplyWiringChain<Chain, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyWiringChain").finish_non_exhaustive()
    }
}

impl<Chain, W> ReplyWiringChain<Chain, W> {
    /// Rebuilds the wrapper over a stepped chain, keeping the wiring: how every forwarded step
    /// reaches the definition underneath.
    fn map_chain<NewChain>(
        self,
        f: impl FnOnce(Chain) -> NewChain,
    ) -> ReplyWiringChain<NewChain, W> {
        ReplyWiringChain {
            chain: f(self.chain),
            wiring: self.wiring,
        }
    }
}

// A wiring chain is not mountable until `.build()` seals it, so it carries the same diagnostic
// form token an unsealed definition does - and, through the `Declared` blanket over it, the whole
// settings surface, forwarded step by step below.
impl<Chain, W> crate::runtime::router::IncludeDef for ReplyWiringChain<Chain, W> {
    type Form = UnbuiltDefinition;
}

/// Implements one settings step on the wiring chain by forwarding it to the chain underneath.
/// The step's own typestate travels with it, so a setting is still fixed exactly once whichever
/// side of `.publisher(..)` names it.
macro_rules! forward_settings_step {
    ($($trait:ident<$($param:ident),*>::$method:ident($($arg:ident: $ty:ty),*)),+ $(,)?) => {$(
        impl<Chain: $trait<$($param),*>, W $(, $param)*> $trait<$($param),*>
            for ReplyWiringChain<Chain, W>
        {
            type Out = ReplyWiringChain<<Chain as $trait<$($param),*>>::Out, W>;

            fn $method(self $(, $arg: $ty)*) -> Self::Out {
                self.map_chain(|chain| chain.$method($($arg),*))
            }
        }
    )+};
}

forward_settings_step! {
    NameStep<>::apply_name(name: Cow<'static, str>),
    WorkersStep<>::apply_workers(workers: Workers),
    FailureStep<>::apply_failures(policies: FailurePolicies),
    StartAtStep<P>::apply_start_at(position: P),
    BatchStep<>::apply_batch(size: NonZeroUsize),
    MapSourceStep<F>::apply_map_source(f: F),
}

impl<Chain, W> ReplyWiringChain<Chain, W> {
    /// Encodes the reply with `codec` instead of the
    /// [`DefaultCodec`](crate::codec::DefaultCodec). Named once per registration.
    pub fn codec<Cd>(self, codec: Cd) -> ReplyWiringChain<Chain, W::Out>
    where
        W: NameReplyCodec<Cd, Slot: CodecSlotOpen>,
    {
        ReplyWiringChain {
            chain: self.chain,
            wiring: self.wiring.name_codec(codec),
        }
    }

    /// Composes a static [`PublishTransform`](crate::runtime::PublishTransform) onto every reply
    /// of this registration. The first one added runs first (closest to the encoded value).
    pub fn transform<N>(self, transform: N) -> ReplyWiringChain<Chain, W::Out>
    where
        W: AddReplyTransform<N>,
    {
        ReplyWiringChain {
            chain: self.chain,
            wiring: self.wiring.add_transform(transform),
        }
    }

    /// Composes a [`BatchPublishTransform`](crate::runtime::BatchPublishTransform) onto every
    /// reply of a page, after the per-message stack.
    pub fn batch_transform<N>(self, transform: N) -> ReplyWiringChain<Chain, W::Out>
    where
        W: AddBatchReplyTransform<N>,
    {
        ReplyWiringChain {
            chain: self.chain,
            wiring: self.wiring.add_batch_transform(transform),
        }
    }

    /// Publishes a page's replies inside one broker transaction: they all become visible
    /// atomically on commit, or none of them do. The policy's live publisher has to be a
    /// [`TransactionalPublisher`](crate::TransactionalPublisher), which the mount checks against
    /// its own broker.
    pub fn transactional(self) -> ReplyWiringChain<Chain, W::Out>
    where
        W: TransactionalReply<State: PublishingDirectly>,
    {
        ReplyWiringChain {
            chain: self.chain,
            wiring: self.wiring.into_transactional(),
        }
    }
}

// The value steps reach through the wiring the same way the settings do, so `.describe(..)`
// and `.undocumented()` read the same on either side of `.publisher(..)`.
impl<A, R, O, C, H, Doc, Dest, Src, State, DC, W>
    ReplyWiringChain<UnwiredReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC>, W>
{
    /// See [`describe`](SubscriberBuilder::describe) on the reply chain.
    pub fn describe(self, text: impl Into<Cow<'static, str>>) -> Self {
        self.map_chain(|chain| chain.describe(text))
    }

    /// See [`undocumented`](SubscriberBuilder::undocumented) on the reply chain.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the axes
    pub fn undocumented(
        self,
    ) -> ReplyWiringChain<UnwiredReplyChain<A, R, O, C, H, Undocumented, Dest, Src, State, DC>, W>
    where
        Doc: IsDocumented,
    {
        // The method path is ambiguous here (the plain and the reply chain both define
        // `undocumented`), so the closure is what picks the reply chain's one.
        #[allow(clippy::redundant_closure_for_method_calls)]
        self.map_chain(|chain| chain.undocumented())
    }
}

impl<V, Dest, Src, State, DC, W> ReplyWiringChain<ReplyChain<V, Dest, (), Src, State, DC>, W> {
    /// Folds the wiring back into the definition and seals it for `include`.
    #[must_use]
    pub fn build(self) -> SealedChain<ReplyValue<V, Dest, WithSource<W>>, Src, State, DC> {
        let wiring = self.wiring;
        self.chain.map_def(|def| {
            Sealed(ReplyValue {
                value: def.value,
                dest: def.dest,
                attach: WithSource::new(wiring),
            })
        })
    }
}

/// The sealed reply chain with its wiring lifted out: what
/// [`publisher`](SealedReplyChain::publisher) keeps hold of on the attribute path.
pub type UnwiredSealedReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC> =
    SealedReplyChain<A, R, O, C, H, Doc, Dest, (), Src, State, DC>;

impl<A, R, O, C, H, Doc, Dest, Attach: DefaultReplyAttach, Src, State, DC>
    SealedReplyChain<A, R, O, C, H, Doc, Dest, Attach, Src, State, DC>
{
    /// See [`publisher`](SubscriberBuilder::publisher) on the reply chain: the same step on the
    /// attribute path, whose definition arrives sealed by the expansion.
    ///
    /// The two paths meet here. A `#[subscriber(publish("..."))]` definition mounted bare takes
    /// its reply policy from the include-site chain; one that names any setting - a page body's
    /// mandatory [`batch`](crate::runtime::SubscriberSettings::batch) among them - is a settings
    /// builder over the sealed definition, and this is where its policy attaches. The wiring
    /// grows exactly as it does on the value chain and seals the same way, with
    /// [`build`](ReplyWiringChain::build).
    // Every parameter is one axis or chain state the wiring step carries through; the count is
    // the typestate itself, not incidental nesting an alias could hide.
    #[allow(clippy::type_complexity)]
    pub fn publisher<Policy>(
        self,
        policy: Policy,
    ) -> ReplyWiringChain<
        UnwiredSealedReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC>,
        <Policy as ReplyAttach<<R as ReplyRoute<A::Family>>::Wire>>::Wiring,
    >
    where
        A: Axis,
        R: ReplyRoute<A::Family>,
        Policy: ReplyAttach<R::Wire>,
    {
        ReplyWiringChain {
            chain: self.map_def(|Sealed(def)| {
                Sealed(ReplyValue {
                    value: def.value,
                    dest: def.dest,
                    attach: (),
                })
            }),
            wiring: policy.into_wiring(),
        }
    }
}

impl<A, R, O, C, H, Doc, Dest, Src, State, DC, W>
    ReplyWiringChain<UnwiredSealedReplyChain<A, R, O, C, H, Doc, Dest, Src, State, DC>, W>
{
    /// Folds the wiring back under the seal the expansion already applied.
    #[allow(clippy::type_complexity)] // the chain's own state; an alias would hide the axes
    #[must_use]
    pub fn build(
        self,
    ) -> SealedReplyChain<A, R, O, C, H, Doc, Dest, WithSource<W>, Src, State, DC> {
        let wiring = self.wiring;
        self.chain.map_def(|Sealed(def)| {
            Sealed(ReplyValue {
                value: def.value,
                dest: def.dest,
                attach: WithSource::new(wiring),
            })
        })
    }
}

// The inner-value steps stay reachable after `.reply()`: `.describe(..)` and `.undocumented()`
// reach through the wrapper, so the chain order is free.
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

    /// Seals the definition for `include`.
    // Every parameter is one axis or chain state the seal carries through; the count is the
    // typestate itself, not incidental nesting an alias could hide.
    #[allow(clippy::type_complexity)]
    #[must_use]
    pub fn build(self) -> SealedReplyChain<A, R, O, C, H, Doc, Dest, Attach, Src, State, DC> {
        self.map_def(Sealed)
    }
}
