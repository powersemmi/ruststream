//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions.
//!
//! `include` is one entry point for every single-message definition form and `include_batch` for
//! both batch forms; which machinery runs is picked by the definition's form token
//! ([`IncludeDef::Form`]), so `b.include(handle)`, `b.include(respond).publisher(..)` and
//! `b.include(forward).publisher(..)` all read the same. Publisher-producing forms return a
//! registration builder that commits when the statement ends; `.publisher(..)` attaches the
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target).

use std::marker::PhantomData;

use serde::Serialize;

use crate::codec::Codec;
// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
// The default-reply commits build a `TypedPublisher` over the broker's default policy, so those
// imports are gated with the default codec they require.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::Publisher;
use crate::{
    BatchSubscriber, Broker, BuildContext, Connected, DefaultPublish, PublishPolicy, Subscriber,
    SubscriptionSource,
};

use crate::runtime::SliceHandler;
use crate::runtime::batch::BatchDef;
use crate::runtime::batch_inject::{BatchInjectCall, BatchInjectDef};
use crate::runtime::batch_publishing::BatchPublishingCall;
use crate::runtime::handler::Handler;
use crate::runtime::inject::{FromStartup, InjectCall, InjectDef, InjectHandler};
use crate::runtime::input::{DecodeWith, InputKind, RawBytes};
use crate::runtime::middleware::Layer;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::TypedPublisher;
use crate::runtime::publish::{PublishPipeline, ReplyPublisher};
use crate::runtime::publishing::{PublishingCall, PublishingHandler, ReplySink};
use crate::runtime::subscriber_def::SubscriberDef;
use crate::runtime::typed::Typed;

use super::scope::BrokerScope;

/// Ties a definition type to its form token.
///
/// One `include` entry point then dispatches to the right mounting machinery at compile time.
/// Implemented by the `#[subscriber]` macro; a hand-written definition adds it next to its def
/// trait impl.
pub trait IncludeDef {
    /// The form token: one of the markers in [`forms`].
    type Form;
}

/// Form tokens for [`IncludeDef`]: which mounting machinery a definition uses.
pub mod forms {
    /// A plain subscriber (`#[subscriber("in")]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Subscribing;
    /// A raw-bytes subscriber (`#[subscriber("in", raw)]`): no decode, no codec.
    #[derive(Debug, Clone, Copy)]
    pub struct RawSubscribing;
    /// A byte-reply subscriber (`#[subscriber("in", publish_raw("out"))]`, with or without
    /// `raw` on the input side): the reply bytes go out as-is through a bare publisher.
    #[derive(Debug, Clone, Copy)]
    pub struct RawReply;
    /// A reply-publishing subscriber (`#[subscriber("in", publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Publishing;
    /// A subscriber whose startup injections need a publisher attachment.
    ///
    /// The signature carries an `Out(out): Out<P>` parameter (optionally next to a `Seek`
    /// one), so the include site must chain `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct Out;
    /// A subscriber whose startup injections need nothing from the include site.
    ///
    /// The signature carries a `Seek(seeker): Seek<K>` parameter (and no `Out`).
    #[derive(Debug, Clone, Copy)]
    pub struct Seek;
    /// A reply-publishing subscriber whose handler also takes an `Out` parameter, so the
    /// include site must chain `.out(..)` next to the (optional) `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct PublishingOut;
    /// A byte-reply subscriber whose handler also takes an `Out` parameter.
    #[derive(Debug, Clone, Copy)]
    pub struct RawReplyOut;
    /// A batch subscriber (`#[subscriber(batch("in"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Batch;
    /// A batch reply-publishing subscriber (`#[subscriber(batch("in"), publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishing;
    /// A batch subscriber whose startup injections need a publisher attachment (an `Out`
    /// parameter, optionally next to a `Seek` one).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchOut;
    /// A batch subscriber whose startup injections need nothing from the include site (a
    /// `Seek` parameter and no `Out`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchSeek;
    /// A batch reply-publishing subscriber whose handler also takes an `Out` parameter, so
    /// the include site must chain `.out(..)` next to the (optional) `.publisher(..)`.
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishingOut;
}

/// Form-token dispatch for [`BrokerScope::include`]: implemented by the tokens in [`forms`],
/// generic over the definition and the scope. Machinery; you never implement or name it.
#[doc(hidden)]
pub trait IncludeMount<'s, B: Broker, Layers, C, State, Pipeline, Def> {
    /// What `include` hands back: `()` for eager forms, a registration builder for the
    /// publisher-producing ones.
    type Out;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out;
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a single-message `#[subscriber]` definition: a plain handler mounts eagerly, a
    /// `publish("dest")` or `Out`-taking handler returns a registration builder that commits
    /// at the end of the statement; chain [`publisher`](IncludePublishing::publisher) on it to
    /// attach the publish policy.
    ///
    /// Decoding uses the scope codec when one was set
    /// ([`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)), else the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    pub fn include<'s, Def>(
        &'s mut self,
        def: Def,
    ) -> <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, Def>,
    {
        <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::begin(def, self)
    }

    /// Mounts a batch `#[subscriber(batch(..))]` definition; the `publish("dest")` form returns
    /// a registration builder, exactly like [`include`](Self::include).
    pub fn include_batch<'s, Def>(
        &'s mut self,
        def: Def,
    ) -> <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::Out
    where
        Def: IncludeDef,
        Def::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, Def>,
    {
        <Def::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, Def>>::begin(def, self)
    }
}

/// The codec a scope decodes with: the scope's own codec when one was set, else the default.
/// Machinery behind `include`; the two impls mirror the two `with_broker` forms.
#[doc(hidden)]
pub trait ScopeCodec {
    type Codec: Codec + Clone + Send + Sync + 'static;
    fn scope_codec(&self) -> Self::Codec;
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl ScopeCodec for () {
    type Codec = DefaultCodec;
    fn scope_codec(&self) -> Self::Codec {
        DefaultCodec::default()
    }
}

impl<C: Codec + Clone + Send + Sync + 'static> ScopeCodec for C {
    type Codec = C;
    fn scope_codec(&self) -> Self::Codec {
        self.clone()
    }
}

// ---------------------------------------------------------------------------------------------
// Plain subscribing: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Subscribing
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: SubscriberDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Input,
            C::Codec,
            Def::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.scope_codec();
        scope.mount_subscriber(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Raw subscribing: eager, no builder, and no codec anywhere - the byte input kind decodes with
// `()`, so the scope codec parameter `C` is left unconstrained and the mount works without any
// codec feature.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: SubscriberDef<Input = RawBytes>,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            RawBytes,
            (),
            Def::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_subscriber(source, def, ());
    }
}

// ---------------------------------------------------------------------------------------------
// Attachment-free injections: eager, no builder - a definition whose startup injections need
// nothing from the include site (a Seek parameter without an Out one) resolves against the
// subscription itself.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Seek
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: InjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ()>
        + Send
        + Sync
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<InjectHandler<Def, <C as ScopeCodec>::Codec>> + Clone + Send + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_inject(source, def, ());
    }
}

// ---------------------------------------------------------------------------------------------
// Plain batch: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Batch
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.scope_codec();
        scope.mount_batch(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Builder-producing forms: reply publishing, out injection, and their batch counterparts.
//
// The builder commits on Drop, so `b.include(def)` alone still registers (with the broker's
// default publish policy where one exists), while `b.include(def).publisher(src)` replaces the
// commit with the attached source. User sources are wrapped in `WithSource` so the default
// marker and the source-driven commit live on different type constructors (disjoint impls, no
// negative reasoning needed).
//
// Every form family shares one commit trait, keyed by a mount token: strategies of different
// families are impls on the same attachment types with different concrete tokens, so they
// never overlap without negative reasoning. Two generic builders then serve every family -
// [`IncludeWith`] (one attachment, replaced by `.publisher(..)`) and [`IncludeWithOut`] (a
// reply attachment plus the `Out` parameter's own `.out(..)`) - and the per-form names are
// aliases picking the token and the initial attachment.

/// The default reply commit: the broker's default publish policy under the default codec.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultReply;

/// The default commit of the byte-reply form: the broker's plain publish policy taken bare.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultBareReply;

/// A user-attached source, wrapped so its commit impl cannot overlap the default marker's.
#[doc(hidden)]
#[derive(Debug)]
pub struct WithSource<Source>(Source);

/// The "no source yet" marker of an `Out` attachment. Committing with it is a wiring bug.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingOut;

/// The mount tokens keying [`CommitVia`]: which mount a committed attachment drives.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct InjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct PublishInjectMount;
/// See [`PublishMount`].
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct BatchPublishInjectMount;

/// One commit strategy of a registration builder, keyed by its `Mount` token. Machinery;
/// never named directly.
#[doc(hidden)]
pub trait CommitVia<Mount, B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

/// Committing an `Out` attachment that was never provided: the source is required at the
/// include site, and the miss fires at application build time (the same moment as the
/// `on_startup` ordering assert), never mid-run.
impl<Mount, B, Layers, C, State, Pipeline, Def> CommitVia<Mount, B, Layers, C, State, Pipeline, Def>
    for MissingOut
where
    B: Broker + 'static,
{
    fn commit(self, _def: Def, _scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        panic!(
            "an Out handler was included without a publisher source: chain \
             .publisher(<policy or bound token>) on the include call"
        );
    }
}

/// The two-attachment counterpart: the reply side may be attached or defaulted, but the `Out`
/// side was never provided.
impl<Mount, B, Layers, C, State, Pipeline, Def, Reply>
    CommitVia<Mount, B, Layers, C, State, Pipeline, Def> for (Reply, MissingOut)
where
    B: Broker + 'static,
{
    fn commit(self, _def: Def, _scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        panic!(
            "a publishing handler with an Out parameter was included without its publisher \
             source: chain .out(<policy or bound token>) on the include call"
        );
    }
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def> CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply: the broker's plain publish policy under the default codec,
        // committed as if the user had chained `.publisher(TypedPublisher::new(<policy>))`.
        CommitVia::commit(
            WithSource(TypedPublisher::new(
                <B::Connected as DefaultPublish>::Policy::default(),
            )),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def> CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>
    for DefaultBareReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        CommitVia::commit(
            WithSource(<B::Connected as DefaultPublish>::Policy::default()),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: PublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ()>
        + Send
        + Sync
        + 'static,
    Def::Reply: Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>> + Send + 'static,
    Source::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<PublishingHandler<Def, <C as ScopeCodec>::Codec, Source::Live, Pipeline>>
        + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_publishing_source(source, def, self.0, ());
    }
}

/// A registration builder over one attachment, generic over the [`CommitVia`] mount token.
///
/// Commits when dropped (the end of the `b.include(..)` / `b.include_batch(..)` statement).
/// [`publisher`](Self::publisher) replaces the attachment: the reply source on the publishing
/// forms (defaulted when the call is omitted), the `Out` parameter's source on the out forms
/// (required; committing without it panics at application build time, naming the fix). The
/// per-form names are aliases: [`IncludePublishing`], [`IncludeOut`],
/// [`IncludeBatchPublishing`], [`IncludeBatchOut`].
pub struct IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Attachment)>,
    _mount: PhantomData<Mount>,
}

/// The builder [`BrokerScope::include`] returns for a `publish("dest")` definition: the
/// attachment is the reply source, defaulting to the broker's default publish policy under
/// the default codec.
pub type IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, PublishMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include`] returns for a handler with an
/// [`Out`](crate::runtime::Out) parameter: the attachment is the parameter's publish policy,
/// with no default.
pub type IncludeOut<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, InjectMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include_batch`] returns for a `batch(.., publish("dest"))`
/// definition.
///
/// The attachment is the batch reply source: a typed stack, or its
/// [`transactional`](TypedPublisher::transactional) form for one transaction per batch.
pub type IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, BatchPublishMount, B, Layers, C, State, Pipeline, Def, Source>;

/// The builder [`BrokerScope::include_batch`] returns for a batch handler with an
/// [`Out`](crate::runtime::Out) parameter.
pub type IncludeBatchOut<'s, B, Layers, C, State, Pipeline, Def, Source> =
    IncludeWith<'s, BatchInjectMount, B, Layers, C, State, Pipeline, Def, Source>;

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
    IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the form's publisher source: for a publishing form the reply source (a
    /// [`TypedPublisher`] stack naming the reply codec and transforms, a bare policy on the
    /// byte-reply form), for an out form the [`Out`](crate::runtime::Out) parameter's publish
    /// policy - either way also a [`Bound`](crate::runtime::Bound) token wrapping one for a
    /// cross-broker target. The runtime pairs it after the brokers connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeWith<'s, Mount, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
            _mount: PhantomData,
        }
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Attachment> std::fmt::Debug
    for IncludeWith<'_, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeWith").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Attachment> Drop
    for IncludeWith<'_, Mount, B, Layers, C, State, Pipeline, Def, Attachment>
where
    B: Broker + 'static,
    Attachment: CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}

/// A registration builder with two attachments, for a publishing handler that also takes an
/// [`Out`](crate::runtime::Out) parameter.
///
/// Commits when dropped. The reply side defaults like [`IncludeWith`] (override with
/// [`publisher`](Self::publisher)); the out side has no default, so committing without an
/// [`out`](Self::out) call panics at application build time, naming the fix. The per-form
/// names are aliases: [`IncludePublishingOut`], [`IncludeBatchPublishingOut`].
pub struct IncludeWithOut<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource>
where
    B: Broker + 'static,
    (Reply, OutSource): CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Reply, OutSource)>,
    _mount: PhantomData<Mount>,
}

/// The builder [`BrokerScope::include`] returns for a `publish("dest")` /
/// `publish_raw("dest")` definition whose handler also takes an
/// [`Out`](crate::runtime::Out) parameter.
pub type IncludePublishingOut<'s, B, Layers, C, State, Pipeline, Def, Reply, OutSource> =
    IncludeWithOut<'s, PublishInjectMount, B, Layers, C, State, Pipeline, Def, Reply, OutSource>;

/// The builder [`BrokerScope::include_batch`] returns for a `batch(.., publish("dest"))`
/// definition whose handler also takes an [`Out`](crate::runtime::Out) parameter.
pub type IncludeBatchPublishingOut<'s, B, Layers, C, State, Pipeline, Def, Reply, OutSource> =
    IncludeWithOut<
        's,
        BatchPublishInjectMount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        Reply,
        OutSource,
    >;

impl<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource>
    IncludeWithOut<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource>
where
    B: Broker + 'static,
    (Reply, OutSource): CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the reply source, like [`IncludeWith::publisher`].
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeWithOut<
        's,
        Mount,
        B,
        Layers,
        C,
        State,
        Pipeline,
        Def,
        WithSource<NewSource>,
        OutSource,
    >
    where
        (WithSource<NewSource>, OutSource): CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _default, out) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeWithOut {
            scope: Some(scope),
            parts: Some((def, WithSource(source), out)),
            _mount: PhantomData,
        }
    }

    /// Attaches the source the handler's [`Out`](crate::runtime::Out) parameter pairs from:
    /// the scope broker's publish policy, or a [`Bound`](crate::runtime::Bound) token for a
    /// different registered broker.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn out<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeWithOut<'s, Mount, B, Layers, C, State, Pipeline, Def, Reply, WithSource<NewSource>>
    where
        (Reply, WithSource<NewSource>): CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
    {
        let (def, reply, _missing) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeWithOut {
            scope: Some(scope),
            parts: Some((def, reply, WithSource(source))),
            _mount: PhantomData,
        }
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource> std::fmt::Debug
    for IncludeWithOut<'_, Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource>
where
    B: Broker + 'static,
    (Reply, OutSource): CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeWithOut").finish_non_exhaustive()
    }
}

impl<Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource> Drop
    for IncludeWithOut<'_, Mount, B, Layers, C, State, Pipeline, Def, Reply, OutSource>
where
    B: Broker + 'static,
    (Reply, OutSource): CommitVia<Mount, B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, reply, out)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            (reply, out).commit(def, scope);
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Publishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
            _mount: PhantomData,
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawReply
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultBareReply: CommitVia<PublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultBareReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, DefaultBareReply)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Reply publishing with an Out parameter: two attachment axes on one builder. The reply side
// keeps its default commits (typed or bare policy); the out side has no default, exactly like
// the plain Out form, so committing without `.out(..)` is a build-time panic.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def, OutSource>
    CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (DefaultReply, WithSource<OutSource>)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
        WithSource<OutSource>,
    ): CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply, as if the user had chained `.publisher(..)` themselves.
        CommitVia::commit(
            (
                WithSource(TypedPublisher::new(
                    <B::Connected as DefaultPublish>::Policy::default(),
                )),
                self.1,
            ),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, OutSource>
    CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (DefaultBareReply, WithSource<OutSource>)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<<B::Connected as DefaultPublish>::Policy>,
        WithSource<OutSource>,
    ): CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        CommitVia::commit(
            (
                WithSource(<B::Connected as DefaultPublish>::Policy::default()),
                self.1,
            ),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, OutSource>
    CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (WithSource<Source>, WithSource<OutSource>)
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: PublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, OutSource>
        + Send
        + Sync
        + 'static,
    Def::Reply: Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>> + Send + 'static,
    Source::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
    OutSource: Send + Sync + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<PublishingHandler<Def, <C as ScopeCodec>::Codec, Source::Live, Pipeline>>
        + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_publishing_source(source, def, self.0.0, self.1.0);
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::PublishingOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    (DefaultReply, MissingOut): CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out =
        IncludePublishingOut<'s, B, Layers, C, State, Pipeline, Def, DefaultReply, MissingOut>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWithOut {
            scope: Some(scope),
            parts: Some((def, DefaultReply, MissingOut)),
            _mount: PhantomData,
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawReplyOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    (DefaultBareReply, MissingOut):
        CommitVia<PublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out =
        IncludePublishingOut<'s, B, Layers, C, State, Pipeline, Def, DefaultBareReply, MissingOut>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWithOut {
            scope: Some(scope),
            parts: Some((def, DefaultBareReply, MissingOut)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Out injection: no default source; committing without one is a build-time panic (the blanket
// `MissingOut` strategy above).

impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<InjectMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: InjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, Source>
        + Send
        + Sync
        + 'static,
    Source: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<InjectHandler<Def, <C as ScopeCodec>::Codec>> + Clone + Send + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_inject(source, def, self.0);
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::Out
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: InjectDef,
{
    type Out = IncludeOut<'s, B, Layers, C, State, Pipeline, Def, MissingOut>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, MissingOut)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Batch injections: the batch counterparts of the Seek (eager) and Out (builder) forms.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchSeek
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchInjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ()>
        + Send
        + Sync
        + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_inject(source, def, ());
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source>
    CommitVia<BatchInjectMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchInjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, Source>
        + Send
        + Sync
        + 'static,
    Source: Send + Sync + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_inject(source, def, self.0);
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    Def: BatchInjectDef,
{
    type Out = IncludeBatchOut<'s, B, Layers, C, State, Pipeline, Def, MissingOut>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, MissingOut)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing: the same builder shape; the reply source pairs into a ReplyPublisher.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ()>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_batch_publishing_source(source, def, reply, ());
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply>
    CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, ()>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, self.0, ());
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchPublishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitVia<BatchPublishMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWith {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
            _mount: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing with an Out parameter: the two-attachment builder at the batch shape.

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def, OutSource>
    CommitVia<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (DefaultReply, WithSource<OutSource>)
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    (
        WithSource<TypedPublisher<<B::Connected as DefaultPublish>::Policy, DefaultCodec>>,
        WithSource<OutSource>,
    ): CommitVia<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The typed default reply, as if the user had chained `.publisher(..)` themselves.
        CommitVia::commit(
            (
                WithSource(TypedPublisher::new(
                    <B::Connected as DefaultPublish>::Policy::default(),
                )),
                self.1,
            ),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply, OutSource>
    CommitVia<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>
    for (WithSource<Source>, WithSource<OutSource>)
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber:
        BatchSubscriber + Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
    Def::Injections: FromStartup<B, <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber, OutSource>
        + Send
        + Sync
        + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
    OutSource: Send + Sync + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, self.0.0, self.1.0);
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::BatchPublishingOut
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    (DefaultReply, MissingOut):
        CommitVia<BatchPublishInjectMount, B, Layers, C, State, Pipeline, Def>,
{
    type Out =
        IncludeBatchPublishingOut<'s, B, Layers, C, State, Pipeline, Def, DefaultReply, MissingOut>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeWithOut {
            scope: Some(scope),
            parts: Some((def, DefaultReply, MissingOut)),
            _mount: PhantomData,
        }
    }
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a plain `#[subscriber]` definition on an explicit subscription `source`
    /// (overriding the macro's own source), decoding with the scope codec (or the default).
    pub fn include_on<Source, Def>(&mut self, source: Source, def: Def)
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        <Source::Subscriber as Subscriber>::Message: 'static,
        C: ScopeCodec,
        Def: SubscriberDef,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Handler: 'static,
        Def::Context: BuildContext<<Source::Subscriber as Subscriber>::Message> + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<
            Typed<<Source::Subscriber as Subscriber>::Message, Def::Input, C::Codec, Def::Handler>,
        >,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
    {
        let codec = self.codec.scope_codec();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber(batch(..))]` definition on an explicit subscription `source`,
    /// decoding each element with the scope codec (or the default).
    pub fn include_batch_on<Source, Def>(&mut self, source: Source, def: Def)
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        C: ScopeCodec,
        Def: BatchDef,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Handler: SliceHandler<<Def::Input as InputKind>::Owned, State> + 'static,
        State: Send + Sync + 'static,
    {
        let codec = self.codec.scope_codec();
        self.mount_batch(source, def, codec);
    }
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a `publish("dest")` definition on an explicit subscription `source`, replying
    /// through `publisher` (a typed policy stack, or a [`Bound`](crate::runtime::Bound) token
    /// wrapping one). The positional counterpart of `include(def).publisher(..)` for the
    /// source-override case.
    pub fn include_publishing_on<Source, Def, ReplySource>(
        &mut self,
        source: Source,
        def: Def,
        publisher: ReplySource,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        C: ScopeCodec,
        Def: PublishingCall<State> + 'static,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, ()> + Send + Sync + 'static,
        Def::Reply: Send + Sync + 'static,
        Def::Context:
            BuildContext<<Source::Subscriber as Subscriber>::Message> + Send + Sync + 'static,
        ReplySource: PublishPolicy<Connected<B>> + Send + 'static,
        ReplySource::Live: ReplySink<Def::Reply, Def::Context, Pipeline> + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<PublishingHandler<Def, C::Codec, ReplySource::Live, Pipeline>>
            + Clone
            + Send
            + 'static,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
    {
        self.mount_publishing_source(source, def, publisher, ());
    }

    /// Mounts a `batch(.., publish("dest"))` definition on an explicit subscription `source`,
    /// replying through `publisher` (a typed policy stack, its transactional form, or a
    /// [`Bound`](crate::runtime::Bound) token wrapping either).
    pub fn include_batch_publishing_on<Source, Def, ReplySource, BatchReply>(
        &mut self,
        source: Source,
        def: Def,
        publisher: ReplySource,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Sync + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + 'static,
        C: ScopeCodec,
        Def: BatchPublishingCall<State> + 'static,
        Def::Input: DecodeWith<<C as ScopeCodec>::Codec>,
        Def::Injections: FromStartup<B, Source::Subscriber, ()> + Send + Sync + 'static,
        Def::Reply: Serialize + Send + Sync + 'static,
        ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
        BatchReply: ReplyPublisher + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
    {
        self.mount_batch_publishing_source(source, def, publisher, ());
    }
}
