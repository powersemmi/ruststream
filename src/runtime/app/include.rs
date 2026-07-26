//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions.
//!
//! `include` is one entry point for every single-message definition form and `include_batch` for
//! both batch forms; which machinery runs is picked by the definition's form token
//! ([`IncludeDef::Form`]), so `b.include(handle)`, `b.include(respond).publisher(..)` and
//! `b.include(forward).publisher(..)` all read the same. Publisher-producing forms return a
//! registration builder that commits when the statement ends; `.publisher(..)` attaches the
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target).

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
// `DefaultPublish` powers only the default-reply commits, which need a default codec.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::DefaultPublish;
use crate::{
    BatchSubscriber, Broker, Connected, PublishPolicy, Publisher, Subscriber, SubscriptionSource,
};

use crate::runtime::batch::BatchDef;
use crate::runtime::batch_publishing::BatchPublishingCall;
use crate::runtime::egress::{EgressCall, EgressDef};
use crate::runtime::handler::Handler;
use crate::runtime::middleware::Layer;
use crate::runtime::publish::{PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingCall, PublishingHandler};
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
    /// A reply-publishing subscriber (`#[subscriber("in", publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Publishing;
    /// A subscriber with an injected publisher (`Egress(out): Egress<P>`).
    #[derive(Debug, Clone, Copy)]
    pub struct Egress;
    /// A batch subscriber (`#[subscriber(batch("in"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct Batch;
    /// A batch reply-publishing subscriber (`#[subscriber(batch("in"), publish("out"))]`).
    #[derive(Debug, Clone, Copy)]
    pub struct BatchPublishing;
}

/// Form-token dispatch for [`BrokerScope::include`]: implemented by the tokens in [`forms`],
/// generic over the definition and the scope. Machinery; you never implement or name it.
#[doc(hidden)]
pub trait IncludeMount<'s, B: Broker, Layers, C, State, Pipeline, D> {
    /// What `include` hands back: `()` for eager forms, a registration builder for the
    /// publisher-producing ones.
    type Out;

    fn begin(def: D, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out;
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a single-message `#[subscriber]` definition: a plain handler mounts eagerly, a
    /// `publish("dest")` or `Egress`-taking handler returns a registration builder that commits
    /// at the end of the statement; chain [`publisher`](IncludePublishing::publisher) on it to
    /// attach the publish policy.
    ///
    /// Decoding uses the scope codec when one was set
    /// ([`with_broker_codec`](crate::runtime::RustStream::with_broker_codec)), else the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    pub fn include<'s, D>(
        &'s mut self,
        def: D,
    ) -> <D::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, D>>::Out
    where
        D: IncludeDef,
        D::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, D>,
    {
        <D::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, D>>::begin(def, self)
    }

    /// Mounts a batch `#[subscriber(batch(..))]` definition; the `publish("dest")` form returns
    /// a registration builder, exactly like [`include`](Self::include).
    pub fn include_batch<'s, D>(
        &'s mut self,
        def: D,
    ) -> <D::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, D>>::Out
    where
        D: IncludeDef,
        D::Form: IncludeMount<'s, B, Layers, C, State, Pipeline, D>,
    {
        <D::Form as IncludeMount<'s, B, Layers, C, State, Pipeline, D>>::begin(def, self)
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
    type Codec = crate::codec::DefaultCodec;
    fn scope_codec(&self) -> Self::Codec {
        crate::codec::DefaultCodec::default()
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

impl<'s, B, Layers, C, State, Pipeline, D> IncludeMount<'s, B, Layers, C, State, Pipeline, D>
    for forms::Subscribing
where
    B: Broker + 'static,
    C: ScopeCodec,
    D: SubscriberDef,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Handler: 'static,
    D::Context: crate::BuildContext<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
        Typed<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            D::Input,
            C::Codec,
            D::Handler,
        >,
    >,
    Layers::Handler: Handler<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            D::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: D, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.scope_codec();
        scope.mount_subscriber(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Plain batch: eager, no builder.

impl<'s, B, Layers, C, State, Pipeline, D> IncludeMount<'s, B, Layers, C, State, Pipeline, D>
    for forms::Batch
where
    B: Broker + 'static,
    C: ScopeCodec,
    D: BatchDef,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Handler: crate::runtime::SliceHandler<D::Input, State> + 'static,
    State: Send + Sync + 'static,
{
    type Out = ();

    fn begin(def: D, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let codec = scope.codec.scope_codec();
        scope.mount_batch(source, def, codec);
    }
}

// ---------------------------------------------------------------------------------------------
// Reply publishing, egress and batch publishing: forms that return a registration builder.
//
// The builder commits on Drop, so `b.include(def)` alone still registers (with the broker's
// default publish policy where one exists), while `b.include(def).publisher(src)` replaces the
// commit with the attached source. User sources are wrapped in `WithSource` so the default
// marker and the source-driven commit live on different type constructors (disjoint impls, no
// negative reasoning needed).

/// The default reply commit: the broker's default publish policy under the default codec.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultReply;

/// A user-attached source, wrapped so its commit impl cannot overlap the default marker's.
#[doc(hidden)]
#[derive(Debug)]
pub struct WithSource<S>(S);

/// One commit strategy of a publishing registration builder. Machinery; never named directly.
#[doc(hidden)]
pub trait CommitPublishing<B: Broker, Layers, C, State, Pipeline, D>: Sized {
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, D> CommitPublishing<B, Layers, C, State, Pipeline, D>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    C: ScopeCodec,
    D: PublishingCall<State> + 'static,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Reply: Serialize + Send + Sync + 'static,
    D::Context: crate::BuildContext<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
            PublishingHandler<
                D,
                <C as ScopeCodec>::Codec,
                <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live,
                crate::codec::DefaultCodec,
                crate::runtime::PublishTransformIdentity,
                Pipeline,
            >,
        > + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            D::Context,
            State,
        > + 'static,
{
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_publishing_source(source, def, reply);
    }
}

impl<B, Layers, C, State, Pipeline, D, Src, P, PC, PL>
    CommitPublishing<B, Layers, C, State, Pipeline, D> for WithSource<Src>
where
    B: Broker + 'static,
    C: ScopeCodec,
    D: PublishingCall<State> + 'static,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Reply: Serialize + Send + Sync + 'static,
    D::Context: crate::BuildContext<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Src: PublishPolicy<Connected<B>, Live = TypedPublisher<P, PC, PL>> + Send + 'static,
    P: Publisher + 'static,
    PC: Codec + 'static,
    PL: PublishTransform<D::Context> + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<PublishingHandler<D, <C as ScopeCodec>::Codec, P, PC, PL, Pipeline>>
        + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            D::Context,
            State,
        > + 'static,
{
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_publishing_source(source, def, self.0);
    }
}

/// The registration builder [`BrokerScope::include`] returns for a `publish("dest")` definition.
///
/// Commits when dropped (the end of the `b.include(..)` statement). Without a
/// [`publisher`](Self::publisher) call it commits with the broker's default publish policy under
/// the default codec; with one, the attached source is paired by the runtime at startup.
pub struct IncludePublishing<'s, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitPublishing<B, Layers, C, State, Pipeline, D>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(D, Src)>,
}

impl<'s, B, Layers, C, State, Pipeline, D, Src>
    IncludePublishing<'s, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitPublishing<B, Layers, C, State, Pipeline, D>,
{
    /// Attaches the reply source: a [`TypedPublisher`] stack over a publish policy (naming the
    /// reply codec and transforms), or a [`Bound`](crate::runtime::Bound) token wrapping one for
    /// a cross-broker reply. The runtime pairs it after the brokers connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<S2>(
        mut self,
        source: S2,
    ) -> IncludePublishing<'s, B, Layers, C, State, Pipeline, D, WithSource<S2>>
    where
        WithSource<S2>: CommitPublishing<B, Layers, C, State, Pipeline, D>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludePublishing {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
        }
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> std::fmt::Debug
    for IncludePublishing<'_, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitPublishing<B, Layers, C, State, Pipeline, D>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludePublishing").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> Drop
    for IncludePublishing<'_, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitPublishing<B, Layers, C, State, Pipeline, D>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, D> IncludeMount<'s, B, Layers, C, State, Pipeline, D>
    for forms::Publishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitPublishing<B, Layers, C, State, Pipeline, D>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, D, DefaultReply>;

    fn begin(def: D, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludePublishing {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Egress: no default source; committing without one is a build-time panic.

/// The "no source yet" marker of [`IncludeEgress`]. Committing with it is a wiring bug.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingEgress;

/// One commit strategy of an egress registration builder. Machinery; never named directly.
#[doc(hidden)]
pub trait CommitEgress<B: Broker, Layers, C, State, Pipeline, D>: Sized {
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

impl<B, Layers, C, State, Pipeline, D> CommitEgress<B, Layers, C, State, Pipeline, D>
    for MissingEgress
where
    B: Broker + 'static,
{
    fn commit(self, _def: D, _scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // An Egress parameter has no broker-side default; requiring the source at the include
        // site is the point of the injection. This fires at application build time (the same
        // moment as the on_startup ordering assert), never mid-run.
        panic!(
            "an Egress handler was included without a publisher source: chain \
             .publisher(<policy or bound token>) on b.include(..)"
        );
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> CommitEgress<B, Layers, C, State, Pipeline, D>
    for WithSource<Src>
where
    B: Broker + 'static,
    C: ScopeCodec,
    D: EgressCall<State> + 'static,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Context: crate::BuildContext<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    D::Egress: Send + Sync + 'static,
    Src: PublishPolicy<Connected<B>, Live = D::Egress> + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<crate::runtime::EgressHandler<D, <C as ScopeCodec>::Codec, D::Egress>>
        + Clone
        + Send
        + 'static,
    Layers::Handler: Handler<
            <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            D::Context,
            State,
        > + 'static,
{
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_egress_source(source, def, self.0);
    }
}

/// The registration builder [`BrokerScope::include`] returns for a handler with an
/// [`Egress`](crate::runtime::Egress) parameter.
///
/// Commits when dropped. There is no default source: committing without a
/// [`publisher`](Self::publisher) call panics at application build time, naming the fix.
pub struct IncludeEgress<'s, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitEgress<B, Layers, C, State, Pipeline, D>,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(D, Src)>,
}

impl<'s, B, Layers, C, State, Pipeline, D, Src>
    IncludeEgress<'s, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitEgress<B, Layers, C, State, Pipeline, D>,
{
    /// Attaches the source the handler's [`Egress`](crate::runtime::Egress) parameter pairs
    /// from: the scope broker's publish policy, or a [`Bound`](crate::runtime::Bound) token for
    /// a different registered broker.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<S2>(
        mut self,
        source: S2,
    ) -> IncludeEgress<'s, B, Layers, C, State, Pipeline, D, WithSource<S2>>
    where
        WithSource<S2>: CommitEgress<B, Layers, C, State, Pipeline, D>,
    {
        let (def, _missing) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeEgress {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
        }
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> std::fmt::Debug
    for IncludeEgress<'_, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitEgress<B, Layers, C, State, Pipeline, D>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeEgress").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> Drop
    for IncludeEgress<'_, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitEgress<B, Layers, C, State, Pipeline, D>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, D> IncludeMount<'s, B, Layers, C, State, Pipeline, D>
    for forms::Egress
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    D: EgressDef,
{
    type Out = IncludeEgress<'s, B, Layers, C, State, Pipeline, D, MissingEgress>;

    fn begin(def: D, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeEgress {
            scope: Some(scope),
            parts: Some((def, MissingEgress)),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing: the same builder shape; the reply source pairs into a ReplyPublisher.

/// One commit strategy of a batch publishing registration builder. Machinery.
#[doc(hidden)]
pub trait CommitBatchPublishing<B: Broker, Layers, C, State, Pipeline, D>: Sized {
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, D> CommitBatchPublishing<B, Layers, C, State, Pipeline, D>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    C: ScopeCodec,
    D: BatchPublishingCall<State> + 'static,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Reply: Serialize + Send + Sync + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_batch_publishing_source(source, def, reply);
    }
}

impl<B, Layers, C, State, Pipeline, D, Src, RP>
    CommitBatchPublishing<B, Layers, C, State, Pipeline, D> for WithSource<Src>
where
    B: Broker + 'static,
    C: ScopeCodec,
    D: BatchPublishingCall<State> + 'static,
    D::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <D::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    <<D::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Reply: Serialize + Send + Sync + 'static,
    Src: PublishPolicy<Connected<B>, Live = RP> + Send + 'static,
    RP: ReplyPublisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: D, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, self.0);
    }
}

/// The registration builder [`BrokerScope::include_batch`] returns for a
/// `#[subscriber(batch(..), publish("dest"))]` definition. Commits when dropped; see
/// [`IncludePublishing`].
pub struct IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitBatchPublishing<B, Layers, C, State, Pipeline, D>,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(D, Src)>,
}

impl<'s, B, Layers, C, State, Pipeline, D, Src>
    IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitBatchPublishing<B, Layers, C, State, Pipeline, D>,
{
    /// Attaches the reply source: a [`TypedPublisher`] stack over a publish policy, its
    /// [`transactional`](TypedPublisher::transactional) form for one transaction per batch, or a
    /// [`Bound`](crate::runtime::Bound) token wrapping either.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<S2>(
        mut self,
        source: S2,
    ) -> IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, D, WithSource<S2>>
    where
        WithSource<S2>: CommitBatchPublishing<B, Layers, C, State, Pipeline, D>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeBatchPublishing {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
        }
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> std::fmt::Debug
    for IncludeBatchPublishing<'_, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitBatchPublishing<B, Layers, C, State, Pipeline, D>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeBatchPublishing")
            .finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, D, Src> Drop
    for IncludeBatchPublishing<'_, B, Layers, C, State, Pipeline, D, Src>
where
    B: Broker + 'static,
    Src: CommitBatchPublishing<B, Layers, C, State, Pipeline, D>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, D> IncludeMount<'s, B, Layers, C, State, Pipeline, D>
    for forms::BatchPublishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitBatchPublishing<B, Layers, C, State, Pipeline, D>,
{
    type Out = IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, D, DefaultReply>;

    fn begin(def: D, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeBatchPublishing {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
        }
    }
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Mounts a plain `#[subscriber]` definition on an explicit subscription `source`
    /// (overriding the macro's own source), decoding with the scope codec (or the default).
    pub fn include_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        C: ScopeCodec,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        D::Context: crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + 'static,
        State: Send + Sync + 'static,
        Layers:
            Layer<Typed<<S::Subscriber as Subscriber>::Message, D::Input, C::Codec, D::Handler>>,
        Layers::Handler:
            Handler<<S::Subscriber as Subscriber>::Message, D::Context, State> + 'static,
    {
        let codec = self.codec.scope_codec();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber(batch(..))]` definition on an explicit subscription `source`,
    /// decoding each element with the scope codec (or the default).
    pub fn include_batch_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        C: ScopeCodec,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: crate::runtime::SliceHandler<D::Input, State> + 'static,
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
    pub fn include_publishing_on<S, D, Src, P, PC, PL>(&mut self, source: S, def: D, publisher: Src)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        C: ScopeCodec,
        D: PublishingCall<State> + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        D::Context:
            crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + Sync + 'static,
        Src: PublishPolicy<Connected<B>, Live = TypedPublisher<P, PC, PL>> + Send + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishTransform<D::Context> + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<PublishingHandler<D, C::Codec, P, PC, PL, Pipeline>> + Clone + Send + 'static,
        Layers::Handler:
            Handler<<S::Subscriber as Subscriber>::Message, D::Context, State> + 'static,
    {
        self.mount_publishing_source(source, def, publisher);
    }

    /// Mounts a `batch(.., publish("dest"))` definition on an explicit subscription `source`,
    /// replying through `publisher` (a typed policy stack, its transactional form, or a
    /// [`Bound`](crate::runtime::Bound) token wrapping either).
    pub fn include_batch_publishing_on<S, D, Src, RP>(&mut self, source: S, def: D, publisher: Src)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        <S::Subscriber as Subscriber>::Message: Send + 'static,
        C: ScopeCodec,
        D: BatchPublishingCall<State> + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        Src: PublishPolicy<Connected<B>, Live = RP> + Send + 'static,
        RP: ReplyPublisher + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
    {
        self.mount_batch_publishing_source(source, def, publisher);
    }
}
