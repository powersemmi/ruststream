//! The `include` family on [`BrokerScope`]: mounting macro-generated definitions.
//!
//! `include` is one entry point for every single-message definition form and `include_batch` for
//! both batch forms; which machinery runs is picked by the definition's form token
//! ([`IncludeDef::Form`]), so `b.include(handle)`, `b.include(respond).publisher(..)` and
//! `b.include(forward).publisher(..)` all read the same. Publisher-producing forms return a
//! registration builder that commits when the statement ends; `.publisher(..)` attaches the
//! publish policy (or a [`Bound`](crate::runtime::Bound) token for a cross-broker target).

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
// The typed default-reply commits need a default codec, so that import is gated the same way;
// the raw default-reply commit publishes bare bytes and needs only `DefaultPublish`.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::DefaultCodec;
use crate::{
    BatchSubscriber, Broker, BuildContext, Connected, DefaultPublish, PublishPolicy, Publisher,
    Subscriber, SubscriptionSource,
};

use crate::runtime::SliceHandler;
use crate::runtime::batch::BatchDef;
use crate::runtime::batch_publishing::BatchPublishingCall;
use crate::runtime::handler::Handler;
use crate::runtime::inject::{FromStartup, InjectCall, InjectDef, InjectHandler};
use crate::runtime::lifecycle::BoxError;
use crate::runtime::middleware::Layer;
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::runtime::publish::PublishTransformIdentity;
use crate::runtime::publish::{PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingCall, PublishingHandler};
use crate::runtime::raw::{
    RawPublishingCall, RawPublishingHandler, RawReplyCall, RawReplyHandler, RawSubscriberDef,
    raw_metadata, raw_publishing_metadata, raw_reply_metadata,
};
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
    /// A raw reply-publishing subscriber (`#[subscriber("in", raw, publish_raw("out"))]`):
    /// bytes in, bytes out, no codec on either side.
    #[derive(Debug, Clone, Copy)]
    pub struct RawPublishing;
    /// A typed-input, byte-reply subscriber (`#[subscriber("in", publish_raw("out"))]`): the
    /// input decodes with the scope codec, the reply bytes go out as-is.
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
    Def::Input: DeserializeOwned + Send + Sync + 'static,
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
// Raw subscribing: eager, no builder, and no codec anywhere - the handler runs at the raw level
// over the broker's message type, so the scope codec parameter `C` is left unconstrained and the
// mount works without any codec feature.

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawSubscribing
where
    B: Broker + 'static,
    Def: RawSubscriberDef,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message: 'static,
    Def::Handler: 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<Def::Handler>,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    type Out = ();

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let meta = raw_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = scope.global.layer(def.into_handler());
        scope
            .sink
            .push_subscribe_workers(source, handler, meta, policies, workers);
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
    Def::Input: DeserializeOwned + Send + Sync + 'static,
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
    Def::Input: DeserializeOwned + Send + Sync + 'static,
    Def::Handler: SliceHandler<Def::Input, State> + 'static,
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
// Reply publishing, out injection and batch publishing: forms returning a registration builder.
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
pub struct WithSource<Source>(Source);

/// One commit strategy of a publishing registration builder. Machinery; never named directly.
#[doc(hidden)]
pub trait CommitPublishing<B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def> CommitPublishing<B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    C: ScopeCodec,
    Def: PublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DeserializeOwned + Send + Sync + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
            PublishingHandler<
                Def,
                <C as ScopeCodec>::Codec,
                <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live,
                DefaultCodec,
                PublishTransformIdentity,
                Pipeline,
            >,
        > + Clone
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
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_publishing_source(source, def, reply);
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, Leaf, ReplyCodec, Transforms>
    CommitPublishing<B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: PublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DeserializeOwned + Send + Sync + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>, Live = TypedPublisher<Leaf, ReplyCodec, Transforms>>
        + Send
        + 'static,
    Leaf: Publisher + 'static,
    ReplyCodec: Codec + 'static,
    Transforms: PublishTransform<Def::Context> + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<
            PublishingHandler<
                Def,
                <C as ScopeCodec>::Codec,
                Leaf,
                ReplyCodec,
                Transforms,
                Pipeline,
            >,
        > + Clone
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
        scope.mount_publishing_source(source, def, self.0);
    }
}

/// The registration builder [`BrokerScope::include`] returns for a `publish("dest")` definition.
///
/// Commits when dropped (the end of the `b.include(..)` statement). Without a
/// [`publisher`](Self::publisher) call it commits with the broker's default publish policy under
/// the default codec; with one, the attached source is paired by the runtime at startup.
pub struct IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitPublishing<B, Layers, C, State, Pipeline, Def>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Source)>,
}

impl<'s, B, Layers, C, State, Pipeline, Def, Source>
    IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitPublishing<B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the reply source: a [`TypedPublisher`] stack over a publish policy (naming the
    /// reply codec and transforms), or a [`Bound`](crate::runtime::Bound) token wrapping one for
    /// a cross-broker reply. The runtime pairs it after the brokers connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitPublishing<B, Layers, C, State, Pipeline, Def>,
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

impl<B, Layers, C, State, Pipeline, Def, Source> std::fmt::Debug
    for IncludePublishing<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludePublishing").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> Drop
    for IncludePublishing<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
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
    DefaultReply: CommitPublishing<B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludePublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludePublishing {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Raw reply publishing: the same builder shape as the typed form, but the reply source pairs
// into a bare live publisher (no TypedPublisher, no codec) and the handler's returned bytes are
// published as-is. The scope codec parameter `C` stays unconstrained, so the form mounts without
// any codec feature.

/// One commit strategy of a raw publishing registration builder. Machinery; never named
/// directly.
#[doc(hidden)]
pub trait CommitRawPublishing<B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

impl<B, Layers, C, State, Pipeline, Def> CommitRawPublishing<B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // The default raw reply is the broker's plain publish policy taken bare: unlike the
        // typed default there is no codec to attach, so the policy commits as if the user had
        // chained `.publisher(<default policy>)`. (UFCS: several Commit* traits give
        // `WithSource` a `commit`.)
        CommitRawPublishing::commit(
            WithSource(<B::Connected as DefaultPublish>::Policy::default()),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, Live>
    CommitRawPublishing<B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    B::Connected: 'static,
    Def: RawPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Reply: AsRef<[u8]> + Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>, Live = Live> + Send + 'static,
    Live: Publisher + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<RawPublishingHandler<Def, Live>> + Clone + Send + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let meta = raw_publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let global = scope.global.clone();
        let reply = self.0;
        scope.sink.push_paired_workers(
            source,
            async move |connected: Arc<Connected<B>>| {
                let publisher = reply
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(global.layer(RawPublishingHandler { def, publisher }))
            },
            meta,
            policies,
            workers,
        );
    }
}

/// The registration builder [`BrokerScope::include`] returns for a `raw, publish("dest")`
/// definition.
///
/// Commits when dropped (the end of the `b.include(..)` statement). Without a
/// [`publisher`](Self::publisher) call it commits with the broker's default publish policy;
/// with one, the attached source is paired by the runtime at startup. Either way the reply
/// publisher is the policy's bare live [`Publisher`] - no codec, no transforms: the handler's
/// bytes go on the wire as returned.
pub struct IncludeRawPublishing<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Source)>,
}

impl<'s, B, Layers, C, State, Pipeline, Def, Source>
    IncludeRawPublishing<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the reply source: any publish policy whose live form is a bare [`Publisher`]
    /// (not a [`TypedPublisher`] stack - a raw reply has no codec), or a
    /// [`Bound`](crate::runtime::Bound) token wrapping one for a cross-broker reply. The
    /// runtime pairs it after the brokers connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeRawPublishing<'s, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeRawPublishing {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
        }
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> std::fmt::Debug
    for IncludeRawPublishing<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeRawPublishing")
            .finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> Drop
    for IncludeRawPublishing<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
    }
}

impl<'s, B, Layers, C, State, Pipeline, Def> IncludeMount<'s, B, Layers, C, State, Pipeline, Def>
    for forms::RawPublishing
where
    B: Broker + 'static,
    Layers: 's,
    C: 's,
    State: 's,
    Pipeline: 's,
    DefaultReply: CommitRawPublishing<B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeRawPublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeRawPublishing {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Typed-input, byte-reply publishing (`publish_raw` without `raw`): the consume side decodes
// with the scope codec exactly like the typed reply form, the reply side is the bare live
// publisher of the raw form.

/// One commit strategy of a typed-input, byte-reply registration builder. Machinery; never
/// named directly.
#[doc(hidden)]
pub trait CommitRawReply<B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

impl<B, Layers, C, State, Pipeline, Def> CommitRawReply<B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    WithSource<<B::Connected as DefaultPublish>::Policy>:
        CommitRawReply<B, Layers, C, State, Pipeline, Def>,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // Same shape as the raw default: the reply side carries no codec, so the default is
        // the broker's plain publish policy taken bare. (UFCS: several Commit* traits give
        // `WithSource` a `commit`.)
        CommitRawReply::commit(
            WithSource(<B::Connected as DefaultPublish>::Policy::default()),
            def,
            scope,
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, Live>
    CommitRawReply<B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    B::Connected: 'static,
    C: ScopeCodec,
    Def: RawReplyCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DeserializeOwned + Send + Sync + 'static,
    Def::Reply: AsRef<[u8]> + Send + Sync + 'static,
    Def::Context: BuildContext<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
        > + Send
        + Sync
        + 'static,
    Source: PublishPolicy<Connected<B>, Live = Live> + Send + 'static,
    Live: Publisher + 'static,
    State: Send + Sync + 'static,
    Layers: Layer<RawReplyHandler<Def, C::Codec, Live>> + Clone + Send + 'static,
    Layers::Handler: Handler<
            <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message,
            Def::Context,
            State,
        > + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let meta = raw_reply_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let decode = policies.decode;
        let codec = scope.codec.scope_codec();
        let global = scope.global.clone();
        let reply = self.0;
        scope.sink.push_paired_workers(
            source,
            async move |connected: Arc<Connected<B>>| {
                let publisher = reply
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(global.layer(RawReplyHandler {
                    def,
                    codec,
                    publisher,
                    decode,
                }))
            },
            meta,
            policies,
            workers,
        );
    }
}

/// The registration builder [`BrokerScope::include`] returns for a `publish_raw("dest")`
/// definition with a typed input.
///
/// Commits when dropped, exactly like [`IncludeRawPublishing`]: the broker's default publish
/// policy without a [`publisher`](Self::publisher) call, the attached source with one. The
/// input decodes with the scope codec; the reply bytes go out through the policy's bare live
/// [`Publisher`], unencoded.
pub struct IncludeRawReply<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawReply<B, Layers, C, State, Pipeline, Def>,
{
    // Options only so `publisher` can move the pieces into the replacement builder out of a
    // Drop type; both stay `Some` until the commit or that replacement.
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Source)>,
}

impl<'s, B, Layers, C, State, Pipeline, Def, Source>
    IncludeRawReply<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawReply<B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the reply source: any publish policy whose live form is a bare [`Publisher`]
    /// (a raw reply has no codec), or a [`Bound`](crate::runtime::Bound) token wrapping one for
    /// a cross-broker reply. The runtime pairs it after the brokers connect.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeRawReply<'s, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitRawReply<B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _default) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeRawReply {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
        }
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> std::fmt::Debug
    for IncludeRawReply<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawReply<B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeRawReply").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> Drop
    for IncludeRawReply<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitRawReply<B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
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
    DefaultReply: CommitRawReply<B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeRawReply<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeRawReply {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Out injection: no default source; committing without one is a build-time panic.

/// The "no source yet" marker of [`IncludeOut`]. Committing with it is a wiring bug.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct MissingOut;

/// One commit strategy of an out registration builder. Machinery; never named directly.
#[doc(hidden)]
pub trait CommitOut<B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

impl<B, Layers, C, State, Pipeline, Def> CommitOut<B, Layers, C, State, Pipeline, Def>
    for MissingOut
where
    B: Broker + 'static,
{
    fn commit(self, _def: Def, _scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        // An Out parameter has no broker-side default; requiring the source at the include
        // site is the point of the injection. This fires at application build time (the same
        // moment as the on_startup ordering assert), never mid-run.
        panic!(
            "an Out handler was included without a publisher source: chain \
             .publisher(<policy or bound token>) on b.include(..)"
        );
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> CommitOut<B, Layers, C, State, Pipeline, Def>
    for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: InjectCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: Sync + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + Sync + 'static,
    Def::Input: DeserializeOwned + Send + Sync + 'static,
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

/// The registration builder [`BrokerScope::include`] returns for a handler with an
/// [`Out`](crate::runtime::Out) parameter.
///
/// Commits when dropped. There is no default source: committing without a
/// [`publisher`](Self::publisher) call panics at application build time, naming the fix.
pub struct IncludeOut<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitOut<B, Layers, C, State, Pipeline, Def>,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Source)>,
}

impl<'s, B, Layers, C, State, Pipeline, Def, Source>
    IncludeOut<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitOut<B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the source the handler's [`Out`](crate::runtime::Out) parameter pairs from:
    /// the scope broker's publish policy, or a [`Bound`](crate::runtime::Bound) token for a
    /// different registered broker.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeOut<'s, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitOut<B, Layers, C, State, Pipeline, Def>,
    {
        let (def, _missing) = self
            .parts
            .take()
            .expect("builder parts are present until commit or replacement");
        let scope = self
            .scope
            .take()
            .expect("builder scope is present until commit or replacement");
        IncludeOut {
            scope: Some(scope),
            parts: Some((def, WithSource(source))),
        }
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> std::fmt::Debug
    for IncludeOut<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitOut<B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeOut").finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> Drop
    for IncludeOut<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitOut<B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
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
        IncludeOut {
            scope: Some(scope),
            parts: Some((def, MissingOut)),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Batch publishing: the same builder shape; the reply source pairs into a ReplyPublisher.

/// One commit strategy of a batch publishing registration builder. Machinery.
#[doc(hidden)]
pub trait CommitBatchPublishing<B: Broker, Layers, C, State, Pipeline, Def>: Sized {
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>);
}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<B, Layers, C, State, Pipeline, Def> CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>
    for DefaultReply
where
    B: Broker + 'static,
    B::Connected: DefaultPublish,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DeserializeOwned + Send + Sync + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live:
        Publisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        let reply = TypedPublisher::new(<B::Connected as DefaultPublish>::Policy::default());
        scope.mount_batch_publishing_source(source, def, reply);
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source, BatchReply>
    CommitBatchPublishing<B, Layers, C, State, Pipeline, Def> for WithSource<Source>
where
    B: Broker + 'static,
    C: ScopeCodec,
    Def: BatchPublishingCall<State> + 'static,
    Def::Source: SubscriptionSource<Connected<B>> + Send + 'static,
    <Def::Source as SubscriptionSource<Connected<B>>>::Subscriber: BatchSubscriber + Send + 'static,
    <<Def::Source as SubscriptionSource<Connected<B>>>::Subscriber as Subscriber>::Message:
        Send + 'static,
    Def::Input: DeserializeOwned + Send + Sync + 'static,
    Def::Reply: Serialize + Send + Sync + 'static,
    Source: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
    Pipeline: PublishPipeline + Clone + Send + 'static,
    State: Send + Sync + 'static,
{
    fn commit(self, def: Def, scope: &mut BrokerScope<B, Layers, C, State, Pipeline>) {
        let source = def.source();
        scope.mount_batch_publishing_source(source, def, self.0);
    }
}

/// The registration builder [`BrokerScope::include_batch`] returns for a
/// `#[subscriber(batch(..), publish("dest"))]` definition. Commits when dropped; see
/// [`IncludePublishing`].
pub struct IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>,
{
    scope: Option<&'s mut BrokerScope<B, Layers, C, State, Pipeline>>,
    parts: Option<(Def, Source)>,
}

impl<'s, B, Layers, C, State, Pipeline, Def, Source>
    IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>,
{
    /// Attaches the reply source: a [`TypedPublisher`] stack over a publish policy, its
    /// [`transactional`](TypedPublisher::transactional) form for one transaction per batch, or a
    /// [`Bound`](crate::runtime::Bound) token wrapping either.
    ///
    /// # Panics
    ///
    /// Never in practice: the internal expects guard builder invariants that hold until the
    /// commit or this replacement.
    pub fn publisher<NewSource>(
        mut self,
        source: NewSource,
    ) -> IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, WithSource<NewSource>>
    where
        WithSource<NewSource>: CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>,
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

impl<B, Layers, C, State, Pipeline, Def, Source> std::fmt::Debug
    for IncludeBatchPublishing<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncludeBatchPublishing")
            .finish_non_exhaustive()
    }
}

impl<B, Layers, C, State, Pipeline, Def, Source> Drop
    for IncludeBatchPublishing<'_, B, Layers, C, State, Pipeline, Def, Source>
where
    B: Broker + 'static,
    Source: CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>,
{
    fn drop(&mut self) {
        if let (Some((def, src)), Some(scope)) = (self.parts.take(), self.scope.take()) {
            src.commit(def, scope);
        }
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
    DefaultReply: CommitBatchPublishing<B, Layers, C, State, Pipeline, Def>,
{
    type Out = IncludeBatchPublishing<'s, B, Layers, C, State, Pipeline, Def, DefaultReply>;

    fn begin(def: Def, scope: &'s mut BrokerScope<B, Layers, C, State, Pipeline>) -> Self::Out {
        IncludeBatchPublishing {
            scope: Some(scope),
            parts: Some((def, DefaultReply)),
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
        Def::Input: DeserializeOwned + Send + Sync + 'static,
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
        Def::Input: DeserializeOwned + Send + Sync + 'static,
        Def::Handler: SliceHandler<Def::Input, State> + 'static,
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
    pub fn include_publishing_on<Source, Def, ReplySource, Leaf, ReplyCodec, Transforms>(
        &mut self,
        source: Source,
        def: Def,
        publisher: ReplySource,
    ) where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        C: ScopeCodec,
        Def: PublishingCall<State> + 'static,
        Def::Input: DeserializeOwned + Send + Sync + 'static,
        Def::Reply: Serialize + Send + Sync + 'static,
        Def::Context:
            BuildContext<<Source::Subscriber as Subscriber>::Message> + Send + Sync + 'static,
        ReplySource: PublishPolicy<Connected<B>, Live = TypedPublisher<Leaf, ReplyCodec, Transforms>>
            + Send
            + 'static,
        Leaf: Publisher + 'static,
        ReplyCodec: Codec + 'static,
        Transforms: PublishTransform<Def::Context> + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<PublishingHandler<Def, C::Codec, Leaf, ReplyCodec, Transforms, Pipeline>>
            + Clone
            + Send
            + 'static,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
    {
        self.mount_publishing_source(source, def, publisher);
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
        Source::Subscriber: BatchSubscriber + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + 'static,
        C: ScopeCodec,
        Def: BatchPublishingCall<State> + 'static,
        Def::Input: DeserializeOwned + Send + Sync + 'static,
        Def::Reply: Serialize + Send + Sync + 'static,
        ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
        BatchReply: ReplyPublisher + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
    {
        self.mount_batch_publishing_source(source, def, publisher);
    }
}
