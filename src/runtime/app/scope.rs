//! The per-broker handler registration scope and its shared mount tails.

use std::fmt;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Connected, Publisher, Subscriber, SubscriptionSource};

use crate::PublishPolicy;
use crate::runtime::Bound;
use crate::runtime::batch::{BatchDef, batch_metadata, typed_batch};
use crate::runtime::batch_publishing::{
    BatchPublishingCall, BatchPublishingHandler, batch_publishing_metadata,
};
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::lifecycle::{BoxError, ConnectedSlot};
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity, Layer};
use crate::runtime::out::{OutCall, OutHandler, out_metadata};
use crate::runtime::publish::{
    PublishIdentity, PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher,
};
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::runtime::publishing::{PublishingCall, PublishingHandler, publishing_metadata};
use crate::runtime::router::{RouterDef, RouterSink};
use crate::runtime::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::runtime::typed::{Typed, typed};

use super::include::ScopeCodec;

/// A handler-registration scope bound to one broker.
///
/// Handed to the [`RustStream::with_broker`](crate::runtime::RustStream::with_broker) closure. It
/// is a [`Router`](crate::runtime::Router) plus the broker it is bound to and the global middleware
/// stack `Layers`; registrations are collected and started later, in
/// [`RustStream::run`](crate::runtime::RustStream::run). Each handler registered here is wrapped
/// with `Layers` before it is stored.
pub struct BrokerScope<B: Broker, Layers = Identity, C = (), State = (), Pipeline = PublishIdentity>
{
    pub(super) broker: B,
    /// The slot the runtime fills with this broker's connected form at startup; shared with
    /// every starter of this scope and with the [`Bound`] tokens minted here.
    pub(super) slot: ConnectedSlot<B>,
    pub(super) sink: RouterSink<B, State>,
    pub(super) pipeline: Pipeline,
    pub(super) retry_publisher: Option<Arc<dyn ErasedPublisher>>,
    pub(super) global: Layers,
    pub(super) codec: C,
}

impl<B: Broker + 'static, Layers, C, State, Pipeline> BrokerScope<B, Layers, C, State, Pipeline> {
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Binds `source` to this scope's broker, producing a token usable as the publisher source
    /// of a registration on a *different* broker's scope (a handler consuming one broker while
    /// publishing to this one), or for post-start sending from a sibling task.
    ///
    /// Being minted by the scope is the token's proof of registration: it shares the slot the
    /// runtime fills with this broker's connected form at startup, so pairing cannot pick a
    /// wrong instance.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # fn demo() {
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, RustStream};
    ///
    /// let mut egress = None;
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(MemoryBroker::new(), |b| {
    ///         egress = Some(b.bind(MemoryPublish));
    ///     });
    /// # let _ = (app, egress);
    /// # }
    /// ```
    #[must_use]
    pub fn bind<S>(&self, source: S) -> Bound<B, S>
    where
        S: PublishPolicy<Connected<B>>,
    {
        Bound {
            slot: Arc::clone(&self.slot),
            source,
        }
    }

    /// Wires a publisher for the broker-agnostic `retry_after` fallback on this scope.
    ///
    /// When a handler returns [`HandlerResult::retry_after`](crate::runtime::HandlerResult::retry_after)
    /// (or a delivery is `nack_after`-ed) on a broker that does not natively support delayed
    /// redelivery, the runtime re-publishes the message to its own source subject after the delay,
    /// through `publisher`, with the
    /// [`RETRY_COUNT_HEADER`](crate::runtime::RETRY_COUNT_HEADER) incremented. Pass a publisher
    /// bound to the same broker (`b.broker().publisher()`); a publish to the source subject then
    /// reaches this scope's own subscriptions.
    ///
    /// Brokers with native delayed redelivery do not need this: the runtime uses their
    /// [`nack_after`](crate::IncomingMessage::nack_after) instead. Without it, a `retry_after` on a
    /// non-native broker degrades to an immediate requeue (with a warning).
    ///
    /// # Cancel safety
    ///
    /// The fallback's deferred re-publish is at-most-once over the delay window: see
    /// [`HandlerResult::retry_after`](crate::runtime::HandlerResult::retry_after).
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::BrokerScope;
    /// use ruststream::{Broker, Publisher};
    ///
    /// // Wire a deferred-retry publisher bound to the same broker as the scope.
    /// fn configure<B, P>(scope: &mut BrokerScope<B>, retry_publisher: P)
    /// where
    ///     B: Broker + 'static,
    ///     P: Publisher + 'static,
    /// {
    ///     scope.retry_via(retry_publisher);
    /// }
    /// ```
    pub fn retry_via<P>(&mut self, publisher: P)
    where
        P: Publisher + 'static,
    {
        self.retry_publisher = Some(Arc::new(publisher));
    }

    /// Attaches `handler` (wrapped with the global stack) to an already-created `subscriber`.
    ///
    /// See [`Router::handle`](crate::runtime::Router::handle).
    pub fn handle<S, H, Cx>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        State: Send + Sync + 'static,
        Cx: crate::BuildContext<S::Message> + Send + 'static,
        H: Handler<S::Message, Cx, State> + 'static,
        Layers: Layer<H>,
        Layers::Handler: Handler<S::Message, Cx, State> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink
            .push_handle(subscriber, handler, meta, FailurePolicies::default());
    }

    /// Attaches `handler` (wrapped with the global stack) to a subscription described by `source`.
    ///
    /// See [`Router::subscribe`](crate::runtime::Router::subscribe).
    pub fn subscribe<S, H, Cx>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        State: Send + Sync + 'static,
        Cx: crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + 'static,
        H: Handler<<S::Subscriber as Subscriber>::Message, Cx, State> + 'static,
        Layers: Layer<H>,
        Layers::Handler: Handler<<S::Subscriber as Subscriber>::Message, Cx, State> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink
            .push_subscribe(source, handler, meta, FailurePolicies::default());
    }

    /// Mounts every registration from `router` onto this broker, wrapping each handler with the
    /// app's global middleware stack.
    ///
    /// Unlike a hand-rolled handler group, a [`Router`](crate::runtime::Router) composes with the
    /// app's [`layer`](crate::runtime::RustStream::layer): the global stack must be a
    /// [`BlanketLayer`] (it applies to handlers whose concrete types the router hides), which every
    /// bundled layer and any [`Stack`](crate::runtime::Stack) of them satisfies.
    pub fn include_router<R>(&mut self, router: R)
    where
        R: RouterDef<B, State>,
        State: Send + Sync + 'static,
        Layers: BlanketLayer + Clone + Send + Sync + 'static,
        Pipeline: PublishPipeline + Clone + Send + 'static,
    {
        router.mount(&self.global, &self.pipeline, &mut self.sink);
    }
}

impl<B: Broker + 'static, Layers, SC, State, Pipeline> BrokerScope<B, Layers, SC, State, Pipeline> {
    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the
    /// `include` / `include_on` forms.
    pub(super) fn mount_subscriber<S, D, C>(&mut self, source: S, def: D, codec: C)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Context: crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
        State: Send + Sync + 'static,
        Layers: Layer<Typed<<S::Subscriber as Subscriber>::Message, D::Input, C, D::Handler>>,
        Layers::Handler:
            Handler<<S::Subscriber as Subscriber>::Message, D::Context, State> + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = self
            .global
            .layer(typed(codec, def.into_handler()).on_decode_failure(policies.decode));
        self.sink
            .push_subscribe_workers(source, handler, meta, policies, workers);
    }

    /// Mounts a batch definition on `source`, decoding each element with `codec`. The shared
    /// tail of the `include_batch` / `include_batch_on` forms. Batch handlers are not wrapped by
    /// the global stack: per-message layers cannot wrap a whole-batch handler.
    pub(super) fn mount_batch<S, D, C>(&mut self, source: S, def: D, codec: C)
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: crate::runtime::SliceHandler<D::Input, State> + 'static,
        C: Codec + 'static,
        State: Send + Sync + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = typed_batch(codec, def.into_handler()).with_decode(policies.decode);
        self.sink
            .push_subscribe_batch(source, handler, meta, policies, workers);
    }

    /// Mounts a publishing definition whose reply publisher is a policy source, paired by the
    /// runtime after connect. Decode uses the scope codec; the reply codec and transforms travel
    /// on the source's typed stack.
    pub(super) fn mount_publishing_source<S, D, Src, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        reply: Src,
    ) where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        D: PublishingCall<State> + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        D::Context:
            crate::BuildContext<<S::Subscriber as Subscriber>::Message> + Send + Sync + 'static,
        Src: PublishPolicy<Connected<B>, Live = TypedPublisher<P, PC, PL>> + Send + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishTransform<D::Context> + 'static,
        SC: ScopeCodec,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        Layers:
            Layer<PublishingHandler<D, SC::Codec, P, PC, PL, Pipeline>> + Clone + Send + 'static,
        Layers::Handler:
            Handler<<S::Subscriber as Subscriber>::Message, D::Context, State> + 'static,
        B::Connected: 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.scope_codec();
        let pipeline = self.pipeline.clone();
        let global = self.global.clone();
        self.sink.push_paired_workers(
            source,
            async move |connected: Arc<Connected<B>>| {
                let publisher = reply
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(global.layer(PublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    decode: policies.decode,
                }))
            },
            meta,
            policies,
            workers,
        );
    }

    /// Mounts an out definition: the handler's injected publisher comes from `out`, a policy
    /// source paired by the runtime after connect. Decode uses the scope codec.
    pub(super) fn mount_out_source<Source, Def, OutSource>(
        &mut self,
        source: Source,
        def: Def,
        out: OutSource,
    ) where
        // The subscription side, as in mount_publishing_source.
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + Sync + 'static,
        Def: OutCall<State> + 'static,
        Def::Input: DeserializeOwned + Send + Sync + 'static,
        Def::Context: crate::BuildContext<<Source::Subscriber as Subscriber>::Message>
            + Send
            + Sync
            + 'static,
        Def::Out: Send + Sync + 'static,
        // The injected publisher: the source pairs at startup into exactly the type the
        // handler's Out parameter names.
        OutSource: PublishPolicy<Connected<B>, Live = Def::Out> + Send + 'static,
        SC: ScopeCodec,
        State: Send + Sync + 'static,
        Layers: Layer<OutHandler<Def, SC::Codec, Def::Out>> + Clone + Send + 'static,
        Layers::Handler:
            Handler<<Source::Subscriber as Subscriber>::Message, Def::Context, State> + 'static,
        B::Connected: 'static,
    {
        let meta = out_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.scope_codec();
        let global = self.global.clone();
        self.sink.push_paired_workers(
            source,
            async move |connected: Arc<Connected<B>>| {
                let live = out
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(global.layer(OutHandler {
                    def,
                    codec,
                    out: live,
                    decode: policies.decode,
                }))
            },
            meta,
            policies,
            workers,
        );
    }

    /// Mounts a batch publishing definition whose reply publisher is a policy source, paired by
    /// the runtime after connect. Decode uses the scope codec.
    pub(super) fn mount_batch_publishing_source<Source, Def, ReplySource, BatchReply>(
        &mut self,
        source: Source,
        def: Def,
        reply: ReplySource,
    ) where
        // The subscription side: batches open against the connected form.
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        <Source::Subscriber as Subscriber>::Message: Send + 'static,
        Def: BatchPublishingCall<State> + 'static,
        Def::Input: DeserializeOwned + Send + Sync + 'static,
        Def::Reply: Serialize + Send + Sync + 'static,
        // The reply side: the source pairs at startup into a batch reply wiring (plain or
        // transactional).
        ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
        BatchReply: ReplyPublisher + 'static,
        SC: ScopeCodec,
        Pipeline: PublishPipeline + Clone + Send + 'static,
        State: Send + Sync + 'static,
        B::Connected: 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let codec = self.codec.scope_codec();
        let pipeline = self.pipeline.clone();
        self.sink.push_paired_batch(
            source,
            async move |connected: Arc<Connected<B>>| {
                let publisher = reply
                    .pair(connected.as_ref())
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                Ok(BatchPublishingHandler {
                    def,
                    codec,
                    publisher,
                    pipeline,
                    decode: policies.decode,
                })
            },
            meta,
            policies,
            workers,
        );
    }
}

impl<B: Broker, Layers, C, State, Pipeline> fmt::Debug
    for BrokerScope<B, Layers, C, State, Pipeline>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerScope")
            .field("sink", &self.sink)
            .finish_non_exhaustive()
    }
}
