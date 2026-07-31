//! The registration list: route types, the per-route mount trait and [`RouterDef`].

use serde::Serialize;

use std::sync::Arc;

use crate::codec::Codec;
use crate::{
    BatchSubscriber, Broker, Connected, PublishPolicy, Publisher, Subscriber, SubscriptionSource,
};

use crate::runtime::batch::BatchHandler;
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingHandler};
use crate::runtime::dispatch::{Workers, spawn_dispatch_workers};
use crate::runtime::failure::{DispatchFailure, FailurePolicies};
use crate::runtime::handler::Handler;
use crate::runtime::lifecycle::BoxError;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::{PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingCall, PublishingHandler};

use super::SourceMessage;
use super::sink::RouterSink;

/// One subscription registration: a source plus the handler it dispatches to. An implementation
/// detail of [`Router`](crate::runtime::Router)'s registration list.
#[doc(hidden)]
#[derive(Debug)]
pub struct SubscribeRoute<S, H> {
    pub(super) source: S,
    pub(super) handler: H,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// One registration bound to an already-created subscriber. An implementation detail of
/// [`Router`](crate::runtime::Router).
#[doc(hidden)]
#[derive(Debug)]
pub struct HandleRoute<S, H> {
    pub(super) subscriber: S,
    pub(super) handler: H,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
}

/// One batch-subscription registration: a source plus the batch handler consuming its batches.
/// An implementation detail of [`Router`](crate::runtime::Router)'s registration list.
#[doc(hidden)]
#[derive(Debug)]
pub struct BatchRoute<S, H> {
    pub(super) source: S,
    pub(super) handler: H,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

/// One mountable registration: applies the global blanket layer to its handler and registers it.
/// `State` is the app's shared-state type, threaded so a route only mounts on a sink whose state type
/// its handler matches (a state-agnostic handler matches any).
pub(super) trait MountRoute<B: Broker, State> {
    fn mount_one<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static;
}

/// One registration's `AsyncAPI` metadata, collected independently of the app state type (so
/// [`Router::handlers`](crate::runtime::Router::handlers) works whatever state the handlers read).
pub(super) trait RouteMeta {
    fn collect(&self, out: &mut Vec<HandlerMetadata>);
}

impl<S, H> RouteMeta for SubscribeRoute<S, H> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<S, H> RouteMeta for BatchRoute<S, H> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<S, H> RouteMeta for HandleRoute<S, H> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<B, S, H, State> MountRoute<B, State> for SubscribeRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<Connected<B>> + Send + 'static,
    S::Subscriber: Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    State: Send + Sync + 'static,
    H: Handler<SourceMessage<B, S>, (), State> + 'static,
{
    fn mount_one<G, PP>(self, global: &G, _pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let handler = global.apply::<SourceMessage<B, S>, (), State, H>(self.handler);
        sink.push_subscribe_workers(self.source, handler, self.meta, self.policies, self.workers);
    }
}

impl<B, S, H, State> MountRoute<B, State> for BatchRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<Connected<B>> + Send + 'static,
    S::Subscriber: BatchSubscriber + Send + 'static,
    SourceMessage<B, S>: Send + 'static,
    State: Send + Sync + 'static,
    H: BatchHandler<SourceMessage<B, S>, State> + 'static,
{
    fn mount_one<G, PP>(self, _global: &G, _pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Per-message layers cannot wrap a whole-batch handler, so neither the app-global stack
        // nor the router's own layers apply to batch registrations.
        sink.push_subscribe_batch(
            self.source,
            self.handler,
            self.meta,
            self.policies,
            self.workers,
        );
    }
}

impl<B, S, H, State> MountRoute<B, State> for HandleRoute<S, H>
where
    B: Broker + 'static,
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    State: Send + Sync + 'static,
    H: Handler<S::Message, (), State> + 'static,
{
    fn mount_one<G, PP>(self, global: &G, _pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let handler = global.apply::<S::Message, (), State, H>(self.handler);
        sink.push_handle(self.subscriber, handler, self.meta, self.policies);
    }
}

/// One publishing registration, deferred. Unlike [`SubscribeRoute`], it stores the pieces of a
/// [`PublishingHandler`] rather than a built one: the app's publish pipeline is only known at
/// mount time, and the live reply publisher only exists once the broker connects, so
/// [`mount_one`](MountRoute::mount_one) captures the pieces in a starter closure that pairs the
/// publisher and builds the handler at startup. A router-mounted publishing handler thus picks up
/// the app-wide [`publish_layer`](crate::runtime::RustStream::publish_layer) chain. An
/// implementation detail of [`Router`](crate::runtime::Router)'s registration list.
#[doc(hidden)]
pub struct PublishingRoute<S, D, C, P, PC, PL> {
    pub(super) source: S,
    pub(super) def: D,
    pub(super) codec: C,
    pub(super) publisher: TypedPublisher<P, PC, PL>,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

impl<S, D, C, P, PC, PL> std::fmt::Debug for PublishingRoute<S, D, C, P, PC, PL> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishingRoute")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

/// One batch publishing registration, deferred (see [`PublishingRoute`]). An implementation detail
/// of [`Router`](crate::runtime::Router)'s registration list.
#[doc(hidden)]
pub struct BatchPublishingRoute<S, D, C, R> {
    pub(super) source: S,
    pub(super) def: D,
    pub(super) codec: C,
    pub(super) publisher: R,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
}

impl<S, D, C, R> std::fmt::Debug for BatchPublishingRoute<S, D, C, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchPublishingRoute")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

impl<S, D, C, P, PC, PL> RouteMeta for PublishingRoute<S, D, C, P, PC, PL> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<S, D, C, R> RouteMeta for BatchPublishingRoute<S, D, C, R> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<B, Source, Def, DecodeCodec, Leaf, ReplyCodec, Transforms, State> MountRoute<B, State>
    for PublishingRoute<Source, Def, DecodeCodec, Leaf, ReplyCodec, Transforms>
where
    B: Broker + 'static,
    // The subscription side: the source opens against the connected form, and the definition's
    // handler runs over the messages it yields.
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: Send + 'static,
    SourceMessage<B, Source>: Send + Sync + 'static,
    State: Send + Sync + 'static,
    Def: PublishingCall<State> + 'static,
    Def::Input: crate::runtime::DecodeWith<DecodeCodec>,
    Def::Reply: Serialize + Send + Sync + 'static,
    Def::Context: crate::BuildContext<SourceMessage<B, Source>> + Send + Sync + 'static,
    DecodeCodec: Codec + Send + 'static,
    // The reply side: a typed stack over a policy leaf, paired at startup into the live wiring.
    Leaf: PublishPolicy<Connected<B>> + Send + 'static,
    Leaf::Live: Publisher + 'static,
    ReplyCodec: Codec + Send + 'static,
    Transforms: PublishTransform<Def::Context> + Send + 'static,
{
    fn mount_one<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // The reply wiring is a policy stack: the runtime pairs it against the connected broker
        // at startup, then builds the handler with the app's pipeline and the global stack.
        let global = global.clone();
        let pipeline = pipeline.clone();
        let Self {
            source,
            def,
            codec,
            publisher,
            meta,
            policies,
            workers,
        } = self;
        // Not the paired-factory helper: `BlanketLayer::apply` is an RPITIT whose hidden type
        // captures the layer borrow, so the applied handler cannot be returned out of a factory
        // closure; apply and spawn stay in one block instead.
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        sink.push_raw(
            Box::new(move |connected, state, delivery, shutdown, token| {
                Box::pin(async move {
                    let publisher = publisher
                        .pair(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let subscriber = source
                        .subscribe(connected.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    let handler = global.apply::<SourceMessage<B, Source>, Def::Context, State, _>(
                        PublishingHandler {
                            def,
                            codec,
                            publisher,
                            pipeline,
                            decode: policies.decode,
                        },
                    );
                    let failure = DispatchFailure::new(policies, shutdown);
                    Ok(spawn_dispatch_workers(
                        subscriber,
                        Arc::new(handler),
                        token,
                        name,
                        state,
                        delivery,
                        failure,
                        workers,
                    ))
                })
            }),
            meta,
        );
    }
}

impl<B, Source, Def, DecodeCodec, ReplySource, BatchReply, State> MountRoute<B, State>
    for BatchPublishingRoute<Source, Def, DecodeCodec, ReplySource>
where
    B: Broker + 'static,
    // The subscription side: batches open against the connected form.
    Source: SubscriptionSource<Connected<B>> + Send + 'static,
    Source::Subscriber: BatchSubscriber + Send + 'static,
    SourceMessage<B, Source>: Send + 'static,
    State: Send + Sync + 'static,
    Def: BatchPublishingCall<State> + 'static,
    Def::Input: crate::runtime::DecodeWith<DecodeCodec>,
    Def::Reply: Serialize + Send + Sync + 'static,
    DecodeCodec: Send + Sync + 'static,
    // The reply side: the source pairs at startup into a batch reply wiring (plain or
    // transactional).
    ReplySource: PublishPolicy<Connected<B>, Live = BatchReply> + Send + 'static,
    BatchReply: ReplyPublisher + 'static,
{
    fn mount_one<G, PP>(self, _global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Batch handlers are not wrapped by the per-message global stack, but they do pick up the
        // app's publish pipeline for their replies. The reply wiring pairs at startup.
        let pipeline = pipeline.clone();
        let Self {
            source,
            def,
            codec,
            publisher,
            meta,
            policies,
            workers,
        } = self;
        sink.push_paired_batch(
            source,
            async move |connected: Arc<Connected<B>>| {
                let publisher = publisher
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

/// A mountable group of handler registrations.
///
/// Mounting applies the app's global [`BlanketLayer`] to each handler and registers it, so the
/// app-wide [`layer`](crate::runtime::RustStream::layer) stack reaches router handlers.
/// Implemented by [`Router`](crate::runtime::Router) and its internal registration list; you
/// obtain one from a builder and pass it to
/// [`include_router`](crate::runtime::BrokerScope::include_router). You do not implement it.
///
/// `State` is the app's shared-state type: a router whose handlers read typed state is
/// `RouterDef<B, State>` only for that `State`, while a state-agnostic router is generic over it, so it
/// mounts on any app.
pub trait RouterDef<B: Broker, State = ()> {
    /// Applies `global` to every registration and pushes it into `sink`. Called by `include_router`.
    #[doc(hidden)]
    fn mount<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static;
}

/// Metadata collection over a router's registration list, independent of the app state type.
///
/// Split from [`RouterDef`] so [`Router::handlers`](crate::runtime::Router::handlers) does not have
/// to name the state type a stateful router's handlers read.
pub trait RouterHandlers {
    /// Appends each registration's metadata, in registration order.
    #[doc(hidden)]
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>);
}

impl<B: Broker + 'static, State> RouterDef<B, State> for () {
    fn mount<G, PP>(self, _global: &G, _pipeline: &PP, _sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
    }
}

impl RouterHandlers for () {
    fn collect_handlers(&self, _out: &mut Vec<HandlerMetadata>) {}
}

impl<B, Head, Tail, State> RouterDef<B, State> for (Head, Tail)
where
    B: Broker + 'static,
    Head: MountRoute<B, State>,
    Tail: RouterDef<B, State>,
{
    fn mount<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Registrations are prepended, so the tail holds the earlier ones; mount it first to keep
        // registration order.
        self.1.mount(global, pipeline, sink);
        self.0.mount_one(global, pipeline, sink);
    }
}

impl<Head, Tail> RouterHandlers for (Head, Tail)
where
    Head: RouteMeta,
    Tail: RouterHandlers,
{
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.1.collect_handlers(out);
        self.0.collect(out);
    }
}
