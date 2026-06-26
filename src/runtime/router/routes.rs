//! The registration list: route types, the per-route mount trait and [`RouterDef`].

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Publisher, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchHandler;
use crate::runtime::batch_publishing::{BatchPublishingCall, BatchPublishingHandler};
use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::{PublishLayer, PublishPipeline, ReplyPublisher, TypedPublisher};
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
/// `St` is the app's shared-state type, threaded so a route only mounts on a sink whose state type
/// its handler matches (a state-agnostic handler matches any).
pub(super) trait MountRoute<B, St> {
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    );
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

impl<B, S, H, St> MountRoute<B, St> for SubscribeRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    St: Send + Sync + 'static,
    H: Handler<SourceMessage<B, S>, (), St> + 'static,
{
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        _pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    ) {
        let handler = global.apply::<SourceMessage<B, S>, (), St, H>(self.handler);
        sink.push_subscribe_workers(self.source, handler, self.meta, self.policies, self.workers);
    }
}

impl<B, S, H, St> MountRoute<B, St> for BatchRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: BatchSubscriber + Send + 'static,
    SourceMessage<B, S>: Send + 'static,
    St: Send + Sync + 'static,
    H: BatchHandler<SourceMessage<B, S>, St> + 'static,
{
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        _global: &G,
        _pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    ) {
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

impl<B, S, H, St> MountRoute<B, St> for HandleRoute<S, H>
where
    B: Broker + 'static,
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    St: Send + Sync + 'static,
    H: Handler<S::Message, (), St> + 'static,
{
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        _pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    ) {
        let handler = global.apply::<S::Message, (), St, H>(self.handler);
        sink.push_handle(self.subscriber, handler, self.meta, self.policies);
    }
}

/// One publishing registration, deferred. Unlike [`SubscribeRoute`], it stores the pieces of a
/// [`PublishingHandler`] rather than a built one, because the app's publish pipeline is only known
/// at mount time: [`mount_one`](MountRoute::mount_one) builds the handler with the real pipeline, so
/// a router-mounted publishing handler picks up the app-wide
/// [`publish_layer`](crate::runtime::RustStream::publish_layer) chain. An implementation detail of
/// [`Router`](crate::runtime::Router)'s registration list.
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

impl<B, S, D, C, P, PC, PL, St> MountRoute<B, St> for PublishingRoute<S, D, C, P, PC, PL>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    St: Send + Sync + 'static,
    D: PublishingCall<St> + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Reply: Serialize + Send + Sync + 'static,
    D::Context: crate::BuildContext<SourceMessage<B, S>> + Send + Sync + 'static,
    C: Codec + 'static,
    P: Publisher + 'static,
    PC: Codec + 'static,
    PL: PublishLayer<D::Context> + 'static,
{
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    ) {
        // Build the handler now that the app's pipeline is known, then wrap it with the global
        // blanket layer (as a directly-mounted publishing handler is).
        let handler = global.apply::<SourceMessage<B, S>, D::Context, St, _>(PublishingHandler {
            def: self.def,
            codec: self.codec,
            publisher: self.publisher,
            pipeline: pipeline.clone(),
            decode: self.policies.decode,
        });
        sink.push_subscribe_workers(self.source, handler, self.meta, self.policies, self.workers);
    }
}

impl<B, S, D, C, R, St> MountRoute<B, St> for BatchPublishingRoute<S, D, C, R>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: BatchSubscriber + Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    St: Send + Sync + 'static,
    D: BatchPublishingCall<St> + 'static,
    D::Input: DeserializeOwned + Send + Sync + 'static,
    D::Reply: Serialize + Send + Sync + 'static,
    C: Codec + 'static,
    R: ReplyPublisher + 'static,
{
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        _global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    ) {
        // Batch handlers are not wrapped by the per-message global stack, but they do pick up the
        // app's publish pipeline for their replies.
        let handler = BatchPublishingHandler {
            def: self.def,
            codec: self.codec,
            publisher: self.publisher,
            pipeline: pipeline.clone(),
            decode: self.policies.decode,
        };
        sink.push_subscribe_batch(self.source, handler, self.meta, self.policies, self.workers);
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
/// `St` is the app's shared-state type: a router whose handlers read typed state is
/// `RouterDef<B, St>` only for that `St`, while a state-agnostic router is generic over it, so it
/// mounts on any app.
pub trait RouterDef<B, St = ()> {
    /// Applies `global` to every registration and pushes it into `sink`. Called by `include_router`.
    #[doc(hidden)]
    fn mount<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    );
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

impl<B: Broker + 'static, St> RouterDef<B, St> for () {
    fn mount<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        _global: &G,
        _pipeline: &PP,
        _sink: &mut RouterSink<B, St>,
    ) {
    }
}

impl RouterHandlers for () {
    fn collect_handlers(&self, _out: &mut Vec<HandlerMetadata>) {}
}

impl<B, Head, Tail, St> RouterDef<B, St> for (Head, Tail)
where
    B: Broker + 'static,
    Head: MountRoute<B, St>,
    Tail: RouterDef<B, St>,
{
    fn mount<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, St>,
    ) {
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
