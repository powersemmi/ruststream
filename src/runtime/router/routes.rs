//! The registration list: route types, the per-route mount trait and [`RouterDef`].

use crate::{BatchSubscriber, Broker, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchHandler;
use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::BlanketLayer;

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
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B, St>);
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
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B, St>) {
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
    H: BatchHandler<SourceMessage<B, S>> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, _global: &G, sink: &mut RouterSink<B, St>) {
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
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B, St>) {
        let handler = global.apply::<S::Message, (), St, H>(self.handler);
        sink.push_handle(self.subscriber, handler, self.meta, self.policies);
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
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B, St>);
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
    fn mount<G: BlanketLayer>(self, _global: &G, _sink: &mut RouterSink<B, St>) {}
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
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B, St>) {
        // Registrations are prepended, so the tail holds the earlier ones; mount it first to keep
        // registration order.
        self.1.mount(global, sink);
        self.0.mount_one(global, sink);
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
