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
pub(super) trait MountRoute<B> {
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>);
    fn collect(&self, out: &mut Vec<HandlerMetadata>);
}

impl<B, S, H> MountRoute<B> for SubscribeRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    H: Handler<SourceMessage<B, S>> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        let handler = global.apply::<SourceMessage<B, S>, H>(self.handler);
        sink.push_subscribe_workers(self.source, handler, self.meta, self.policies, self.workers);
    }

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<B, S, H> MountRoute<B> for BatchRoute<S, H>
where
    B: Broker + 'static,
    S: SubscriptionSource<B> + Send + 'static,
    S::Subscriber: BatchSubscriber + Send + 'static,
    SourceMessage<B, S>: Send + 'static,
    H: BatchHandler<SourceMessage<B, S>> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, _global: &G, sink: &mut RouterSink<B>) {
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

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<B, S, H> MountRoute<B> for HandleRoute<S, H>
where
    B: Broker + 'static,
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    H: Handler<S::Message> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        let handler = global.apply::<S::Message, H>(self.handler);
        sink.push_handle(self.subscriber, handler, self.meta, self.policies);
    }

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

/// A mountable group of handler registrations.
///
/// Mounting applies the app's global [`BlanketLayer`] to each handler and registers it, so the
/// app-wide [`layer`](crate::runtime::RustStream::layer) stack reaches router handlers.
/// Implemented by [`Router`](crate::runtime::Router) and its internal registration list; you
/// obtain one from a builder and pass it to
/// [`include_router`](crate::runtime::BrokerScope::include_router). You do not implement it.
pub trait RouterDef<B> {
    /// Applies `global` to every registration and pushes it into `sink`. Called by `include_router`.
    #[doc(hidden)]
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>);

    /// Appends each registration's metadata, in registration order.
    #[doc(hidden)]
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>);
}

impl<B: Broker + 'static> RouterDef<B> for () {
    fn mount<G: BlanketLayer>(self, _global: &G, _sink: &mut RouterSink<B>) {}
    fn collect_handlers(&self, _out: &mut Vec<HandlerMetadata>) {}
}

impl<B, Head, Tail> RouterDef<B> for (Head, Tail)
where
    B: Broker + 'static,
    Head: MountRoute<B>,
    Tail: RouterDef<B>,
{
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        // Registrations are prepended, so the tail holds the earlier ones; mount it first to keep
        // registration order.
        self.1.mount(global, sink);
        self.0.mount_one(global, sink);
    }

    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.1.collect_handlers(out);
        self.0.collect(out);
    }
}
