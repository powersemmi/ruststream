//! The registration list: route types, the per-route mount trait and [`RouterDef`].

use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use crate::{BatchSubscriber, Broker, BuildContext, Connected, Subscriber, SubscriptionSource};

use crate::runtime::batch::BatchHandler;
use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::BlanketLayer;
use crate::runtime::publish::PublishPipeline;
use crate::runtime::redelivery::RetryPairing;
use crate::runtime::retry::RetryOpen;

use super::SourceMessage;
use super::sink::RouterSink;

/// One subscription registration: a source plus the handler it dispatches to. An implementation
/// detail of [`Router`](crate::runtime::Router)'s registration list.
///
/// `Cx` is the broker's typed per-delivery context the handler reads, carried so a definition
/// with a context of its own mounts on a router as it does on a scope.
#[doc(hidden)]
#[derive(Debug)]
pub struct SubscribeRoute<S, H, Cx = ()> {
    pub(super) source: S,
    pub(super) handler: H,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
    pub(super) _context: PhantomData<fn() -> Cx>,
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
///
/// `Cx` is the broker's subscription-scoped batch context the handler reads, carried explicitly
/// because the adapter handlers are generic over it.
#[doc(hidden)]
#[derive(Debug)]
pub struct BatchRoute<S, H, Cx = ()> {
    pub(super) source: S,
    pub(super) handler: H,
    pub(super) meta: HandlerMetadata,
    pub(super) policies: FailurePolicies,
    pub(super) workers: Workers,
    /// The size the subscription opens its batches at, from the registration's `batch(n)`.
    pub(super) batch_size: NonZeroUsize,
    pub(super) _context: PhantomData<fn() -> Cx>,
}

/// One mountable registration: applies the global blanket layer to its handler and registers it.
/// `State` is the app's shared-state type, threaded so a route only mounts on a sink whose state type
/// its handler matches (a state-agnostic handler matches any).
///
/// `retry` is the deferred-retry policy the mount site bound with `.out(Retry, policy)`, erased
/// against the broker; it reaches the starter, which pairs it once the broker is connected.
pub(super) trait MountRoute<B: Broker, State> {
    fn mount_one<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, State>,
        retry: Option<RetryPairing<B>>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static;
}

/// One registration with the deferred-retry position bound: the route, plus the pairing the
/// mount site's policy owes.
///
/// The wrapper is what makes the position bindable once - a bound registration is no longer
/// [`RetryOpen`], so a second `.out(Retry, ..)` has nothing to bind - and it keeps the pairing out
/// of every route type: a route that binds nothing carries nothing.
#[doc(hidden)]
pub struct RetriedRoute<Route, B: Broker> {
    route: Route,
    retry: RetryPairing<B>,
}

impl<Route, B: Broker> RetriedRoute<Route, B> {
    pub(super) fn new(route: Route, retry: RetryPairing<B>) -> Self {
        Self { route, retry }
    }
}

// The pairing is a closure with nothing to print, so the wrapper renders as the route it carries.
impl<Route: fmt::Debug, B: Broker> fmt::Debug for RetriedRoute<Route, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetriedRoute")
            .field("route", &self.route)
            .finish_non_exhaustive()
    }
}

impl<Route: RouteMeta, B: Broker> RouteMeta for RetriedRoute<Route, B> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.route.collect(out);
    }
}

// The wrapped route mounts as it always does, with the pairing the mount site bound: this is the
// one place a `Some` reaches the sink.
impl<B, Route, State> MountRoute<B, State> for RetriedRoute<Route, B>
where
    B: Broker + 'static,
    Route: MountRoute<B, State>,
{
    fn mount_one<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, State>,
        _retry: Option<RetryPairing<B>>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        self.route
            .mount_one(global, pipeline, sink, Some(self.retry));
    }
}

/// Implements [`RetryOpen`] for the routes a mount site produces: none has bound the
/// deferred-retry position, which is what `.out(Retry, policy)` asks.
macro_rules! impl_retry_open {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> RetryOpen for $route<$($param),+> {}
    )+};
}

impl_retry_open!(SubscribeRoute<S, H, Cx>, BatchRoute<S, H, Cx>);

/// One registration's `AsyncAPI` metadata, collected independently of the app state type (so
/// [`Router::handlers`](crate::runtime::Router::handlers) works whatever state the handlers read).
pub(super) trait RouteMeta {
    fn collect(&self, out: &mut Vec<HandlerMetadata>);
}

impl<S, H, Cx> RouteMeta for SubscribeRoute<S, H, Cx> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<S, H, Cx> RouteMeta for BatchRoute<S, H, Cx> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<S, H> RouteMeta for HandleRoute<S, H> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

impl<B, S, H, Cx, State> MountRoute<B, State> for SubscribeRoute<S, H, Cx>
where
    B: Broker + 'static,
    S: SubscriptionSource<Connected<B>> + Send + 'static,
    S::Subscriber: Send + 'static,
    SourceMessage<B, S>: Send + Sync + 'static,
    Cx: BuildContext<SourceMessage<B, S>> + Send + 'static,
    State: Send + Sync + 'static,
    H: Handler<SourceMessage<B, S>, Cx, State> + 'static,
{
    fn mount_one<G, PP>(
        self,
        global: &G,
        _pipeline: &PP,
        sink: &mut RouterSink<B, State>,
        retry: Option<RetryPairing<B>>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // The apply-and-push tail: the app's stack wraps through `BlanketLayer::apply`, whose
        // return type cannot be named, so this one step stays here rather than in a helper.
        let handler = global.apply::<SourceMessage<B, S>, Cx, State, H>(self.handler);
        sink.push_subscribe_workers(
            self.source,
            handler,
            self.meta,
            self.policies,
            self.workers,
            retry,
        );
    }
}

impl<B, S, H, Cx, State> MountRoute<B, State> for BatchRoute<S, H, Cx>
where
    B: Broker + 'static,
    S: SubscriptionSource<Connected<B>> + Send + 'static,
    S::Subscriber: BatchSubscriber + Send + 'static,
    SourceMessage<B, S>: Send + 'static,
    Cx: crate::BuildBatchContext<SourceMessage<B, S>> + Send + 'static,
    State: Send + Sync + 'static,
    H: BatchHandler<SourceMessage<B, S>, Cx, State> + 'static,
{
    fn mount_one<G, PP>(
        self,
        _global: &G,
        _pipeline: &PP,
        sink: &mut RouterSink<B, State>,
        retry: Option<RetryPairing<B>>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Per-message layers cannot wrap a whole-batch handler, so neither the app-global stack
        // nor the router's own layers apply to batch registrations.
        sink.push_subscribe_batch::<_, _, Cx>(
            self.source,
            self.handler,
            self.meta,
            self.policies,
            self.workers,
            self.batch_size,
            retry,
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
    fn mount_one<G, PP>(
        self,
        global: &G,
        _pipeline: &PP,
        sink: &mut RouterSink<B, State>,
        retry: Option<RetryPairing<B>>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let handler = global.apply::<S::Message, (), State, H>(self.handler);
        sink.push_handle(self.subscriber, handler, self.meta, self.policies, retry);
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
        // registration order. A route's own deferred-retry position rides the route (see
        // `RetriedRoute`), so the list has none of its own to hand down.
        self.1.mount(global, pipeline, sink);
        self.0.mount_one(global, pipeline, sink, None);
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
