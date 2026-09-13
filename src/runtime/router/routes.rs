//! The registration list: route types, the per-route mount trait and [`RouterDef`].

use std::borrow::Cow;
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
use crate::runtime::publish::{PublishIdentity, PublishPipeline};
use crate::runtime::redelivery::{CopyPathAddress, CopyPathPairing, RetryPairing, RetrySetup};
use crate::runtime::retry::OpenDestination;
use crate::runtime::retry::RetryOpen;
use crate::{CopyPath, RetryDeclaration};

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
/// `setup` is what the mount chain declared about this registration's retries: the publisher a
/// `.out(Retry, policy)` named, erased against the broker, and the cap and destination the
/// declaration steps carried. A registration that named no publisher gets the broker's default
/// here, from `RetryPipeline`, which is the publish path a retry copy travels - the same one the
/// named policy would have travelled.
pub(super) trait MountRoute<B: Broker, State, RetryPipeline> {
    /// The broker's typed per-delivery context this registration's handler reads, which is also
    /// what a transform on its retry position reads.
    type Context;

    fn mount_one<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
        setup: RetrySetup<B, Self::Context>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static;
}

/// The subscription descriptor one registration mounts on, read off its route so the mount chain
/// can ask what that descriptor declares about its retry copies. Machinery; never named directly.
#[doc(hidden)]
pub trait RouteSubscription<B: Broker> {
    /// The descriptor, carried so a compile error about the copy path can name it.
    type Source;

    /// What it declares: [`AddressedCopies`](crate::AddressedCopies),
    /// [`NamedCopies`](crate::NamedCopies) or [`BrokerMoves`](crate::BrokerMoves).
    type Copies: CopyPath;

    /// The broker's typed per-delivery context the registration's handler reads.
    type Context;

    /// Whether the registration has already been told where its retry copies go.
    type Destination;
}

/// One registration's metadata, reachable while the router still holds the route: how a
/// declaration writes its dead-letter destination into the `AsyncAPI` document. Machinery; never
/// named directly.
#[doc(hidden)]
pub trait RouteMetadata {
    /// The metadata this registration was built with.
    fn metadata_mut(&mut self) -> &mut HandlerMetadata;
}

/// One registration with the deferred-retry position bound: the route, plus the pairing the
/// mount site's policy owes.
///
/// The wrapper is what makes the position bindable once - a bound registration is no longer
/// [`RetryOpen`], so a second `.out(Retry, ..)` has nothing to bind - and it keeps the pairing out
/// of every route type: a route that binds nothing carries nothing.
#[doc(hidden)]
pub struct RetriedRoute<Route, B: Broker, Cx> {
    route: Route,
    retry: RetryPairing<B, Cx>,
    destination: Option<Cow<'static, str>>,
    named_per_delivery: bool,
}

impl<Route, B: Broker, Cx> RetriedRoute<Route, B, Cx> {
    pub(super) fn new(
        route: Route,
        retry: RetryPairing<B, Cx>,
        destination: Option<Cow<'static, str>>,
        named_per_delivery: bool,
    ) -> Self {
        Self {
            route,
            retry,
            destination,
            named_per_delivery,
        }
    }
}

/// One registration carrying the retry declaration its mount chain made: the route, plus the cap
/// and the destination that reach the subscription descriptor at startup.
///
/// The wrapper is what makes each declaration step bindable once and what keeps the declaration
/// out of every route type: a route that declares nothing carries nothing.
#[doc(hidden)]
#[derive(Debug)]
pub struct DeclaredRoute<Route, Dest = OpenDestination> {
    route: Route,
    declaration: RetryDeclaration,
    destination: Option<Cow<'static, str>>,
    _dest: PhantomData<fn() -> Dest>,
}

impl<Route, Dest> DeclaredRoute<Route, Dest> {
    pub(super) fn new(
        route: Route,
        declaration: RetryDeclaration,
        destination: Option<Cow<'static, str>>,
    ) -> Self {
        Self {
            route,
            declaration,
            destination,
            _dest: PhantomData,
        }
    }
}

impl<Route: RouteMeta, Dest> RouteMeta for DeclaredRoute<Route, Dest> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.route.collect(out);
    }
}

impl<Route: RouteMetadata, Dest> RouteMetadata for DeclaredRoute<Route, Dest> {
    fn metadata_mut(&mut self) -> &mut HandlerMetadata {
        self.route.metadata_mut()
    }
}

impl<Route: RetryOpen, Dest> RetryOpen for DeclaredRoute<Route, Dest> {}

impl<B: Broker, Route: RouteSubscription<B>, Dest> RouteSubscription<B>
    for DeclaredRoute<Route, Dest>
{
    type Source = Route::Source;
    type Copies = Route::Copies;
    type Context = Route::Context;
    type Destination = Dest;
}

// The wrapped route mounts as it always does, with the declaration the mount site made: this is
// the one place a declaration reaches the sink.
impl<B, Route, State, RetryPipeline, Dest> MountRoute<B, State, RetryPipeline>
    for DeclaredRoute<Route, Dest>
where
    B: Broker + 'static,
    Route: MountRoute<B, State, RetryPipeline>,
{
    type Context = Route::Context;

    fn mount_one<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
        setup: RetrySetup<B, Route::Context>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        self.route.mount_one(
            global,
            pipeline,
            retry_pipeline,
            sink,
            setup.with_declaration(self.declaration, self.destination),
        );
    }
}

// The pairing is a closure with nothing to print, so the wrapper renders as the route it carries.
impl<Route: fmt::Debug, B: Broker, Cx> fmt::Debug for RetriedRoute<Route, B, Cx> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetriedRoute")
            .field("route", &self.route)
            .finish_non_exhaustive()
    }
}

impl<Route: RouteMeta, B: Broker, Cx> RouteMeta for RetriedRoute<Route, B, Cx> {
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.route.collect(out);
    }
}

impl<Route: RouteMetadata, B: Broker, Cx> RouteMetadata for RetriedRoute<Route, B, Cx> {
    fn metadata_mut(&mut self) -> &mut HandlerMetadata {
        self.route.metadata_mut()
    }
}

// The wrapped route mounts as it always does, with the pairing the mount site bound: this is the
// one place a `Some` reaches the sink.
impl<B, Route, State, RetryPipeline, Cx> MountRoute<B, State, RetryPipeline>
    for RetriedRoute<Route, B, Cx>
where
    B: Broker + 'static,
    Route: MountRoute<B, State, RetryPipeline, Context = Cx>,
{
    type Context = Cx;

    fn mount_one<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
        setup: RetrySetup<B, Cx>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        self.route.mount_one(
            global,
            pipeline,
            retry_pipeline,
            sink,
            setup.with_publisher(self.retry, self.destination, self.named_per_delivery),
        );
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

/// Implements [`RouteMetadata`] for the routes that carry their metadata in a `meta` field, which
/// is every route a mount site produces.
macro_rules! impl_route_metadata {
    ($($route:ident<$($param:ident),+>),+ $(,)?) => {$(
        impl<$($param),+> RouteMetadata for $route<$($param),+> {
            fn metadata_mut(&mut self) -> &mut HandlerMetadata {
                &mut self.meta
            }
        }
    )+};
}

impl_route_metadata!(
    SubscribeRoute<S, H, Cx>,
    BatchRoute<S, H, Cx>,
    HandleRoute<S, H>,
);

/// Reads the subscription's declarations off the routes that mount on a descriptor.
macro_rules! impl_route_subscription {
    ($($route:ident<$source:ident, $handler:ident, $context:ident>),+ $(,)?) => {$(
        impl<B, $source, $handler, $context> RouteSubscription<B>
            for $route<$source, $handler, $context>
        where
            B: Broker,
            $source: SubscriptionSource<Connected<B>>,
        {
            type Source = $source;
            type Copies = <$source as SubscriptionSource<Connected<B>>>::Copies;
            type Context = $context;
            type Destination = OpenDestination;
        }
    )+};
}

impl_route_subscription!(SubscribeRoute<S, H, Cx>, BatchRoute<S, H, Cx>);

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

impl<B, S, H, Cx, State, RetryPipeline> MountRoute<B, State, RetryPipeline>
    for SubscribeRoute<S, H, Cx>
where
    B: Broker + 'static,
    S: SubscriptionSource<Connected<B>> + Send + 'static,
    S::Subscriber: Send + 'static,
    S::Copies: CopyPathPairing<B, Cx, RetryPipeline> + CopyPathAddress<Connected<B>, S>,
    SourceMessage<B, S>: Send + Sync + 'static,
    Cx: BuildContext<SourceMessage<B, S>> + Send + Sync + 'static,
    State: Send + Sync + 'static,
    H: Handler<SourceMessage<B, S>, Cx, State> + 'static,
    RetryPipeline: Clone,
{
    type Context = Cx;

    fn mount_one<G, PP>(
        self,
        global: &G,
        _pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
        setup: RetrySetup<B, Cx>,
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
            setup.resolve::<S::Copies, _>(retry_pipeline),
        );
    }
}

impl<B, S, H, Cx, State, RetryPipeline> MountRoute<B, State, RetryPipeline> for BatchRoute<S, H, Cx>
where
    B: Broker + 'static,
    S: SubscriptionSource<Connected<B>> + Send + 'static,
    S::Subscriber: BatchSubscriber + Send + 'static,
    S::Copies: CopyPathPairing<B, Cx, RetryPipeline> + CopyPathAddress<Connected<B>, S>,
    SourceMessage<B, S>: Send + 'static,
    Cx: crate::BuildBatchContext<SourceMessage<B, S>> + Send + Sync + 'static,
    State: Send + Sync + 'static,
    H: BatchHandler<SourceMessage<B, S>, Cx, State> + 'static,
    RetryPipeline: Clone,
{
    type Context = Cx;

    fn mount_one<G, PP>(
        self,
        _global: &G,
        _pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
        setup: RetrySetup<B, Cx>,
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
            setup.resolve::<S::Copies, _>(retry_pipeline),
        );
    }
}

impl<B, S, H, State, RetryPipeline> MountRoute<B, State, RetryPipeline> for HandleRoute<S, H>
where
    B: Broker + 'static,
    S: Subscriber + Send + 'static,
    S::Message: Send + Sync + 'static,
    State: Send + Sync + 'static,
    H: Handler<S::Message, (), State> + 'static,
{
    type Context = ();

    fn mount_one<G, PP>(
        self,
        global: &G,
        _pipeline: &PP,
        _retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
        _setup: RetrySetup<B, ()>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let handler = global.apply::<S::Message, (), State, H>(self.handler);
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
/// `State` is the app's shared-state type: a router whose handlers read typed state is
/// `RouterDef<B, State>` only for that `State`, while a state-agnostic router is generic over it, so it
/// mounts on any app.
///
/// `RetryPipeline` is the publish path a registration's retry copies travel: a router hands its
/// own down to its registrations, and the parameter defaults to
/// [`PublishIdentity`](crate::runtime::PublishIdentity), which is the path of a router built
/// without publish middleware - so `RouterDef<B>` names what a builder function returns.
pub trait RouterDef<B: Broker, State = (), RetryPipeline = PublishIdentity> {
    /// Applies `global` to every registration and pushes it into `sink`. Called by `include_router`.
    #[doc(hidden)]
    fn mount<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
    ) where
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

impl<B: Broker + 'static, State, RetryPipeline> RouterDef<B, State, RetryPipeline> for () {
    fn mount<G, PP>(
        self,
        _global: &G,
        _pipeline: &PP,
        _retry_pipeline: &RetryPipeline,
        _sink: &mut RouterSink<B, State>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
    }
}

impl RouterHandlers for () {
    fn collect_handlers(&self, _out: &mut Vec<HandlerMetadata>) {}
}

impl<B, Head, Tail, State, RetryPipeline> RouterDef<B, State, RetryPipeline> for (Head, Tail)
where
    B: Broker + 'static,
    Head: MountRoute<B, State, RetryPipeline>,
    Tail: RouterDef<B, State, RetryPipeline>,
{
    fn mount<G, PP>(
        self,
        global: &G,
        pipeline: &PP,
        retry_pipeline: &RetryPipeline,
        sink: &mut RouterSink<B, State>,
    ) where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        // Registrations are prepended, so the tail holds the earlier ones; mount it first to keep
        // registration order. A route's own retry publisher and declaration ride the route (see
        // `RetriedRoute` and `DeclaredRoute`), so the list has none of its own to hand down.
        self.1.mount(global, pipeline, retry_pipeline, sink);
        self.0.mount_one(
            global,
            pipeline,
            retry_pipeline,
            sink,
            RetrySetup::default(),
        );
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
