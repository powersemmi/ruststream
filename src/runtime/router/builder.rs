//! The [`Router`] builder: chaining registrations, codecs and layers, and mounting the result.

use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Connected, Publisher, Subscriber, SubscriptionSource};

use crate::runtime::batch::{BatchDef, SliceHandler, batch_metadata, typed_batch};
use crate::runtime::batch_publishing::{BatchPublishingDef, batch_publishing_metadata};
use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity, Stack};
use crate::runtime::publish::{PublishPipeline, PublishTransform, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingDef, publishing_metadata};
use crate::runtime::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::runtime::typed::typed;

use super::routes::{
    BatchPublishingRoute, BatchRoute, HandleRoute, MountRoute, PublishingRoute, RouteMeta,
    RouterDef, RouterHandlers, SubscribeRoute,
};
use super::sink::RouterSink;
use super::{
    BatchPublishingRouter, IncludedBatchRouter, IncludedRouter, MergedRouter, PublishingRouter,
    SourceMessage, SubscribedBatchRouter,
};

/// A statically-typed, lazily-bound group of handler registrations, not attached to any broker.
///
/// Build it by chaining (each call consumes the router and returns a new type that carries the
/// added registration), then mount it with
/// [`include_router`](crate::runtime::BrokerScope::include_router). The registration list `Routes` is
/// an opaque nested tuple, so a builder function returns `impl RouterDef<B>` instead of naming the
/// type.
///
/// The codec parameter `C` is the codec `include` / `include_on` / `include_publishing*` decode
/// with. It starts as `()`, meaning the [`DefaultCodec`](crate::codec::DefaultCodec); switch it
/// for the rest of the chain with [`with_codec`](Self::with_codec).
///
/// The layer parameter `Layers` is the router's own middleware stack, grown with
/// [`layer`](Self::layer). It wraps every handler in the router when the router is mounted, inside
/// the app's global stack.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # fn build() {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, Router, RouterDef};
/// use ruststream::Name;
///
/// fn routes() -> impl RouterDef<MemoryBroker> {
///     Router::<MemoryBroker>::new().subscribe(
///         Name::new("events"),
///         |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
///         HandlerMetadata::raw("events"),
///     )
/// }
/// // later: app.with_broker(broker, |b| b.include_router(routes()));
/// # }
/// ```
pub struct Router<B, Routes = (), C = (), Layers = Identity> {
    pub(super) routes: Routes,
    pub(super) codec: C,
    pub(super) layers: Layers,
    pub(super) _broker: PhantomData<fn() -> B>,
}

impl<B: Broker + 'static> Default for Router<B, (), (), Identity> {
    fn default() -> Self {
        Self {
            routes: (),
            codec: (),
            layers: Identity,
            _broker: PhantomData,
        }
    }
}

impl<B, Routes, C, Layers> std::fmt::Debug for Router<B, Routes, C, Layers> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Router").finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Router<B, ()> {
    /// Creates an empty router decoding with the [`DefaultCodec`](crate::codec::DefaultCodec).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers>
    Router<B, Routes, RouteCodec, RouteLayers>
{
    /// Sets the codec that subsequent `include` / `include_on` / `include_publishing*` calls
    /// decode with, replacing the default.
    ///
    /// Registrations already in the chain keep the codec they were mounted with, so the codec can
    /// change mid-chain.
    #[must_use]
    pub fn with_codec<C>(self, codec: C) -> Router<B, Routes, C, RouteLayers> {
        Router {
            routes: self.routes,
            codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Adds a router-scope middleware layer, wrapping every handler in this router (regardless of
    /// registration order) when the router is mounted. The first layer added runs outermost within
    /// the router; the app's global [`layer`](crate::runtime::RustStream::layer) stack wraps
    /// outside it.
    ///
    /// The layer must be a [`BlanketLayer`] (it applies to handlers whose concrete types the
    /// router hides), like the app-global stack.
    #[must_use]
    pub fn layer<N>(self, layer: N) -> Router<B, Routes, RouteCodec, Stack<N, RouteLayers>> {
        Router {
            routes: self.routes,
            codec: self.codec,
            layers: Stack::new(layer, self.layers),
            _broker: PhantomData,
        }
    }

    /// Appends every registration of `other` after this router's own, keeping each router's codec
    /// and layer stack.
    ///
    /// The merged router's handlers stay wrapped by its own layers; this router's layers wrap
    /// around them as well once mounted (scopes nest).
    #[must_use]
    pub fn merge<R2, C2, L2>(
        self,
        other: Router<B, R2, C2, L2>,
    ) -> MergedRouter<B, R2, C2, L2, RouteCodec, RouteLayers, Routes>
    where
        L2: BlanketLayer,
    {
        Router {
            routes: (other, self.routes),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Attaches `handler` to an already-created `subscriber`.
    ///
    /// The subscriber is created up front (before connect). Use this for brokers whose subscription
    /// does not need a live connection, or when you already hold a subscriber.
    pub fn handle<S, H>(
        self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> Router<B, (HandleRoute<S, H>, Routes), RouteCodec, RouteLayers>
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        Router {
            routes: (
                HandleRoute {
                    subscriber,
                    handler,
                    meta,
                    policies: FailurePolicies::default(),
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Attaches `handler` to a subscription described by `source`.
    ///
    /// The subscription is opened when the application runs, after the broker is connected, so this
    /// is the path that works for brokers requiring a live connection to subscribe.
    pub fn subscribe<S, H>(
        self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> Router<B, (SubscribeRoute<S, H>, Routes), RouteCodec, RouteLayers>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<SourceMessage<B, S>> + 'static,
    {
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
                    policies: FailurePolicies::default(),
                    workers: Workers::sequential(),
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Wraps `handler` in a [`TypedBatch`](crate::runtime::TypedBatch) decoding with `codec` and
    /// prepends the batch registration. The shared tail of the `subscribe_batch` forms.
    pub(super) fn push_batch_route<S, T, C, H>(
        self,
        source: S,
        handler: H,
        codec: C,
        meta: HandlerMetadata,
    ) -> SubscribedBatchRouter<B, S, T, C, H, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        T: DeserializeOwned + Send + Sync + 'static,
        C: Codec + 'static,
        H: SliceHandler<T> + 'static,
    {
        Router {
            routes: (
                BatchRoute {
                    source,
                    handler: typed_batch(codec, handler),
                    meta,
                    policies: FailurePolicies::default(),
                    workers: Workers::sequential(),
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the
    /// `include` / `include_on` forms.
    pub(super) fn mount_subscriber<S, D, C>(
        self,
        source: S,
        def: D,
        codec: C,
    ) -> IncludedRouter<B, S, D, C, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = typed(codec, def.into_handler()).on_decode_failure(policies.decode);
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Mounts a batch definition on `source`, decoding with `codec`. The shared tail of the
    /// `include_batch` / `include_batch_on` forms.
    pub(super) fn mount_batch<S, D, C>(
        self,
        source: S,
        def: D,
        codec: C,
    ) -> IncludedBatchRouter<B, S, D, C, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = typed_batch(codec, def.into_handler()).with_decode(policies.decode);
        Router {
            routes: (
                BatchRoute {
                    source,
                    handler,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Mounts a batch publishing definition on `source`, decoding with `codec` and replying
    /// through `publisher`. The shared tail of the `include_batch_publishing` /
    /// `include_batch_publishing_on` forms.
    pub(super) fn mount_batch_publishing<S, D, C, RP>(
        self,
        source: S,
        def: D,
        codec: C,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, D, C, RP, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        RP: ReplyPublisher + 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        // Defer building the handler: the app's publish pipeline is only known at mount time, so the
        // reply pipeline is injected then (see `BatchPublishingRoute`), letting a router-mounted
        // batch publishing handler pick up the app-wide `publish_layer` chain.
        Router {
            routes: (
                BatchPublishingRoute {
                    source,
                    def,
                    codec,
                    publisher,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }

    /// Mounts a publishing definition on `source`, decoding with `codec` and replying through
    /// `publisher`. The shared tail of the `include_publishing` / `include_publishing_on` forms.
    pub(super) fn mount_publishing<S, D, C, P, PC, PL>(
        self,
        source: S,
        def: D,
        codec: C,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, S, D, C, P, PC, PL, RouteCodec, RouteLayers, Routes>
    where
        S: SubscriptionSource<Connected<B>> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishTransform<D::Context> + 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        // Defer building the handler: the app's publish pipeline is only known at mount time, so the
        // reply pipeline is injected then (see `PublishingRoute`), letting a router-mounted
        // publishing handler pick up the app-wide `publish_layer` chain.
        Router {
            routes: (
                PublishingRoute {
                    source,
                    def,
                    codec,
                    publisher,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            _broker: PhantomData,
        }
    }
}

impl<B, S, H, Routes, RouteCodec, RouteLayers>
    Router<B, (SubscribeRoute<S, H>, Routes), RouteCodec, RouteLayers>
{
    /// Sets the concurrency policy of the registration just added (the preceding `subscribe` /
    /// `include` call), replacing its default.
    ///
    /// The functional-path counterpart of the macro's `workers(..)` clause: [`Workers::pool`]
    /// processes up to `n` deliveries of this subscriber concurrently, [`Workers::keyed`]
    /// dispatches over per-key sequential lanes. On an `include`d definition this overrides the
    /// attribute's `workers(..)` clause.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "memory")]
    /// # fn build() {
    /// use ruststream::nonzero;
    ///
    /// use ruststream::Name;
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, Router, Workers};
    ///
    /// let router = Router::<MemoryBroker>::new()
    ///     .subscribe(
    ///         Name::new("jobs"),
    ///         |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
    ///         HandlerMetadata::raw("jobs"),
    ///     )
    ///     .workers(Workers::pool(nonzero!(4)));
    /// # }
    /// ```
    #[must_use]
    pub fn workers(mut self, workers: Workers) -> Self {
        self.routes.0.workers = workers;
        self
    }
}

impl<B, S, H, Routes, RouteCodec, RouteLayers>
    Router<B, (BatchRoute<S, H>, Routes), RouteCodec, RouteLayers>
{
    /// Sets the concurrency policy of the batch registration just added (the preceding
    /// `subscribe_batch` / `include_batch` call), replacing its default.
    ///
    /// [`Workers::pool`] keeps up to `n` batches in flight at once. Keyed lanes order single
    /// messages per key and do not apply to batches: a [`Workers::keyed`] policy here behaves
    /// like a plain pool of the same size.
    #[must_use]
    pub fn workers(mut self, workers: Workers) -> Self {
        self.routes.0.workers = workers;
        self
    }
}

impl<B: Broker + 'static, Routes: RouterHandlers, C, Layers> Router<B, Routes, C, Layers> {
    /// Returns metadata for every registered handler, in registration order.
    #[must_use]
    pub fn handlers(&self) -> Vec<HandlerMetadata> {
        let mut out = Vec::new();
        self.routes.collect_handlers(&mut out);
        out
    }
}

/// Composes the mount-time global stack (outer) with a router's own layer stack (inner) by
/// reference, so [`RouterDef::mount`] can pass both down without cloning either.
struct ComposedBlanket<'a, Outer, Inner> {
    outer: &'a Outer,
    inner: &'a Inner,
}

impl<Outer: BlanketLayer, Inner: BlanketLayer> BlanketLayer for ComposedBlanket<'_, Outer, Inner> {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        self.outer
            .apply::<M, C, S, _>(self.inner.apply::<M, C, S, _>(handler))
    }
}

impl<B, Routes, C, Layers, State> RouterDef<B, State> for Router<B, Routes, C, Layers>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State>,
    Layers: BlanketLayer,
{
    fn mount<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, State>,
    ) {
        let composed = ComposedBlanket {
            outer: global,
            inner: &self.layers,
        };
        self.routes.mount(&composed, pipeline, sink);
    }
}

impl<B, Routes, C, Layers> RouterHandlers for Router<B, Routes, C, Layers>
where
    Routes: RouterHandlers,
{
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}

// Lets a whole router be a single registration inside another router's list (`Router::merge`).
impl<B, Routes, C, Layers, State> MountRoute<B, State> for Router<B, Routes, C, Layers>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State>,
    Layers: BlanketLayer,
{
    fn mount_one<G: BlanketLayer, PP: PublishPipeline + Clone + 'static>(
        self,
        global: &G,
        pipeline: &PP,
        sink: &mut RouterSink<B, State>,
    ) {
        RouterDef::mount(self, global, pipeline, sink);
    }
}

// Lets a merged router contribute its registrations' metadata to the outer router's `handlers()`.
impl<B, Routes, C, Layers> RouteMeta for Router<B, Routes, C, Layers>
where
    Routes: RouterHandlers,
{
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}
