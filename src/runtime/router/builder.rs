//! The [`Router`] builder: chaining registrations, codecs and layers, and mounting the result.

use std::fmt;
use std::marker::PhantomData;

use crate::{BatchSubscriber, Broker, Connected, Subscriber, SubscriptionSource};

use crate::runtime::batch::{BatchDef, DeserializedBatch, TypedBatch, batch_metadata};
use crate::runtime::batch_inject::{BatchInjectDef, batch_inject_metadata};
use crate::runtime::batch_publishing::{BatchPublishingDef, batch_publishing_metadata};
use crate::runtime::dispatch::Workers;
use crate::runtime::failure::FailurePolicies;
use crate::runtime::handler::Handler;
use crate::runtime::inject::{InjectDef, inject_metadata};
use crate::runtime::input::{DecodeWith, Provided};
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity, Layer, Stack};
use crate::runtime::publish::{PublishIdentity, PublishPipeline};
use crate::runtime::publishing::{PublishingDef, publishing_metadata};
use crate::runtime::settings::BatchSized;
use crate::runtime::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::runtime::typed::Typed;

use super::routes::{
    BatchRoute, HandleRoute, MountRoute, RouteMeta, RouterDef, RouterHandlers, SubscribeRoute,
};
use super::routes_inject::{BatchInjectRoute, InjectRoute};
use super::routes_publish::{BatchPublishingRoute, PublishingRoute, RawReplyRoute};
use super::sink::RouterSink;
use super::{
    BatchInjectedRouter, BatchPublishingRouter, IncludedBatchRouter, IncludedRawBatchRouter,
    IncludedRouter, InjectedRouter, MergedRouter, PublishingRouter, RawReplyRouter,
};

/// A statically-typed, lazily-bound group of handler registrations, not attached to any broker.
///
/// Build it by chaining (each call consumes the router and returns a new type that carries the
/// added registration), then mount it with
/// [`include_router`](crate::runtime::BrokerScope::include_router). The registration list `Routes` is
/// an opaque nested tuple, so a builder function returns `impl RouterDef<B>` instead of naming the
/// type.
///
/// The codec parameter `C` is the codec the `include` family decodes with. It starts as `()`, meaning the [`DefaultCodec`](crate::codec::DefaultCodec); switch it
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
/// use ruststream::runtime::{Context, Handle, HandlerOutcome, Router, RouterDef, subscriber};
///
/// #[derive(serde::Deserialize, schemars::JsonSchema)]
/// struct Event {
///     id: u64,
/// }
///
/// struct Audit;
///
/// impl Handle<Event> for Audit {
///     async fn handle(
///         &self,
///         event: &Event,
///         _outs: &(),
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         let _ = event.id;
///         Ok(())
///     }
/// }
///
/// fn routes() -> impl RouterDef<MemoryBroker> {
///     Router::<MemoryBroker>::new().include(subscriber("events", Audit).build())
/// }
/// // later: app.with_broker(broker, |b| b.include_router(routes()));
/// # }
/// ```
pub struct Router<B, Routes = (), C = (), Layers = Identity, Pipe = PublishIdentity> {
    pub(super) routes: Routes,
    pub(super) codec: C,
    pub(super) layers: Layers,
    /// The publish pipeline this chain's [`Out`](crate::runtime::Out) slots send through, under
    /// each slot's own `.transform(..)` steps.
    ///
    /// A slot's pipeline is part of the instantiated definition's type, so it is fixed when the
    /// slot binds - which is at `include`, before an app exists. A router built on its own
    /// therefore carries [`PublishIdentity`] here and its slots publish with nothing in the way;
    /// the chain a [`BrokerScope`](crate::runtime::BrokerScope) drives carries the app's own
    /// pipeline, because there the app is already known. Replies are not affected either way:
    /// their publisher pairs at startup, so they travel the app's pipeline on both surfaces.
    pub(super) pipeline: Pipe,
    pub(super) _broker: PhantomData<fn() -> B>,
}

impl<B: Broker + 'static> Default for Router<B> {
    fn default() -> Self {
        Self {
            routes: (),
            codec: (),
            layers: Identity,
            pipeline: PublishIdentity,
            _broker: PhantomData,
        }
    }
}

impl<B, Routes, C, Layers, Pipe> fmt::Debug for Router<B, Routes, C, Layers, Pipe> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

impl<B: Broker + 'static, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (), RouteCodec, RouteLayers, RoutePipe>
{
    /// Adds a router-scope middleware layer, wrapping every handler in this router when the
    /// router is mounted. The first layer added runs outermost within the router; the app's
    /// global [`layer`](crate::runtime::RustStream::layer) stack wraps outside it.
    ///
    /// The layer must be a [`BlanketLayer`] (it applies to handlers whose concrete types the
    /// router hides), like the app-global stack, and it is declared before the router's first
    /// registration: after one, `.layer(..)` rides that registration instead
    /// ([`Router::layer`](Router::layer) on a registration), exactly as the other steps of the
    /// chain ride the position named before them.
    #[must_use]
    pub fn layer<N>(self, layer: N) -> Router<B, (), RouteCodec, Stack<N, RouteLayers>, RoutePipe> {
        Router {
            routes: self.routes,
            codec: self.codec,
            layers: Stack::new(layer, self.layers),
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B: Broker + 'static, RouteCodec, RoutePipe> Router<B, (), RouteCodec, Identity, RoutePipe> {
    /// The empty chain a [`BrokerScope`](crate::runtime::BrokerScope) drives one registration
    /// through: the scope's codec and its publish pipeline, with no router-scope layers of its
    /// own (the app's stack wraps at the drain, as it does for any router).
    pub(crate) fn for_scope(codec: RouteCodec, pipeline: RoutePipe) -> Self {
        Self {
            routes: (),
            codec,
            layers: Identity,
            pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B: Broker + 'static, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, Routes, RouteCodec, RouteLayers, RoutePipe>
{
    /// Sets the codec that subsequent `include` calls decode with, replacing the default.
    ///
    /// Registrations already in the chain keep the codec they were mounted with, so the codec can
    /// change mid-chain.
    #[must_use]
    pub fn with_codec<C>(self, codec: C) -> Router<B, Routes, C, RouteLayers, RoutePipe> {
        Router {
            routes: self.routes,
            codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Appends every registration of `other` after this router's own, keeping each router's codec
    /// and layer stack.
    ///
    /// The merged router's handlers stay wrapped by its own layers; this router's layers wrap
    /// around them as well once mounted (scopes nest).
    #[must_use]
    pub fn merge<R2, C2, L2, P2>(
        self,
        other: Router<B, R2, C2, L2, P2>,
    ) -> MergedRouter<B, R2, C2, L2, P2, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        L2: BlanketLayer,
    {
        Router {
            routes: (other, self.routes),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Attaches `handler` to an already-created `subscriber`. Machinery, not the user path: a
    /// service mounts definitions with [`include`](Router::include) and the value constructors
    /// ([`subscriber`](crate::runtime::subscriber), ...).
    ///
    /// The subscriber is created up front (before connect), the handler arrives fully wired (a
    /// decode adapter included, see [`typed`](crate::runtime::typed)), and the metadata is the
    /// caller's to assemble. That is the shape the runtime's own dispatch tests and a broker
    /// author holding a hand-built subscriber need, and nothing above it.
    #[allow(clippy::type_complexity)] // the grown chain's own type; an alias would hide the route
    pub fn handle<S, H>(
        self,
        subscriber: S,
        handler: H,
        meta: HandlerMetadata,
    ) -> Router<B, (HandleRoute<S, H>, Routes), RouteCodec, RouteLayers, RoutePipe>
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
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the plain and
    /// raw `include` forms.
    pub(crate) fn mount_subscriber<Source, Def, DecodeCodec>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) -> IncludedRouter<B, Source, Def, DecodeCodec, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: SubscriberDef,
        Def::Input: DecodeWith<DecodeCodec>,
        Def::Handler: 'static,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let handler = Typed::over(codec, def.into_handler()).on_decode_failure(policies.decode);
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
                    policies,
                    workers,
                    _context: PhantomData,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Mounts a batch definition on `source`, decoding with `codec`. The shared tail of the
    /// plain batch `include` form.
    pub(crate) fn mount_batch<Source, Def, DecodeCodec>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
    ) -> IncludedBatchRouter<B, Source, Def, DecodeCodec, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef + BatchSized,
        Def::Input: DecodeWith<DecodeCodec>,
        Def::Handler: 'static,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        // The handler bound alone cannot pin the kind, so the adapter names the def's input
        // kind explicitly.
        let handler = TypedBatch::<_, Def::Input, _, _>::over(codec, def.into_handler())
            .with_decode(policies.decode);
        Router {
            routes: (
                BatchRoute {
                    source,
                    handler,
                    meta,
                    policies,
                    workers,
                    batch_size,
                    _context: PhantomData,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Mounts a self-deserializing batch definition on `source`: each element constructs itself
    /// from its delivery's payload, so no codec takes part.
    pub(super) fn mount_raw_batch<Source, Def, F>(
        self,
        source: Source,
        def: Def,
    ) -> IncludedRawBatchRouter<B, Source, Def, F, RouteCodec, RouteLayers, RoutePipe, Routes>
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchDef<Input = Provided<F>> + BatchSized,
        Def::Handler: 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        let handler =
            DeserializedBatch::<_, F, _>::over(def.into_handler()).with_decode(policies.decode);
        Router {
            routes: (
                BatchRoute {
                    source,
                    handler,
                    meta,
                    policies,
                    workers,
                    batch_size,
                    _context: PhantomData,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Mounts an injected definition on `source`: its startup injections (an attached publish
    /// policy pairing into an `Out` parameter) resolve right after the subscription opens, so
    /// the handler holds live handles by construction. The tail of the `Out` form.
    pub(super) fn mount_inject<Source, Def, DecodeCodec, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        extra: Extra,
    ) -> InjectedRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: InjectDef + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = inject_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        Router {
            routes: (
                InjectRoute {
                    source,
                    def,
                    codec,
                    extra,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// The batch counterpart of [`mount_inject`](Self::mount_inject): the tail of the
    /// `BatchOut` form.
    pub(super) fn mount_batch_inject<Source, Def, DecodeCodec, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        extra: Extra,
    ) -> BatchInjectedRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchInjectDef + BatchSized + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        DecodeCodec: Send + Sync + 'static,
    {
        let meta = batch_inject_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        Router {
            routes: (
                BatchInjectRoute {
                    source,
                    def,
                    codec,
                    extra,
                    meta,
                    policies,
                    workers,
                    batch_size,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Mounts a batch publishing definition on `source`, decoding with `codec` and replying
    /// through the `publisher` policy, paired by the runtime after connect. The shared tail of
    /// the `BatchPublishing` and `BatchPublishingOut` forms.
    pub(super) fn mount_batch_publishing_source<Source, Def, DecodeCodec, ReplySource, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        publisher: ReplySource,
        extra: Extra,
    ) -> BatchPublishingRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: BatchSubscriber + Send + 'static,
        Def: BatchPublishingDef + BatchSized + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        DecodeCodec: Send + Sync + 'static,
        ReplySource: 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        let batch_size = def.batch_size();
        // Defer building the handler: the app's publish pipeline is only known at mount time and
        // the live reply publisher only exists once the broker connects, so mounting captures the
        // pieces in a starter that pairs and builds at startup (see `BatchPublishingRoute`),
        // letting a router-mounted batch publishing handler pick up the app-wide `publish_layer`
        // chain.
        Router {
            routes: (
                BatchPublishingRoute {
                    source,
                    def,
                    codec,
                    publisher,
                    extra,
                    meta,
                    policies,
                    workers,
                    batch_size,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// Mounts a publishing definition on `source` whose reply travels the encoded wiring: the
    /// shared tail of the `Publishing` and `PublishingOut` forms. See
    /// [`mount_batch_publishing_source`](Self::mount_batch_publishing_source) for why the
    /// handler is deferred.
    pub(super) fn mount_publishing_source<Source, Def, DecodeCodec, ReplySource, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        publisher: ReplySource,
        extra: Extra,
    ) -> PublishingRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: PublishingDef + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        // Not `Codec`: `DecodeWith` already carries what the input asks of it, and a byte
        // input asks for nothing - the route is built with `()` there.
        DecodeCodec: 'static,
        ReplySource: 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        Router {
            routes: (
                PublishingRoute {
                    source,
                    def,
                    codec,
                    publisher,
                    extra,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }

    /// The byte-reply counterpart of
    /// [`mount_publishing_source`](Self::mount_publishing_source): the reply bytes go out as-is
    /// through a bare publisher. The shared tail of the `RawReply` and `RawReplyOut` forms.
    pub(super) fn mount_raw_reply_source<Source, Def, DecodeCodec, ReplySource, Extra>(
        self,
        source: Source,
        def: Def,
        codec: DecodeCodec,
        publisher: ReplySource,
        extra: Extra,
    ) -> RawReplyRouter<
        B,
        Source,
        Def,
        DecodeCodec,
        ReplySource,
        Extra,
        RouteCodec,
        RouteLayers,
        RoutePipe,
        Routes,
    >
    where
        Source: SubscriptionSource<Connected<B>> + Send + 'static,
        Source::Subscriber: Send + 'static,
        Def: PublishingDef + 'static,
        Def::Input: DecodeWith<DecodeCodec>,
        // Not `Codec`: `DecodeWith` already carries what the input asks of it, and a byte
        // input asks for nothing - the route is built with `()` there.
        DecodeCodec: 'static,
        ReplySource: 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let policies = def.failure_policies();
        let workers = def.workers();
        Router {
            routes: (
                RawReplyRoute {
                    source,
                    def,
                    codec,
                    publisher,
                    extra,
                    meta,
                    policies,
                    workers,
                },
                self.routes,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B, S, H, Cx, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (SubscribeRoute<S, H, Cx>, Routes), RouteCodec, RouteLayers, RoutePipe>
{
    /// Sets the concurrency policy of the registration just added (the preceding `include`
    /// call), replacing its default.
    ///
    /// The functional-path counterpart of the macro's `workers(..)` clause: [`Workers::pool`]
    /// processes up to `n` deliveries of this subscriber concurrently, [`Workers::keyed`]
    /// dispatches over per-key sequential lanes. On an `include`d definition this overrides the
    /// attribute's `workers(..)` clause and the chained
    /// [`workers`](crate::runtime::SubscriberSettings::workers) setting alike.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # fn build() {
    /// use ruststream::nonzero;
    ///
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{Context, Handle, HandlerOutcome, Router, Workers, subscriber};
    ///
    /// # #[derive(serde::Deserialize, schemars::JsonSchema)]
    /// # struct Job { id: u64 }
    /// # struct Work;
    /// # impl Handle<Job> for Work {
    /// #     async fn handle(&self, _job: &Job, _outs: &(), _ctx: &mut Context<'_>) -> Result<(), HandlerOutcome> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// let router = Router::<MemoryBroker>::new()
    ///     .include(subscriber("jobs", Work).build())
    ///     .workers(Workers::keyed(nonzero!(4)));
    /// # }
    /// ```
    #[must_use]
    pub fn workers(mut self, workers: Workers) -> Self {
        self.routes.0.workers = workers;
        self
    }

    /// Wraps the registration just added (the preceding `include` call) with `layer`, outside its
    /// decode step, so the layer sees the raw delivery.
    ///
    /// The per-registration counterpart of the app-wide
    /// [`RustStream::layer`](crate::runtime::RustStream::layer) and the router-wide
    /// [`layer`](Router::layer) on a fresh router: those two wrap every handler in their scope, so
    /// they take a [`BlanketLayer`], which cannot be written for a layer fixed to one message
    /// type. This one wraps exactly one registration, whose handler type is still concrete here,
    /// so it takes an ordinary [`Layer`] - which is what a [`DynStack`](crate::runtime::DynStack)
    /// over the broker's own message type is.
    ///
    /// Which of the two a `.layer(..)` call is follows what the chain named before it, like every
    /// other step: on a router with no registrations yet it is the router's own, and after an
    /// `include` it is that registration's. It repeats: each call wraps what the calls before it
    /// produced, the last one outermost.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(all(feature = "memory", feature = "macros", feature = "json"))]
    /// # fn build() {
    /// use ruststream::memory::MemoryBroker;
    /// use ruststream::runtime::{HandlerOutcome, Router, layers::TracingLayer};
    /// use ruststream::subscriber;
    /// # #[derive(serde::Deserialize)]
    /// # struct Job { id: u64 }
    ///
    /// #[subscriber("jobs")]
    /// async fn work(job: &Job) -> HandlerOutcome {
    ///     let _ = job.id;
    ///     HandlerOutcome::ack()
    /// }
    ///
    /// let router = Router::<MemoryBroker>::new()
    ///     .include(work)
    ///     .layer(TracingLayer::default());
    /// # }
    /// ```
    #[must_use]
    // The call site reads `.layer(TracingLayer::default())`, so the layer travels by value like
    // every other builder argument; `Layer::layer` only borrows it.
    #[allow(clippy::needless_pass_by_value, clippy::type_complexity)]
    pub fn layer<N>(
        self,
        layer: N,
    ) -> Router<B, (SubscribeRoute<S, N::Handler, Cx>, Routes), RouteCodec, RouteLayers, RoutePipe>
    where
        N: Layer<H>,
    {
        let SubscribeRoute {
            source,
            handler,
            meta,
            policies,
            workers,
            _context: context,
        } = self.routes.0;
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler: layer.layer(handler),
                    meta,
                    policies,
                    workers,
                    _context: context,
                },
                self.routes.1,
            ),
            codec: self.codec,
            layers: self.layers,
            pipeline: self.pipeline,
            _broker: PhantomData,
        }
    }
}

impl<B, S, H, Cx, Routes, RouteCodec, RouteLayers, RoutePipe>
    Router<B, (BatchRoute<S, H, Cx>, Routes), RouteCodec, RouteLayers, RoutePipe>
{
    /// Sets the concurrency policy of the batch registration just added (the preceding batch
    /// `include` call), replacing its default.
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

impl<B: Broker + 'static, Routes: RouterHandlers, C, Layers, Pipe>
    Router<B, Routes, C, Layers, Pipe>
{
    /// Returns metadata for every registered handler, in registration order.
    #[must_use]
    pub fn handlers(&self) -> Vec<HandlerMetadata> {
        let mut out = Vec::new();
        self.routes.collect_handlers(&mut out);
        out
    }
}

/// Composes the mount-time global stack (outer) with a router's own layer stack (inner), owned
/// so publishing mounts can carry the composition into their startup pairing closures.
#[derive(Clone)]
struct ComposedBlanket<Outer, Inner> {
    outer: Outer,
    inner: Inner,
}

impl<Outer: BlanketLayer, Inner: BlanketLayer> BlanketLayer for ComposedBlanket<Outer, Inner> {
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

impl<B, Routes, C, Layers, Pipe, State> RouterDef<B, State> for Router<B, Routes, C, Layers, Pipe>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State>,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
{
    fn mount<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        let composed = ComposedBlanket {
            outer: global.clone(),
            inner: self.layers,
        };
        self.routes.mount(&composed, pipeline, sink);
    }
}

impl<B, Routes, C, Layers, Pipe> RouterHandlers for Router<B, Routes, C, Layers, Pipe>
where
    Routes: RouterHandlers,
{
    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}

// Lets a whole router be a single registration inside another router's list (`Router::merge`).
impl<B, Routes, C, Layers, Pipe, State> MountRoute<B, State> for Router<B, Routes, C, Layers, Pipe>
where
    B: Broker + 'static,
    Routes: RouterDef<B, State>,
    Layers: BlanketLayer + Clone + Send + Sync + 'static,
{
    fn mount_one<G, PP>(self, global: &G, pipeline: &PP, sink: &mut RouterSink<B, State>)
    where
        G: BlanketLayer + Clone + Send + Sync + 'static,
        PP: PublishPipeline + Clone + Send + 'static,
    {
        RouterDef::mount(self, global, pipeline, sink);
    }
}

// Lets a merged router contribute its registrations' metadata to the outer router's `handlers()`.
impl<B, Routes, C, Layers, Pipe> RouteMeta for Router<B, Routes, C, Layers, Pipe>
where
    Routes: RouterHandlers,
{
    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}
