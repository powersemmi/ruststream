//! The [`Router`] builder: chaining registrations, codecs and layers, and mounting the result.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{BatchSubscriber, Broker, Publisher, Subscriber, SubscriptionSource};

use crate::runtime::batch::{BatchDef, batch_metadata, typed_batch};
use crate::runtime::batch_publishing::{
    BatchPublishingDef, BatchPublishingHandler, batch_publishing_metadata,
};
use crate::runtime::dispatch::Workers;
use crate::runtime::handler::Handler;
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{BlanketLayer, Identity, Stack};
use crate::runtime::publish::{PublishLayer, PublishMiddleware, ReplyPublisher, TypedPublisher};
use crate::runtime::publishing::{PublishingDef, PublishingHandler, publishing_metadata};
use crate::runtime::subscriber_def::{SubscriberDef, subscriber_metadata};
use crate::runtime::typed::typed;

use super::routes::{BatchRoute, HandleRoute, MountRoute, RouterDef, SubscribeRoute};
use super::sink::RouterSink;
use super::{
    BatchPublishingRouter, IncludedBatchRouter, IncludedRouter, MergedRouter, PublishingRouter,
    SourceMessage,
};

/// A statically-typed, lazily-bound group of handler registrations, not attached to any broker.
///
/// Build it by chaining (each call consumes the router and returns a new type that carries the
/// added registration), then mount it with
/// [`include_router`](crate::runtime::BrokerScope::include_router). The registration list `R` is
/// an opaque nested tuple, so a builder function returns `impl RouterDef<B>` instead of naming the
/// type.
///
/// The codec parameter `C` is the codec `include` / `include_on` / `include_publishing*` decode
/// with. It starts as `()`, meaning the [`DefaultCodec`](crate::codec::DefaultCodec); switch it
/// for the rest of the chain with [`with_codec`](Self::with_codec).
///
/// The layer parameter `L` is the router's own middleware stack, grown with
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
pub struct Router<B, R = (), C = (), L = Identity> {
    pub(super) routes: R,
    pub(super) codec: C,
    pub(super) layers: L,
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

impl<B, R, C, L> std::fmt::Debug for Router<B, R, C, L> {
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

impl<B: Broker + 'static, R, RC, RL> Router<B, R, RC, RL> {
    /// Sets the codec that subsequent `include` / `include_on` / `include_publishing*` calls
    /// decode with, replacing the default.
    ///
    /// Registrations already in the chain keep the codec they were mounted with, so the codec can
    /// change mid-chain.
    #[must_use]
    pub fn with_codec<C>(self, codec: C) -> Router<B, R, C, RL> {
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
    pub fn layer<N>(self, layer: N) -> Router<B, R, RC, Stack<N, RL>> {
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
    ) -> MergedRouter<B, R2, C2, L2, RC, RL, R>
    where
        R2: RouterDef<B>,
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
    ) -> Router<B, (HandleRoute<S, H>, R), RC, RL>
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
    ) -> Router<B, (SubscribeRoute<S, H>, R), RC, RL>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<SourceMessage<B, S>> + 'static,
    {
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
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
    ) -> IncludedRouter<B, S, D, C, RC, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let workers = def.workers();
        let handler = typed(codec, def.into_handler());
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
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
    ) -> IncludedBatchRouter<B, S, D, C, RC, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
    {
        let meta = batch_metadata(source.name().to_owned(), &def);
        let workers = def.workers();
        let handler = typed_batch(codec, def.into_handler());
        Router {
            routes: (
                BatchRoute {
                    source,
                    handler,
                    meta,
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
    ) -> BatchPublishingRouter<B, S, D, C, RP, RC, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        RP: ReplyPublisher + 'static,
    {
        let meta = batch_publishing_metadata(source.name().to_owned(), &def);
        let workers = def.workers();
        let pipeline: Arc<[Arc<dyn PublishMiddleware>]> = Arc::from([]);
        let handler = BatchPublishingHandler {
            def,
            codec,
            publisher,
            pipeline,
        };
        Router {
            routes: (
                BatchRoute {
                    source,
                    handler,
                    meta,
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
    ) -> PublishingRouter<B, S, D, C, P, PC, PL, RC, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let workers = def.workers();
        let pipeline: Arc<[Arc<dyn PublishMiddleware>]> = Arc::from([]);
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline,
        };
        Router {
            routes: (
                SubscribeRoute {
                    source,
                    handler,
                    meta,
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

impl<B: Broker + 'static, R: RouterDef<B>, C, L> Router<B, R, C, L> {
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
    fn apply<M, H>(&self, handler: H) -> impl Handler<M> + 'static
    where
        M: Send + Sync + 'static,
        H: Handler<M> + 'static,
    {
        self.outer.apply::<M, _>(self.inner.apply::<M, _>(handler))
    }
}

impl<B, R, C, L> RouterDef<B> for Router<B, R, C, L>
where
    B: Broker + 'static,
    R: RouterDef<B>,
    L: BlanketLayer,
{
    fn mount<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        let composed = ComposedBlanket {
            outer: global,
            inner: &self.layers,
        };
        self.routes.mount(&composed, sink);
    }

    fn collect_handlers(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}

// Lets a whole router be a single registration inside another router's list (`Router::merge`).
impl<B, R, C, L> MountRoute<B> for Router<B, R, C, L>
where
    B: Broker + 'static,
    R: RouterDef<B>,
    L: BlanketLayer,
{
    fn mount_one<G: BlanketLayer>(self, global: &G, sink: &mut RouterSink<B>) {
        RouterDef::mount(self, global, sink);
    }

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        self.routes.collect_handlers(out);
    }
}
