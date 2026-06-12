//! [`Router`]: a broker-agnostic, statically-typed group of handler registrations.
//!
//! A `Router` collects subscriber registrations without a live broker, so a set of handlers can be
//! defined in its own module and mounted later. It is a consuming builder: each `include`/
//! `subscribe`/`handle` call takes the router by value and returns a new type carrying the added
//! registration, so the full registration list lives in the type. A builder function therefore
//! returns an opaque [`RouterDef`] rather than naming that type.
//!
//! Bind it to a broker by passing it to [`BrokerScope::include_router`](super::BrokerScope::include_router)
//! inside [`RustStream::with_broker`](super::RustStream::with_broker). Nothing connects or
//! subscribes until the application runs. Unlike a hand-rolled callback group, the app's global
//! [`layer`](super::RustStream::layer) stack DOES reach router handlers: each is wrapped with the
//! app's [`BlanketLayer`] global when the router is mounted.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::codec::Codec;
use crate::{Broker, Publisher, Subscriber, SubscriptionSource};

use crate::BatchSubscriber;

use super::batch::{BatchDef, BatchHandler, TypedBatch, batch_metadata, typed_batch};
use super::batch_publishing::{
    BatchPublishingDef, BatchPublishingHandler, batch_publishing_metadata,
};
use super::context::State;
use super::dispatch::{Delivery, spawn_batch_dispatch, spawn_dispatch};
use super::handler::Handler;
use super::lifecycle::{BoxError, BoxFuture};
use super::metadata::HandlerMetadata;
use super::middleware::{BlanketLayer, Identity, Stack};
use super::publish::{PublishLayer, PublishMiddleware, ReplyPublisher, TypedPublisher};
use super::publishing::{PublishingDef, PublishingHandler, publishing_metadata};
use super::subscriber_def::{SubscriberDef, subscriber_metadata};
use super::typed::{Typed, typed};

/// A deferred registration: given the broker (after connect), shared state, the per-scope publish
/// [`Delivery`] context, and the shutdown token, it opens the subscription and spawns the dispatch
/// task. The source and handler are captured and type-erased.
pub(crate) type BoundStarter<B> = Box<
    dyn FnOnce(
            Arc<B>,
            Arc<State>,
            Arc<Delivery>,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The message a source's subscriber yields, for broker `B`. Tames the long projection in bounds
/// and return types.
type SourceMessage<B, S> = <<S as SubscriptionSource<B>>::Subscriber as Subscriber>::Message;

/// The route a [`SubscriberDef`] `D` mounted on source `S` (decoded with `C`) becomes. Names the
/// otherwise unwieldy registration type.
type TypedRoute<B, S, D, C> = SubscribeRoute<
    S,
    Typed<SourceMessage<B, S>, <D as SubscriberDef>::Input, C, <D as SubscriberDef>::Handler>,
>;

/// The router that mounting a [`SubscriberDef`] `D` on source `S` (decoded with `C`) onto `R`
/// produces. `RC` / `RL` are the router's own codec and layer parameters, carried unchanged.
type IncludedRouter<B, S, D, C, RC, RL, R> = Router<B, (TypedRoute<B, S, D, C>, R), RC, RL>;

/// The route a [`BatchDef`] `D` mounted on source `S` (decoded with `C`) becomes.
type BatchTypedRoute<B, S, D, C> = BatchRoute<
    S,
    TypedBatch<SourceMessage<B, S>, <D as BatchDef>::Input, C, <D as BatchDef>::Handler>,
>;

/// The router that mounting a [`BatchDef`] `D` on source `S` (decoded with `C`) onto `R`
/// produces. `RC` / `RL` are the router's own codec and layer parameters, carried unchanged.
type IncludedBatchRouter<B, S, D, C, RC, RL, R> =
    Router<B, (BatchTypedRoute<B, S, D, C>, R), RC, RL>;

/// The router that mounting a publishing [`PublishingDef`] `D` on source `S` (decoded with `C`,
/// replying through a `P`/`PC`/`PL` publisher) onto `R` produces. `RC` / `RL` are the router's own
/// codec and layer parameters, carried unchanged.
type PublishingRouter<B, S, D, C, P, PC, PL, RC, RL, R> =
    Router<B, (SubscribeRoute<S, PublishingHandler<D, C, P, PC, PL>>, R), RC, RL>;

/// The router that mounting a batch publishing [`BatchPublishingDef`] `D` on source `S` (decoded
/// with `C`, replying through the [`ReplyPublisher`] `RP`) onto `R` produces.
type BatchPublishingRouter<B, S, D, C, RP, RC, RL, R> =
    Router<B, (BatchRoute<S, BatchPublishingHandler<D, C, RP>>, R), RC, RL>;

/// The router that [`Router::merge`] produces: the merged router becomes one registration in the
/// list.
type MergedRouter<B, R2, C2, L2, RC, RL, R> = Router<B, (Router<B, R2, C2, L2>, R), RC, RL>;

/// The runtime collector a router mounts into: type-erased starters plus handler metadata.
///
/// Created and drained inside the application; a [`RouterDef`] pushes into it during
/// [`include_router`](super::BrokerScope::include_router). You do not construct one directly.
pub struct RouterSink<B> {
    starters: Vec<BoundStarter<B>>,
    handlers: Vec<HandlerMetadata>,
}

impl<B> std::fmt::Debug for RouterSink<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterSink")
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> RouterSink<B> {
    pub(crate) fn new() -> Self {
        Self {
            starters: Vec::new(),
            handlers: Vec::new(),
        }
    }

    /// Erases an already-created subscriber and its handler into a starter.
    pub(crate) fn push_handle<S, H>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters
            .push(Box::new(move |_broker, state, delivery, token| {
                Box::pin(async move {
                    Ok(spawn_dispatch(
                        subscriber, handler, token, name, state, delivery,
                    ))
                })
            }));
        self.handlers.push(meta);
    }

    /// Erases a source and its batch handler into a starter driving
    /// [`BatchSubscriber::batches`]; the subscription opens after connect.
    pub(crate) fn push_subscribe_batch<S, H>(
        &mut self,
        source: S,
        handler: H,
        meta: HandlerMetadata,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        H: BatchHandler<SourceMessage<B, S>> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters
            .push(Box::new(move |broker: Arc<B>, state, delivery, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(broker.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    Ok(spawn_batch_dispatch(
                        subscriber, handler, token, name, state, delivery,
                    ))
                })
            }));
        self.handlers.push(meta);
    }

    /// Erases a source and its handler into a starter; the subscription opens after connect.
    pub(crate) fn push_subscribe<S, H>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<SourceMessage<B, S>> + 'static,
    {
        let handler = Arc::new(handler);
        let name: Arc<str> = Arc::from(meta.name.as_ref());
        self.starters
            .push(Box::new(move |broker: Arc<B>, state, delivery, token| {
                Box::pin(async move {
                    let subscriber = source
                        .subscribe(broker.as_ref())
                        .await
                        .map_err(|e| Box::new(e) as BoxError)?;
                    Ok(spawn_dispatch(
                        subscriber, handler, token, name, state, delivery,
                    ))
                })
            }));
        self.handlers.push(meta);
    }

    pub(crate) fn into_parts(self) -> (Vec<BoundStarter<B>>, Vec<HandlerMetadata>) {
        (self.starters, self.handlers)
    }
}

/// One subscription registration: a source plus the handler it dispatches to. An implementation
/// detail of [`Router`]'s registration list.
#[doc(hidden)]
#[derive(Debug)]
pub struct SubscribeRoute<S, H> {
    source: S,
    handler: H,
    meta: HandlerMetadata,
}

/// One registration bound to an already-created subscriber. An implementation detail of [`Router`].
#[doc(hidden)]
#[derive(Debug)]
pub struct HandleRoute<S, H> {
    subscriber: S,
    handler: H,
    meta: HandlerMetadata,
}

/// One batch-subscription registration: a source plus the batch handler consuming its batches.
/// An implementation detail of [`Router`]'s registration list.
#[doc(hidden)]
#[derive(Debug)]
pub struct BatchRoute<S, H> {
    source: S,
    handler: H,
    meta: HandlerMetadata,
}

/// One mountable registration: applies the global blanket layer to its handler and registers it.
trait MountRoute<B> {
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
        sink.push_subscribe(self.source, handler, self.meta);
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
    H: BatchHandler<SourceMessage<B, S>> + 'static,
{
    fn mount_one<G: BlanketLayer>(self, _global: &G, sink: &mut RouterSink<B>) {
        // Per-message layers cannot wrap a whole-batch handler, so neither the app-global stack
        // nor the router's own layers apply to batch registrations.
        sink.push_subscribe_batch(self.source, self.handler, self.meta);
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
        sink.push_handle(self.subscriber, handler, self.meta);
    }

    fn collect(&self, out: &mut Vec<HandlerMetadata>) {
        out.push(self.meta.clone());
    }
}

/// A mountable group of handler registrations.
///
/// Mounting applies the app's global [`BlanketLayer`] to each handler and registers it, so the
/// app-wide [`layer`](super::RustStream::layer) stack reaches router handlers. Implemented by
/// [`Router`] and its internal registration list; you obtain one from a builder and pass it to
/// [`include_router`](super::BrokerScope::include_router). You do not implement it.
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

/// A statically-typed, lazily-bound group of handler registrations, not attached to any broker.
///
/// Build it by chaining (each call consumes the router and returns a new type that carries the
/// added registration), then mount it with
/// [`include_router`](super::BrokerScope::include_router). The registration list `R` is an opaque
/// nested tuple, so a builder function returns `impl RouterDef<B>` instead of naming the type.
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
    routes: R,
    codec: C,
    layers: L,
    _broker: PhantomData<fn() -> B>,
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
    /// the router; the app's global [`layer`](super::RustStream::layer) stack wraps outside it.
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
    fn mount_subscriber<S, D, C>(
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
        let handler = typed(codec, def.into_handler());
        self.subscribe(source, handler, meta)
    }

    /// Mounts a batch definition on `source`, decoding with `codec`. The shared tail of the
    /// `include_batch` / `include_batch_on` forms.
    fn mount_batch<S, D, C>(
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
        let handler = typed_batch(codec, def.into_handler());
        Router {
            routes: (
                BatchRoute {
                    source,
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

    /// Mounts a batch publishing definition on `source`, decoding with `codec` and replying
    /// through `publisher`. The shared tail of the `include_batch_publishing` /
    /// `include_batch_publishing_on` forms.
    fn mount_batch_publishing<S, D, C, RP>(
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
    fn mount_publishing<S, D, C, P, PC, PL>(
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
        let pipeline: Arc<[Arc<dyn PublishMiddleware>]> = Arc::from([]);
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline,
        };
        self.subscribe(source, handler, meta)
    }
}

impl<B: Broker + 'static, R, RL> Router<B, R, (), RL> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Name a codec for the chain with [`with_codec`](Self::with_codec). The router-level
    /// counterpart of [`BrokerScope::include`](super::BrokerScope::include).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<D>(
        self,
        def: D,
    ) -> IncludedRouter<B, D::Source, D, crate::codec::DefaultCodec, (), RL, R>
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let source = def.source();
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`
    /// (overriding the macro's own source), decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// Useful to retarget a handler - e.g. mount it on an in-memory source in tests, or a
    /// different broker descriptor per deployment. The subscription name in metadata comes from
    /// `source`. The router-level counterpart of
    /// [`BrokerScope::include_on`](super::BrokerScope::include_on).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_on<S, D>(
        self,
        source: S,
        def: D,
    ) -> IncludedRouter<B, S, D, crate::codec::DefaultCodec, (), RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// The source's subscriber must implement [`BatchSubscriber`] - natively, or through the
    /// [`Buffered`](crate::Buffered) adapter. Router and app middleware wrap per-message handlers
    /// and do not apply to batch registrations.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch<D>(
        self,
        def: D,
    ) -> IncludedBatchRouter<B, D::Source, D, crate::codec::DefaultCodec, (), RL, R>
    where
        D: BatchDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let source = def.source();
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source` (overriding the macro's own source), decoding each element with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec).
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_batch_on<S, D>(
        self,
        source: S,
        def: D,
    ) -> IncludedBatchRouter<B, S, D, crate::codec::DefaultCodec, (), RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        self.mount_batch(source, def, crate::codec::DefaultCodec::default())
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the `publisher`'s own codec and publishing the replies
    /// through it.
    ///
    /// `publisher` is either a plain [`TypedPublisher`] (each reply published independently) or
    /// a [`Transactional`](super::Transactional) one (the batch's replies inside one
    /// transaction). Router handlers run with an empty dynamic publish pipeline, like
    /// [`include_publishing`](Self::include_publishing).
    pub fn include_batch_publishing<D, RP>(
        self,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, D::Source, D, RP::Codec, RP, (), RL, R>
    where
        D: BatchPublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        RP::Codec: Clone + 'static,
    {
        let codec = publisher.reply_codec().clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the `publisher`'s own codec.
    pub fn include_batch_publishing_on<S, D, RP>(
        self,
        source: S,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, D, RP::Codec, RP, (), RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
        RP::Codec: Clone + 'static,
    {
        let codec = publisher.reply_codec().clone();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and sending the reply through it.
    ///
    /// Router handlers run with an empty dynamic publish pipeline - the app's
    /// [`publish_layer`](super::RustStream::publish_layer)s do not apply; the publisher's own static
    /// [`PublishLayer`] stack still does.
    pub fn include_publishing<D, P, PC, PL>(
        self,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, D::Source, D, PC, P, PC, PL, (), RL, R>
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
    {
        let codec = publisher.codec().clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the `publisher`'s own codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, S, D, PC, P, PC, PL, (), RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
    {
        let codec = publisher.codec().clone();
        self.mount_publishing(source, def, codec, publisher)
    }
}

impl<B: Broker + 'static, R, C: Codec + Clone + 'static, RL> Router<B, R, C, RL> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include<D>(self, def: D) -> IncludedRouter<B, D::Source, D, C, C, RL, R>
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_subscriber(source, def, codec)
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_on<S, D>(self, source: S, def: D) -> IncludedRouter<B, S, D, C, C, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_subscriber(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on its own source, decoding each
    /// element with the chain's codec (set by [`with_codec`](Self::with_codec)).
    pub fn include_batch<D>(self, def: D) -> IncludedBatchRouter<B, D::Source, D, C, C, RL, R>
    where
        D: BatchDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..))]`-generated definition on an explicit subscription
    /// `source`, decoding each element with the chain's codec (set by
    /// [`with_codec`](Self::with_codec)).
    pub fn include_batch_on<S, D>(
        self,
        source: S,
        def: D,
    ) -> IncludedBatchRouter<B, S, D, C, C, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch(source, def, codec)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on its own
    /// source, decoding each element with the chain's codec and publishing the replies through
    /// `publisher`.
    pub fn include_batch_publishing<D, RP>(
        self,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, D::Source, D, C, RP, C, RL, R>
    where
        D: BatchPublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: BatchSubscriber + Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(batch(..), publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding each element with the chain's codec.
    pub fn include_batch_publishing_on<S, D, RP>(
        self,
        source: S,
        def: D,
        publisher: RP,
    ) -> BatchPublishingRouter<B, S, D, C, RP, C, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: BatchSubscriber + Send + 'static,
        D: BatchPublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        RP: ReplyPublisher + 'static,
    {
        let codec = self.codec.clone();
        self.mount_batch_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the chain's codec and replying through `publisher`.
    pub fn include_publishing<D, P, PC, PL>(
        self,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, D::Source, D, C, P, PC, PL, C, RL, R>
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher)
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the chain's codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) -> PublishingRouter<B, S, D, C, P, PC, PL, C, RL, R>
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
    {
        let codec = self.codec.clone();
        self.mount_publishing(source, def, codec, publisher)
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
