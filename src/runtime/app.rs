//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error as StdError,
    future::Future,
    sync::Arc,
    time::Duration,
};

#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{Broker, Publisher, ServerSpec, Subscriber, SubscriptionSource};

use super::context::State;
use super::dispatch::{Delivery, Publishers};
use super::handler::Handler;
use super::lifecycle::{BoxError, BoxFuture, BrokerLifecycle};
use super::metadata::HandlerMetadata;
use super::middleware::{BlanketLayer, Identity, Layer, Stack};
use super::publish::{PublishLayer, PublishMiddleware, TypedPublisher};
use super::publisher_registry::ErasedPublisher;
use super::publishing::{PublishingDef, PublishingHandler, publishing_metadata};
use super::router::{RouterDef, RouterSink};
use super::subscriber_def::{SubscriberDef, subscriber_metadata};
use super::typed::{Typed, typed};

/// A registration deferred until [`RustStream::run`]: given the shutdown token, it opens the
/// subscription (after the broker is connected) and spawns the dispatch task. The broker, source
/// and handler are captured and type-erased.
type Starter = Box<
    dyn FnOnce(
            Arc<State>,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The `on_startup` lifespan hook: runs once before brokers connect. It receives the app [`State`]
/// by value (so its future can own it across awaits - e.g. connect a database, then insert the
/// pool) and returns it, populated.
type StartupHook = Box<dyn FnOnce(State) -> BoxFuture<'static, Result<State, BoxError>> + Send>;

/// A read-only lifespan hook (`after_startup` / `on_shutdown` / `after_shutdown`): runs once at the
/// corresponding lifecycle point with a shared [`State`] handle (read via [`State::get`]).
type LifecycleHook = Box<dyn FnOnce(Arc<State>) -> BoxFuture<'static, Result<(), BoxError>> + Send>;

/// Which read-only lifecycle hook list a hook is appended to.
#[derive(Clone, Copy)]
enum LifecyclePhase {
    AfterStartup,
    OnShutdown,
    AfterShutdown,
}

/// Service-level metadata, surfaced to the `AsyncAPI` generator as the spec `Info` object.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AppInfo {
    /// Human-readable service title.
    pub title: String,
    /// Service version string.
    pub version: String,
    /// Optional longer description.
    pub description: Option<String>,
}

impl AppInfo {
    /// Creates info with a title and version and no description.
    #[must_use]
    pub fn new(title: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            version: version.into(),
            description: None,
        }
    }

    /// Sets the description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// Errors surfaced while running a [`RustStream`] service.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RustStreamError {
    /// A broker failed to [`connect`](Broker::connect) at startup.
    #[error("broker connect failed: {0}")]
    Connect(#[source] BoxError),
    /// An `on_startup` or `after_startup` lifespan hook failed.
    #[error("startup hook failed: {0}")]
    Startup(#[source] BoxError),
    /// A subscription failed to open after connect.
    #[error("subscription failed: {0}")]
    Subscribe(#[source] BoxError),
    /// A broker failed to [`shutdown`](Broker::shutdown) during graceful shutdown.
    #[error("broker shutdown failed: {0}")]
    Shutdown(#[source] BoxError),
    /// A dispatch task panicked or was aborted.
    #[error("dispatch task failed: {0}")]
    Join(#[source] tokio::task::JoinError),
}

/// The top-level application object.
///
/// `RustStream` binds one or more brokers, the handlers attached to each, and the service
/// lifecycle into a single runnable unit. Handlers are registered through [`with_broker`], which
/// hands a scope bound to that broker; nothing connects or subscribes until [`run`]. Brokers are
/// held type-erased (only their lifecycle), so a single service can mix broker types.
///
/// The type parameter `L` is the global middleware stack applied to every handler registered
/// directly on a broker scope; it defaults to the no-op [`Identity`] and grows as [`layer`] is
/// called. Add all layers before [`with_broker`], since a layer only applies to handlers
/// registered after it (and not to handlers brought in via
/// [`include_router`](BrokerScope::include_router)).
///
/// [`with_broker`]: Self::with_broker
/// [`layer`]: Self::layer
/// [`run`]: Self::run
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() -> Result<(), ruststream::runtime::RustStreamError> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerResult, RustStream};
/// use ruststream::runtime::layers::TracingLayer;
///
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
///     .layer(TracingLayer::default())
///     .with_broker(MemoryBroker::new(), |b| {
///         let subscriber = b.broker().subscribe("orders");
///         b.handle(
///             subscriber,
///             |_msg: &_, _ctx: &mut Context| async { HandlerResult::Ack },
///             HandlerMetadata::raw("orders"),
///         );
///     });
/// app.run().await
/// # }
/// ```
pub struct RustStream<L = Identity> {
    info: AppInfo,
    brokers: Vec<Arc<dyn BrokerLifecycle>>,
    starters: Vec<Starter>,
    handlers: Vec<HandlerMetadata>,
    servers: BTreeMap<String, ServerSpec>,
    publishers: Publishers,
    publish_layers: Vec<Arc<dyn PublishMiddleware>>,
    state: State,
    on_startup: Vec<StartupHook>,
    after_startup: Vec<LifecycleHook>,
    on_shutdown: Vec<LifecycleHook>,
    after_shutdown: Vec<LifecycleHook>,
    shutdown_timeout: Option<Duration>,
    global: L,
}

impl<L> std::fmt::Debug for RustStream<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustStream")
            .field("info", &self.info)
            .field("brokers", &self.brokers.len())
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl RustStream<Identity> {
    /// Creates an empty service with the given metadata and no global middleware.
    #[must_use]
    pub fn new(info: AppInfo) -> Self {
        Self {
            info,
            brokers: Vec::new(),
            starters: Vec::new(),
            handlers: Vec::new(),
            servers: BTreeMap::new(),
            publishers: HashMap::new(),
            publish_layers: Vec::new(),
            state: State::default(),
            on_startup: Vec::new(),
            after_startup: Vec::new(),
            on_shutdown: Vec::new(),
            after_shutdown: Vec::new(),
            shutdown_timeout: None,
            global: Identity,
        }
    }
}

impl<L> RustStream<L> {
    /// Adds a global middleware layer, applied to every handler registered after it.
    ///
    /// The first layer added runs outermost. Call before [`with_broker`](Self::with_broker).
    #[must_use]
    pub fn layer<N>(self, layer: N) -> RustStream<Stack<N, L>> {
        RustStream {
            info: self.info,
            brokers: self.brokers,
            starters: self.starters,
            handlers: self.handlers,
            servers: self.servers,
            publishers: self.publishers,
            publish_layers: self.publish_layers,
            state: self.state,
            on_startup: self.on_startup,
            after_startup: self.after_startup,
            on_shutdown: self.on_shutdown,
            after_shutdown: self.after_shutdown,
            shutdown_timeout: self.shutdown_timeout,
            global: Stack::new(layer, self.global),
        }
    }

    /// Inserts a shared application state value, readable from handlers and middleware via
    /// [`Context::get`](super::Context::get).
    ///
    /// One value per type; inserting the same type again replaces it.
    #[must_use]
    pub fn insert_state<T>(mut self, value: T) -> Self
    where
        T: std::any::Any + Send + Sync,
    {
        self.state.insert(value);
        self
    }

    /// Adds a hook run before brokers connect. It receives the [`State`] by value for lazily
    /// creating shared resources (a database pool, a client) and returns it populated. A failing
    /// hook aborts startup.
    #[must_use]
    pub fn on_startup<F, Fut, E>(mut self, hook: F) -> Self
    where
        F: FnOnce(State) -> Fut + Send + 'static,
        Fut: Future<Output = Result<State, E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.on_startup.push(Box::new(move |state| {
            Box::pin(async move { hook(state).await.map_err(|e| Box::new(e) as BoxError) })
        }));
        self
    }

    /// Adds a hook run after brokers connect and handlers are spawned (for example, to publish an
    /// initial message or signal readiness). A failing hook aborts startup.
    #[must_use]
    pub fn after_startup<F, Fut, E>(self, hook: F) -> Self
    where
        F: FnOnce(Arc<State>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.push_lifecycle_hook(LifecyclePhase::AfterStartup, hook)
    }

    /// Adds a hook run when shutdown begins, while brokers are still connected. Errors are logged.
    #[must_use]
    pub fn on_shutdown<F, Fut, E>(self, hook: F) -> Self
    where
        F: FnOnce(Arc<State>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.push_lifecycle_hook(LifecyclePhase::OnShutdown, hook)
    }

    /// Adds a hook run after brokers have shut down (for final async resource teardown). Errors are
    /// logged.
    #[must_use]
    pub fn after_shutdown<F, Fut, E>(self, hook: F) -> Self
    where
        F: FnOnce(Arc<State>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.push_lifecycle_hook(LifecyclePhase::AfterShutdown, hook)
    }

    fn push_lifecycle_hook<F, Fut, E>(mut self, phase: LifecyclePhase, hook: F) -> Self
    where
        F: FnOnce(Arc<State>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        let boxed: LifecycleHook = Box::new(move |state| {
            Box::pin(async move { hook(state).await.map_err(|e| Box::new(e) as BoxError) })
        });
        match phase {
            LifecyclePhase::AfterStartup => self.after_startup.push(boxed),
            LifecyclePhase::OnShutdown => self.on_shutdown.push(boxed),
            LifecyclePhase::AfterShutdown => self.after_shutdown.push(boxed),
        }
        self
    }

    /// Sets how long [`run`](Self::run) waits for in-flight handlers to finish after shutdown is
    /// triggered. After the timeout, the remaining handler tasks are aborted. Defaults to waiting
    /// indefinitely.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    /// Registers a named publisher, so handlers can publish to it by name (including from a
    /// different broker's scope).
    ///
    /// The publisher is held type-erased; resolve it with
    /// [`BrokerScope::publisher`](BrokerScope::publisher).
    #[must_use]
    pub fn publisher<P>(mut self, name: impl Into<String>, publisher: P) -> Self
    where
        P: Publisher + 'static,
    {
        self.publishers.insert(name.into(), Arc::new(publisher));
        self
    }

    /// Adds an outgoing publish middleware, run on every published reply before it reaches the
    /// broker (a Confluent / Avro envelope, publish metrics, dead-letter). The first one added runs
    /// outermost. Call before [`with_broker`](Self::with_broker).
    #[must_use]
    pub fn publish_layer<M>(mut self, middleware: M) -> Self
    where
        M: PublishMiddleware + 'static,
    {
        self.publish_layers.push(Arc::new(middleware));
        self
    }

    /// Registers a broker for lifecycle management only (connect / shutdown), without attaching
    /// subscribers. Use for publish-only brokers.
    #[must_use]
    pub fn register_broker<B>(mut self, broker: B) -> Self
    where
        B: Broker + 'static,
    {
        self.brokers.push(Arc::new(broker));
        self
    }

    /// Records an `AsyncAPI` server (one per broker the service connects to).
    ///
    /// Build the [`ServerSpec`] directly, or get it from a broker that implements
    /// [`DescribeServer`](crate::DescribeServer): `app.server("nats", broker.describe_server())`.
    /// `build_spec` emits these in the document's `servers` section.
    #[must_use]
    pub fn server(mut self, name: impl Into<String>, spec: ServerSpec) -> Self {
        self.servers.insert(name.into(), spec);
        self
    }

    /// Registers a broker and the handlers attached to it.
    ///
    /// `build` receives a [`BrokerScope`] typed to this broker; use it to attach handlers. The
    /// broker is then held for lifecycle management. Call this once per broker.
    ///
    /// The scope has no default codec, so macro handlers are mounted with an explicit one
    /// (`b.include(handle, JsonCodec)`). To set a scope default and drop the per-call codec, use
    /// [`with_broker_codec`](Self::with_broker_codec).
    #[must_use]
    pub fn with_broker<B, F>(mut self, broker: B, build: F) -> Self
    where
        B: Broker + 'static,
        L: Clone,
        F: FnOnce(&mut BrokerScope<B, L>),
    {
        let broker = Arc::new(broker);
        let mut scope = self.new_scope(&broker, ());
        build(&mut scope);
        self.collect_scope(&broker, scope);
        self
    }

    /// Registers a broker with a default `codec`, so its macro handlers are mounted without
    /// repeating it: `b.include(handle)` instead of `b.include(handle, codec)`.
    ///
    /// `build` receives a [`BrokerScope`] whose [`include`](BrokerScope::include) and
    /// [`include_publishing`](BrokerScope::include_publishing) take just the definition and decode
    /// it with `codec`.
    #[must_use]
    pub fn with_broker_codec<B, C, F>(mut self, broker: B, codec: C, build: F) -> Self
    where
        B: Broker + 'static,
        C: Codec + Clone + 'static,
        L: Clone,
        F: FnOnce(&mut BrokerScope<B, L, C>),
    {
        let broker = Arc::new(broker);
        let mut scope = self.new_scope(&broker, codec);
        build(&mut scope);
        self.collect_scope(&broker, scope);
        self
    }

    /// Builds a fresh scope bound to `broker` carrying `codec` and the app's publishers / pipeline.
    fn new_scope<B, C>(&self, broker: &Arc<B>, codec: C) -> BrokerScope<B, L, C>
    where
        B: Broker + 'static,
        L: Clone,
    {
        BrokerScope {
            broker: broker.clone(),
            sink: RouterSink::new(),
            publishers: self.publishers.clone(),
            pipeline: self.publish_layers.iter().cloned().collect(),
            global: self.global.clone(),
            codec,
        }
    }

    /// Drains a built scope's registrations into the app and holds the broker for lifecycle.
    fn collect_scope<B, C>(&mut self, broker: &Arc<B>, scope: BrokerScope<B, L, C>)
    where
        B: Broker + 'static,
    {
        let lifecycle: Arc<dyn BrokerLifecycle> = broker.clone();
        let delivery = Arc::new(Delivery {
            publishers: self.publishers.clone(),
            pipeline: scope.pipeline.clone(),
        });
        let (starters, handlers) = scope.sink.into_parts();
        for (bound, meta) in starters.into_iter().zip(handlers) {
            let broker = broker.clone();
            let delivery = delivery.clone();
            self.starters.push(Box::new(move |state, token| {
                bound(broker, state, delivery, token)
            }));
            self.handlers.push(meta);
        }
        self.brokers.push(lifecycle);
    }

    /// Returns metadata for every registered handler, in registration order. Input to the
    /// `AsyncAPI` generator.
    #[must_use]
    pub fn handlers(&self) -> &[HandlerMetadata] {
        &self.handlers
    }

    /// Returns the service metadata.
    #[must_use]
    pub fn info(&self) -> &AppInfo {
        &self.info
    }

    /// Returns the registered `AsyncAPI` servers, keyed by name. Input to the `AsyncAPI` generator.
    #[must_use]
    pub fn servers(&self) -> &BTreeMap<String, ServerSpec> {
        &self.servers
    }

    /// Runs the service until an interrupt (`SIGINT` / `SIGTERM`) is received, then shuts down
    /// gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a broker fails to connect, a subscription fails to open, a
    /// dispatch task panics, or a broker fails to shut down.
    pub async fn run(self) -> Result<(), RustStreamError> {
        self.run_until(wait_for_signal()).await
    }

    /// Runs the service until `shutdown` resolves, then shuts down gracefully.
    ///
    /// Use this instead of [`run`](Self::run) to drive shutdown from a caller-owned future (a
    /// name, a timeout, a test signal) rather than from process signals.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a broker fails to connect, a subscription fails to open, a
    /// dispatch task panics, or a broker fails to shut down.
    pub async fn run_until<F>(self, shutdown: F) -> Result<(), RustStreamError>
    where
        F: Future<Output = ()> + Send,
    {
        let Self {
            info,
            brokers,
            starters,
            handlers,
            mut state,
            on_startup,
            after_startup,
            on_shutdown,
            after_shutdown,
            shutdown_timeout,
            ..
        } = self;

        info!(
            target: "ruststream::lifecycle",
            service = %info.title,
            version = %info.version,
            brokers = brokers.len(),
            subscribers = starters.len(),
            "starting service",
        );

        if !on_startup.is_empty() {
            debug!(target: "ruststream::lifecycle", count = on_startup.len(), "running on_startup hooks");
        }
        for hook in on_startup {
            state = hook(state).await.map_err(RustStreamError::Startup)?;
        }
        let state = Arc::new(state);

        for broker in &brokers {
            broker.connect().await.map_err(RustStreamError::Connect)?;
            info!(target: "ruststream::lifecycle", broker = broker.name(), "broker connected");
        }

        let token = CancellationToken::new();
        let mut handles = Vec::with_capacity(starters.len());
        for (starter, meta) in starters.into_iter().zip(handlers) {
            let handle = starter(state.clone(), token.clone())
                .await
                .map_err(RustStreamError::Subscribe)?;
            info!(
                target: "ruststream::dispatch",
                subscriber = %meta.name,
                input = meta.input_type,
                "subscriber started",
            );
            handles.push(handle);
        }

        if !after_startup.is_empty() {
            debug!(target: "ruststream::lifecycle", count = after_startup.len(), "running after_startup hooks");
        }
        for hook in after_startup {
            hook(Arc::clone(&state))
                .await
                .map_err(RustStreamError::Startup)?;
        }

        info!(target: "ruststream::lifecycle", subscribers = handles.len(), "service running");

        shutdown.await;
        info!(target: "ruststream::lifecycle", "shutdown signal received");

        for hook in on_shutdown {
            if let Err(err) = hook(Arc::clone(&state)).await {
                warn!(target: "ruststream::lifecycle", error = %err, "on_shutdown hook failed");
            }
        }

        token.cancel();
        debug!(target: "ruststream::lifecycle", "draining in-flight handlers");
        drain_handles(handles, shutdown_timeout).await?;

        for broker in brokers.iter().rev() {
            broker.shutdown().await.map_err(RustStreamError::Shutdown)?;
            debug!(target: "ruststream::lifecycle", broker = broker.name(), "broker shut down");
        }

        for hook in after_shutdown {
            if let Err(err) = hook(Arc::clone(&state)).await {
                warn!(target: "ruststream::lifecycle", error = %err, "after_shutdown hook failed");
            }
        }
        info!(target: "ruststream::lifecycle", "service stopped");
        Ok(())
    }
}

/// A handler-registration scope bound to one broker.
///
/// Handed to the [`RustStream::with_broker`] closure. It is a [`Router`] plus the broker it is
/// bound to and the global middleware stack `L`; registrations are collected and started later, in
/// [`RustStream::run`]. Each handler registered here is wrapped with `L` before it is stored.
pub struct BrokerScope<B, L = Identity, C = ()> {
    broker: Arc<B>,
    sink: RouterSink<B>,
    publishers: Publishers,
    pipeline: Arc<[Arc<dyn PublishMiddleware>]>,
    global: L,
    codec: C,
}

impl<B: Broker + 'static, L, C> BrokerScope<B, L, C> {
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Resolves a named publisher registered with
    /// [`RustStream::publisher`](RustStream::publisher), to capture in a handler and publish to.
    #[must_use]
    pub fn publisher(&self, name: &str) -> Option<Arc<dyn ErasedPublisher>> {
        self.publishers.get(name).cloned()
    }

    /// Attaches `handler` (wrapped with the global stack) to an already-created `subscriber`.
    ///
    /// See [`Router::handle`].
    pub fn handle<S, H>(&mut self, subscriber: S, handler: H, meta: HandlerMetadata)
    where
        S: Subscriber + Send + 'static,
        H: Handler<S::Message> + 'static,
        L: Layer<H>,
        L::Handler: Handler<S::Message> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink.push_handle(subscriber, handler, meta);
    }

    /// Attaches `handler` (wrapped with the global stack) to a subscription described by `source`.
    ///
    /// See [`Router::subscribe`].
    pub fn subscribe<S, H>(&mut self, source: S, handler: H, meta: HandlerMetadata)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        H: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
        L: Layer<H>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let handler = self.global.layer(handler);
        self.sink.push_subscribe(source, handler, meta);
    }

    /// Mounts every registration from `router` onto this broker, wrapping each handler with the
    /// app's global middleware stack.
    ///
    /// Unlike a hand-rolled handler group, a [`Router`] composes with the app's
    /// [`layer`](RustStream::layer): the global stack must be a [`BlanketLayer`] (it applies to
    /// handlers whose concrete types the router hides), which every bundled layer and any
    /// [`Stack`](super::Stack) of them satisfies.
    pub fn include_router<R>(&mut self, router: R)
    where
        R: RouterDef<B>,
        L: BlanketLayer,
    {
        router.mount(&self.global, &mut self.sink);
    }
}

impl<B: Broker + 'static, L> BrokerScope<B, L, ()> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec) and wrapping the handler with the global stack.
    ///
    /// Name a codec by setting a scope default with
    /// [`with_broker_codec`](RustStream::with_broker_codec), or per handler with
    /// [`include_with`](Self::include_with). The source comes from the macro: a [`Name`] for
    /// `#[subscriber("topic")]` (the broker must implement [`Subscribe`]) or a broker descriptor for
    /// `#[subscriber(RedisStream::new(..))]`.
    ///
    /// [`Name`]: crate::Name
    /// [`Subscribe`]: crate::Subscribe
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include<D>(&mut self, def: D)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        L: Layer<
            Typed<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                D::Input,
                crate::codec::DefaultCodec,
                D::Handler,
            >,
        >,
        L::Handler: Handler<<<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message>
            + 'static,
    {
        let source = def.source();
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default());
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the [`DefaultCodec`](crate::codec::DefaultCodec).
    ///
    /// See [`include_on_with`](Self::include_on_with) for the explicit-codec form.
    #[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
    pub fn include_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        L: Layer<
            Typed<
                <S::Subscriber as Subscriber>::Message,
                D::Input,
                crate::codec::DefaultCodec,
                D::Handler,
            >,
        >,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        self.mount_subscriber(source, def, crate::codec::DefaultCodec::default());
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on its own source,
    /// decoding its input with the `publisher`'s own codec and replying through it.
    ///
    /// Name the codec once, on the `publisher`. Override the decode codec per handler with
    /// [`include_publishing_with`](Self::include_publishing_with), or set a scope default with
    /// [`with_broker_codec`](RustStream::with_broker_codec).
    pub fn include_publishing<D, P, PC, PL>(&mut self, def: D, publisher: TypedPublisher<P, PC, PL>)
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
        L: Layer<PublishingHandler<D, PC, P, PC, PL>>,
        L::Handler: Handler<<<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message>
            + 'static,
    {
        let codec = publisher.codec().clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(.., publish("name"))]`-generated definition on an explicit
    /// subscription `source`, decoding its input with the `publisher`'s own codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + Clone + 'static,
        PL: PublishLayer + 'static,
        L: Layer<PublishingHandler<D, PC, P, PC, PL>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let codec = publisher.codec().clone();
        self.mount_publishing(source, def, codec, publisher);
    }
}

impl<B: Broker + 'static, L, C: Codec + Clone + 'static> BrokerScope<B, L, C> {
    /// Mounts a `#[subscriber]`-generated definition on its own source, decoding its input with the
    /// scope's default codec (set by [`with_broker_codec`](RustStream::with_broker_codec)).
    pub fn include<D>(&mut self, def: D)
    where
        D: SubscriberDef,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        L: Layer<
            Typed<
                <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message,
                D::Input,
                C,
                D::Handler,
            >,
        >,
        L::Handler: Handler<<<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message>
            + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber]`-generated definition on an explicit subscription `source`, decoding
    /// its input with the scope's default codec.
    pub fn include_on<S, D>(&mut self, source: S, def: D)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        L: Layer<Typed<<S::Subscriber as Subscriber>::Message, D::Input, C, D::Handler>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let codec = self.codec.clone();
        self.mount_subscriber(source, def, codec);
    }

    /// Mounts a `#[subscriber(.., publish)]`-generated definition on its own source, decoding its
    /// input with the scope's default codec and sending the reply through `publisher`.
    pub fn include_publishing<D, P, PC, PL>(&mut self, def: D, publisher: TypedPublisher<P, PC, PL>)
    where
        D: PublishingDef + 'static,
        D::Source: SubscriptionSource<B> + Send + 'static,
        <D::Source as SubscriptionSource<B>>::Subscriber: Send + 'static,
        <<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message: 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
        L: Layer<PublishingHandler<D, C, P, PC, PL>>,
        L::Handler: Handler<<<D::Source as SubscriptionSource<B>>::Subscriber as Subscriber>::Message>
            + 'static,
    {
        let codec = self.codec.clone();
        let source = def.source();
        self.mount_publishing(source, def, codec, publisher);
    }

    /// Mounts a `#[subscriber(.., publish)]`-generated definition on an explicit subscription
    /// `source`, decoding its input with the scope's default codec.
    pub fn include_publishing_on<S, D, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
        L: Layer<PublishingHandler<D, C, P, PC, PL>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let codec = self.codec.clone();
        self.mount_publishing(source, def, codec, publisher);
    }
}

impl<B: Broker + 'static, L, SC> BrokerScope<B, L, SC> {
    /// Mounts a definition on `source`, decoding with `codec`. The shared tail of the
    /// `include` / `include_on` forms.
    fn mount_subscriber<S, D, C>(&mut self, source: S, def: D, codec: C)
    where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
        L: Layer<Typed<<S::Subscriber as Subscriber>::Message, D::Input, C, D::Handler>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let meta = subscriber_metadata(source.name().to_owned(), &def);
        let handler = typed(codec, def.into_handler());
        self.subscribe(source, handler, meta);
    }

    /// Mounts a publishing definition on `source`, decoding with `codec` and replying through
    /// `publisher`. The shared tail of the `include_publishing` / `include_publishing_on` forms.
    fn mount_publishing<S, D, C, P, PC, PL>(
        &mut self,
        source: S,
        def: D,
        codec: C,
        publisher: TypedPublisher<P, PC, PL>,
    ) where
        S: SubscriptionSource<B> + Send + 'static,
        S::Subscriber: Send + 'static,
        <S::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        P: Publisher + 'static,
        PC: Codec + 'static,
        PL: PublishLayer + 'static,
        L: Layer<PublishingHandler<D, C, P, PC, PL>>,
        L::Handler: Handler<<S::Subscriber as Subscriber>::Message> + 'static,
    {
        let meta = publishing_metadata(source.name().to_owned(), &def);
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            pipeline: self.pipeline.clone(),
        };
        self.subscribe(source, handler, meta);
    }
}

impl<B, L, C> std::fmt::Debug for BrokerScope<B, L, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerScope")
            .field("sink", &self.sink)
            .finish_non_exhaustive()
    }
}

/// Awaits all handler tasks, bounded by `timeout` if set. On timeout the remaining tasks are
/// aborted; without a timeout, a panicking task surfaces as [`RustStreamError::Join`].
async fn drain_handles(
    handles: Vec<JoinHandle<()>>,
    timeout: Option<Duration>,
) -> Result<(), RustStreamError> {
    let Some(timeout) = timeout else {
        for handle in handles {
            handle.await.map_err(RustStreamError::Join)?;
        }
        return Ok(());
    };

    let aborts: Vec<_> = handles.iter().map(JoinHandle::abort_handle).collect();
    if tokio::time::timeout(timeout, futures::future::join_all(handles))
        .await
        .is_err()
    {
        warn!(
            target: "ruststream::lifecycle",
            "graceful shutdown timed out; aborting in-flight handlers",
        );
        for abort in aborts {
            abort.abort();
        }
    }
    Ok(())
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
