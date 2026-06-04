//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

use std::{
    collections::HashMap, error::Error as StdError, future::Future, sync::Arc, time::Duration,
};

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::codec::Codec;
use crate::{Broker, Publisher, Subscribe, Subscriber, SubscriptionSource, Topic};

use super::context::State;
use super::handler::Handler;
use super::lifecycle::{BoxError, BoxFuture, BrokerLifecycle};
use super::metadata::HandlerMetadata;
use super::middleware::{Identity, Layer, Stack};
use super::publish::PublishMiddleware;
use super::publisher_registry::ErasedPublisher;
use super::publishing::{PublishingDef, PublishingHandler};
use super::router::Router;
use super::subscriber_def::SubscriberDef;
use super::typed::{Typed, typed};

type Publishers = HashMap<String, Arc<dyn ErasedPublisher>>;

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
/// by value (so its future can own it across awaits — e.g. connect a database, then insert the
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

    /// Registers a broker and the handlers attached to it.
    ///
    /// `build` receives a [`BrokerScope`] typed to this broker; use it to attach handlers. The
    /// broker is then held for lifecycle management. Call this once per broker.
    #[must_use]
    pub fn with_broker<B, F>(mut self, broker: B, build: F) -> Self
    where
        B: Broker + 'static,
        L: Clone,
        F: FnOnce(&mut BrokerScope<B, L>),
    {
        let broker = Arc::new(broker);
        let lifecycle: Arc<dyn BrokerLifecycle> = broker.clone();
        let mut scope = BrokerScope {
            broker: broker.clone(),
            router: Router::new(),
            publishers: self.publishers.clone(),
            pipeline: self.publish_layers.iter().cloned().collect(),
            global: self.global.clone(),
        };
        build(&mut scope);
        let (starters, handlers) = scope.router.into_parts();
        for (bound, meta) in starters.into_iter().zip(handlers) {
            let broker = broker.clone();
            self.starters
                .push(Box::new(move |state, token| bound(broker, state, token)));
            self.handlers.push(meta);
        }
        self.brokers.push(lifecycle);
        self
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
    /// channel, a timeout, a test signal) rather than from process signals.
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
            brokers,
            starters,
            mut state,
            on_startup,
            after_startup,
            on_shutdown,
            after_shutdown,
            shutdown_timeout,
            ..
        } = self;

        for hook in on_startup {
            state = hook(state).await.map_err(RustStreamError::Startup)?;
        }
        let state = Arc::new(state);

        for broker in &brokers {
            broker.connect().await.map_err(RustStreamError::Connect)?;
        }

        let token = CancellationToken::new();
        let mut handles = Vec::with_capacity(starters.len());
        for starter in starters {
            let handle = starter(state.clone(), token.clone())
                .await
                .map_err(RustStreamError::Subscribe)?;
            handles.push(handle);
        }

        for hook in after_startup {
            hook(Arc::clone(&state))
                .await
                .map_err(RustStreamError::Startup)?;
        }

        shutdown.await;

        for hook in on_shutdown {
            if let Err(err) = hook(Arc::clone(&state)).await {
                warn!(target: "ruststream::lifecycle", error = %err, "on_shutdown hook failed");
            }
        }

        token.cancel();
        drain_handles(handles, shutdown_timeout).await?;

        for broker in brokers.iter().rev() {
            broker.shutdown().await.map_err(RustStreamError::Shutdown)?;
        }

        for hook in after_shutdown {
            if let Err(err) = hook(Arc::clone(&state)).await {
                warn!(target: "ruststream::lifecycle", error = %err, "after_shutdown hook failed");
            }
        }
        Ok(())
    }
}

/// A handler-registration scope bound to one broker.
///
/// Handed to the [`RustStream::with_broker`] closure. It is a [`Router`] plus the broker it is
/// bound to and the global middleware stack `L`; registrations are collected and started later, in
/// [`RustStream::run`]. Each handler registered here is wrapped with `L` before it is stored.
pub struct BrokerScope<B, L = Identity> {
    broker: Arc<B>,
    router: Router<B>,
    publishers: Publishers,
    pipeline: Arc<[Arc<dyn PublishMiddleware>]>,
    global: L,
}

impl<B: Broker + 'static, L> BrokerScope<B, L> {
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
        self.router.handle(subscriber, handler, meta);
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
        self.router.subscribe(source, handler, meta);
    }

    /// Mounts a `#[subscriber]`-generated definition, decoding its input with `codec`.
    ///
    /// Subscribes via a [`Topic`] descriptor (so the broker must implement [`Subscribe`]) and wraps
    /// the handler with the global middleware stack, just like [`subscribe`](Self::subscribe).
    pub fn include<D, C>(&mut self, def: D, codec: C)
    where
        B: Subscribe,
        <B as Broker>::Subscriber: Send + 'static,
        <<B as Broker>::Subscriber as Subscriber>::Message: 'static,
        D: SubscriberDef,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Handler: 'static,
        C: Codec + 'static,
        L: Layer<
            Typed<<<B as Broker>::Subscriber as Subscriber>::Message, D::Input, C, D::Handler>,
        >,
        L::Handler: Handler<<<B as Broker>::Subscriber as Subscriber>::Message> + 'static,
    {
        let channel = def.channel().to_owned();
        let mut meta = HandlerMetadata::typed::<D::Input>(channel.clone());
        if let Some(description) = def.description() {
            meta = meta.with_description(description.to_owned());
        }
        let handler = typed(codec, def.into_handler());
        self.subscribe(Topic::new(channel), handler, meta);
    }

    /// Mounts a `#[subscriber(.., publish(..))]`-generated definition: decodes its input with
    /// `codec`, runs the handler, then encodes and publishes the reply through the named publisher.
    ///
    /// The publisher is resolved from the registry by name now; register it with
    /// [`RustStream::publisher`](RustStream::publisher) before this call. If it is missing, the
    /// reply is dropped (logged) at dispatch.
    pub fn include_publishing<D, C>(&mut self, def: D, codec: C)
    where
        B: Subscribe,
        <B as Broker>::Subscriber: Send + 'static,
        <<B as Broker>::Subscriber as Subscriber>::Message: 'static,
        D: PublishingDef + 'static,
        D::Input: DeserializeOwned + Send + Sync + 'static,
        D::Reply: Serialize + Send + Sync + 'static,
        C: Codec + 'static,
        L: Layer<PublishingHandler<D, C>>,
        L::Handler: Handler<<<B as Broker>::Subscriber as Subscriber>::Message> + 'static,
    {
        let publisher = self.publishers.get(def.publisher_name()).cloned();
        let subscribe_channel = def.subscribe_channel().to_owned();
        let topic = def.publish_channel().to_owned();
        let description = def.description().map(str::to_owned);
        let mut meta = HandlerMetadata::typed::<D::Input>(subscribe_channel.clone())
            .with_output_type(std::any::type_name::<D::Reply>());
        if let Some(description) = description {
            meta = meta.with_description(description);
        }
        let handler = PublishingHandler {
            def,
            codec,
            publisher,
            topic,
            pipeline: self.pipeline.clone(),
        };
        self.subscribe(Topic::new(subscribe_channel), handler, meta);
    }

    /// Mounts every registration from `router` onto this broker.
    ///
    /// The global middleware stack does **not** apply to these handlers: a [`Router`] is built
    /// independently and its handlers are already finalized. Wrap them in the router if needed.
    pub fn include_router(&mut self, router: Router<B>) {
        self.router.merge(router);
    }
}

impl<B, L> std::fmt::Debug for BrokerScope<B, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrokerScope")
            .field("router", &self.router)
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
        use tokio::signal::unix::{SignalKind, signal};
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
