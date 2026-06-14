//! The [`RustStream`] builder: construction, configuration and handler registration.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error as StdError,
    future::Future,
    sync::Arc,
    time::Duration,
};

use crate::codec::Codec;
use crate::{Broker, Publisher, ServerSpec};

use crate::runtime::context::State;
use crate::runtime::dispatch::{Delivery, Publishers};
use crate::runtime::lifecycle::{BoxError, BrokerLifecycle};
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{Identity, Stack};
use crate::runtime::publish::PublishMiddleware;
use crate::runtime::router::RouterSink;

use super::scope::BrokerScope;
use super::{AppInfo, LifecycleHook, LifecyclePhase, Starter, StartupHook};

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
    pub(super) info: AppInfo,
    pub(super) brokers: Vec<Arc<dyn BrokerLifecycle>>,
    pub(super) starters: Vec<Starter>,
    pub(super) handlers: Vec<HandlerMetadata>,
    pub(super) servers: BTreeMap<String, ServerSpec>,
    pub(super) publishers: Publishers,
    pub(super) publish_layers: Vec<Arc<dyn PublishMiddleware>>,
    pub(super) state: State,
    pub(super) on_startup: Vec<StartupHook>,
    pub(super) after_startup: Vec<LifecycleHook>,
    pub(super) on_shutdown: Vec<LifecycleHook>,
    pub(super) after_shutdown: Vec<LifecycleHook>,
    pub(super) shutdown_timeout: Option<Duration>,
    pub(super) global: L,
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
    /// [`Context::state`](crate::runtime::Context::state) then
    /// [`State::get`](crate::runtime::State::get). For data scoped to a single delivery, use the
    /// per-delivery extensions ([`Context::insert`](crate::runtime::Context::insert) /
    /// [`Context::get`](crate::runtime::Context::get)) instead.
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
            retry_publisher: None,
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
            retry_publisher: scope.retry_publisher.clone(),
        });
        let (starters, handlers) = scope.sink.into_parts();
        for (bound, meta) in starters.into_iter().zip(handlers) {
            let broker = broker.clone();
            let delivery = delivery.clone();
            self.starters.push(Box::new(move |state, shutdown, token| {
                bound(broker, state, delivery, shutdown, token)
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
}
