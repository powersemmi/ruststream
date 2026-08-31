//! The [`RustStream`] builder: construction, configuration and handler registration.

use std::{
    collections::BTreeMap, error::Error as StdError, fmt, future::Future, marker::PhantomData,
    sync::Arc, time::Duration,
};

use crate::codec::Codec;
use crate::runtime::publish_source::BrokerRegistration;
use crate::{Broker, DescribeServer, ServerSpec};

use tokio_util::task::TaskTracker;

use crate::runtime::dispatch::Delivery;
use crate::runtime::lifecycle::{BoxError, BrokerCell, BrokerLifecycle, ConnectedSlot};
use crate::runtime::metadata::HandlerMetadata;
use crate::runtime::middleware::{Identity, Stack};
use crate::runtime::publish::{PublishIdentity, PublishLayer, PublishStack};
use crate::runtime::router::RouterSink;
#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

use super::scope::BrokerScope;
use super::{AppInfo, LifecycleHook, LifecyclePhase, Starter, StateInit};

/// The top-level application object.
///
/// `RustStream` binds one or more brokers, the handlers attached to each, and the service
/// lifecycle into a single runnable unit. Handlers are registered through [`with_broker`], which
/// hands a scope bound to that broker; nothing connects or subscribes until [`run`]. Brokers are
/// held type-erased (only their lifecycle), so a single service can mix broker types.
///
/// The type parameter `Layers` is the global middleware stack applied to every per-message
/// handler registered on a broker scope, whether directly or via
/// [`include_router`](BrokerScope::include_router) (batch handlers are the exception: a
/// per-message layer cannot wrap a whole-batch handler); it defaults to the no-op [`Identity`]
/// and grows as [`layer`] is called. Add all layers before [`with_broker`], since a layer only
/// applies to handlers registered after it.
///
/// The type parameter `Phase` tracks the builder phase at compile time: [`new`](Self::new)
/// starts in [`Setup`], where the state ([`on_startup`]), the middleware stack ([`layer`]) and
/// the publish pipeline ([`publish_layer`](Self::publish_layer)) are still configurable; the
/// first [`with_broker`] moves it to [`Wired`], where those methods no longer exist - so a
/// configuration call that would silently not apply to already-registered handlers does not
/// compile. `Phase` defaults to [`Wired`], the built service; `Setup` lives inside a builder
/// chain and is rarely written out.
///
/// [`with_broker`]: Self::with_broker
/// [`layer`]: Self::layer
/// [`on_startup`]: Self::on_startup
/// [`run`]: Self::run
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() -> Result<(), ruststream::runtime::RustStreamError> {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, Context, HandlerMetadata, HandlerOutcome, RustStream};
/// use ruststream::runtime::layers::TracingLayer;
///
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
///     .layer(TracingLayer::default())
///     .with_broker(MemoryBroker::new(), |b| {
///         let subscriber = b.broker().subscribe("orders");
///         b.handle(
///             subscriber,
///             |_msg: &_, _ctx: &mut Context| async { HandlerOutcome::ack() },
///             HandlerMetadata::raw("orders"),
///         );
///     });
/// app.run().await
/// # }
/// ```
pub struct RustStream<Layers = Identity, State = (), Pipeline = PublishIdentity, Phase = Wired> {
    pub(super) info: AppInfo,
    pub(super) brokers: Vec<RegisteredBroker>,
    pub(super) starters: Vec<Starter<State>>,
    pub(super) handlers: Vec<HandlerMetadata>,
    pub(super) servers: BTreeMap<String, ServerSpec>,
    pub(super) publish_pipeline: Pipeline,
    pub(super) state_init: StateInit<State>,
    pub(super) after_startup: Vec<LifecycleHook<State>>,
    pub(super) on_shutdown: Vec<LifecycleHook<State>>,
    pub(super) after_shutdown: Vec<LifecycleHook<State>>,
    pub(super) shutdown_timeout: Option<Duration>,
    /// Tracks post-settle `and_after` continuations spawned during dispatch, so a graceful
    /// shutdown drains them after the dispatch loops stop. Shared (cloned) into every
    /// [`Delivery`].
    pub(super) continuations: TaskTracker,
    /// Shared recording-and-quiescence hooks for the [`TestApp`](crate::testing::TestApp) harness,
    /// cloned into every scope's [`Delivery`]. Empty until a harness installs a coordinator, so a
    /// non-harness run with the `testing` feature enabled stays inert.
    #[cfg(feature = "testing")]
    pub(super) test_hooks: Arc<TestHooks>,
    pub(super) global: Layers,
    // `fn() -> Phase` keeps the marker out of auto-trait and variance considerations, matching
    // the router builder's broker marker.
    pub(super) phase: PhantomData<fn() -> Phase>,
}

/// [`RustStream`] phase marker: the builder is still being configured - the state type, the
/// middleware stack, and the publish pipeline may change, and no broker is registered yet.
#[derive(Debug, Clone, Copy, Default)]
pub struct Setup;

/// [`RustStream`] phase marker: at least one broker (with its handlers) is registered, so the
/// state type, the middleware stack, and the publish pipeline are fixed.
#[derive(Debug, Clone, Copy, Default)]
pub struct Wired;

/// A broker held by the app for lifecycle management, paired with its optional label.
///
/// The label is the broker's stable runtime identity and its `AsyncAPI` server name; it is `Some`
/// for a broker registered through [`with_broker_labeled`](RustStream::with_broker_labeled) (or its
/// codec variant) and `None` otherwise.
pub(crate) struct RegisteredBroker {
    pub(crate) lifecycle: Box<dyn BrokerLifecycle>,
    pub(crate) label: Option<String>,
}

/// The internals the [`TestApp`](crate::testing::TestApp) harness needs to drive an app without
/// connecting: the brokers (to recover and instrument), the deferred starters, the lifecycle hooks,
/// and the shared test hooks slot. Produced by [`RustStream::into_test_parts`].
#[cfg(feature = "testing")]
pub(crate) struct TestParts<State> {
    pub(crate) brokers: Vec<RegisteredBroker>,
    pub(crate) starters: Vec<Starter<State>>,
    pub(crate) state_init: StateInit<State>,
    pub(crate) after_startup: Vec<LifecycleHook<State>>,
    pub(crate) shutdown_timeout: Option<Duration>,
    pub(crate) continuations: TaskTracker,
    pub(crate) test_hooks: Arc<TestHooks>,
}

impl<Layers, State, Pipeline, Phase> fmt::Debug for RustStream<Layers, State, Pipeline, Phase> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RustStream")
            .field("info", &self.info)
            .field("brokers", &self.brokers.len())
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl RustStream<Identity, (), PublishIdentity, Setup> {
    /// Creates an empty service with the given metadata, no global middleware, and the unit
    /// application state `()`. Produce a typed state with [`on_startup`](Self::on_startup).
    #[must_use]
    pub fn new(info: AppInfo) -> Self {
        Self {
            info,
            brokers: Vec::new(),
            starters: Vec::new(),
            handlers: Vec::new(),
            servers: BTreeMap::new(),
            publish_pipeline: PublishIdentity,
            state_init: Box::new(|| Box::pin(async { Ok(()) })),
            after_startup: Vec::new(),
            on_shutdown: Vec::new(),
            after_shutdown: Vec::new(),
            shutdown_timeout: None,
            continuations: TaskTracker::new(),
            #[cfg(feature = "testing")]
            test_hooks: Arc::new(TestHooks::detached()),
            global: Identity,
            phase: PhantomData,
        }
    }
}

impl<Layers, State, Pipeline> RustStream<Layers, State, Pipeline, Setup> {
    /// Adds a global middleware layer, applied to every handler registered later.
    ///
    /// The first layer added runs outermost. Only available before the first
    /// [`with_broker`](Self::with_broker): a layer added later could not wrap the handlers
    /// already registered, so that ordering does not compile.
    #[must_use]
    pub fn layer<N>(self, layer: N) -> RustStream<Stack<N, Layers>, State, Pipeline, Setup> {
        RustStream {
            info: self.info,
            brokers: self.brokers,
            starters: self.starters,
            handlers: self.handlers,
            servers: self.servers,
            publish_pipeline: self.publish_pipeline,
            state_init: self.state_init,
            after_startup: self.after_startup,
            on_shutdown: self.on_shutdown,
            after_shutdown: self.after_shutdown,
            shutdown_timeout: self.shutdown_timeout,
            continuations: self.continuations,
            #[cfg(feature = "testing")]
            test_hooks: self.test_hooks,
            global: Stack::new(layer, self.global),
            phase: PhantomData,
        }
    }

    /// Produces the typed application state at startup, transitioning the app's state type from the
    /// previous `State` to `State2`.
    ///
    /// The hook runs once before brokers connect; its future can `await` (open a database pool,
    /// connect a client), and the produced `State2` is shared with every handler (read via
    /// [`Context::state`](crate::runtime::Context::state)) and the read-only lifecycle hooks. A
    /// failing hook aborts startup. The initial state is `()` (from [`new`](Self::new)), so the
    /// first call's hook receives `()`.
    ///
    /// Only available before the first [`with_broker`](Self::with_broker): the call fixes the
    /// app's state type, and handlers registered against a different state type could not be
    /// carried across - so that ordering does not compile.
    ///
    /// # Panics
    ///
    /// Panics if a lifecycle hook ([`after_startup`](Self::after_startup),
    /// [`on_shutdown`](Self::on_shutdown), [`after_shutdown`](Self::after_shutdown)) was
    /// registered first: hooks close over the state type and cannot be carried across the state
    /// change. Register hooks after `on_startup`.
    #[must_use]
    pub fn on_startup<F, Fut, State2, E>(
        self,
        hook: F,
    ) -> RustStream<Layers, State2, Pipeline, Setup>
    where
        F: FnOnce(State) -> Fut + Send + 'static,
        Fut: Future<Output = Result<State2, E>> + Send,
        State: Send + 'static,
        State2: Send + Sync + 'static,
        E: StdError + Send + Sync + 'static,
    {
        assert!(
            self.after_startup.is_empty()
                && self.on_shutdown.is_empty()
                && self.after_shutdown.is_empty(),
            "on_startup must be called before lifecycle hooks are registered: hooks close over \
             the state type and cannot be carried across the state change"
        );
        let prev = self.state_init;
        RustStream {
            info: self.info,
            brokers: self.brokers,
            // Provably empty: with_broker (the only way to add starters) leaves Setup.
            starters: Vec::new(),
            handlers: self.handlers,
            servers: self.servers,
            publish_pipeline: self.publish_pipeline,
            state_init: Box::new(move || {
                Box::pin(async move {
                    let prev_state = prev().await?;
                    hook(prev_state).await.map_err(|e| Box::new(e) as BoxError)
                })
            }),
            after_startup: Vec::new(),
            on_shutdown: Vec::new(),
            after_shutdown: Vec::new(),
            shutdown_timeout: self.shutdown_timeout,
            continuations: self.continuations,
            #[cfg(feature = "testing")]
            test_hooks: self.test_hooks,
            global: self.global,
            phase: PhantomData,
        }
    }

    /// Adds an outgoing publish middleware, run on every published reply before it reaches the
    /// broker (a Confluent / Avro envelope, publish metrics, dead-letter). It composes into the
    /// pipeline type parameter, so the *last* one added wraps the rest and runs outermost (unlike the
    /// consume-side [`layer`](Self::layer), where the first added is outermost); the middleware must
    /// be [`Clone`] (the pipeline is cloned into each publishing handler). Only available before
    /// the first [`with_broker`](Self::with_broker): a middleware added later could not wrap the
    /// publishers already handed out, so that ordering does not compile.
    #[must_use]
    pub fn publish_layer<M>(
        self,
        middleware: M,
    ) -> RustStream<Layers, State, PublishStack<M, Pipeline>, Setup>
    where
        M: PublishLayer + Clone + 'static,
    {
        // Prepend `middleware` as the new outermost wrapper: the publish pipeline stays a statically
        // composed type (no `dyn` dispatch), and the last one added runs outermost.
        RustStream {
            info: self.info,
            brokers: self.brokers,
            starters: self.starters,
            handlers: self.handlers,
            servers: self.servers,
            publish_pipeline: PublishStack::new(middleware, self.publish_pipeline),
            state_init: self.state_init,
            after_startup: self.after_startup,
            on_shutdown: self.on_shutdown,
            after_shutdown: self.after_shutdown,
            shutdown_timeout: self.shutdown_timeout,
            continuations: self.continuations,
            #[cfg(feature = "testing")]
            test_hooks: self.test_hooks,
            global: self.global,
            phase: PhantomData,
        }
    }
}

impl<Layers, State, Pipeline, Phase> RustStream<Layers, State, Pipeline, Phase> {
    /// Rebuilds the app under a different phase marker; the fields move as they are.
    fn into_phase<Q>(self) -> RustStream<Layers, State, Pipeline, Q> {
        RustStream {
            info: self.info,
            brokers: self.brokers,
            starters: self.starters,
            handlers: self.handlers,
            servers: self.servers,
            publish_pipeline: self.publish_pipeline,
            state_init: self.state_init,
            after_startup: self.after_startup,
            on_shutdown: self.on_shutdown,
            after_shutdown: self.after_shutdown,
            shutdown_timeout: self.shutdown_timeout,
            continuations: self.continuations,
            #[cfg(feature = "testing")]
            test_hooks: self.test_hooks,
            global: self.global,
            phase: PhantomData,
        }
    }

    /// Adds a hook run after brokers connect and handlers are spawned (for example, to publish an
    /// initial message or signal readiness). A failing hook aborts startup.
    #[must_use]
    pub fn after_startup<F, Fut, E>(self, hook: F) -> Self
    where
        State: Send + Sync + 'static,
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
        State: Send + Sync + 'static,
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
        State: Send + Sync + 'static,
        F: FnOnce(Arc<State>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.push_lifecycle_hook(LifecyclePhase::AfterShutdown, hook)
    }

    fn push_lifecycle_hook<F, Fut, E>(mut self, phase: LifecyclePhase, hook: F) -> Self
    where
        State: Send + Sync + 'static,
        F: FnOnce(Arc<State>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        let boxed: LifecycleHook<State> = Box::new(move |state| {
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
    /// triggered. After the timeout, the remaining handler tasks are aborted. The same bound then
    /// applies to draining post-settle `and_after` continuations; on timeout they are abandoned
    /// (they are at-most-once side effects, so this loses follow-up work, never a settlement).
    /// Defaults to waiting indefinitely.
    #[must_use]
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    /// Registers a broker for lifecycle management only (connect / shutdown), without attaching
    /// subscribers. Use for publish-only brokers.
    #[must_use]
    pub fn register_broker<R>(mut self, broker: R) -> Self
    where
        R: BrokerRegistration,
    {
        // No subscriptions read the slot, but a Bindable registration's tokens do.
        let (broker, slot) = broker.into_parts();
        self.brokers.push(RegisteredBroker {
            lifecycle: Box::new(BrokerCell { broker, slot }),
            label: None,
        });
        self
    }

    /// Records an `AsyncAPI` server (one per broker the service connects to).
    ///
    /// Build the [`ServerSpec`] directly, or get it from a broker that implements
    /// [`DescribeServer`](crate::DescribeServer): `app.server("nats", broker.describe_server())`.
    /// `build_spec` emits these in the document's `servers` section.
    ///
    /// For a self-describing broker, prefer
    /// [`with_broker_labeled`](Self::with_broker_labeled), which derives this entry from the broker
    /// under its label in one step. Use this method for brokers without a network address (the
    /// in-memory broker), or to override a labeled broker's own spec.
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
    /// The scope carries no codec of its own, so macro handlers mounted with `b.include(handle)`
    /// decode with the [`DefaultCodec`](crate::codec::DefaultCodec). To decode with another codec
    /// scope-wide, use [`with_broker_codec`](Self::with_broker_codec).
    ///
    /// The first call moves the builder to the [`Wired`] phase: the state, the middleware stack,
    /// and the publish pipeline are fixed from here on, so a configuration call that could not
    /// apply to this broker's handlers does not compile.
    #[must_use]
    pub fn with_broker<R, F>(
        self,
        broker: R,
        build: F,
    ) -> RustStream<Layers, State, Pipeline, Wired>
    where
        R: BrokerRegistration,
        Layers: Clone,
        Pipeline: Clone,
        State: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<R::Broker, Layers, (), State, Pipeline>),
    {
        let mut this = self.into_phase::<Wired>();
        let (broker, slot) = broker.into_parts();
        let mut scope = this.new_scope(broker, slot, ());
        build(&mut scope);
        this.collect_scope(scope, None);
        this
    }

    /// Registers a broker with a scope-wide `codec`, replacing the
    /// [`DefaultCodec`](crate::codec::DefaultCodec) a codec-less scope decodes with.
    ///
    /// `build` receives a [`BrokerScope`] whose [`include`](BrokerScope::include) family reads
    /// the same as on a codec-less scope (`b.include(handle)`) but decodes with `codec`.
    #[must_use]
    pub fn with_broker_codec<R, C, F>(
        self,
        broker: R,
        codec: C,
        build: F,
    ) -> RustStream<Layers, State, Pipeline, Wired>
    where
        R: BrokerRegistration,
        C: Codec + Clone + 'static,
        Layers: Clone,
        Pipeline: Clone,
        State: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<R::Broker, Layers, C, State, Pipeline>),
    {
        let mut this = self.into_phase::<Wired>();
        let (broker, slot) = broker.into_parts();
        let mut scope = this.new_scope(broker, slot, codec);
        build(&mut scope);
        this.collect_scope(scope, None);
        this
    }

    /// Registers a self-describing broker under `label`, along with the handlers attached to it.
    ///
    /// The `label` is the broker's stable identity in the service and the name of its `AsyncAPI`
    /// server: the broker's [`describe_server`](DescribeServer::describe_server) coordinates are
    /// recorded in the `servers` map under `label`, so the document stays in sync with the brokers
    /// actually mounted, with no separate [`server`](Self::server) call. An explicit
    /// [`server(label, spec)`](Self::server) entry for the same label takes precedence.
    ///
    /// Like [`with_broker`](Self::with_broker), the scope decodes with the
    /// [`DefaultCodec`](crate::codec::DefaultCodec); use
    /// [`with_broker_labeled_codec`](Self::with_broker_labeled_codec) to name another.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "memory")]
    /// # async fn run() -> Result<(), ruststream::runtime::RustStreamError> {
    /// use ruststream::runtime::{AppInfo, RustStream};
    /// use ruststream::{Broker, DescribeServer, ServerSpec};
    ///
    /// # struct NatsBroker;
    /// # struct ConnectedNats;
    /// # impl NatsBroker { fn new(_: &str) -> Self { Self } }
    /// # impl Broker for NatsBroker {
    /// #     type Error = std::io::Error;
    /// #     type Connected = ConnectedNats;
    /// #     async fn connect(self) -> Result<ConnectedNats, Self::Error> { Ok(ConnectedNats) }
    /// # }
    /// # impl ruststream::ConnectedBroker for ConnectedNats {
    /// #     type Error = std::io::Error;
    /// #     type Closed = ();
    /// #     async fn shutdown(self) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl DescribeServer for NatsBroker {
    /// #     fn describe_server(&self) -> ServerSpec { ServerSpec::new("nats:4222", "nats") }
    /// # }
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker_labeled("ingress", NatsBroker::new("nats://localhost"), |_b| {});
    /// // The AsyncAPI `servers` map now carries "ingress" with the broker's host / protocol.
    /// app.run().await
    /// # }
    /// ```
    #[must_use]
    pub fn with_broker_labeled<R, F>(
        self,
        label: impl Into<String>,
        broker: R,
        build: F,
    ) -> RustStream<Layers, State, Pipeline, Wired>
    where
        R: BrokerRegistration,
        R::Broker: DescribeServer,
        Layers: Clone,
        Pipeline: Clone,
        State: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<R::Broker, Layers, (), State, Pipeline>),
    {
        let mut this = self.into_phase::<Wired>();
        let (broker, slot) = broker.into_parts();
        let label = this.record_server(label, &broker);
        let mut scope = this.new_scope(broker, slot, ());
        build(&mut scope);
        this.collect_scope(scope, Some(label));
        this
    }

    /// Registers a self-describing broker under `label` with a default `codec`.
    ///
    /// Combines [`with_broker_labeled`](Self::with_broker_labeled) (the label is the broker's
    /// identity and `AsyncAPI` server name) with
    /// [`with_broker_codec`](Self::with_broker_codec) (the scope's macro handlers decode with
    /// `codec`).
    #[must_use]
    pub fn with_broker_labeled_codec<R, C, F>(
        self,
        label: impl Into<String>,
        broker: R,
        codec: C,
        build: F,
    ) -> RustStream<Layers, State, Pipeline, Wired>
    where
        R: BrokerRegistration,
        R::Broker: DescribeServer,
        C: Codec + Clone + 'static,
        Layers: Clone,
        Pipeline: Clone,
        State: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<R::Broker, Layers, C, State, Pipeline>),
    {
        let mut this = self.into_phase::<Wired>();
        let (broker, slot) = broker.into_parts();
        let label = this.record_server(label, &broker);
        let mut scope = this.new_scope(broker, slot, codec);
        build(&mut scope);
        this.collect_scope(scope, Some(label));
        this
    }

    /// Records `broker`'s server coordinates under `label` (keeping an explicit
    /// [`server`](Self::server) entry already set for the same label), returning the owned label.
    fn record_server<B: DescribeServer>(&mut self, label: impl Into<String>, broker: &B) -> String {
        let label = label.into();
        self.servers
            .entry(label.clone())
            .or_insert_with(|| broker.describe_server());
        label
    }

    /// Builds a fresh scope bound to `broker` carrying `codec` and the app's publishers / pipeline.
    fn new_scope<B, C>(
        &self,
        broker: B,
        slot: ConnectedSlot<B>,
        codec: C,
    ) -> BrokerScope<B, Layers, C, State, Pipeline>
    where
        B: Broker + 'static,
        Layers: Clone,
        Pipeline: Clone,
        State: Send + Sync + 'static,
    {
        BrokerScope {
            broker,
            slot,
            startup_hooks: Vec::new(),
            sink: RouterSink::new(),
            pipeline: self.publish_pipeline.clone(),
            retry_publisher: None,
            global: self.global.clone(),
            codec,
        }
    }

    /// Drains a built scope's registrations into the app and holds the broker for lifecycle,
    /// recording `label` as the broker's stable runtime identity (`None` when unlabeled).
    ///
    /// The broker itself is boxed into a [`BrokerCell`] whose consuming `connect` publishes the
    /// typed connected form into a shared slot; each starter reads the slot at startup, after
    /// every broker connected and before any subscription opens.
    fn collect_scope<B, C>(
        &mut self,
        scope: BrokerScope<B, Layers, C, State, Pipeline>,
        label: Option<String>,
    ) where
        B: Broker + 'static,
        State: Send + Sync + 'static,
    {
        let BrokerScope {
            broker,
            slot,
            startup_hooks,
            sink,
            retry_publisher,
            ..
        } = scope;
        self.after_startup.extend(startup_hooks);
        // The scope id is the index this broker will occupy once pushed below; the harness uses it
        // to scope recorded deliveries per broker.
        #[cfg(feature = "testing")]
        let delivery = Arc::new(Delivery::instrumented(
            retry_publisher,
            self.continuations.clone(),
            self.test_hooks.clone(),
            self.brokers.len(),
        ));
        #[cfg(not(feature = "testing"))]
        let delivery = Arc::new(Delivery::detached(
            retry_publisher,
            self.continuations.clone(),
        ));
        let (starters, handlers) = sink.into_parts();
        for (bound, meta) in starters.into_iter().zip(handlers) {
            let slot = Arc::clone(&slot);
            let delivery = delivery.clone();
            self.starters.push(Box::new(move |state, shutdown, token| {
                let connected = slot
                    .lock()
                    .expect("connected slot mutex poisoned")
                    .clone()
                    .expect("brokers connect before subscriptions open");
                bound(connected, state, delivery, shutdown, token)
            }));
            self.handlers.push(meta);
        }
        self.brokers.push(RegisteredBroker {
            lifecycle: Box::new(BrokerCell { broker, slot }),
            label,
        });
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

    /// Decomposes the app into the pieces the [`TestApp`](crate::testing::TestApp) harness drives:
    /// the brokers, the deferred starters, the lifecycle hooks, and the shared test-hooks slot.
    /// Handlers metadata and `AsyncAPI` servers are dropped (the harness does not need them).
    #[cfg(feature = "testing")]
    pub(crate) fn into_test_parts(self) -> TestParts<State> {
        TestParts {
            brokers: self.brokers,
            starters: self.starters,
            state_init: self.state_init,
            after_startup: self.after_startup,
            shutdown_timeout: self.shutdown_timeout,
            continuations: self.continuations,
            test_hooks: self.test_hooks,
        }
    }
}
