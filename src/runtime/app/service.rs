//! The [`RustStream`] builder: construction, configuration and handler registration.

use std::{
    collections::BTreeMap, error::Error as StdError, future::Future, sync::Arc, time::Duration,
};

use crate::codec::Codec;
use crate::{Broker, DescribeServer, ServerSpec};

use tokio_util::task::TaskTracker;

use crate::runtime::dispatch::Delivery;
use crate::runtime::lifecycle::{BoxError, BrokerLifecycle};
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
pub struct RustStream<L = Identity, St = (), PP = PublishIdentity> {
    pub(super) info: AppInfo,
    pub(super) brokers: Vec<RegisteredBroker>,
    pub(super) starters: Vec<Starter<St>>,
    pub(super) handlers: Vec<HandlerMetadata>,
    pub(super) servers: BTreeMap<String, ServerSpec>,
    pub(super) publish_pipeline: PP,
    pub(super) state_init: StateInit<St>,
    pub(super) after_startup: Vec<LifecycleHook<St>>,
    pub(super) on_shutdown: Vec<LifecycleHook<St>>,
    pub(super) after_shutdown: Vec<LifecycleHook<St>>,
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
    pub(super) global: L,
}

/// A broker held by the app for lifecycle management, paired with its optional label.
///
/// The label is the broker's stable runtime identity and its `AsyncAPI` server name; it is `Some`
/// for a broker registered through [`with_broker_labeled`](RustStream::with_broker_labeled) (or its
/// codec variant) and `None` otherwise.
pub(crate) struct RegisteredBroker {
    pub(crate) lifecycle: Arc<dyn BrokerLifecycle>,
    pub(crate) label: Option<String>,
}

/// The internals the [`TestApp`](crate::testing::TestApp) harness needs to drive an app without
/// connecting: the brokers (to recover and instrument), the deferred starters, the lifecycle hooks,
/// and the shared test hooks slot. Produced by [`RustStream::into_test_parts`].
#[cfg(feature = "testing")]
pub(crate) struct TestParts<St> {
    pub(crate) brokers: Vec<RegisteredBroker>,
    pub(crate) starters: Vec<Starter<St>>,
    pub(crate) state_init: StateInit<St>,
    pub(crate) after_startup: Vec<LifecycleHook<St>>,
    pub(crate) shutdown_timeout: Option<Duration>,
    pub(crate) continuations: TaskTracker,
    pub(crate) test_hooks: Arc<TestHooks>,
}

impl<L, St, PP> std::fmt::Debug for RustStream<L, St, PP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RustStream")
            .field("info", &self.info)
            .field("brokers", &self.brokers.len())
            .field("handlers", &self.handlers.len())
            .finish_non_exhaustive()
    }
}

impl RustStream<Identity, (), PublishIdentity> {
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
        }
    }
}

impl<L, St, PP> RustStream<L, St, PP> {
    /// Adds a global middleware layer, applied to every handler registered after it.
    ///
    /// The first layer added runs outermost. Call before [`with_broker`](Self::with_broker).
    #[must_use]
    pub fn layer<N>(self, layer: N) -> RustStream<Stack<N, L>, St, PP> {
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
        }
    }

    /// Produces the typed application state at startup, transitioning the app's state type from the
    /// previous `St` to `St2`.
    ///
    /// The hook runs once before brokers connect; its future can `await` (open a database pool,
    /// connect a client), and the produced `St2` is shared with every handler (read via
    /// [`Context::state`](crate::runtime::Context::state)) and the read-only lifecycle hooks. A
    /// failing hook aborts startup. The initial state is `()` (from [`new`](Self::new)), so the
    /// first call's hook receives `()`.
    ///
    /// Call this BEFORE registering handlers or read-only hooks: it fixes the app's state type, and
    /// registrations made earlier (which saw a different state type) are not carried across.
    #[must_use]
    pub fn on_startup<F, Fut, St2, E>(self, hook: F) -> RustStream<L, St2, PP>
    where
        F: FnOnce(St) -> Fut + Send + 'static,
        Fut: Future<Output = Result<St2, E>> + Send,
        St: Send + 'static,
        St2: Send + Sync + 'static,
        E: StdError + Send + Sync + 'static,
    {
        let prev = self.state_init;
        RustStream {
            info: self.info,
            brokers: self.brokers,
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
        }
    }

    /// Adds a hook run after brokers connect and handlers are spawned (for example, to publish an
    /// initial message or signal readiness). A failing hook aborts startup.
    #[must_use]
    pub fn after_startup<F, Fut, E>(self, hook: F) -> Self
    where
        St: Send + Sync + 'static,
        F: FnOnce(Arc<St>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.push_lifecycle_hook(LifecyclePhase::AfterStartup, hook)
    }

    /// Adds a hook run when shutdown begins, while brokers are still connected. Errors are logged.
    #[must_use]
    pub fn on_shutdown<F, Fut, E>(self, hook: F) -> Self
    where
        St: Send + Sync + 'static,
        F: FnOnce(Arc<St>) -> Fut + Send + 'static,
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
        St: Send + Sync + 'static,
        F: FnOnce(Arc<St>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        self.push_lifecycle_hook(LifecyclePhase::AfterShutdown, hook)
    }

    fn push_lifecycle_hook<F, Fut, E>(mut self, phase: LifecyclePhase, hook: F) -> Self
    where
        St: Send + Sync + 'static,
        F: FnOnce(Arc<St>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
        E: StdError + Send + Sync + 'static,
    {
        let boxed: LifecycleHook<St> = Box::new(move |state| {
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

    /// Adds an outgoing publish middleware, run on every published reply before it reaches the
    /// broker (a Confluent / Avro envelope, publish metrics, dead-letter). It composes into the
    /// pipeline type parameter, so the *last* one added wraps the rest and runs outermost (unlike the
    /// consume-side [`layer`](Self::layer), where the first added is outermost); the middleware must
    /// be [`Clone`] (the pipeline is cloned into each publishing handler). Call before
    /// [`with_broker`](Self::with_broker).
    #[must_use]
    pub fn publish_layer<M>(self, middleware: M) -> RustStream<L, St, PublishStack<M, PP>>
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
        }
    }

    /// Registers a broker for lifecycle management only (connect / shutdown), without attaching
    /// subscribers. Use for publish-only brokers.
    #[must_use]
    pub fn register_broker<B>(mut self, broker: B) -> Self
    where
        B: Broker + 'static,
    {
        self.brokers.push(RegisteredBroker {
            lifecycle: Arc::new(broker),
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
    /// The scope has no default codec, so macro handlers are mounted with an explicit one
    /// (`b.include(handle, JsonCodec)`). To set a scope default and drop the per-call codec, use
    /// [`with_broker_codec`](Self::with_broker_codec).
    #[must_use]
    pub fn with_broker<B, F>(mut self, broker: B, build: F) -> Self
    where
        B: Broker + 'static,
        L: Clone,
        PP: Clone,
        St: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<B, L, (), St, PP>),
    {
        let broker = Arc::new(broker);
        let mut scope = self.new_scope(&broker, ());
        build(&mut scope);
        self.collect_scope(&broker, scope, None);
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
        PP: Clone,
        St: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<B, L, C, St, PP>),
    {
        let broker = Arc::new(broker);
        let mut scope = self.new_scope(&broker, codec);
        build(&mut scope);
        self.collect_scope(&broker, scope, None);
        self
    }

    /// Registers a self-describing broker under `label`, along with the handlers attached to it.
    ///
    /// The `label` is the broker's stable identity in the service and the name of its `AsyncAPI`
    /// server: the broker's [`describe_server`](DescribeServer::describe_server) coordinates are
    /// recorded in the `servers` map under `label`, so the document stays in sync with the brokers
    /// actually mounted, with no separate [`server`](Self::server) call. An explicit
    /// [`server(label, spec)`](Self::server) entry for the same label takes precedence.
    ///
    /// Like [`with_broker`](Self::with_broker), the scope has no default codec; use
    /// [`with_broker_labeled_codec`](Self::with_broker_labeled_codec) to set one.
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
    /// # impl NatsBroker { fn new(_: &str) -> Self { Self } }
    /// # impl Broker for NatsBroker {
    /// #     type Error = std::io::Error;
    /// #     async fn connect(&self) -> Result<(), Self::Error> { Ok(()) }
    /// #     async fn shutdown(&self) -> Result<(), Self::Error> { Ok(()) }
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
    pub fn with_broker_labeled<B, F>(
        mut self,
        label: impl Into<String>,
        broker: B,
        build: F,
    ) -> Self
    where
        B: DescribeServer + 'static,
        L: Clone,
        PP: Clone,
        St: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<B, L, (), St, PP>),
    {
        let label = self.record_server(label, &broker);
        let broker = Arc::new(broker);
        let mut scope = self.new_scope(&broker, ());
        build(&mut scope);
        self.collect_scope(&broker, scope, Some(label));
        self
    }

    /// Registers a self-describing broker under `label` with a default `codec`.
    ///
    /// Combines [`with_broker_labeled`](Self::with_broker_labeled) (the label is the broker's
    /// identity and `AsyncAPI` server name) with
    /// [`with_broker_codec`](Self::with_broker_codec) (macro handlers mount without repeating the
    /// codec).
    #[must_use]
    pub fn with_broker_labeled_codec<B, C, F>(
        mut self,
        label: impl Into<String>,
        broker: B,
        codec: C,
        build: F,
    ) -> Self
    where
        B: DescribeServer + 'static,
        C: Codec + Clone + 'static,
        L: Clone,
        PP: Clone,
        St: Send + Sync + 'static,
        F: FnOnce(&mut BrokerScope<B, L, C, St, PP>),
    {
        let label = self.record_server(label, &broker);
        let broker = Arc::new(broker);
        let mut scope = self.new_scope(&broker, codec);
        build(&mut scope);
        self.collect_scope(&broker, scope, Some(label));
        self
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
    fn new_scope<B, C>(&self, broker: &Arc<B>, codec: C) -> BrokerScope<B, L, C, St, PP>
    where
        B: Broker + 'static,
        L: Clone,
        PP: Clone,
        St: Send + Sync + 'static,
    {
        BrokerScope {
            broker: broker.clone(),
            sink: RouterSink::new(),
            pipeline: self.publish_pipeline.clone(),
            retry_publisher: None,
            global: self.global.clone(),
            codec,
        }
    }

    /// Drains a built scope's registrations into the app and holds the broker for lifecycle,
    /// recording `label` as the broker's stable runtime identity (`None` when unlabeled).
    fn collect_scope<B, C>(
        &mut self,
        broker: &Arc<B>,
        scope: BrokerScope<B, L, C, St, PP>,
        label: Option<String>,
    ) where
        B: Broker + 'static,
        St: Send + Sync + 'static,
    {
        let lifecycle: Arc<dyn BrokerLifecycle> = broker.clone();
        // The scope id is the index this broker will occupy once pushed below; the harness uses it
        // to scope recorded deliveries per broker.
        #[cfg(feature = "testing")]
        let delivery = Arc::new(Delivery::instrumented(
            scope.retry_publisher.clone(),
            self.continuations.clone(),
            self.test_hooks.clone(),
            self.brokers.len(),
        ));
        #[cfg(not(feature = "testing"))]
        let delivery = Arc::new(Delivery::detached(
            scope.retry_publisher.clone(),
            self.continuations.clone(),
        ));
        let (starters, handlers) = scope.sink.into_parts();
        for (bound, meta) in starters.into_iter().zip(handlers) {
            let broker = broker.clone();
            let delivery = delivery.clone();
            self.starters.push(Box::new(move |state, shutdown, token| {
                bound(broker, state, delivery, shutdown, token)
            }));
            self.handlers.push(meta);
        }
        self.brokers.push(RegisteredBroker { lifecycle, label });
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
    pub(crate) fn into_test_parts(self) -> TestParts<St> {
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
