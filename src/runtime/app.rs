//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

use std::{future::Future, sync::Arc};

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{Broker, Subscriber, SubscriptionSource};

use super::handler::Handler;
use super::lifecycle::{BoxError, BoxFuture, BrokerLifecycle};
use super::metadata::HandlerMetadata;
use super::middleware::{Identity, Layer, Stack};
use super::router::Router;

/// A registration deferred until [`RustStream::run`]: given the shutdown token, it opens the
/// subscription (after the broker is connected) and spawns the dispatch task. The broker, source
/// and handler are captured and type-erased.
type Starter = Box<
    dyn FnOnce(CancellationToken) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>> + Send,
>;

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
/// use ruststream::runtime::{AppInfo, HandlerMetadata, HandlerResult, RustStream};
/// use ruststream::runtime::layers::TracingLayer;
///
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
///     .layer(TracingLayer::default())
///     .with_broker(MemoryBroker::new(), |b| {
///         let subscriber = b.broker().subscribe("orders");
///         b.handle(
///             subscriber,
///             |_msg: &_| async { HandlerResult::Ack },
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
            global: Stack::new(layer, self.global),
        }
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
            global: self.global.clone(),
        };
        build(&mut scope);
        let (starters, handlers) = scope.router.into_parts();
        for (bound, meta) in starters.into_iter().zip(handlers) {
            let broker = broker.clone();
            self.starters
                .push(Box::new(move |token| bound(broker, token)));
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
            brokers, starters, ..
        } = self;

        for broker in &brokers {
            broker.connect().await.map_err(RustStreamError::Connect)?;
        }

        let token = CancellationToken::new();
        let mut handles = Vec::with_capacity(starters.len());
        for starter in starters {
            let handle = starter(token.clone())
                .await
                .map_err(RustStreamError::Subscribe)?;
            handles.push(handle);
        }

        shutdown.await;
        token.cancel();

        for handle in handles {
            handle.await.map_err(RustStreamError::Join)?;
        }
        for broker in brokers.iter().rev() {
            broker.shutdown().await.map_err(RustStreamError::Shutdown)?;
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
    global: L,
}

impl<B: Broker + 'static, L> BrokerScope<B, L> {
    /// Returns the broker, for creating subscribers or publishers with its own API.
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
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
