//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

mod app_trait;
mod health;
mod include;
mod run;
mod scope;
mod service;

pub use app_trait::App;
pub use health::{HealthProbe, HealthState};
pub use include::{
    IncludeBatchOut, IncludeBatchPublishing, IncludeDef, IncludeOut, IncludePublishing, forms,
};
pub use run::RunningApp;
pub use scope::BrokerScope;
#[cfg(feature = "testing")]
pub(crate) use service::{RegisteredBroker, TestParts};
pub use service::{RustStream, Setup, Wired};

use std::sync::Arc;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::runtime::failure::ErrorShutdown;
use crate::runtime::lifecycle::{BoxError, BoxFuture};

/// A registration deferred until [`RustStream::run`]: given the app's error-shutdown handle and the
/// shutdown token, it opens the subscription (after the broker is connected) and spawns the dispatch
/// task. The broker, source and handler are captured and type-erased.
pub(crate) type Starter<St> = Box<
    dyn FnOnce(
            Arc<St>,
            ErrorShutdown,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The state initializer: produces the app state `St` once at startup (before brokers connect).
/// The `on_startup` producer chain; a failing initializer aborts startup. The default is the unit
/// state `()`.
pub(crate) type StateInit<St> =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<St, BoxError>> + Send>;

/// A read-only lifespan hook (`after_startup` / `on_shutdown` / `after_shutdown`): runs once at the
/// corresponding lifecycle point with a shared `Arc<St>` handle to the app state.
pub(crate) type LifecycleHook<St> =
    Box<dyn FnOnce(Arc<St>) -> BoxFuture<'static, Result<(), BoxError>> + Send>;

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
    /// A broker failed to [`connect`](crate::Broker::connect) at startup.
    #[error("broker connect failed: {0}")]
    Connect(#[source] BoxError),
    /// An `on_startup` or `after_startup` lifespan hook failed.
    #[error("startup hook failed: {0}")]
    Startup(#[source] BoxError),
    /// A subscription failed to open after connect.
    #[error("subscription failed: {0}")]
    Subscribe(#[source] BoxError),
    /// A broker failed to [`shutdown`](crate::ConnectedBroker::shutdown) during graceful
    /// shutdown.
    #[error("broker shutdown failed: {0}")]
    Shutdown(#[source] BoxError),
    /// A dispatch task panicked or was aborted.
    #[error("dispatch task failed: {0}")]
    Join(#[source] tokio::task::JoinError),
    /// A subscriber hit a fail-fast failure (a handler panic, or a decode failure under
    /// `on_failure(decode = fail_fast)`) and tore the service down. The string names the
    /// subscription and the reason.
    #[error("dispatch failed: {0}")]
    Dispatch(String),
}

/// Boxing helpers for scope-registered lifecycle hooks.
pub(super) mod lifecycle_hooks {
    use std::{error::Error as StdError, future::Future, sync::Arc};

    use crate::runtime::lifecycle::{BoxError, ConnectedSlot};
    use crate::runtime::publish_source::pair_bound;
    use crate::{Broker, Connected, PublishPolicy};

    use super::LifecycleHook;

    /// Erases a scope-level startup publish (pair `source`, run `hook` with the live publisher)
    /// into an app lifecycle hook. The state handle is ignored: the hook's input is the
    /// publisher, and app state stays reachable through the app-level `after_startup`.
    pub(crate) fn box_startup_publish<B, State, Source, Hook, Fut, E>(
        slot: ConnectedSlot<B>,
        source: Source,
        hook: Hook,
    ) -> LifecycleHook<State>
    where
        B: Broker + 'static,
        Source: PublishPolicy<Connected<B>> + Send + 'static,
        Source::Live: Send,
        Hook: FnOnce(Source::Live) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        Box::new(move |_state: Arc<State>| {
            Box::pin(async move {
                let live = pair_bound::<B, Source>(&slot, source)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                hook(live).await.map_err(|e| Box::new(e) as BoxError)
            })
        })
    }
}
