//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

mod include;
mod run;
mod scope;
mod service;

pub use scope::BrokerScope;
pub use service::RustStream;

use std::sync::Arc;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::runtime::context::State;
use crate::runtime::failure::ErrorShutdown;
use crate::runtime::lifecycle::{BoxError, BoxFuture};

/// A registration deferred until [`RustStream::run`]: given the app's error-shutdown handle and the
/// shutdown token, it opens the subscription (after the broker is connected) and spawns the dispatch
/// task. The broker, source and handler are captured and type-erased.
type Starter = Box<
    dyn FnOnce(
            Arc<State>,
            ErrorShutdown,
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
    /// A broker failed to [`connect`](crate::Broker::connect) at startup.
    #[error("broker connect failed: {0}")]
    Connect(#[source] BoxError),
    /// An `on_startup` or `after_startup` lifespan hook failed.
    #[error("startup hook failed: {0}")]
    Startup(#[source] BoxError),
    /// A subscription failed to open after connect.
    #[error("subscription failed: {0}")]
    Subscribe(#[source] BoxError),
    /// A broker failed to [`shutdown`](crate::Broker::shutdown) during graceful shutdown.
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
