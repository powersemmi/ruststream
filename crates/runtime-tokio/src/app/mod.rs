//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

mod app_trait;
mod health;
mod include;
mod run;
mod scope;
mod service;
mod teardown;

pub use app_trait::App;
pub(crate) use health::HealthReport;
pub use health::{HealthProbe, SubscriptionEnd};
#[doc(hidden)]
pub use include::MountRoutes;
pub use run::RunningApp;
pub use ruststream_runtime::{HealthState, SubscriptionCount};
pub use scope::BrokerScope;
#[doc(hidden)]
pub use scope::{ScopeRegistrations, ScopedHandle, ScopedRouter, ScopedStartup};
pub use service::RegisteredBroker;
#[cfg(feature = "testing")]
pub use service::TestParts;
pub use service::{RustStream, Setup, Wired};
pub use teardown::{ConnectedEntry, ShutdownHooks, Teardown};

pub use ruststream_runtime::AppInfo;

use std::sync::Arc;

use thiserror::Error;
use tokio::task::{JoinError, JoinHandle};

use crate::shutdown::Shutdown;

use crate::{BoxError, BoxFuture, LocalBoxFuture};

/// A registration deferred until [`RustStream::run`].
///
/// Given the app's shutdown signal, it opens the subscription (after the broker is connected) and
/// spawns the dispatch task. The broker, source and handler are captured and type-erased.
pub type Starter<St> = Box<
    dyn FnOnce(Arc<St>, Shutdown) -> LocalBoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The state initializer: produces the app state `St` once at startup (before brokers connect).
/// The `on_startup` producer chain; a failing initializer aborts startup. The default is the unit
/// state `()`.
pub(crate) type StateInit<St> =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<St, BoxError>> + Send>;

/// A read-only lifespan hook (`after_startup` / `on_shutdown` / `after_shutdown`): runs once at the
/// corresponding lifecycle point with a shared `Arc<St>` handle to the app state.
pub type LifecycleHook<St> =
    Box<dyn FnOnce(Arc<St>) -> LocalBoxFuture<'static, Result<(), BoxError>> + Send>;

/// Which read-only lifecycle hook list a hook is appended to.
#[derive(Clone, Copy)]
enum LifecyclePhase {
    AfterStartup,
    OnShutdown,
    AfterShutdown,
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
    /// A task of the runner itself ended abnormally: a fault of the framework, to be reported as
    /// a bug, or a runtime shut down under the running service, which cancels its tasks. A panic
    /// of the service's own code never ends here: the handler's goes to its panic policy, and any
    /// other fails the service as [`Dispatch`](Self::Dispatch).
    #[error("dispatch task failed: {0}")]
    Join(#[source] JoinError),
    /// A subscriber hit a fail-fast failure and tore the service down: a handler panic, a panic
    /// outside the handler (in the subscription's dispatch loop, a worker of its pool or one of
    /// its dedicated threads), or a decode failure under `on_failure(decode = fail_fast)`. The
    /// string names the subscription and the reason.
    #[error("dispatch failed: {0}")]
    Dispatch(String),
}

/// Boxing helpers for scope-registered lifecycle hooks.
pub(super) mod lifecycle_hooks {
    use std::{error::Error as StdError, sync::Arc};

    use crate::BoxError;
    use crate::bindable::ConnectedSlot;
    use crate::bindable::pair_bound;
    use crate::{Connected, PublishPolicy, SendBroker};

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
        B: SendBroker,
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
                    .map_err(BoxError::from)?;
                hook(live).await.map_err(BoxError::from)
            })
        })
    }
}
