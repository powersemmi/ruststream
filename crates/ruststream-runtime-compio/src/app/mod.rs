//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one service.
//!
//! - [`RustStream`]: the builder, with the names of the tokio runner's.
//! - [`BrokerScope`]: what a `with_broker` closure registers handlers on.
//! - [`Launch`] and [`Mounted`]: what each mount is started with, once the cores exist.
//! - [`RustStream::run`], [`run_until`](RustStream::run_until) and [`start`](RustStream::start):
//!   running it, and [`RunningApp`], the handle of a started one.
//! - [`HealthProbe`]: what a sibling task reads of the running service.

mod app_trait;
mod health;
mod include;
mod run;
mod scope;
mod service;
mod teardown;
#[cfg(feature = "testing")]
mod testing;

pub use app_trait::App;
pub use health::HealthProbe;
pub use run::RunningApp;
pub use ruststream_runtime::{HealthState, SubscriptionCount, SubscriptionEnd};

#[doc(hidden)]
pub use include::MountRoutes;
pub use ruststream_runtime::AppInfo;
pub use scope::BrokerScope;
#[doc(hidden)]
pub use scope::{ScopeRegistrations, ScopedHandle, ScopedRouter, ScopedStartup};
pub use service::RegisteredBroker;
pub use service::{RustStream, Setup, Wired};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use testing::{Harness, InProcessError, InProcessStart, StateSource};

use std::sync::Arc;

use compio::runtime::JoinHandle;
use thiserror::Error;

use crate::dispatch::{Fabric, LayoutError};
use crate::{BoxError, BoxFuture, CoresError, LocalBoxFuture, Stop, Workers};

/// What a mount is started with: the stop signal of the app, the mailboxes through which core 0
/// starts lanes on the other cores, and the cores this mount was placed on.
#[derive(Clone, Debug)]
pub struct Launch {
    /// Raised to stop every loop of the app.
    pub stop: Stop,
    /// The mailboxes of the cores.
    pub fabric: Fabric,
    /// The cores of this mount, one per worker or lane, as [`Layout`](crate::dispatch::Layout)
    /// placed them.
    pub cores: Vec<usize>,
    /// The hooks of the harness the app runs under, where a test drives it: each mount records
    /// its deliveries there.
    #[cfg(feature = "testing")]
    pub harness: Option<Harness>,
}

/// A registration deferred until the app runs.
///
/// Given the app state and the [`Launch`] of its mount, it opens the subscription (after the broker
/// is connected) and spawns the dispatch task on core 0. The caller keeps the returned handle for
/// the life of the subscription: dropping it cancels the loop.
pub type Starter<St> = Box<
    dyn FnOnce(Arc<St>, Launch) -> LocalBoxFuture<'static, Result<JoinHandle<()>, BoxError>> + Send,
>;

/// One mount of the app: its starter and the concurrency it declared, which the placement of the
/// cores is planned from.
// Machinery the run step drives; it holds an erased closure that has no useful `Debug`.
#[allow(missing_debug_implementations)]
#[doc(hidden)]
pub struct Mounted<St> {
    /// The declared `workers(..)` or `threads(..)`.
    pub workers: Workers,
    /// Opens the subscription and starts its loop.
    pub start: Starter<St>,
}

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
    /// A dispatch task panicked. The string says what the task reported.
    #[error("dispatch task failed: {0}")]
    Join(String),
    /// A subscriber hit a fail-fast failure (a handler panic, or a decode failure under
    /// `on_failure(decode = fail_fast)`) and tore the service down. The string names the
    /// subscription and the reason.
    #[error("dispatch failed: {0}")]
    Dispatch(String),
    /// The `threads` mounts need more cores than the service has besides core 0.
    #[error("the mounts do not fit the cores: {0}")]
    Layout(#[from] LayoutError),
    /// A core could not start or ended with a failure.
    #[error("a core failed: {0}")]
    Cores(#[from] CoresError),
}
