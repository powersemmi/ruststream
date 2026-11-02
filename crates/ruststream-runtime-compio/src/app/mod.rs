//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one service.
//!
//! - [`RustStream`]: the builder, with the names of the tokio runner's.
//! - [`BrokerScope`]: what a `with_broker` closure registers handlers on.
//! - [`Launch`] and [`Mounted`]: what each mount is started with, once the cores exist.

mod include;
mod scope;
mod service;

#[doc(hidden)]
pub use include::MountRoutes;
pub use ruststream_runtime::AppInfo;
pub use scope::BrokerScope;
#[doc(hidden)]
pub use scope::{ScopeRegistrations, ScopedHandle, ScopedRouter, ScopedStartup};
pub use service::RegisteredBroker;
pub use service::{RustStream, Setup, Wired};

use std::sync::Arc;

use compio::runtime::JoinHandle;

use crate::dispatch::Fabric;
use crate::{BoxError, BoxFuture, LocalBoxFuture, Stop, Workers};

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
