//! The app's lifecycle hooks as typed lists: each hook is the closure the service wrote, called
//! once, in the order it was added.

// The hooks' futures are the service's own and run on the app's one task.
#![allow(clippy::future_not_send)]

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::error::Error as StdError;
use core::future::ready;

use ruststream_runtime::BoxError;
use tracing::warn;

/// The hooks of each phase of the app's life.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct Hooks<AfterStartup, OnShutdown, AfterShutdown> {
    pub(crate) after_startup: AfterStartup,
    pub(crate) on_shutdown: OnShutdown,
    pub(crate) after_shutdown: AfterShutdown,
}

/// An app with no hooks.
#[doc(hidden)]
pub type NoHooks = Hooks<(), (), ()>;

/// One phase's hooks: called in the order they were added.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a lifecycle hook of the embedded app",
    note = "a hook takes the app's state as `Arc<State>` and returns a future of \
            `Result<(), E>`, `E` an error type"
)]
pub trait HookList<State> {
    /// Calls every hook; the first error ends the phase.
    ///
    /// # Errors
    ///
    /// The first error a hook returned.
    fn call(self, state: &Arc<State>) -> impl Future<Output = Result<(), BoxError>>;

    /// Calls every hook, logging an error and going on, for the phases of shutdown.
    fn call_all(self, state: &Arc<State>, phase: &'static str) -> impl Future<Output = ()>;
}

impl<State> HookList<State> for () {
    fn call(self, _state: &Arc<State>) -> impl Future<Output = Result<(), BoxError>> {
        ready(Ok(()))
    }

    fn call_all(self, _state: &Arc<State>, _phase: &'static str) -> impl Future<Output = ()> {
        ready(())
    }
}

impl<State, Earlier, Hook, Fut, E> HookList<State> for (Earlier, Hook)
where
    Earlier: HookList<State>,
    Hook: FnOnce(Arc<State>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: StdError + Send + Sync + 'static,
{
    async fn call(self, state: &Arc<State>) -> Result<(), BoxError> {
        self.0.call(state).await?;
        (self.1)(Arc::clone(state)).await.map_err(Box::from)
    }

    async fn call_all(self, state: &Arc<State>, phase: &'static str) {
        self.0.call_all(state, phase).await;
        if let Err(err) = (self.1)(Arc::clone(state)).await {
            warn!(target: "ruststream::embassy", phase, error = %err, "lifecycle hook failed");
        }
    }
}
