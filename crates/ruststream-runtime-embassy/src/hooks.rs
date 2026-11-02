//! The app's lifecycle hooks as typed lists: each hook is the closure the service wrote, called
//! once, in the order it was added.

// The hooks' futures are the service's own and run on the app's one task.
#![allow(clippy::future_not_send)]

#[cfg(ruststream_alloc)]
use alloc::sync::Arc;
use core::error::Error as StdError;
use core::future::ready;

use ruststream_runtime::BoxError;

/// The app's state as the hooks and the loops are lent it: counted with `alloc`, the value
/// itself without, where the app future holds it and every user borrows.
#[cfg(ruststream_alloc)]
pub(crate) type AppState<State> = Arc<State>;
/// The app's state as the hooks and the loops are lent it.
#[cfg(not(ruststream_alloc))]
pub(crate) type AppState<State> = State;

use ruststream_core::diag::warn;

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
    fn call(self, state: &AppState<State>) -> impl Future<Output = Result<(), BoxError>>;

    /// Calls every hook, logging an error and going on, for the phases of shutdown.
    fn call_all(self, state: &AppState<State>, phase: &'static str) -> impl Future<Output = ()>;
}

impl<State> HookList<State> for () {
    fn call(self, _state: &AppState<State>) -> impl Future<Output = Result<(), BoxError>> {
        ready(Ok(()))
    }

    fn call_all(self, _state: &AppState<State>, _phase: &'static str) -> impl Future<Output = ()> {
        ready(())
    }
}

#[cfg(ruststream_alloc)]
impl<State, Earlier, Hook, Fut, E> HookList<State> for (Earlier, Hook)
where
    Earlier: HookList<State>,
    Hook: FnOnce(Arc<State>) -> Fut,
    Fut: Future<Output = Result<(), E>>,
    E: StdError + Send + Sync + 'static,
{
    async fn call(self, state: &AppState<State>) -> Result<(), BoxError> {
        self.0.call(state).await?;
        (self.1)(shared(state)).await.map_err(BoxError::from)
    }

    async fn call_all(self, state: &AppState<State>, phase: &'static str) {
        self.0.call_all(state, phase).await;
        if let Err(err) = (self.1)(shared(state)).await {
            warn!(target: "ruststream::embassy", phase, error = %err, "lifecycle hook failed");
        }
    }
}

#[cfg(not(ruststream_alloc))]
impl<State, Earlier, Hook, E> HookList<State> for (Earlier, Hook)
where
    Earlier: HookList<State>,
    Hook: AsyncFnOnce(&State) -> Result<(), E>,
    E: StdError + Send + Sync + 'static,
{
    async fn call(self, state: &AppState<State>) -> Result<(), BoxError> {
        self.0.call(state).await?;
        (self.1)(shared(state)).await.map_err(BoxError::from)
    }

    async fn call_all(self, state: &AppState<State>, phase: &'static str) {
        self.0.call_all(state, phase).await;
        if let Err(err) = (self.1)(shared(state)).await {
            warn!(target: "ruststream::embassy", phase, error = %err, "lifecycle hook failed");
        }
    }
}

/// What a hook is handed of the state.
#[cfg(ruststream_alloc)]
fn shared<State>(state: &Arc<State>) -> Arc<State> {
    Arc::clone(state)
}

/// What a hook is handed of the state.
#[cfg(not(ruststream_alloc))]
const fn shared<State>(state: &State) -> &State {
    state
}

/// The state the loops read, in place.
#[cfg(ruststream_alloc)]
pub(crate) fn borrowed<State>(state: &Arc<State>) -> &State {
    state
}

/// The state the loops read, in place.
#[cfg(not(ruststream_alloc))]
pub(crate) const fn borrowed<State>(state: &State) -> &State {
    state
}
