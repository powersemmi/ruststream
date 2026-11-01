//! A value the harness keeps for a test that runs on one thread.
//!
//! With the tokio runner a recorded value or a registered hook is `Send + Sync`, as the bound of
//! the code that produced it says. Without it the app and the test share a thread, nothing in the
//! contract asks for `Send`, and the harness's records still sit behind a coordinator that crosses
//! threads in type. The wrapper there holds the value for that thread alone and refuses a touch
//! from any other.

use core::ops::Deref;

#[cfg(not(feature = "tokio"))]
use send_wrapper::SendWrapper;

/// A value held for the thread that made it when the runner is not tokio; the value itself
/// otherwise.
#[derive(Debug)]
pub(crate) struct OnThread<T> {
    #[cfg(feature = "tokio")]
    value: T,
    #[cfg(not(feature = "tokio"))]
    value: SendWrapper<T>,
}

impl<T> OnThread<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            #[cfg(feature = "tokio")]
            value,
            #[cfg(not(feature = "tokio"))]
            value: SendWrapper::new(value),
        }
    }

    /// The value, taken back on the thread that made it.
    #[cfg(not(feature = "tokio"))]
    pub(crate) fn into_inner(self) -> T {
        self.value.take()
    }
}

impl<T> Deref for OnThread<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}
