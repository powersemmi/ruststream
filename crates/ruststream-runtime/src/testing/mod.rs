//! The broker contract of the test harness and the in-process dispatch it drives.
//!
//! It holds what a broker crate implements under its `testing` feature ([`InProcess`],
//! [`TestableBroker`]), the [`Coordinator`] that steps a reaction to quiescence, and the
//! assertions a test reads.
//!
//! Re-exported as [`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/)
//! beside the `TestApp` harness.

#[cfg(feature = "testing")]
use core::ops::{Deref, DerefMut};

#[cfg(feature = "testing")]
mod assertions;
#[cfg(feature = "testing")]
mod attribution;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
pub(crate) mod coordinator;
#[cfg(feature = "testing")]
mod error;
#[cfg(feature = "testing")]
mod harness_future;
#[cfg(feature = "testing")]
pub(crate) mod hooks;
#[cfg(feature = "testing")]
mod in_process;
#[cfg(feature = "testing")]
pub(crate) mod on_thread;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use attribution::{attribute, split_record};
#[cfg(all(feature = "testing", ruststream_alloc))]
pub use broker::TestableRegistration;
#[cfg(feature = "testing")]
pub use broker::{Backlog, InProcess, TestableBroker, expect_published};
#[cfg(all(feature = "testing", ruststream_alloc))]
#[doc(hidden)]
pub use broker::{LivePublish, live_publish};
#[cfg(feature = "testing")]
pub use coordinator::{Coordinator, Outcome};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use coordinator::{LiveSubscription, TestHooks};
#[cfg(feature = "testing")]
pub use error::TestError;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use harness_future::{HarnessFuture, harness_future};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use hooks::{HookFailures, PendingHooks, PublishTarget, Within};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use in_process::{InProcessRun, RunBroker};

/// Runs the harness's own bookkeeping. The failing allocator of a build without `alloc` lets an
/// allocation made here through, so what still fails is an allocation of the framework itself.
#[cfg(all(feature = "testing", not(ruststream_alloc)))]
#[doc(hidden)]
pub fn harness_section<T>(section: impl FnOnce() -> T) -> T {
    assert_no_alloc::permit_alloc(section)
}

/// Runs the harness's own bookkeeping; a build with `alloc` has no failing allocator.
#[cfg(all(feature = "testing", ruststream_alloc))]
#[doc(hidden)]
pub fn harness_section<T>(section: impl FnOnce() -> T) -> T {
    section()
}

/// A value of the harness's bookkeeping that the app's code holds for a while: dropping it is
/// harness work too, so it frees inside a [`harness_section`].
#[cfg(feature = "testing")]
#[doc(hidden)]
#[derive(Debug)]
pub struct Bookkept<T>(Option<T>);

#[cfg(feature = "testing")]
impl<T> Bookkept<T> {
    /// Takes `value` into the app's hands.
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self(Some(value))
    }

    /// The value, for the harness to work with.
    ///
    /// # Panics
    ///
    /// Never: the value is present until the wrapper is dropped, and this consumes it.
    #[must_use]
    pub fn into_inner(mut self) -> T {
        // Only `drop` empties it, and `drop` runs after this consumed `self`.
        self.0
            .take()
            .expect("a bookkept value is present until it is dropped")
    }
}

#[cfg(feature = "testing")]
impl<T> Drop for Bookkept<T> {
    fn drop(&mut self) {
        if let Some(value) = self.0.take() {
            harness_section(|| drop(value));
        }
    }
}

#[cfg(feature = "testing")]
impl<T> Deref for Bookkept<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0
            .as_ref()
            .expect("a bookkept value is present until it is dropped")
    }
}

#[cfg(feature = "testing")]
impl<T> DerefMut for Bookkept<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.0
            .as_mut()
            .expect("a bookkept value is present until it is dropped")
    }
}
