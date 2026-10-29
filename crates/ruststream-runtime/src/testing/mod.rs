//! The broker contract of the test harness and the in-process dispatch it drives.
//!
//! It holds what a broker crate implements under its `testing` feature ([`InProcess`],
//! [`TestableBroker`]), the [`Coordinator`] that steps a reaction to quiescence, and the
//! assertions a test reads.
//!
//! Re-exported as [`ruststream::testing`](https://docs.rs/ruststream/latest/ruststream/testing/)
//! beside the `TestApp` harness.

#[cfg(feature = "testing")]
mod assertions;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod attribution;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod coordinator;
#[cfg(feature = "testing")]
mod error;
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod hooks;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
pub use broker::{Backlog, InProcess, TestableBroker, TestableRegistration, expect_published};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use broker::{LivePublish, live_publish};
#[cfg(feature = "testing")]
pub use coordinator::{Coordinator, Outcome};
#[cfg(feature = "testing")]
pub use error::TestError;
