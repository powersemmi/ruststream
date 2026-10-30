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
mod attribution;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
pub(crate) mod coordinator;
#[cfg(feature = "testing")]
mod error;
#[cfg(feature = "testing")]
pub(crate) mod hooks;
#[cfg(feature = "testing")]
mod in_process;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use attribution::{attribute, split_record};
#[cfg(feature = "testing")]
pub use broker::{
    Backlog, InProcess, LocalInProcess, LocalTestableBroker, TestableBroker, TestableRegistration,
    expect_published,
};
#[cfg(feature = "testing")]
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
pub use hooks::{HookFailures, PublishTarget, Within};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use in_process::{InProcessRun, RunBroker};
