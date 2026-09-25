#![doc = include_str!("README.md")]

#[cfg(feature = "testing")]
mod assertions;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
pub(crate) mod coordinator;
#[cfg(feature = "testing")]
mod handle;
#[cfg(feature = "testing")]
mod harness;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
pub use broker::{Backlog, InProcess, TestableBroker, TestableRegistration, expect_published};
#[cfg(feature = "testing")]
pub use coordinator::{Coordinator, Outcome};
#[cfg(feature = "testing")]
pub use handle::{BrokerHandle, InjectSink};
#[cfg(feature = "testing")]
pub use harness::{TestApp, TestBrokers, TestError};
