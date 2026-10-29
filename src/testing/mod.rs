#![doc = include_str!("README.md")]

#[cfg(feature = "testing")]
mod handle;
#[cfg(feature = "testing")]
mod harness;

#[cfg(feature = "testing")]
pub use ruststream_runtime::testing::{
    Backlog, Coordinator, InProcess, Outcome, PublishedAssertions, SubscriberAssertions, TestError,
    TestableBroker, TestableRegistration, expect_published,
};

#[cfg(feature = "testing")]
pub use handle::{BrokerHandle, InjectSink};
#[cfg(feature = "testing")]
pub use harness::{TestApp, TestBrokers};
