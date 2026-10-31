#![doc = include_str!("README.md")]

#[cfg(feature = "testing")]
pub use ruststream_runtime::testing::{
    Backlog, Coordinator, InProcess, LocalTestableBroker, Outcome, PublishedAssertions,
    SubscriberAssertions, TestError, TestableBroker, TestableRegistration, expect_published,
};
/// The harness of the embedded runner's app, driven on the host.
#[cfg(all(feature = "testing", feature = "embassy"))]
pub mod embedded {
    pub use ruststream_testing::embedded::{BrokerHandle, InjectSink, TestApp};
}
#[cfg(all(feature = "testing", feature = "tokio"))]
pub use ruststream_testing::{BrokerHandle, InjectSink, TestApp, TestBrokers};
