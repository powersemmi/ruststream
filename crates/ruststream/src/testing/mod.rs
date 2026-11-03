#![doc = include_str!("README.md")]

#[cfg(all(feature = "testing", ruststream_alloc))]
pub use ruststream_runtime::testing::TestableRegistration;
#[cfg(feature = "testing")]
pub use ruststream_runtime::testing::{
    Backlog, Coordinator, InProcess, Outcome, PublishedAssertions, SubscriberAssertions, TestError,
    TestableBroker, expect_published,
};
/// The harness of the embedded runner's app, driven on the host.
#[cfg(all(feature = "testing", feature = "embassy"))]
pub mod embedded {
    pub use ruststream_testing::embedded::{BrokerHandle, InjectSink, TestApp};
}
/// The harness of the compio runner's app, driven on the test's own compio runtime.
#[cfg(all(feature = "testing", feature = "compio"))]
pub mod compio {
    pub use ruststream_testing::compio::{BrokerHandle, InjectSink, TestApp, TestBrokers};
}
#[cfg(all(feature = "testing", feature = "tokio"))]
pub use ruststream_testing::{BrokerHandle, InjectSink, TestApp, TestBrokers};
