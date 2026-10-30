//! The error the test harness returns: the dispatch reports it while driving a reaction, and
//! `TestApp` returns it from every step.

use core::error;
use core::time::Duration;

use thiserror::Error;

/// An error from the test harness.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TestError {
    /// An `on_startup` or `after_startup` lifecycle hook failed while starting the harness.
    #[error("startup hook failed: {0}")]
    Startup(#[source] Box<dyn error::Error + Send + Sync>),
    /// A subscription failed to open while starting the harness.
    #[error("subscription failed: {0}")]
    Subscribe(#[source] Box<dyn error::Error + Send + Sync>),
    /// A reaction did not settle within the step budget (a non-terminating requeue?).
    #[error("the reaction did not settle within {processed} dispatched deliveries")]
    NotQuiescent {
        /// How many deliveries were dispatched before the harness gave up.
        processed: usize,
    },
    /// A live reaction did not settle before the deadline: a subscription had not handled what was
    /// published to a destination it reads, or a redelivery that fell due, or a handler was still
    /// running.
    #[error(
        "the live reaction did not settle within {deadline:?}: {}",
        unsettled(subscription.as_deref(), *handled, *expected)
    )]
    NotSettled {
        /// The subscription still owed a delivery, or `None` when every subscription had handled
        /// what it was owed and a handler was still running.
        subscription: Option<String>,
        /// How many deliveries that subscription had handled.
        handled: usize,
        /// How many it was owed: the publishes the harness saw to it, and the redeliveries due.
        expected: usize,
        /// The deadline the harness was started with.
        deadline: Duration,
    },
    /// A broker failed to connect while starting the harness: its in-process transition, or its
    /// `connect` in live mode.
    #[error("broker {broker} failed to connect: {source}")]
    Connect {
        /// The broker's label, or its type for unlabeled brokers.
        broker: String,
        /// The broker's own connect error.
        #[source]
        source: Box<dyn error::Error + Send + Sync>,
    },
    /// A live broker refused a message the test published onto it.
    #[error("broker {broker} refused the test's publish: {source}")]
    Publish {
        /// The broker's label, or its type for unlabeled brokers.
        broker: String,
        /// The broker's own publish error.
        #[source]
        source: Box<dyn error::Error + Send + Sync>,
    },
    /// A publish was attempted after a fail-fast failure tore the service down.
    #[error("publish after the service shut down")]
    ShutDown,
    /// An unscoped [`TestApp::publish`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.publish) is ambiguous: more than one broker is registered.
    #[error("more than one broker is registered; address one with broker::<B>() or broker_named()")]
    Ambiguous,
    /// A broker of the app has no in-process mode, so the harness cannot start the app: its type
    /// is not registered with [`register_testable_broker!`](crate::register_testable_broker),
    /// which is what a broker crate does under its `testing` feature.
    ///
    /// In live mode it reports an injection through an untyped handle (the sole broker,
    /// [`broker_named`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.broker_named)) onto a broker of such a type: address it with
    /// [`broker`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.broker) instead, which publishes through the type's default policy.
    #[error(
        "broker {0} has no in-process mode; enable the `testing` feature of the crate that \
         provides it in `[dev-dependencies]`"
    )]
    NoTransport(String),
    /// [`TestApp::start_live`](https://docs.rs/ruststream/latest/ruststream/testing/struct.TestApp.html#method.start_live) was called on a paused clock. A network client's timeouts would
    /// fire at once against it, so a live test runs on the real clock.
    #[error(
        "the clock is paused, and a live test runs against brokers on the real clock; drop \
         `start_paused` / `tokio::time::pause()` from this test, or start it in process with \
         `TestApp::start`"
    )]
    PausedClock,
    /// The message failed to encode for publishing.
    #[error("failed to encode the message: {0}")]
    Encode(String),
}

/// What a live settle was still waiting on at its deadline, for [`TestError::NotSettled`].
fn unsettled(subscription: Option<&str>, handled: usize, expected: usize) -> String {
    subscription.map_or_else(
        || "a handler was still running".to_owned(),
        |name| format!("subscription {name:?} handled {handled} of {expected} deliveries"),
    )
}
