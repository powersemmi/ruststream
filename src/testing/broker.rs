//! The [`TestableBroker`] contract: the single test interface a broker implements so its in-process
//! routing works with both the [`TestApp`](super::TestApp) harness and the
//! [`conformance`](crate::conformance) suite.

use std::time::Duration;

use crate::{OutgoingMessage, RawMessage};

use super::Coordinator;

/// A broker whose in-process routing can be driven by the test tooling.
///
/// A broker crate ships an in-process transport - a normal [`Broker`](crate::Broker) that routes in
/// memory with no server, emulating the broker's Core routing - and implements `TestableBroker` on
/// its **connected form** (the harness drives brokers after their consuming
/// [`connect`](crate::Broker::connect)). That one type then works with both the
/// [`TestApp`](super::TestApp) harness (application unit tests) and
/// [`conformance::harness::run_suite`](crate::conformance::harness::run_suite) (routing self-check).
///
/// To plug into the harness, the broker also:
/// - calls [`Coordinator::enqueued`] on every live enqueue into a subscriber and
///   [`Coordinator::consumed`] when a delivery is acked, nacked, or dropped (so the harness can tell
///   when the reaction has settled), and routes delayed redeliveries through
///   [`Coordinator::schedule_redelivery`];
/// - registers its concrete type with [`register_testable_broker!`](crate::register_testable_broker)
///   so the harness can recover it from the type-erased app.
///
/// [`ConnectedMemoryBroker`](crate::memory::ConnectedMemoryBroker) is the in-tree reference
/// implementation.
///
/// It is a separate, object-safe capability (not a
/// [`ConnectedBroker`](crate::ConnectedBroker) supertrait, since the broker traits are not
/// dyn-compatible), so the harness can hold `&dyn TestableBroker` recovered from the type-erased
/// app.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # async fn demo() {
/// use ruststream::Broker;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::testing::TestableBroker;
///
/// fn published<B: TestableBroker>(broker: &B, name: &str) -> usize {
///     broker.published(name).len()
/// }
///
/// let connected = MemoryBroker::new().connect().await.unwrap();
/// assert_eq!(published(&connected, "orders"), 0);
/// # }
/// ```
pub trait TestableBroker: Send + Sync {
    /// Installs the harness coordinator into this broker's bus for a test run. Idempotent: a second
    /// install on the same broker is ignored.
    fn install_coordinator(&self, coordinator: Coordinator);

    /// Injects a message onto the bus as an external producer would, synchronously (no awaiting).
    /// Routes through the broker's normal fanout, so it is recorded and counted like any publish.
    fn inject(&self, message: OutgoingMessage<'_>);

    /// Returns every message published to `name` on this broker, in publish order. Backs the
    /// harness's `published::<T>(name)` assertions and [`expect_published`].
    fn published(&self, name: &str) -> Vec<RawMessage>;
}

/// A downcaster from a type-erased broker to a concrete [`TestableBroker`].
///
/// Submitted once per broker type with
/// [`register_testable_broker!`](crate::register_testable_broker); the harness iterates the
/// registered downcasters to recover each broker's transport from the built app.
pub struct TestableRegistration {
    downcast: fn(&dyn core::any::Any) -> Option<&dyn TestableBroker>,
}

impl std::fmt::Debug for TestableRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestableRegistration")
            .finish_non_exhaustive()
    }
}

impl TestableRegistration {
    /// Wraps a downcaster (the [`register_testable_broker!`](crate::register_testable_broker) macro
    /// builds this for you).
    #[must_use]
    pub const fn new(downcast: fn(&dyn core::any::Any) -> Option<&dyn TestableBroker>) -> Self {
        Self { downcast }
    }

    /// Resolves `any` to a `TestableBroker` if it is this registration's broker type.
    pub(crate) fn resolve<'a>(
        &self,
        any: &'a dyn core::any::Any,
    ) -> Option<&'a dyn TestableBroker> {
        (self.downcast)(any)
    }
}

inventory::collect!(TestableRegistration);

/// Registers a concrete [`TestableBroker`] type for harness recovery from a built application.
///
/// Call it once at broker-crate scope (under the crate's `testing` feature) so
/// [`TestApp`](crate::testing::TestApp) can recover that broker from the type-erased app.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # {
/// use ruststream::memory::MemoryBroker;
/// // The in-tree `MemoryBroker` is already registered; a broker crate registers its own type:
/// // ruststream::register_testable_broker!(MyTestBroker);
/// # let _ = MemoryBroker::new();
/// # }
/// ```
#[macro_export]
macro_rules! register_testable_broker {
    ($ty:ty) => {
        $crate::inventory::submit! {
            $crate::testing::TestableRegistration::new(|any: &dyn ::core::any::Any| {
                any.downcast_ref::<$ty>()
                    .map(|broker| broker as &dyn $crate::testing::TestableBroker)
            })
        }
    };
}

/// Waits until at least `count` messages have been published to `name` on `broker`.
///
/// Returns all observed messages; on timeout it returns those seen so far (so assert on the returned
/// messages, not just on length). For application tests prefer [`TestApp`](super::TestApp), which
/// drives to quiescence without polling; this helper is for tests that run a service via
/// [`run_until`](crate::runtime::RustStream::run_until) directly.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # async fn demo() {
/// use std::time::Duration;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::testing::{TestableBroker, expect_published};
/// use ruststream::{Broker, OutgoingMessage};
///
/// let connected = MemoryBroker::new().connect().await.unwrap();
/// connected.inject(OutgoingMessage::new("out", b"x".as_slice()));
/// let seen = expect_published(&connected, "out", 1, Duration::from_secs(1)).await;
/// assert_eq!(seen.len(), 1);
/// # }
/// ```
pub async fn expect_published<B: TestableBroker>(
    broker: &B,
    name: &str,
    count: usize,
    timeout: Duration,
) -> Vec<RawMessage> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let observed = broker.published(name);
        if observed.len() >= count || tokio::time::Instant::now() >= deadline {
            return observed;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
