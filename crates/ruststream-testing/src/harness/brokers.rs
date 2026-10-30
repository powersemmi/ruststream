//! The app's brokers as the harness holds them, the view of them a state builder borrows, and
//! the transport they were connected to.

use std::any::{TypeId, type_name};
use std::fmt;
use std::time::Duration;

use ruststream_core::Broker;
use ruststream_runtime::ConnectedLifecycle;

use ruststream_runtime::testing::{Coordinator, LiveSubscription};
use ruststream_runtime::testing::{TestError, TestableBroker, TestableRegistration};

#[cfg(doc)]
use super::TestApp;

/// One broker of the app under test: its label, its erased connected handle (for type/label
/// addressing), and its type's registration, when a broker crate made one. In process every broker
/// has one, since it connected through it.
pub(crate) struct BrokerEntry {
    pub(super) label: Option<String>,
    pub(super) lifecycle: Box<dyn ConnectedLifecycle>,
    pub(super) registration: Option<&'static TestableRegistration>,
}

impl BrokerEntry {
    /// The broker's `TestableBroker` view, recovered from the erased handle via its registration.
    pub(super) fn testable(&self) -> Option<&dyn TestableBroker> {
        self.registration?.resolve(self.lifecycle.as_any())
    }

    /// The name used to address this broker in diagnostics: its label, else its broker type name.
    pub(super) fn display(&self) -> String {
        self.label
            .clone()
            .unwrap_or_else(|| self.lifecycle.name().to_owned())
    }
}

/// How a broker the harness has no registration for is taken to route: to every subscription of
/// the destination's name.
fn same_names(destination: &str, subscriptions: &[&str]) -> Vec<usize> {
    subscriptions
        .iter()
        .enumerate()
        .filter(|(_, name)| **name == destination)
        .map(|(position, _)| position)
        .collect()
}

/// The registration of the broker type `broker`, or `None` when no crate registered one with
/// [`register_testable_broker!`](crate::register_testable_broker).
pub(super) fn registration_of(broker: TypeId) -> Option<&'static TestableRegistration> {
    inventory::iter::<TestableRegistration>
        .into_iter()
        .find(|registration| registration.registers(broker))
}

/// Borrowed view of the app's brokers handed to a [`TestApp::with_state`] builder, so it can wire a
/// mirror state's publishers onto the same bus the assertions read.
pub struct TestBrokers<'a> {
    pub(super) entries: &'a [BrokerEntry],
}

impl fmt::Debug for TestBrokers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestBrokers")
            .field("brokers", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl TestBrokers<'_> {
    /// Returns the connected form of the unique registered broker of type `B`, for building a
    /// mirror state's publishers (`tb.broker::<MemoryBroker>().publisher()`).
    ///
    /// # Panics
    ///
    /// Panics if no broker of type `B` is registered, or more than one is (disambiguate the app, or
    /// address by label is not supported when building state).
    #[must_use]
    pub fn broker<B: Broker + 'static>(&self) -> &B::Connected {
        let mut found = self
            .entries
            .iter()
            .filter(|e| e.lifecycle.broker_type() == TypeId::of::<B>())
            .filter_map(|e| e.lifecycle.as_any().downcast_ref::<B::Connected>());
        let first = found
            .next()
            .unwrap_or_else(|| panic!("no registered broker of type {}", type_name::<B>()));
        assert!(
            found.next().is_none(),
            "more than one broker of type {} is registered",
            type_name::<B>(),
        );
        first
    }
}

/// Which transport the harness connected the app's brokers to.
///
/// A run-time value rather than a type parameter: every method of the harness has a meaning in
/// both modes, so nothing is closed off by it.
#[derive(Debug)]
pub(crate) enum Mode {
    /// Each broker through its in-process transition: the transports count what is in flight, so
    /// a settle waits for that count to reach zero.
    InProcess,
    /// Each broker through its `connect`, against a running stand.
    Live {
        /// How long a settle waits for the reaction.
        deadline: Duration,
        /// The app's subscriptions, which a settle waits on.
        subscriptions: Vec<LiveSubscription>,
    },
}

impl Mode {
    /// Waits for the reaction the harness can see to settle: the in-process transports' count
    /// reaching zero, or, live, every subscription having handled what it is owed.
    ///
    /// A panic a test's hook raised meanwhile is raised again here, in the test's own task.
    pub(crate) async fn settle(
        &self,
        coordinator: &Coordinator,
        brokers: &[BrokerEntry],
    ) -> Result<(), TestError> {
        let settled = self.reaction(coordinator, brokers).await;
        coordinator.hooks().raise_failure();
        settled
    }

    async fn reaction(
        &self,
        coordinator: &Coordinator,
        brokers: &[BrokerEntry],
    ) -> Result<(), TestError> {
        match self {
            Self::InProcess => coordinator.drive().await,
            Self::Live {
                deadline,
                subscriptions,
            } => {
                // Each broker routes its own publishes: the harness asks it, and knows no routing
                // but equal names for a broker it has no registration for.
                let routing = |scope_id: usize, destination: &str, names: &[&str]| {
                    brokers[scope_id].testable().map_or_else(
                        || same_names(destination, names),
                        |broker| broker.routes(destination, names),
                    )
                };
                coordinator
                    .settle_live(subscriptions, &routing, *deadline)
                    .await
            }
        }
    }
}
