//! The app's brokers as the harness holds them, and the view of them a state builder borrows.

use std::any::{TypeId, type_name};
use std::fmt;

use ruststream_core::Broker;
use ruststream_runtime::ConnectedLifecycle;
use ruststream_runtime::testing::TestableRegistration;

#[cfg(doc)]
use super::TestApp;

/// The registration of the broker type `broker`, or `None` when no crate registered one with
/// [`register_testable_broker!`](crate::register_testable_broker).
pub(super) fn registration_of(broker: TypeId) -> Option<&'static TestableRegistration> {
    inventory::iter::<TestableRegistration>
        .into_iter()
        .find(|registration| registration.registers(broker))
}

/// The app's brokers, connected in process, handed to the state a test builds with
/// [`TestApp::with_state`], so the state can hold what it needs of them.
pub struct TestBrokers<'a> {
    pub(super) connected: &'a [&'a (dyn ConnectedLifecycle + 'static)],
}

impl fmt::Debug for TestBrokers<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestBrokers")
            .field("brokers", &self.connected.len())
            .finish_non_exhaustive()
    }
}

impl TestBrokers<'_> {
    /// The connected form of the app's broker of type `B`, the production type the app was
    /// built with.
    ///
    /// # Panics
    ///
    /// Panics if the app has no broker of type `B`, or more than one.
    #[must_use]
    pub fn broker<B: Broker + 'static>(&self) -> &B::Connected {
        let mut found = self
            .connected
            .iter()
            .filter(|entry| entry.broker_type() == TypeId::of::<B>())
            .filter_map(|entry| entry.as_any().downcast_ref::<B::Connected>());
        let first = found
            .next()
            .unwrap_or_else(|| panic!("the app has no broker of type {}", type_name::<B>()));
        assert!(
            found.next().is_none(),
            "the app has more than one broker of type {}",
            type_name::<B>(),
        );
        first
    }
}
