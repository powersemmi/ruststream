//! Application unit-testing support, behind the `testing` feature.
//!
//! [`TestApp`] drives a built [`RustStream`](crate::runtime::RustStream) application in process - no
//! network `connect`, no server - and records what each handler received (raw and decoded), how it
//! settled, and what it published, so a unit test can assert on it. A broker plugs into the harness
//! (and into the [`conformance`](crate::conformance) suite) by implementing one contract,
//! [`TestableBroker`], on its in-process transport and registering it with
//! [`register_testable_broker!`](crate::register_testable_broker).
//!
//! Note that [`MemoryBroker`](crate::memory::MemoryBroker) is a real broker (local in-process
//! queues), not a testing tool; the harness drives it (or any broker) through the same dispatch
//! path the production runtime uses.

#[cfg(feature = "testing")]
mod assertions;
#[cfg(feature = "testing")]
mod broker;
#[cfg(feature = "testing")]
pub(crate) mod coordinator;
#[cfg(feature = "testing")]
mod harness;

#[cfg(feature = "testing")]
pub use assertions::{PublishedAssertions, SubscriberAssertions};
#[cfg(feature = "testing")]
pub use broker::{TestableBroker, TestableRegistration, expect_published};
#[cfg(feature = "testing")]
pub use coordinator::{Coordinator, Outcome};
#[cfg(feature = "testing")]
pub use harness::{BrokerHandle, InjectSink, TestApp, TestBrokers, TestError};
