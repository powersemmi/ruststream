//! The [`TestableBroker`] extension contract: how a broker plugs its in-process bus into the
//! [`TestApp`](super::TestApp) harness.

use crate::{Broker, RawMessage};

use super::Coordinator;

/// A broker whose in-process bus can be driven by the [`TestApp`](super::TestApp) harness.
///
/// A broker crate implements this under its `testing` feature: it installs the harness
/// [`Coordinator`] into its bus (calling [`Coordinator::enqueued`] on every live enqueue into a
/// subscriber and [`Coordinator::consumed`] when a delivery is acked, nacked, or dropped), and
/// exposes its recorded publish log so the harness can assert downstream publishes.
/// [`MemoryBroker`](crate::memory::MemoryBroker) is the in-tree reference implementation.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "memory")]
/// # {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::testing::TestableBroker;
///
/// fn published<B: TestableBroker>(broker: &B, name: &str) -> usize {
///     broker.published_raw(name).len()
/// }
///
/// let broker = MemoryBroker::new();
/// assert_eq!(published(&broker, "orders"), 0);
/// # }
/// ```
pub trait TestableBroker: Broker {
    /// Installs the harness coordinator into this broker's bus for the duration of a test run.
    /// Idempotent: a second install on the same broker is ignored.
    fn install_coordinator(&self, coordinator: Coordinator);

    /// Returns every message published to `name` on this broker, in publish order. Backs the
    /// harness's `published::<T>(name)` assertions.
    fn published_raw(&self, name: &str) -> Vec<RawMessage>;
}
