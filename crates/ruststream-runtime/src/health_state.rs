//! [`HealthState`] and [`SubscriptionCount`]: the values a runner's health probe reports.

use alloc::string::String;

/// The lifecycle state reported by a `HealthProbe`.
///
/// `RustStream::start` is the readiness gate (it resolves only after
/// subscriptions are open), so the probe covers the post-startup half of the lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HealthState {
    /// Subscriptions are open and the dispatch loops are running.
    Running,
    /// An orderly `shutdown` is in progress.
    ShuttingDown,
    /// The service shut down cleanly.
    Stopped,
    /// The service tore itself down on a fail-fast failure, or teardown itself failed.
    Failed {
        /// The diagnostic the fail-fast path recorded (subscription and cause), the same string
        /// `run` returns inside
        /// `RustStreamError::Dispatch`.
        reason: String,
    },
}

impl HealthState {
    /// Whether the state reports a live, dispatching service.
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

/// How many of the service's subscriptions are consuming, as a `HealthProbe` reports it.
///
/// A subscription counts once, whatever its worker count. It is active from the moment its
/// dispatch loop starts until the loop ends: on shutdown, or when its stream ends while the
/// service runs (see `SubscriptionEnd`). Whether a service with fewer active subscriptions than
/// it started is healthy is the service's own call.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::{HealthProbe, RustStreamError, SubscriptionCount};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// /// Healthy while the service runs and every subscription it started still reads its queue.
/// fn healthy(health: &HealthProbe) -> bool {
///     let SubscriptionCount {
///         active, started, ..
///     } = health.subscriptions();
///     health.is_running() && active == started
/// }
///
/// pub async fn run() -> Result<(), RustStreamError> {
///     let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         MemoryBroker::new(),
///         |b| {
///             b.include(accept)
///         },
///     );
///     let running = app.start().await?;
///     tracing::info!(healthy = healthy(&running.health()), "service started");
///     running.shutdown().await
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SubscriptionCount {
    /// The subscriptions whose dispatch loop is running.
    pub active: usize,
    /// The subscriptions the service started.
    pub started: usize,
}

/// What a subscription's stream ending while the service runs does, set with
/// `RustStream::on_subscription_end`.
///
/// A stream can end without a shutdown: the server cancels a consumer when its queue is deleted,
/// a broker's reconnection policy gives up, or a broker is configured not to reconnect. An end
/// during shutdown is part of the shutdown and is never reported.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use ruststream::runtime::SubscriptionEnd;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// /// The service is useless without its queue, so it stops when the queue stops.
/// pub fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0"))
///         .on_subscription_end(SubscriptionEnd::FailFast)
///         .with_broker(MemoryBroker::new(), |b| {
///             b.include(accept)
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum SubscriptionEnd {
    /// The loop logs the end and exits; the rest of the service runs on. The subscription leaves
    /// the active count of `HealthProbe::subscriptions`.
    #[default]
    Log,
    /// The end is a fail-fast failure: an error naming the subscription, a graceful shutdown,
    /// `run` returning `RustStreamError::Dispatch`, and the probe reporting
    /// [`HealthState::Failed`].
    FailFast,
}
