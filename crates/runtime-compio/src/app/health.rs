//! The [`HealthProbe`]: a cloneable view of the running service's lifecycle state and of how many
//! of its subscriptions are consuming.
//!
//! The standard deployment shape runs the messaging service beside another task, typically an HTTP
//! server with a `/healthz` route. When the service fails fast, the process stays alive because of
//! that sibling task, and the probe is what lets its endpoint report "the app died, stop routing
//! traffic here" instead of a permanent 200.

use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use event_listener::Event;
use ruststream_runtime::{HealthState, SubscriptionCount};

const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const STOPPED: u8 = 2;
const FAILED: u8 = 3;

/// What the probes read. Written only on a lifecycle transition and when a subscription's loop
/// starts or ends; nothing here is touched per delivery.
#[derive(Debug)]
struct Shared {
    /// One of the codes above; the reason of a failure is read after it.
    state: AtomicU8,
    reason: Mutex<String>,
    started: AtomicUsize,
    active: AtomicUsize,
    /// Counts every change, so a probe tells whether it has seen the latest.
    version: AtomicU64,
    changed: Event,
}

impl Shared {
    /// Publishes a change to every probe parked in [`HealthProbe::changed`].
    fn announce(&self) {
        self.version.fetch_add(1, Ordering::AcqRel);
        self.changed.notify(usize::MAX);
    }
}

/// A cheap, cloneable handle reporting the service's [`HealthState`] and how many of its
/// subscriptions are consuming.
///
/// Obtained from [`RunningApp::health`](super::RunningApp::health). Reading is a few atomic loads,
/// and nothing runs on the per-delivery hot path: the probe is written only on lifecycle
/// transitions and when a subscription's loop starts or ends. The probe outlives
/// [`shutdown`](super::RunningApp::shutdown), which consumes the app handle, so a sibling task
/// keeps answering with the terminal state. It is `Send` and `Sync`, so it may be read from any
/// thread.
///
/// Rapid transitions (`ShuttingDown` immediately followed by `Stopped`) may coalesce for a probe
/// parked in [`changed`](Self::changed): read [`state`](Self::state) for a snapshot and use
/// `changed` only as a wake-up.
///
/// Dropping the [`RunningApp`](super::RunningApp) without calling `shutdown` detaches the service;
/// a probe then keeps reporting the last observed state, and [`changed`](Self::changed) resolves
/// only if a fail-fast failure still flips the state or a subscription still ends.
///
/// # Examples
///
/// ```
/// use compio::runtime::Runtime;
/// use ruststream_memory::local::LocalMemoryBroker;
/// use ruststream_runtime::{AppInfo, HealthState};
/// use ruststream_runtime_compio::RustStream;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// Runtime::new()?.block_on(async {
///     let app = RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(LocalMemoryBroker::<4>::new());
///     let running = app.start().await?;
///     let health = running.health();
///     assert!(health.is_running());
///
///     running.shutdown().await?;
///     // The probe outlives the app handle and reports where the service ended.
///     assert_eq!(health.state(), HealthState::Stopped);
///     Ok(())
/// })
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct HealthProbe {
    shared: Arc<Shared>,
    /// The version this probe has reported.
    seen: u64,
}

impl HealthProbe {
    /// A snapshot of the current state.
    #[must_use]
    pub fn state(&self) -> HealthState {
        match self.shared.state.load(Ordering::Acquire) {
            RUNNING => HealthState::Running,
            SHUTTING_DOWN => HealthState::ShuttingDown,
            STOPPED => HealthState::Stopped,
            _ => HealthState::Failed {
                reason: self
                    .shared
                    .reason
                    .lock()
                    .map(|reason| reason.clone())
                    .unwrap_or_default(),
            },
        }
    }

    /// Whether the service is currently running (the healthy answer for a liveness endpoint).
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.shared.state.load(Ordering::Acquire) == RUNNING
    }

    /// How many subscriptions are consuming, and how many the service started.
    ///
    /// A subscription whose stream ended while the service runs leaves the active count, under
    /// the default [`SubscriptionEnd::Log`](ruststream_runtime::SubscriptionEnd::Log). The
    /// lifecycle state stays [`Running`](HealthState::Running): the service decides what a partly
    /// working service reports outside.
    ///
    /// # Examples
    ///
    /// A readiness answer that holds while every subscription the service started still consumes:
    ///
    /// ```
    /// use ruststream_runtime_compio::HealthProbe;
    ///
    /// fn ready(health: &HealthProbe) -> bool {
    ///     let count = health.subscriptions();
    ///     health.is_running() && count.active == count.started
    /// }
    /// ```
    #[must_use]
    pub fn subscriptions(&self) -> SubscriptionCount {
        // The count is `#[non_exhaustive]`, so it is filled in rather than built.
        let mut count = SubscriptionCount::default();
        count.active = self.shared.active.load(Ordering::Acquire);
        count.started = self.shared.started.load(Ordering::Acquire);
        count
    }

    /// Waits for the next change, a lifecycle transition or a subscription starting or ending,
    /// and returns the state.
    ///
    /// If the service was detached (the app handle dropped without `shutdown`) and no further
    /// change can arrive, the future stays pending rather than spinning the caller's loop; pair
    /// it with [`state`](Self::state) when a snapshot is enough.
    ///
    /// # Cancel safety
    ///
    /// Cancel-safe: dropping the future loses nothing; a fresh call reports the same change.
    pub async fn changed(&mut self) -> HealthState {
        loop {
            // Registered first and the version read after, so a change in between is not lost.
            let parked = self.shared.changed.listen();
            let version = self.shared.version.load(Ordering::Acquire);
            if version != self.seen {
                self.seen = version;
                return self.state();
            }
            parked.await;
        }
    }
}

/// The write half the running app, its fail-fast watcher and the loops' guards drive; probes are
/// taken off it, so the public surface stays read-only.
#[derive(Debug, Clone)]
pub(crate) struct HealthReport(Arc<Shared>);

impl HealthReport {
    /// A running service with no subscription started yet.
    pub(crate) fn new() -> Self {
        Self(Arc::new(Shared {
            state: AtomicU8::new(RUNNING),
            reason: Mutex::new(String::new()),
            started: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            version: AtomicU64::new(0),
            changed: Event::new(),
        }))
    }

    /// A probe that has seen everything up to now.
    pub(crate) fn probe(&self) -> HealthProbe {
        HealthProbe {
            shared: Arc::clone(&self.0),
            seen: self.0.version.load(Ordering::Acquire),
        }
    }

    /// Moves the lifecycle state to `state`.
    pub(crate) fn transition(&self, state: HealthState) {
        let code = match state {
            HealthState::Running => RUNNING,
            HealthState::ShuttingDown => SHUTTING_DOWN,
            HealthState::Failed { reason } => {
                if let Ok(mut slot) = self.0.reason.lock() {
                    *slot = reason;
                }
                FAILED
            }
            // `Stopped`, and a state the enum may name later, read as a stopped service.
            _ => STOPPED,
        };
        self.0.state.store(code, Ordering::Release);
        self.0.announce();
    }

    /// Counts a subscription's loop as consuming until the returned guard drops.
    ///
    /// Made where the loop is started and moved into the task that holds it, so the count covers
    /// the loop from before `start` returns, and the guard dropping with that task (an end, a
    /// shutdown, a cancel, a panic) takes it off again.
    pub(crate) fn consuming(&self) -> Consuming {
        self.0.started.fetch_add(1, Ordering::AcqRel);
        self.0.active.fetch_add(1, Ordering::AcqRel);
        self.0.announce();
        Consuming(Arc::clone(&self.0))
    }
}

/// A subscription's loop counted as consuming; see [`HealthReport::consuming`].
#[derive(Debug)]
pub(crate) struct Consuming(Arc<Shared>);

impl Drop for Consuming {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.announce();
    }
}
