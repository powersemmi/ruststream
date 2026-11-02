//! The compio runner of [RustStream](https://docs.rs/ruststream): one compio runtime per core,
//! with the clock, the shutdown and the launcher a service on `io_uring` runs on. The runner builds
//! on compio 0.20 beta and moves to compio 0.20 final when it ships.
//!
//! - [`CompioClock`] and [`time::sleep`]: the app's clock over compio's timers.
//! - [`Cores`]: the launcher, core 0 on the calling thread and a thread per further core.
//! - [`Stop`]: the shutdown signal, a flag and a wait.
//! - [`Tasks`]: where the work a delivery leaves behind runs, and the wait for it to end.
//! - [`RustStream`]: the app builder, with the tokio runner's names, and the run that starts the
//!   cores: [`run`](RustStream::run), [`run_until`](RustStream::run_until), [`start`](RustStream::start).
//! - [`yield_now`]: the cooperative yield of a delivery loop.

// The contract and the shared body, under the names the runner's modules reach them by
// (`crate::Broker`, `crate::Router`, ...).
#[allow(clippy::wildcard_imports)]
use ruststream_core::*;
#[allow(clippy::wildcard_imports)]
use ruststream_runtime::*;

mod app;
mod clock;
mod cores;
#[doc(hidden)]
pub mod dispatch;
#[cfg(feature = "testing")]
mod quiescence;
mod sink;
mod spawn;
mod stop;
mod tasks;
pub mod time;
#[cfg(feature = "testing")]
mod virtual_clock;
mod yield_now;

pub use app::{
    App, AppInfo, BrokerScope, HealthProbe, HealthState, RunningApp, RustStream, RustStreamError,
    Setup, SubscriptionCount, SubscriptionEnd, Wired,
};
#[cfg(feature = "testing")]
#[doc(hidden)]
pub use app::{Harness, InProcessError, InProcessStart, StateSource};
#[doc(hidden)]
pub use app::{
    Launch, MountRoutes, Mounted, ScopeRegistrations, ScopedHandle, ScopedRouter, ScopedStartup,
};
#[doc(hidden)]
pub use app::{LifecycleHook, RegisteredBroker, Starter};
pub use clock::CompioClock;
pub use cores::{Cores, CoresError};
pub use sink::CoreSink;
pub use stop::{Cancelled, Stop};
pub use tasks::Tasks;
pub use yield_now::{YieldNow, yield_now};

/// The virtual clock the test harness moves.
#[cfg(feature = "testing")]
#[doc(hidden)]
pub mod testing {
    pub use crate::quiescence::{Quiescence, Runnable};
    use crate::spawn::spawn;
    pub use crate::virtual_clock::VirtualClock;
    use crate::virtual_clock::armed;

    /// Starts `work` on this thread's runtime as a task the harness follows, and lets it run on.
    pub fn spawn_detached(work: impl Future<Output = ()> + 'static) {
        spawn(work).detach();
    }

    /// How many timers are armed on the calling thread's virtual clock.
    #[must_use]
    pub fn armed_timers() -> usize {
        armed()
    }
}
