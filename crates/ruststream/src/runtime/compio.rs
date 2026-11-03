//! The compio runner: one runtime per core, core 0 receiving, on `io_uring` where the kernel has it.
//!
//! - [Compio runtime][runtime]: [`RustStream`], the app builder with the names of the tokio
//!   runner's, [`RustStream::run`] over the [`Cores`] the service runs on, and
//!   `#[ruststream::app(runtime = compio, cores = n)]`, which generates the `main`.
//! - [Placement][runtime]: core 0 receives every subscription, `workers(n)` and `threads(n)` are
//!   placed across the cores, and [`Core`] tells a handler which core it runs on.
//! - [Shutdown and health][runtime]: the signals end the run, [`RustStream::run_until`] ends it on
//!   a future of the caller's, and [`RunningApp`] hands out a [`HealthProbe`].
//! - [Time][runtime]: [`time::sleep`] sleeps on the app's clock, [`CompioClock`].
//! - [Testing][testing]: `ruststream::testing::compio::TestApp` drives the app on the test's own
//!   compio runtime, with a virtual clock that `advance` moves.
//!
//! [runtime]: https://powersemmi.github.io/ruststream/latest/components/application/compio-runtime/
//! [testing]: https://powersemmi.github.io/ruststream/latest/components/application/testing/compio-app/

pub use ruststream_runtime::Core;
pub use ruststream_runtime_compio::{
    App, AppInfo, BrokerScope, CompioClock, Cores, CoresError, HealthProbe, HealthState,
    RunningApp, RustStream, RustStreamError, Setup, SubscriptionCount, SubscriptionEnd, Wired,
};

/// The entry point `#[ruststream::app(runtime = compio)]` expands to.
pub use ruststream_cli::compio::run_main;

/// Time for service code on the compio runner: the sleep that runs on the app's clock.
pub mod time {
    pub use ruststream_runtime_compio::time::sleep;
}
