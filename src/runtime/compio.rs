//! The compio runner: one runtime per core, core 0 receiving, on `io_uring` where the kernel has it.
//!
//! [`RustStream`] here is the compio counterpart of the tokio runner's app builder, with the same
//! methods; [`RustStream::run`] takes the [`Cores`] the service runs on.

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
