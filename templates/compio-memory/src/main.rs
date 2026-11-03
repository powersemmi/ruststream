//! {{project-name}} - a RustStream service on the compio runtime.
//!
//! Handlers live in `orders`, wiring in `routes`. `main` installs the log subscriber and hands the
//! app to `run_main`, which starts one single-threaded runtime per core (`io_uring` on Linux):
//! core 0 owns the bus and receives, and `cargo run` runs the service until `SIGINT` or `SIGTERM`.

mod orders;
mod routes;

use std::process::ExitCode;

// Links the critical section the bus locks through.
use critical_section as _;
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::local::LocalMemoryBroker;
use ruststream::runtime::compio::{App, AppInfo, RustStream, run_main};

/// The in-process bus: each subscription holds up to 64 messages, waiting or in a handler's hands.
pub type Bus = LocalMemoryBroker<64>;

/// Builds the service: the bus with the orders router mounted.
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0"))
        .logger(TracingLogger)
        .with_broker(Bus::new(), |b| b.include_router(routes::orders()))
}

/// Prints the framework's and the handlers' log lines, then runs the app on every core the
/// process may use (`Some(n)` names a count).
fn main() -> ExitCode {
    tracing_subscriber::fmt().init();
    run_main(None, app)
}
