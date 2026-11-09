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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use compio::runtime::Runtime;
    use ruststream::runtime::HandlerOutcome;
    use ruststream::testing::compio::TestApp;

    use super::{Bus, app};
    use crate::orders::{Confirmation, Order};

    /// The harness runs the app as core 0 of 1 on the test's own runtime, so the test is a plain
    /// `#[test]` that blocks on one future.
    #[test]
    fn a_placed_order_is_handled_and_acknowledged() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            let order = Order { id: 7, quantity: 2 };
            tb.broker::<Bus>().message(&order).publish().await?;

            tb.broker::<Bus>()
                .subscriber("orders")
                .assert_called(2)
                .with(&order)
                .settled(HandlerOutcome::ack());
            tb.shutdown().await?;
            Ok(())
        })
    }

    /// The reply is recorded on its destination, where the test reads it back.
    #[test]
    fn an_order_is_confirmed_on_the_confirmations_channel() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            tb.broker::<Bus>()
                .message(&Order { id: 7, quantity: 0 })
                .publish()
                .await?;

            tb.broker::<Bus>()
                .published::<Confirmation>("confirmations")
                .assert_called_once()
                .with(&Confirmation {
                    id: 7,
                    accepted: false,
                    core: 0,
                });
            tb.shutdown().await?;
            Ok(())
        })
    }
}
