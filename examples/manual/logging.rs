//! The service of `examples/logging.rs` without the `macros` feature: `main` is written by hand,
//! so it installs the logger itself, with defaults of its own, before it hands the builder to
//! `run_main`.
//!
//! ```text
//! cargo run --example manual_logging --no-default-features \
//!     --features testing,memory,json,logging -- run
//! cargo test --example manual_logging --no-default-features --features testing,memory,json,logging
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::logging::{Logging, LoggingInitError};
use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/captured_logs.rs"]
mod captured_logs;

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:app]
struct Accept;

impl Handle<Order> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order accepted");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Accept).build())
    })
}

fn main() -> Result<ExitCode, LoggingInitError> {
    // `RUST_LOG` still wins; this filter applies when it is unset.
    Logging::new()
        .with_default_filter("ruststream=debug,info")
        .try_init()?;
    // `run_main` finds the logger installed and leaves it in place.
    Ok(run_main(app))
}
// --8<-- [end:app]

// `main` installs the logger; this test reads the handler's event through a subscriber of its
// own, on the same builder.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tracing::Level;

    use super::captured_logs::CapturedLogs;
    use super::{Order, app};

    #[tokio::test]
    async fn the_handler_logs_the_order_it_accepts() -> Result<(), Box<dyn Error>> {
        let logs = CapturedLogs::default();
        let _guard = logs.install(Level::INFO);
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        orders
            .message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await?;

        orders.subscriber("orders").assert_called_once();
        let text = logs.text();
        assert!(
            text.contains("order accepted") && text.contains("order.id=7"),
            "the handler's event is missing from the log:\n{text}"
        );
        Ok(())
    }
}
