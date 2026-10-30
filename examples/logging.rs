//! Console logging with no setup: the `main` that `#[ruststream::app]` generates installs the
//! colored logger on `run`, so the handler's own events and the framework's show in the terminal.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example logging \
//!     --features testing,macros,memory,json,logging -- run
//! cargo test --example logging --features testing,macros,memory,json,logging
//! ```

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "support/captured_logs.rs"]
mod captured_logs;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
}

// --8<-- [start:app]
#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order accepted");
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(accept))
}
// --8<-- [end:app]

// The generated `main` installs the logger; this test reads the handler's event through a
// subscriber of its own, on the same builder.
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
