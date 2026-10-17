//! `TracingLayer` on the app: every delivery and its outcome become `tracing` events, and the
//! handlers stay free of logging calls. The `logging` feature renders them on the console.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example tracing_layer \
//!     --features testing,macros,memory,json,logging -- run
//! cargo test --example tracing_layer --features testing,macros,memory,json,logging
//! ```

use ruststream::memory::prelude::*;
use ruststream::runtime::layers::TracingLayer;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "support/captured_logs.rs"]
mod captured_logs;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Order {
    id: u64,
    quantity: u32,
}

// --8<-- [start:app]
#[subscriber("orders")]
async fn confirm(order: &Order) -> HandlerOutcome {
    if order.quantity == 0 {
        // The layer logs this outcome as a WARN with `requeue=false`.
        return HandlerOutcome::drop();
    }
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Wraps every handler the app mounts, router handlers included.
        .layer(TracingLayer::default())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(confirm);
        })
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;
    use tracing::Level;

    use super::captured_logs::CapturedLogs;
    use super::{Order, app};

    #[tokio::test]
    async fn every_delivery_and_its_outcome_are_logged() -> Result<(), Box<dyn Error>> {
        let logs = CapturedLogs::default();
        let _guard = logs.install(Level::DEBUG);
        let tb = TestApp::start(app()).await?;
        let orders = tb.broker::<MemoryBroker>();
        for quantity in [3, 0] {
            orders
                .message(&Order { id: 7, quantity })
                .to("orders")
                .publish()
                .await?;
        }

        orders.subscriber("orders").assert_called(2);
        let text = logs.text();
        let lines = |needle: &str| text.lines().filter(|line| line.contains(needle)).count();
        assert_eq!(lines("delivery received"), 2, "{text}");
        assert_eq!(lines("INFO ruststream::dispatch: handler ack"), 1, "{text}");
        assert_eq!(
            lines("WARN ruststream::dispatch: handler nack requeue=false"),
            1,
            "{text}"
        );
        Ok(())
    }
}
