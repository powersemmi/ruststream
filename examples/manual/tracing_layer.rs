//! The service of `examples/tracing_layer.rs` without the `macros` feature. The layer is runtime
//! API, so it mounts the same way; the handler is a named type and `main` is written by hand.
//!
//! ```text
//! RUST_LOG=ruststream=debug,info cargo run --example manual_tracing_layer \
//!     --no-default-features --features testing,memory,json,logging -- run
//! cargo test --example manual_tracing_layer --no-default-features \
//!     --features testing,memory,json,logging
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;
use ruststream::runtime::layers::TracingLayer;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "../support/captured_logs.rs"]
mod captured_logs;

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    quantity: u32,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

// --8<-- [start:app]
struct Confirm;

impl Handle<Order> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        if order.quantity == 0 {
            // The layer logs this outcome as a WARN with `requeue=false`.
            return ready(Err(HandlerOutcome::drop()));
        }
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Wraps every handler the app mounts, router handlers included.
        .layer(TracingLayer::default())
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Confirm).build());
        })
}
// --8<-- [end:app]

// What `#[ruststream::app]` writes; with the `logging` feature it installs the console logger on
// `run`, which renders the layer's events.
fn main() -> ExitCode {
    run_main(app)
}

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
