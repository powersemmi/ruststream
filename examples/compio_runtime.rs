//! A service on the compio runner: one runtime per core, core 0 receiving, a subscription with
//! four workers placed across the cores, and a reply over the same bus.
//!
//! ```text
//! cargo run --example compio_runtime --no-default-features --features compio,testing,macros,memory,json -- run
//! cargo test --example compio_runtime --no-default-features --features compio,testing,macros,memory,json
//! ```

use serde::{Deserialize, Serialize};

// --8<-- [start:app]
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::local::{LocalMemoryBroker, LocalMemoryPublish, LocalMemorySource};
use ruststream::runtime::Ctx;
use ruststream::runtime::HandlerOutcome;
use ruststream::runtime::compio::{App, AppInfo, Core, RustStream};
use ruststream::{Outgoing, subscriber};

/// Up to four orders are confirmed at once, on the cores the app places the workers on.
#[subscriber(LocalMemorySource::new("orders"), workers(4), reply)]
async fn confirm(order: &Order, Ctx(core): Ctx<Core>) -> Confirmation {
    Confirmation {
        id: order.id,
        core: core.index(),
    }
}

/// Audits on core 0, where the broker receives.
#[subscriber(LocalMemorySource::new("orders"))]
async fn audit(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "audited");
    HandlerOutcome::ack()
}

#[ruststream::app(runtime = compio, cores = 4)]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .logger(TracingLogger)
        .with_broker(LocalMemoryBroker::<4>::new(), |b| {
            b.include(confirm)
                .out_reply(LocalMemoryPublish)
                .include(audit)
        })
}
// --8<-- [end:app]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    id: u64,
    core: usize,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use compio::runtime::Runtime;
    use ruststream::memory::local::LocalMemoryBroker;
    use ruststream::testing::compio::TestApp;

    use super::{Confirmation, Order, app};

    // --8<-- [start:test]
    #[test]
    fn an_order_is_confirmed_and_audited() -> Result<(), Box<dyn Error>> {
        Runtime::new()?.block_on(async {
            let tb = TestApp::start(app()).await?;
            let orders = tb.broker::<LocalMemoryBroker<4>>();
            orders.message(&Order { id: 7 }).publish().await?;

            // The harness runs the app as the one core of the test's runtime.
            orders
                .published::<Confirmation>("confirmations")
                .assert_called_once()
                .with(&Confirmation { id: 7, core: 0 });
            orders.subscriber("orders").assert_called(2);
            tb.shutdown().await?;
            Ok(())
        })
    }
    // --8<-- [end:test]
}
