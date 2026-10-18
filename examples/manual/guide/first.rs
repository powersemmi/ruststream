//! The shop of `examples/guide/first.rs` written without the `macros` feature: the subscriber is
//! a named type with an `impl Handle`, mounted with the `subscriber` constructor.
//!
//! ```text
//! cargo run --example manual_guide_first --no-default-features \
//!     --features testing,memory,json,logging -- run
//! cargo test --example manual_guide_first --no-default-features --features testing,memory,json
//! ```

use std::future::ready;
use std::process::ExitCode;

use ruststream::memory::prelude::*;
use ruststream::runtime::cli::run_main;
use serde::{Deserialize, Serialize};

// --8<-- [start:order]
/// An order placed in the storefront.
#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
    customer: String,
}

// What `#[derive(Outgoing)]` with `#[outgoing(name = "orders.placed")]` writes.
impl OutgoingDestination for Order {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.placed";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}
// --8<-- [end:order]

// --8<-- [start:accept]
/// Takes every placed order; for now it only logs it.
struct Accept;

impl Handle<Order> for Accept {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order = order.id, customer = %order.customer, "order accepted");
        ready(Ok(()))
    }
}
// --8<-- [end:accept]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders.placed", Accept).build());
    })
}

/// What `#[ruststream::app]` writes: the `run` and `asyncapi gen` commands over `app`.
fn main() -> ExitCode {
    run_main(app)
}
// --8<-- [end:app]

// `main` runs the builder; this test runs the same builder in process.
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn a_placed_order_is_accepted() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let order = Order {
            id: 7,
            customer: "ada@example.com".to_owned(),
        };
        tb.message(&order).publish().await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders.placed")
            .assert_called_once()
            .with(&order)
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
