//! {{project-name}} - a RustStream service.
//!
//! Handlers live in `orders`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.
//! - `cargo run -- help` lists the commands the binary understands.

mod orders;
mod routes;

use ruststream::diagnostics::TracingLogger;
use ruststream::memory::prelude::*;

/// Builds the service: one in-memory broker with the orders router mounted.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0"))
        .logger(TracingLogger)
        .with_broker(MemoryBroker::new(), |b| b.include_router(routes::orders()))
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::app;
    use crate::orders::{Confirmation, Order};

    fn order(quantity: u32) -> Order {
        Order {
            id: 7,
            item: "tea".to_owned(),
            quantity,
        }
    }

    /// The test starts the production app unchanged and places an order through its broker.
    #[tokio::test]
    async fn a_placed_order_is_handled_and_acknowledged() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&order(2))
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&order(2))
            .settled(HandlerOutcome::ack());
        Ok(())
    }

    /// The reply is recorded on its destination, where the test reads it back.
    #[tokio::test]
    async fn an_order_is_confirmed_on_the_confirmations_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<MemoryBroker>()
            .message(&order(0))
            .publish()
            .await?;

        tb.broker::<MemoryBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation {
                id: 7,
                accepted: false,
            });
        Ok(())
    }
}
