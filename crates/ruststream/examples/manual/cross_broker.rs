//! The service of `examples/cross_broker.rs` written without the `macros` feature. The token
//! and the scopes are the same in both forms; only the handlers differ.
//!
//! ```text
//! cargo test --example manual_cross_broker --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::future::ready;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Order {
    id: u64,
}

impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
struct Mirrored {
    id: u64,
}

impl OutgoingDestination for Mirrored {
    type Form = FixedName;

    const DESTINATION: &'static str = "orders.mirrored";
}

impl MessageHeaders for Mirrored {
    type Contract = NoHeaders;
}

struct Mirror;

impl Handle<Order, Mirrored> for Mirror {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Mirrored, HandlerOutcome>> {
        ready(Ok(Mirrored { id: order.id }))
    }
}

struct Record;

impl Handle<Mirrored> for Record {
    fn handle(
        &self,
        order: &Mirrored,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "mirrored order recorded");
        ready(Ok(()))
    }
}

// --8<-- [start:app]
fn app() -> impl App {
    // The target broker is wrapped first, so its token exists before either scope is built.
    let west = MemoryBroker::new().bindable();
    let to_west = west.bind(Publish);
    RustStream::new(AppInfo::new("bridge", "0.1.0"))
        .with_broker_labeled("east", MemoryBroker::new(), |b| {
            // Consumed in the east, answered in the west.
            b.include(subscriber("orders", Mirror).reply().build())
                .out_reply(to_west)
        })
        .with_broker_labeled("west", west, |b| {
            b.include(subscriber("orders.mirrored", Record).build())
        })
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Mirrored, Order, app};

    #[tokio::test]
    async fn an_order_from_the_east_reaches_the_west() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker_named("east")
            .message(&Order { id: 5 })
            .to("orders")
            .publish()
            .await?;

        tb.broker_named("west")
            .published::<Mirrored>("orders.mirrored")
            .assert_called_once()
            .with(&Mirrored { id: 5 });
        tb.broker_named("west")
            .subscriber("orders.mirrored")
            .assert_called_once()
            .settled(HandlerOutcome::ack());
        tb.broker_named("east")
            .published::<Mirrored>("orders.mirrored")
            .assert_not_called();
        Ok(())
    }
}
