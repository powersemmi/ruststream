//! The service of `examples/publisher.rs` written without the `macros` feature. The policy and
//! the startup hook are the same in both forms; only the handler differs.
//!
//! ```text
//! cargo test --example manual_publisher --no-default-features --features testing,memory,json
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

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
struct Started {
    service: String,
}

impl OutgoingDestination for Started {
    type Form = FixedName;

    const DESTINATION: &'static str = "services.started";
}

impl MessageHeaders for Started {
    type Contract = NoHeaders;
}

// --8<-- [start:handler]
/// The bound names a capability, never the broker's publisher type.
struct Archive;

impl<Egress> Handle<Order, (), Outs<(Egress,)>> for Archive
where
    Egress: OutEntry<DefaultSlot, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(Egress,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        if outs
            .get(DefaultSlot)
            .message(order)
            .to("orders.archive")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}
// --8<-- [end:handler]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("archive", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        // A startup hook gets the live publisher of the policy it names.
        b.after_startup(Publish, async move |publisher| {
            publisher
                .message(&Started {
                    service: "archive".into(),
                })
                .publish()
                .await
        })
        // A policy holds no connection; startup pairs it with the connected broker.
        .include(subscriber("orders", Archive).build())
        .out(DefaultSlot, Publish)
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

    use super::{Order, Started, app};

    #[tokio::test]
    async fn the_service_announces_itself_once_started() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<MemoryBroker>()
            .published::<Started>("services.started")
            .assert_called_once()
            .with(&Started {
                service: "archive".into(),
            });
        Ok(())
    }

    #[tokio::test]
    async fn an_order_is_archived() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 6 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Order>("orders.archive")
            .assert_called_once()
            .with(&Order { id: 6 });
        Ok(())
    }
}
